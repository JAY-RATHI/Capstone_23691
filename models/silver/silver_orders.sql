{{ config(
    materialized='table'
) }}

-- Step 1: pull the bronze rows we need, nothing more
with source_data as (

    select
        SOURCE_FILE,
        ROW_NUMBER,
        RAW_DATA,
        LOADED_AT,
        BATCH_ID
    from {{ ref('br_orders') }}

),

-- Step 2: unnest the orders_data array so each order is its own row
flattened_orders as (

    select
        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,
        order_data.value as order_data
    from source_data s,
    lateral flatten(input => s.RAW_DATA:orders_data) order_data

),

-- Step 3: extract the order-header fields (everything except line items)
order_header as (

    select

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,

        nullif(trim(order_data:order_id::varchar), '') as order_id,
        nullif(trim(order_data:customer_id::varchar), '') as customer_id,
        nullif(trim(order_data:store_id::varchar), '') as store_id,
        nullif(trim(order_data:employee_id::varchar), '') as employee_id,

        -- required by Gold Fact_MarketingPerformance for campaign attribution
        nullif(trim(order_data:campaign_id::varchar), '') as campaign_id,

        -- keep full timestamp so order_hour can be derived downstream
        try_to_timestamp_ntz(nullif(trim(order_data:order_date::varchar), '')) as order_datetime,
        try_to_date(nullif(trim(order_data:order_date::varchar), '')) as order_date,

        try_to_date(nullif(trim(order_data:shipping_date::varchar), '')) as shipping_date,
        try_to_date(nullif(trim(order_data:delivery_date::varchar), '')) as delivery_date,
        try_to_date(nullif(trim(order_data:estimated_delivery_date::varchar), '')) as estimated_delivery_date,

        -- discount is a rate/fraction, not a dollar amount
        coalesce(
            try_to_decimal(nullif(trim(order_data:discount_amount::varchar), ''), 18, 6),
            0
        ) as order_discount_amount,

        -- currency string parsing, e.g. "$24,005.75"
        coalesce(
            try_to_decimal(
                nullif(regexp_replace(trim(order_data:shipping_cost::varchar), '[$,]', ''), ''),
                18, 2
            ),
            0.00
        ) as shipping_cost,

        coalesce(
            try_to_decimal(
                nullif(regexp_replace(trim(order_data:tax_amount::varchar), '[$,]', ''), ''),
                18, 2
            ),
            0.00
        ) as tax_amount,

        order_data:order_items as order_items

    from flattened_orders

),

-- Step 4: unnest each order's order_items array
flattened_items as (

    select
        o.SOURCE_FILE,
        o.ROW_NUMBER,
        o.LOADED_AT,
        o.BATCH_ID,
        o.order_id,
        item.value as item_data
    from order_header o,
    lateral flatten(input => o.order_items) item

),

-- Step 5: clean the individual line items
cleaned_items as (

    select

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,
        order_id,

        nullif(trim(item_data:product_id::varchar), '') as product_id,

        coalesce(
            try_to_number(nullif(trim(item_data:quantity::varchar), '')),
            0
        ) as quantity,

        coalesce(
            try_to_decimal(
                nullif(regexp_replace(trim(item_data:unit_price::varchar), '[$,]', ''), ''),
                18, 2
            ),
            0.00
        ) as unit_price,

        coalesce(
            try_to_decimal(
                nullif(regexp_replace(trim(item_data:cost_price::varchar), '[$,]', ''), ''),
                18, 2
            ),
            0.00
        ) as cost_price,

        -- item discount is a rate/fraction, not a dollar amount
        coalesce(
            try_to_decimal(nullif(trim(item_data:discount_amount::varchar), ''), 18, 6),
            0
        ) as item_discount_amount

    from flattened_items

),

-- Step 6: roll line items up to order grain (one row per order)
order_item_aggregates as (

    select
        order_id,
        count(product_id) as total_items,
        sum(quantity) as total_quantity,
        sum(quantity * unit_price) as total_amount,
        sum(quantity * cost_price) as total_cost,
        sum(item_discount_amount) as total_discount,

        -- revenue net of the item-level discount
        sum(quantity * unit_price * (1 - item_discount_amount)) as line_revenue,
        sum(quantity * cost_price) as line_cost

    from cleaned_items
    group by order_id

),

-- Step 7: join the header back to its item-level aggregates
combined as (

    select

        o.SOURCE_FILE,
        o.ROW_NUMBER,
        o.LOADED_AT,
        o.BATCH_ID,

        o.order_id,
        o.customer_id,
        o.store_id,
        o.employee_id,
        o.campaign_id,

        o.order_datetime,
        o.order_date,

        o.shipping_date,
        o.delivery_date,
        o.estimated_delivery_date,

        o.order_discount_amount,

        o.shipping_cost,
        o.tax_amount,

        coalesce(i.total_items, 0) as total_items,
        coalesce(i.total_quantity, 0) as total_quantity,
        coalesce(i.total_amount, 0.00) as total_amount,
        coalesce(i.total_cost, 0.00) as total_cost,
        coalesce(i.total_discount, 0.00) as total_discount,
        coalesce(i.line_revenue, 0.00) as line_revenue,
        coalesce(i.line_cost, 0.00) as line_cost

    from order_header o
    left join order_item_aggregates i
        on o.order_id = i.order_id

),

-- Step 8: derive order-specific attributes on top of the combined columns
derived as (

    select

        c.*,

        extract(hour from c.order_datetime) as order_hour,

        -- half-open ranges so buckets never overlap
        case
            when extract(hour from c.order_datetime) >= 5
                 and extract(hour from c.order_datetime) < 12
                then 'Morning'
            when extract(hour from c.order_datetime) >= 12
                 and extract(hour from c.order_datetime) < 17
                then 'Afternoon'
            when extract(hour from c.order_datetime) >= 17
                 and extract(hour from c.order_datetime) < 22
                then 'Evening'
            else 'Night'
        end as order_time_of_day,

        week(c.order_date) as order_week,
        month(c.order_date) as order_month,
        quarter(c.order_date) as order_quarter,
        year(c.order_date) as order_year,

        -- line_revenue is already net of the item-level discount; the
        -- order-level discount is then applied multiplicatively on top
        (c.line_revenue * (1 - c.order_discount_amount))
            - c.line_cost - c.shipping_cost - c.tax_amount
            as profit_amount,

        case
            when c.line_revenue > 0 then
                (
                    (
                        (c.line_revenue * (1 - c.order_discount_amount))
                        - c.line_cost - c.shipping_cost - c.tax_amount
                    ) / c.line_revenue
                ) * 100
            else null
        end as profit_margin_percentage,

        datediff(day, c.order_date, c.shipping_date) as processing_days,
        datediff(day, c.shipping_date, c.delivery_date) as shipping_days,

        case
            when c.delivery_date is not null and c.delivery_date <= c.estimated_delivery_date
                then 'On Time'
            when c.delivery_date is not null and c.delivery_date > c.estimated_delivery_date
                then 'Delayed'
            when c.delivery_date is null and current_date() > c.estimated_delivery_date
                then 'Potentially Delayed'
            else 'In Transit'
        end as delivery_status

    from combined c

),

-- Step 9: collapse to one row per order_id, keeping the freshest version
-- (rows with no order_id are kept individually rather than collapsed together)
deduplicated as (

    select *
    from derived
    qualify row_number() over (
        partition by
            case
                when order_id is not null then order_id
                else concat('_NULL_', SOURCE_FILE, '_', ROW_NUMBER)
            end
        order by
            order_datetime desc nulls last,
            LOADED_AT desc,
            SOURCE_FILE desc,
            ROW_NUMBER desc
    ) = 1

)

select * from deduplicated