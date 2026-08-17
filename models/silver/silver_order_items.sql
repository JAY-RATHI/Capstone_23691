{{ config(
    materialized='table'
) }}

-- Step 1: pull the bronze rows we need, nothing more
with order_items_source as (

    select
        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,
        RAW_DATA
    from {{ ref('br_orders') }}

),

-- Step 2: unnest the orders_data array - the full order object is needed
-- here since store_id lives at the order-header level, not on each item
flattened_orders as (

    select
        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,
        order_data.value as order_data
    from order_items_source s,
    lateral flatten(input => s.RAW_DATA:orders_data) as order_data

),

-- Step 3: pull out order-header fields that each line item will inherit
-- (store_id comes from the parent order, not the item itself)
order_header as (

    select
        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,

        nullif(trim(order_data:order_id::varchar), '') as order_id,
        nullif(trim(order_data:customer_id::varchar), '') as customer_id,
        nullif(trim(order_data:store_id::varchar), '') as store_id,
        try_to_date(nullif(trim(order_data:order_date::varchar), '')) as order_date,
        nullif(trim(order_data:order_status::varchar), '') as order_status,

        order_data:order_items as order_items

    from flattened_orders

),

-- Step 4: unnest each order's items, converting the zero-based FLATTEN
-- index into a human-readable, 1-based item_number
flattened_items as (

    select
        o.SOURCE_FILE,
        o.ROW_NUMBER,
        o.LOADED_AT,
        o.BATCH_ID,

        o.order_id,
        o.customer_id,
        o.store_id,
        o.order_date,
        o.order_status,

        item.index + 1 as item_number,
        item.value as item_data

    from order_header o,
    lateral flatten(input => o.order_items) as item

),

-- Step 5: clean each individual line item
cleaned as (

    select

        -- natural grain: order_id + item_number
        {{ dbt_utils.generate_surrogate_key([
            'order_id',
            'item_number'
        ]) }} as order_item_key,

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,

        order_id,
        item_number,

        order_date,
        order_status,
        customer_id,

        -- inherited from the parent order, not the item itself
        store_id,

        -- comes from the order item
        nullif(trim(item_data:product_id::varchar), '') as product_id,

        coalesce(
            try_to_number(nullif(trim(item_data:quantity::varchar), '')),
            0
        ) as quantity,

        -- currency string parsing, e.g. "$24,005.75"
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

        -- kept under its existing project name (discount_percentage),
        -- even though the raw value is a fractional rate
        coalesce(
            try_to_decimal(nullif(trim(item_data:discount_amount::varchar), ''), 18, 6),
            0
        ) as discount_percentage

    from flattened_items

),

-- Step 6: derive a normalized 0-1 discount rate alongside the raw value
derived as (

    select
        c.*,
        case
            when c.discount_percentage is not null then c.discount_percentage / 100
            else 0
        end as discount_rate
    from cleaned c

),

-- Step 7: collapse to one row per order_id + item_number, keeping the
-- freshest version - protects downstream inventory sold_quantity from
-- counting duplicate historical copies of the same line item
deduplicated as (

    select *
    from derived
    qualify row_number() over (
        partition by order_id, item_number
        order by
            order_date desc nulls last,
            LOADED_AT desc,
            SOURCE_FILE desc,
            ROW_NUMBER desc
    ) = 1

)

-- Step 8: final column selection for the silver order items table
select

    order_item_key,

    SOURCE_FILE,
    ROW_NUMBER,
    LOADED_AT,
    BATCH_ID,

    order_id,
    item_number,

    order_date,
    order_status,
    customer_id,
    store_id,

    product_id,
    quantity,

    unit_price,
    cost_price,

    discount_percentage,
    discount_rate

from deduplicated