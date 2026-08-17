{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='product_history_key',
    on_schema_change='sync_all_columns'
) }}

-- Step 1: pull only new snapshot files on incremental runs, comparing the
-- date embedded in the filename against the latest date already loaded
with source_files as (

    select
        SOURCE_FILE,
        RAW_DATA,
        LOADED_AT,
        BATCH_ID
    from {{ ref('br_product') }}

    {% if is_incremental() %}
    where try_to_date(regexp_substr(SOURCE_FILE, '[0-9]{4}-[0-9]{2}-[0-9]{2}'))
        > (
            select coalesce(max(source_snapshot_date), date '1900-01-01')
            from {{ this }}
        )
    {% endif %}

),

-- Step 2: unnest the products_data array, tagging every row with the
-- snapshot date pulled from its source filename
flattened as (

    select
        SOURCE_FILE,
        LOADED_AT,
        BATCH_ID,
        try_to_date(regexp_substr(SOURCE_FILE, '[0-9]{4}-[0-9]{2}-[0-9]{2}')) as source_snapshot_date,
        product.value as product_data
    from source_files,
    lateral flatten(input => RAW_DATA:products_data) as product

),

-- Step 3: extract + clean every output column
cleaned as (

    select

        -- history key = product + snapshot date (store is not part of the
        -- product JSON, so it isn't part of this key)
        {{ dbt_utils.generate_surrogate_key([
            'product_data:product_id::VARCHAR',
            'source_snapshot_date'
        ]) }} as product_history_key,

        -- source metadata
        SOURCE_FILE,
        source_snapshot_date,
        LOADED_AT,
        BATCH_ID,

        -- natural key
        nullif(trim(product_data:product_id::varchar), '') as product_id,

        -- trim, strip non-alphanumerics, collapse to PascalCase
        regexp_replace(
            initcap(trim(product_data:name::varchar)),
            '[^A-Za-z0-9]',
            ''
        ) as product_name,

        -- name + short_description + technical_specs
        trim(
            concat_ws(
                ' - ',
                nullif(trim(product_data:name::varchar), ''),
                nullif(trim(product_data:short_description::varchar), ''),
                nullif(trim(product_data:technical_specs::varchar), '')
            )
        ) as full_description,

        -- description components, cleaned of disallowed characters
        trim(
            regexp_replace(product_data:short_description::varchar, '[^A-Za-z0-9 ''.,;:/()&%-]', '')
        ) as short_description,

        trim(
            regexp_replace(product_data:technical_specs::varchar, '[^A-Za-z0-9 ''.,;:/()&%_=-]', '')
        ) as technical_specs,

        -- product hierarchy fields, standardized to PascalCase
        regexp_replace(initcap(trim(product_data:category::varchar)), '[^A-Za-z0-9]', '') as category,
        regexp_replace(initcap(trim(product_data:subcategory::varchar)), '[^A-Za-z0-9]', '') as subcategory,
        regexp_replace(initcap(trim(product_data:product_line::varchar)), '[^A-Za-z0-9]', '') as product_line,

        -- basic attributes
        initcap(trim(product_data:brand::varchar)) as brand,
        initcap(trim(product_data:color::varchar)) as color,
        initcap(trim(product_data:size::varchar)) as size,

        -- currency string parsing
        try_to_decimal(
            nullif(regexp_replace(trim(product_data:unit_price::varchar), '[$,]', ''), ''),
            18, 2
        ) as unit_price,

        try_to_decimal(
            nullif(regexp_replace(trim(product_data:cost_price::varchar), '[$,]', ''), ''),
            18, 2
        ) as cost_price,

        -- inventory
        try_to_number(nullif(trim(product_data:stock_quantity::varchar), '')) as stock_quantity,
        try_to_number(nullif(trim(product_data:reorder_level::varchar), '')) as reorder_level,

        -- links to DIM_Supplier
        nullif(trim(product_data:supplier_id::varchar), '') as supplier_id,

        -- remaining attributes
        trim(product_data:dimensions::varchar) as dimensions,
        trim(product_data:warranty_period::varchar) as warranty_period,

        try_to_decimal(
            nullif(regexp_replace(lower(trim(product_data:weight::varchar)), '[^0-9.\-]', ''), ''),
            10, 2
        ) as weight_kg,

        try_to_date(nullif(trim(product_data:launch_date::varchar), '')) as launch_date,

        coalesce(product_data:is_featured::boolean, false) as is_featured,

        -- source modification date
        try_to_date(nullif(trim(product_data:last_modified_date::varchar), '')) as last_modified_date

    from flattened

),

-- Step 4: derive attributes on top of the cleaned columns
derived as (

    select
        c.*,

        trim(
            concat_ws(
                ' > ',
                nullif(c.category, ''),
                nullif(c.subcategory, ''),
                nullif(c.product_line, '')
            )
        ) as product_hierarchy,

        -- guarded against divide-by-zero
        case
            when c.unit_price > 0
                then ((c.unit_price - c.cost_price) / c.unit_price) * 100
            else null
        end as profit_margin_percentage,

        -- unknown when either side of the comparison is missing
        case
            when c.stock_quantity is null or c.reorder_level is null then null
            when c.stock_quantity < c.reorder_level then true
            else false
        end as low_stock_flag

    from cleaned c

),

-- Step 5: exactly one row per product per snapshot date
deduplicated as (

    select *
    from derived
    qualify row_number() over (
        partition by product_id, source_snapshot_date
        order by SOURCE_FILE desc, LOADED_AT desc
    ) = 1

)

-- Step 6: final column selection for the silver product history table
select

    product_history_key,

    SOURCE_FILE,
    source_snapshot_date,
    LOADED_AT,
    BATCH_ID,

    product_id,

    product_name,
    full_description,
    short_description,
    technical_specs,

    category,
    subcategory,
    product_line,
    product_hierarchy,

    brand,
    color,
    size,

    unit_price,
    cost_price,
    profit_margin_percentage,

    stock_quantity,
    reorder_level,
    low_stock_flag,

    supplier_id,

    dimensions,
    weight_kg,
    warranty_period,

    is_featured,
    launch_date,
    last_modified_date

from deduplicated