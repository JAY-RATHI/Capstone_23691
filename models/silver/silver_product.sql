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
    from {{ ref('br_product') }}

),

-- Step 2: unnest the products_data array so each product is its own row
flattened as (

    select
        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,
        product.value as product_data
    from source_data s,
    lateral flatten(input => s.RAW_DATA:products_data) product

),

-- Step 3: extract + clean + standardize every output column
cleaned as (

    select

        -- lineage columns, carried through as-is
        SOURCE_FILE,
        ROW_NUMBER,
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

        -- trim + strip characters outside the allowed punctuation set
        trim(
            regexp_replace(
                product_data:short_description::varchar,
                '[^A-Za-z0-9 ''.,;:/()&%-]',
                ''
            )
        ) as short_description,

        -- trim + strip characters outside the allowed punctuation set
        -- (technical_specs additionally allows underscore and equals sign)
        trim(
            regexp_replace(
                product_data:technical_specs::varchar,
                '[^A-Za-z0-9 ''.,;:/()&%_=-]',
                ''
            )
        ) as technical_specs,

        -- standardized to PascalCase
        regexp_replace(
            initcap(trim(product_data:category::varchar)),
            '[^A-Za-z0-9]',
            ''
        ) as category,

        -- standardized to PascalCase
        regexp_replace(
            initcap(trim(product_data:subcategory::varchar)),
            '[^A-Za-z0-9]',
            ''
        ) as subcategory,

        -- standardized to PascalCase
        regexp_replace(
            initcap(trim(product_data:product_line::varchar)),
            '[^A-Za-z0-9]',
            ''
        ) as product_line,

        -- required for DIM_Product
        initcap(
            regexp_replace(trim(product_data:brand::varchar), '[^A-Za-z0-9 ''&.-]', '')
        ) as brand,

        -- required for DIM_Product
        initcap(
            regexp_replace(trim(product_data:color::varchar), '[^A-Za-z0-9 ''&/-]', '')
        ) as color,

        -- required for DIM_Product; kept as a cleaned string since values
        -- can be mixed formats (S, M, L, XL, 42, 12 Oz, etc.)
        trim(
            regexp_replace(product_data:size::varchar, '[^A-Za-z0-9 ''.-]', '')
        ) as size,

        -- links DIM_Product to DIM_Supplier
        nullif(trim(product_data:supplier_id::varchar), '') as supplier_id,

        -- currency string parsing, e.g. "$24,005.75"
        try_to_decimal(
            nullif(regexp_replace(trim(product_data:unit_price::varchar), '[$,]', ''), ''),
            18, 2
        ) as unit_price,

        -- currency string parsing
        try_to_decimal(
            nullif(regexp_replace(trim(product_data:cost_price::varchar), '[$,]', ''), ''),
            18, 2
        ) as cost_price,

        coalesce(
            try_to_number(nullif(trim(product_data:stock_quantity::varchar), '')),
            0
        ) as stock_quantity,

        coalesce(
            try_to_number(nullif(trim(product_data:reorder_level::varchar), '')),
            0
        ) as reorder_level,

        -- standardized to DATE
        try_to_date(
            nullif(trim(product_data:last_modified_date::varchar), '')
        ) as last_modified_date

    from flattened

),

-- Step 4: derive product-specific attributes on top of the cleaned columns
derived as (

    select
        p.*,

        -- name + short_description + technical_specs
        concat_ws(
            ' - ',
            nullif(trim(p.product_name), ''),
            nullif(trim(p.short_description), ''),
            nullif(trim(p.technical_specs), '')
        ) as product_full_description,

        -- category > subcategory > product_line
        concat_ws(
            ' > ',
            nullif(trim(p.category), ''),
            nullif(trim(p.subcategory), ''),
            nullif(trim(p.product_line), '')
        ) as product_hierarchy,

        -- (unit_price - cost_price) / unit_price * 100, guarded against
        -- divide-by-zero
        case
            when p.unit_price > 0
                then ((p.unit_price - p.cost_price) / p.unit_price) * 100
            else null
        end as profit_margin_percentage,

        -- true when stock has fallen below the reorder threshold
        p.stock_quantity < p.reorder_level as low_stock_flag

    from cleaned p

),

-- Step 5: collapse to one row per product_id, keeping the freshest version
-- (rows with no product_id are kept individually rather than collapsed together)
deduplicated as (

    select *
    from derived
    qualify row_number() over (
        partition by
            case
                when product_id is not null then product_id
                else concat('_NULL_', SOURCE_FILE, '_', ROW_NUMBER)
            end
        order by
            last_modified_date desc nulls last,
            LOADED_AT desc,
            SOURCE_FILE desc,
            ROW_NUMBER desc
    ) = 1

)

select * from deduplicated