{{ config(
    materialized='table'
) }}

-- Step 1: pull only the product columns this dimension needs
with products as (

    select
        product_id,
        product_name,
        category,
        subcategory,
        brand,
        color,
        size,
        unit_price,
        cost_price,
        supplier_id
    from {{ ref('silver_product') }}

),

-- Step 2: pull only the supplier columns needed for enrichment
suppliers as (

    select
        supplier_id,
        supplier_name
    from {{ ref('silver_supplier') }}

),

-- Step 3: build the dimension, generating the surrogate key and
-- enriching each product with its supplier name
final as (

    select

        -- surrogate key, generated from the natural product_id
        {{ dbt_utils.generate_surrogate_key(['p.product_id']) }} as product_key,

        -- natural key
        p.product_id,

        -- product attributes
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,
        p.color,
        p.size,

        -- financial attributes
        p.unit_price,
        p.cost_price,

        -- supplier information
        p.supplier_id,
        s.supplier_name

    from products p
    left join suppliers s
        on p.supplier_id = s.supplier_id

)

select * from final