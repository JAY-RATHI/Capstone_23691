{{ config(
    materialized='table'
) }}

-- Step 1: pull only the store columns this dimension needs
with stores as (

    select
        store_id,
        store_name,
        standardized_address,
        region,
        store_type,
        opening_date,
        store_size_category
    from {{ ref('silver_store') }}

),

-- Step 2: build the dimension, generating the surrogate key and
-- renaming fields to their Gold-layer names
final as (

    select

        -- surrogate key, generated from the natural store_id
        {{ dbt_utils.generate_surrogate_key(['store_id']) }} as store_key,

        -- natural key
        store_id,

        -- store attributes
        store_name,
        standardized_address as address,
        region,
        store_type,
        opening_date,
        store_size_category as size_category

    from stores

)

select * from final