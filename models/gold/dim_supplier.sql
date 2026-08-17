{{ config(
    materialized='table'
) }}

-- Step 1: pull only the supplier columns this dimension needs
with suppliers as (

    select
        supplier_id,
        supplier_name,
        contact_name,
        email,
        phone,
        standardized_address,
        payment_terms,
        supplier_type
    from {{ ref('silver_supplier') }}

),

-- Step 2: build the dimension, generating the surrogate key and
-- collapsing the individual contact fields into one reporting field
final as (

    select

        -- surrogate key, generated from the natural supplier_id
        {{ dbt_utils.generate_surrogate_key(['supplier_id']) }} as supplier_key,

        -- natural key
        supplier_id,

        -- supplier name
        supplier_name,

        -- cleaned contact_name / email / phone combined into one
        -- reporting-friendly field
        concat_ws(
            ' | ',
            nullif(trim(contact_name), ''),
            nullif(trim(email), ''),
            nullif(trim(phone), '')
        ) as contact_information,

        -- payment terms
        payment_terms,

        -- supplier type
        supplier_type

    from suppliers

)

select * from final