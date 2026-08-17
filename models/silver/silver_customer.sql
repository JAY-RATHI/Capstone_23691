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
    from {{ ref('br_customer') }}

),

-- Step 2: unnest the customers_data array so each customer is its own row
flattened as (

    select
        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,
        customer.value as customer_data
    from source_data s,
    lateral flatten(input => s.RAW_DATA:customers_data) customer

),

-- Step 3: pre-extract the raw phone digits once, reused by every phone
-- calculation below instead of re-running the same regex repeatedly
phone_digits as (

    select
        *,
        regexp_replace(
            trim(customer_data:phone::varchar),
            '[^0-9]',
            ''
        ) as phone_digits_only
    from flattened

),

-- Step 4: extract + clean + standardize every output column
cleaned as (

    select

        -- lineage columns, carried through as-is
        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,

        -- natural key
        nullif(trim(customer_data:customer_id::varchar), '') as customer_id,

        -- trim, strip disallowed characters, title-case
        initcap(
            regexp_replace(trim(customer_data:first_name::varchar), '[^A-Za-z0-9 ''-]', '')
        ) as first_name,

        initcap(
            regexp_replace(trim(customer_data:last_name::varchar), '[^A-Za-z0-9 ''-]', '')
        ) as last_name,

        -- lowercased if it passes validation, else null
        case
            when regexp_like(
                lower(trim(customer_data:email::varchar)),
                '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$',
                'i'
            )
            then lower(trim(customer_data:email::varchar))
            else null
        end as email,

        not regexp_like(
            lower(trim(customer_data:email::varchar)),
            '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$',
            'i'
        ) as invalid_email_flag,

        -- canonical (XXX) XXX-XXXX; supports 10-digit numbers and
        -- 11-digit numbers with a leading 1
        case
            when length(phone_digits_only) = 10
                then '(' || substr(phone_digits_only, 1, 3) || ') '
                     || substr(phone_digits_only, 4, 3) || '-'
                     || substr(phone_digits_only, 7, 4)

            when length(phone_digits_only) = 11 and left(phone_digits_only, 1) = '1'
                then '(' || substr(phone_digits_only, 2, 3) || ') '
                     || substr(phone_digits_only, 5, 3) || '-'
                     || substr(phone_digits_only, 8, 4)

            else null
        end as phone,

        -- valid: 10 digits, or 11 digits beginning with 1
        not (
            length(phone_digits_only) = 10
            or (length(phone_digits_only) = 11 and left(phone_digits_only, 1) = '1')
        ) as invalid_phone_flag,

        -- individual address components, standardized
        initcap(trim(customer_data:address:street::varchar)) as street,
        initcap(trim(customer_data:address:city::varchar)) as city,
        upper(trim(customer_data:address:state::varchar)) as state,
        upper(trim(customer_data:address:country::varchar)) as country,
        trim(customer_data:address:zip_code::varchar) as zip_code,

        -- components re-assembled into one clean, comma-separated string
        concat_ws(
            ', ',
            nullif(initcap(trim(customer_data:address:street::varchar)), ''),
            nullif(initcap(trim(customer_data:address:city::varchar)), ''),
            nullif(upper(trim(customer_data:address:state::varchar)), ''),
            nullif(trim(customer_data:address:zip_code::varchar), ''),
            nullif(upper(trim(customer_data:address:country::varchar)), '')
        ) as standardized_address,

        -- customer attributes
        upper(trim(customer_data:income_bracket::varchar)) as income_bracket,
        initcap(trim(customer_data:occupation::varchar)) as occupation,
        upper(trim(customer_data:loyalty_tier::varchar)) as loyalty_tier,
        upper(trim(customer_data:preferred_communication::varchar)) as preferred_communication,
        upper(trim(customer_data:preferred_payment_method::varchar)) as preferred_payment_method,
        coalesce(customer_data:marketing_opt_in::boolean, false) as marketing_opt_in,

        -- dates, standardized to DATE
        try_to_date(nullif(trim(customer_data:birth_date::varchar), '')) as birth_date,
        try_to_date(nullif(trim(customer_data:registration_date::varchar), '')) as registration_date,
        try_to_date(nullif(trim(customer_data:last_purchase_date::varchar), '')) as last_purchase_date,
        try_to_date(nullif(trim(customer_data:last_modified_date::varchar), '')) as last_modified_date,

        -- numeric values
        coalesce(try_to_number(customer_data:total_purchases::varchar), 0) as total_purchases,
        coalesce(try_to_decimal(customer_data:total_spend::varchar, 18, 2), 0.00) as total_spend

    from phone_digits

),

-- Step 5: derive customer-specific attributes on top of the cleaned columns
derived as (

    select
        c.*,

        -- FirstName || ' ' || LastName
        trim(
            concat_ws(' ', nullif(c.first_name, ''), nullif(c.last_name, ''))
        ) as full_name,

        case
            when c.birth_date is not null
                then datediff(year, c.birth_date, current_date())
            else null
        end as customer_age

    from cleaned c

),

-- Step 6: assign the customer segment
-- non-overlapping bands: Young 18-35, Middle-aged 36-55, Senior 56+
segmented as (

    select
        d.*,
        case
            when customer_age between 18 and 35 then 'Young'
            when customer_age between 36 and 55 then 'Middle-aged'
            when customer_age >= 56 then 'Senior'
            else null
        end as customer_segment
    from derived d

),

-- Step 7: collapse to one row per customer_id, keeping the freshest version
-- (rows with no customer_id are kept individually rather than collapsed together)
deduplicated as (

    select *
    from segmented
    qualify row_number() over (
        partition by
            case
                when customer_id is not null then customer_id
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