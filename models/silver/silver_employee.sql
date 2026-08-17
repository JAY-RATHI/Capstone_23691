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
    from {{ ref('br_employee') }}

),

-- Step 2: unnest the employees_data array so each employee is its own row
flattened as (

    select
        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,
        employee.value as employee_data
    from source_data s,
    lateral flatten(input => s.RAW_DATA:employees_data) as employee

),

-- Step 3: pre-extract the raw phone digits once, reused by every phone
-- calculation below instead of re-running the same regex repeatedly
phone_digits as (

    select
        *,
        regexp_replace(
            trim(employee_data:phone::varchar),
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
        nullif(trim(employee_data:employee_id::varchar), '') as employee_id,

        initcap(
            regexp_replace(trim(employee_data:first_name::varchar), '[^A-Za-z0-9 ''-]', '')
        ) as first_name,

        initcap(
            regexp_replace(trim(employee_data:last_name::varchar), '[^A-Za-z0-9 ''-]', '')
        ) as last_name,

        -- lowercased if it passes validation, else null
        case
            when regexp_like(
                lower(trim(employee_data:email::varchar)),
                '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$',
                'i'
            )
            then lower(trim(employee_data:email::varchar))
            else null
        end as email,

        not regexp_like(
            lower(trim(employee_data:email::varchar)),
            '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$',
            'i'
        ) as invalid_email_flag,

        -- canonical (XXX) XXX-XXXX, handling both 10-digit and
        -- 11-digit-with-leading-1 formats
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

        not (
            length(phone_digits_only) = 10
            or (length(phone_digits_only) = 11 and left(phone_digits_only, 1) = '1')
        ) as invalid_phone_flag,

        -- source JSON field = role
        initcap(
            regexp_replace(trim(employee_data:role::varchar), '[^A-Za-z0-9 ''&/-]', '')
        ) as job_title,

        initcap(
            regexp_replace(trim(employee_data:department::varchar), '[^A-Za-z0-9 ''&/-]', '')
        ) as department,

        -- source JSON field = work_location
        nullif(trim(employee_data:work_location::varchar), '') as store_id,

        try_to_date(nullif(trim(employee_data:hire_date::varchar), '')) as hire_date,

        try_to_decimal(
            nullif(trim(employee_data:salary::varchar), ''),
            18, 2
        ) as salary,

        try_to_timestamp_ntz(
            nullif(trim(employee_data:last_modified_date::varchar), '')
        ) as last_modified_date

    from phone_digits

),

-- Step 5: derive attributes on top of the cleaned columns
derived as (

    select
        e.*,
        trim(
            concat_ws(' ', nullif(e.first_name, ''), nullif(e.last_name, ''))
        ) as full_name
    from cleaned e

),

-- Step 6: collapse to one row per employee_id, keeping the freshest version
-- (rows with no employee_id are kept individually rather than collapsed together)
deduplicated as (

    select *
    from derived
    qualify row_number() over (
        partition by
            case
                when employee_id is not null then employee_id
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