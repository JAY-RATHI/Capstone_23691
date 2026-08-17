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
    from {{ ref('br_supplier') }}

),

-- Step 2: unnest the suppliers_data array so each supplier is its own row
flattened as (

    select
        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,
        supplier.value as supplier_data
    from source_data s,
    lateral flatten(input => s.RAW_DATA:suppliers_data) as supplier

),

-- Step 3a: pre-extract the raw phone digits once, reused by every phone
-- calculation below instead of re-running the same regex repeatedly
phone_digits as (

    select
        *,
        regexp_replace(
            trim(supplier_data:contact_information:phone::varchar),
            '[^0-9]',
            ''
        ) as phone_digits_only
    from flattened

),

-- Step 3b: pre-split the raw address once, reused by every address
-- component below instead of re-running split_part repeatedly
address_parts as (

    select
        *,
        trim(supplier_data:contact_information:address::varchar) as address_full,
        trim(split_part(supplier_data:contact_information:address::varchar, ',', 1)) as address_part_street,
        trim(split_part(supplier_data:contact_information:address::varchar, ',', 2)) as address_part_city,
        trim(split_part(supplier_data:contact_information:address::varchar, ',', 3)) as address_part_state,
        trim(split_part(supplier_data:contact_information:address::varchar, ',', 4)) as address_part_zip,
        trim(split_part(supplier_data:contact_information:address::varchar, ',', 5)) as address_part_country
    from phone_digits

),

-- Step 4: build every cleaned/standardized output column
cleaned as (

    select

        -- lineage columns, carried through as-is
        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,

        -- natural key
        nullif(trim(supplier_data:supplier_id::varchar), '') as supplier_id,

        -- supplier display name, title-cased with junk characters stripped
        initcap(
            regexp_replace(
                trim(supplier_data:supplier_name::varchar),
                '[^A-Za-z0-9 ''&.,/-]',
                ''
            )
        ) as supplier_name,

        -- contact_information.contact_person -> title-cased contact name
        initcap(
            regexp_replace(
                trim(supplier_data:contact_information:contact_person::varchar),
                '[^A-Za-z0-9 ''-]',
                ''
            )
        ) as contact_name,

        -- contact_information.email -> lowercased if it passes validation, else null
        case
            when regexp_like(
                lower(trim(supplier_data:contact_information:email::varchar)),
                '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$',
                'i'
            )
            then lower(trim(supplier_data:contact_information:email::varchar))
            else null
        end as email,

        not regexp_like(
            lower(trim(supplier_data:contact_information:email::varchar)),
            '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$',
            'i'
        ) as invalid_email_flag,

        -- contact_information.phone -> canonical (XXX) XXX-XXXX,
        -- handling both 10-digit and 11-digit-with-leading-1 formats
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

        -- raw, unparsed address string as it arrived
        address_full as raw_address,

        -- address broken into components
        -- expected shape: street, city, state, zip, country
        address_part_street as street,
        initcap(address_part_city) as city,
        upper(address_part_state) as state,

        case
            when regexp_like(address_part_zip, '^[0-9]{5}(-[0-9]{4})?$')
                then address_part_zip
            else null
        end as postal_code,

        not regexp_like(address_part_zip, '^[0-9]{5}(-[0-9]{4})?$') as invalid_postal_code_flag,

        upper(address_part_country) as country,

        -- address components re-assembled into one clean, comma-separated string
        concat_ws(
            ', ',
            nullif(address_part_street, ''),
            nullif(initcap(address_part_city), ''),
            nullif(upper(address_part_state), ''),
            nullif(address_part_zip, ''),
            nullif(upper(address_part_country), '')
        ) as standardized_address,

        initcap(trim(supplier_data:payment_terms::varchar)) as payment_terms,

        initcap(
            regexp_replace(
                trim(supplier_data:supplier_type::varchar),
                '[^A-Za-z0-9 ''&/-]',
                ''
            )
        ) as supplier_type,

        try_to_timestamp_ntz(
            nullif(trim(supplier_data:last_modified_date::varchar), '')
        ) as last_modified_date

    from address_parts

),

-- Step 5: collapse to one row per supplier_id, keeping the freshest version
-- (rows with no supplier_id are kept individually rather than collapsed together)
deduplicated as (

    select *
    from cleaned
    qualify row_number() over (
        partition by
            case
                when supplier_id is not null then supplier_id
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