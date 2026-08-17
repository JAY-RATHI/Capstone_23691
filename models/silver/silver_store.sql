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
    from {{ ref('br_store') }}

),

-- Step 2: unnest the stores_data array so each store is its own row
flattened as (

    select
        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,
        store.value as store_data
    from source_data s,
    lateral flatten(input => s.RAW_DATA:stores_data) as store

),

-- Step 3: resolve the nested-vs-flat address fallbacks once, so every
-- downstream column (individual fields + standardized_address) reads
-- from the same resolved value instead of repeating the coalesce
address_fields as (

    select
        *,
        coalesce(store_data:address:street::varchar, store_data:street::varchar) as street_resolved,
        coalesce(store_data:address:city::varchar, store_data:city::varchar) as city_resolved,
        coalesce(store_data:address:state::varchar, store_data:state::varchar) as state_resolved,
        coalesce(store_data:address:country::varchar, store_data:country::varchar) as country_resolved,
        coalesce(
            store_data:address:zip_code::varchar,
            store_data:address:postal_code::varchar,
            store_data:zip_code::varchar,
            store_data:postal_code::varchar
        ) as zip_resolved
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
        nullif(trim(store_data:store_id::varchar), '') as store_id,

        -- prefer store_name, fall back to name, collapse to PascalCase
        -- e.g. "denver downtown" -> "DenverDowntown"
        regexp_replace(
            initcap(trim(coalesce(store_data:store_name::varchar, store_data:name::varchar))),
            '[^A-Za-z0-9]+',
            ''
        ) as store_name,

        initcap(regexp_replace(trim(street_resolved), '[^A-Za-z0-9 ''#.,/-]', '')) as street,
        initcap(regexp_replace(trim(city_resolved), '[^A-Za-z0-9 ''-]', '')) as city,
        upper(trim(state_resolved)) as state,
        upper(trim(country_resolved)) as country,

        -- accepts 5-digit or ZIP+4; anything else becomes null
        case
            when regexp_like(trim(zip_resolved), '^[0-9]{5}(-[0-9]{4})?$')
                then trim(zip_resolved)
            else null
        end as postal_code,

        not regexp_like(trim(zip_resolved), '^[0-9]{5}(-[0-9]{4})?$') as invalid_postal_code_flag,

        -- components re-assembled into one clean, comma-separated string
        concat_ws(
            ', ',
            nullif(initcap(regexp_replace(trim(street_resolved), '[^A-Za-z0-9 ''#.,/-]', '')), ''),
            nullif(initcap(regexp_replace(trim(city_resolved), '[^A-Za-z0-9 ''-]', '')), ''),
            nullif(upper(trim(state_resolved)), ''),
            nullif(trim(zip_resolved), ''),
            nullif(upper(trim(country_resolved)), '')
        ) as standardized_address,

        -- required for DIM_Store
        initcap(regexp_replace(trim(store_data:region::varchar), '[^A-Za-z0-9 ''&/-]', '')) as region,

        -- required for DIM_Store
        initcap(regexp_replace(trim(store_data:store_type::varchar), '[^A-Za-z0-9 ''&/-]', '')) as store_type,

        coalesce(
            try_to_decimal(nullif(trim(store_data:size_sq_ft::varchar), ''), 18, 2),
            0
        ) as size_sq_ft,

        try_to_date(nullif(trim(store_data:opening_date::varchar), '')) as opening_date,

        -- currency string parsing
        coalesce(
            try_to_decimal(
                nullif(regexp_replace(trim(store_data:sales_target::varchar), '[$,]', ''), ''),
                18, 2
            ),
            0.00
        ) as sales_target,

        -- currency string parsing
        coalesce(
            try_to_decimal(
                nullif(regexp_replace(trim(store_data:current_sales::varchar), '[$,]', ''), ''),
                18, 2
            ),
            0.00
        ) as current_sales,

        coalesce(
            try_to_number(nullif(trim(store_data:employee_count::varchar), '')),
            0
        ) as employee_count,

        -- keep full timestamp precision; used later for dedup ordering
        try_to_timestamp_ntz(
            nullif(trim(store_data:last_modified_date::varchar), '')
        ) as last_modified_date

    from address_fields

),

-- Step 5: derive store-specific attributes on top of the cleaned columns
derived as (

    select
        c.*,

        -- <5000 = Small, 5000-10000 = Medium, >10000 = Large
        case
            when c.size_sq_ft < 5000 then 'Small'
            when c.size_sq_ft between 5000 and 10000 then 'Medium'
            when c.size_sq_ft > 10000 then 'Large'
            else null
        end as store_size_category,

        -- guard against a future opening_date producing negative age
        case
            when c.opening_date is not null and c.opening_date <= current_date()
                then datediff(year, c.opening_date, current_date())
            else null
        end as store_age_years,

        -- guard against divide-by-zero on target
        case
            when c.sales_target > 0
                then (c.current_sales / c.sales_target) * 100
            else null
        end as sales_target_achievement_percentage,

        -- guard against divide-by-zero on square footage
        case
            when c.size_sq_ft > 0
                then c.current_sales / c.size_sq_ft
            else null
        end as revenue_per_sq_ft,

        -- guard against divide-by-zero on employee count
        case
            when c.employee_count > 0
                then c.current_sales / c.employee_count
            else null
        end as employee_efficiency

    from cleaned c

),

-- Step 6: flag stores tracking below 90% of target
performance_flagged as (

    select
        d.*,
        d.sales_target_achievement_percentage < 90 as performance_issue_flag
    from derived d

),

-- Step 7: collapse to one row per store_id, keeping the freshest version
-- (rows with no store_id are kept individually rather than collapsed together)
deduplicated as (

    select *
    from performance_flagged
    qualify row_number() over (
        partition by
            case
                when store_id is not null then store_id
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