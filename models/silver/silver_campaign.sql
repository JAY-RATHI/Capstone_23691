{{ config(materialized="table") }}

-- Step 1: pull the bronze rows we need, nothing more
with source_data as (

    select
        source_file,
        row_number,
        raw_data,
        loaded_at,
        batch_id
    from {{ ref("br_campaign") }}

),

-- Step 2: unnest the campaigns_data array so each campaign is its own row
flattened as (

    select
        s.source_file,
        s.row_number,
        s.loaded_at,
        s.batch_id,
        campaign.value as campaign_data
    from source_data s,
    lateral flatten(input => s.raw_data:campaigns_data) as campaign

),

-- Step 3: extract + clean + standardize every output column
cleaned as (

    select

        -- lineage columns, carried through as-is
        source_file,
        row_number,
        loaded_at,
        batch_id,

        -- natural key
        nullif(trim(campaign_data:campaign_id::varchar), '') as campaign_id,

        -- trim, strip unwanted characters, title-case
        initcap(
            regexp_replace(trim(campaign_data:campaign_name::varchar), '[^A-Za-z0-9 ''&-]', '')
        ) as campaign_name,

        -- this is the actual campaign_type field from the Bronze JSON;
        -- it is NOT derived from target_audience_segmentation
        initcap(
            regexp_replace(trim(campaign_data:campaign_type::varchar), '[^A-Za-z0-9 ''&/-]', '')
        ) as campaign_type,

        -- preserves the complete demographic description as-is
        initcap(
            regexp_replace(trim(campaign_data:target_audience::varchar), '[^A-Za-z0-9 ''&/-]', '')
        ) as target_audience_segmentation,

        try_to_date(nullif(trim(campaign_data:start_date::varchar), '')) as start_date,
        try_to_date(nullif(trim(campaign_data:end_date::varchar), '')) as end_date,

        -- currency string parsing
        try_to_decimal(
            nullif(regexp_replace(trim(campaign_data:budget::varchar), '[$,]', ''), ''),
            18, 2
        ) as budget,

        try_to_decimal(
            nullif(regexp_replace(trim(campaign_data:total_cost::varchar), '[$,]', ''), ''),
            18, 2
        ) as total_cost,

        try_to_decimal(
            nullif(regexp_replace(trim(campaign_data:total_revenue::varchar), '[$,]', ''), ''),
            18, 2
        ) as total_revenue,

        -- cast the source value as-is; ROI is not calculated in this model
        try_to_decimal(
            nullif(trim(campaign_data:roi_calculation::varchar), ''),
            18, 4
        ) as roi_calculation,

        try_to_timestamp_ntz(
            nullif(trim(campaign_data:last_modified_date::varchar), '')
        ) as last_modified_date

    from flattened

),

-- Step 4: derive campaign-specific attributes on top of the cleaned columns
derived as (

    select
        c.*,

        -- days elapsed between start_date and end_date; null if either is missing
        case
            when c.start_date is not null and c.end_date is not null
                then datediff(day, c.start_date, c.end_date)
            else null
        end as campaign_duration_days

    from cleaned c

),

-- Step 5: collapse to one row per campaign_id, keeping the freshest version
-- (rows with no campaign_id are kept individually rather than collapsed together)
deduplicated as (

    select *
    from derived
    qualify row_number() over (
        partition by
            case
                when campaign_id is not null then campaign_id
                else concat('_NULL_', source_file, '_', row_number)
            end
        order by
            last_modified_date desc nulls last,
            loaded_at desc,
            source_file desc,
            row_number desc
    ) = 1

)

select * from deduplicated