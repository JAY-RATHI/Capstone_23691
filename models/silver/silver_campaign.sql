{{ config(materialized="table") }}

-- Step 1: pull the bronze rows we need, nothing more
WITH source_data AS (

    SELECT
        source_file,
        row_number,
        raw_data,
        loaded_at,
        batch_id
    FROM {{ ref("br_campaign") }}

),

-- Step 2: unnest the campaigns_data array so each campaign is its own row
flattened AS (

    SELECT
        s.source_file,
        s.row_number,
        s.loaded_at,
        s.batch_id,
        campaign.value AS campaign_data
    FROM source_data s,
    LATERAL FLATTEN(input => s.raw_data:campaigns_data) AS campaign

),

-- Step 3: extract + clean + standardize every output column
cleaned AS (

    SELECT

        -- lineage columns, carried through as-is
        source_file,
        row_number,
        loaded_at,
        batch_id,

        -- natural key
        NULLIF(TRIM(campaign_data:campaign_id::VARCHAR), '') AS campaign_id,

        -- trim, strip unwanted characters, title-case
        INITCAP(
            REGEXP_REPLACE(
                TRIM(campaign_data:campaign_name::VARCHAR),
                '[^A-Za-z0-9 ''&-]',
                ''
            )
        ) AS campaign_name,

        -- preserves the full demographic description, e.g.
        -- "families 18-25" -> "Families 18-25", "suburban" -> "Suburban"
        INITCAP(
            REGEXP_REPLACE(
                TRIM(campaign_data:target_audience::VARCHAR),
                '[^A-Za-z0-9 ''&/-]',
                ''
            )
        ) AS target_audience_segmentation,

        TRY_TO_DATE(NULLIF(TRIM(campaign_data:start_date::VARCHAR), '')) AS start_date,
        TRY_TO_DATE(NULLIF(TRIM(campaign_data:end_date::VARCHAR), '')) AS end_date,

        -- currency string parsing, e.g. "$24,005.75" -> 24005.75
        TRY_TO_DECIMAL(
            NULLIF(REGEXP_REPLACE(TRIM(campaign_data:budget::VARCHAR), '[$,]', ''), ''),
            18, 2
        ) AS budget,

        TRY_TO_DECIMAL(
            NULLIF(REGEXP_REPLACE(TRIM(campaign_data:total_cost::VARCHAR), '[$,]', ''), ''),
            18, 2
        ) AS total_cost,

        -- normalized to numeric only; NOT used to calculate final ROI here
        TRY_TO_DECIMAL(
            NULLIF(REGEXP_REPLACE(TRIM(campaign_data:total_revenue::VARCHAR), '[$,]', ''), ''),
            18, 2
        ) AS total_revenue,

        -- cast as-is from the source; ROI is NOT calculated in this model
        TRY_TO_DECIMAL(
            NULLIF(TRIM(campaign_data:roi_calculation::VARCHAR), ''),
            18, 4
        ) AS roi_calculation,

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(TRIM(campaign_data:last_modified_date::VARCHAR), '')
        ) AS last_modified_date

    FROM flattened

),

-- Step 4: derive campaign-specific attributes on top of the cleaned columns
derived AS (

    SELECT
        c.*,

        -- days elapsed between start_date and end_date; null if either is missing
        CASE
            WHEN c.start_date IS NOT NULL AND c.end_date IS NOT NULL
                THEN DATEDIFF(day, c.start_date, c.end_date)
            ELSE NULL
        END AS campaign_duration_days

    FROM cleaned c

),

-- Step 5: collapse to one row per campaign_id, keeping the freshest version
-- (rows with no campaign_id are kept individually rather than collapsed together)
deduplicated AS (

    SELECT *
    FROM derived
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            CASE
                WHEN campaign_id IS NOT NULL THEN campaign_id
                ELSE CONCAT('_NULL_', source_file, '_', row_number)
            END
        ORDER BY
            last_modified_date DESC NULLS LAST,
            loaded_at DESC,
            source_file DESC,
            row_number DESC
    ) = 1

)

SELECT *
FROM deduplicated