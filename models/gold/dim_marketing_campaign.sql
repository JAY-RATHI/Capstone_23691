{{ config(
    materialized='table'
) }}

-- Step 1: pull only the campaign columns this dimension needs
with campaigns as (

    select
        campaign_id,
        campaign_name,
        campaign_type,
        target_audience_segmentation,
        budget,
        campaign_duration_days,
        roi_calculation,
        start_date,
        end_date
    from {{ ref('silver_campaign') }}

),

-- Step 2: build the dimension, generating the surrogate key and
-- renaming fields to their Gold-layer names
final as (

    select

        -- surrogate key, generated from the natural campaign_id
        {{ dbt_utils.generate_surrogate_key(['campaign_id']) }} as campaign_key,

        -- natural key
        campaign_id,

        campaign_name,

        -- actual campaign type supplied by the source JSON;
        -- this is NOT the target audience
        campaign_type,

        -- target audience
        target_audience_segmentation as target_audience_segment,

        -- campaign budget
        budget,

        -- days between start_date and end_date
        campaign_duration_days as duration,

        -- normalized source ROI from Silver
        roi_calculation as roi,

        -- campaign dates
        start_date,
        end_date

    from campaigns

)

select * from final