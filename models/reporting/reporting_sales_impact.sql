{{ config(
    schema='reporting',
    materialized='view'
) }}

SELECT
    fmp.campaign_key,
    dmc.campaign_id,
    dmc.target_audience_segment AS campaign_type,

    fmp.date_key,
    dd.full_date,

    fmp.total_sales_influenced,
    fmp.total_campaign_cost,
    fmp.roi

FROM {{ ref('fact_marketing_performance') }} AS fmp

LEFT JOIN {{ ref('dim_marketing_campaign') }} AS dmc
    ON dmc.campaign_key = fmp.campaign_key

LEFT JOIN {{ ref('dim_date') }} AS dd
    ON dd.date_key = fmp.date_key
