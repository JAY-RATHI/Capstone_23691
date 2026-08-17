{{ config(
    schema='reporting',
    materialized='view'
) }}

SELECT
    dmc.target_audience_segment AS campaign_type,

    COUNT(DISTINCT fmp.campaign_key) AS campaign_count,

    SUM(fmp.total_sales_influenced) AS total_sales_influenced,

    SUM(fmp.total_campaign_cost) AS total_campaign_cost,

    AVG(fmp.roi) AS average_roi,

    CASE
        WHEN SUM(fmp.total_campaign_cost) > 0 THEN
            (
                SUM(fmp.total_sales_influenced)
                - SUM(fmp.total_campaign_cost)
            )
            / SUM(fmp.total_campaign_cost) * 100
        ELSE NULL
    END AS calculated_roi

FROM {{ ref('fact_marketing_performance') }} AS fmp

LEFT JOIN {{ ref('dim_marketing_campaign') }} AS dmc
    ON dmc.campaign_key = fmp.campaign_key

GROUP BY
    dmc.target_audience_segment
