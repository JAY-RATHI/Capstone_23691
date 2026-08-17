{{ config(
    schema='reporting',
    materialized='view'
) }}

WITH supplier_purchase AS (
    SELECT
        fi.supplier_key,
        dsp.supplier_id,
        dsp.supplier_name,
        SUM(fi.purchased_quantity) AS total_purchased_quantity
    FROM {{ ref('fact_inventory') }} AS fi
    LEFT JOIN {{ ref('dim_supplier') }} AS dsp
        ON dsp.supplier_key = fi.supplier_key
    GROUP BY
        fi.supplier_key,
        dsp.supplier_id,
        dsp.supplier_name
),

total_purchase AS (
    SELECT
        SUM(total_purchased_quantity) AS total_purchased_quantity
    FROM supplier_purchase
),

supplier_share AS (
    SELECT
        sp.supplier_key,
        sp.supplier_id,
        sp.supplier_name,
        sp.total_purchased_quantity,
        tp.total_purchased_quantity AS all_supplier_purchased_quantity,

        CASE
            WHEN tp.total_purchased_quantity > 0 THEN
                100.0 * sp.total_purchased_quantity
                / NULLIF(tp.total_purchased_quantity, 0)
            ELSE NULL
        END AS supplier_purchase_share_percentage

    FROM supplier_purchase AS sp
    CROSS JOIN total_purchase AS tp
)

SELECT
    supplier_key,
    supplier_id,
    supplier_name,
    total_purchased_quantity,
    all_supplier_purchased_quantity,
    supplier_purchase_share_percentage,

    CASE
        WHEN supplier_purchase_share_percentage >= 50 THEN 'Very High'
        WHEN supplier_purchase_share_percentage >= 30 THEN 'High'
        WHEN supplier_purchase_share_percentage >= 15 THEN 'Medium'
        ELSE 'Low'
    END AS supplier_concentration_risk

FROM supplier_share
