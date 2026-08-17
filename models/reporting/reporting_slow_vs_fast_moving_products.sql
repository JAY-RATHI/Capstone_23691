{{ config(
    schema='reporting',
    materialized='view'
) }}

WITH product_turnover AS (
    SELECT
        fi.product_key,
        dp.product_id,
        dp.product_name,
        dp.category,
        dp.subcategory,
        SUM(fi.sold_quantity) AS total_sold_quantity,
        AVG(fi.stock_turnover_ratio) AS average_stock_turnover_ratio,
        SUM(fi.inventory_value) AS total_inventory_value
    FROM {{ ref('fact_inventory') }} AS fi
    LEFT JOIN {{ ref('dim_product') }} AS dp
        ON dp.product_key = fi.product_key
    GROUP BY
        fi.product_key,
        dp.product_id,
        dp.product_name,
        dp.category,
        dp.subcategory
),

classified AS (
    SELECT
        product_turnover.*,
        NTILE(4) OVER (
            ORDER BY average_stock_turnover_ratio
        ) AS turnover_quartile
    FROM product_turnover
)

SELECT
    product_key,
    product_id,
    product_name,
    category,
    subcategory,
    total_sold_quantity,
    average_stock_turnover_ratio,
    total_inventory_value,

    CASE
        WHEN average_stock_turnover_ratio IS NULL THEN 'No Movement'
        WHEN turnover_quartile = 1 THEN 'Slow-Moving'
        WHEN turnover_quartile = 4 THEN 'Fast-Moving'
        ELSE 'Medium-Moving'
    END AS product_movement_category

FROM classified
