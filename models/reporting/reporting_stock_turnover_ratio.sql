{{ config(
    schema='reporting',
    materialized='view'
) }}

SELECT
    fi.product_key,
    dp.product_id,
    dp.product_name,
    dp.category,
    dp.subcategory,

    fi.store_key,
    ds.store_id,
    ds.store_name,

    fi.date_key,
    dd.full_date,

    fi.sold_quantity,
    fi.beginning_stock,
    fi.ending_stock,
    fi.stock_turnover_ratio,

    CASE
        WHEN fi.stock_turnover_ratio IS NULL
            THEN 'No Turnover Data'
        WHEN fi.stock_turnover_ratio = 0
            THEN 'No Movement'
        WHEN fi.stock_turnover_ratio >= 1
            THEN 'High Turnover'
        ELSE 'Low Turnover'
    END AS turnover_category

FROM {{ ref('fact_inventory') }} AS fi

LEFT JOIN {{ ref('dim_product') }} AS dp
    ON dp.product_key = fi.product_key

LEFT JOIN {{ ref('dim_store') }} AS ds
    ON ds.store_key = fi.store_key

LEFT JOIN {{ ref('dim_date') }} AS dd
    ON dd.date_key = fi.date_key
