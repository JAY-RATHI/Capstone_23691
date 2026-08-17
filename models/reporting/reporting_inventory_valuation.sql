{{ config(
    schema='reporting',
    materialized='view'
) }}

SELECT
    fi.date_key,
    dd.full_date,
    fi.product_key,
    dp.product_id,
    dp.product_name,
    dp.category,
    dp.subcategory,
    fi.store_key,
    ds.store_id,
    ds.store_name,
    fi.supplier_key,
    dsp.supplier_id,
    dsp.supplier_name,
    fi.ending_stock,
    fi.inventory_value

FROM {{ ref('fact_inventory') }} AS fi

LEFT JOIN {{ ref('dim_date') }} AS dd
    ON dd.date_key = fi.date_key

LEFT JOIN {{ ref('dim_product') }} AS dp
    ON dp.product_key = fi.product_key

LEFT JOIN {{ ref('dim_store') }} AS ds
    ON ds.store_key = fi.store_key

LEFT JOIN {{ ref('dim_supplier') }} AS dsp
    ON dsp.supplier_key = fi.supplier_key
