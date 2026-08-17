{{ config(materialized='view') }}

WITH inventory AS (
    SELECT
        inventory_key,
        product_key,
        date_key,
        store_key,
        supplier_key,
        beginning_stock,
        purchased_quantity,
        sold_quantity,
        ending_stock,
        inventory_value,
        snapshot_gap_flag,
        snapshot_gap_days
    FROM {{ ref('fact_inventory') }}
),

suppliers AS (
    SELECT
        supplier_key,
        supplier_id,
        supplier_name,
        supplier_type
    FROM {{ ref('dim_supplier') }}
),

stores AS (
    SELECT
        store_key,
        store_id,
        store_name,
        region,
        store_type
    FROM {{ ref('dim_store') }}
),

dates AS (
    SELECT
        date_key,
        full_date,
        year,
        month,
        quarter
    FROM {{ ref('dim_date') }}
),

classified AS (
    SELECT
        i.inventory_key,
        i.product_key,
        i.date_key,
        i.store_key,
        i.supplier_key,

        d.full_date,
        d.year,
        d.month,
        d.quarter,

        s.supplier_id,
        s.supplier_name,
        s.supplier_type,

        st.store_id,
        st.store_name,
        st.region,
        st.store_type,

        i.beginning_stock,
        i.purchased_quantity,
        i.sold_quantity,
        i.ending_stock,
        i.inventory_value,
        i.snapshot_gap_flag,
        i.snapshot_gap_days,

        CASE
            WHEN COALESCE(i.snapshot_gap_flag, FALSE)
                THEN 'Delayed'
            ELSE 'On Time'
        END AS supply_status

    FROM inventory AS i

    LEFT JOIN suppliers AS s
        ON s.supplier_key = i.supplier_key

    LEFT JOIN stores AS st
        ON st.store_key = i.store_key

    LEFT JOIN dates AS d
        ON d.date_key = i.date_key
)

SELECT
    supplier_key,
    supplier_id,
    supplier_name,
    supplier_type,

    store_key,
    store_id,
    store_name,
    region,
    store_type,

    date_key,
    full_date,
    year,
    month,
    quarter,

    supply_status,

    COUNT(DISTINCT inventory_key) AS inventory_snapshot_count,

    SUM(purchased_quantity) AS total_purchased_quantity,

    SUM(sold_quantity) AS total_sold_quantity,

    SUM(ending_stock) AS total_ending_inventory,

    SUM(inventory_value) AS total_inventory_value,

    AVG(snapshot_gap_days) AS average_snapshot_gap_days,

    COUNT(DISTINCT CASE
        WHEN supply_status = 'On Time'
        THEN inventory_key
    END) AS on_time_snapshot_count,

    COUNT(DISTINCT CASE
        WHEN supply_status = 'Delayed'
        THEN inventory_key
    END) AS delayed_snapshot_count,

    ROUND(
        100.0 * COUNT(DISTINCT CASE
            WHEN supply_status = 'On Time'
            THEN inventory_key
        END) / NULLIF(COUNT(DISTINCT inventory_key), 0),
        2
    ) AS on_time_percentage,

    ROUND(
        100.0 * COUNT(DISTINCT CASE
            WHEN supply_status = 'Delayed'
            THEN inventory_key
        END) / NULLIF(COUNT(DISTINCT inventory_key), 0),
        2
    ) AS delayed_percentage

FROM classified

GROUP BY
    supplier_key,
    supplier_id,
    supplier_name,
    supplier_type,
    store_key,
    store_id,
    store_name,
    region,
    store_type,
    date_key,
    full_date,
    year,
    month,
    quarter,
    supply_status
