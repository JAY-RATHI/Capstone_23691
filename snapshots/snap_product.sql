{% snapshot snap_product %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='timestamp',
        updated_at='last_modified_date'
    )
}}

WITH flattened AS (

    SELECT
        product.value:product_id::VARCHAR AS product_id,

        product.value:last_modified_date::TIMESTAMP_NTZ
            AS last_modified_date,

        product.value AS raw_product_data,

        b.SOURCE_FILE

    FROM {{ ref('br_product') }} AS b,

    LATERAL FLATTEN(
        INPUT => b.RAW_DATA:products_data
    ) AS product

),

latest_product AS (

    SELECT
        product_id,
        last_modified_date,
        raw_product_data

    FROM flattened

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY product_id
        ORDER BY
            last_modified_date DESC,
            SOURCE_FILE DESC
    ) = 1

)

SELECT
    product_id,
    last_modified_date,
    raw_product_data

FROM latest_product

{% endsnapshot %}