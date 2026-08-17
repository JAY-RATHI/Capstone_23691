{% snapshot snap_store %}

{{
    config(
        target_schema='snapshots',
        unique_key='store_id',
        strategy='timestamp',
        updated_at='last_modified_date'
    )
}}

WITH flattened AS (

    SELECT
        store.value:store_id::VARCHAR AS store_id,

        store.value:last_modified_date::TIMESTAMP_NTZ
            AS last_modified_date,

        store.value AS raw_store_data,

        b.SOURCE_FILE

    FROM {{ ref('br_store') }} AS b,

    LATERAL FLATTEN(
        INPUT => b.RAW_DATA:stores_data
    ) AS store

),

latest_store AS (

    SELECT
        store_id,
        last_modified_date,
        raw_store_data

    FROM flattened

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY store_id
        ORDER BY
            last_modified_date DESC,
            SOURCE_FILE DESC
    ) = 1

)

SELECT
    store_id,
    last_modified_date,
    raw_store_data

FROM latest_store

{% endsnapshot %}