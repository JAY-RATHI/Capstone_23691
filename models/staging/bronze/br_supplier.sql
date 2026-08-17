{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['SOURCE_FILE','ROW_NUMBER']
) }}

SELECT
    RAW_DATA,
    METADATA$FILENAME AS SOURCE_FILE,
    METADATA$FILE_ROW_NUMBER AS ROW_NUMBER,
    CURRENT_TIMESTAMP() AS LOADED_AT,
    '{{ invocation_id }}' AS BATCH_ID
FROM {{ source('external','supplier') }}