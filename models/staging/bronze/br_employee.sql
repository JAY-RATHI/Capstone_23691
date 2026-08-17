{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='SOURCE_FILE'
) }}

SELECT
    RAW_DATA,
    METADATA$FILENAME AS SOURCE_FILE,
    CURRENT_TIMESTAMP() AS LOADED_AT,
    METADATA$FILE_ROW_NUMBER AS ROW_NUMBER,
    '{{ invocation_id }}' AS BATCH_ID
FROM {{ source('external','employee') }}