{% macro create_external_table(
    table_name,
    folder_name,
    database_name='CT_JAY_RATHI_DB',
    schema_name='EXTERNAL',
    stage_name='CT_JAY_RATHI_DB.BRONZE.BRONZE_STAGE',
    file_name='Capstone_Project_Data',
    file_format='CT_JAY_RATHI_DB.BRONZE.JSON_FORMAT'
) %}

    {% set sql %}

    CREATE OR REPLACE EXTERNAL TABLE {{ database_name }}.{{ schema_name }}.{{ table_name }}
    (
    RAW_DATA VARIANT AS (VALUE),
    SOURCE_FILE_NAME VARCHAR AS (METADATA$FILENAME),
    FILE_LAST_MODIFIED TIMESTAMP_NTZ AS (METADATA$FILE_LAST_MODIFIED)
    )
    LOCATION = @{{ stage_name }}/{{file_name}}/{{ folder_name }}/
    FILE_FORMAT = {{ file_format }}
    AUTO_REFRESH = FALSE;

    {% endset %}

    {{ log(sql, info=True) }}

    {% do run_query(sql) %}

{% endmacro %}