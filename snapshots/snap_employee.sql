{% snapshot snap_employee %}

{{
    config(
        target_schema='snapshots',
        unique_key='employee_id',
        strategy='timestamp',
        updated_at='last_modified_date'
    )
}}

WITH flattened AS (

    SELECT
        employee.value:employee_id::VARCHAR AS employee_id,

        employee.value:last_modified_date::TIMESTAMP_NTZ
            AS last_modified_date,

        employee.value AS raw_employee_data,

        b.SOURCE_FILE

    FROM {{ ref('br_employee') }} AS b,

    LATERAL FLATTEN(
        INPUT => b.RAW_DATA:employees_data
    ) AS employee

),

latest_employee AS (

    SELECT
        employee_id,
        last_modified_date,
        raw_employee_data

    FROM flattened

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY employee_id
        ORDER BY
            last_modified_date DESC,
            SOURCE_FILE DESC
    ) = 1

)

SELECT
    employee_id,
    last_modified_date,
    raw_employee_data

FROM latest_employee

{% endsnapshot %}