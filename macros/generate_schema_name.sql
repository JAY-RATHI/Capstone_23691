 
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- set dbt_env = env_var("DBT_CLOUD_ENVIRONMENT_NAME") -%}
    {%- if dbt_env == "Development" -%} {{ default_schema | trim }}
    {%- elif custom_schema_name is none -%} {{ default_schema | trim }}
    {%- elif custom_schema_name[:13] ==  "DBT_CLOUD_PR_" -%} {{ default_schema | trim }}
    {%- elif target.name ==  "PROD_CI" -%} {{ default_schema | trim }}
    {%- elif custom_schema_name is not none -%} {{ custom_schema_name | trim }}
    {%- else -%} {{ default_schema | trim }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
 
 