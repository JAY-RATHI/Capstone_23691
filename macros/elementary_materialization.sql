{% materialization test, adapter='snowflake' %}
 
    {{ return(elementary.materialization_test_snowflake()) }}
 
{% endmaterialization %}