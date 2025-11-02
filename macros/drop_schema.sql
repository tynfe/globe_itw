{% macro drop_schema(schema_name, database) %}
  {% set sql %}
    USE DATABASE {{ database }};
    DROP SCHEMA IF EXISTS {{ schema_name }} CASCADE;
  {% endset %}

  {% do log("Dropping schema: " ~ schema_name, info=True) %}
  {% do run_query(sql) %}
  {% do log("Schema dropped successfully", info=True) %}
{% endmacro %}