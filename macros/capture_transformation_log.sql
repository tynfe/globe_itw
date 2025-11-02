{% macro capture_transformation_log_metadata() %}
    {% set query %}
        INSERT INTO {{ target.database }}.{{ target.schema }}.TRANSFORMATION_LOGS (
            log_id,
            model_name,
            schema_name,
            database_name,
            run_started_at,
            run_completed_at,
            rows_affected,
            status,
            materialization,
            target_name,
            invocation_id
        ) VALUES (
            UUID_STRING(),
            '{{ this.name }}',
            '{{ this.schema }}',
            '{{ this.database }}',
            '{{ run_started_at }}'::TIMESTAMP_NTZ,
            CURRENT_TIMESTAMP(),
            (SELECT COUNT(*) FROM {{ this }}),
            'SUCCESS',
            '{{ config.get("materialized") }}',
            '{{ target.name }}',
            '{{ invocation_id }}'
        );
    {% endset %}

    {% do run_query(query) %}
    {% do log("✅ Metadata captured for " ~ this.name, info=true) %}
{% endmacro %}