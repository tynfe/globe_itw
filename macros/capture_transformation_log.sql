{% macro capture_transformation_log_metadata() %}
    {% if execute %}
        {% set query %}
            INSERT INTO {{ target.database }}.{{ target.schema }}.TRANSFORMATION_LOGS (
                model_name,
                schema_name,
                database_name,
                run_started_at,
                run_completed_at,
                execution_duration_seconds,
                rows_affected,
                target_name,
                invocation_id,
                status,
                error_message,
                run_by,
                created_at
            )
            SELECT
                '{{ this.name }}',
                '{{ this.schema }}',
                '{{ this.database }}',
                '{{ run_started_at }}'::TIMESTAMP_NTZ,
                CURRENT_TIMESTAMP(),
                TIMESTAMPDIFF(SECOND, '{{ run_started_at }}'::TIMESTAMP_NTZ, CURRENT_TIMESTAMP()),
                (SELECT COUNT(*) FROM {{ this }}),
                '{{ target.name }}',
                '{{ invocation_id }}',
                'SUCCESS',
                'NULL',
                CURRENT_USER(),
                CURRENT_TIMESTAMP()
            ;
        {% endset %}

        {% do run_query(query) %}
        {% do log("✅ Metadata captured for " ~ this.name, info=true) %}
    {% endif %}
{% endmacro %}