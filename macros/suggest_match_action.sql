{% macro suggest_match_action(score_column, name_similarity_column, distance_column) %}
    CASE
        WHEN {{ score_column }} >= 75 THEN 'MANUAL_REVIEW_RECOMMENDED'
        WHEN {{ name_similarity_column }} >= 70 AND {{ distance_column }} < 200 THEN 'CHECK_NAME_VARIATIONS'
        WHEN {{ distance_column }} > 500 THEN 'VERIFY_COORDINATES'
        WHEN {{ name_similarity_column }} < 50 THEN 'LIKELY_DIFFERENT_STORE'
        ELSE 'NEEDS_DATA_QUALITY_IMPROVEMENT'
    END
{% endmacro %}