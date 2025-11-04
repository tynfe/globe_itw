{% macro diagnose_match_failure(score_column, name_similarity_column, distance_column) %}
    CASE
        WHEN {{ score_column }} >= 80 AND {{ score_column }} < 81 THEN 'SCORE_JUST_BELOW_THRESHOLD'
        WHEN {{ name_similarity_column }} >= 80 AND {{ distance_column }} >= 150 THEN 'GOOD_NAME_BUT_TOO_FAR'
        WHEN {{ name_similarity_column }} < 80 AND {{ distance_column }} < 150 THEN 'CLOSE_BUT_DIFFERENT_NAME'
        WHEN {{ distance_column }} >= 5000 THEN 'NO_CANDIDATES_IN_RADIUS'
        WHEN {{ name_similarity_column }} < 60 THEN 'NAME_VERY_DIFFERENT'
        ELSE 'BOTH_NAME_AND_DISTANCE_INSUFFICIENT'
    END
{% endmacro %}
