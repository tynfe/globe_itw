{% macro calculate_match_score(name_hash_1, name_hash_2, name_similarity, distance_meters) %}
    CASE
        WHEN {{ name_hash_1 }} = {{ name_hash_2 }} AND {{ distance_meters }} < 50 THEN 100
        WHEN {{ name_similarity }} > 90 AND {{ distance_meters }} < 100 THEN 95
        WHEN {{ name_similarity }} > 80 AND {{ distance_meters }} < 150 THEN 85
        ELSE {{ name_similarity }} * 0.6
    END
{% endmacro %}