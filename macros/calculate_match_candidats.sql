{% macro calculate_match_candidates(
    unmatched_alias,
    candidate_alias,
    unmatched_name_cleaned,
    candidate_name_cleaned,
    unmatched_lat,
    unmatched_lon,
    candidate_lat,
    candidate_lon,
    partition_by='store_id'
) %}

    -- Similarité du nom
    JAROWINKLER_SIMILARITY(
        {{ unmatched_alias }}.{{ unmatched_name_cleaned }},
        {{ candidate_alias }}.{{ candidate_name_cleaned }}
    ) as name_similarity_score,

    -- Distance géographique
    ST_DISTANCE(
        TO_GEOGRAPHY(CONCAT('POINT(', {{ unmatched_alias }}.{{ unmatched_lon }}, ' ', {{ unmatched_alias }}.{{ unmatched_lat }}, ')')),
        TO_GEOGRAPHY(CONCAT('POINT(', {{ candidate_alias }}.{{ candidate_lon }}, ' ', {{ candidate_alias }}.{{ candidate_lat }}, ')'))
    ) as distance_meters,

    {{ calculate_match_score(
        name_hash_1=unmatched_alias ~ '.' ~ unmatched_name_hash,
        name_hash_2=candidate_alias ~ '.' ~ candidate_name_hash,
        name_similarity='JAROWINKLER_SIMILARITY(' ~ unmatched_alias ~ '.' ~ unmatched_name_cleaned ~ ', ' ~ candidate_alias ~ '.' ~ candidate_name_cleaned ~ ')',
        distance_meters='ST_DISTANCE(TO_GEOGRAPHY(CONCAT(\'POINT(\', ' ~ unmatched_alias ~ '.' ~ unmatched_lon ~ ', \' \', ' ~ unmatched_alias ~ '.' ~ unmatched_lat ~ ', \')\')), TO_GEOGRAPHY(CONCAT(\'POINT(\', ' ~ candidate_alias ~ '.' ~ candidate_lon ~ ', \' \', ' ~ candidate_alias ~ '.' ~ candidate_lat ~ ', \')\')))'
    ) }} as potential_match_score,

    -- Rang du candidat
    ROW_NUMBER() OVER (
        PARTITION BY {{ unmatched_alias }}.{{ partition_by }}
        ORDER BY ST_DISTANCE(
            TO_GEOGRAPHY(CONCAT('POINT(', {{ unmatched_alias }}.{{ unmatched_lon }}, ' ', {{ unmatched_alias }}.{{ unmatched_lat }}, ')')),
            TO_GEOGRAPHY(CONCAT('POINT(', {{ candidate_alias }}.{{ candidate_lon }}, ' ', {{ candidate_alias }}.{{ candidate_lat }}, ')'))
        ) ASC
    ) as candidate_rank

{% endmacro %}