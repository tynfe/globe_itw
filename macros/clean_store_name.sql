{% macro clean_store_name(column_name) %}
    UPPER(
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        {{ column_name }},
                        '[^A-Za-z0-9 ]', ''  -- Supprime caractères spéciaux
                    ),
                    '\\s+', ' '  -- Normalise espaces multiples
                ),
                '\\b(SAS|SARL|SA|EURL|SNC|SASU|EI|EARL|GIE|SCI|SCP|SEL|SELARL|LE|LA|LES|DU|DE|DES|AU|AUX|CHEZ)\\b', ''  -- Supprime mots communs
            )
        )
    )
{% endmacro %}