SELECT
    geohash_150m,
    COUNT(*) AS nb_magasins,
    LISTAGG(nom, ', ') AS liste_magasins
FROM {{ ref('stg_magasin') }}
GROUP BY geohash_1km
HAVING COUNT(*) > 5