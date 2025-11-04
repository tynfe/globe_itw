Welcome to your new dbt project!


![Architecture du pipeline](diagram.svg)


# 🏪 Globe ITW – Store Matching & Master Data Management
## Vue d’ensemble

Globe ITW est un système de réconciliation et d’unification de référentiels magasins multi-sources.
Il vise à construire un référentiel unique et fiable à partir de deux bases hétérogènes :

GI magasins : source de référence principale
TH magasins : source complémentaire

Le pipeline DBT met en œuvre un algorithme de matching hybride combinant :  
Similarité textuelle (Jaro–Winkler sur noms nettoyés)  
Proximité géographique (distance de Haversine)  
Indexation spatiale via geohash pour accélérer la recherche de candidats  

# Architecture DBT – Modèle Medallion

## Bronze Layer – Raw Models (models/raw/)

Rôle : ingestion et préparation initiale des données brutes.

Sources traitées :
```
raw__gi_stores
raw__th_stores
```

Transformations appliquées :

Standardisation des types de données  
Normalisation des noms (minuscules, suppression des accents et caractères spéciaux)  
Génération d’un hash MD5 pour le matching exact  
Calcul de geohash multi-précision (150 m / 1200 m)  
Ajout d’un UUID unique (generated_id)  
Validation des coordonnées GPS (latitude ∈ [-90, 90], longitude ∈ [-180, 180]) 


## Staging Layer – Models (models/staging/)

Rôle : moteur de matching, historisation et gestion des états.

Modèle principal : stg__stores_unified  
Type : table incrémentale

### Phase 1 – Matching automatique

Sélection des candidats : magasins partageant le même geohash_1200m 
https://fr.wikipedia.org/wiki/Geohash


```
Calcul du score composite :
+40 : correspondance exacte du hash de nom
+40 : similarité Jaro–Winkler > 0.8
+20 : distance < 150 m
```

Matching validé si score > 80

Déduplication : conservation du meilleur score par magasin GI

### Phase 2 – Classification des magasins

```
Statut	Description
MATCHED	Score > 80, correspondance trouvée
GI_ONLY	Magasin GI sans correspondance TH
TH_ONLY	Magasin TH sans correspondance GI
```

### Phase 3 – Enrichissement et qualité

Flag qualité :
````
Flag	Description
PERFECT_MATCH	Score = 100 (hash + distance parfaits)
HIGH_CONFIDENCE	Score ≥ 95
MEDIUM_CONFIDENCE	Score ≥ 80
MISSING_IN_TH	Magasin GI sans correspondance
MISSING_IN_GI	Magasin TH sans correspondance
````

Détection des changements :
````
Type	Description
INSERT	Nouveau magasin
SCORE_CHANGED	Variation du score
STATUS_CHANGED	Changement de statut MATCHED ↔ UNMATCHED
NAME_CHANGED	Nom modifié
LOCATION_CHANGED	Déplacement GPS
NO_CHANGE	Aucune modification
````


## Gold Layer – Models (models/mart/)

Référentiel validé des magasins appariés avec certitude.

mart__view__matched_stores

````
SELECT store_id, store_name, latitude, longitude
FROM stg__stores_unified
WHERE record_status = 'MATCHED'
````

Référentiel des magasins qui n'ont pas de match, vue analytique des magasins non appariés.

mart__view__unmatched_stores

Contenu :  
Top 3 des candidats les plus proches (< 5 km)  
Détails des scores (nom, distance)   
Diagnostic automatique de l’échec  
Suggestion d’action manuelle ou ajustement du seuil


# 🔧 Macros & utilitaires
````
Macro	Description
calculate_match_score()	Calcule le score composite de matching
diagnose_match_failure()	Analyse la raison d’un échec de matching
suggest_match_action()	Suggère une action corrective
drop_dev_schemas()	Nettoyage automatique des schémas de développement
capture_transformation_log_metadata()	Post-hook pour journaliser les transformations
````

voici le lien  https://tynfe.github.io/globe_itw/ de la page github qui host : 
- lien de la documentation du projet DBT 
- lien de la page de test / test de qualité / freshness / anomalies 

# ROADMAP 
## V0

1. Combiner les deux sources pour construire une dimension magasin unique et historisée dans le DWH. => **DONE** 
2. Mettre en place un workflow DataOps assurant :
a. Le versionnement et la traçabilité du DWH, => **DONE** 
b. Le déploiement automatisé et sécurisé depuis DEV_DWH vers PROD_DWH, => **DONE**
c. La gestion des migrations et le contrôle manuel des déploiements en production. => **DONE**
3. Garantir une gouvernance et une sécurité robustes (RBAC) : => **NOT_DONE_BUT_DOCUMENTATION_FOUND**
a. Définir une gestion claire des rôles et droits d’accès internes (ex. Data Engineer, Analyst, Product
Owner)


4. Mettre en avant la qualité, les tests et l’observabilité : **=> DONE**
a. Inclure des tests (not_null, unique, relations, custom), **=> DONE**
b. Définir des indicateurs de qualité de données (completude, fraîcheur, cohérence), **=> DONE**

## v1 


```` 

![Road Map](https://raw.githubusercontent.com/tynfe/globe_itw/main/image/roadmap.png)







