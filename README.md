Welcome to your new dbt project!


![Architecture du pipeline](diagram.svg)


# Cataloguing / Linéage / Quality / Monitoring 

voici le lien  https://tynfe.github.io/globe_itw/ des pages de documentation de l'ETL ainsi que le suivi des metrics de tests pour chaque env 
staging ou production qui host: 
- lien de la documentation du projet DBT 
- lien de la page de test / test de qualité / freshness / anomalies 


# Globe ITW 
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


## 🔧 Macros & utilitaires
````
Macro	Description
calculate_match_score()	Calcule le score composite de matching
diagnose_match_failure()	Analyse la raison d’un échec de matching
suggest_match_action()	Suggère une action corrective
drop_dev_schemas()	Nettoyage automatique des schémas de développement
capture_transformation_log_metadata()	Post-hook pour journaliser les transformations
````

# Workflow Git + DBT 

Une fois qu’un développeur a validé sa pipeline en environnement de développement, il peut pousser ses changements sur la branche staging.

Afin d’éviter que plusieurs contributeurs n’interfèrent entre eux sur cet environnement partagé, chaque développement est d’abord validé dans une pipeline annexe, exécutée en référence à l’état actuel de staging.
Cette étape permet de s’assurer que les nouvelles modifications restent compatibles avec la base stable avant intégration.

```
Feature Branch (dev_tyron_ferreira_feature_add_gi_source)
    ↓ develop + test isolated
    ↓ defer to staging (on build que le model qu'on change par rapport a staging pour eviter de tuer le runner de la ci) 
    ↓ 
Staging Branch
    ↓ validate + full build
    ↓ staging artifacts saved
    ↓
Tag v1.x.x
    ↓ deploy to production manuelle declencé par une relase + doc 
    ↓ cleanup dev schemas
    ↓
Production

Dans Snowflake ça donne :
DHW_DEV_TYRON
├── ETL                                        ← Staging (référence stable)
├── dev_tyron_ferreira_feature_add_new_source  ← TON schema isolé
├── dev_ilyas_fix_matching_score               ← Schema de Ilyas
└── dev_ikram_geohash                          ← Schema de Ikram
```

Une fois la validation effectuée, le développeur ouvre une Merge Request (MR) vers production.
Cette MR est ensuite relue et approuvée par un ou deux reviewers pour garantir la qualité et la conformité des changements :
https://github.com/tynfe/globe_itw/pulls?q=is%3Apr+is%3Aclosed

Après validation et fusion sur production, une release est créée.
Cette release génère automatiquement un tag versionné, qui déclenche le workflow CI/CD de production.
Ce tag correspond à la version officiellement déployée en production 
https://github.com/tynfe/globe_itw/actions/runs/19072632369/job/54479368129


un **dashboard** `magasin_analyses` est aussi disponible dans l'onget dashboard du service account pour pouvoir faire une étude ad-hoc des matchings 

# ROADMAP 
### V0 

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

### Définir une approche de priorisation entre nouvelles sources, maintenance, dette technique et exigences réglementaires,


**1: Produits =>**
regarder le document Roadmap.png 

Axe prioritaire, il répond directement aux besoins clients ou aux retours produits. Ces demandes sont donc traitées en top priorité.
Plus le volume de demandes sur cet axe est important, plus la bande passante disponible pour l’axe 3 (Foundation) diminue.
Cet axe inclut également les améliorations continues et la maintenance du produit (par exemple : la source d’ingestion ou les algorithmes de matching).

**2: Data driven décision** 

Cet axe vise à améliorer nos produits et processus en s’appuyant sur les analyses issues des données que nous générons.
Il regroupe la définition des KPI de performance, les études analytiques et les outils de pilotage destinés à démocratiser la culture data au sein de l’entreprise et à évangéliser les autres équipes à une approche orientée données.

**3: foundation** 

Il s’agit de l’axe infrastructure et socle technique, garant de la scalabilité, de la qualité et de la fiabilité du Data Platform.
Le schéma d’architecture actuel s’appuie sur des modules non encore disponibles dans la version 0 (Airflow, RBAC, DDL Terraformé, etc.).
Cet axe, bien que secondaire dans la priorisation, s’adapte aux besoins issus des deux autres axes.
Il englobe l’ensemble des sujets infra/scaling/alerting/monitoring/qualité, souvent structurés sous forme d’epics, et constitue la base de notre future Data Platform.

### Expliquer comment prioriser les développements et déploiements dans un contexte de forte demande métier,

regarder le documetn epic.png  

[TEAM OBJECTIF] => indicateur de réussite pour valider => [ LIST d'EPICs ]  
[EPIC ] => 1 famille de task   
[ TASK ] => unité la plus petite représentant une tache     


# Lien utile 

la solution RBAC création de droit et set up des DDL (numéro 1)
https://medium.com/snowflake/snowflake-ci-cd-explained-automating-object-creation-with-terraform-dbt-and-github-8c2e38b70ec6  
a trigger via https://cli.github.com/manual/gh_workflow_run  

https://github.com/Infostrux-Solutions/terraform-snowflake-rbac-infra  
https://github.com/Infostrux-Solutions/terraform-snowflake-rbac  
https://github.com/Infostrux-Solutions/terraform-snowflake-database  
https://github.com/Infostrux-Solutions/terraform-snowflake-rbac-infra  
https://github.com/Infostrux-Solutions/terraform-snowflake-warehouse  

Data quality 
https://medium.com/@sdezoysa/tackling-data-quality-challenges-using-data-metric-functions-in-snowflake-a62593effbc6  
https://xebia.com/blog/monitoring-dbt-model-and-test-executions-using-elementary-data/  

CI/CD 
https://nolanbconaway.github.io/blog/2023/my-dbt-continuous-integration-setup.html pour la partie dev to staging  
https://medium.com/@lucasrbarbosa/snowflake-data-platform-episode-ii-deep-dive-dbt-projects-with-github-actions-on-snowflake-615126a6fc35 pour l'env staging / prod  

airflow / cosmos & DBT pour le schema de la roadmap 
https://www.snowflake.com/en/developers/guides/data-engineering-with-apache-airflow/#creating-a-dag-with-cosmos-and-snowpark  
https://github.com/astronomer/astronomer-cosmos  
