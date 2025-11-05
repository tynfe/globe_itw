🏗️ Architecture Flow 

📋 Vue d'ensemble  
Ce document décrit le flow complet de notre pipeline de matching entre deux sources de données magasins (GI et TH), avec un système d'auto-healing pour corriger les incohérences détectées.

🔄 Flow détaillé - Step by Step
Step 1️⃣ : User DBT - Initialisation du projet  
Qui : Data Engineer Quoi : Le Data Engineer initialise le projet dbt en clonant le repository et en configurant son environnement de travail local.
Il vérifie que la connexion à Snowflake fonctionne correctement.  
Pourquoi : C'est le point de départ pour pouvoir développer les modèles de matching. Sans cette étape, impossible de travailler sur les transformations.  
Résultat : Environnement dbt opérationnel, le DE peut commencer à coder ses modèles.         

Step 2️⃣ : Création de DDL avec Terraform et GitHub action   
Qui : Platform (Airflow operator trigger action github)   
Quoi : Création automatisée de toutes les tables nécessaires dans Snowflake via Terraform. Cela inclut les tables de données (RAW, STAGING, GOLD) mais aussi les tables techniques (reprocess_triggers, transformation_logs).     
Pourquoi : L'infrastructure doit exister avant de pouvoir y écrire des données. Terraform assure que tout est versionné et reproductible entre les environnements.    
Résultat : Toutes les tables sont créées dans Snowflake avec le bon format (notamment Iceberg pour la gestion de versions).  

Step 3️⃣ : Projet DBT dans Snowflake
Qui : Data Engineer  
Quoi : Développement et organisation des modèles dbt selon une architecture en couches : RAW (sources brutes), STAGING (transformations et matching), GOLD (données finales enrichies), et DIAGNOSTICS (vues d'analyse).  
Pourquoi : Cette organisation permet de séparer clairement les responsabilités : les sources brutes, la logique de matching, les données consommables, et les outils de surveillance.  
Résultat : Structure du projet dbt claire et maintenable, avec tous les modèles nécessaires au matching des stores.  

Step 4️⃣ : RBAC via Terraform + GitHub Actions  
Qui : Platform Quoi : Configuration des rôles et permissions dans Snowflake pour définir qui peut lire/écrire dans quelles tables. Le déploiement est automatisé via GitHub Actions à chaque changement.  
Pourquoi : Sécurité et gouvernance. On veut s'assurer que dbt ne peut écrire que dans STAGING/GOLD, que les utilisateurs métier ne peuvent que lire GOLD, etc.  
Résultat : Matrice de permissions configurée et automatiquement déployée. Chaque service/utilisateur a exactement les droits nécessaires.  

Step 5️⃣ : Publish ID version Iceberg
Qui : Système automatisé (CI/CD)  
Quoi : Après chaque exécution réussie de dbt, Iceberg génère automatiquement un snapshot unique (identifiant de version). Ce snapshot est enregistré dans une table de registry pour tracer quelle version est active en production.  
Pourquoi : Permet de savoir exactement quelle version des données est en production, de revenir en arrière en cas de problème (rollback), et d'auditer l'historique des changements.  
Résultat : Chaque version des données est identifiée de manière unique et traçable. On peut pointer vers une version spécifique à tout moment.

Step 6️⃣ : Mise à jour des views du dossier MART avec le bon commit  
Qui : CI/CD Pipeline  
Quoi : Les vues métier (dans le dossier MART) sont automatiquement mises à jour pour pointer vers la dernière version validée publiée à l'étape 5. Ces vues sont consommées par les dashboards et outils analytiques.  
Pourquoi : Les utilisateurs finaux (analystes, dashboards) ne doivent voir que des données validées et stables. Ils ne doivent jamais pointer directement vers les tables en cours de transformation.  
Résultat : Les dashboards et rapports consomment toujours une version stable et validée des données, sans être impactés par les reprocessing en cours.  

Step 7️⃣ : Process des UNMATCHED_STORES (activé via Airflow )   
Qui : Job Airflow planifié (ex: chaque nuit)  
Quoi : Un job automatique analyse la vue diagnostic qui contient tous les stores non matchés (GI_ONLY et TH_ONLY) avec leurs candidats potentiels. Pour chaque store ayant un score potentiel intéressant (>70), le job crée un "trigger" de reprocessing.  
Pourquoi : Détection automatique des problèmes de matching. Au lieu d'attendre qu'un humain regarde manuellement, le système identifie proactivement les cas qui pourraient être améliorés.  
Résultat : Table "reprocess_triggers" alimentée automatiquement avec les IDs des stores à retraiter, priorisés selon leur score potentiel.     

Step 7️⃣ **approche** **2** : Process des UNMATCHED_STORES (activé via Airflow) - Version Full Refresh  
Qui : Job Airflow planifié (ex: chaque nuit)    
Résultat : Soit un flag "full_refresh_needed" est activé et le prochain run dbt reconstruit toute la table depuis zéro avec la logique actuelle, soit le job déclenche immédiatement un `dbt run --full-refresh` si la situation est critique.   
Toutes les incohérences sont corrigées d'un coup, au prix d'un temps de calcul plus long (mais exécuté de nuit donc transparent pour les utilisateurs).

Step 8️⃣ : User (Analyst) analyse via Streamlit  
Qui : Data Analyst / Data Steward  
Quoi : Un analyste utilise une application Streamlit pour visualiser et comprendre les patterns d'échec de matching. Il peut voir les distributions d'erreurs, identifier les causes principales (ex: "40% des échecs = noms mal normalisés"), et explorer les cas individuels.  
Pourquoi : Les métriques brutes ne suffisent pas. Un humain doit comprendre pourquoi ça ne matche pas pour identifier le bon fix. On deploié une app Streamlit par dessus snowlake qui facilite cette analyse exploratoire.  
Résultat : L'analyste identifie un pattern clair (ex: les formes juridiques "SAS", "SARL" perturbent le matching) et documente ce qu'il faut corriger.  

Step 9️⃣ : Nouveau commit avec fix sur matching score    
Qui : Data Engineer    
Quoi : Suite à l'analyse de l'étape 8, le DE développe un fix (ex: amélioration de la normalisation des noms pour retirer les formes juridiques). Il crée une Pull Request avec son changement, qui est testé automatiquement.  
Pourquoi : Amélioration continue du matching. Chaque pattern d'erreur détecté devient un fix codé et versionné dans Git.  
Résultat : Code amélioré mergé dans la branche principale, prêt à être déployé.    
 
Step 1️⃣0 : Projet DBT (re-triggered)  
Qui : CI/CD Pipeline (Workflow GitHub Actions managé par Airflow)   
Quoi : Le merge du code à l'étape 9 déclenche automatiquement une nouvelle exécution de dbt. Cette fois, au lieu de tout recalculer, dbt va lire la table "reprocess_triggers" créée à l'étape 7 et ne retraiter QUE les stores identifiés comme ayant un potentiel d'amélioration.
Pourquoi : Efficacité. Plutôt que de refaire tout le matching sur X pays (coût élevé, temps long), on cible uniquement les cas qui peuvent bénéficier du fix.
Résultat : Reprocessing ciblé en cours, limité au scope défini par les triggers.  

Step 1️⃣1️⃣ : Tables dans Snowflake (en cours de processing)
Qui : Snowflake (exécution des requêtes dbt)  
Quoi : Les trois couches de tables (RAW, STAGING, GOLD) travaillent ensemble. RAW contient toujours les sources brutes, STAGING reçoit les mises à jour incrémentales via le merge Iceberg (seuls les stores du scope sont recalculés), et GOLD est enrichi avec les nouveaux résultats. Le contexte Terraform observe les métriques.  
Pourquoi : Architecture en couches = séparation des responsabilités. Chaque couche a son rôle : source de vérité (RAW), logique métier (STAGING), données consommables (GOLD).  
Résultat : Les stores qui étaient "GI_ONLY" ou "TH_ONLY" sont maintenant correctement matchés grâce au fix. Un nouveau snapshot Iceberg est créé. 

Step 1️⃣2️⃣:Les triggers sont marqués comme "consommés" pour éviter de les retraiter.  



