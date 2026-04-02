# TP Ingénierie des Données - Pipeline ETL RNE

Bienvenue dans ce travail pratique sur l'**ingénierie des données** ! Ce projet vous permettra de construire un pipeline ETL (Extract, Transform, Load) complet utilisant Apache Spark, Airflow et PostgreSQL.

##  Vue d'ensemble

Ce TP traite les données du **Répertoire National des Élus (RNE)** français. Vous allez:

1. **Extraire** 12 tables sources de PostgreSQL vers HDFS
2. **Transformer** et unifier les données en une seule table
3. **Agréger** les données pour créer des vues analytiques
4. **Orchestrer** le tout avec Apache Airflow

Les données couvrent les élus à différents niveaux (maires, députés, conseillers, etc.).

##  Architecture du Pipeline

```
PostgreSQL (12 tables) 
    ↓ [Job Extract]
HDFS (Raw Data)
    ↓ [Job Transform]
HDFS (Unified Table)
    ↓ [Job Aggregate]
PostgreSQL (4 tables agrégées)
```

### Pipeline Airflow
```
check_source → extract_job → transform_job → aggregate_job → validate_output
```

##  Structure du projet

```
TP_ref/
├── dags/                          # DAGs Airflow
│   └── rne_pipeline.py           # Pipeline principal ETL
├── spark_jobs/                    # Jobs Spark
│   ├── config.py                 # Configuration (chemins, URLs, tables)
│   ├── job_extract.py            # Extraction PostgreSQL → HDFS
│   ├── job_transform.py          # Transformation des données
│   └── job_aggregate.py          # Agrégation et calculs
├── infra/                         # Infrastructure Docker
│   ├── docker-compose.yml        # Orchestration des services
│   ├── Dockerfile.airflow        # Image Airflow personnalisée
│   ├── Dockerfile.jupyter        # Image Jupyter
│   ├── init-postgres.sql         # Script d'initialisation DB
│   ├── migrate_csv_to_postgres.py # Migration CSV → PostgreSQL
│   └── notebooks/                # Notebooks Jupyter pour exploration
├── rep_national_elus/            # Données brutes (CSV)
│   └── elus-*.csv                # 12 fichiers CSV par type d'élus
├── enonce_tp/                    # Énoncé du TP
│   └── tp_etudiant.pdf          # Sujet détaillé
└── requirements.txt              # Dépendances Python
```

##  Prérequis

- **Python 3.10+**
- **Docker & Docker Compose**
- **Java 11+** (pour Spark)
- **~6 GB d'espace disque**

## Installation et démarrage

### 1. Cloner et installer les dépendances

```bash
cd /Users/mouaad/GoldenCollar/BAC4/TP_ref
pip install -r requirements.txt
```

### 2. Démarrer les services Docker

```bash
cd infra
docker-compose up -d
```

Cela démarre:
- **PostgreSQL** (port 5432)
- **Apache Airflow** (port 8080)
- **Jupyter Notebook** (port 8888)
- **Hadoop/HDFS** (port 50070)

### 3. Vérifier les services

```bash
docker-compose ps
```

### 4. Charger les données initiales

Une fois PostgreSQL prêt, charger les données CSV:

```bash
python infra/migrate_csv_to_postgres.py
```

Cela popule les 12 tables sources dans PostgreSQL.

### 5. Accéder à Airflow

1. Ouvrez http://localhost:8080
2. Identifiants par défaut: `airflow` / `airflow`
3. Vous verrez le DAG `ETL-DAG-RNE`

##  Exécution du Pipeline

### Via l'interface Airflow

1. Allez sur le DAG `ETL-DAG-RNE`
2. Cliquez sur **"Trigger DAG"**
3. Suivez l'exécution en temps réel

### Via la ligne de commande

```bash
docker exec airflow_container airflow dags trigger ETL-DAG-RNE
```

##  Étapes du pipeline

###  **check_source** (PythonOperator)
Vérifie que les 12 tables sources existent et ne sont pas vides dans PostgreSQL.

**Tables attendues:**
- `rne.elus_deputes_dep`
- `rne.elus_senateurs_sen`
- `rne.elus_maires_mai`
- `rne.elus_conseillers_municipaux_cm`
- `rne.elus_conseillers_departementaux_cd`
- `rne.elus_conseillers_regionaux_cr`
- `rne.elus_conseillers_communautaires_epci`
- `rne.elus_conseillers_darrondissements_ca`
- `rne.elus_representants_parlement_europeen_rpe`
- `rne.elus_membres_dune_assemblee_ma`
- `rne.elus_assemblee_des_francais_de_letranger_afe`
- `rne.elus_conseillers_des_francais_de_letranger_cons`

###  **job_extract** (SparkSubmitOperator)
Exporte les 12 tables de PostgreSQL vers HDFS en parallèle.

**Sortie:** Fichiers Parquet dans `/data/raw/`

###  **job_transform** (SparkSubmitOperator)
- Unifie les 12 tables en une seule `elus_unified`
- Normalise les colonnes communes
- Ajoute la colonne `type_elu` pour identifier le niveau

**Sortie:** Parquet `/data/refined/elus_unified/`

###  **job_aggregate** (SparkSubmitOperator)
Crée 4 tables agrégées:
- `nb_elus_par_region` - Nombre d'élus par région
- `nb_elus_par_departement` - Nombre d'élus par département
- `gender_distribution` - Répartition par genre
- `top_communes` - Top 100 communes avec plus d'élus

**Sortie:** Tables PostgreSQL dans le schéma `rne`

###  **validate_output** (PythonOperator)
Vérifie que les 4 tables agrégées ont bien été créées et contiennent des données.

##  Configuration

Éditez `spark_jobs/config.py` pour modifier:

```python
# PostgreSQL
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_USER = "postgres"
POSTGRES_PASS = "password"
POSTGRES_DB = "rne_db"

# HDFS
HDFS_NAMENODE = "hdfs://namenode:9000"
HDFS_RAW = "/data/raw"
HDFS_REFINED = "/data/refined"
HDFS_GOLD = "/data/gold"

# Spark
SPARK_MASTER = "spark://spark:7077"
```

## Fichiers importants

| Fichier | Description |
|---------|-------------|
| `dags/rne_pipeline.py` | DAG principal - dépendances des tâches |
| `spark_jobs/config.py` | Configuration centralisée |
| `spark_jobs/job_extract.py` | Logique d'extraction |
| `spark_jobs/job_transform.py` | Logique de transformation |
| `spark_jobs/job_aggregate.py` | Logique d'agrégation |
| `infra/init-postgres.sql` | Schéma PostgreSQL |
| `infra/docker-compose.yml` | Services Docker |

##  Dépannage

### Les services ne démarrent pas
```bash
docker-compose logs  # Voir les logs
docker-compose down  # Réinitialiser
docker-compose up --build  # Reconstruire les images
```

### PostgreSQL dit "connection refused"
Attendez 30 secondes que PostgreSQL soit prêt:
```bash
docker-compose ps  # Vérifier que le service est "Up"
```

### Airflow ne trouve pas les DAGs
```bash
docker-compose restart airflow  # Redémarrer Airflow
```

### Les jobs Spark échouent
- Vérifiez les logs dans l'UI Airflow
- Vérifiez l'espace disque: `df -h`
- Vérifiez les permissions HDFS

### Je ne vois pas les données après l'extraction
```sql
SELECT * FROM rne.elus_deputes_dep LIMIT 5;
```
Si vide → relancer `migrate_csv_to_postgres.py`

## Documentation supplémentaire

Voir `enonce_tp/tp_etudiant.pdf` pour:
- Énoncé détaillé des exercices
- Questions à répondre
- Critères d'évaluation
- Cas d'usage et requêtes SQL

##  Conseils utiles

1. **Examinez les données**: Utilisez Jupyter pour explorer les données avant de les traiter
2. **Commencez petit**: Testez vos transformations sur un sous-ensemble avant de traiter tout
3. **Logs**: Consultez les logs Airflow pour déboguer les jobs
4. **Schéma**: Imprimez le schéma des DataFrames pour comprendre la structure
5. **Performance**: Utilisez `explain()` pour analyser les plans d'exécution Spark

##  Objectifs du TP

À la fin de ce TP, vous devriez savoir:

* Construire un pipeline ETL complet avec Spark  
* Orchestrer des jobs avec Airflow  
* Intégrer PostgreSQL et HDFS  
* Transformer et agréger des données à grande échelle  
* Monitorer et déboguer des pipelines en production  

## Support

En cas de problème:
1. Consultez les logs: `docker-compose logs <service>`
2. Vérifiez la configuration dans `config.py`
3. Relancez le service concerné
4. Demandez à l'instructeur

---

**Bon TP ! **
