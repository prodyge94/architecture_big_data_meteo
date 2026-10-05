# Prévision météo à court terme : architecture Big Data

Projet ESGI 5 « Architecture de projet Big Data » (sujet 4 : prévision à court terme).
Le pipeline prédit **Tmax, Tmin et la probabilité de pluie du lendemain (J+1)** pour les stations
synoptiques Météo-France de métropole, à partir des données ouvertes Infoclimat
([portail](https://portail.chom.engineering/), API REST sans clé).

Le cadrage détaillé est dans [PLAN.md](PLAN.md).

## Architecture

```
                     ┌──────────────┐  batch quotidien (Airflow)
 API Infoclimat ────►│  Ingestion   │─────────────────────────────┐
 (REST, JSON)        │  Python      │                             ▼
                     └──────┬───────┘                ┌────────────────────────┐
                            │ rejeu / live           │  DATA LAKE (MinIO, S3) │
                            ▼                        │  bronze : JSON brut    │
                     ┌──────────────┐                │  silver : Delta, QC    │
                     │    Kafka     │                │  gold   : features     │
                     └──────┬───────┘                └───────────┬────────────┘
                            ▼                                    │
                     ┌──────────────────────────────────────────▼────┐
                     │  Spark (master + worker)                       │
                     │  batch : bronze→silver→gold, entraînement GBT  │
                     │  streaming : Kafka → MERGE silver → prévision  │
                     └──────┬──────────────────────────────┬──────────┘
                            ▼                              ▼
                     ┌──────────────┐               ┌──────────────┐
                     │ PostgreSQL   │               │   MLflow     │
                     │ (serving)    │               │ suivi+registre│
                     └──────┬───────┘               └──────────────┘
                 ┌──────────┴──────────┐
                 ▼                     ▼
          API FastAPI :8000     Dashboard Streamlit :8501
```

| Couche | Techno | Rôle |
|---|---|---|
| Ingestion | Python (`requests`, `boto3`) | appels API paginés (`suite.from`), retry/backoff, écriture bronze |
| Messagerie | Kafka (KRaft) | flux d'observations, découplage producteur/consommateur |
| Stockage | MinIO + **Delta Lake** | lake S3 en couches bronze / silver / gold, partitionné par année |
| Traitement | **Spark 3.5** (cluster standalone) | ETL batch, features, Structured Streaming |
| ML | Spark MLlib (GBT) + **MLflow** | entraînement, métriques vs baselines, registre de modèles (alias `production`) |
| Orchestration | **Airflow** (DockerOperator) | DAGs backfill / quotidien / réentraînement hebdomadaire |
| Serving | PostgreSQL, FastAPI, Streamlit | prévisions servies et suivi de l'erreur en production |

### Couches de données

| Couche | Chemin | Contenu |
|---|---|---|
| bronze | `s3://lake/bronze/infoclimat/v2_journaliere/ingest_date=…/station=…/*.json` | réponse brute de l'API (immuable, rejouable) |
| bronze | `s3://lake/bronze/infoclimat/ref_stations/…` | référentiel des stations (GeoJSON) |
| silver | `s3://lake/silver/observations_daily` (Delta, partition `annee`) | typé, contrôle qualité, dédoublonné |
| silver | `s3://lake/silver/stations` | stations suivies |
| gold | `s3://lake/gold/features_daily` (Delta, partition `annee`) | 30 features + cibles J+1 |
| gold | `s3://lake/gold/climatologie`, `gold/station_voisins` | normales lissées, k plus proches voisines |
| modèles | `s3://lake/models/<modèle>/<run_id>` | pipelines Spark ML, référencés par le registre MLflow |

### Modèle

- **Features** (jour D) : Tn/Tx/pluie, retards J-1 à J-3, moyennes sur 7 jours, cumul de pluie sur 3 jours, tendances,
  climatologie du jour et du lendemain, anomalie à la normale, moyenne des 3 stations voisines,
  saisonnalité (sin/cos), position et altitude.
- **Cibles** (jour D+1) : Tmax, Tmin (régression GBT), pluie ≥ 1 mm (classification GBT).
- **Validation temporelle** : train ≤ 2022 < validation 2023 < test ≥ 2024, sans split aléatoire.
- **Baselines** : persistance (demain = aujourd'hui) et climatologie. Le skill score vs persistance est loggé dans MLflow.

Résultats sur le jeu de test (40 stations, historique depuis 2005, test du 01/2024 au 09/2026, GBT 20 itérations) :

| | MAE modèle | MAE persistance | MAE climatologie |
|---|---|---|---|
| Tmax J+1 | **1,93 °C** | 2,06 °C | 2,93 °C |
| Tmin J+1 | **1,66 °C** | 1,90 °C | 2,57 °C |
| Pluie J+1 (Brier) | **0,196** | 0,313 | 0,226 |

## Démarrage

Prérequis : Docker Desktop avec **8 Go de RAM** alloués.

```bash
cp .env.example .env          # optionnel : ajuster MAX_STATIONS, HISTORY_START...
docker compose up -d --build
```

| Service | URL |
|---|---|
| Airflow | http://localhost:8080 (admin / admin) |
| Spark master | http://localhost:8081 |
| MLflow | http://localhost:5000 |
| MinIO (console) | http://localhost:9001 (minioadmin / minioadmin) |
| API | http://localhost:8000/docs |
| Dashboard | http://localhost:8501 |

### 1. Initialiser (historique + modèles)

Dans Airflow, activer puis lancer le DAG **`meteo_backfill`**, ou bien lancer les étapes à la main :

```bash
docker compose run --rm jobs python -m meteo.jobs.ingest_ref
docker compose run --rm jobs python -m meteo.jobs.ingest_daily --full
docker compose run --rm jobs python -m meteo.jobs.bronze_to_silver
docker compose run --rm jobs python -m meteo.jobs.silver_to_gold
docker compose run --rm jobs python -m meteo.jobs.train
docker compose run --rm jobs python -m meteo.jobs.predict_batch --jours 90
```

Durées mesurées pour 40 stations depuis 2005 (310 000 lignes) : ingestion 30 s, bronze→silver 1 min,
silver→gold 1 min, entraînement 2 min (20 itérations). Avec les ~220 stations depuis 1990, compter environ 5 fois plus.

### 2. Quotidien

DAG **`meteo_quotidien`** (09:00 UTC, après la publication de J-1 par l'API) : ingestion des 10 derniers jours,
puis silver, gold et prévision J+1. DAG **`meteo_entrainement`** : réentraînement le lundi.

### 3. Streaming (démo temps réel)

L'API ne publie qu'une fois par jour. Un producteur **rejoue** donc l'historique du bronze dans Kafka, un jour simulé
toutes les N secondes, et Spark Structured Streaming produit les prévisions au fil de l'eau :

```bash
REPLAY_START=2026-07-01 REPLAY_END=2026-08-01 REPLAY_SECONDS_PER_DAY=5 \
  docker compose --profile streaming up -d
docker compose logs -f streaming
```

Chaque micro-batch : nettoyage (mêmes règles que le batch), `MERGE` Delta dans silver (idempotent), recalcul des
features des jours reçus, prévision avec les modèles `production` et publication dans PostgreSQL (`source = streaming`).

Partage du cluster : chaque application Spark est plafonnée (`spark.cores.max`). Le streaming, qui tourne en continu,
garde 1 cœur et 512 Mo, et les jobs batch lancés par Airflow disposent des 3 cœurs restants. Sans ce plafond, le
streaming prend tout le worker et les jobs batch attendent indéfiniment.

## Structure du dépôt

```
├── docker-compose.yml        # toute la stack
├── docker/                   # images : spark (jobs + cluster), mlflow, init PostgreSQL
├── dags/                     # DAGs Airflow (DockerOperator -> image meteo-spark)
├── src/meteo/
│   ├── api_client.py         # client API (pagination, retry, Retry-After)
│   ├── stations.py           # sélection des stations, voisins (haversine)
│   ├── transform.py          # QC, dédoublonnage, climatologie, features (Spark)
│   ├── models.py             # pipelines GBT, registre MLflow, inférence
│   ├── db.py                 # schéma et upserts PostgreSQL
│   └── jobs/                 # points d'entrée : ingest_*, bronze_to_silver, silver_to_gold,
│                             #   train, predict_batch, replay_producer, streaming_forecast
├── serving/api               # FastAPI
├── serving/dashboard         # Streamlit
└── tests/                    # pytest (client API, stations, transformations Spark)
```

Tests :

```bash
docker compose run --rm jobs python -m pytest -q tests
```

## Limites et suites

- **Pas de données horaires en accès libre.** La prévision à +6 h (exemple du sujet) demande l'Archive API
  (mesures infra-horaires), délivrée sur token. L'architecture est prête : nouvel ingesteur vers un bronze
  `archive_series`, mêmes couches, features au pas horaire.
- Le streaming rejoue l'historique, faute de flux temps réel public.
- Les fichiers bronze quotidiens s'accumulent (petits fichiers) : prévoir une compaction périodique.

## Données et licence

Données Météo-France (Licence Ouverte Etalab 2.0) et Infoclimat (politique opendata Infoclimat), via
https://portail.chom.engineering/. Attribution : « Météo-France / Infoclimat ».
