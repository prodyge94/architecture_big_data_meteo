# Projet Big Data : prévision météo à court terme

> Sujet 4 : prédire la météo des prochaines heures ou du lendemain à partir des données récentes.
> Source : API Infoclimat / Météo-France, https://portail.chom.engineering/ (doc machine : `/llms-full.txt`, `/openapi.json`)
> Cours : architecture d'un projet Big Data

---

## 1. Ce que les données permettent vraiment (vérifié le 05/10/2026)

| Accès | Granularité | Auth | Usage dans le projet |
|---|---|---|---|
| REST `climato.chom.engineering/v2/journaliere`, `/ic/journaliere`, `/statIC/journaliere` | **journalière** (tn, tx, tm, rr, n_obs) | aucune | **source principale** |
| REST `/ref/stations`, `/ref/station-parametre` | référentiel (lat, lon, altitude, paramètres mesurés) | aucune | features spatiales, choix des voisins |
| REST `/dataclimat/baseline_1991_2020` | normales journalières par station | aucune | baseline et feature « écart à la normale » |
| Live/Archive API `archive.chom.engineering/series` | **pas de mesure brut (infra-horaire)**, SI (Kelvin, Pa) | **token Bearer, sur demande** | prévision à +6 h (si on obtient le token) |
| Bulk Iceberg/Parquet (S3 OVH) | datasets Gold complets | credentials S3 | optionnel |

**Contraintes à connaître :**
- 20 000 lignes max par requête. Il faut paginer avec `suite.from` et **toujours tester `tronque`**.
- Pas de quota, mais l'API tourne sur une seule machine : on limite le parallélisme (2 à 4 requêtes en parallèle, avec retry et backoff, et on respecte `Retry-After` sur les 503).
- Fraîcheur : publication quotidienne (J-1 à partir de 08:00 UTC). Le 05/10/2026, `/v2/journaliere` allait jusqu'au **29/09/2026** (environ une semaine de latence). `/coverage` et le champ `derniere_activite` du référentiel affichaient le 12/08 : ce sont des indicateurs en retard, on ne s'y fie pas. Il n'y a pas de flux temps réel public : on **rejoue l'historique en streaming** (voir §3).
- `statut_extremes` vaut `partiel` ou `releve_unique` quand la journée est mal échantillonnée : il faut filtrer ces jours en silver.

**Décision de cadrage :**
- **V1 (sans dépendance externe)** : prévoir **J+1** (Tmin, Tmax et pluie oui/non) pour chaque station à partir des N derniers jours de la station et de ses voisines.
- **V2 (si token obtenu)** : prévoir la **température à +1 h, +3 h et +6 h** à partir des séries infra-horaires. C'est l'exemple exact du sujet. **Il faut demander le token dès cette semaine.**

---

## 2. Pourquoi c'est un projet Big Data (à défendre à l'oral)

- **Volume** : 39 Md de mesures dans le fonds source, des dizaines de milliers de stations dans le référentiel, et des séries qui remontent jusqu'à 1777. En journalier, même restreint à environ 1 000 stations actives sur 30 ans, cela fait déjà environ 11 M de lignes. En infra-horaire (V2), on passe à plusieurs centaines de millions.
- **Vélocité** : un flux continu d'observations à ingérer, puis une prédiction à produire dès qu'une nouvelle mesure arrive.
- **Variété** : JSON REST, plusieurs réseaux hétérogènes (Météo-France, SYNOP, StatIC participatif), des identifiants hétérogènes (`ic_id` fait foi), des unités différentes selon l'endpoint (°C ou K).
- **Véracité** : contrôle qualité, valeurs rejetées, stations participatives peu fiables.

---

## 3. Architecture cible

```
            ┌───────────────────────── INGESTION ─────────────────────────┐
API REST ──►│ Batch historique (Airflow DAG, pagination, retry)           │──► BRONZE (JSON brut)
Infoclimat  │ "Streaming" : producer qui rejoue l'historique jour/heure   │──► Kafka topic `obs.raw`
            └──────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
            ┌──────────────────────── TRAITEMENT (Spark) ─────────────────┐
            │ SILVER : typage, dédup, filtres QC, unités, jointure ref    │
            │ GOLD   : features (lags, moyennes glissantes, saison,       │
            │          écart à la normale, voisins spatiaux) + cible J+1  │
            │ Spark Structured Streaming : features calculées à la volée  │
            └──────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
            ┌──────────────────────── ML ─────────────────────────────────┐
            │ Entraînement (Spark MLlib / LightGBM), suivi MLflow         │
            │ Registry : modèle "Production"                              │
            └──────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
            ┌──────────────────────── SERVING ────────────────────────────┐
            │ Prédictions stockées (table `predictions`)                  │
            │ API FastAPI  GET /forecast?station=07156                    │
            │ Dashboard (Streamlit ou Grafana) : prévu vs observé, erreur │
            └──────────────────────────────────────────────────────────────┘
```

### Stack proposée (le tout en Docker Compose)

| Couche | Outil | Justification |
|---|---|---|
| Stockage objet (data lake) | **MinIO** (compatible S3, image `pgsty/minio` : l'image officielle n'est plus publiée sur Docker Hub) | même paradigme que le bucket OVH de la source |
| Format de table | **Delta Lake** sur Parquet (retenu) | ACID, time travel, partitionnement (`station`, `année`). La source utilise déjà Iceberg, c'est un bon argument |
| Messagerie | **Kafka** (ou Redpanda, plus léger) | découple l'ingestion du traitement et simule le temps réel |
| Traitement | **Apache Spark** (batch + Structured Streaming) | scale horizontal, une seule API pour le batch et le streaming |
| Orchestration | **Airflow** | DAG d'ingestion quotidienne, de réentraînement et de backfill |
| ML | Spark MLlib ou LightGBM, **MLflow** | tracking des expériences et registre de modèles |
| Serving | **FastAPI** et PostgreSQL (dernières prédictions) | lecture rapide par station |
| Visualisation | **Streamlit** ou Grafana | démo |

> Version allégée si les machines sont faibles : DuckDB/Polars à la place de Spark, et pas de Kafka (micro-batch Airflow toutes les X minutes). On garde quand même la séparation bronze/silver/gold.

### Modèle de données (gold)

`features_daily` (partitionnée par `annee`, `ic_id`)
| ic_id | date | tn, tx, rr | tn_lag1..7, tx_lag1..7 | tx_mean_7j | rr_sum_3j | doy_sin, doy_cos | ecart_normale | tx_voisins_moy, tn_voisins_moy | lat, lon, alt | **cible_tx_j1, cible_tn_j1, cible_pluie_j1** |

`predictions`
| ic_id | date_emission | date_cible | variable | valeur_predite | modele_version | valeur_observee (renseignée après coup) |

---

## 4. Démarche ML

1. **Baselines** (obligatoires, ce sont elles qui donnent du sens aux résultats) :
   - persistance : demain = aujourd'hui
   - climatologie : demain = normale 1991-2020 du jour
2. **Modèles** : régression linéaire, puis Random Forest / Gradient Boosting (LightGBM ou GBT MLlib). Option : LSTM si on a le temps.
3. **Validation temporelle** : on entraîne jusqu'à 2022, on valide sur 2023 et on teste sur 2024-2025. **Pas de split aléatoire** (sinon fuite de données).
4. **Métriques** : MAE et RMSE en °C pour tn/tx, F1 et Brier pour la pluie. On compare toujours à la persistance (skill score = 1 - MAE_modèle / MAE_persistance).
5. **Monitoring** : erreur glissante par station dans le dashboard, avec réentraînement déclenché par Airflow si elle dérive.

---

## 5. Planning (à adapter aux dates de rendu)

| Phase | Contenu | Livrable |
|---|---|---|
| **S1 : cadrage** | choix des stations (ex. 50 à 200 stations synop / Météo-France principales actives), demande du token, docker-compose (MinIO, Spark, Kafka, Airflow) | schéma d'archi, repo initialisé |
| **S2 : ingestion** | client API (pagination, retry, logs), DAG de backfill historique vers bronze, référentiel stations | bronze rempli |
| **S3 : silver/gold** | jobs Spark de nettoyage et de feature engineering, voisins spatiaux (k plus proches via lat/lon) | tables silver et gold |
| **S4 : ML** | baselines, modèles, MLflow, choix du modèle | notebook de résultats et modèle enregistré |
| **S5 : temps réel** | producer de replay vers Kafka, puis Spark Streaming, puis prédiction et table `predictions` | pipeline de bout en bout |
| **S6 : serving + démo** | FastAPI, dashboard, README, slides | démo et soutenance |

---

## 6. Répartition possible (4 personnes)

- **Data engineer ingestion** : client API, Airflow, Kafka producer
- **Data engineer traitement** : Spark silver/gold, format de table, partitionnement
- **Data scientist** : features, baselines, modèles, MLflow
- **Ops / serving** : docker-compose, FastAPI, dashboard, monitoring, doc

---

## 7. Structure du repo

```
archi-big-data/
├── docker-compose.yml
├── README.md
├── docs/                 # schéma d'archi, choix techniques, slides
├── ingestion/
│   ├── client.py         # appels API + pagination `suite.from`
│   ├── backfill.py
│   └── replay_producer.py
├── dags/                 # Airflow
├── spark_jobs/
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│   └── streaming_features.py
├── ml/
│   ├── baselines.py
│   ├── train.py
│   └── evaluate.py
├── serving/
│   ├── api/              # FastAPI
│   └── dashboard/        # Streamlit
└── notebooks/            # exploration
```

---

## 8. Risques et parades

| Risque | Parade |
|---|---|
| Pas de token, donc pas d'infra-horaire | V1 journalière déjà complète et défendable. On présente le +6 h comme une extension |
| Latence de la source (environ une semaine) et pas de flux temps réel | streaming par replay de l'historique, et on l'assume à l'oral (c'est le schéma classique de test de pipeline) |
| API mono-machine lente ou instable | cache bronze (on n'appelle jamais deux fois la même page), parallélisme limité, backoff |
| Machines trop faibles pour Spark + Kafka + Airflow | profil Compose « light » (DuckDB, sans Kafka) |
| Stations trouées ou défaillantes | filtre sur `statut_extremes = quotidien` et `n_obs`, et features voisines pour compenser |

---

## 9. Avancement

- [x] Docker Compose complet (MinIO, Spark master + worker, Kafka, Airflow, MLflow, PostgreSQL, FastAPI, Streamlit)
- [x] Client API (pagination `suite.from`, flag `tronque`, retry et `Retry-After`) + tests
- [x] Sélection des stations (synop Météo-France de métropole, environ 220) et ingestion bronze
- [x] Spark bronze → silver → gold (Delta), features et voisins spatiaux
- [x] Entraînement GBT, baselines, MLflow (suivi et registre), prévisions batch
- [x] Streaming : rejeu Kafka → Spark Structured Streaming → PostgreSQL
- [x] DAGs Airflow (backfill, quotidien, réentraînement hebdomadaire)
- [ ] Demander le token Archive API (pour la V2 à +6 h)
- [ ] Faire valider le cadrage (J+1 en V1, +6 h en V2) par le prof
- [ ] Lancer le backfill complet (toutes les stations depuis 1990) et figer les résultats pour le rapport
- [ ] Slides et schéma d'architecture pour la soutenance
