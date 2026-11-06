# University Data Platform

Plateforme complète de collecte, traitement et recherche de données universitaires marocaines. La plateforme est conçue pour être **plug-and-play** : un simple `docker compose up` suffit pour tout lancer.

```
 Sources (UM5, UCA, FSJESM, etc.)
        │
        ▼
 ┌─────────────┐     ┌──────────────┐     ┌──────────────────┐
 │  Ingestion   │────▶│   MinIO      │────▶│  Spark ETL       │
 │  (scrapers)  │     │  (raw zone)  │     │  (traitement)    │
 └─────────────┘     └──────────────┘     └────────┬─────────┘
                                                    │
                                                    ▼
                                          ┌──────────────────┐
                                          │  Hudi Tables     │
                                          │  (curated zone)  │
                                          └────────┬─────────┘
                                                    │
                              ┌──────────────────────┼──────────────────────┐
                              ▼                      ▼                      ▼
                    ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
                    │ Elasticsearch│     │    Qdrant    │     │   Trino      │
                    │ (keyword)    │     │  (vectoriel) │     │  (SQL)       │
                    └──────┬───────┘     └──────┬───────┘     └──────────────┘
                           │                    │
                           ▼                    ▼
                    ┌──────────────────────────────────┐
                    │        Search API (FastAPI)       │
                    │   /es/*  /qdrant/*  /rag/*        │
                    └──────────────────────────────────┘
```

---

## Table des matières

1. [Prérequis](#1--prérequis)
2. [Installation et lancement](#2--installation-et-lancement)
3. [Vérification](#3--vérification)
4. [Interfaces web](#4--interfaces-web)
5. [Utilisation](#5--utilisation)
6. [Architecture de la plateforme](#6--architecture-de-la-plateforme)
7. [Sources de données](#7--sources-de-données)
8. [Classification des pages](#8--classification-des-pages)
9. [Embeddings](#9--embeddings)
10. [Recherche](#10--recherche)
11. [Configuration](#11--configuration)
12. [Structure du projet](#12--structure-du-projet)
13. [Documentation technique](#13--documentation-technique)

---

## 1 — Prérequis

| Composant | Version requise | Notes |
|---|---|---|
| **Docker Desktop** | Dernière version | Avec au moins **8 Go de RAM** allouée |
| **Docker Compose** | v2+ | Inclus avec Docker Desktop |
| **Git** | Dernière version | Pour cloner le dépôt |

**Ports requis** (doivent être libres) :

| Port | Service |
|---|---|
| 9000 | MinIO API |
| 9001 | MinIO Console |
| 8083 | Airflow UI |
| 8080 | Spark Master UI |
| 8081 | Trino SQL |
| 3000 | Metabase BI |
| 9200 | Elasticsearch |
| 5601 | Kibana |
| 6333 | Qdrant REST |
| 6334 | Qdrant gRPC |
| 8000 | Search API (FastAPI) |

---

## 2 — Installation et lancement

### Cloner le dépôt

```bash
git clone <url-du-depot>
cd university-data-platform
```

### Configurer l'environnement

```bash
cp .env.example .env
```

Le fichier `.env` fonctionne avec les valeurs par défaut. Aucune clé API n'est nécessaire pour démarrer — la plateforme utilise des embeddings locaux par défaut.

### Construire et lancer

```bash
docker compose build
docker compose up -d
```

> **Première construction** : 5 à 10 minutes (téléchargement des images Spark, Airflow, Elasticsearch, etc.).

### Vérifier que tout fonctionne

```bash
docker compose ps
```

Tous les services doivent afficher `healthy` (ou `exited (0)` pour les conteneurs d'initialisation comme `airflow-init`).

### Arrêter la plateforme

```bash
docker compose down
```

### Tout supprimer (données incluses)

```bash
docker compose down -v
```

> **Attention** : cela supprime toutes les données MinIO, Elasticsearch, Qdrant et les logs Airflow.

---

## 3 — Vérification

Après le lancement, vérifiez que les services critiques sont accessibles :

```bash
# MinIO
curl -s http://localhost:9000/minio/health/live

# Elasticsearch
curl -s http://localhost:9200/_cluster/health | python -m json.tool

# Qdrant
curl -s http://localhost:6333/collections

# Search API
curl -s http://localhost:8000/health
```

---

## 4 — Interfaces web

| Service | URL | Identifiants | Description |
|---|---|---|---|
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` | Stockage objet S3 — visualiser les fichiers bruts |
| **Airflow** | http://localhost:8083 | `admin` / `admin` | Orchestration des pipelines ETL |
| **Spark UI** | http://localhost:8080 | — | Monitoring des jobs Spark |
| **Trino SQL** | http://localhost:8081 | — | Requêtes SQL sur les tables Hudi |
| **Metabase BI** | http://localhost:3000 | — | Tableaux de bord et visualisations |
| **Elasticsearch** | http://localhost:9200 | — | API REST Elasticsearch |
| **Kibana** | http://localhost:5601 | — | Visualisation des logs et données ES |
| **Qdrant Dashboard** | http://localhost:6333/dashboard | — | Gestion des collections vectorielles |
| **Search API** | http://localhost:8000/docs | — | Documentation Swagger interactive |
| **Chat UI** | http://localhost:8000/ui | — | Interface de test pour la recherche sémantique et le RAG |

---

## 5 — Utilisation

### 5.1 Lancer un pipeline de traitement

#### Via l'interface Airflow (recommandé)

1. Ouvrez http://localhost:8083
2. Activez le DAG `university_spark_pipeline`
3. Cliquez sur **Trigger DAG**

Ce pipeline :
- Lit les données brutes depuis MinIO
- Les traite avec Spark (parsing, classification, normalisation, déduplication, embeddings)
- Écrit les résultats dans les tables Hudi (zone curée)

#### Via la ligne de commande

```bash
# Traiter toutes les sources
docker exec airflow-webserver airflow dags trigger university_spark_pipeline

# Traiter une source spécifique
docker exec airflow-webserver airflow dags trigger spark_um5
docker exec airflow-webserver airflow dags trigger spark_fsjesm
```

### 5.2 Indexer les données

Après le traitement, indexez les données dans Elasticsearch et Qdrant :

```bash
docker exec airflow-webserver airflow dags trigger university_search_indexing
```

### 5.3 Rechercher des données

#### Recherche par mot-clé (Elasticsearch)

```bash
curl -X POST http://localhost:8000/es/faculty_profiles/search \
  -H 'Content-Type: application/json' \
  -d '{"query": "professeurs de droit", "size": 10}'
```

#### Recherche sémantique (Qdrant)

```bash
curl -X POST http://localhost:8000/qdrant/semantic-search \
  -H 'Content-Type: application/json' \
  -d '{"collection_name": "faculty_profiles_chunks", "query": "professeurs de droit"}'
```

#### Recherche hybride (ES + Qdrant)

```bash
curl -X POST http://localhost:8000/qdrant/hybrid-search \
  -H 'Content-Type: application/json' \
  -d '{"es_index": "faculty_profiles", "query": "professeurs de droit"}'
```

#### Recherche avec RAG (LLM)

```bash
curl -X POST http://localhost:8000/rag/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "Quels sont les professeurs de droit à l'\''UM5 ?"}'
```

### 5.4 Interroger avec SQL (Trino)

Connectez-vous à Trino via l'interface web ou un client SQL :

```sql
-- Voir toutes les tables disponibles
SHOW SCHEMAS FROM hive;
SHOW TABLES FROM hive.curated;

-- Requêter les profils enseignants
SELECT full_name, department, email
FROM hive.curated.faculty_profiles
WHERE source_system = 'um5_website'
LIMIT 10;
```

### 5.5 Ajouter une nouvelle source de données

Éditez `sources.yml` à la racine du projet :

```yaml
sources:
  - type: html
    url: https://www.nouvelle-universite.ac.ma/
    source_system: nouvelle_universite_website
```

Puis relancez le pipeline d'ingestion :

```bash
docker exec airflow-webserver airflow dags trigger university_ingestion_daily
```

---

## 6 — Architecture de la plateforme

La plateforme est organisée en 4 phases, chacune gérée par des services Docker distincts.

### Phase 1 — Ingestion (collecte de données brutes)

Des scrapers collectent le contenu des sites web universitaires et les stockent dans MinIO (stockage objet S3-compatible).

| Scraper | Utilisation |
|---|---|
| **HtmlScraper** | Pages HTML statiques, suit les redirections JS |
| **DeepScraper** | Récursion BFS complète (HTML + images + PDFs + liens) |
| **PlaywrightScraper** | Pages dynamiques rendues par JavaScript |
| **ApiConnector** | APIs académiques (OpenAlex, Crossref, ORCID) |
| **FileConnector** | Fichiers PDF, CSV, documents Office |

Chaque fichier est accompagné d'un fichier `.meta.json` contenant les métadonnées de crawl (URL source, timestamp, checksum).

### Phase 2 — Traitement Spark (ETL)

Un cluster Spark (1 master + 2 workers) traite les données brutes et les écrit dans des tables Hudi versionnées.

**Étapes du traitement :**

1. **Lecture** des données HTML brutes depuis MinIO
2. **Parsing** du HTML en champs structurés (noms, emails, départements, cours...)
3. **Classification** du type de page (profil enseignant / catalogue de cours / actualité / autre)
4. **Normalisation** des champs (noms propres, adresses email, codes de cours...)
5. **Déduction** par hash de contenu et URL source
6. **Enrichissement** avec détection de langue
7. **Découpage** du texte en chunks pour le RAG (512 mots, 64 de chevauchement)
8. **Embedding** vectoriel des chunks (modèle local par défaut)
9. **Écriture** dans les tables Hudi (`s3a://curated-zone/`)

**Tables Hudi produites :**

| Table | Contenu |
|---|---|
| `faculty_profiles` | Profils enseignants (noms, emails, départements, publications, chunks, embeddings) |
| `course_catalog` | Catalogue de cours (noms, codes, descriptions, prérequis, chunks, embeddings) |
| `university_news` | Actualités universitaires (titres, dates, catégories, chunks, embeddings) |
| `pdf_documents` | Documents PDF extraits (texte, métadonnées, chunks, embeddings) |

### Phase 3 — Orchestration Airflow

Airflow orchestre tous les pipelines : ingestion, traitement Spark, et indexation.

**Principaux DAGs :**

| DAG | Fréquence | Description |
|---|---|---|
| `university_spark_pipeline` | Quotidien (4h) | Pipeline ETL complet : toutes les sources → Hudi |
| `university_search_indexing` | Quotidien (5h) | Indexation Hudi → Elasticsearch + Qdrant |
| `university_ingestion_daily` | Quotidien (2h) | Collecte brute depuis `sources.yml` |
| `spark_um5` | Toutes les 10 min | Traitement spécifique UM5 (itération rapide) |
| `spark_fsjesm` | Quotidien (6h) | Traitement spécifique FSJESM |
| `spark_fsac` | Quotidien (6h) | Traitement spécifique FSAC |
| `spark_hcp` | Quotidien (6h) | Traitement spécifique HCP |

Chaque source universitaire a ses propres DAGs d'ingestion et de traitement pour une planification indépendante.

### Phase 4 — Recherche et RAG

| Service | Rôle |
|---|---|
| **Elasticsearch** | Recherche par mot-clé, facettes, filtres, observabilité |
| **Qdrant** | Recherche vectorielle sémantique, recherche hybride |
| **Search API** | API REST unifiée (FastAPI) — endpoints ES, Qdrant et RAG |
| **Kibana** | Visualisation des données Elasticsearch |
| **Metabase** | Tableaux de bord BI sur les données Hudi via Trino |

---

## 7 — Sources de données

La plateforme collecte des données de sources diverses, configurées dans `sources.yml` :

| Catégorie | Sources | Type |
|---|---|---|
| **Universités** | UM5, UCA, USMBA, UH2C, USMS, UIZ | HTML scraping |
| **Services étudiants** | ONOUC, ENSSUP, MEN | HTML scraping |
| **Recherche** | IMIST, Toubkal | HTML scraping |
| **Open Data** | data.gov.ma, HCP | HTML scraping |
| **APIs académiques** | OpenAlex, Crossref, ORCID | API REST |
| **Contenus éducatifs** | Wikipédia (mathématiques) | HTML scraping |

---

## 8 — Classification des pages

La plateforme classifie automatiquement chaque page web en 4 catégories :

| Type | Description |
|---|---|
| `faculty_profile` | Page de profil d'enseignant (nom, département, publications) |
| `course_catalog` | Page de cours (intitulé, code, prérequis, description) |
| `university_news` | Actualité, announcement, emploi du temps |
| `other` | Page non classifiée (accueil, annuaire, etc.) |

La classification utilise un système **multi-signal** :

1. **Sémantique** (principal) — Modèle `paraphrase-multilingual-mpnet-base-v2` comparant le contenu à des ancres textuelles décrivant chaque catégorie
2. **Regex** (correction) — Patterns d'URL et mots-clés pour affiner la classification

---

## 9 — Embeddings

La plateforme génère des embeddings vectoriels pour la recherche sémantique. Trois méthodes sont disponibles, avec bascule automatique :

| Priorité | Méthode | Dimensions | Configuration |
|---|---|---|---|
| 1 | **Gemini API** (`gemini-embedding-2`) | 768 | Clé API requise (`GEMINI_API_KEY` dans `.env`) |
| 2 | **Local** (`paraphrase-multilingual-mpnet-base-v2`) | 768 | Fonctionne sans configuration |
| 3 | **TF-IDF** | 256 | Dernier recours, pas de modèle externe |

> La plateforme fonctionne **sans aucune clé API** — les embeddings locaux sont utilisés par défaut.

---

## 10 — Recherche

### Endpoints de l'API Search

| Endpoint | Méthode | Description |
|---|---|---|
| `GET /health` | État de santé des services |
| `POST /es/{index}/search` | Recherche par mot-clé (multi-champ, fuzzy) |
| `POST /es/{index}/faceted-search` | Recherche + comptages de facettes |
| `POST /qdrant/semantic-search` | Recherche vectorielle pure |
| `POST /qdrant/hybrid-search` | Combinaison ES + Qdrant (keyword + sémantique) |
| `POST /rag/query` | Pipeline RAG : récupération → contexte → génération LLM |
| `GET /indexes` | Liste des index et collections disponibles |
| `GET /ui` | Interface web de test (chat) |

### Recherche hybride

La recherche hybride combine les forces de chaque moteur :

- **Elasticsearch** : pertinence lexicale, fuzzy matching, filtres structurés
- **Qdrant** : similarité sémantique, compréhension du sens

Les résultats sont fusionnés avec un score combiné pour une pertinence optimale.

---

## 11 — Configuration

### Variables d'environnement

Copiez `.env.example` en `.env` et modifiez selon vos besoins :

```bash
cp .env.example .env
```

**Variables principales :**

| Variable | Défaut | Description |
|---|---|---|
| `MINIO_ROOT_USER` | `minioadmin` | Identifiant MinIO |
| `MINIO_ROOT_PASSWORD` | `minioadmin` | Mot de passe MinIO |
| `GEMINI_API_KEY` | *(vide)* | Clé API Gemini pour embeddings (optionnel) |
| `LOCAL_EMBEDDING_MODEL` | `paraphrase-multilingual-mpnet-base-v2` | Modèle d'embedding local |
| `RAG_MODEL` | `gemma-4-31b-it` | Modèle LLM pour le RAG |
| `DEFAULT_LANGUAGE` | `fr` | Langue par défaut pour la détection |

### Ajouter des sources

Éditez `sources.yml` pour ajouter ou modifier les sources de données. Chaque source nécessite :

- `type` : `html`, `api`, ou `file`
- `url` : URL de départ (pour le scraping)
- `source_system` : identifiant unique de la source

---

## 12 — Structure du projet

```
university-data-platform/
├── university_data_platform/     # Code source Python
│   ├── config/                   # Modèles de configuration Pydantic
│   ├── ingestion/                # Scrapers (HTML, API, fichier, Playwright)
│   ├── storage/                  # Client MinIO
│   ├── processing/               # Pipeline ETL Spark, classifieur, embedder
│   ├── search/                   # Indexeur ES, recherche Qdrant, FastAPI
│   ├── pipelines/                # Pipelines d'ingestion
│   ├── metadata/                 # Schémas de métadonnées
│   └── tests/                    # Suite de tests
│
├── airflow/
│   ├── dags/                     # DAGs Airflow (montés dans les conteneurs)
│   └── scripts/                  # Scripts d'indexation (Hudi → ES / Qdrant)
│
├── docker/
│   ├── airflow/                  # Dockerfile Airflow
│   ├── spark/                    # Dockerfile Spark
│   ├── hive/                     # Dockerfile Hive Metastore
│   ├── trino/                    # Dockerfile Trino
│   └── search-api/               # Dockerfile Search API
│
├── docker-compose.yml            # Orchestration de tous les services
├── sources.yml                   # Configuration des sources de données
├── requirements.txt              # Dépendances Python
├── .env.example                  # Template de configuration
└── README.md                     # Ce fichier
```

---

## 13 — Documentation technique

Pour les explications détaillées sur l'architecture interne, les algorithmes, le schéma des données, et les décisions de conception, consultez :

> **[Documentation technique (docs/TECHNICAL.md)](docs/TECHNICAL.md)**

Ce document couvre en détail :
- Schéma complet des tables Hudi et Elasticsearch
- Algorithme de classification multi-signal
- Pipeline d'embedding et choix du modèle
- Architecture de l'indexation (Hudi → ES / Qdrant)
- Configuration de Trino et Hive Metastore
- Pipeline RAG (retrieval-augmented generation)
- Observabilité et monitoring

---

## Licence

Projet universitaire — données collectées à des fins de recherche académique.
