# Documentation technique — University Data Platform

Ce document contient les explications techniques détaillées de la plateforme. Il est destiné aux développeurs et contributeurs souhaitant comprendre l'architecture interne, les algorithmes et les choix de conception.

---

## Table des matières

1. [Schéma des données](#1--schéma-des-données)
2. [Pipeline ETL Spark](#2--pipeline-etl-spark)
3. [Classificateur multi-signal](#3--classificateur-multi-signal)
4. [Système d'embedding](#4--système-dembedding)
5. [Indexation Hudi → ES / Qdrant](#5--indexation-hudi--es--qdrant)
6. [Architecture de recherche](#6--architecture-de-recherche)
7. [Pipeline RAG](#7--pipeline-rag)
8. [Configuration Trino / Hive Metastore](#8--configuration-trino--hive-metastore)
9. [Observabilité](#9--observabilité)
10. [Performance et optimisations](#10--performance-et-optimisations)

---

## 1 — Schéma des données

### Tables Hudi (curated zone)

Tables stockées dans `s3a://curated-zone/` au format COPY_ON_WRITE, synchronisées avec le Hive Metastore.

#### `faculty_profiles`

| Colonne | Type | Description |
|---|---|---|
| `record_id` | STRING | MD5 de `source_url` — clé unique |
| `record_type` | STRING | `faculty_profile` |
| `source_system` | STRING | Identifiant de la source (ex: `um5_website`) |
| `source_url` | STRING | URL d'origine |
| `content_hash` | STRING | Hash MD5 du contenu brut |
| `crawl_timestamp` | LONG | Timestamp du crawl (millisecondes) |
| `business_timestamp` | TIMESTAMP | Date de dernière modification |
| `title` | STRING | Titre de la page |
| `full_name` | STRING | Nom complet normalisé |
| `email` | STRING | Adresse email |
| `department` | STRING | Département / faculté |
| `faculty` | STRING | Faculté |
| `orcid_id` | STRING | Identifiant ORCID |
| `research_interests` | STRING | Thématiques de recherche |
| `publications` | STRING | Liste de publications |
| `normalized_text` | STRING | Texte nettoyé et normalisé |
| `language` | STRING | Langue détectée (fr, en, ar...) |
| `is_deleted` | STRING | Flag de suppression logique |
| `chunks` | ARRAY\<STRING\> | Texte découpé en chunks |
| `embedding` | ARRAY\<FLOAT\> | Vecteur d'embedding (768 dim) |
| `embedding_method` | STRING | Méthode utilisée (local, gemini, tfidf) |

**Partitionnement** : `source_system`
**Record key** : `record_id`

#### `course_catalog`

| Colonne | Type | Description |
|---|---|---|
| `record_id` | STRING | MD5 de `source_url` |
| `record_type` | STRING | `course_catalog` |
| `course_name` | STRING | Intitulé du cours |
| `course_code` | STRING | Code du cours |
| `instructor` | STRING | Enseignant responsable |
| `level` | STRING | Niveau (licence, master...) |
| `description` | STRING | Description du cours |
| `department` | STRING | Département |
| `faculty` | STRING | Faculté |
| `normalized_text` | STRING | Texte normalisé |
| `language` | STRING | Langue détectée |
| `chunks` | ARRAY\<STRING\> | Chunks de texte |
| `embedding` | ARRAY\<FLOAT\> | Vecteur d'embedding |
| `embedding_method` | STRING | Méthode d'embedding |

#### `university_news`

| Colonne | Type | Description |
|---|---|---|
| `record_id` | STRING | MD5 de `source_url` |
| `record_type` | STRING | `university_news` |
| `title` | STRING | Titre de l'actualité |
| `description` | STRING | Résumé |
| `category` | STRING | Catégorie (événement, résultat, etc.) |
| `normalized_text` | STRING | Texte normalisé |
| `language` | STRING | Langue détectée |
| `chunks` | ARRAY\<STRING\> | Chunks de texte |
| `embedding` | ARRAY\<FLOAT\> | Vecteur d'embedding |

#### `pdf_documents`

| Colonne | Type | Description |
|---|---|---|
| `record_id` | STRING | Hash du contenu PDF |
| `record_type` | STRING | `pdf_document` |
| `file_name` | STRING | Nom du fichier |
| `content_type` | STRING | Type MIME |
| `extracted_text` | STRING | Texte extrait du PDF |
| `normalized_text` | STRING | Texte normalisé |
| `language` | STRING | Langue détectée |
| `chunks` | ARRAY\<STRING\> | Chunks de texte |
| `embedding` | ARRAY\<FLOAT\> | Vecteur d'embedding |

### Index Elasticsearch

#### `faculty_profiles`

```json
{
  "mappings": {
    "properties": {
      "record_id": { "type": "keyword" },
      "record_type": { "type": "keyword" },
      "source_system": { "type": "keyword" },
      "source_url": { "type": "keyword" },
      "title": { "type": "text", "analyzer": "french", "fields": { "keyword": { "type": "keyword" } } },
      "full_name": { "type": "text", "analyzer": "french", "fields": { "keyword": { "type": "keyword" } } },
      "email": { "type": "keyword" },
      "department": { "type": "text", "analyzer": "french", "fields": { "keyword": { "type": "keyword" } } },
      "faculty": { "type": "text", "analyzer": "french", "fields": { "keyword": { "type": "keyword" } } },
      "normalized_text": { "type": "text", "analyzer": "french" },
      "language": { "type": "keyword" },
      "publications": { "type": "text", "analyzer": "french" },
      "research_interests": { "type": "text", "analyzer": "french" },
      "orcid_id": { "type": "keyword" }
    }
  }
}
```

#### `course_catalog`

```json
{
  "mappings": {
    "properties": {
      "record_id": { "type": "keyword" },
      "record_type": { "type": "keyword" },
      "source_system": { "type": "keyword" },
      "course_name": { "type": "text", "analyzer": "french", "fields": { "keyword": { "type": "keyword" } } },
      "course_code": { "type": "keyword" },
      "instructor": { "type": "text", "analyzer": "french", "fields": { "keyword": { "type": "keyword" } } },
      "level": { "type": "keyword" },
      "description": { "type": "text", "analyzer": "french" },
      "department": { "type": "text", "analyzer": "french", "fields": { "keyword": { "type": "keyword" } } },
      "normalized_text": { "type": "text", "analyzer": "french" },
      "language": { "type": "keyword" }
    }
  }
}
```

### Collections Qdrant

| Collection | Dimension | Description |
|---|---|---|
| `faculty_profiles_chunks` | 768 | Chunks de profils enseignants |
| `course_catalog_chunks` | 768 | Chunks de catalogue de cours |
| `university_news_chunks` | 768 | Chunks d'actualités |
| `pdf_documents_chunks` | 768 | Chunks de documents PDF |

Chaque point Qdrant contient :
- `vector` : embedding du chunk (768 dimensions)
- `payload` : `record_id`, `source_system`, `source_url`, `chunk_index`, `text`, `record_type`

---

## 2 — Pipeline ETL Spark

### Architecture du cluster

```
spark-master:7077 (cluster manager)
    ├── spark-worker (2 cores, 2 Go RAM)
    └── spark-worker-2 (2 cores, 2 Go RAM)
```

### Entrée / Sortie

```
Entrée : s3a://raw-zone/{source_system}/  (MinIO)
Sortie : s3a://curated-zone/{table_name}/  (Hudi COPY_ON_WRITE)
```

### Étapes du pipeline

Chaque pipeline (`build_faculty_profiles`, `build_course_catalog`, `build_university_news`, `build_pdf_documents`) suit le même schéma :

```
1. read_raw_html()              → DataFrame(source_url, content_checksum, raw_object_path, ...)
2. fetch_raw_html_content()     → Ajoute la colonne raw_html (contenu depuis MinIO)
3. parse_*_from_raw()           → Schéma cible (faculty/course/news/pdf)
4. parse_html_content()         → normalized_text (extraction BS4)
5. F.filter(raw_html.startswith("<"))  → Filtre les lignes non-HTML
6. _combined_classify()         → record_type (sémantique + regex)
7. F.filter(record_type == target)    → Garde uniquement le type cible
8. normalize_*()                → Nettoyage des champs
9. deduplicate_by_hash()        → Déduplication (content_hash + source_url)
10. enrich_with_language()      → Détection de langue (fastText ou heuristic)
11. generate_chunks()           → Découpage en chunks (512 mots, overlap 64)
12. add_embeddings_to_df()      → Vecteurs d'embedding
13. write_to_hudi()             → Écriture Hudi (s3a://curated-zone/)
```

### Gestion des erreurs

- Les étapes 10-12 (OpenAlex enrichment, embeddings) sont dans des blocs `try/except` — une失败 ne bloque pas le pipeline
- Si le modèle sémantique ne charge pas, la classification passe en mode regex seul
- Si aucun API key n'est configuré, les embeddings TF-IDF sont utilisés en fallback

---

## 3 — Classificateur multi-signal

Le classificateur détermine le type de chaque page web. Il utilise deux signaux combinés.

### Signal 1 : Classification sémantique (principal)

**Modèle** : `paraphrase-multilingual-mpnet-base-v2` (sentence-transformers)

**Principe** : Embedding par similarité cosinus entre le contenu de la page et des "ancres" textuelles prédéfinies.

**Ancres par catégorie** :

```python
_ANCHOR_TEXTS = {
    "faculty_profile": [
        "Professeur d'université, département, faculté, email, bureau, téléphone",
        "Chercheur, publications scientifiques, articles, revues, conférences",
        "Thèse de doctorat, encadrement, supervision, laboratoire de recherche",
        "Curriculum vitae, parcours académique, grade universitaire, HDR",
        "Profil personnel d'un enseignant-chercheur universitaire",
        "ORCID, HAL, Google Scholar, indice h, citations scientifiques",
        "Biographie d'un professeur, parcours d'enseignement et de recherche",
    ],
    "course_catalog": [
        "Module universitaire, semestre, programme d'études, maquette pédagogique",
        "Cours magistral, travaux dirigés, travaux pratiques, volume horaire",
        "Crédits ECTS, coefficient, prérequis, objectifs pédagogiques",
        "Syllabus, plan de cours, contenu pédagogique, compétences visées",
        "Fiche matière, descriptif de cours, évaluation, examen",
        "Licence, master, filière, spécialisation, parcours universitaire",
        "Emploi du temps, calendrier des cours, salle de cours",
    ],
    "university_news": [
        "Actualité universitaire, annonce officielle, information importante",
        "Événement, colloque, séminaire, conférence, atelier, journée d'étude",
        "Avis aux étudiants, convocation, inscription, calendrier des examens",
        "Concours de recrutement, offre d'emploi, appel à candidatures",
        "Résultats, listes de candidats, délibération, décret",
        "Nouvelle universitaire, communication officielle, communiqué de presse",
        "Calendrier académique, rentrée universitaire, vacances, session d'examens",
    ],
}
```

**Processus** :
1. Encoder toutes les ancres avec le modèle → vecteurs pré-calculés (une seule fois)
2. Encoder le texte de la page (500 premiers caractères du titre + normalized_text)
3. Calculer la similarité cosinus avec chaque ancre
4. Moyenner les scores par catégorie → score sémantique

### Signal 2 : Classification regex (correction)

Basée sur les patterns d'URL et les mots-clés :

- **Patterns d'URL** : `/prof/`, `/faculty/`, `/teacher/` → faculty ; `/course/`, `/module/`, `/syllabus/` → course ; `/news/`, `/actualite/`, `/event/` → news
- **Mots-clés du titre** : "professeur", "enseignant" → faculty ; "cours", "module" → course ; "actualité", "événement" → news
- **Mots-clés du corps** : "publications", "département" → faculty ; "crédits", "ECTS" → course ; "annonce", "convocation" → news

**Seuil de score** : 6+ pour une correspondance faible, 12+ pour une correspondance forte.

### Combinaison des signaux

```python
def combine_semantic_and_regex(
    sem_label, sem_faculty, sem_course, sem_news,
    regex_label, regex_faculty, regex_course, regex_news,
    target_type,
) -> str:
```

**Règles de décision** :

| Règle | Condition | Résultat |
|---|---|---|
| 1 | Les deux signaux sont d'accord | Label commun (haute confiance) |
| 2 | Sémantique incertain (< 0.30) + regex confiant (> 0.50) | Regex gagne |
| 3 | Sémantique dit "other" + regex confiant | Regex gagne |
| 4 | Sémantique a un label + regex dit "other" | Sémantique gagne |
| 5 | Labels différents, les deux confiants | Sémantique gagne |
| 6 | Aucune règle ne s'applique | `target_type` (le type attendu par le pipeline) |

### Exécution dans Spark

Le classificateur est exécuté via des UDFs PySpark :

```python
# UDF sémantique
classify_semantic_udf = F.udf(_classify_semantic_row, StructType([...]))

# UDF regex
classify_regex_udf = F.udf(_classify_regex_row, StructType([...]))

# UDF combiné
combine_udf = F.udf(combine_semantic_and_regex, StringType())
```

Le modèle est chargé une seule fois par worker (variable globale lazy-loaded) et réutilisé pour toutes les lignes.

---

## 4 — Système d'embedding

### Stratégie de fallback

```python
def add_embeddings_to_df(spark, df, method="auto"):
    if method == "auto":
        # 1. Essayer Gemini (si clé API disponible)
        # 2. Essayer local sentence-transformers
        # 3. Fallback TF-IDF
```

### Méthode 1 : Gemini API

- **Modèle** : `gemini-embedding-2` (768 dimensions)
- **Requête** : `POST https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-2:embedContent`
- **Batch** : 100 textes par requête
- **Rate limit** : Géré avec retry automatique

### Méthode 2 : Local sentence-transformers

- **Modèle** : `paraphrase-multilingual-mpnet-base-v2` (768 dimensions)
- **Exécution** : Sur le driver Spark (pas distribué)
- **Batch** : 64 textes par traitement
- **Cache** : Modèle téléchargé une fois, stocké dans `/tmp/hf_cache/`
- **Normalisation** : `normalize_embeddings=True` (cosine similarity)

### Méthode 3 : TF-IDF (fallback)

- **Dimensions** : 256
- **Implémentation** : Spark MLlib (`HashingTF` + `IDF`)
- **Avantage** : Fonctionne sans modèle externe
- **Inconvénient** : Qualité inférieure pour la recherche sémantique

### Stockage

Les embeddings sont stockés :
- **Hudi** : Colonne `embedding` (ARRAY\<FLOAT\>) dans chaque table
- **Qdrant** : Vecteur principal de chaque point dans les collections `*_chunks`

---

## 5 — Indexation Hudi → ES / Qdrant

### Script d'indexation

`airflow/scripts/index_hudi_to_qdrant.py` — exécuté par le DAG `university_search_indexing`.

**Processus** :

```
1. Lire les tables Hudi depuis s3a://curated-zone/
2. Pour chaque table :
   a. Indexer dans Elasticsearch (bulk API)
   b. Découper en chunks (si pas déjà fait)
   c. Générer les embeddings (si pas déjà fait)
   d. Indexer dans Qdrant (upsert par batch)
```

### Indexation Elasticsearch

```python
# Bulk indexing avec retry
for chunk in chunks(rows, size=500):
    actions = [{"index": {"_index": index_name, "_id": r["record_id"]}} for r in chunk]
    es.bulk(body=actions + chunk, timeout="60s")
```

### Indexation Qdrant

```python
# Upsert par batch
for chunk in chunks(rows, size=100):
    points = [
        PointStruct(
            id=hash(record_id + chunk_index),
            vector=embedding,
            payload={"record_id": record_id, "text": text, ...}
        )
        for row in chunk
        for i, (text, embedding) in enumerate(zip(row["chunks"], row["embedding"]))
    ]
    qdrant.upsert(collection_name=collection, points=points)
```

### Gestion des doublons

- **ES** : `record_id` comme `_id` → upsert automatique
- **Qdrant** : ID basé sur `hash(record_id + chunk_index)` → upsert = mise à jour

---

## 6 — Architecture de recherche

### Search API (FastAPI)

L'API search expose une interface REST unifiée pour tous les moteurs de recherche.

**Routes principales** :

```
POST /es/{index}/search
  → Recherche multi-champ avec fuzziness
  → Paramètres: query, size, from, filters, sort

POST /es/{index}/faceted-search
  → Même chose + agrégations par champ
  → Retourne: hits + facettes (comptages par valeur)

POST /qdrant/semantic-search
  → Recherche vectorielle par similarité cosinus
  → Paramètres: collection_name, query, limit, score_threshold

POST /qdrant/hybrid-search
  → Combine ES (keyword) + Qdrant (sémantique)
  → Score hybride: 0.3 * ES_score + 0.7 * Qdrant_score

POST /rag/query
  → Pipeline RAG complet (voir section 7)
```

### Recherche hybride

```python
def hybrid_search(query, es_index, collection_name, limit=10):
    # 1. Recherche ES (keyword)
    es_results = search(es_index, query, size=limit * 2)

    # 2. Recherche Qdrant (sémantique)
    qdrant_results = semantic_search(collection_name, query, limit=limit * 2)

    # 3. Fusion des résultats
    # Score hybride = 0.3 * es_score_normalized + 0.7 * qdrant_score
    # Dédoublonnage par record_id
    # Tri par score hybride décroissant
    # Retourne top-N résultats
```

---

## 7 — Pipeline RAG

### Architecture

```
Utilisateur → Query → Embedding → Qdrant (top-K chunks)
                                    ↓
                              Context chunks
                                    ↓
                              Prompt construit
                                    ↓
                              LLM (Gemma/Gemini)
                                    ↓
                              Réponse grounded
```

### Étapes

1. **Embedding de la requête** : La question est encodée avec le même modèle que les chunks
2. **Récupération** : Recherche dans Qdrant pour les K chunks les plus similaires
3. **Construction du prompt** : Les chunks récupérés sont insérés comme contexte
4. **Génération** : Le LLM génère une réponse basée sur le contexte

### Configuration

```python
RAG_MODEL = "gemma-4-31b-it"      # ou "gemini-2.0-flash"
RAG_MAX_CHUNKS = 5                 # Nombre de chunks récupérés
RAG_TEMPERATURE = 0.3              # Température de génération
```

### Modèles supportés

| Modèle | Qualité | Vitesse | API requise |
|---|---|---|---|
| `gemma-4-31b-it` | Élevée | Lente | Gemini API |
| `gemma-4-26b-a4b-it` | Bonne | Rapide | Gemini API |
| `gemini-2.0-flash` | Moyenne | Très rapide | Gemini API |

---

## 8 — Configuration Trino / Hive Metastore

### Hive Metastore

- **Image** : `apache/hive:3.1.3-jdk17`
- **Backend** : PostgreSQL (`hive-metastore-db`)
- **Port** : 9083 (Thrift)
- **Rôle** : Catalogue de tables pour Spark et Trino

### Trino

- **Version** : 435
- **Connecteur** : Hive Metastore
- **Port** : 8080 (interne), 8081 (hôte)
- **Rôle** : Moteur SQL pour interroger les tables Hudi

### Requêtes Trino

```sql
-- Voir les schémas
SHOW SCHEMAS FROM hive;

-- Voir les tables
SHOW TABLES FROM hive.curated;

-- Requêter
SELECT full_name, department, email
FROM hive.curated.faculty_profiles
WHERE source_system = 'um5_website';
```

### Métastore sync

Spark écrit les tables Hudi avec l'option :
```python
.option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
.option("hoodie.table.name", table_name)
```

Le Hive Metastore est automatiquement mis à jour lors de l'écriture.

---

## 9 — Observabilité

### Logs Airflow

- Stockés dans `airflow/logs/` (monté en volume)
- Accessibles via l'UI Airflow (http://localhost:8083)

### Logs Spark

- Accessibles via le Spark UI (http://localhost:8080)
- Logs des stages, tasks, et executors

### Logs Elasticsearch

- Index `platform-logs` dans Elasticsearch
- Accessibles via Kibana (http://localhost:5601)
- Le DAG `observability_dag` collecte les métriques

### Monitoring

- **Airflow** : État des DAGs, historique des exécutions, alertes
- **Spark UI** : Progression des jobs, utilisation mémoire, lecture/écriture
- **Kibana** : Logs centralisés, tableaux de bord
- **Metabase** : Métriques métier via Trino

---

## 10 — Performance et optimisations

### Spark

- **Partitionnement** : Les tables Hudi sont partitionnées par `source_system` → lecture sélective
- **Format** : COPY_ON_WRITE pour des performances de lecture optimales
- **Cache** : Le modèle sémantique est chargé une seule fois par worker (variable globale)
- **UDFs** : Le classificateur est exécuté via des UDFs Spark (distribué sur les workers)

### Elasticsearch

- **Analyseur** : `french` pour les champs textuels (stemming, stop words)
- **Mapping** : Champs keyword pour les filtres, text pour la recherche
- **Bulk indexing** : Batch de 500 documents pour l'indexation

### Qdrant

- **Distance** : Cosine similarity (normalisé)
- **Index** : HNSW (approximate nearest neighbors)
- **Upsert** : Mise à jour incrémentale par batch de 100

### Recherche

- **Hybride** : Score combiné 30% keyword + 70% sémantique (ajustable)
- **Cache** : Les embeddings sont pré-calculés et stockés (pas de calcul à la recherche)
- **Pagination** : Support `offset` + `limit` sur tous les endpoints

---

## Annexe : Variables d'environnement

| Variable | Défaut | Description |
|---|---|---|
| `MINIO_ENDPOINT` | `minio:9000` | Endpoint MinIO (interne Docker) |
| `MINIO_ROOT_USER` | `minioadmin` | Identifiant MinIO |
| `MINIO_ROOT_PASSWORD` | `minioadmin` | Mot de passe MinIO |
| `HF_HOME` | `/tmp/hf_cache` | Cache HuggingFace (modèles) |
| `TRANSFORMERS_CACHE` | `/tmp/hf_cache` | Cache Transformers |
| `LOCAL_EMBEDDING_MODEL` | `paraphrase-multilingual-mpnet-base-v2` | Modèle d'embedding local |
| `GEMINI_API_KEY` | *(vide)* | Clé API Gemini |
| `RAG_MODEL` | `gemma-4-31b-it` | Modèle LLM pour RAG |
| `RAG_MAX_CHUNKS` | `5` | Nombre de chunks RAG |
| `RAG_TEMPERATURE` | `0.3` | Température LLM |
| `ELASTICSEARCH_HOSTS` | `http://elasticsearch:9200` | Endpoint ES |
| `QDRANT_HOST` | `qdrant` | Host Qdrant |
| `QDRANT_PORT` | `6333` | Port Qdrant REST |
| `TRINO_URL` | `http://trino:8080/v1/statement` | Endpoint Trino |
| `DEFAULT_LANGUAGE` | `fr` | Langue par défaut |
| `AUTO_PLAYWRIGHT` | `true` | Auto-install Playwright |
| `REQUEST_DELAY` | `0.5` | Délai entre requêtes (secondes) |
