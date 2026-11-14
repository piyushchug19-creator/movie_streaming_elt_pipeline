# 🎬 Movie Streaming ELT Pipeline (`movie-streaming-elt-pipeline`)

An end-to-end, production-grade **Data Engineering ELT & Conversational BI Microproject** built with the **Modern Data Stack**:
**Airbyte ➔ Google BigQuery ➔ dbt Core ➔ Apache Airflow ➔ Streamlit Conversational BI (Gemini + LangChain)**.

[![dbt Core](https://img.shields.io/badge/dbt-Core%20v1.7+-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.8+-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Google BigQuery](https://img.shields.io/badge/Google-BigQuery-669DF6?logo=google-cloud&logoColor=white)](https://cloud.google.com/bigquery)
[![Airbyte](https://img.shields.io/badge/Airbyte-ELT-615EFF?logo=airbyte&logoColor=white)](https://airbyte.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.31+-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Google Gemini](https://img.shields.io/badge/Google-Gemini%20Flash-8E75B2?logo=google&logoColor=white)](https://aistudio.google.com/)

---

## 📑 Table of Contents
- [Architecture & Data Flow](#-architecture--data-flow)
- [Repository Structure](#-repository-structure)
- [Data Sources & Modeling Architecture](#-data-sources--modeling-architecture)
  - [Mathematical Formulations (Bayesian Rating & ROI)](#mathematical-formulations)
- [Prerequisites](#-prerequisites)
- [Quick Start Guide](#-quick-start-guide)
  - [1. Environment & Credentials](#1-environment--credentials)
  - [2. Google BigQuery Sandbox Setup](#2-google-bigquery-sandbox-setup)
  - [3. Airbyte Ingestion Setup (or Fast-Track Seeder)](#3-airbyte-ingestion-setup-or-fast-track-seeder)
  - [4. Apache Airflow Orchestration](#4-apache-airflow-orchestration)
  - [5. dbt Core Transformation & Testing](#5-dbt-core-transformation--testing)
  - [6. Streamlit Conversational BI Web App](#6-streamlit-conversational-bi-web-app)
- [Data Quality & Testing](#-data-quality--testing)
- [Troubleshooting & FAQ](#-troubleshooting--faq)

---

## 🏛 Architecture & Data Flow

```
┌─────────────────────────────────┐       ┌─────────────────────────────────┐
│       MovieLens Small Data      │       │     TMDB REST API Endpoint      │
│     (movies.csv, ratings.csv)   │       │     (/trending/movie/week)      │
└────────────────┬────────────────┘       └────────────────┬────────────────┘
                 │                                         │
                 └────────────────────┬────────────────────┘
                                      │ Ingest via Airbyte
                                      ▼
             ┌─────────────────────────────────────────────────┐
             │       Google BigQuery: raw_movies Dataset       │
             │       - movies                                  │
             │       - ratings                                 │
             │       - tmdb_trending                           │
             └────────────────────────┬────────────────────────┘
                                      │
                                      │ Orchestrated via Apache Airflow
                                      ▼ (@daily DAG: movie_elt_pipeline)
             ┌─────────────────────────────────────────────────┐
             │            dbt Core Transformations             │
             │  ┌───────────────────────────────────────────┐  │
             │  │ Staging (Views in analytics_movies)       │  │
             │  │ - stg_movies                              │  │
             │  │ - stg_ratings                             │  │
             │  │ - stg_tmdb_trending                       │  │
             │  └─────────────────────┬─────────────────────┘  │
             │                        ▼                        │
             │  ┌───────────────────────────────────────────┐  │
             │  │ Dimensional Marts (Clustered BigQuery)    │  │
             │  │ - mart_movie_performance                  │  │
             │  │ - mart_genre_trends                       │  │
             │  └───────────────────────────────────────────┘  │
             └────────────────────────┬────────────────────────┘
                                      │
                                      ▼ dbt test (Uniqueness, Not-Null, Ranges)
             ┌─────────────────────────────────────────────────┐
             │       Streamlit Conversational BI Web App       │
             │    Natural Language ➔ Gemini ➔ BigQuery SQL     │
             │    Interactive Plotly Visualizations & Insights │
             └─────────────────────────────────────────────────┘
```

---

## 📂 Repository Structure

```text
movie-streaming-elt-pipeline/
├── .env.example                     # Environment template (BigQuery, Gemini, Airbyte, TMDB)
├── .gitignore                       # Git exclusion rules (credentials, caches, logs)
├── README.md                        # Exhaustive project documentation
├── docker-compose.yml               # Local Airflow (Webserver, Scheduler, Postgres, dbt)
├── airflow/
│   ├── Dockerfile                   # Custom Airflow image with dbt-bigquery and Airbyte provider
│   ├── requirements.txt             # Airflow Python dependencies
│   └── dags/
│       └── movie_elt_dag.py         # Airflow DAG (Airbyte sync -> dbt run -> dbt test)
├── dbt_movies/
│   ├── dbt_project.yml              # dbt project configurations and variables
│   ├── profiles.yml                 # BigQuery service account profile
│   ├── profiles.yml.example         # Sample profile configuration
│   ├── packages.yml                 # dbt packages (dbt-labs/dbt_utils)
│   ├── macros/
│   │   ├── calculate_bayesian_rating.sql  # Macro for Bayesian weighted score
│   │   └── generate_schema_name.sql       # Custom schema naming override
│   └── models/
│       ├── staging/
│       │   ├── sources.yml          # BigQuery raw_movies source declarations & tests
│       │   ├── stg_movies.sql       # Cleans titles, extracts years, splits genres
│       │   ├── stg_ratings.sql      # Casts timestamps, validates rating range (0.5-5.0)
│       │   └── stg_tmdb_trending.sql# Cleans budget, revenue, popularity, parses dates
│       └── marts/
│           ├── mart_movie_performance.sql # Movie-level ratings, financials & ROI
│           ├── mart_genre_trends.sql      # Genre & decade unnested trend aggregations
│           └── schema.yml           # Data quality tests (unique, not_null, accepted_range)
├── bi_app/
│   ├── app.py                       # Streamlit Conversational BI (Gemini + Plotly + BigQuery)
│   └── requirements.txt             # BI application Python dependencies
└── scripts/
    ├── init_environment.sh          # One-click environment initializer
    └── load_sample_data.py          # Fast-track seeder to populate BigQuery raw_movies directly
```

---

## 📊 Data Sources & Modeling Architecture

### 1. Raw Ingestion Layer (`raw_movies`)
- **MovieLens Small**: 100,000 ratings across 9,700 movies submitted by 600 users (`movies.csv`, `ratings.csv`).
- **The Movie Database (TMDB)**: `/trending/movie/week` weekly top trending titles enriched with box office `budget`, `revenue`, and `popularity`.

### 2. Staging Layer (`analytics_movies` Views)
- `stg_movies`:
  - Extracts 4-digit release year using `REGEXP_EXTRACT(title, r'\((\d{4})\)\s*$')`.
  - Normalizes movie titles by removing the trailing year suffix.
  - Splits pipe-separated genres into a typed BigQuery `ARRAY<STRING>`.
  - Deduplicates records by `movie_id` using `QUALIFY ROW_NUMBER() OVER (...) = 1`.
- `stg_ratings`:
  - Converts Unix seconds timestamps to native `TIMESTAMP` and partition-friendly `DATE`.
  - Filters out corrupt or out-of-bounds ratings (only ratings between `0.5` and `5.0` are retained).
- `stg_tmdb_trending`:
  - Cleans production `budget` and gross `revenue` by converting `$0` and negative values to `NULL` to prevent skewed ROI calculations.
  - Safely parses ISO release dates (`YYYY-MM-DD`).

### 3. Marts Layer (`analytics_movies` Clustered Tables)
- `mart_movie_performance`:
  - Joins catalog movies with aggregated rating volume, mean rating, and standard deviation.
  - Enriches with TMDB budget, revenue, and gross profit.
  - Computes **Financial Return on Investment (ROI)** and **Bayesian Weighted Rating Score**.
  - Clustered on `primary_genre` and `release_year` for sub-second BI query latency.
- `mart_genre_trends`:
  - Unnests `genre_array` using `CROSS JOIN UNNEST(genre_array) AS genre`.
  - Groups by `genre` and release `decade` (e.g., 1980s, 1990s, 2000s, 2010s, 2020s).
  - Surfaces total movie volume, cumulative rating volume, average Bayesian score, total box office gross, and historical profitability rate.

### Mathematical Formulations

#### 1. Return on Investment (ROI)
$$\text{ROI} = \frac{\text{Revenue} - \text{Budget}}{\text{Budget}}$$

Implemented via BigQuery Standard SQL:
```sql
ROUND(SAFE_DIVIDE((revenue - budget), NULLIF(budget, 0)), 4) AS return_on_investment
```

#### 2. Bayesian Weighted Rating Score (IMDb Formula)
To prevent movies with only 1 or 2 perfect $5.0$ ratings from outranking classic masterpieces with thousands of ratings, we employ a Bayesian prior:
$$WR = \left(\frac{v}{v + m}\right) \cdot R + \left(\frac{m}{v + m}\right) \cdot C$$

Where:
- $v$ = Number of ratings for the movie (`rating_count`)
- $m$ = Minimum rating threshold required to be considered credible ($m = 25.0$)
- $R$ = Arithmetic average rating for the movie (`avg_rating`)
- $C$ = Mean rating across the entire catalog prior ($C \approx 3.53$)

---

## ⚡ Prerequisites

- **Docker Desktop** (v20.10+ with Docker Compose v2.0+)
- **Python 3.9+**
- **Google Cloud Platform Account** (BigQuery Sandbox is completely free, no credit card required)
- **Google AI Studio API Key** (for Gemini LLM conversational BI: [Get Free API Key](https://aistudio.google.com/app/apikey))
- *(Optional)* **TMDB API Key** (for live API extraction: [Get TMDB Key](https://www.themoviedb.org/settings/api))

---

## 🚀 Quick Start Guide

### 1. Environment & Credentials

Run the automated initializer script:
```bash
bash scripts/init_environment.sh
```

Or manually copy `.env.example`:
```bash
cp .env.example .env
```

Open `.env` and fill in your credentials:
```env
BIGQUERY_PROJECT=your-actual-gcp-project-id
BIGQUERY_LOCATION=US
BIGQUERY_DATASET_RAW=raw_movies
BIGQUERY_DATASET_ANALYTICS=analytics_movies
GOOGLE_APPLICATION_CREDENTIALS=service_account.json
GEMINI_API_KEY=your_gemini_api_key_here
TMDB_API_KEY=your_tmdb_api_key_here
AIRBYTE_CONNECTION_ID=your_airbyte_connection_id_here
```

---

### 2. Google BigQuery Sandbox Setup

1. Navigate to the [Google Cloud Console](https://console.cloud.google.com/).
2. Create a new Google Cloud Project (e.g., `movie-streaming-elt`).
3. Enable the **BigQuery API**:
   ```bash
   gcloud services enable bigquery.googleapis.com --project your-actual-gcp-project-id
   ```
4. Create a Service Account for dbt, Airflow, and the BI app:
   - Go to **IAM & Admin** ➔ **Service Accounts** ➔ **Create Service Account**.
   - Name: `movie-elt-sa`.
   - Grant the following roles:
     - `BigQuery Admin` (or `BigQuery Data Editor` + `BigQuery Job User`).
5. Generate a Key:
   - Click the newly created Service Account ➔ **Keys** tab ➔ **Add Key** ➔ **Create new key (JSON)**.
   - Save the downloaded file as `service_account.json` in the root of this repository.

---

### 3. Airbyte Ingestion Setup (or Fast-Track Seeder)

#### Option A: Fast-Track Direct BigQuery Seeder (Recommended for Quick Testing)
If you want to test dbt transformations and the Streamlit BI app immediately without waiting to configure Airbyte connectors, run our built-in ingestion script:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r bi_app/requirements.txt
python3 scripts/load_sample_data.py
```
This automatically creates the `raw_movies` BigQuery dataset, downloads MovieLens small, fetches TMDB trending data, and uploads:
- `raw_movies.movies`
- `raw_movies.ratings`
- `raw_movies.tmdb_trending`

#### Option B: Production Airbyte Ingestion
1. Start your local Airbyte instance:
   ```bash
   git clone https://github.com/airbytehq/airbyte.git
   cd airbyte && ./run-ab-platform.sh
   ```
2. Open `http://localhost:8000` (default login: `airbyte` / `password`).
3. **Configure Source 1 (MovieLens)**:
   - Type: `File (CSV)` or `S3 / GCS`.
   - Ingest `movies.csv` and `ratings.csv`.
4. **Configure Source 2 (TMDB Trending API)**:
   - Type: `HTTP / REST API`.
   - URL: `https://api.themoviedb.org/3/trending/movie/week`.
   - Authentication: Add Query Param `api_key = <TMDB_API_KEY>`.
5. **Configure Destination (Google BigQuery)**:
   - Destination Type: `Google BigQuery`.
   - Default Dataset ID: `raw_movies`.
   - Upload your `service_account.json`.
6. **Create Connection & Copy Connection ID**:
   - Set sync frequency to `@daily`.
   - Copy the Connection ID from the URL (`http://localhost:8000/workspaces/<id>/connections/<connection_id>`) into `.env`:
     ```env
     AIRBYTE_CONNECTION_ID=8a4f89d3-52c7-4e16-bb7d-a9b1c2d3e4f5
     ```

---

### 4. Apache Airflow Orchestration

Launch the local orchestration stack:
```bash
docker compose up -d
```

Verify that the containers are healthy:
```bash
docker compose ps
```

Access the Airflow Web UI:
- **URL**: [http://localhost:8080](http://localhost:8080)
- **Username**: `admin`
- **Password**: `admin`

#### DAG Details: `movie_elt_pipeline`
The DAG coordinates three sequential stages:
1. `trigger_airbyte_sync`: Calls `AirbyteTriggerSyncOperator` using the parameterized `AIRBYTE_CONNECTION_ID`.
2. `dbt_run`: Executes all staging models and marts within `dbt_movies`.
3. `dbt_test`: Runs schema validation tests.

To manually trigger the pipeline:
1. Unpause the `movie_elt_pipeline` toggle.
2. Click **Trigger DAG** (▶️).

---

### 5. dbt Core Transformation & Testing

You can also run dbt locally from the command line:

```bash
cd dbt_movies

# 1. Install dbt dependencies (dbt_utils)
dbt deps --profiles-dir .

# 2. Test BigQuery connectivity
dbt debug --profiles-dir .

# 3. Run all staging and mart models
dbt run --profiles-dir .

# 4. Run data quality tests
dbt test --profiles-dir .
```

Generate and view documentation:
```bash
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

---

### 6. Streamlit Conversational BI Web App

Launch the Conversational BI web interface:
```bash
streamlit run bi_app/app.py
```

The web app will open at [http://localhost:8501](http://localhost:8501).

#### Key Features:
- 🗣 **Natural Language to SQL**: Converts questions into BigQuery Standard SQL using Google Gemini.
- 🛡 **Security Guardrails**: Enforces read-only execution (strictly `SELECT` statements).
- 📈 **Dynamic Plotly Visualizations**:
  - Horizontal bar charts for categorical metrics (ratings, ROI, popularity).
  - Multi-series line charts for time-series decade trends.
  - Correlation scatter plots (budget vs revenue, rating vs ROI).
- 🧠 **AI Takeaways**: Generates executive-level bullet points summarizing findings.
- 📥 **CSV Export**: One-click download of query result datasets.

#### Try These Example Queries:
- *"What are the top 5 highest-rated Sci-Fi movies with at least 100 ratings?"*
- *"Show the top 10 movies by Bayesian weighted rating score."*
- *"Compare average ROI and total revenue across genres in the 2010s."*
- *"Which genres have the highest profitability rate with at least 50 movies?"*
- *"Show the budget, revenue, and ROI for the top 10 most popular movies."*

---

## 🧪 Data Quality & Testing

Data quality is enforced using `dbt_utils` and standard dbt assertions defined in `dbt_movies/models/marts/schema.yml`:

| Model | Column | Test Type | Purpose |
| :--- | :--- | :--- | :--- |
| `mart_movie_performance` | `movie_id` | `unique`, `not_null` | Enforces primary key integrity |
| `mart_movie_performance` | `title` | `not_null` | Prevents unnamed movie records |
| `mart_movie_performance` | `avg_rating` | `accepted_range [0.0, 5.0]` | Validates rating scale bounds |
| `mart_movie_performance` | `bayesian_weighted_score` | `accepted_range [0.0, 5.0]` | Validates Bayesian formula score |
| `mart_movie_performance` | `financial_status` | `accepted_values` | Restricts to `['Profitable', 'Loss', 'Break-Even', 'No Financial Data']` |
| `mart_genre_trends` | `(genre, decade)` | `unique_combination_of_columns` | Guarantees uniqueness per genre-decade cohort |
| `mart_genre_trends` | `total_movies` | `accepted_range [>= 1]` | Ensures aggregated groups have non-zero volume |

---

## 🛠 Troubleshooting & FAQ

#### Q: BigQuery returns `Dataset not found` or `Access Denied`?
- Ensure the Service Account in `service_account.json` has `BigQuery Admin` or `BigQuery Data Editor` and `BigQuery Job User` roles on your GCP Project.
- Check that `BIGQUERY_PROJECT` in `.env` matches your Google Cloud Project ID.

#### Q: How can I run dbt inside Docker without a local Python setup?
- The Airflow Docker container already has `dbt-bigquery` installed. You can execute:
  ```bash
  docker compose run --rm airflow-webserver bash -c "cd /opt/airflow/dbt_movies && dbt run --profiles-dir ."
  ```

#### Q: Airbyte connection times out or cannot reach host?
- In Docker on macOS/Windows, use `AIRBYTE_HOST=host.docker.internal` to allow Airflow to access Airbyte running on the host.

---

## 📜 License
This project is open-source and available under the [MIT License](LICENSE).

