"""
Streamlit Conversational BI Application
Natural Language to BigQuery SQL powered by Google Gemini and LangChain.
Interrogates the 'analytics_movies' dataset and visualizes movie performance metrics.
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

# LangChain & Gemini imports (kept for Ollama support)
from langchain_core.messages import HumanMessage, SystemMessage

# Google GenAI (new SDK — supports AQ. format keys)
try:
    from google import genai as google_genai
    _GENAI_AVAILABLE = True
except ImportError:
    _GENAI_AVAILABLE = False

# Fallback to old LangChain Gemini if new SDK not available
try:
    from langchain_google_genai import ChatGoogleGenerativeAI
    _LANGCHAIN_GENAI_AVAILABLE = True
except ImportError:
    _LANGCHAIN_GENAI_AVAILABLE = False

# BigQuery & SQLAlchemy
from google.cloud import bigquery
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Page Configuration & Styling
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Movie Streaming Conversational BI",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom modern CSS
st.markdown(
    """
    <style>
    .main-title {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1E293B;
        margin-bottom: 0.2rem;
    }
    .sub-title {
        font-size: 1.05rem;
        color: #64748B;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background-color: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 12px 16px;
        margin-bottom: 8px;
    }
    .stCodeBlock {
        border-radius: 6px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Load Environment Variables
# ---------------------------------------------------------------------------
# Search parent directory or root for .env
env_path = Path(__file__).resolve().parent.parent / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    load_dotenv()

DEFAULT_PROJECT = os.getenv("BIGQUERY_PROJECT", "")
DEFAULT_DATASET = os.getenv("BIGQUERY_DATASET_ANALYTICS", "analytics_movies")
DEFAULT_LOCATION = os.getenv("BIGQUERY_LOCATION", "US")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY", "")

# ---------------------------------------------------------------------------
# Dataset Schema Definitions for LLM Grounding
# ---------------------------------------------------------------------------
SCHEMA_METADATA = f"""
You have access to Google BigQuery Standard SQL tables in the dataset `{DEFAULT_DATASET}`:

1. Table: `{{project}}.{DEFAULT_DATASET}.mart_movie_performance`
   Description: Granular movie-level analytics combining MovieLens metadata, rating statistics, and TMDB financials.
   Columns:
     - `movie_id` (INT64): Unique MovieLens identifier
     - `title` (STRING): Full movie title with release year (e.g., 'Toy Story (1995)')
     - `clean_title` (STRING): Normalized title without year
     - `release_year` (INT64): Four-digit theatrical release year
     - `genre_array` (ARRAY<STRING>): Array of genre tags (e.g. ['Adventure', 'Animation', 'Comedy'])
     - `raw_genres` (STRING): Pipe-delimited genres
     - `primary_genre` (STRING): Primary genre classification
     - `rating_count` (INT64): Total count of user ratings received
     - `avg_rating` (FLOAT64): Arithmetic average user rating (0.5 to 5.0 scale)
     - `min_rating` (FLOAT64): Lowest user rating submitted
     - `max_rating` (FLOAT64): Highest user rating submitted
     - `rating_stddev` (FLOAT64): Standard deviation of user ratings
     - `bayesian_weighted_score` (FLOAT64): Bayesian weighted score formula: (v/(v+25))*R + (25/(v+25))*C
     - `budget` (NUMERIC): Production budget in USD (NULL if unknown)
     - `revenue` (NUMERIC): Global box office revenue in USD (NULL if unknown)
     - `gross_profit` (NUMERIC): revenue - budget
     - `return_on_investment` (FLOAT64): ROI computed as (revenue - budget) / NULLIF(budget, 0)
     - `financial_status` (STRING): 'Profitable', 'Loss', 'Break-Even', 'No Financial Data'
     - `rating_tier` (STRING): 'Masterpiece (>=4.5)', 'Acclaimed (4.0 - 4.49)', 'Favorable (3.5 - 3.99)', etc.
     - `popularity` (FLOAT64): TMDB weekly popularity score

2. Table: `{{project}}.{DEFAULT_DATASET}.mart_genre_trends`
   Description: Aggregated time-series and genre trends grouped by genre and 10-year release decade.
   Columns:
     - `genre` (STRING): Genre name (e.g., 'Action', 'Sci-Fi', 'Drama', 'Comedy')
     - `decade` (INT64): Decade start year (e.g., 1980, 1990, 2000, 2010)
     - `decade_label` (STRING): Formatted decade label (e.g., '1980s', '1990s', '2010s')
     - `total_movies` (INT64): Total distinct movies released in this genre and decade
     - `total_ratings` (INT64): Cumulative rating volume
     - `avg_movie_rating` (FLOAT64): Average movie rating in this cohort
     - `avg_bayesian_score` (FLOAT64): Mean Bayesian weighted score
     - `movies_with_financials` (INT64): Number of movies with known budget and revenue
     - `total_budget` (NUMERIC): Aggregate production budget
     - `total_revenue` (NUMERIC): Aggregate gross box office revenue
     - `total_gross_profit` (NUMERIC): Aggregate profit
     - `avg_return_on_investment` (FLOAT64): Mean ROI
     - `profitable_movies_count` (INT64): Number of profitable movies
     - `profitability_rate` (FLOAT64): Ratio of profitable movies to total movies with financial records
"""

SYSTEM_PROMPT_TEMPLATE = f"""
You are an expert Data Engineer and Analytics Specialist for a Movie Streaming Platform.
Your task is to convert natural language business questions into precise, production-grade Google BigQuery Standard SQL queries.

{SCHEMA_METADATA}

STRICT SQL RULES:
1. Always generate valid Google BigQuery Standard SQL.
2. Fully qualify all tables: `{{project}}.{DEFAULT_DATASET}.mart_movie_performance` or `{{project}}.{DEFAULT_DATASET}.mart_genre_trends`.
3. ONLY generate `SELECT` queries.
4. In `mart_movie_performance`: To match genre (e.g. Sci-Fi), check `(LOWER(primary_genre) = 'sci-fi' OR LOWER(raw_genres) LIKE '%sci-fi%')`.
5. In `mart_genre_trends`: 
   - Column for ROI is `avg_return_on_investment` (do NOT use return_on_investment).
   - If you SELECT `decade_label`, you MUST include `decade_label` in `GROUP BY decade, decade_label`.
6. Enclose your final SQL query inside a single ```sql ... ``` markdown code block.

FEW-SHOT EXAMPLES:
Question: What are the top 5 highest-rated Sci-Fi movies with at least 100 ratings?
```sql
SELECT clean_title, release_year, primary_genre, avg_rating, rating_count, bayesian_weighted_score
FROM `{{project}}.{DEFAULT_DATASET}.mart_movie_performance`
WHERE (LOWER(primary_genre) = 'sci-fi' OR LOWER(raw_genres) LIKE '%sci-fi%') AND rating_count >= 100
ORDER BY avg_rating DESC
LIMIT 5
```

Question: Show the top 10 movies by Bayesian weighted rating score with their release year and ratings.
```sql
SELECT clean_title, release_year, primary_genre, avg_rating, rating_count, bayesian_weighted_score
FROM `{{project}}.{DEFAULT_DATASET}.mart_movie_performance`
WHERE rating_count >= 25
ORDER BY bayesian_weighted_score DESC
LIMIT 10
```

Question: Show the budget, revenue, and ROI for the top 10 most popular movies with known financials.
```sql
SELECT clean_title, release_year, budget, revenue, return_on_investment, popularity
FROM `{{project}}.{DEFAULT_DATASET}.mart_movie_performance`
WHERE budget IS NOT NULL AND revenue IS NOT NULL
ORDER BY popularity DESC
LIMIT 10
```

Question: Compare average ROI and total revenue across genres in the 2010s decade.
```sql
SELECT genre, decade_label, avg_return_on_investment, total_revenue, total_movies
FROM `{{project}}.{DEFAULT_DATASET}.mart_genre_trends`
WHERE decade = 2010
ORDER BY total_revenue DESC
LIMIT 15
```

Question: Show the average movie rating and total movies produced across decades since 1970.
```sql
SELECT decade_label, ROUND(AVG(avg_movie_rating), 2) AS avg_movie_rating, SUM(total_movies) AS total_movies
FROM `{{project}}.{DEFAULT_DATASET}.mart_genre_trends`
WHERE decade >= 1970
GROUP BY decade, decade_label
ORDER BY decade ASC
LIMIT 15
```

Question: Which genres have the highest profitability rate with at least 50 movies in the catalog?
```sql
SELECT genre, ROUND(AVG(profitability_rate), 4) AS profitability_rate, SUM(total_movies) AS total_movies
FROM `{{project}}.{DEFAULT_DATASET}.mart_genre_trends`
GROUP BY genre
HAVING SUM(total_movies) >= 50
ORDER BY profitability_rate DESC
LIMIT 10
```
"""

# ---------------------------------------------------------------------------
# BigQuery & LLM Helper Functions
# ---------------------------------------------------------------------------
@st.cache_resource
def get_bigquery_client(project: str, location: str) -> Optional[bigquery.Client]:
    """Initializes and caches the BigQuery client."""
    try:
        if project:
            return bigquery.Client(project=project, location=location)
        return bigquery.Client(location=location)
    except Exception as e:
        st.sidebar.error(f"BigQuery Client Initialization Error: {e}")
        return None

def fix_common_sql_mistakes(sql: str) -> str:
    """Auto-corrects common small-LLM BigQuery SQL mistakes."""
    if not sql:
        return sql
    
    # 1. In mart_genre_trends: fix return_on_investment -> avg_return_on_investment
    if "mart_genre_trends" in sql:
        sql = re.sub(r"\bAVG\s*\(\s*return_on_investment\s*\)", "avg_return_on_investment", sql, flags=re.IGNORECASE)
        sql = re.sub(r"(?<!avg_)\breturn_on_investment\b", "avg_return_on_investment", sql, flags=re.IGNORECASE)
        
        # 2. Fix GROUP BY decade when decade_label is in SELECT
        if "decade_label" in sql and "GROUP BY" in sql:
            parts = sql.split("GROUP BY")
            if len(parts) > 1 and "decade_label" not in parts[1]:
                sql = re.sub(r"GROUP BY\s+decade\b", "GROUP BY decade, decade_label", sql, flags=re.IGNORECASE)
            
    # 3. In mart_movie_performance: expand primary_genre check to also check raw_genres
    # MovieLens movies often have Action as primary and Sci-Fi as secondary
    if "mart_movie_performance" in sql and "sci-fi" in sql.lower():
        if "raw_genres" not in sql:
            sql = re.sub(
                r"LOWER\s*\(\s*primary_genre\s*\)\s*=\s*'sci-fi'",
                "(LOWER(primary_genre) = 'sci-fi' OR LOWER(raw_genres) LIKE '%sci-fi%')",
                sql,
                flags=re.IGNORECASE
            )
            sql = re.sub(
                r"primary_genre\s*=\s*'Sci-Fi'",
                "(primary_genre = 'Sci-Fi' OR raw_genres LIKE '%Sci-Fi%')",
                sql
            )
    return sql

def execute_bigquery_query(client: bigquery.Client, query: str) -> Tuple[Optional[pd.DataFrame], float, Optional[str]]:
    """Executes a SQL query on BigQuery and returns the dataframe and latency."""
    start_time = time.time()
    try:
        # Enforce read-only query safety
        sanitized = re.sub(r"--.*?$|\/\*.*?\*\/", "", query, flags=re.MULTILINE).strip()
        prohibited_verbs = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "TRUNCATE", "CREATE"]
        first_token = sanitized.split()[0].upper() if sanitized.split() else ""
        if first_token in prohibited_verbs or any(re.search(rf"\b{verb}\b", sanitized, re.IGNORECASE) for verb in prohibited_verbs):
            return None, 0.0, "Security Guardrail Triggered: Only SELECT queries are permitted."

        query_job = client.query(query)
        df = query_job.to_dataframe()
        elapsed = time.time() - start_time
        return df, elapsed, None
    except Exception as e:
        elapsed = time.time() - start_time
        return None, elapsed, str(e)

def generate_sql_with_ollama(
    user_query: str,
    project_id: str,
    dataset: str,
    model_name: str = "llama3.2:3b",
    ollama_url: str = "http://localhost:11434",
) -> Tuple[str, str]:
    """Generates BigQuery Standard SQL using local Ollama model."""
    try:
        formatted_system_prompt = SYSTEM_PROMPT_TEMPLATE.replace("{project}", project_id)
        payload = {
            "model": model_name,
            "messages": [
                {"role": "system", "content": formatted_system_prompt},
                {"role": "user", "content": f"Question: {user_query}\nGenerate BigQuery SQL:"}
            ],
            "stream": False,
            "options": {"temperature": 0.0}
        }
        req = urllib.request.Request(
            f"{ollama_url.rstrip('/')}/api/chat",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode())
            content = data.get("message", {}).get("content", "")

        sql_match = re.search(r"```(?:sql)?\s*([\s\S]*?)\s*```", content, re.IGNORECASE)
        if sql_match:
            generated_sql = sql_match.group(1).strip()
        else:
            generated_sql = content.strip()

        return generated_sql, ""
    except Exception as e:
        return "", f"Ollama Error: {str(e)}. Make sure Ollama is running (`ollama serve`)."

def generate_insights_with_ollama(
    user_query: str,
    df: pd.DataFrame,
    model_name: str = "llama3.2:3b",
    ollama_url: str = "http://localhost:11434",
) -> str:
    """Generates business insights using local Ollama model."""
    if df is None or df.empty:
        return ""
    try:
        sample_data = df.head(10).to_markdown(index=False)
        prompt = (
            f"User asked: '{user_query}'\n\n"
            f"Here is the top data returned from BigQuery:\n{sample_data}\n\n"
            "Provide 2 or 3 concise, executive-level key takeaways or findings based strictly on this data. "
            "Use bullet points with emojis."
        )
        payload = {
            "model": model_name,
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "stream": False,
            "options": {"temperature": 0.2}
        }
        req = urllib.request.Request(
            f"{ollama_url.rstrip('/')}/api/chat",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode())
            return data.get("message", {}).get("content", "").strip()
    except Exception:
        return ""

def generate_sql_with_gemini(
    user_query: str,
    project_id: str,
    dataset: str,
    gemini_key: str,
    model_name: str = "gemini-3.8-flash",
) -> Tuple[str, str]:
    """Generates BigQuery Standard SQL using Google Gemini (new google-genai SDK first, LangChain fallback)."""
    if not gemini_key:
        return "", "GEMINI_API_KEY is not set. Please provide it in the sidebar or .env file."

    formatted_system_prompt = SYSTEM_PROMPT_TEMPLATE.replace("{project}", project_id)
    full_prompt = (
        formatted_system_prompt
        + f"\n\nQuestion: {user_query}\nGenerate BigQuery SQL:"
    )

    clean_model = model_name.strip()
    if clean_model.startswith("models/"):
        clean_model = clean_model[7:]

    # --- Try new google-genai SDK first (supports AQ. format keys) ---
    if _GENAI_AVAILABLE:
        try:
            client = google_genai.Client(api_key=gemini_key)
            response = client.models.generate_content(
                model=clean_model,
                contents=full_prompt,
            )
            content = response.text or ""
            sql_match = re.search(r"```(?:sql)?\s*([\s\S]*?)\s*```", content, re.IGNORECASE)
            generated_sql = sql_match.group(1).strip() if sql_match else content.strip()
            if generated_sql:
                return generated_sql, ""
        except Exception as e:
            err_str = str(e)
            # If it's a model not found / auth error, return immediately instead of hanging in retries
            if any(k in err_str.lower() for k in ["404", "not found", "403", "permission", "invalid"]):
                return "", f"Gemini API Error ({clean_model}): {err_str}"

    # --- Fallback: LangChain google-genai (with max_retries=1 to prevent freezing) ---
    if _LANGCHAIN_GENAI_AVAILABLE:
        try:
            from langchain_google_genai import ChatGoogleGenerativeAI
            llm = ChatGoogleGenerativeAI(
                model=clean_model,
                google_api_key=gemini_key,
                temperature=0.0,
                max_output_tokens=1024,
                max_retries=1,
                request_timeout=20,
            )
            messages = [
                SystemMessage(content=formatted_system_prompt),
                HumanMessage(content=f"Question: {user_query}\nGenerate BigQuery SQL:"),
            ]
            response = llm.invoke(messages)
            content = response.content
            sql_match = re.search(r"```(?:sql)?\s*([\s\S]*?)\s*```", content, re.IGNORECASE)
            generated_sql = sql_match.group(1).strip() if sql_match else content.strip()
            return generated_sql, ""
        except Exception as e:
            return "", f"Gemini SQL Generation Error: {str(e)}"

    return "", "No Gemini SDK available. Run: pip install google-genai"

def generate_insights_with_gemini(
    user_query: str,
    df: pd.DataFrame,
    gemini_key: str,
    model_name: str = "gemini-3.8-flash",
) -> str:
    """Generates business insights using Google Gemini."""
    if not gemini_key or df is None or df.empty:
        return ""

    sample_data = df.head(10).to_markdown(index=False)
    prompt = (
        f"User asked: '{user_query}'\n\n"
        f"Here is the top data returned from BigQuery:\n{sample_data}\n\n"
        "Provide 2 or 3 concise, executive-level key takeaways or findings based strictly on this data. "
        "Use bullet points with emojis."
    )

    clean_model = model_name.strip()
    if clean_model.startswith("models/"):
        clean_model = clean_model[7:]

    if _GENAI_AVAILABLE:
        try:
            client = google_genai.Client(api_key=gemini_key)
            response = client.models.generate_content(model=clean_model, contents=prompt)
            return response.text.strip() if response.text else ""
        except Exception:
            pass

    if _LANGCHAIN_GENAI_AVAILABLE:
        try:
            from langchain_google_genai import ChatGoogleGenerativeAI
            llm = ChatGoogleGenerativeAI(
                model=clean_model,
                google_api_key=gemini_key,
                temperature=0.2,
                max_output_tokens=512,
                max_retries=1,
                request_timeout=20,
            )
            response = llm.invoke([HumanMessage(content=prompt)])
            return response.content.strip()
        except Exception:
            pass

    return ""

# ---------------------------------------------------------------------------
# Automatic Plotly Chart Recommendation
# ---------------------------------------------------------------------------
def render_dynamic_chart(df: pd.DataFrame, title: str):
    """Automatically determines and renders the best Plotly chart for the dataset."""
    if df.empty or len(df.columns) < 2:
        return

    # Identify categorical, temporal, and numeric columns
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    string_cols = df.select_dtypes(include=["object", "string", "category"]).columns.tolist()

    # Detect decade or temporal column
    temporal_cols = [c for c in df.columns if any(k in c.lower() for k in ["decade", "year", "date"])]

    try:
        # Case 1: Temporal / Trend chart (Decade or Year + Numeric metric)
        if temporal_cols and numeric_cols:
            x_col = temporal_cols[0]
            y_col = numeric_cols[0]
            color_col = string_cols[0] if string_cols and string_cols[0] != x_col else None
            
            fig = px.line(
                df,
                x=x_col,
                y=y_col,
                color=color_col,
                markers=True,
                title=f"📈 Trend Analysis: {y_col.replace('_', ' ').title()} by {x_col.replace('_', ' ').title()}",
                template="plotly_white",
            )
            fig.update_layout(height=420, margin=dict(l=20, r=20, t=50, b=20))
            st.plotly_chart(fig, use_container_width=True)
            return

        # Case 2: Categorical comparison (Bar Chart: Title/Genre + Numeric metric)
        if string_cols and numeric_cols:
            cat_col = "clean_title" if "clean_title" in string_cols else ("title" if "title" in string_cols else string_cols[0])
            num_col = numeric_cols[0]

            # Sort descending for better bar visual
            df_plot = df.sort_values(by=num_col, ascending=True).tail(20)

            fig = px.bar(
                df_plot,
                x=num_col,
                y=cat_col,
                orientation="h",
                text=num_col,
                title=f"📊 Comparison: {num_col.replace('_', ' ').title()} by {cat_col.replace('_', ' ').title()}",
                template="plotly_white",
                color=num_col,
                color_continuous_scale="Blues",
            )
            fig.update_traces(texttemplate="%{text:.2s}", textposition="outside")
            fig.update_layout(height=max(380, len(df_plot) * 26), margin=dict(l=20, r=20, t=50, b=20))
            st.plotly_chart(fig, use_container_width=True)
            return

        # Case 3: Correlation / Multi-metric (Scatter Plot: Budget vs Revenue or Rating vs ROI)
        if len(numeric_cols) >= 2:
            x_col, y_col = numeric_cols[0], numeric_cols[1]
            hover_col = string_cols[0] if string_cols else None
            fig = px.scatter(
                df,
                x=x_col,
                y=y_col,
                hover_name=hover_col,
                title=f"🔍 Correlation: {y_col.replace('_', ' ').title()} vs {x_col.replace('_', ' ').title()}",
                template="plotly_white",
                color_discrete_sequence=["#2563EB"],
            )
            fig.update_layout(height=420, margin=dict(l=20, r=20, t=50, b=20))
            st.plotly_chart(fig, use_container_width=True)
    except Exception as chart_err:
        st.caption(f"Chart preview omitted: {chart_err}")

# ---------------------------------------------------------------------------
# Sidebar UI & Configurations
# ---------------------------------------------------------------------------
with st.sidebar:
    st.image("https://raw.githubusercontent.com/dbt-labs/dbt-core/main/core/dbt/include/global_project/dbt.png", width=60)
    st.title("Settings & Config")
    st.markdown("---")

    gcp_project = st.text_input(
        "GCP Project ID",
        value=DEFAULT_PROJECT,
        help="Your Google Cloud Platform Project hosting BigQuery.",
    )
    bq_dataset = st.text_input(
        "Analytics Dataset",
        value=DEFAULT_DATASET,
        help="Target dataset populated by dbt Core.",
    )
    bq_location = st.selectbox(
        "BigQuery Location",
        options=["US", "EU", "asia-northeast1", "us-central1"],
        index=0,
    )

    st.markdown("---")
    st.subheader("LLM Provider Settings")
    llm_provider = st.selectbox(
        "LLM Provider",
        options=["Ollama (Local)", "Google Gemini (Cloud)"],
        index=0,
    )

    if llm_provider == "Ollama (Local)":
        model_choice = st.text_input(
            "Ollama Model Name",
            value="llama3.2:3b",
            help="Model installed in Ollama (e.g., llama3.2:3b, llama3.2:1b, llama3)",
        )
        ollama_url = st.text_input(
            "Ollama Base URL",
            value="http://localhost:11434",
            help="Local Ollama instance URL",
        )
        api_key_input = "local_ollama"
    else:
        api_key_input = st.text_input(
            "Gemini API Key",
            value=GEMINI_API_KEY,
            type="password",
            help="Google AI Studio Gemini API Key.",
        )
        model_choice = st.selectbox(
            "Gemini Model",
            options=["gemini-3.8-flash", "gemini-2.5-flash", "gemini-2.0-flash"],
            index=0,
        )
        ollama_url = ""

    st.markdown("---")
    st.subheader("Dataset Schema")
    with st.expander("Explore `mart_movie_performance`"):
        st.markdown(
            """
            - **Identifiers**: `movie_id`, `title`, `clean_title`, `release_year`
            - **Ratings**: `avg_rating`, `rating_count`, `bayesian_weighted_score`, `rating_tier`
            - **Financials**: `budget`, `revenue`, `gross_profit`, `return_on_investment`
            - **Metadata**: `primary_genre`, `genre_array`, `popularity`
            """
        )
    with st.expander("Explore `mart_genre_trends`"):
        st.markdown(
            """
            - **Dimensions**: `genre`, `decade`, `decade_label`
            - **Aggregations**: `total_movies`, `total_ratings`, `avg_movie_rating`, `avg_bayesian_score`
            - **Economics**: `total_budget`, `total_revenue`, `avg_return_on_investment`, `profitability_rate`
            """
        )

# ---------------------------------------------------------------------------
# Main Application Header & Prompt Shortcuts
# ---------------------------------------------------------------------------
st.markdown('<div class="main-title">🎬 Movie Streaming Conversational BI</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub-title">Ask questions in plain English. Powered by Google Gemini, dbt Core & BigQuery.</div>',
    unsafe_allow_html=True,
)

# Suggested queries
st.markdown("##### 💡 Suggested Questions")
col1, col2, col3 = st.columns(3)

query_to_run = None
with col1:
    if st.button("🌟 Top 5 Sci-Fi Movies (Ratings >= 100)", use_container_width=True):
        query_to_run = "What are the top 5 highest-rated Sci-Fi movies with at least 100 ratings?"
    if st.button("🏆 Top 10 by Bayesian Score", use_container_width=True):
        query_to_run = "Show the top 10 movies by Bayesian weighted rating score with their release year and ratings."

with col2:
    if st.button("💰 Budget vs Revenue: Top 10 Popular", use_container_width=True):
        query_to_run = "Show the budget, revenue, and ROI for the top 10 most popular movies with known financials."
    if st.button("📈 ROI & Revenue by Genre (2010s)", use_container_width=True):
        query_to_run = "Compare average ROI and total revenue across genres in the 2010s decade."

with col3:
    if st.button("📅 Rating Evolution by Decade", use_container_width=True):
        query_to_run = "Show the average movie rating and total movies produced across decades since 1970."
    if st.button("🚀 Most Profitable Genres (>= 50 movies)", use_container_width=True):
        query_to_run = "Which genres have the highest profitability rate with at least 50 movies in the catalog?"

# ---------------------------------------------------------------------------
# User Input Form
# ---------------------------------------------------------------------------
st.markdown("---")
user_input = st.chat_input("Ask a question about movies, ratings, genres, or box office ROI...")

active_query = query_to_run or user_input

# ---------------------------------------------------------------------------
# Query Processing & Visualization Execution
# ---------------------------------------------------------------------------
if active_query:
    st.chat_message("user").markdown(f"**{active_query}**")

    # Validation check
    if not gcp_project:
        st.error("⚠️ Please specify your GCP Project ID in the sidebar or `.env` file to execute queries.")
    elif llm_provider == "Google Gemini (Cloud)" and not api_key_input:
        st.error("⚠️ Please set your Gemini API Key in the sidebar or `GEMINI_API_KEY` in `.env`.")
    else:
        with st.chat_message("assistant"):
            spinner_msg = f"🦙 Ollama ({model_choice}) is translating your question into BigQuery SQL..." if llm_provider == "Ollama (Local)" else "🤖 Gemini is translating your question into BigQuery SQL..."
            with st.spinner(spinner_msg):
                if llm_provider == "Ollama (Local)":
                    sql_query, gen_err = generate_sql_with_ollama(
                        user_query=active_query,
                        project_id=gcp_project,
                        dataset=bq_dataset,
                        model_name=model_choice,
                        ollama_url=ollama_url,
                    )
                else:
                    sql_query, gen_err = generate_sql_with_gemini(
                        user_query=active_query,
                        project_id=gcp_project,
                        dataset=bq_dataset,
                        gemini_key=api_key_input,
                        model_name=model_choice,
                    )

            if gen_err:
                st.error(gen_err)
            else:
                # Auto-heal common small-LLM SQL slips
                sql_query = fix_common_sql_mistakes(sql_query)

                # Show generated SQL in an expander
                with st.expander("🔍 View Generated BigQuery SQL", expanded=True):
                    st.code(sql_query, language="sql")

                # Execute query on BigQuery
                with st.spinner("⚡ Executing query on Google BigQuery..."):
                    client = get_bigquery_client(gcp_project, bq_location)
                    if client is None:
                        st.error("Could not establish connection to BigQuery. Verify credentials.")
                    else:
                        df, elapsed, exec_err = execute_bigquery_query(client, sql_query)

                        if exec_err:
                            st.error(f"❌ BigQuery Execution Error:\n\n`{exec_err}`")
                            st.info("💡 Hint: Ensure dbt models have been built in BigQuery dataset: `" f"{gcp_project}.{bq_dataset}`.")
                        elif df is None or df.empty:
                            st.warning("Query executed successfully, but returned 0 rows matching your criteria.")
                        else:
                            # Metric summary row
                            m1, m2, m3 = st.columns(3)
                            m1.metric("Rows Returned", f"{len(df):,}")
                            m2.metric("Execution Latency", f"{elapsed:.2f}s")
                            m3.metric("Columns", len(df.columns))

                            # Results Data Table
                            st.markdown("#### 📋 Query Results")
                            st.dataframe(df, use_container_width=True)

                            # Plotly Visualizations
                            st.markdown("#### 📊 Interactive Visualization")
                            render_dynamic_chart(df, active_query)

                            # AI Executive Summary / Insights
                            with st.spinner("✨ Generating AI Takeaways..."):
                                if llm_provider == "Ollama (Local)":
                                    insights = generate_insights_with_ollama(
                                        active_query,
                                        df,
                                        model_name=model_choice,
                                        ollama_url=ollama_url,
                                    )
                                else:
                                    insights = generate_insights_with_gemini(
                                        active_query,
                                        df,
                                        gemini_key=api_key_input,
                                        model_name=model_choice,
                                    )
                                if insights:
                                    st.markdown("#### 🧠 AI Insights")
                                    st.markdown(insights)

                            # Export CSV button
                            csv = df.to_csv(index=False).encode("utf-8")
                            st.download_button(
                                label="📥 Download Results as CSV",
                                data=csv,
                                file_name="movie_analytics_query_result.csv",
                                mime="text/csv",
                            )

