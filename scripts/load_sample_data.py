#!/usr/bin/env python3
"""
Seed Script: Load MovieLens Small and TMDB Sample Data into Google BigQuery (raw_movies).
Useful for immediate testing and verification without requiring Airbyte setup.
"""

from __future__ import annotations

import io
import json
import os
import sys
import urllib.request
import zipfile
from pathlib import Path
from typing import Dict, List

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Load environment
root_dir = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=root_dir / ".env")

PROJECT_ID = os.getenv("BIGQUERY_PROJECT")
DATASET_ID = os.getenv("BIGQUERY_DATASET_RAW", "raw_movies")
LOCATION = os.getenv("BIGQUERY_LOCATION", "US")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

MOVIELENS_URL = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"

def get_client() -> bigquery.Client:
    """Initializes and returns a BigQuery Client."""
    if not PROJECT_ID:
        print("ERROR: BIGQUERY_PROJECT environment variable is not set in .env")
        sys.exit(1)
    return bigquery.Client(project=PROJECT_ID, location=LOCATION)

def ensure_dataset(client: bigquery.Client, dataset_name: str) -> None:
    """Ensures that the raw_movies BigQuery dataset exists."""
    dataset_ref = f"{PROJECT_ID}.{dataset_name}"
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset '{dataset_ref}' already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = LOCATION
        dataset.description = "Raw ingested data for Movie Streaming ELT Pipeline"
        client.create_dataset(dataset)
        print(f"Successfully created BigQuery dataset '{dataset_ref}'.")

def download_movielens() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Downloads and extracts MovieLens Small dataset in-memory."""
    print(f"Downloading MovieLens Small dataset from {MOVIELENS_URL}...")
    req = urllib.request.Request(MOVIELENS_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        zip_bytes = resp.read()

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        with z.open("ml-latest-small/movies.csv") as f_movies:
            df_movies = pd.read_csv(f_movies)
        with z.open("ml-latest-small/ratings.csv") as f_ratings:
            df_ratings = pd.read_csv(f_ratings)

    print(f"Loaded MovieLens: {len(df_movies):,} movies, {len(df_ratings):,} ratings.")
    return df_movies, df_ratings

def get_tmdb_trending_data(api_key: Optional[str]) -> pd.DataFrame:
    """Fetches trending movies from TMDB API or produces representative sample records."""
    if api_key and api_key.strip():
        try:
            print("Fetching weekly trending movies from TMDB API (/trending/movie/week)...")
            url = f"https://api.themoviedb.org/3/trending/movie/week?api_key={api_key.strip()}"
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
                results = data.get("results", [])
                
            records = []
            for item in results:
                movie_id = item.get("id")
                # Query movie details for budget and revenue if possible
                budget, revenue = None, None
                try:
                    det_url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={api_key.strip()}"
                    with urllib.request.urlopen(det_url, timeout=5) as det_resp:
                        det = json.loads(det_resp.read().decode())
                        budget = det.get("budget") or None
                        revenue = det.get("revenue") or None
                except Exception:
                    pass

                records.append({
                    "id": movie_id,
                    "title": item.get("title") or item.get("name"),
                    "overview": item.get("overview"),
                    "popularity": item.get("popularity"),
                    "vote_average": item.get("vote_average"),
                    "vote_count": item.get("vote_count"),
                    "release_date": item.get("release_date"),
                    "budget": budget,
                    "revenue": revenue,
                })
            df = pd.DataFrame(records)
            print(f"Fetched {len(df)} trending movies from TMDB.")
            return df
        except Exception as e:
            print(f"Warning: Failed to fetch live TMDB API: {e}. Falling back to sample trending dataset.")

    # High-quality representative trending sample
    sample_tmdb = [
        {"id": 862, "title": "Toy Story", "overview": "A cowboy doll is profoundly threatened and jealous when a new spaceman figure supplants him.", "popularity": 115.4, "vote_average": 8.0, "vote_count": 17800, "release_date": "1995-10-30", "budget": 30000000, "revenue": 373554033},
        {"id": 8844, "title": "Jumanji", "overview": "When two kids find and play a magical board game, they release a man trapped for decades.", "popularity": 78.2, "vote_average": 7.2, "vote_count": 10200, "release_date": "1995-12-15", "budget": 65000000, "revenue": 262797249},
        {"id": 949, "title": "Heat", "overview": "A group of high-end professional thieves start to feel the police closing in.", "popularity": 64.9, "vote_average": 7.9, "vote_count": 7100, "release_date": "1995-12-15", "budget": 60000000, "revenue": 187436818},
        {"id": 11, "title": "Star Wars", "overview": "Princess Leia is captured and Luke Skywalker embarks on a journey.", "popularity": 132.8, "vote_average": 8.2, "vote_count": 19500, "release_date": "1977-05-25", "budget": 11000000, "revenue": 775398007},
        {"id": 1891, "title": "The Empire Strikes Back", "overview": "The epic saga continues as Luke Skywalker trains with Jedi Master Yoda.", "popularity": 89.5, "vote_average": 8.4, "vote_count": 16400, "release_date": "1980-05-20", "budget": 18000000, "revenue": 538400000},
        {"id": 27205, "title": "Inception", "overview": "Cobb steals information from targets by entering their dreams.", "popularity": 145.2, "vote_average": 8.4, "vote_count": 35200, "release_date": "2010-07-15", "budget": 160000000, "revenue": 825532764},
        {"id": 157336, "title": "Interstellar", "overview": "The adventures of a group of explorers who make use of a newly discovered wormhole.", "popularity": 188.7, "vote_average": 8.4, "vote_count": 34100, "release_date": "2014-11-05", "budget": 165000000, "revenue": 677471339},
        {"id": 278, "title": "The Shawshank Redemption", "overview": "Framed in the 1940s for double murder, upstanding banker Andy Dufresne begins a new life.", "popularity": 135.6, "vote_average": 8.7, "vote_count": 26800, "release_date": "1994-09-23", "budget": 25000000, "revenue": 28341469},
        {"id": 238, "title": "The Godfather", "overview": "Spanning the years 1945 to 1955, a chronicle of the fictional Italian-American Corleone crime family.", "popularity": 142.1, "vote_average": 8.7, "vote_count": 20400, "release_date": "1972-03-14", "budget": 6000000, "revenue": 245066411},
        {"id": 155, "title": "The Dark Knight", "overview": "Batman raises the stakes in his war on crime with the help of Lt. Jim Gordon and Harvey Dent.", "popularity": 156.4, "vote_average": 8.5, "vote_count": 32500, "release_date": "2008-07-16", "budget": 185000000, "revenue": 1004558444},
        {"id": 680, "title": "Pulp Fiction", "overview": "A burger-loving hit man, his philosophical partner, and a washed-up boxer converge.", "popularity": 128.3, "vote_average": 8.5, "vote_count": 27600, "release_date": "1994-09-10", "budget": 8000000, "revenue": 213928762},
        {"id": 550, "title": "Fight Club", "overview": "A ticking-time-bomb insomniac and a slippery soap salesman channel primal male aggression.", "popularity": 112.5, "vote_average": 8.4, "vote_count": 28900, "release_date": "1999-10-15", "budget": 63000000, "revenue": 100853753},
        {"id": 13, "title": "Forrest Gump", "overview": "A man with a low IQ has accomplished great things in his life.", "popularity": 120.4, "vote_average": 8.5, "vote_count": 27200, "release_date": "1994-06-23", "budget": 55000000, "revenue": 677387716},
        {"id": 603, "title": "The Matrix", "overview": "Set in the 22nd century, The Matrix tells the story of a computer hacker.", "popularity": 124.9, "vote_average": 8.2, "vote_count": 25100, "release_date": "1999-03-30", "budget": 63000000, "revenue": 463517383},
        {"id": 19995, "title": "Avatar", "overview": "In the 22nd century, a paraplegic Marine is dispatched to the moon Pandora.", "popularity": 160.2, "vote_average": 7.6, "vote_count": 31200, "release_date": "2009-12-15", "budget": 237000000, "revenue": 2923706026},
        {"id": 597, "title": "Titanic", "overview": "101-year-old Rose DeWitt Bukater tells the story of her life aboard the Titanic.", "popularity": 140.5, "vote_average": 7.9, "vote_count": 24800, "release_date": "1997-11-18", "budget": 200000000, "revenue": 2264162353},
        {"id": 299534, "title": "Avengers: Endgame", "overview": "After the devastating events of Infinity War, the universe is in ruins.", "popularity": 175.3, "vote_average": 8.3, "vote_count": 25300, "release_date": "2019-04-24", "budget": 356000000, "revenue": 2797800564},
        {"id": 438631, "title": "Dune", "overview": "Paul Atreides leads nomadic tribes in a battle to control the desert planet Arrakis.", "popularity": 165.7, "vote_average": 7.8, "vote_count": 11500, "release_date": "2021-09-15", "budget": 165000000, "revenue": 402027583},
        {"id": 872585, "title": "Oppenheimer", "overview": "The story of J. Robert Oppenheimer’s role in the development of the atomic bomb.", "popularity": 198.4, "vote_average": 8.1, "vote_count": 8900, "release_date": "2023-07-19", "budget": 100000000, "revenue": 957000000},
        {"id": 346698, "title": "Barbie", "overview": "Barbie and Ken are having the time of their lives in the colorful Barbie Land.", "popularity": 185.0, "vote_average": 7.1, "vote_count": 8500, "release_date": "2023-07-19", "budget": 145000000, "revenue": 1445638421},
    ]
    return pd.DataFrame(sample_tmdb)

def upload_dataframe(client: bigquery.Client, df: pd.DataFrame, table_name: str) -> None:
    """Uploads a pandas DataFrame to a target BigQuery table (overwrite mode)."""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    print(f"Uploading {len(df):,} rows to '{table_id}'...")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for completion
    print(f"Successfully loaded '{table_id}'.")

def main():
    print("=" * 65)
    print(" Movie Streaming ELT: BigQuery Raw Data Seeder")
    print("=" * 65)

    client = get_client()
    ensure_dataset(client, DATASET_ID)

    # 1. MovieLens
    df_movies, df_ratings = download_movielens()
    upload_dataframe(client, df_movies, "movies")
    upload_dataframe(client, df_ratings, "ratings")

    # 2. TMDB Trending
    df_tmdb = get_tmdb_trending_data(TMDB_API_KEY)
    upload_dataframe(client, df_tmdb, "tmdb_trending")

    print("\n All raw datasets seeded successfully into BigQuery!")
    print(f"Target: `{PROJECT_ID}.{DATASET_ID}`")
    print("Next step: Run `dbt run` inside `dbt_movies` or trigger the Airflow DAG.")

if __name__ == "__main__":
    main()

