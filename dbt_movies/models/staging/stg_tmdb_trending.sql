{{ config(
    materialized='view',
    tags=['staging', 'tmdb']
) }}

/*
  Staging model for The Movie Database (TMDB) trending movies.
  - Normalizes IDs and cleans title strings.
  - Validates and parses release dates (YYYY-MM-DD).
  - Cleans budget and box-office revenue (replacing 0 and negative values with NULL).
  - Casts popularity and TMDB vote metrics to numeric types.
  - Deduplicates by tmdb_id keeping the highest popularity record.
*/

WITH source AS (
    SELECT
        SAFE_CAST(id AS INT64) AS tmdb_id,
        TRIM(title) AS tmdb_title,
        TRIM(overview) AS overview,
        SAFE_CAST(popularity AS FLOAT64) AS popularity,
        SAFE_CAST(vote_average AS FLOAT64) AS tmdb_vote_average,
        SAFE_CAST(vote_count AS INT64) AS tmdb_vote_count,
        CAST(release_date AS STRING) AS raw_release_date,
        SAFE_CAST(budget AS NUMERIC) AS raw_budget,
        SAFE_CAST(revenue AS NUMERIC) AS raw_revenue
    FROM {{ source('raw_movies', 'tmdb_trending') }}
    WHERE id IS NOT NULL
),

cleaned AS (
    SELECT
        tmdb_id,
        tmdb_title,
        overview,
        -- Replace 0 or negative budgets with NULL to avoid skewed ROI and division-by-zero
        NULLIF(CASE WHEN raw_budget <= 0 THEN NULL ELSE raw_budget END, 0) AS budget,
        -- Replace 0 or negative revenues with NULL
        NULLIF(CASE WHEN raw_revenue <= 0 THEN NULL ELSE raw_revenue END, 0) AS revenue,
        COALESCE(popularity, 0.0) AS popularity,
        tmdb_vote_average,
        COALESCE(tmdb_vote_count, 0) AS tmdb_vote_count,
        -- Parse ISO date strings safely
        SAFE.PARSE_DATE('%Y-%m-%d', raw_release_date) AS release_date,
        SAFE_CAST(REGEXP_EXTRACT(raw_release_date, r'^(\d{4})') AS INT64) AS tmdb_release_year,
        CURRENT_TIMESTAMP() AS stg_loaded_at
    FROM source
)

SELECT
    tmdb_id,
    tmdb_title,
    overview,
    budget,
    revenue,
    popularity,
    tmdb_vote_average,
    tmdb_vote_count,
    release_date,
    tmdb_release_year,
    stg_loaded_at
FROM cleaned
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY tmdb_id 
    ORDER BY popularity DESC, release_date DESC NULLS LAST
) = 1

