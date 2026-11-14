{{ config(
    materialized='view',
    tags=['staging', 'movies']
) }}

/*
  Staging model for MovieLens raw movies.
  - Cleans movie titles and extracts the 4-digit release year.
  - Splits pipe-separated genres into a typed BigQuery ARRAY<STRING>.
  - Deduplicates by movie_id using BigQuery QUALIFY.
*/

WITH source AS (
    SELECT
        SAFE_CAST(movieId AS INT64) AS movie_id,
        TRIM(title) AS title,
        TRIM(genres) AS raw_genres
    FROM {{ source('raw_movies', 'movies') }}
    WHERE movieId IS NOT NULL
),

cleaned AS (
    SELECT
        movie_id,
        title,
        -- Extract release year from the trailing '(YYYY)' pattern in the title string
        SAFE_CAST(REGEXP_EXTRACT(title, r'\((\d{4})\)\s*$') AS INT64) AS release_year,
        
        -- Strip out the year suffix to get the normalized clean movie title
        TRIM(REGEXP_REPLACE(title, r'\s*\(\d{4}\)\s*$', '')) AS clean_title,
        
        -- Split pipe-separated genres into a BigQuery array; convert '(no genres listed)' to empty array
        CASE
            WHEN raw_genres IS NULL OR raw_genres = '' OR raw_genres = '(no genres listed)'
                THEN []
            ELSE SPLIT(raw_genres, '|')
        END AS genre_array,
        
        raw_genres,
        CURRENT_TIMESTAMP() AS stg_loaded_at
    FROM source
)

SELECT
    movie_id,
    title,
    clean_title,
    release_year,
    genre_array,
    raw_genres,
    stg_loaded_at
FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY movie_id ORDER BY release_year DESC NULLS LAST, title) = 1

