{{ config(
    materialized='view',
    tags=['staging', 'ratings']
) }}

/*
  Staging model for MovieLens raw ratings.
  - Converts Unix epoch timestamps (seconds) to BigQuery TIMESTAMP and DATE.
  - Filters out records with invalid ratings outside the official 0.5 to 5.0 range.
  - Deduplicates ratings per (user_id, movie_id) taking the most recent submission.
*/

WITH source AS (
    SELECT
        SAFE_CAST(userId AS INT64) AS user_id,
        SAFE_CAST(movieId AS INT64) AS movie_id,
        SAFE_CAST(rating AS FLOAT64) AS rating,
        SAFE_CAST(timestamp AS INT64) AS unix_timestamp
    FROM {{ source('raw_movies', 'ratings') }}
    WHERE userId IS NOT NULL
      AND movieId IS NOT NULL
      AND rating IS NOT NULL
      AND timestamp IS NOT NULL
),

validated AS (
    SELECT
        user_id,
        movie_id,
        rating,
        -- Convert Unix seconds to native BigQuery timestamp and partition-friendly date
        TIMESTAMP_SECONDS(unix_timestamp) AS rating_timestamp,
        DATE(TIMESTAMP_SECONDS(unix_timestamp)) AS rating_date,
        CURRENT_TIMESTAMP() AS stg_loaded_at
    FROM source
    WHERE rating >= 0.5 
      AND rating <= 5.0
)

SELECT
    user_id,
    movie_id,
    rating,
    rating_timestamp,
    rating_date,
    stg_loaded_at
FROM validated
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY user_id, movie_id 
    ORDER BY rating_timestamp DESC
) = 1

