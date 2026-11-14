{{ config(
    materialized='table',
    tags=['marts', 'movies', 'bi'],
    cluster_by=['primary_genre', 'release_year']
) }}

/*
  Mart: Movie Performance & Financial Analytics
  Combines MovieLens catalog metadata, user rating aggregations, and TMDB financial metrics.
  Computes:
  - Average rating and rating volume
  - Return on Investment (ROI) = (revenue - budget) / budget
  - Bayesian Weighted Rating Score using global dataset mean and vote count threshold
*/

WITH ratings_agg AS (
    SELECT
        movie_id,
        COUNT(1) AS rating_count,
        ROUND(AVG(rating), 4) AS avg_rating,
        MIN(rating) AS min_rating,
        MAX(rating) AS max_rating,
        ROUND(STDDEV_SAMP(rating), 4) AS rating_stddev,
        MIN(rating_timestamp) AS first_rated_at,
        MAX(rating_timestamp) AS last_rated_at
    FROM {{ ref('stg_ratings') }}
    GROUP BY movie_id
),

global_stats AS (
    -- Global mean rating across all ratings for the Bayesian prior (C)
    SELECT
        ROUND(AVG(rating), 4) AS global_mean_rating
    FROM {{ ref('stg_ratings') }}
),

movies AS (
    SELECT
        movie_id,
        title,
        clean_title,
        release_year,
        genre_array,
        raw_genres,
        CASE 
            WHEN ARRAY_LENGTH(genre_array) > 0 THEN genre_array[OFFSET(0)]
            ELSE 'Uncategorized'
        END AS primary_genre
    FROM {{ ref('stg_movies') }}
),

tmdb AS (
    SELECT
        tmdb_id,
        tmdb_title,
        budget,
        revenue,
        popularity,
        tmdb_vote_average,
        tmdb_vote_count,
        release_date,
        tmdb_release_year
    FROM {{ ref('stg_tmdb_trending') }}
),

joined AS (
    SELECT
        m.movie_id,
        m.title,
        m.clean_title,
        m.release_year,
        m.genre_array,
        m.raw_genres,
        m.primary_genre,
        
        -- Rating aggregations
        COALESCE(r.rating_count, 0) AS rating_count,
        COALESCE(r.avg_rating, 0.0) AS avg_rating,
        r.min_rating,
        r.max_rating,
        r.rating_stddev,
        r.first_rated_at,
        r.last_rated_at,
        
        -- TMDB Financials & Metadata
        t.tmdb_id,
        t.budget,
        t.revenue,
        t.popularity,
        t.tmdb_vote_average,
        t.tmdb_vote_count,
        
        -- Global prior
        g.global_mean_rating
    FROM movies m
    LEFT JOIN ratings_agg r 
        ON m.movie_id = r.movie_id
    LEFT JOIN tmdb t 
        ON LOWER(m.clean_title) = LOWER(t.tmdb_title)
       AND (m.release_year = t.tmdb_release_year OR m.release_year IS NULL OR t.tmdb_release_year IS NULL)
    CROSS JOIN global_stats g
)

SELECT
    movie_id,
    title,
    clean_title,
    release_year,
    genre_array,
    raw_genres,
    primary_genre,
    
    -- Ratings Metrics
    rating_count,
    ROUND(avg_rating, 2) AS avg_rating,
    min_rating,
    max_rating,
    rating_stddev,
    first_rated_at,
    last_rated_at,
    
    -- Bayesian Weighted Rating Score: WR = (v / (v + m)) * R + (m / (v + m)) * C
    {{ calculate_bayesian_rating('rating_count', 'avg_rating', 'global_mean_rating', min_votes=25.0) }} AS bayesian_weighted_score,
    
    -- Financial Metrics
    budget,
    revenue,
    CASE 
        WHEN budget IS NOT NULL AND revenue IS NOT NULL 
            THEN (revenue - budget)
        ELSE NULL 
    END AS gross_profit,
    
    -- Return on Investment (ROI)
    ROUND(
        SAFE_DIVIDE((revenue - budget), NULLIF(budget, 0)),
        4
    ) AS return_on_investment,
    
    -- Categorical Classifications for BI filtering
    CASE
        WHEN budget IS NULL OR revenue IS NULL THEN 'No Financial Data'
        WHEN revenue > budget THEN 'Profitable'
        WHEN revenue < budget THEN 'Loss'
        ELSE 'Break-Even'
    END AS financial_status,
    
    CASE
        WHEN avg_rating >= 4.5 THEN 'Masterpiece (>=4.5)'
        WHEN avg_rating >= 4.0 THEN 'Acclaimed (4.0 - 4.49)'
        WHEN avg_rating >= 3.5 THEN 'Favorable (3.5 - 3.99)'
        WHEN avg_rating >= 2.5 THEN 'Mixed (2.5 - 3.49)'
        WHEN avg_rating > 0.0  THEN 'Unfavorable (<2.5)'
        ELSE 'Unrated'
    END AS rating_tier,
    
    popularity,
    tmdb_vote_average,
    tmdb_vote_count,
    CURRENT_TIMESTAMP() AS mart_created_at
FROM joined

