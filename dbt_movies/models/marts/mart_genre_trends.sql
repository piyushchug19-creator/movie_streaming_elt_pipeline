{{ config(
    materialized='table',
    tags=['marts', 'genres', 'bi'],
    cluster_by=['genre', 'decade']
) }}

/*
  Mart: Genre & Decade Trend Analysis
  Unnests movie genres and groups by genre and release decade to surface:
  - Aggregate movie volume per genre over time
  - Average user ratings and Bayesian weighted scores
  - Financial ROI, total revenues, and profitability rates by genre and era
*/

WITH exploded_genres AS (
    SELECT
        movie_id,
        clean_title,
        release_year,
        -- Calculate the 10-year decade (e.g., 1995 -> 1990, 2012 -> 2010)
        CASE 
            WHEN release_year IS NOT NULL AND release_year > 1880 
                THEN CAST(FLOOR(release_year / 10) * 10 AS INT64)
            ELSE NULL
        END AS decade,
        genre,
        rating_count,
        avg_rating,
        bayesian_weighted_score,
        budget,
        revenue,
        gross_profit,
        return_on_investment
    FROM {{ ref('mart_movie_performance') }},
    UNNEST(genre_array) AS genre
    WHERE genre IS NOT NULL 
      AND genre != ''
      AND genre != '(no genres listed)'
),

aggregated AS (
    SELECT
        genre,
        COALESCE(decade, 0) AS decade,
        CASE 
            WHEN decade IS NOT NULL THEN CONCAT(CAST(decade AS STRING), 's')
            ELSE 'Unknown'
        END AS decade_label,
        
        -- Volume metrics
        COUNT(DISTINCT movie_id) AS total_movies,
        SUM(rating_count) AS total_ratings,
        
        -- Rating metrics
        ROUND(AVG(avg_rating), 2) AS avg_movie_rating,
        ROUND(AVG(bayesian_weighted_score), 2) AS avg_bayesian_score,
        
        -- Financial metrics
        COUNTIF(budget IS NOT NULL AND revenue IS NOT NULL) AS movies_with_financials,
        SUM(budget) AS total_budget,
        SUM(revenue) AS total_revenue,
        SUM(gross_profit) AS total_gross_profit,
        
        -- ROI metrics
        ROUND(AVG(return_on_investment), 4) AS avg_return_on_investment,
        COUNTIF(return_on_investment > 0) AS profitable_movies_count,
        ROUND(
            SAFE_DIVIDE(
                COUNTIF(return_on_investment > 0),
                NULLIF(COUNTIF(return_on_investment IS NOT NULL), 0)
            ),
            4
        ) AS profitability_rate
    FROM exploded_genres
    GROUP BY 
        genre,
        decade,
        decade_label
)

SELECT
    genre,
    decade,
    decade_label,
    total_movies,
    total_ratings,
    avg_movie_rating,
    avg_bayesian_score,
    movies_with_financials,
    total_budget,
    total_revenue,
    total_gross_profit,
    avg_return_on_investment,
    profitable_movies_count,
    profitability_rate,
    CURRENT_TIMESTAMP() AS mart_created_at
FROM aggregated
ORDER BY 
    genre ASC, 
    decade ASC

