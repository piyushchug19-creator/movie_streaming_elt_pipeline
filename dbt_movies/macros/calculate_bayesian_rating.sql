{% macro calculate_bayesian_rating(rating_count, avg_rating, global_mean_rating, min_votes=25.0) %}
  {#-
    Bayesian Weighted Rating formula:
    WR = (v / (v + m)) * R + (m / (v + m)) * C
    where:
      v = count of ratings for the movie
      m = minimum ratings threshold
      R = average rating for the movie
      C = mean rating across all movies
  -#}
  ROUND(
    (
      (SAFE_CAST({{ rating_count }} AS FLOAT64) / (SAFE_CAST({{ rating_count }} AS FLOAT64) + {{ min_votes }})) * SAFE_CAST({{ avg_rating }} AS FLOAT64)
    ) + (
      ({{ min_votes }} / (SAFE_CAST({{ rating_count }} AS FLOAT64) + {{ min_votes }})) * SAFE_CAST({{ global_mean_rating }} AS FLOAT64)
    ),
    3
  )
{% endmacro %}

