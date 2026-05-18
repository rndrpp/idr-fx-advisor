WITH staging AS (
    SELECT * FROM {{ ref('stg_rates') }}
),

with_stats AS (
    SELECT
        *,
        CAST(ROUND(AVG(rate) OVER (PARTITION BY quote ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 8) AS NUMERIC) AS avg_30d,
        CAST(ROUND(STDDEV(rate) OVER (PARTITION BY quote ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 8) AS NUMERIC) AS std_30d
        FROM staging
),

final AS (
    SELECT
        *,
        ROUND((rate - avg_30d) / NULLIF(std_30d, 0), 2) AS z_score
    FROM with_stats
),

signal AS (
    SELECT
        *,
        CASE
            WHEN z_score > 1 THEN 'BUY'
            WHEN z_score < -1 THEN 'WAIT'
            ELSE 'NEUTRAL'
        END AS signal
    FROM final
)

SELECT * FROM signal