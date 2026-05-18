WITH staging AS (
    SELECT * FROM {{ ref('stg_rates') }}
),

with_stats AS (
    SELECT
        *,
        AVG(rate) OVER (PARTITION BY quote ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS avg_30d,
        STDDEV(rate) OVER (PARTITION BY quote ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS std_30d
    FROM staging
),

final AS (
    SELECT
        *,
        (rate - avg_30d) / NULLIF(std_30d, 0) AS z_score
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