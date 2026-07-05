WITH staging AS (
    SELECT * FROM {{ ref('stg_rates') }}
),

with_stats AS (
    SELECT
        *,
        CAST(ROUND(AVG(rate) OVER (PARTITION BY quote ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 8) AS NUMERIC) AS avg_30d,
        CAST(ROUND(STDDEV(rate) OVER (PARTITION BY quote ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 8) AS NUMERIC) AS std_30d,
        CAST(ROUND(AVG(rate) OVER (PARTITION BY quote ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW), 8) AS NUMERIC) AS avg_90d,
        CAST(ROUND(STDDEV(rate) OVER (PARTITION BY quote ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW), 8) AS NUMERIC) AS std_90d,
        CAST(ROUND(AVG(rate) OVER (PARTITION BY quote ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW), 8) AS NUMERIC) AS avg_365d,
        CAST(ROUND(STDDEV(rate) OVER (PARTITION BY quote ORDER BY date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW), 8) AS NUMERIC) AS std_365d,
        CAST(ROUND(PERCENT_RANK() OVER (
            PARTITION BY quote 
            ORDER BY rate
        ), 8) AS NUMERIC) AS percentile_rank
    FROM staging
),

final AS (
    SELECT
        *,
        ROUND((rate - avg_30d) / NULLIF(std_30d, 0), 2) AS z_score_30d,
        ROUND((rate - avg_90d) / NULLIF(std_90d, 0), 2) AS z_score_90d,
        ROUND((rate - avg_365d) / NULLIF(std_365d, 0), 2) AS z_score_365d
    FROM with_stats
),

-- counts AS (
--     SELECT
--         *,
--         (CASE WHEN z_score_30d > 1 THEN 1 ELSE 0 END +
--          CASE WHEN z_score_90d > 1 THEN 1 ELSE 0 END +
--          CASE WHEN z_score_365d > 1 THEN 1 ELSE 0 END) AS buy_count,
--         (CASE WHEN z_score_30d < -1 THEN 1 ELSE 0 END +
--          CASE WHEN z_score_90d < -1 THEN 1 ELSE 0 END +
--          CASE WHEN z_score_365d < -1 THEN 1 ELSE 0 END) AS wait_count
--     FROM final
-- ),

signal AS (
    SELECT
        *,
    CASE
        WHEN percentile_rank > 0.6 AND z_score_30d > 0 THEN 'STRONG BUY'
        WHEN percentile_rank > 0.6 THEN 'BUY'
        WHEN percentile_rank < 0.4 AND z_score_30d < 0 THEN 'STRONG WAIT'
        WHEN percentile_rank < 0.4 THEN 'WAIT'
        ELSE 'NEUTRAL'
    END AS signal
    FROM final
)

SELECT * FROM signal