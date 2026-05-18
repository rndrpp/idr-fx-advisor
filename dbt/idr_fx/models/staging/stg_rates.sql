SELECT
    date,
    base,
    quote,
    CAST(ROUND(rate, 8) AS NUMERIC) AS rate,
    ROUND(1/rate, 2) AS idr_per_quote
FROM `idr-fx-advisor.fx_data.daily_rates`