SELECT
    date,
    base,
    quote,
    rate,
    1/rate AS idr_per_quote
FROM `idr-fx-advisor.fx_data.daily_rates`