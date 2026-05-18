# Architecture

## Problem
Indonesians who want to exchange IDR to foreign currency and maximize their investments want to know if today's rate is good or bad without spending time to manually research and analyze best time to buy.


## Target Users
Indonesian saving or investing their IDR to foreign currency

## Solution
This pipeline helps Indonesians decide when to exchange IDR to foreign currency 
by sending a daily Telegram alert with a BUY, NEUTRAL, or WAIT signal.

The signal is based on Z-Score — a statistical measure that compares today's 
rate against the 30-day historical average and standard deviation. This detects 
whether today's rate is unusually good or bad relative to recent history, 
regardless of the long-term weakening trend of IDR.

Users are encouraged to combine this signal with Dollar Cost Averaging (DCA) — 
exchange a fixed amount monthly, but exchange more when the signal is BUY and 
less when it's WAIT.

Technical approach:
- Z-Score = (today_rate - 30day_avg) / 30day_std_dev
- BUY: Z-Score > 1 (IDR unusually strong)
- NEUTRAL: -1 ≤ Z-Score ≤ 1 (normal range)
- WAIT: Z-Score < -1 (IDR unusually weak)
- Historical data backfilled from 2021-01-01
- Daily updates via Frankfurter API (IDR as base)

## Pipeline
1. Airflow fetches daily rates from Frankfurter API
2. Raw data loaded into BigQuery daily_rates table
3. dbt calculates Z-Score and generates suggestion in mart
4. Airflow sends suggestion to Telegram

## Tool Choices
- **Airflow** — Schedule and monitor the jobs daily
- **dbt** — Version-controlled transformations
- **BigQuery** — Load and store the data
- **Terraform** — Reproducible infrastructure
- **Docker** — Virtual environment for our workflow
- **Telegram** — Simple API implementation

## Data Flow
Frankfurter API -> Airflow -> BigQuery -> dbt -> mart_exchange_suggestion -> Telegram alert
