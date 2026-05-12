# Architecture

## Problem
Indonesians who want to exchange IDR to foreign currency and maximize their investments want to know if today's rate is good or bad without spending time to manually research and analyze best time to buy.


## Target Users
Indonesian saving or investing their IDR to foreign currency

## Solution
Suggest the time to buy foreign currency to maximize profits and reduce loss based on SMA.

## Pipeline
1. Airflow fetches daily rates from Frankfurter API
2. Raw data loaded into BigQuery daily_rates table
3. dbt cleans and deduplicates into staging model
4. dbt calculates 7-day SMA and generates suggestion in mart
5. Airflow sends suggestion to Telegram

## Tool Choices
- **Airflow** — Schedule and monitor the jobs daily
- **dbt** — Version-controlled transformations
- **BigQuery** — Load and store the data
- **Terraform** — Reproducible infrastructure
- **Docker** — Virtual environment for our workflow
- **Telegram** — Simple API implementation

## Data Flow
Frankfurter API -> Airflow -> BigQuery -> dbt -> mart_exchange_suggestion -> Telegram alert
