from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from google.cloud import bigquery

with DAG(
    dag_id="idr_fx_ingestion",
    start_date=datetime(2026, 5, 1),
    schedule="0 16 * * *",
    catchup=False,
    max_active_runs=5,
    tags=["project", "idr-fx"],
) as dag:

    def fetch_rates(**context):
        ds = context['ds']

        url = f"https://api.frankfurter.dev/v2/rates?base=IDR&quotes=USD,SGD,JPY,CHF&date={ds}"
        response = requests.get(url)
        
        if not response.text or response.status_code != 200:
            print(f"No data for {ds} — skipping")
            return []
        
        return response.json()

    def load_rates(**context):
        rows = context['ti'].xcom_pull(task_ids='fetch_rates')
        if not rows:
            return
        
        client = bigquery.Client()
        table_id = "idr-fx-advisor.fx_data.daily_rates"
        
        # Check if data already exists for this date
        ds = context['ds']
        query = f"SELECT COUNT(*) as cnt FROM `{table_id}` WHERE date = '{ds}'"
        result = list(client.query(query).result())
        
        if result[0].cnt > 0:
            print(f"Data for {ds} already exists — skipping")
            return
        
        errors = client.insert_rows_json(table_id, rows)

        
        if errors:
            raise Exception(f"BigQuery insert error: {errors}")
        else:
            print(f"Inserted {len(rows)} rows")

    fetch_task = PythonOperator(
        task_id="fetch_rates",
        python_callable=fetch_rates,
    )

    load_task = PythonOperator(
        task_id="load_rates",
        python_callable=load_rates,
    )

    fetch_task >> load_task