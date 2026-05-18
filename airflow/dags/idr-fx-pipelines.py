from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from google.cloud import bigquery
from airflow.operators.bash import BashOperator
from datetime import date
import os

with DAG(
    dag_id="idr_fx_pipeline",
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

    def send_discord_alert(**context):
        ds = context['ds']

        # Only send alert for today's run
        if ds != str(date.today()):
            print(f"Skipping alert for historical date {ds}")
            return
        
        client = bigquery.Client()
        query = f"""
            SELECT quote, idr_per_quote, signal, z_score
            FROM `idr-fx-advisor.fx_data.mart_fx_suggestion`
            WHERE date = '{ds}'
            ORDER BY quote
        """
        
        rows = list(client.query(query).result())
        
        if not rows:
            print(f"No data for {ds}")
            return
        
        # Build message
        message = f"📊 **IDR FX Daily Signal — {ds}**\n\n"
        
        for row in rows:
            emoji = "🟢" if row.signal == "BUY" else "🔴" if row.signal == "WAIT" else "🟡"
            message += f"**{row.quote}:** {row.idr_per_quote:,.2f} IDR → {emoji} {row.signal}\n"
        
        message += "\n_Based on 30-day Z-Score analysis_"
        
        # Send to Discord
        webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
        requests.post(webhook_url, json={"content": message})


    download_task = PythonOperator(
        task_id="fetch_rates",
        python_callable=fetch_rates,
    )

    load_task = PythonOperator(
        task_id="load_rates",
        python_callable=load_rates  ,
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="cd /opt/airflow/dbt/idr_fx && dbt run --profiles-dir /opt/airflow",
    )


    alert_task = PythonOperator(  # ← add here
        task_id="send_discord_alert",
        python_callable=send_discord_alert,
    )


    download_task >> load_task >> run_dbt >> alert_task