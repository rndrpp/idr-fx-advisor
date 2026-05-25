from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from google.cloud import bigquery
from datetime import date
import os
from airflow.sensors.external_task import ExternalTaskSensor


with DAG(
    dag_id="idr_fx_alert",
    start_date=datetime(2026, 5, 1),
    schedule="0 16 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["project", "idr-fx"],
) as dag:

    def send_discord_alert(**context):
        client = bigquery.Client()
        query = """
            SELECT quote, idr_per_quote, signal, percentile_rank,
                z_score_30d, z_score_90d, z_score_365d
            FROM `idr-fx-advisor.fx_data.mart_fx_suggestion`
            WHERE date = (SELECT MAX(date) FROM `idr-fx-advisor.fx_data.mart_fx_suggestion`)
            ORDER BY quote
        """
        
        rows = list(client.query(query).result())
        
        if not rows:
            print("No data available")
            return
        
        # Build message
        message = "📊 **IDR FX Signal**\n\n"
        for row in rows:
            if row.signal == "STRONG BUY":
                emoji = "🟢🟢"
            elif row.signal == "BUY":
                emoji = "🟢"
            elif row.signal == "STRONG WAIT":
                emoji = "🔴🔴"
            elif row.signal == "WAIT":
                emoji = "🔴"
            else:
                emoji = "🟡"
    
            percentile_pct = round(row.percentile_rank * 100, 1)
            message += f"{emoji} **{row.quote}:** {row.idr_per_quote:,.0f} IDR — {row.signal}\n"
            message += f"   ↳ Percentile: {percentile_pct}% | 30d: `{row.z_score_30d}` | 90d: `{row.z_score_90d}` | 365d: `{row.z_score_365d}`\n\n"
                
        
        # Send to Discord
        webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
        requests.post(webhook_url, json={"content": message})

    wait_for_transformation = ExternalTaskSensor(
        task_id="wait_for_transformation",
        external_dag_id="idr_fx_transformation",
        external_task_id="run_dbt",
        timeout=3600,
        poke_interval=60,
        mode='reschedule',
        execution_delta=timedelta(seconds=0),
    )

    alert_task = PythonOperator(  # ← add here
        task_id="send_discord_alert",
        python_callable=send_discord_alert,
    )    

    wait_for_transformation >> alert_task

        
        