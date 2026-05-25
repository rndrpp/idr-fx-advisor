from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id="idr_fx_transformation",
    start_date=datetime(2026, 5, 1),
    schedule="0 16 * * *",
    catchup=False,
    max_active_runs=5,
    tags=["project", "idr-fx"],
) as dag:
    
    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="cd /opt/airflow/dbt/idr_fx && dbt run --profiles-dir /opt/airflow",
    )

    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id="idr_fx_ingestion",
        external_task_id="load_rates",
        timeout=3600,
        poke_interval=60,
        mode='reschedule',
        execution_delta=timedelta(seconds=0),
    )


    wait_for_ingestion >> run_dbt