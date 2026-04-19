from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="observability_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command="python /app/src/main.py"
    )