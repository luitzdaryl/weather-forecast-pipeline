import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Makes the mounted data-pipeline folder importable, so we can reuse
# fetch_weather.py directly instead of duplicating its logic here.
sys.path.insert(0, "/opt/airflow/data-pipeline/scripts")

from fetch_weather import fetch_weather, insert_into_snowflake


def run_pipeline():
    reading = fetch_weather()
    print("Fetched:", reading)
    insert_into_snowflake(reading)

default_args = {
    "owner": "airflow",
    "retries": 3,                          # was 1 — more attempts for transient failures
    "retry_delay": timedelta(minutes=2),   # was 5 — recover faster
}

with DAG(
    dag_id="weather_ingestion",
    default_args=default_args,
    description="Fetch live weather data and load it into Snowflake",
    schedule_interval="@hourly",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["weather", "ingestion"],
) as dag:

    fetch_and_load = PythonOperator(
        task_id="fetch_and_load_weather",
        python_callable=run_pipeline,
    )