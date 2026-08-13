import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/data-pipeline/scripts")


def run_ingestion():
    from fetch_weather import fetch_weather, insert_into_snowflake
    reading = fetch_weather()
    print("Fetched:", reading)
    insert_into_snowflake(reading)


def run_transform():
    # Imported lazily, inside the function, not at the top of the file —
    # PySpark is a heavy import, and Airflow re-parses every DAG file
    # every ~30 seconds to check for changes. A top-level pyspark import
    # would slow down that constant background scanning for no benefit.
    sys.path.insert(0, "/opt/airflow/spark/scripts")
    from transform_weather import main as transform_main
    transform_main()


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_ingestion",
    default_args=default_args,
    description="Fetch live weather data, load into Snowflake, then transform with Spark",
    schedule_interval="@hourly",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["weather", "ingestion", "transformation"],
) as dag:

    fetch_and_load = PythonOperator(
        task_id="fetch_and_load_weather",
        python_callable=run_ingestion,
    )

    transform = PythonOperator(
        task_id="transform_weather",
        python_callable=run_transform,
    )

    fetch_and_load >> transform  # transform only runs after ingestion succeeds