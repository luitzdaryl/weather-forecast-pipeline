import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_PIPELINE_SCRIPTS = "/opt/airflow/data-pipeline/scripts"
SPARK_SCRIPTS = "/opt/airflow/spark/scripts"
KAFKA_SCRIPTS = "/opt/airflow/kafka/scripts"

# Kafka's broker address INSIDE Docker's network — matches the "kafka" service
# name from docker-compose.yml, not localhost (that would mean "this container").
os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "kafka:9092"


def run_produce():
    sys.path.insert(0, DATA_PIPELINE_SCRIPTS)
    sys.path.insert(0, KAFKA_SCRIPTS)
    from producer_weather import main as produce_main
    produce_main()


def run_consume():
    sys.path.insert(0, DATA_PIPELINE_SCRIPTS)
    sys.path.insert(0, KAFKA_SCRIPTS)
    from consumer_weather import main as consume_main
    consume_main()


def run_transform():
    sys.path.insert(0, SPARK_SCRIPTS)
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
    description="Fetch weather data via Kafka, load into Snowflake, then transform with Spark",
    schedule_interval="@hourly",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["weather", "ingestion", "kafka", "transformation"],
) as dag:

    produce = PythonOperator(
        task_id="produce_weather",
        python_callable=run_produce,
        retries=5,
        retry_delay=timedelta(minutes=1),
    )
    consume = PythonOperator(task_id="consume_weather", python_callable=run_consume)
    transform = PythonOperator(task_id="transform_weather", python_callable=run_transform)

    produce >> consume >> transform