# Weather Forecast Pipeline

A data engineering pipeline that ingests live weather data, stores it in a cloud data warehouse, and (eventually) trains a forecasting model on top of it. Built as a hands-on project to learn the modern data stack: **Snowflake**, **Apache Airflow**, **Apache Spark**, and **Apache Kafka**.

## Architecture

```
Open-Meteo API (live weather data)
        │
        ▼
  Kafka Producer (fetches + publishes)
        │
        ▼
  Kafka topic: weather-readings
        │
        ▼
  Kafka Consumer (reads + inserts)
        │
        ▼
Snowflake — RAW schema (as-ingested data)
        │
        ▼
  Apache Spark (dedupe, decode weather codes, rolling averages)
        │
        ▼
Snowflake — CLEANED schema (analysis-ready data)
        │
        ▼
  Scikit-learn model (next-hour temperature forecast)
        │
        ▼
  Streamlit dashboard

Apache Airflow orchestrates ingestion and transformation hourly.
Model training and the dashboard currently run standalone (see Roadmap).
```

## Tech Stack

- **Snowflake** — cloud data warehouse, stores all historical weather readings
- **Python** — fetches data from [Open-Meteo](https://open-meteo.com) (free, no API key required) and loads it into Snowflake
- **Apache Airflow** — orchestrates and schedules the ingestion pipeline, running hourly in Docker
- **Apache Spark** — cleans, deduplicates, and enriches raw weather data, computing rolling temperature averages via window functions
- **Apache Kafka** — decouples ingestion from storage via a producer/consumer pattern, with offset-based delivery guarantees
- **Scikit-learn** — Random Forest model predicting next-hour temperature, evaluated against a naive persistence baseline to confirm it adds real value
- **Streamlit** — dashboard showing live conditions, temperature history, and the model's next-hour forecast

## Project Structure

```
weather-forecast-pipeline/
├── data-pipeline/
│   ├── sql/
│   │   ├── 01_setup_warehouse_db_table.sql   # warehouse, database, schema, raw table
│   │   └── 02_setup_cleaned_schema.sql        # cleaned schema for Spark output
│   ├── scripts/
│   │   └── fetch_weather.py                    # fetch_weather() + insert_into_snowflake(), reused by Kafka
│   ├── .env.example                              # template for required credentials
│   ├── .gitignore
│   └── requirements.txt
├── spark/
│   ├── scripts/
│   │   └── transform_weather.py                # dedupe, decode weather codes, rolling averages
│   └── requirements.txt
├── kafka/
│   ├── scripts/
│   │   ├── producer_weather.py                 # fetches + publishes to weather-readings topic
│   │   └── consumer_weather.py                  # consumes + inserts into Snowflake
│   ├── docker-compose.yml                         # standalone broker, for local testing only
│   └── requirements.txt
├── airflow/
│   ├── dags/
│   │   └── weather_dag.py                       # produce_weather >> consume_weather >> transform_weather
│   ├── logs/
│   ├── Dockerfile                                 # extends apache/airflow, adds JDK 17 + pip deps
│   ├── requirements.txt
│   └── docker-compose.yml                         # postgres, kafka, airflow-init/webserver/scheduler
├── ml-pipeline/                                     # forecasting model (Milestone 4)
└── README.md
```

## Setup

### 1. Snowflake

Run `data-pipeline/sql/01_setup_warehouse_db_table.sql` in a Snowflake worksheet to create the warehouse, database, schema, and table.

### 2. Python ingestion script

```bash
cd data-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in your own Snowflake credentials:

```bash
cp .env.example .env
```

Run the script manually:

```bash
python3 scripts/fetch_weather.py
```

### 3. Airflow (automated scheduling)

The ingestion script runs automatically every hour via Apache Airflow, orchestrated in Docker.

```bash
cd airflow
docker compose up --build
```

Open `http://localhost:8081` (username/password: `admin` / `admin`) to view and trigger the `weather_ingestion` DAG.

### 4. Spark transformation

Runs automatically as the second step in the Airflow DAG, after ingestion. To run it standalone instead:

```bash
cd spark
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 scripts/transform_weather.py
```

Reads from `weather_db.raw.weather_observations`, and writes deduplicated, enriched data — including a human-readable weather description and a rolling 3-reading temperature average — to `weather_db.cleaned.weather_observations_cleaned`.

### 5. Kafka (decoupled ingestion)

Runs automatically as the first two steps in the Airflow DAG — a producer publishes fetched weather data to a topic, a separate consumer reads it and inserts into Snowflake. To run standalone instead:

```bash
cd kafka
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 scripts/producer_weather.py
python3 scripts/consumer_weather.py
```

The broker itself runs as part of `airflow/docker-compose.yml`.

### 6. ML pipeline (forecasting + dashboard)

```bash
cd ml-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 scripts/train_model.py    # trains and saves the model
streamlit run scripts/app.py      # launches the dashboard
```

The model predicts next-hour temperature using a Random Forest, trained on Snowflake's cleaned data. It's evaluated against a naive "next hour = current temperature" baseline during training — the model must beat this baseline to be considered useful. Current result: **MAE 0.71°C vs. a 0.78°C baseline**, on ~250 rows of collected data. Accuracy is expected to improve as the pipeline continues collecting data over time.

## Roadmap

- [x] **Milestone 1** — Batch ingestion: Python script → Snowflake (manual run)
- [x] **Milestone 1b** — Automate ingestion on a schedule with Apache Airflow
- [x] **Milestone 2** — Add Apache Spark for data transformation
- [x] **Milestone 3** — Add Apache Kafka for real-time streaming ingestion
- [x] **Milestone 4** — Train a forecasting model, build a Streamlit dashboard
- [ ] **Milestone 5** — Add a natural-language chatbot over the data (local Ollama)

## License

MIT