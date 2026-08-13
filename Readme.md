# Weather Forecast Pipeline

A data engineering pipeline that ingests live weather data, stores it in a cloud data warehouse, and (eventually) trains a forecasting model on top of it. Built as a hands-on project to learn the modern data stack: **Snowflake**, **Apache Airflow**, **Apache Spark**, and **Apache Kafka**.

## Architecture

```
Open-Meteo API (live weather data)
        │
        ▼
  Python ingestion script
        │
        ▼
Snowflake — RAW schema (as-ingested data)
        │
        ▼
  Apache Spark (dedupe, decode weather codes, rolling averages)
        │
        ▼
Snowflake — CLEANED schema (analysis-ready data)

Apache Airflow orchestrates both steps hourly, in sequence.
```

*(Kafka join this diagram in later milestones — see Roadmap below.)*

## Tech Stack

- **Snowflake** — cloud data warehouse, stores all historical weather readings
- **Python** — fetches data from [Open-Meteo](https://open-meteo.com) (free, no API key required) and loads it into Snowflake
- **Apache Airflow** — orchestrates and schedules the ingestion pipeline, running hourly in Docker
- **Apache Spark** — cleans, deduplicates, and enriches raw weather data, computing rolling temperature averages via window functions
- **Apache Kafka** — real-time streaming ingestion *(planned)*
- **Scikit-learn / XGBoost** — forecasting model *(planned)*
- **Streamlit** — dashboard + natural-language chatbot over the data, powered by a local Ollama model *(planned)*

## Project Structure

```
weather-forecast-pipeline/
├── data-pipeline/
│   ├── sql/                # Snowflake schema setup scripts
│   ├── scripts/             # Python ingestion scripts
│   ├── .env.example         # template for required credentials
│   └── requirements.txt
├── ml-pipeline/              # forecasting model (coming in a later milestone)
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

## Roadmap

- [x] **Milestone 1** — Batch ingestion: Python script → Snowflake (manual run)
- [x] **Milestone 1b** — Automate ingestion on a schedule with Apache Airflow
- [x] **Milestone 2** — Add Apache Spark for data transformation
- [ ] **Milestone 3** — Add Apache Kafka for real-time streaming ingestion
- [ ] **Milestone 4** — Train a forecasting model, build a Streamlit dashboard
- [ ] **Milestone 5** — Add a natural-language chatbot over the data (local Ollama)

## License

MIT