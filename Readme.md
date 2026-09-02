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

## How the system was build???

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
- [x] **Milestone 5** — Add a natural-language chatbot over the data (local Ollama)

## Known Issues & Lessons Learned

**Silent Kafka failures (Aug 17 – Aug 31, 2026):** after consolidating Kafka into the Airflow Docker Compose stack, the producer and consumer scripts still had `localhost:9092` hardcoded as the broker address. Inside a container, `localhost` refers to the container itself, not the separate Kafka broker container — so every produce/consume attempt silently failed to connect. Because the original scripts treated "zero messages processed" as a successful, error-free outcome, Airflow's dashboard showed consistent green checkmarks for two weeks while no new data was actually being ingested.

**Root cause:** a hardcoded network address that should have been an environment variable, combined with error handling that didn't distinguish "ran successfully" from "did nothing."

**Fix:** 
- Both scripts now read the broker address from a `KAFKA_BOOTSTRAP_SERVERS` environment variable, falling back to `localhost:9092` only for local standalone testing.
- Both scripts now raise an explicit error if zero messages are produced/consumed, rather than exiting cleanly — a task that does nothing should look like a failure, not a success.
- The Kafka broker's data directory is now backed by a named Docker volume, so a container restart doesn't silently reset all topics and offsets.

**Takeaway:** dashboards and monitoring only catch what they're explicitly designed to check for. A task can report success while accomplishing nothing if failure conditions aren't defined precisely — worth validating end-to-end data freshness (e.g., checking `MAX(timestamp)` in the destination table), not just orchestration-level success/failure status.

---

## Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop) (running)
- Python 3.10+
- [Ollama](https://ollama.com), installed and running, with at least one model pulled (`ollama pull llama3.2`)
- A [Snowflake trial account](https://signup.snowflake.com) (no card required)
- Git

### 1. Clone the repo

```bash
git clone https://github.com/yourusername/weather-forecast-pipeline.git
cd weather-forecast-pipeline
```

### 2. Set up Snowflake

Open a Snowflake worksheet and run these two files, in order:

```sql
-- paste and run:
data-pipeline/sql/01_setup_warehouse_db_table.sql
data-pipeline/sql/02_setup_cleaned_schema.sql
```

This creates the warehouse, database, `RAW` and `CLEANED` schemas, and tables.

### 3. Configure credentials

```bash
cp data-pipeline/.env.example data-pipeline/.env
```

Edit `data-pipeline/.env` and fill in your real Snowflake account identifier, username, and password. **This single file is the only place credentials live** — every other component (Spark, Kafka, Airflow, the ML pipeline) reads from it directly.

### 4. Start the automated pipeline

```bash
cd airflow
docker compose up --build -d
```

This builds and starts everything: Postgres (Airflow's internal metadata store), the Kafka broker, and the Airflow scheduler/webserver. First run takes a few minutes (installing Java, Python packages, and pulling images).

Open [http://localhost:8081](http://localhost:8081) — log in with `admin` / `admin`. Find `weather_ingestion` in the DAG list, **unpause it** (toggle on the left), then click **▶ Trigger DAG** a few times over the next several minutes to seed some initial data rather than waiting for the hourly schedule.

### 5. Verify data is flowing

In Snowsight:

```sql
SELECT COUNT(*), MAX(OBSERVED_AT) FROM weather_db.raw.weather_observations;
SELECT COUNT(*), MAX(OBSERVED_AT) FROM weather_db.cleaned.weather_observations_cleaned;
```

Both should show a growing row count and a recent timestamp.

### 6. Run the ML model and dashboard

```bash
cd ../ml-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 scripts/train_model.py
streamlit run scripts/app.py
```

Opens automatically at `http://localhost:8501`. Note: with only a handful of rows, the model and chatbot will work but won't be very accurate yet — both improve automatically as the pipeline (Step 4) keeps running in the background.

---

## Everyday operation

```bash
cd airflow
docker compose up -d       # start the pipeline
docker compose down        # stop it
docker compose logs -f     # watch logs live
```

No rebuild (`--build`) needed unless you've changed code in `airflow/`, `data-pipeline/`, `spark/`, or `kafka/`.

## Troubleshooting

If something isn't working, check **Known Issues & Lessons Learned** below first — it documents the real failure modes this pipeline has actually hit and how they were diagnosed and fixed.
---


## Running Individual Components Standalone (development/testing only)

The steps above are all you need for normal use — Docker runs everything together. The sections below are for developing or debugging one piece in isolation, the way each was originally built and tested during this project. None of this is required for a working install.

All three require `data-pipeline/.env` to already be configured (Step 3 above).

### Data ingestion only

Fetches one live reading and inserts it directly into Snowflake, bypassing Kafka entirely.

```bash
cd data-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 scripts/fetch_weather.py
```

### Spark transformation only

Reads from `RAW`, transforms, writes to `CLEANED`. Requires Java 17 locally (`brew install openjdk@17` on Mac) — Spark needs a JVM to run, separate from Python itself.

```bash
cd spark
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 scripts/transform_weather.py
```

First run downloads the Snowflake Spark connector JARs (~30s); cached after that.

### Kafka producer/consumer only

**Requires the Airflow stack already running** (`cd airflow && docker compose up -d`) — that's what provides the actual Kafka broker, exposed to your host machine at `localhost:9092`. There's no separate standalone broker anymore; the earlier standalone `kafka/docker-compose.yml` was removed after it caused confusion by silently duplicating (and drifting out of sync with) the real broker config in `airflow/docker-compose.yml` — see **Known Issues** below.

```bash
cd kafka
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# publish one reading to the topic
python3 scripts/producer_weather.py

# read it back and insert into Snowflake
python3 scripts/consumer_weather.py
```

### ML pipeline only

Already covered in Step 6 above — training and the dashboard were always meant to run this way, not inside Docker.

---

## License

MIT