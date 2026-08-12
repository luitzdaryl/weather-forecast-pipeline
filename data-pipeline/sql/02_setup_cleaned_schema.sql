-- Milestone 2: schema for Spark-transformed (cleaned) data,
-- kept separate from raw so the original ingested data stays untouched.

CREATE SCHEMA IF NOT EXISTS weather_db.cleaned;