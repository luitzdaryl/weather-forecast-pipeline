-- The compute engine. XSMALL is the cheapest size, and auto_suspend=60
-- means it shuts off 60 seconds after you stop querying, to conserve credits.
CREATE WAREHOUSE IF NOT EXISTS weather_wh
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- The storage container for this whole project
CREATE DATABASE IF NOT EXISTS weather_db;

-- A namespace inside the database — like a folder for related tables
CREATE SCHEMA IF NOT EXISTS weather_db.raw;

-- The actual table our Python script will insert rows into
CREATE TABLE IF NOT EXISTS weather_db.raw.weather_observations (
  city              STRING,
  observed_at       TIMESTAMP_NTZ,   -- when the reading was taken
  temperature_c     FLOAT,
  humidity_pct      FLOAT,
  wind_speed_kmh    FLOAT,
  weather_code      INT,
  ingested_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()  -- when WE loaded it
);