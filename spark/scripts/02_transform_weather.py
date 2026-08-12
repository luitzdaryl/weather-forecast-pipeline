import os
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from dotenv import load_dotenv

load_dotenv("../data-pipeline/.env")

# Base connection info — schema gets specified separately per read/write below,
# since we're reading from RAW but writing to CLEANED.
SNOWFLAKE_OPTIONS = {
    "sfURL": f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
    "sfUser": os.environ["SNOWFLAKE_USER"],
    "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
    "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
    "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
}

# Official WMO weather interpretation codes — the same fixed standard Open-Meteo
# documents. Static reference data, safe to hardcode; it doesn't change.
WEATHER_CODE_MAP = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Depositing rime fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    56: "Light freezing drizzle", 57: "Dense freezing drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    66: "Light freezing rain", 67: "Heavy freezing rain",
    71: "Slight snow fall", 73: "Moderate snow fall", 75: "Heavy snow fall",
    77: "Snow grains",
    80: "Slight rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
    85: "Slight snow showers", 86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm with slight hail", 99: "Thunderstorm with heavy hail",
}


def main():
    spark = (
        SparkSession.builder.appName("WeatherTransform")
        .config(
            "spark.jars.packages",
            "net.snowflake:snowflake-jdbc:3.19.0,net.snowflake:spark-snowflake_2.13:3.1.1",
        )
        .getOrCreate()
    )

    # --- Read raw data ---
    raw_df = (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**SNOWFLAKE_OPTIONS)
        .option("sfSchema", "RAW")
        .option("dbtable", "weather_observations")
        .load()
    )
    print(f"Raw row count: {raw_df.count()}")

    # --- Cleaning: remove duplicate readings ---
    # Two rows count as "the same reading" if CITY + OBSERVED_AT match, regardless
    # of when we happened to insert them. Keep only the earliest-ingested copy.
    dedupe_window = Window.partitionBy("CITY", "OBSERVED_AT").orderBy(F.col("INGESTED_AT").asc())

    deduped_df = (
        raw_df
        .withColumn("row_num", F.row_number().over(dedupe_window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    print(f"After dedup: {deduped_df.count()}")

    # --- Enrichment: decode weather_code into a readable label ---
    code_lookup = F.create_map([F.lit(x) for pair in WEATHER_CODE_MAP.items() for x in pair])

    enriched_df = (
        deduped_df
        .withColumn("WEATHER_CODE", F.col("WEATHER_CODE").cast("int"))
        .withColumn("WEATHER_DESCRIPTION", code_lookup[F.col("WEATHER_CODE")])
        .withColumn("WEATHER_DESCRIPTION", F.coalesce(F.col("WEATHER_DESCRIPTION"), F.lit("Unknown")))
    )

    # --- Aggregation: rolling 3-reading average temperature, per city ---
    rolling_window = Window.partitionBy("CITY").orderBy("OBSERVED_AT").rowsBetween(-2, 0)

    final_df = enriched_df.withColumn(
        "TEMP_ROLLING_AVG_C",
        F.round(F.avg("TEMPERATURE_C").over(rolling_window), 2),
    )

    final_df.orderBy("CITY", "OBSERVED_AT").show(10, truncate=False)

    # --- Write to weather_db.cleaned ---
    (
        final_df.write.format("net.snowflake.spark.snowflake")
        .options(**SNOWFLAKE_OPTIONS)
        .option("sfSchema", "CLEANED")
        .option("dbtable", "weather_observations_cleaned")
        .mode("overwrite")
        .save()
    )
    print("Write complete.")

    spark.stop()


if __name__ == "__main__":
    main()