import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv("../data-pipeline/.env")  # reuse the same credentials file, don't duplicate secrets

SNOWFLAKE_OPTIONS = {
    "sfURL": f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
    "sfUser": os.environ["SNOWFLAKE_USER"],
    "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
    "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
    "sfSchema": os.environ["SNOWFLAKE_SCHEMA"],
    "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
}

spark = (
    SparkSession.builder.appName("SnowflakeRead")
    .config(
        "spark.jars.packages",
        "net.snowflake:snowflake-jdbc:3.19.0,net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4",
    )
    .getOrCreate()
)

df = (
    spark.read.format("net.snowflake.spark.snowflake")
    .options(**SNOWFLAKE_OPTIONS)
    .option("dbtable", "weather_observations")
    .load()
)

print(f"Row count: {df.count()}")
df.show(5)
df.printSchema()

spark.stop()