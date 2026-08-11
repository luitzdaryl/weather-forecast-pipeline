from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkTest").getOrCreate()

data = [("Taipei", 30.5), ("Tokyo", 22.1), ("Seoul", 18.9)]
df = spark.createDataFrame(data, ["city", "temperature_c"])

df.show()

spark.stop()