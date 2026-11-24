from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

spark = (
    SparkSession.builder
    .appName("StreamSenseIoTAnalytics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Sensor data schema
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", DoubleType(), True)
])

# Read Kafka stream
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "sensor-data")
    .option("startingOffsets", "latest")
    .load()
)

# Parse JSON messages
parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .filter(
        col("sensor_id").isNotNull()
        & col("temperature").isNotNull()
        & col("humidity").isNotNull()
        & col("timestamp").isNotNull()
    )
)

# Query 1: Raw sensor data
raw_query = (
    parsed_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "/opt/output/parquet")
    .option("checkpointLocation", "/opt/output/checkpoints")
    .start()
)

# Convert Unix timestamp to Spark event time
event_df = parsed_df.withColumn(
    "event_time",
    col("timestamp").cast("timestamp")
)

# Query 2: Minute-level aggregation
minute_df = (
    event_df
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "1 minute")
    )
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity"),
        count("*").alias("total_readings")
    )
    .select(
        col("window.start").alias("minute"),
        "avg_temperature",
        "avg_humidity",
        "total_readings"
    )
)

analytics_query = (
    minute_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "/opt/output/minute_analytics")
    .option(
        "checkpointLocation",
        "/opt/output/minute_analytics_checkpoints"
    )
    .start()
)

# Keep both queries running
spark.streams.awaitAnyTermination()