# docker exec -it kafka kafka-topics create topic sensor-data bootstrap-server localhost:9092 partitions 1 replication-factor 1
# docker exec -it kafka kafka-topics list bootstrap-server localhost:9092

# spark-submit packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_streaming.py
# docker exec -it spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/spark-apps/spark_streaming.py

# docker exec -it spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/spark-apps/spark_streaming.py
#



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Crear sesión Spark
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")  # Cambiar a DEBUG si quieres más detalles

# 2. Definir el esquema de los datos que vienen de Kafka
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("value", DoubleType(), True)
])

# 3. Leer stream desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-data") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Convertir los mensajes de Kafka de bytes a string y parsear JSON
df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# 5. Escribir en parquet
query = df_parsed.writeStream \
    .format("parquet") \
    .option("path", "/opt/output/parquet") \
    .option("checkpointLocation", "/opt/output/checkpoints") \
    .outputMode("append") \
    .start()

# 6. Esperar a que finalice el stream
query.awaitTermination()


