This project implements a real-time data pipeline using Apache Kafka and Apache Spark Structured Streaming. 
Incoming sensor events are consumed from Kafka, processed in micro-batches, and persisted to disk in Parquet format with checkpointing enabled for fault tolerance.

# Spark + Kafka Streaming Project (Docker)

This project demonstrates a basic real-time data pipeline using:

- Apache Kafka
- Apache Spark Structured Streaming
- Docker & Docker Compose
- Python

## Architecture

Python Producer
      ↓
Apache Kafka (topic: sensor-data)
      ↓
Spark Structured Streaming
      ↓
Parquet files (batch-oriented sink)

## Components

- Kafka & Zookeeper running in Docker
- Python producer simulating sensor data
- Spark streaming job consuming Kafka topic and writing Parquet files

## Kafka Message Schema

Each message is a JSON object with the following structure:

json
{
  "sensor_id": 1,
  "temperature": 24.7,
  "humidity": 51.2,
  "timestamp": 1765911360
}

## Monitoring

Spark UI available at:
http://localhost:4040

## Streaming Output

The Spark Structured Streaming job writes the processed data to disk in Parquet format.

- Output format: **Parquet**
- Storage location: `spark-data/parquet`
- Checkpointing: enabled (`spark-data/checkpoints`)
- Processing mode: micro-batch (Structured Streaming)

Parquet is used to enable efficient analytical queries and downstream batch processing.

## How to run

1. Start all services:
   docker-compose up -d

2. Check producer logs:
   docker logs -f producer

3. Run Spark streaming job:
   docker exec -it spark /opt/spark/bin/spark-submit \
   --conf spark.jars.ivy=/tmp/ivy \
   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
   /opt/spark-apps/spark_streaming.py

4. Once running, the Spark job continuously consumes messages from Kafka and writes them as Parquet files to the local filesystem via Docker volumes.

### Data Persistence

The streaming pipeline persists incoming Kafka events using Apache Spark Structured Streaming.
Data is written incrementally in Parquet format, with checkpointing enabled to ensure exactly-once semantics and fault tolerance.

This approach allows the generated data to be efficiently queried or reused in downstream batch analytics workflows.

