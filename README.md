# StreamSense – Real-Time IoT Data Analytics

StreamSense is a real-time IoT data analytics pipeline that simulates sensor readings, streams them through Apache Kafka, processes them using Apache Spark Structured Streaming, and presents live analytics through an interactive Streamlit dashboard.

The project demonstrates event streaming, distributed data processing, window-based aggregation, Parquet storage, and real-time data visualization using Dockerized services.

## Architecture

```text
Python Sensor Simulator
         |
         v
   Apache Kafka
  (sensor-data topic)
         |
         v
Spark Structured Streaming
         |
         +----------------------+
         |                      |
         v                      v
   Raw Sensor Data       Window Aggregations
         |                 (1-minute windows)
         v                      |
   Parquet Storage              v
         |                Parquet Storage
         |                      |
         +----------+-----------+
                    |
                    v
           Streamlit Dashboard
                    |
                    v
          Interactive Analytics
```

## Technology Stack

- **Python** – Sensor data simulation and application logic
- **Apache Kafka** – Real-time event streaming
- **Apache Spark 3.5.0** – Structured Streaming and data processing
- **Docker & Docker Compose** – Containerized infrastructure
- **Apache Parquet** – Columnar data storage
- **Streamlit** – Interactive analytics dashboard
- **Pandas & PyArrow** – Data loading and processing
- **Plotly** – Interactive visualizations

## Key Features

### 1. IoT Sensor Simulation
- Simulates readings from five sensors.
- Generates temperature and humidity measurements.
- Publishes JSON events to the Kafka `sensor-data` topic.

### 2. Real-Time Data Streaming
- Consumes sensor events using Spark Structured Streaming.
- Processes incoming Kafka messages in micro-batches.
- Writes raw sensor records to Parquet files.
- Uses checkpointing for streaming query recovery.

### 3. Window-Based Analytics
- Groups incoming events into one-minute event-time windows.
- Calculates average temperature and humidity.
- Counts readings within each window.
- Uses a two-minute watermark to manage late-arriving events.

### 4. Interactive Dashboard
- Displays total readings and active sensors.
- Shows average temperature and humidity.
- Provides temperature and humidity trend charts.
- Supports sensor filtering and configurable temperature thresholds.
- Displays sensor-wise comparisons.
- Provides threshold-based alert history.
- Shows latest reading-based sensor status.
- Displays minute-level aggregated analytics.
- Supports CSV downloads.
- Automatically refreshes dashboard data every 10 seconds.

## Project Structure

```text
StreamSense/
│
├── dashboard/
│   └── app.py
│
├── producer/
│   └── producer.py
│
├── spark/
│   └── spark_streaming.py
│
├── output/                  # Generated data (not committed)
│
├── docker-compose.yml
├── fake_api.py
├── README.md
└── .gitignore
```

## Setup and Execution

### Prerequisites

- Docker Desktop
- Python 3.10 or later
- Git
- VS Code (recommended)

### Step 1: Start the Infrastructure

Start Kafka, Zookeeper, Spark, and the producer:

```powershell
docker compose up -d
```

Check running containers:

```powershell
docker compose ps
```

### Step 2: Run Spark Structured Streaming

Execute the Spark streaming application:

```powershell
docker exec -it spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/spark-apps/spark_streaming.py
```

The application consumes Kafka messages and writes raw sensor records and one-minute analytics to Parquet storage.

### Step 3: Launch the Dashboard

Open another PowerShell terminal and install the Python dependencies:

```powershell
py -m pip install streamlit pandas pyarrow plotly streamlit-autorefresh
```

Launch the dashboard:

```powershell
py -m streamlit run dashboard/app.py
```

Open the localhost URL displayed in the terminal.

### Step 4: Monitor Spark

The Spark application UI is available at:

```text
http://localhost:4040
```

## Data Storage

The pipeline stores its output in the mounted `output/` directory:

| Directory | Purpose |
|---|---|
| `output/parquet/` | Raw sensor readings |
| `output/checkpoints/` | Raw streaming query checkpoints |
| `output/minute_analytics/` | One-minute aggregated analytics |
| `output/minute_analytics_checkpoints/` | Aggregation query checkpoints |

Generated Parquet files and checkpoint data are excluded from Git using `.gitignore`.

## Analytics and Processing Logic

The pipeline calculates:

- Average temperature per one-minute window.
- Average humidity per one-minute window.
- Total sensor readings per window.
- Threshold-based temperature alerts.

The dashboard reads the persisted Parquet data and presents it through interactive charts, metrics, and tables.

## Project Scope and Limitations

- Sensor readings are simulated; physical IoT hardware is not connected.
- Temperature alerts are based on configurable thresholds, not machine-learning-based anomaly prediction.
- Sensor status is determined from the latest available reading and threshold comparison.
- The dashboard reads persisted Parquet files rather than consuming Kafka directly.

## Learning Outcomes

This project demonstrates practical implementation of:

- Event-driven data pipelines.
- Kafka producers and consumers.
- Apache Spark Structured Streaming.
- Event-time window aggregation and watermarking.
- Parquet-based analytical storage.
- Docker-based service orchestration.
- Interactive data analytics and visualization.

## Acknowledgement

This project builds upon the open-source Kafka and Spark Docker starter project by [ederreklab](https://github.com/ederreklab/Spark-Kafka-Streaming-Project-Docker-).

The existing foundation was adapted and extended with one-minute streaming analytics, additional Parquet outputs, and an interactive Streamlit dashboard.

---

**StreamSense – Turning simulated IoT events into real-time insights.**