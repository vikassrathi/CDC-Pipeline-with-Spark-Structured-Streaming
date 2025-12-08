# Real-Time CDC Pipeline with Debezium, Kafka, and Delta Lake

This project implements a low-latency **Change Data Capture (CDC)** pipeline. It captures row-level changes from a transactional PostgreSQL database, streams them via Apache Kafka, and uses Apache Spark Structured Streaming to apply "Merge-on-Read" (Upsert) logic into a Delta Lake.

## Features

- **Real-time Data Capture**: Captures INSERT, UPDATE, and DELETE operations from PostgreSQL in real-time
- **Zero Data Loss**: Debezium reads transaction logs directly, ensuring no data is missed
- **ACID Compliance**: Delta Lake provides ACID transactions for reliable data storage
- **Scalable Architecture**: Designed to scale from local development to enterprise production
- **Merge-on-Read**: Implements upsert logic to maintain the current state of data

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Setup & Execution Guide](#setup--execution-guide)
  - [Prerequisites](#prerequisites)
  - [Phase 1: Infrastructure Initialization](#phase-1-infrastructure-initialization)
  - [Phase 2: Database Seeding](#phase-2-database-seeding)
  - [Phase 3: Activate CDC (Debezium)](#phase-3-activate-cdc-debezium)
  - [Phase 4: Start the Pipeline](#phase-4-start-the-pipeline)
- [Verification](#verification)
- [Scaling to Production](#scaling-to-production)
- [Troubleshooting](#troubleshooting)
- [Project Structure](#project-structure)

## Architecture

### Data Flow

```
PostgreSQL (Source) → Debezium (CDC) → Apache Kafka (Buffer) → Spark Structured Streaming (Engine) → Delta Lake (Storage)
```

## Tech Stack

| Component | Technology | Why we used it? |
| :--- | :--- | :--- |
| **Source DB** | **PostgreSQL 13** | A standard RDBMS configured with `wal_level=logical` to expose the Write-Ahead Log (WAL) for CDC. |
| **CDC Engine** | **Debezium** | **The Magic Layer.** Unlike batch scripts that query "SELECT *", Debezium reads the database transaction logs directly. This means 0% data loss (even deletes) and near-zero impact on database performance. |
| **Buffer** | **Apache Kafka** | Acts as a shock absorber. If Spark goes down, Kafka holds the data so nothing is lost. It decouples the source from the destination. |
| **Processing** | **Spark Structured Streaming** | Handles high-throughput data. We use it to parse the complex JSON from Debezium and handle logic like deduplication and micro-batching. |
| **Storage** | **Delta Lake** | Brings ACID transactions to the data lake. Allows us to perform `MERGE` (Upsert) operations to ensure the data lake reflects the *exact* current state of the database. |

## Setup & Execution Guide

### Prerequisites

- **Docker & Docker Compose** - For infrastructure
- **Python 3.10+** - For the pipeline script
- **Java 17 (OpenJDK)** - Required for Spark 3.5.x (*Ensure `JAVA_HOME` is set correctly*)

### Phase 1: Infrastructure Initialization

Spin up the containers (Postgres, Zookeeper, Kafka, Debezium).

```bash
docker-compose up -d
```

### Phase 2: Database Seeding

Create the source table in Postgres.

```bash
docker-compose exec postgres psql -U user -d mydb -c "
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_id INT,
    amount DECIMAL(10, 2),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO transactions (customer_id, amount, status) VALUES (101, 50.00, 'PENDING');
"
```

### Phase 3: Activate CDC (Debezium)

Register the connector to start watching the Postgres WAL.

```bash
curl -i -X POST \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  localhost:8083/connectors/ \
  -d '{
    "name": "pg-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "user",
      "database.password": "password",
      "database.dbname": "mydb",
      "database.server.name": "dbserver1",
      "table.include.list": "public.transactions",
      "plugin.name": "pgoutput",
      "topic.prefix": "cdc",
      "slot.name": "debezium_slot"
    }
  }'
```

### Phase 4: Start the Pipeline

Install dependencies and run the Spark job.

> **Note:** Version matching is critical here to avoid Scala errors.

```bash
# Ensure you are in your virtual environment
pip install pyspark==3.5.3 delta-spark==3.2.0

# Run the pipeline
python cdc_pipeline.py
```

Wait for the message: `Starting Stream...`

## Verification

To verify the pipeline is working in real-time, open a **second terminal** and follow these steps:

### Step 1: Perform an INSERT (New Data)

Add a new record to Postgres:

```bash
docker-compose exec postgres psql -U user -d mydb -c "INSERT INTO transactions (customer_id, amount, status) VALUES (200, 150.00, 'NEW');"
```

**Expected Result:** Check the Python script output. You should see a batch being processed.

### Step 2: Perform an UPDATE (Change Data)

Update the status of an existing transaction:

```bash
docker-compose exec postgres psql -U user -d mydb -c "UPDATE transactions SET status = 'PAID' WHERE transaction_id = 200;"
```

**Expected Result:** Delta Lake will receive the new row. Because of the `MERGE` logic in our script, the Silver table will update the existing row rather than duplicating it.

### Step 3: Verify Data in Delta Lake

Run this Python snippet to view the final table state:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("--- Final Delta Table State ---")
spark.read.format("delta").load("data_lake/silver/transactions").show()
```

## Scaling to Production

This project runs locally on Docker. Here is how you scale it for enterprise usage:

| Component | Local Strategy | Production Strategy |
| :--- | :--- | :--- |
| **Kafka** | Single Broker | **Cluster Mode:** Use Amazon MSK or Confluent Cloud. Increase partitions to allow parallel processing. |
| **Spark** | Local Mode (`local[*]`) | **Cluster Mode:** Run on Databricks, AWS EMR, or Kubernetes. Tune `spark.executor.instances` to match Kafka partitions. |
| **Storage** | Local Filesystem | **Cloud Object Store:** Switch paths to S3 (`s3a://`), Azure Data Lake (`abfss://`), or GCS (`gs://`). |
| **Schema** | Hardcoded | **Schema Registry:** Use Confluent Schema Registry to handle schema evolution (e.g., adding columns) automatically. |
| **State** | Local Checkpoints | **Remote Checkpoints:** Store checkpoints in S3 so if the cluster crashes, a new cluster can resume exactly where it left off. |

## Troubleshooting

### Error: `JAVA_GATEWAY_EXITED`

**Cause:** PySpark cannot find Java.

**Fix:** Ensure Java 17 is installed and `JAVA_HOME` is set.

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

### Error: `GenTraversableOnce` or `ClassNotFound`

**Cause:** Mismatch between Spark and Delta versions.

**Fix:** Ensure you use `pyspark==3.5.3` and `delta-spark==3.2.0`. Clear Ivy cache:

```bash
rm -rf ~/.ivy2/jars
```

### Error: `AnalysisException: Queries with streaming sources must be executed with writeStream`

**Cause:** Trying to run batch operations (like `show()` or `count()`) on a streaming DataFrame before starting the stream.

**Fix:** Define schema explicitly (`StructType`) and remove any inference logic (`.rdd.map`).

---

## Project Structure

```
CDC-Pipeline-with-Spark-Structured-Streaming/
├── cdc_pipeline.py           # Main Spark Structured Streaming pipeline
├── read_data.py              # Utility script to read Delta Lake data
├── docker-compose.yaml       # Docker infrastructure configuration
├── Dockerfile                # Custom Docker image (if needed)
├── data_lake/                # Delta Lake storage directory
│   ├── bronze/               # Raw CDC events
│   └── silver/               # Processed & merged data
└── README.md                 # This file
```
