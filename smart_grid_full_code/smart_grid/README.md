# Smart Grid Energy Analytics Platform
### Real-Time Anomaly Detection & Demand Forecasting using Hadoop, Spark, Hive, HBase, Pig & Kafka

---

## Project Overview

A distributed big-data platform that:
- Ingests smart meter readings via **Apache Kafka**
- Detects anomalies in real time using **Spark Structured Streaming**
- Stores all data in **HDFS** (Parquet format)
- Runs batch demand forecasting with **Spark MLlib**
- Serves SQL analytics via **Apache Hive**
- Provides fast per-meter lookups via **Apache HBase**
- Cleans raw data with **Apache Pig**
- Visualises everything via **Apache Superset**

---

## Directory Structure

```
smart_grid/
├── README.md
├── config/
│   ├── cluster.env                   # Cluster host/port configuration
│   └── kafka_topics.sh               # Topic creation script
├── data/
│   └── download_datasets.sh          # Dataset download instructions
├── pig/
│   └── etl_clean.pig                 # Raw CSV cleaning & Parquet load
├── kafka/
│   ├── producer.py                   # Kafka producer (replays London dataset)
│   └── requirements.txt
├── spark/
│   ├── streaming/
│   │   └── anomaly_detector.py       # Spark Structured Streaming job
│   ├── batch/
│   │   ├── train_forecast_model.py   # GBT demand forecast model training
│   │   ├── run_forecast.py           # Hourly forecast generation
│   │   └── hdfs_loader.py            # Load CSV → HDFS Parquet
│   └── requirements.txt
├── hive/
│   ├── create_tables.hql             # External table definitions
│   └── analytics_queries.hql         # Sample analytics queries
├── hbase/
│   ├── create_tables.rb              # HBase shell schema setup
│   └── hbase_utils.py                # Python HBase client helpers
├── dashboard/
│   └── superset_setup.md             # Superset connection & chart setup guide
├── scripts/
│   ├── setup_all.sh                  # One-shot cluster setup script
│   ├── start_pipeline.sh             # Start all services in order
│   └── stop_pipeline.sh              # Graceful shutdown
└── docs/
    └── architecture.md               # Detailed architecture notes
```

---

## Datasets Required

| Dataset | Source | Size |
|---|---|---|
| London Smart Meters | https://www.kaggle.com/datasets/jeanmidev/smart-meters-in-london | ~10 GB |
| UCI Household Power | https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption | ~20 MB |
| PJM Hourly Energy | https://www.kaggle.com/datasets/robikscube/hourly-energy-consumption | ~1 GB |

---

## Quick Start

```bash
# 1. Configure cluster hosts
cp config/cluster.env.example config/cluster.env
nano config/cluster.env

# 2. Setup all services
bash scripts/setup_all.sh

# 3. Download and load data
bash data/download_datasets.sh
spark-submit spark/batch/hdfs_loader.py

# 4. Run Pig ETL
pig -f pig/etl_clean.pig

# 5. Create Hive tables
hive -f hive/create_tables.hql

# 6. Create HBase tables
hbase shell hbase/create_tables.rb

# 7. Train ML model
spark-submit spark/batch/train_forecast_model.py

# 8. Start Kafka producer (in one terminal)
python kafka/producer.py

# 9. Start Spark Streaming anomaly detector (in another terminal)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
             spark/streaming/anomaly_detector.py

# 10. Start hourly forecasting job
spark-submit spark/batch/run_forecast.py
```

---

## Cluster Layout

```
Cluster A (Streaming)    Cluster B (Storage)       Cluster C (Analytics)
┌──────────────────┐     ┌──────────────────┐       ┌──────────────────┐
│ Kafka Broker     │────▶│ HDFS NameNode    │◀─────▶│ Hive Metastore   │
│ Zookeeper        │     │ HDFS DataNodes   │       │ Spark Batch      │
│ Spark Streaming  │────▶│ HBase Master     │       │ Spark MLlib      │
│ Spark MLlib      │     │ HBase RegionSrvr │       │ Pig              │
└──────────────────┘     └──────────────────┘       └──────────────────┘
```
