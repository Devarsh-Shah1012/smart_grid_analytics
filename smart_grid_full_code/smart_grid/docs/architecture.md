# Smart Grid Analytics Platform — Architecture Notes

## Overview

The platform is split across three logical clusters, each with a
dedicated master node and two worker nodes (9 VMs total).

---

## Data Flow

```
                         ┌─────────────────────────────────┐
                         │    DATA SOURCES                 │
                         │  London Smart Meters (10 GB)    │
                         │  PJM Hourly Demand (1 GB)       │
                         │  UCI Household Power (20 MB)    │
                         └────────────┬────────────────────┘
                                      │
                                      ▼
                         ┌────────────────────────────┐
                         │   CLUSTER C — ETL Phase    │
                         │   Apache Pig               │
                         │   - Parse & validate CSVs  │
                         │   - Join with metadata     │
                         │   - Aggregate to hourly    │
                         │   - Output clean CSV/HDFS  │
                         └────────────┬───────────────┘
                                      │
                                      ▼
                         ┌────────────────────────────┐
                         │   CLUSTER B — HDFS         │
                         │   Raw + Clean Parquet store │
                         └────────────┬───────────────┘
                                      │
                    ┌─────────────────┼──────────────────────┐
                    ▼                 ▼                       ▼
          ┌──────────────┐  ┌──────────────────┐  ┌──────────────────────┐
          │ CLUSTER C    │  │ CLUSTER A        │  │ CLUSTER C            │
          │ Spark Batch  │  │ Kafka Producer   │  │ Spark MLlib          │
          │ hdfs_loader  │  │ (replays data)   │  │ train_forecast_model │
          │ CSV→Parquet  │  └───────┬──────────┘  └──────────┬───────────┘
          └──────────────┘          │                         │
                                    ▼                         │ (model saved
                         ┌──────────────────┐                │  to HDFS)
                         │ CLUSTER A        │                │
                         │ Kafka Broker     │                │
                         │ topic:           │                │
                         │ meter-readings   │                │
                         └────────┬─────────┘                │
                                  │                           │
                                  ▼                           │
                         ┌──────────────────┐                │
                         │ CLUSTER A        │                │
                         │ Spark Structured │◀───────────────┘
                         │ Streaming        │  (loads model)
                         │                  │
                         │ Per-meter Z-score│
                         │ state tracking   │
                         │ mapGroupsWithState│
                         └──┬───────┬───────┘
                            │       │
                  ┌─────────┘       └──────────┐
                  ▼                             ▼
       ┌──────────────────┐        ┌──────────────────────┐
       │ CLUSTER B        │        │ CLUSTER A            │
       │ HDFS Parquet     │        │ Kafka                │
       │ (ALL readings)   │        │ topic:               │
       │                  │        │ anomaly-alerts       │
       │ CLUSTER B        │        └──────────────────────┘
       │ HBase            │
       │ (ANOMALY rows)   │
       └────────┬─────────┘
                │
                ▼
       ┌──────────────────────────────────────┐
       │  CLUSTER C — Analytics & Dashboard  │
       │                                      │
       │  Hive: SQL queries over Parquet      │
       │  Spark Batch: run_forecast.py        │
       │  HBase Flask API: live alert feed    │
       │  Apache Superset: dashboard UI       │
       └──────────────────────────────────────┘
```

---

## Component Interactions

### Kafka ↔ Spark Streaming
- Spark uses `spark-sql-kafka` connector
- Reads from `meter-readings` with `startingOffsets=latest`
- Writes alerts back to `anomaly-alerts` topic
- Checkpoint stored in HDFS to support job restart/recovery

### Spark Streaming ↔ HBase
- Uses `happybase` Python library via Thrift server
- `foreachBatch` pattern: Spark calls Python function per micro-batch
- Row key: `{meter_id}_{reverse_epoch}` — newest alerts scan first

### Spark ↔ Hive
- Spark uses HiveContext with metastore URI
- Writes forecasts directly to Hive-managed Parquet table
- Hive views pre-aggregate data for Superset

### Hive ↔ Superset
- JDBC connection via HiveServer2 on port 10000
- SQLAlchemy URI: `hive://CLUSTER_C_MASTER:10000/smart_grid`
- Superset caches query results for 5 minutes

---

## Stateful Streaming — State Management

The anomaly detector uses `applyInPandasWithState` (Spark 3.4+).

**State per meter:**
```
{
  meter_id:          "MAC000002",
  count:             8760,         # Total readings processed
  rolling_mean:      0.312,        # Welford online mean
  rolling_stddev:    0.089,        # Welford online stddev
  consecutive_high:  0,            # Consecutive above-threshold count
  last_n_zscore:     [...],        # JSON list of last 48 z-scores
  last_ts:           "2012-03-15T14:00:00"
}
```

**Welford's Algorithm (numerically stable online mean/variance):**
```
count  += 1
delta   = new_value - mean
mean   += delta / count
delta2  = new_value - mean
M2     += delta * delta2
variance = M2 / count
stddev   = sqrt(variance)
```

**Why not a window function?**
- Window functions require all data in a time window to be in memory
- For 5,567 households × 7-day rolling window = 940K+ rows in memory
- mapGroupsWithState stores only 8 scalar stats per meter → ~450 KB total

---

## HDFS Storage Layout

```
/smart_grid/
├── raw/
│   ├── london/           # Original CSVs from Kaggle
│   ├── pjm/              # PJM demand CSVs
│   └── uci/              # UCI household power CSV
├── clean/
│   ├── meter_readings/   # Pig ETL output (CSV)
│   └── household_stats/  # Per-meter baseline stats (CSV)
├── parquet/
│   ├── meter_readings/   # year=YYYY/month=MM partitioned Parquet
│   │   ├── year=2012/
│   │   │   ├── month=01/
│   │   │   └── ...
│   │   └── year=2013/
│   ├── household_stats/  # Single Parquet file (5,567 rows)
│   └── pjm_demand/       # year/month partitioned Parquet
├── streaming/
│   └── meter_readings/   # Live stream output (appended by Spark)
├── models/
│   └── gbt_demand_forecast/  # Saved PipelineModel
├── forecasts/
│   └── AEP/              # Hourly forecast output
├── warehouse/            # Hive managed tables
└── checkpoints/          # Spark Streaming checkpoints
    └── anomaly_detector/
        ├── hdfs/
        ├── hbase/
        └── kafka_out/
```

---

## HBase Schema

### Table: `meter_readings`
```
Row key: MAC000002_99991234567   (meter_id + reverse_epoch)
cf:reading:kwh_hourly  → "0.312"
cf:reading:kwh_max     → "0.201"
cf:reading:ts          → "2013-06-15T14:00:00"
cf:meta:tariff_type    → "Std"
cf:meta:acorn_group    → "Adversity"
```

### Table: `anomaly_alerts`
```
Row key: MAC000002_99991234567
cf:meter_id      → "MAC000002"
cf:ts            → "2013-06-15T14:00:00"
cf:kwh_hourly    → "3.847"
cf:z_score       → "5.23"
cf:anomaly_type  → "sudden_spike"
cf:severity      → "1.74"
cf:acorn_group   → "Adversity"
cf:status        → "open"
cf:created_at    → "2013-06-15T14:00:31"
```

### Table: `meter_state`
```
Row key: MAC000002
stats:count            → "8760"
stats:rolling_mean     → "0.31245"
stats:rolling_stddev   → "0.08912"
stats:consecutive_high → "0"
stats:last_updated     → "2013-06-15T14:00:31"
```

---

## ML Model Details

### GBT Demand Forecasting Model
- Algorithm: Gradient Boosted Trees (Spark MLlib GBTRegressor)
- Training data: PJM hourly demand, ~8 years, one zone
- Features: 21 (temporal, lag, rolling, seasonal, cyclical)
- Train/test split: chronological (last 20% = test)
- Expected metrics: RMSE ~150 MW, MAE ~110 MW, R² ~0.97
- Model artifacts: PipelineModel (VectorAssembler + StandardScaler + GBT)

### Z-Score Anomaly Detection
- Method: Welford's online algorithm (per meter, stateful)
- Threshold: |Z| > 3.0 (configurable via env var)
- Warm-up: minimum 10 readings before scoring
- State timeout: 30 days of inactivity clears meter state

---

## Port Reference

| Service | Host | Port |
|---|---|---|
| HDFS NameNode RPC | Cluster B | 9000 |
| HDFS NameNode Web | Cluster B | 9870 |
| YARN ResourceManager | Cluster B | 8088 |
| HBase Master RPC | Cluster B | 16000 |
| HBase Master Web | Cluster B | 16010 |
| HBase Thrift | Cluster B | 9090 |
| HBase Zookeeper | Cluster B | 2181 |
| Kafka Broker | Cluster A | 9092 |
| Kafka Zookeeper | Cluster A | 2181 |
| Spark Master (A) | Cluster A | 7077 |
| Spark Web UI (A) | Cluster A | 8080 |
| Spark Master (C) | Cluster C | 7077 |
| Spark Web UI (C) | Cluster C | 8080 |
| Hive Metastore | Cluster C | 9083 |
| HiveServer2 | Cluster C | 10000 |
| Superset | Cluster C | 8088 |
| HBase Flask API | Cluster B | 5050 |
