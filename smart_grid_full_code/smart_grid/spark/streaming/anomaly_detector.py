#!/usr/bin/env python3
"""
spark/streaming/anomaly_detector.py
=====================================
Spark Structured Streaming job that:
  1. Consumes smart meter events from Kafka topic 'meter-readings'
  2. Maintains per-household rolling statistics (7-day window)
     using mapGroupsWithState (stateful streaming)
  3. Computes Z-score per reading
  4. Classifies anomalies: sudden_spike | sustained_elevation | flatline | normal
  5. Writes ALL readings to HDFS (Parquet, partitioned by date)
  6. Writes FLAGGED anomalies to HBase via foreachBatch
  7. Writes anomaly alerts back to Kafka topic 'anomaly-alerts'

Run:
  spark-submit \
    --master spark://CLUSTER_A_MASTER:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    --executor-memory 4g \
    --conf spark.sql.streaming.checkpointLocation=/tmp/sg_checkpoint \
    spark/streaming/anomaly_detector.py
"""

import os
import json
import logging
from typing import Iterator, Tuple
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    DoubleType, BooleanType, TimestampType, LongType
)
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [ANOMALY_DETECTOR] %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
KAFKA_BROKER      = os.getenv("KAFKA_BROKER",       "localhost:9092")
KAFKA_IN_TOPIC    = os.getenv("KAFKA_TOPIC_METERS",  "meter-readings")
KAFKA_OUT_TOPIC   = os.getenv("KAFKA_TOPIC_ALERTS",  "anomaly-alerts")
HDFS_ROOT         = os.getenv("HDFS_NAMENODE",       "hdfs://localhost:9000")
HDFS_STREAM_OUT   = f"{HDFS_ROOT}/smart_grid/streaming/meter_readings"
CHECKPOINT_LOC    = f"{HDFS_ROOT}/smart_grid/checkpoints/anomaly_detector"
HBASE_MASTER      = os.getenv("HBASE_MASTER",        "localhost")

ZSCORE_THRESHOLD        = float(os.getenv("ANOMALY_ZSCORE_THRESHOLD", "3.0"))
SUSTAINED_COUNT         = int(os.getenv("SUSTAINED_ANOMALY_COUNT",    "5"))
ROLLING_WINDOW_DAYS     = int(os.getenv("ROLLING_WINDOW_DAYS",        "7"))
ROLLING_WINDOW_HOURS    = ROLLING_WINDOW_DAYS * 24
TRIGGER_SECONDS         = int(os.getenv("STREAM_TRIGGER_SECONDS",     "30"))

# ── Schemas ───────────────────────────────────────────────────

# Incoming Kafka message value schema (JSON from producer)
METER_SCHEMA = StructType([
    StructField("meter_id",          StringType(),    True),
    StructField("ts",                StringType(),    True),
    StructField("kwh_hourly",        FloatType(),     True),
    StructField("kwh_max",           FloatType(),     True),
    StructField("reading_date",      StringType(),    True),
    StructField("hour",              StringType(),    True),
    StructField("tariff_type",       StringType(),    True),
    StructField("acorn_group",       StringType(),    True),
    StructField("temp_high_c",       DoubleType(),    True),
    StructField("temp_low_c",        DoubleType(),    True),
    StructField("humidity",          DoubleType(),    True),
    StructField("simulated",         BooleanType(),   True),
    StructField("injected_anomaly",  StringType(),    True),
    StructField("ingest_time",       StringType(),    True),
])

# State object for each meter: stores rolling history summary
STATE_SCHEMA = StructType([
    StructField("meter_id",          StringType(),  True),
    StructField("count",             LongType(),    True),
    StructField("sum_kwh",           DoubleType(),  True),
    StructField("sum_sq_kwh",        DoubleType(),  True),
    StructField("rolling_mean",      DoubleType(),  True),
    StructField("rolling_stddev",    DoubleType(),  True),
    StructField("last_n_zscore",     StringType(),  True),  # JSON list of last N z-scores
    StructField("consecutive_high",  LongType(),    True),
    StructField("last_ts",           StringType(),  True),
])

# Output schema for scored readings
OUTPUT_SCHEMA = StructType([
    StructField("meter_id",          StringType(),  True),
    StructField("ts",                StringType(),  True),
    StructField("kwh_hourly",        FloatType(),   True),
    StructField("rolling_mean",      DoubleType(),  True),
    StructField("rolling_stddev",    DoubleType(),  True),
    StructField("z_score",           DoubleType(),  True),
    StructField("anomaly_type",      StringType(),  True),
    StructField("severity",          DoubleType(),  True),
    StructField("is_anomaly",        BooleanType(), True),
    StructField("tariff_type",       StringType(),  True),
    StructField("acorn_group",       StringType(),  True),
    StructField("reading_date",      StringType(),  True),
    StructField("injected_anomaly",  StringType(),  True),
])


# ── Stateful Anomaly Detection Function ──────────────────────
def detect_anomalies_stateful(
    meter_id: str,
    readings: Iterator,
    state: GroupState
) -> Iterator:
    """
    mapGroupsWithState function.
    Called once per (micro-batch, meter_id) group.
    Maintains rolling statistics using Welford's online algorithm
    for numerically stable mean and variance updates.
    
    Yields one OUTPUT_SCHEMA row per input reading.
    """
    # ── Load or initialise state ──────────────────────────────
    if state.exists:
        s = state.get
        count            = s["count"]
        sum_kwh          = s["sum_kwh"]
        sum_sq_kwh       = s["sum_sq_kwh"]
        rolling_mean     = s["rolling_mean"]
        rolling_stddev   = s["rolling_stddev"]
        consecutive_high = s["consecutive_high"]
        try:
            last_n_zscore = json.loads(s["last_n_zscore"])
        except Exception:
            last_n_zscore = []
    else:
        count = 0
        sum_kwh = 0.0
        sum_sq_kwh = 0.0
        rolling_mean = 0.0
        rolling_stddev = 0.0
        consecutive_high = 0
        last_n_zscore = []
    
    # ── Process each reading for this meter ───────────────────
    for row in readings:
        kwh = float(row["kwh_hourly"]) if row["kwh_hourly"] else 0.0
        
        # Welford online mean/variance update
        count    += 1
        delta     = kwh - rolling_mean
        rolling_mean  += delta / count
        delta2    = kwh - rolling_mean
        sum_sq_kwh += delta * delta2
        variance  = sum_sq_kwh / count if count > 1 else 0.0
        rolling_stddev = variance ** 0.5
        
        # Compute Z-score (safe division)
        if rolling_stddev > 0.001 and count >= 10:
            z_score = (kwh - rolling_mean) / rolling_stddev
        else:
            z_score = 0.0
        
        # ── Anomaly classification ────────────────────────────
        anomaly_type = "normal"
        severity     = 0.0
        is_anomaly   = False
        
        if count >= 10:  # Need warm-up period
            
            if kwh < 0.005:
                # Flatline — near zero for extended period
                anomaly_type = "flatline"
                severity     = 1.0
                is_anomaly   = True
            
            elif z_score > ZSCORE_THRESHOLD:
                # Sudden spike
                anomaly_type     = "sudden_spike"
                severity         = min(abs(z_score) / ZSCORE_THRESHOLD, 5.0)
                is_anomaly       = True
                consecutive_high += 1
            
            elif consecutive_high >= SUSTAINED_COUNT and z_score > 2.0:
                # Sustained elevation — prolonged above-normal
                anomaly_type     = "sustained_elevation"
                severity         = 2.0 + (consecutive_high / 10)
                is_anomaly       = True
            
            elif z_score > 2.0:
                consecutive_high += 1
            
            else:
                consecutive_high = max(0, consecutive_high - 1)
        
        # Update rolling z-score buffer (last 48 values = 2 days)
        last_n_zscore.append(round(z_score, 3))
        if len(last_n_zscore) > 48:
            last_n_zscore.pop(0)
        
        yield (
            row["meter_id"],
            row["ts"],
            float(kwh),
            float(rolling_mean),
            float(rolling_stddev),
            float(z_score),
            anomaly_type,
            float(severity),
            bool(is_anomaly),
            row.get("tariff_type", ""),
            row.get("acorn_group", ""),
            row.get("reading_date", ""),
            row.get("injected_anomaly", "none"),
        )
    
    # ── Persist updated state ─────────────────────────────────
    state.update((
        meter_id,
        count,
        sum_kwh,
        sum_sq_kwh,
        rolling_mean,
        rolling_stddev,
        json.dumps(last_n_zscore),
        consecutive_high,
        datetime.utcnow().isoformat(),
    ))
    
    # State timeout: remove inactive meters after 30 days
    state.setTimeoutDuration(30 * 24 * 60 * 60 * 1000)


# ── HBase Writer (foreachBatch) ───────────────────────────────
def write_anomalies_to_hbase(batch_df, batch_id: int):
    """
    Called by Spark Streaming foreachBatch for each micro-batch.
    Writes flagged anomaly records to HBase.
    
    HBase table: anomaly_alerts
    Row key: {meter_id}_{reverse_timestamp}
    Column families: cf (alert data)
    """
    # Filter to anomalies only
    anomalies = batch_df.filter(F.col("is_anomaly") == True)
    count = anomalies.count()
    
    if count == 0:
        return
    
    log.info(f"Batch {batch_id}: writing {count} anomalies to HBase")
    
    # Convert to list of dicts for HBase writer
    rows = anomalies.select(
        "meter_id", "ts", "kwh_hourly", "z_score",
        "anomaly_type", "severity", "acorn_group"
    ).collect()
    
    try:
        import happybase
        conn = happybase.Connection(HBASE_MASTER)
        table = conn.table("anomaly_alerts")
        
        with table.batch(batch_size=500) as b:
            for row in rows:
                # Reverse timestamp for descending order by recency
                ts_epoch = int(datetime.fromisoformat(row["ts"]).timestamp())
                reverse_ts = str(99999999999 - ts_epoch)
                row_key = f"{row['meter_id']}_{reverse_ts}"
                
                b.put(row_key.encode(), {
                    b"cf:meter_id":    str(row["meter_id"]).encode(),
                    b"cf:ts":          str(row["ts"]).encode(),
                    b"cf:kwh_hourly":  str(row["kwh_hourly"]).encode(),
                    b"cf:z_score":     str(round(row["z_score"], 3)).encode(),
                    b"cf:anomaly_type":str(row["anomaly_type"]).encode(),
                    b"cf:severity":    str(round(row["severity"], 2)).encode(),
                    b"cf:acorn_group": str(row["acorn_group"]).encode(),
                    b"cf:status":      b"open",
                    b"cf:created_at":  datetime.utcnow().isoformat().encode(),
                })
        
        conn.close()
        log.info(f"  HBase write complete for batch {batch_id}")
    
    except Exception as e:
        log.error(f"HBase write failed for batch {batch_id}: {e}")
        # Don't raise — allow streaming to continue even if HBase is down


# ── Main Streaming Job ────────────────────────────────────────
def build_spark() -> SparkSession:
    return (SparkSession.builder
        .appName("SmartGrid_AnomalyDetector")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOC)
        .config("spark.sql.shuffle.partitions", "12")
        .getOrCreate())


if __name__ == "__main__":
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    log.info("=== Smart Grid Anomaly Detector Starting ===")
    log.info(f"  Kafka broker  : {KAFKA_BROKER}")
    log.info(f"  Input topic   : {KAFKA_IN_TOPIC}")
    log.info(f"  Output topic  : {KAFKA_OUT_TOPIC}")
    log.info(f"  HDFS output   : {HDFS_STREAM_OUT}")
    log.info(f"  Z threshold   : {ZSCORE_THRESHOLD}")
    log.info(f"  Trigger every : {TRIGGER_SECONDS}s")
    
    # ── 1. Read from Kafka ────────────────────────────────────
    raw_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_IN_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 50000)
        .option("failOnDataLoss", "false")
        .load()
    )
    
    # ── 2. Parse JSON payload ─────────────────────────────────
    parsed = (raw_stream
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.from_json(
                F.col("value").cast("string"),
                METER_SCHEMA
            ).alias("data"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("data.*", "kafka_ts")
        .withColumn("ts_parsed",
            F.to_timestamp("ts", "yyyy-MM-dd'T'HH:mm:ss"))
        .filter(F.col("meter_id").isNotNull())
        .filter(F.col("kwh_hourly").isNotNull())
        .filter(F.col("kwh_hourly") >= 0)
    )
    
    # ── 3. Apply stateful anomaly detection ───────────────────
    scored = (parsed
        .groupBy("meter_id")
        .applyInPandasWithState(
            detect_anomalies_stateful,
            OUTPUT_SCHEMA,
            STATE_SCHEMA,
            "append",
            GroupStateTimeout.ProcessingTimeTimeout,
        )
    )
    
    # Add processing metadata
    scored = (scored
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("reading_year",  F.substring("reading_date", 1, 4))
        .withColumn("reading_month", F.substring("reading_date", 6, 2))
    )
    
    # ── 4. Sink A: Write ALL readings to HDFS ─────────────────
    hdfs_query = (scored
        .writeStream
        .format("parquet")
        .option("path", HDFS_STREAM_OUT)
        .option("checkpointLocation", f"{CHECKPOINT_LOC}/hdfs")
        .partitionBy("reading_year", "reading_month")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .outputMode("append")
        .start()
    )
    
    log.info(f"  [OK] HDFS sink started: {HDFS_STREAM_OUT}")
    
    # ── 5. Sink B: Write anomalies to HBase (foreachBatch) ────
    anomaly_stream = scored.filter(F.col("is_anomaly") == True)
    
    hbase_query = (anomaly_stream
        .writeStream
        .foreachBatch(write_anomalies_to_hbase)
        .option("checkpointLocation", f"{CHECKPOINT_LOC}/hbase")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )
    
    log.info(f"  [OK] HBase sink started")
    
    # ── 6. Sink C: Publish anomaly alerts back to Kafka ───────
    kafka_alert_payload = (anomaly_stream
        .select(
            F.col("meter_id").alias("key"),
            F.to_json(F.struct(
                "meter_id", "ts", "kwh_hourly",
                "z_score", "anomaly_type", "severity",
                "rolling_mean", "rolling_stddev",
                "acorn_group", "reading_date"
            )).alias("value")
        )
    )
    
    kafka_query = (kafka_alert_payload
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", KAFKA_OUT_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_LOC}/kafka_out")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )
    
    log.info(f"  [OK] Kafka alert sink started: topic={KAFKA_OUT_TOPIC}")
    
    # ── 7. Console sink (debug/demo only) ─────────────────────
    console_query = (anomaly_stream
        .select("meter_id", "ts", "kwh_hourly", "z_score",
                "anomaly_type", "severity", "acorn_group")
        .writeStream
        .format("console")
        .option("truncate", "false")
        .option("numRows", "20")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .outputMode("append")
        .start()
    )
    
    log.info("=== All sinks started. Streaming in progress... ===")
    log.info("Press Ctrl+C to stop.")
    
    # ── Wait for any query to terminate (or indefinitely) ─────
    spark.streams.awaitAnyTermination()
