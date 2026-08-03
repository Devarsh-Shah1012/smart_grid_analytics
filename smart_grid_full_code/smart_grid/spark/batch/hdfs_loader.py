#!/usr/bin/env python3
"""
spark/batch/hdfs_loader.py
==========================
Converts Pig ETL output (CSV on HDFS) to optimised Parquet format.
Also loads raw London/PJM datasets directly when skipping Pig.

Run:
  spark-submit \
    --master spark://CLUSTER_C_MASTER:7077 \
    --executor-memory 4g \
    --num-executors 4 \
    spark/batch/hdfs_loader.py
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, DoubleType, IntegerType, TimestampType
)

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [HDFS_LOADER] %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
HDFS_ROOT       = os.getenv("HDFS_NAMENODE",   "hdfs://localhost:9000")
HIVE_METASTORE  = os.getenv("HIVE_METASTORE",  "thrift://localhost:9083")
PIG_CLEAN_OUT   = f"{HDFS_ROOT}/smart_grid/clean/meter_readings"
PIG_STATS_OUT   = f"{HDFS_ROOT}/smart_grid/clean/household_stats"
PARQUET_METERS  = f"{HDFS_ROOT}/smart_grid/parquet/meter_readings"
PARQUET_STATS   = f"{HDFS_ROOT}/smart_grid/parquet/household_stats"
PJM_RAW         = f"{HDFS_ROOT}/smart_grid/raw/pjm"
PARQUET_PJM     = f"{HDFS_ROOT}/smart_grid/parquet/pjm_demand"


def build_spark() -> SparkSession:
    return (SparkSession.builder
        .appName("SmartGrid_HDFS_Loader")
        .config("spark.sql.warehouse.dir", f"{HDFS_ROOT}/smart_grid/warehouse")
        .config("hive.metastore.uris", HIVE_METASTORE)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .enableHiveSupport()
        .getOrCreate())


# ── Schema for Pig CSV output ─────────────────────────────────
PIG_SCHEMA = StructType([
    StructField("meter_id",     StringType(),  True),
    StructField("reading_date", StringType(),  True),
    StructField("hour",         StringType(),  True),
    StructField("tariff_type",  StringType(),  True),
    StructField("acorn_group",  StringType(),  True),
    StructField("kwh_hourly",   FloatType(),   True),
    StructField("kwh_max",      FloatType(),   True),
    StructField("temp_high_c",  DoubleType(),  True),
    StructField("temp_low_c",   DoubleType(),  True),
    StructField("humidity",     DoubleType(),  True),
])

STATS_SCHEMA = StructType([
    StructField("meter_id",       StringType(),  True),
    StructField("total_readings", IntegerType(), True),
    StructField("global_mean",    DoubleType(),  True),
    StructField("global_min",     FloatType(),   True),
    StructField("global_max",     FloatType(),   True),
    StructField("global_stddev",  DoubleType(),  True),
])


def load_meter_readings(spark: SparkSession):
    """
    Load Pig-cleaned CSV → add computed columns → write Parquet.
    Partitioned by year and month for efficient Hive queries.
    """
    log.info("Loading meter readings from Pig ETL output...")
    
    df = spark.read.csv(PIG_CLEAN_OUT, schema=PIG_SCHEMA, header=False)
    
    # ── Build full timestamp ──────────────────────────────────
    df = df.withColumn(
        "ts",
        F.to_timestamp(
            F.concat(F.col("reading_date"), F.lit("T"), F.col("hour"), F.lit(":00:00")),
            "yyyy-MM-dd'T'HH:mm:ss"
        )
    )
    
    # ── Extract partition columns ─────────────────────────────
    df = (df
        .withColumn("year",  F.year("ts").cast(StringType()))
        .withColumn("month", F.lpad(F.month("ts").cast(StringType()), 2, "0"))
    )
    
    # ── Compute temperature features (useful for ML) ──────────
    df = df.withColumn("temp_range_c", F.col("temp_high_c") - F.col("temp_low_c"))
    
    # ── Deduplicate on (meter_id, ts) ─────────────────────────
    df = df.dropDuplicates(["meter_id", "ts"])
    
    row_count = df.count()
    log.info(f"  Loaded {row_count:,} meter reading rows")
    log.info(f"  Writing Parquet to: {PARQUET_METERS}")
    
    # ── Write partitioned Parquet ─────────────────────────────
    (df.repartition(50)
       .write
       .mode("overwrite")
       .partitionBy("year", "month")
       .parquet(PARQUET_METERS))
    
    log.info("  [OK] Meter readings Parquet written")
    return df


def load_household_stats(spark: SparkSession):
    """Load Pig household stats CSV → Parquet (no partitioning needed)."""
    log.info("Loading household statistics...")
    
    df = spark.read.csv(PIG_STATS_OUT, schema=STATS_SCHEMA, header=False)
    df = df.dropDuplicates(["meter_id"])
    
    log.info(f"  Loaded {df.count():,} household stat rows")
    
    (df.write
       .mode("overwrite")
       .parquet(PARQUET_STATS))
    
    log.info("  [OK] Household stats Parquet written")
    return df


def load_pjm_demand(spark: SparkSession):
    """
    Load PJM hourly energy demand CSV files.
    All zone files are unioned into a single Parquet store.
    """
    log.info("Loading PJM hourly demand data...")
    
    # PJM files have format: Datetime, AEP_MW  (column name varies by zone)
    # Load all CSVs with generic column names then normalise
    raw = spark.read.option("header", "true").csv(f"{PJM_RAW}/*.csv")
    
    # Columns are: Datetime, <ZONE>_MW
    # Pivot to a normalised schema: datetime, zone, mw_demand
    cols = raw.columns
    datetime_col = cols[0]   # Always "Datetime"
    demand_col   = cols[1]   # e.g. "AEP_MW", "DOM_MW", etc.
    zone_name    = demand_col.replace("_MW", "")
    
    df = (raw
        .select(
            F.col(datetime_col).alias("datetime_str"),
            F.col(demand_col).cast(DoubleType()).alias("mw_demand")
        )
        .withColumn("zone", F.lit(zone_name))
        .withColumn(
            "ts",
            F.to_timestamp("datetime_str", "yyyy-MM-dd HH:mm:ss")
        )
        .drop("datetime_str")
        .withColumn("year",  F.year("ts").cast(StringType()))
        .withColumn("month", F.lpad(F.month("ts").cast(StringType()), 2, "0"))
        .withColumn("hour",  F.hour("ts"))
        .withColumn("day_of_week", F.dayofweek("ts"))
        .withColumn("is_weekend",
            F.when(F.dayofweek("ts").isin([1, 7]), True).otherwise(False))
        .dropna(subset=["ts", "mw_demand"])
    )
    
    log.info(f"  Loaded {df.count():,} PJM demand rows for zone: {zone_name}")
    
    (df.repartition(10)
       .write
       .mode("overwrite")
       .partitionBy("year", "month")
       .parquet(PARQUET_PJM))
    
    log.info("  [OK] PJM demand Parquet written")
    return df


def print_summary(spark: SparkSession):
    """Print row counts and sample rows for verification."""
    log.info("\n=== Data Load Summary ===")
    
    for label, path in [
        ("Meter Readings", PARQUET_METERS),
        ("Household Stats", PARQUET_STATS),
        ("PJM Demand",     PARQUET_PJM),
    ]:
        try:
            df = spark.read.parquet(path)
            log.info(f"  {label:20s} : {df.count():>12,} rows @ {path}")
        except Exception as e:
            log.warning(f"  {label:20s} : FAILED — {e}")


# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        load_meter_readings(spark)
        load_household_stats(spark)
        load_pjm_demand(spark)
        print_summary(spark)
    finally:
        spark.stop()
        log.info("HDFS Loader complete.")
