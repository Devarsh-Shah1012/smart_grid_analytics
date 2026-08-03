#!/usr/bin/env python3
"""
spark/batch/run_forecast.py
============================
Hourly batch job: loads the trained GBT model and generates a
24-hour-ahead demand forecast. Results are written to HDFS
(Parquet) and a Hive table so Superset can read them.

Run manually or via cron/Airflow every hour:
  spark-submit \
    --master spark://CLUSTER_C_MASTER:7077 \
    --executor-memory 4g \
    spark/batch/run_forecast.py [--zone AEP] [--hours 24]
"""

import os
import argparse
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
from pyspark.ml import PipelineModel

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [RUN_FORECAST] %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

HDFS_ROOT      = os.getenv("HDFS_NAMENODE",  "hdfs://localhost:9000")
HIVE_METASTORE = os.getenv("HIVE_METASTORE", "thrift://localhost:9083")
MODEL_PATH     = f"{HDFS_ROOT}/smart_grid/models/gbt_demand_forecast"
PJM_PARQUET    = f"{HDFS_ROOT}/smart_grid/parquet/pjm_demand"
FORECAST_PATH  = f"{HDFS_ROOT}/smart_grid/forecasts"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--zone",  default="AEP", help="Grid zone name")
    p.add_argument("--hours", type=int, default=24, help="Forecast horizon in hours")
    return p.parse_args()


def build_spark():
    return (SparkSession.builder
        .appName("SmartGrid_RunForecast")
        .config("hive.metastore.uris", HIVE_METASTORE)
        .enableHiveSupport()
        .getOrCreate())


def get_recent_history(spark, zone: str, lookback_hours: int = 200):
    """
    Pull the most recent N hours of actual demand for a zone.
    Used to build lag features for the forecast window.
    """
    df = spark.read.parquet(PJM_PARQUET)
    df = df.filter(F.col("zone") == zone)
    
    # Get the most recent timestamp in the data
    max_ts = df.agg(F.max("ts")).collect()[0][0]
    cutoff = max_ts - timedelta(hours=lookback_hours)
    
    history = (df
        .filter(F.col("ts") >= cutoff)
        .orderBy("ts")
        .select("ts", "mw_demand", "hour", "month", "day_of_week",
                "is_weekend", "zone")
        .withColumn("hour",       F.col("hour").cast(DoubleType()))
        .withColumn("month",      F.col("month").cast(DoubleType()))
        .withColumn("is_weekend", F.col("is_weekend").cast(DoubleType()))
    )
    
    log.info(f"History rows for zone {zone}: {history.count():,}")
    return history, max_ts


def build_forecast_window(spark, history_df, zone: str, forecast_hours: int, anchor_ts):
    """
    Build a DataFrame of future time slots (next N hours) with
    all required features populated from historical lag values.
    
    For lag features, we use the most recent available actuals.
    """
    log.info(f"Building {forecast_hours}-hour forecast window from {anchor_ts}")
    
    # Collect recent history to driver for lag feature lookup
    history_list = (history_df
        .orderBy("ts")
        .select("ts", "mw_demand")
        .collect())
    
    # Build lookup: ts → mw_demand
    demand_lookup = {row["ts"]: row["mw_demand"] for row in history_list}
    demand_series = [(row["ts"], row["mw_demand"]) for row in history_list]
    
    # Build list of future slots
    forecast_rows = []
    for h in range(1, forecast_hours + 1):
        future_ts = anchor_ts + timedelta(hours=h)
        
        def get_lag(lag_h):
            target = anchor_ts + timedelta(hours=h) - timedelta(hours=lag_h)
            return demand_lookup.get(target, None)
        
        # Rolling 7-day mean from historical series
        rolling_vals = [
            row[1] for row in demand_series
            if row[0] <= anchor_ts + timedelta(hours=h)
               and row[0] >= anchor_ts + timedelta(hours=h) - timedelta(days=7)
        ]
        rolling_mean   = sum(rolling_vals) / len(rolling_vals) if rolling_vals else 0.0
        rolling_stddev = (
            (sum((x - rolling_mean)**2 for x in rolling_vals) / len(rolling_vals)) ** 0.5
            if len(rolling_vals) > 1 else 0.0
        )
        
        rolling_24h = [
            row[1] for row in demand_series
            if row[0] <= anchor_ts + timedelta(hours=h)
               and row[0] >= anchor_ts + timedelta(hours=h) - timedelta(hours=24)
        ]
        rolling_24h_mean = sum(rolling_24h) / len(rolling_24h) if rolling_24h else rolling_mean
        
        import math
        hour_val  = float(future_ts.hour)
        month_val = float(future_ts.month)
        dow       = float(future_ts.weekday() + 1)
        is_wknd   = 1.0 if future_ts.weekday() >= 5 else 0.0
        
        row = {
            "ts":                 future_ts,
            "zone":               zone,
            "mw_demand":          0.0,   # Placeholder; not used in inference
            "hour":               hour_val,
            "month":              month_val,
            "day_of_week":        dow,
            "is_weekend":         is_wknd,
            "lag_1h":             get_lag(1),
            "lag_2h":             get_lag(2),
            "lag_24h":            get_lag(24),
            "lag_168h":           get_lag(168),
            "rolling_7d_mean":    rolling_mean,
            "rolling_7d_stddev":  rolling_stddev,
            "rolling_24h_mean":   rolling_24h_mean,
            "seasonal_index":     rolling_mean,  # Simplified; Hive join not available here
            "seasonal_deviation": 0.0,
            "hour_sin":           math.sin(2 * math.pi * hour_val / 24),
            "hour_cos":           math.cos(2 * math.pi * hour_val / 24),
            "month_sin":          math.sin(2 * math.pi * month_val / 12),
            "month_cos":          math.cos(2 * math.pi * month_val / 12),
            "dow_1":  1.0 if dow == 1 else 0.0,
            "dow_2":  1.0 if dow == 2 else 0.0,
            "dow_3":  1.0 if dow == 3 else 0.0,
            "dow_4":  1.0 if dow == 4 else 0.0,
            "dow_5":  1.0 if dow == 5 else 0.0,
            "dow_6":  1.0 if dow == 6 else 0.0,
            "dow_7":  1.0 if dow == 7 else 0.0,
        }
        forecast_rows.append(row)
    
    forecast_df = spark.createDataFrame(forecast_rows)
    # Drop rows with null lags (warm-up period)
    forecast_df = forecast_df.dropna(subset=["lag_24h", "lag_168h"])
    
    log.info(f"  Forecast window rows: {forecast_df.count()}")
    return forecast_df


def save_forecast(spark, predictions_df, zone: str, run_ts: str):
    """
    Save forecast to HDFS Parquet and write to Hive table.
    """
    result = (predictions_df
        .select("ts", "zone", "prediction", "mw_demand")
        .withColumnRenamed("prediction", "predicted_mw")
        .withColumnRenamed("mw_demand",  "actual_mw")
        .withColumn("forecast_run_ts", F.lit(run_ts))
        .withColumn("year",  F.year("ts").cast(StringType()))
        .withColumn("month", F.lpad(F.month("ts").cast(StringType()), 2, "0"))
    )
    
    out_path = f"{FORECAST_PATH}/{zone}"
    result.write.mode("overwrite").partitionBy("year", "month").parquet(out_path)
    log.info(f"  Forecast Parquet saved to: {out_path}")
    
    # Write to Hive
    result.write.mode("overwrite").saveAsTable("smart_grid.demand_forecasts")
    log.info("  Forecast written to Hive table: smart_grid.demand_forecasts")
    
    # Print sample
    log.info("=== Forecast Sample (next 24h) ===")
    result.select("ts", "predicted_mw").orderBy("ts").show(24, truncate=False)


# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        run_ts = datetime.utcnow().isoformat()
        log.info(f"=== Forecast Run: {run_ts} | Zone: {args.zone} ===")
        
        # Load history
        history, anchor_ts = get_recent_history(spark, args.zone)
        
        # Build feature window
        forecast_df = build_forecast_window(
            spark, history, args.zone, args.hours, anchor_ts)
        
        # Load model and predict
        log.info(f"Loading model from: {MODEL_PATH}")
        model = PipelineModel.load(MODEL_PATH)
        
        predictions = model.transform(forecast_df)
        
        # Save results
        save_forecast(spark, predictions, args.zone, run_ts)
    
    finally:
        spark.stop()
        log.info("Forecast job complete.")
