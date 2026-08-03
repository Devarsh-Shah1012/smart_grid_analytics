#!/usr/bin/env python3
"""
spark/batch/train_forecast_model.py
====================================
Trains a Gradient Boosted Tree (GBT) Regressor using Spark MLlib
to forecast 24-hour-ahead electricity demand.

Features engineered from PJM hourly demand data:
  - Temporal: hour, day_of_week, month, is_weekend
  - Lag features: demand at t-1h, t-2h, t-24h, t-168h (1 week)
  - Rolling statistics: 7-day rolling mean and stddev
  - Seasonal index: average demand for that (month, hour) pair

Model is saved to HDFS for use by run_forecast.py and
the Spark Streaming job.

Run:
  spark-submit \
    --master spark://CLUSTER_C_MASTER:7077 \
    --executor-memory 6g \
    --num-executors 4 \
    spark/batch/train_forecast_model.py
"""

import os
import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [TRAIN_MODEL] %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
HDFS_ROOT    = os.getenv("HDFS_NAMENODE",   "hdfs://localhost:9000")
PJM_PARQUET  = f"{HDFS_ROOT}/smart_grid/parquet/pjm_demand"
MODEL_PATH   = f"{HDFS_ROOT}/smart_grid/models/gbt_demand_forecast"
METRICS_PATH = f"{HDFS_ROOT}/smart_grid/models/gbt_metrics.csv"
TEST_SIZE    = 0.2
RANDOM_SEED  = 42


def build_spark() -> SparkSession:
    return (SparkSession.builder
        .appName("SmartGrid_Train_ForecastModel")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "50")
        .getOrCreate())


def engineer_features(df):
    """
    Add all lag, rolling, and seasonal features required by the GBT model.
    Uses Spark Window functions — no UDFs, fully distributed.
    """
    log.info("Engineering features...")
    
    # ── Sort by timestamp within each zone ───────────────────
    zone_time_window = Window.partitionBy("zone").orderBy("ts")
    
    # ── Lag features ─────────────────────────────────────────
    # t-1h: 1 row back within zone
    # t-2h: 2 rows back
    # t-24h: 24 rows back (1 day ago, same hour)
    # t-168h: 168 rows back (1 week ago, same hour)
    df = (df
        .withColumn("lag_1h",   F.lag("mw_demand", 1).over(zone_time_window))
        .withColumn("lag_2h",   F.lag("mw_demand", 2).over(zone_time_window))
        .withColumn("lag_24h",  F.lag("mw_demand", 24).over(zone_time_window))
        .withColumn("lag_168h", F.lag("mw_demand", 168).over(zone_time_window))
    )
    
    # ── Rolling statistics (7-day = 168 hours) ───────────────
    rolling_window = (zone_time_window
        .rowsBetween(-167, 0))
    
    df = (df
        .withColumn("rolling_7d_mean",
            F.avg("mw_demand").over(rolling_window))
        .withColumn("rolling_7d_stddev",
            F.stddev("mw_demand").over(rolling_window))
        .withColumn("rolling_24h_mean",
            F.avg("mw_demand").over(
                zone_time_window.rowsBetween(-23, 0)))
    )
    
    # ── Seasonal index: average demand for (zone, month, hour) ─
    seasonal_avg = (df
        .groupBy("zone", "month", "hour")
        .agg(F.avg("mw_demand").alias("seasonal_index")))
    
    df = df.join(seasonal_avg, on=["zone", "month", "hour"], how="left")
    
    # ── Deviation from seasonal index ────────────────────────
    df = df.withColumn("seasonal_deviation",
        F.col("mw_demand") - F.col("seasonal_index"))
    
    # ── One-hot encode day_of_week (1–7) ─────────────────────
    for d in range(1, 8):
        df = df.withColumn(
            f"dow_{d}",
            F.when(F.col("day_of_week") == d, 1.0).otherwise(0.0)
        )
    
    # ── Cyclical encoding of hour (sin/cos) ──────────────────
    # Better than raw hour for capturing cyclical time-of-day pattern
    df = (df
        .withColumn("hour_sin", F.sin(2 * 3.14159 * F.col("hour") / 24))
        .withColumn("hour_cos", F.cos(2 * 3.14159 * F.col("hour") / 24))
        .withColumn("month_sin", F.sin(2 * 3.14159 * F.col("month").cast(DoubleType()) / 12))
        .withColumn("month_cos", F.cos(2 * 3.14159 * F.col("month").cast(DoubleType()) / 12))
    )
    
    # ── Drop rows with any null features (lag warm-up period) ─
    feature_cols = [
        "lag_1h", "lag_2h", "lag_24h", "lag_168h",
        "rolling_7d_mean", "rolling_7d_stddev", "rolling_24h_mean",
        "seasonal_index", "seasonal_deviation",
        "hour_sin", "hour_cos", "month_sin", "month_cos",
        "is_weekend",
        "dow_1", "dow_2", "dow_3", "dow_4", "dow_5", "dow_6", "dow_7",
    ]
    df = df.dropna(subset=feature_cols)
    
    log.info(f"  Feature engineering complete. Rows: {df.count():,}")
    return df, feature_cols


def build_pipeline(feature_cols: list) -> Pipeline:
    """Assemble the ML pipeline: VectorAssembler → Scaler → GBT."""
    
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="raw_features",
        handleInvalid="skip"
    )
    
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withMean=True,
        withStd=True
    )
    
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="mw_demand",
        maxIter=100,          # Number of trees
        maxDepth=6,           # Tree depth
        stepSize=0.1,         # Learning rate
        subsamplingRate=0.8,  # Row subsampling per tree
        featureSubsetStrategy="sqrt",  # Feature subsampling
        seed=RANDOM_SEED,
    )
    
    return Pipeline(stages=[assembler, scaler, gbt])


def evaluate_model(predictions, label_col="mw_demand", pred_col="prediction"):
    """Compute RMSE, MAE, R² metrics."""
    evaluator_rmse = RegressionEvaluator(
        labelCol=label_col, predictionCol=pred_col, metricName="rmse")
    evaluator_mae = RegressionEvaluator(
        labelCol=label_col, predictionCol=pred_col, metricName="mae")
    evaluator_r2 = RegressionEvaluator(
        labelCol=label_col, predictionCol=pred_col, metricName="r2")
    
    return {
        "rmse": evaluator_rmse.evaluate(predictions),
        "mae":  evaluator_mae.evaluate(predictions),
        "r2":   evaluator_r2.evaluate(predictions),
    }


# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        log.info("=== Demand Forecast Model Training ===")
        
        # 1. Load PJM demand Parquet
        log.info("Loading PJM demand data...")
        df = spark.read.parquet(PJM_PARQUET)
        df = df.withColumn("hour",  F.col("hour").cast(DoubleType()))
        df = df.withColumn("month", F.col("month").cast(DoubleType()))
        df = df.withColumn("is_weekend", F.col("is_weekend").cast(DoubleType()))
        log.info(f"  Rows loaded: {df.count():,}")
        
        # 2. Engineer features
        df_feat, feature_cols = engineer_features(df)
        
        # 3. Time-based train/test split (last 20% chronologically = test)
        # Using orderBy + row_number is expensive — use date cutoff instead
        max_date = df_feat.agg(F.max("ts")).collect()[0][0]
        from datetime import timedelta
        cutoff = max_date - timedelta(days=int(365 * TEST_SIZE))
        
        train_df = df_feat.filter(F.col("ts") <= cutoff)
        test_df  = df_feat.filter(F.col("ts") >  cutoff)
        
        log.info(f"  Train rows : {train_df.count():,}")
        log.info(f"  Test rows  : {test_df.count():,}")
        log.info(f"  Test cutoff: {cutoff}")
        
        # 4. Build and train pipeline
        log.info("Building ML pipeline...")
        pipeline = build_pipeline(feature_cols)
        
        log.info("Training GBT model (this may take 5–10 minutes)...")
        model = pipeline.fit(train_df)
        log.info("  [OK] Model trained")
        
        # 5. Evaluate on test set
        log.info("Evaluating on test set...")
        predictions = model.transform(test_df)
        metrics = evaluate_model(predictions)
        
        log.info("=== Model Evaluation Metrics ===")
        log.info(f"  RMSE  : {metrics['rmse']:.4f} MW")
        log.info(f"  MAE   : {metrics['mae']:.4f} MW")
        log.info(f"  R²    : {metrics['r2']:.4f}")
        
        # Save metrics as CSV to HDFS
        metrics_df = spark.createDataFrame([{
            "model":      "GBT_v1",
            "train_rows": train_df.count(),
            "test_rows":  test_df.count(),
            "rmse":       round(metrics["rmse"], 4),
            "mae":        round(metrics["mae"],  4),
            "r2":         round(metrics["r2"],   4),
            "features":   str(len(feature_cols)),
            "max_iter":   "100",
        }])
        metrics_df.write.mode("overwrite").csv(METRICS_PATH, header=True)
        
        # 6. Feature importance
        gbt_model = model.stages[-1]
        importances = list(zip(feature_cols, gbt_model.featureImportances.toArray()))
        importances.sort(key=lambda x: -x[1])
        log.info("=== Top 10 Feature Importances ===")
        for feat, imp in importances[:10]:
            bar = "█" * int(imp * 100)
            log.info(f"  {feat:30s} {imp:.4f}  {bar}")
        
        # 7. Save model to HDFS
        log.info(f"Saving model to: {MODEL_PATH}")
        model.write().overwrite().save(MODEL_PATH)
        log.info("  [OK] Model saved")
    
    finally:
        spark.stop()
        log.info("Training job complete.")
