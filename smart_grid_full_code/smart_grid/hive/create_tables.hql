-- ============================================================
--  hive/create_tables.hql
--  Creates all Hive external tables over HDFS Parquet data.
--  Run: hive -f hive/create_tables.hql
-- ============================================================

-- ── Create database ───────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS smart_grid
COMMENT 'Smart Grid Energy Analytics Platform'
LOCATION 'hdfs://localhost:9000/smart_grid/warehouse';

USE smart_grid;

-- ============================================================
-- TABLE 1: meter_readings
-- External table over cleaned, Parquet-formatted meter data.
-- Partitioned by year/month for efficient time-range queries.
-- ============================================================
DROP TABLE IF EXISTS meter_readings;

CREATE EXTERNAL TABLE meter_readings (
    meter_id        STRING      COMMENT 'Unique household meter identifier',
    reading_date    STRING      COMMENT 'Reading date (YYYY-MM-DD)',
    hour            STRING      COMMENT 'Hour of reading (00–23)',
    tariff_type     STRING      COMMENT 'Standard or Time of Use tariff',
    acorn_group     STRING      COMMENT 'Acorn socio-economic classification',
    kwh_hourly      FLOAT       COMMENT 'Total kWh consumed in this hour',
    kwh_max         FLOAT       COMMENT 'Peak kWh in a 30-min slot within the hour',
    temp_high_c     DOUBLE      COMMENT 'Daily high temperature (Celsius)',
    temp_low_c      DOUBLE      COMMENT 'Daily low temperature (Celsius)',
    temp_range_c    DOUBLE      COMMENT 'Temperature range (high - low)',
    humidity        DOUBLE      COMMENT 'Relative humidity (0.0 to 1.0)',
    ts              TIMESTAMP   COMMENT 'Full timestamp of reading'
)
PARTITIONED BY (
    year  STRING  COMMENT 'Reading year (e.g. 2012)',
    month STRING  COMMENT 'Reading month, zero-padded (e.g. 01)'
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/smart_grid/parquet/meter_readings'
TBLPROPERTIES (
    'parquet.compress'='SNAPPY',
    'comment'='London Smart Meters dataset — cleaned and enriched via Pig ETL'
);

-- Recover partitions from HDFS
MSCK REPAIR TABLE meter_readings;

-- ============================================================
-- TABLE 2: household_stats
-- Per-household baseline statistics (global across all time).
-- Used by the streaming anomaly detector for warm-start.
-- ============================================================
DROP TABLE IF EXISTS household_stats;

CREATE EXTERNAL TABLE household_stats (
    meter_id        STRING  COMMENT 'Meter identifier',
    total_readings  INT     COMMENT 'Total number of valid readings',
    global_mean     DOUBLE  COMMENT 'Mean kWh per 30-min slot across all time',
    global_min      FLOAT   COMMENT 'Minimum reading ever recorded',
    global_max      FLOAT   COMMENT 'Maximum reading ever recorded',
    global_stddev   DOUBLE  COMMENT 'Standard deviation across all readings'
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/smart_grid/parquet/household_stats'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- ============================================================
-- TABLE 3: pjm_demand
-- PJM regional hourly electricity demand.
-- Used for demand forecasting model training and evaluation.
-- ============================================================
DROP TABLE IF EXISTS pjm_demand;

CREATE EXTERNAL TABLE pjm_demand (
    ts              TIMESTAMP   COMMENT 'Reading timestamp',
    zone            STRING      COMMENT 'Grid zone name (e.g. AEP, DOM)',
    mw_demand       DOUBLE      COMMENT 'Electricity demand in megawatts',
    hour            DOUBLE      COMMENT 'Hour of day (0–23)',
    day_of_week     DOUBLE      COMMENT 'Day of week (1=Sun, 7=Sat)',
    is_weekend      BOOLEAN     COMMENT 'True if Saturday or Sunday'
)
PARTITIONED BY (
    year  STRING,
    month STRING
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/smart_grid/parquet/pjm_demand'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

MSCK REPAIR TABLE pjm_demand;

-- ============================================================
-- TABLE 4: streamed_readings
-- Live readings written by Spark Structured Streaming.
-- Appended to continuously as the pipeline runs.
-- ============================================================
DROP TABLE IF EXISTS streamed_readings;

CREATE EXTERNAL TABLE streamed_readings (
    meter_id        STRING,
    ts              STRING,
    kwh_hourly      FLOAT,
    rolling_mean    DOUBLE,
    rolling_stddev  DOUBLE,
    z_score         DOUBLE,
    anomaly_type    STRING,
    severity        DOUBLE,
    is_anomaly      BOOLEAN,
    tariff_type     STRING,
    acorn_group     STRING,
    reading_date    STRING,
    injected_anomaly STRING,
    processed_at    TIMESTAMP
)
PARTITIONED BY (
    reading_year  STRING,
    reading_month STRING
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/smart_grid/streaming/meter_readings'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

MSCK REPAIR TABLE streamed_readings;

-- ============================================================
-- TABLE 5: demand_forecasts
-- 24-hour-ahead demand predictions from GBT model.
-- Refreshed hourly by run_forecast.py.
-- ============================================================
DROP TABLE IF EXISTS demand_forecasts;

CREATE TABLE IF NOT EXISTS demand_forecasts (
    ts                  TIMESTAMP   COMMENT 'Forecast target timestamp',
    zone                STRING      COMMENT 'Grid zone',
    predicted_mw        DOUBLE      COMMENT 'Model-predicted demand (MW)',
    actual_mw           DOUBLE      COMMENT 'Actual demand if available (0 for future)',
    forecast_run_ts     STRING      COMMENT 'When this forecast was generated'
)
PARTITIONED BY (
    year  STRING,
    month STRING
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/smart_grid/forecasts'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- ============================================================
-- VIEWS — pre-built for Superset dashboards
-- ============================================================

-- View: Hourly anomaly counts (last 30 days)
DROP VIEW IF EXISTS v_hourly_anomaly_counts;
CREATE VIEW v_hourly_anomaly_counts AS
SELECT
    reading_date,
    hour,
    anomaly_type,
    COUNT(*)                    AS anomaly_count,
    AVG(severity)               AS avg_severity,
    AVG(kwh_hourly)             AS avg_kwh
FROM streamed_readings
WHERE is_anomaly = TRUE
  AND reading_date >= DATE_SUB(CURRENT_DATE, 30)
GROUP BY reading_date, hour, anomaly_type;

-- View: Top 50 meters by anomaly frequency
DROP VIEW IF EXISTS v_top_anomaly_meters;
CREATE VIEW v_top_anomaly_meters AS
SELECT
    meter_id,
    acorn_group,
    tariff_type,
    COUNT(*)            AS total_anomalies,
    SUM(CASE WHEN anomaly_type = 'sudden_spike'        THEN 1 ELSE 0 END) AS spikes,
    SUM(CASE WHEN anomaly_type = 'sustained_elevation' THEN 1 ELSE 0 END) AS sustained,
    SUM(CASE WHEN anomaly_type = 'flatline'            THEN 1 ELSE 0 END) AS flatlines,
    AVG(severity)       AS avg_severity,
    MAX(z_score)        AS max_z_score
FROM streamed_readings
WHERE is_anomaly = TRUE
GROUP BY meter_id, acorn_group, tariff_type
ORDER BY total_anomalies DESC
LIMIT 50;

-- View: Daily consumption by Acorn group
DROP VIEW IF EXISTS v_daily_consumption_by_acorn;
CREATE VIEW v_daily_consumption_by_acorn AS
SELECT
    reading_date,
    acorn_group,
    tariff_type,
    COUNT(DISTINCT meter_id)    AS meter_count,
    SUM(kwh_hourly)             AS total_kwh,
    AVG(kwh_hourly)             AS avg_kwh_per_meter_hour,
    MAX(kwh_hourly)             AS peak_kwh,
    AVG(temp_high_c)            AS avg_temp_high
FROM meter_readings
GROUP BY reading_date, acorn_group, tariff_type;

-- View: Forecast vs actual (last 7 days where actuals available)
DROP VIEW IF EXISTS v_forecast_accuracy;
CREATE VIEW v_forecast_accuracy AS
SELECT
    f.ts,
    f.zone,
    f.predicted_mw,
    p.mw_demand                             AS actual_mw,
    ABS(f.predicted_mw - p.mw_demand)       AS abs_error_mw,
    ABS(f.predicted_mw - p.mw_demand)
        / NULLIF(p.mw_demand, 0) * 100      AS pct_error
FROM demand_forecasts f
LEFT JOIN pjm_demand p
    ON f.ts = p.ts AND f.zone = p.zone
WHERE f.ts >= DATE_SUB(CURRENT_TIMESTAMP, 7);

-- ── Quick validation queries ──────────────────────────────────
SELECT 'meter_readings'   AS tbl, COUNT(*) AS row_count FROM meter_readings
UNION ALL
SELECT 'household_stats',          COUNT(*) FROM household_stats
UNION ALL
SELECT 'pjm_demand',               COUNT(*) FROM pjm_demand
UNION ALL
SELECT 'streamed_readings',        COUNT(*) FROM streamed_readings
UNION ALL
SELECT 'demand_forecasts',         COUNT(*) FROM demand_forecasts;
