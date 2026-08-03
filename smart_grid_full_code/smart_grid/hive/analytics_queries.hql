-- ============================================================
--  hive/analytics_queries.hql
--  Pre-built analytics queries for the Smart Grid platform.
--  These power the Superset dashboard reports.
--  Run individually: hive -e "USE smart_grid; <query>"
-- ============================================================

USE smart_grid;

-- ============================================================
-- QUERY 1: Peak demand hours — average consumption by hour of day
-- Identifies which hours drive highest grid load
-- ============================================================
SELECT
    hour,
    ROUND(AVG(kwh_hourly), 4)       AS avg_kwh,
    ROUND(MAX(kwh_hourly), 4)       AS peak_kwh,
    ROUND(STDDEV(kwh_hourly), 4)    AS stddev_kwh,
    COUNT(DISTINCT meter_id)        AS meters
FROM meter_readings
GROUP BY hour
ORDER BY CAST(hour AS INT);

-- ============================================================
-- QUERY 2: Monthly energy consumption trend
-- Seasonal pattern — shows winter peaks in London data
-- ============================================================
SELECT
    year,
    month,
    ROUND(SUM(kwh_hourly), 2)           AS total_kwh,
    ROUND(AVG(kwh_hourly), 4)           AS avg_kwh_per_meter_hour,
    COUNT(DISTINCT meter_id)            AS active_meters,
    ROUND(AVG(temp_high_c), 2)          AS avg_temp_high_c
FROM meter_readings
GROUP BY year, month
ORDER BY year, month;

-- ============================================================
-- QUERY 3: Anomaly breakdown by type and Acorn group
-- Shows which socio-economic groups have most anomalies
-- ============================================================
SELECT
    acorn_group,
    anomaly_type,
    COUNT(*)                            AS anomaly_count,
    ROUND(AVG(severity), 3)            AS avg_severity,
    ROUND(AVG(z_score), 3)             AS avg_zscore,
    COUNT(DISTINCT meter_id)           AS affected_meters
FROM streamed_readings
WHERE is_anomaly = TRUE
GROUP BY acorn_group, anomaly_type
ORDER BY anomaly_count DESC;

-- ============================================================
-- QUERY 4: Day-over-day anomaly rate trend (last 60 days)
-- Used for the time-series chart on dashboard
-- ============================================================
SELECT
    reading_date,
    COUNT(*)                            AS total_readings,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomaly_count,
    ROUND(
        SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) * 100.0
        / COUNT(*), 2
    )                                   AS anomaly_rate_pct
FROM streamed_readings
WHERE reading_date >= DATE_SUB(CURRENT_DATE, 60)
GROUP BY reading_date
ORDER BY reading_date;

-- ============================================================
-- QUERY 5: Top 20 highest-consumption meters (all time)
-- Potential large commercial properties or illegal draws
-- ============================================================
SELECT
    mr.meter_id,
    hs.acorn_group,
    mr.tariff_type,
    ROUND(SUM(mr.kwh_hourly), 2)        AS total_kwh,
    ROUND(AVG(mr.kwh_hourly), 4)        AS avg_kwh_hourly,
    ROUND(MAX(mr.kwh_hourly), 4)        AS peak_kwh,
    COUNT(*)                            AS reading_count
FROM meter_readings mr
JOIN household_stats hs ON mr.meter_id = hs.meter_id
GROUP BY mr.meter_id, hs.acorn_group, mr.tariff_type
ORDER BY total_kwh DESC
LIMIT 20;

-- ============================================================
-- QUERY 6: Demand forecast accuracy report (last 7 days)
-- Compares predicted MW vs actual MW from PJM data
-- ============================================================
SELECT
    DATE(f.ts)                          AS forecast_date,
    f.zone,
    COUNT(*)                            AS forecast_points,
    ROUND(AVG(ABS(f.predicted_mw - p.mw_demand)), 2)  AS mae_mw,
    ROUND(
        SQRT(AVG(POWER(f.predicted_mw - p.mw_demand, 2))), 2
    )                                   AS rmse_mw,
    ROUND(
        AVG(ABS(f.predicted_mw - p.mw_demand) / p.mw_demand) * 100, 2
    )                                   AS mape_pct
FROM demand_forecasts f
JOIN pjm_demand p
    ON f.ts = p.ts AND f.zone = p.zone
WHERE f.ts >= DATE_SUB(CURRENT_TIMESTAMP, 7)
  AND p.mw_demand > 0
GROUP BY DATE(f.ts), f.zone
ORDER BY forecast_date;

-- ============================================================
-- QUERY 7: Weekend vs weekday consumption comparison
-- ============================================================
SELECT
    CASE WHEN DAYOFWEEK(TO_DATE(reading_date)) IN (1, 7)
         THEN 'Weekend' ELSE 'Weekday' END  AS day_type,
    hour,
    ROUND(AVG(kwh_hourly), 4)              AS avg_kwh,
    ROUND(STDDEV(kwh_hourly), 4)           AS stddev_kwh
FROM meter_readings
GROUP BY
    CASE WHEN DAYOFWEEK(TO_DATE(reading_date)) IN (1, 7)
         THEN 'Weekend' ELSE 'Weekday' END,
    hour
ORDER BY day_type, CAST(hour AS INT);

-- ============================================================
-- QUERY 8: Temperature vs consumption correlation by month
-- Validates that heating/cooling drives seasonal peaks
-- ============================================================
SELECT
    year,
    month,
    ROUND(AVG(kwh_hourly), 4)           AS avg_kwh,
    ROUND(AVG(temp_high_c), 2)          AS avg_temp_high,
    ROUND(AVG(temp_low_c), 2)           AS avg_temp_low,
    ROUND(CORR(kwh_hourly, temp_high_c), 4) AS corr_kwh_temp_high,
    ROUND(CORR(kwh_hourly, humidity), 4)    AS corr_kwh_humidity
FROM meter_readings
WHERE temp_high_c IS NOT NULL
GROUP BY year, month
ORDER BY year, month;

-- ============================================================
-- QUERY 9: ToU (Time of Use) vs Standard tariff comparison
-- Do ToU customers shift load away from peak hours?
-- ============================================================
SELECT
    tariff_type,
    hour,
    ROUND(AVG(kwh_hourly), 4)           AS avg_kwh,
    COUNT(DISTINCT meter_id)            AS meter_count
FROM meter_readings
GROUP BY tariff_type, hour
ORDER BY tariff_type, CAST(hour AS INT);

-- ============================================================
-- QUERY 10: Injection validation — did anomaly detector find
--           the synthetically injected anomalies?
-- Used for evaluation / demo report
-- ============================================================
SELECT
    injected_anomaly,
    anomaly_type             AS detected_as,
    COUNT(*)                 AS count,
    ROUND(AVG(severity), 3)  AS avg_severity,
    ROUND(AVG(z_score), 3)   AS avg_zscore,
    SUM(CASE WHEN is_anomaly = TRUE  THEN 1 ELSE 0 END)    AS correctly_flagged,
    SUM(CASE WHEN is_anomaly = FALSE THEN 1 ELSE 0 END)    AS missed
FROM streamed_readings
WHERE injected_anomaly != 'none'
GROUP BY injected_anomaly, anomaly_type
ORDER BY injected_anomaly;
