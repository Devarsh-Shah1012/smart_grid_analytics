# Apache Superset Setup & Dashboard Configuration
## Smart Grid Energy Analytics Platform

---

## 1. Install Superset (Cluster C Master Node)

```bash
# Create Python virtual environment
python3 -m venv /opt/superset_venv
source /opt/superset_venv/bin/activate

# Install Superset with Hive support
pip install apache-superset
pip install pyhive[hive]
pip install thrift
pip install thrift-sasl
pip install sasl
pip install happybase

# Initialise Superset database
export SUPERSET_SECRET_KEY="smart_grid_secret_$(openssl rand -hex 16)"
superset db upgrade
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@smartgrid.local \
  --password admin123
superset init

# Start Superset
superset run -h 0.0.0.0 -p 8088 --with-threads --reload &
```

---

## 2. Database Connections

### Connection A: Apache Hive (batch analytics)

In Superset UI → Settings → Database Connections → + Database

| Field | Value |
|---|---|
| Database Type | Apache Hive |
| Display Name | SmartGrid Hive |
| SQLAlchemy URI | `hive://localhost:10000/smart_grid` |
| Expose in SQL Lab | ✅ |
| Allow DML | ❌ |

Test connection → Save

### Connection B: HBase via Flask API (live alerts)

HBase doesn't have a native SQLAlchemy connector. We expose it via
a tiny Flask REST API running on Cluster B, which Superset calls
via a custom REST datasource.

Start the Flask API:
```bash
python dashboard/hbase_api.py &
# Listens on http://CLUSTER_B_MASTER:5050
```

In Superset: Add a REST API dataset pointing to
`http://CLUSTER_B_MASTER:5050/api/alerts/recent`

---

## 3. Datasets to Create

In Superset → Datasets → + Dataset

| Dataset Name | Database | Table / Query |
|---|---|---|
| meter_readings | SmartGrid Hive | `smart_grid.meter_readings` |
| streamed_readings | SmartGrid Hive | `smart_grid.streamed_readings` |
| demand_forecasts | SmartGrid Hive | `smart_grid.demand_forecasts` |
| v_hourly_anomaly_counts | SmartGrid Hive | `smart_grid.v_hourly_anomaly_counts` (VIEW) |
| v_top_anomaly_meters | SmartGrid Hive | `smart_grid.v_top_anomaly_meters` (VIEW) |
| v_forecast_accuracy | SmartGrid Hive | `smart_grid.v_forecast_accuracy` (VIEW) |

---

## 4. Dashboard Panels — Configuration

Create a new Dashboard: **"Smart Grid Operations Centre"**

### Panel 1: Live Anomaly Alert Feed (Table Chart)
- Dataset: `streamed_readings`
- Chart Type: Table
- Columns: `ts`, `meter_id`, `anomaly_type`, `severity`, `kwh_hourly`, `z_score`, `acorn_group`
- Filters: `is_anomaly = TRUE`, `reading_date >= TODAY - 1`
- Sort: `ts DESC`
- Row limit: 50
- Auto-refresh: 30 seconds

### Panel 2: Anomaly Type Distribution (Pie Chart)
- Dataset: `streamed_readings`
- Chart Type: Pie Chart
- Metric: `COUNT(*)`
- Group by: `anomaly_type`
- Filters: `is_anomaly = TRUE`
- Auto-refresh: 60 seconds

### Panel 3: Anomaly Rate Over Time (Time Series Line)
- Dataset: `v_hourly_anomaly_counts`
- Chart Type: Line Chart
- X-axis: `reading_date`
- Metrics: `SUM(anomaly_count)` (per day)
- Group by: `anomaly_type`
- Time range: Last 30 days
- Auto-refresh: 5 minutes

### Panel 4: 24-Hour Demand Forecast vs Actual (Dual Line Chart)
- Dataset: `v_forecast_accuracy`
- Chart Type: Mixed Chart (two lines)
- X-axis: `ts`
- Line 1: `predicted_mw` (solid, blue)
- Line 2: `actual_mw` (dashed, orange)
- Time range: Last 48 hours
- Auto-refresh: 60 seconds

### Panel 5: Top Flagged Meters (Bar Chart)
- Dataset: `v_top_anomaly_meters`
- Chart Type: Horizontal Bar Chart
- Y-axis (labels): `meter_id`
- X-axis (metric): `total_anomalies`
- Group by: `acorn_group` (for colour coding)
- Limit: 20 meters

### Panel 6: Grid Load Gauge (Big Number + KPIs)
- Dataset: Custom SQL on `demand_forecasts`:
  ```sql
  SELECT
      MAX(predicted_mw) AS peak_predicted_24h,
      AVG(predicted_mw) AS avg_predicted_24h,
      MAX(ts)           AS last_forecast_run
  FROM smart_grid.demand_forecasts
  WHERE ts >= CURRENT_TIMESTAMP
    AND ts <= CURRENT_TIMESTAMP + INTERVAL 24 HOURS
  ```
- Chart Type: Big Number with Trendline
- Auto-refresh: 5 minutes

### Panel 7: Consumption by Acorn Group Heatmap
- Dataset: Custom SQL:
  ```sql
  SELECT
      acorn_group,
      CAST(hour AS INT) AS hour_of_day,
      ROUND(AVG(kwh_hourly), 4) AS avg_kwh
  FROM smart_grid.meter_readings
  GROUP BY acorn_group, hour
  ```
- Chart Type: Heatmap
- X-axis: `hour_of_day`
- Y-axis: `acorn_group`
- Metric: `avg_kwh`

### Panel 8: Forecast Accuracy KPIs (Big Numbers row)
- Dataset: `v_forecast_accuracy`
- Three Big Number cards:
  - `AVG(mae_mw)` → "Mean Absolute Error (MW)"
  - `AVG(rmse_mw)` → "Root Mean Square Error (MW)"
  - `AVG(mape_pct)` → "Mean Absolute % Error"

---

## 5. Dashboard Layout

```
┌──────────────────────────────────────────────────────────────────┐
│         SMART GRID ENERGY ANALYTICS — OPERATIONS CENTRE          │
├─────────────────────┬────────────────────┬───────────────────────┤
│  KPI: Peak 24h      │  KPI: Avg Demand   │  KPI: Active Alerts  │
│  Forecast (MW)      │  Forecast (MW)     │  (last hour)         │
├─────────────────────┴────────────────────┴───────────────────────┤
│                 Demand Forecast vs Actual (Line Chart)            │
│                        Last 48 hours                              │
├──────────────────────────────┬───────────────────────────────────┤
│  Live Alert Feed (Table)     │  Anomaly Type Distribution (Pie)  │
│  Auto-refresh 30s            │  Auto-refresh 60s                 │
├──────────────────────────────┴───────────────────────────────────┤
│              Anomaly Rate Over Time (Line, 30 days)               │
├──────────────────────────────┬───────────────────────────────────┤
│  Top Flagged Meters (Bar)    │  Consumption Heatmap              │
│                              │  (Acorn Group × Hour)             │
├──────────────────────────────┴───────────────────────────────────┤
│  Forecast Accuracy: MAE      │  RMSE           │  MAPE           │
└──────────────────────────────┴─────────────────┴─────────────────┘
```

---

## 6. Auto-Refresh Settings

In Dashboard Settings → Auto-refresh → 30 seconds

Individual charts that need faster refresh:
- Live Alert Feed: 30s
- Anomaly Type Distribution: 60s
- Everything else: 5 minutes (reduce HiveServer2 load)

---

## 7. Superset SQL Lab Useful Queries

Open SQL Lab (top menu) → Select `SmartGrid Hive` database

```sql
-- How many anomalies in the last hour?
SELECT anomaly_type, COUNT(*) AS cnt
FROM smart_grid.streamed_readings
WHERE is_anomaly = TRUE
  AND processed_at >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
GROUP BY anomaly_type;

-- What's the current grid demand forecast for next 6 hours?
SELECT ts, predicted_mw, actual_mw
FROM smart_grid.demand_forecasts
WHERE ts >= CURRENT_TIMESTAMP
ORDER BY ts
LIMIT 6;

-- Drill into a specific meter
SELECT ts, kwh_hourly, z_score, anomaly_type
FROM smart_grid.streamed_readings
WHERE meter_id = 'MAC000002'
ORDER BY ts DESC
LIMIT 48;
```
