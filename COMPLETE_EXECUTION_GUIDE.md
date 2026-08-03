# Smart Grid Platform - Complete Execution Guide
## From Data Upload to Live Dashboard

---

## 📋 Prerequisites Checklist

Before starting, verify:

```bash
# Run on ANY device
bash check_cluster_health.sh

# You should see:
# ✓ HDFS NameNode running
# ✓ HBase Master running
# ✓ Kafka Broker running
# ✓ Hive Metastore running
# ✓ All 3 clusters reachable
```

---

## 🗂️ PHASE 1: Project Files Setup

### Step 1.1: Copy Project Files to Each Cluster

**On Mac (Cluster C - Analytics):**

```bash
# Create project directory
mkdir -p ~/smart_grid_project
cd ~/smart_grid_project

# Copy ALL project files from /mnt/project/ to here
# You can use scp, rsync, or manually copy
cp -r /path/to/your/downloaded/project/* .

# Verify you have these key files:
ls -la

# You should see:
# - cluster.env
# - kafka_topics.sh
# - hbase_api.py
# - create_tables.rb
# - hbase_utils.py
# - producer.py
# - hdfs_loader.py
# - train_forecast_model.py
# - run_forecast.py
# - anomaly_detector.py
# - create_tables.hql (downloaded from previous step)
# - requirements.txt
```

**On Windows 1 (Cluster A - Streaming):**

```bash
# Create project directory
mkdir -p ~/smart_grid_project
cd ~/smart_grid_project

# Copy these specific files:
# - producer.py
# - anomaly_detector.py
# - hbase_utils.py
# - kafka_topics.sh
# - cluster.env
# - requirements.txt

# You can scp from Mac:
# scp user@mac-cluster-c:~/smart_grid_project/{producer.py,anomaly_detector.py,hbase_utils.py,kafka_topics.sh,cluster.env,requirements.txt} .
```

**On Windows 2 (Cluster B - Storage):**

```bash
# Create project directory
mkdir -p ~/smart_grid_project
cd ~/smart_grid_project

# Copy these specific files:
# - create_tables.rb
# - hbase_utils.py
# - hbase_api.py
# - cluster.env
# - requirements.txt
```

---

### Step 1.2: Update cluster.env with YOUR Actual IPs

**On ALL 3 devices**, edit the cluster.env file:

```bash
nano ~/smart_grid_project/cluster.env
```

Update these lines with your actual IPs:

```bash
CLUSTER_A_MASTER=192.168.1.XXX  # Your Windows 1 IP
CLUSTER_B_MASTER=192.168.1.XXX  # Your Windows 2 IP
CLUSTER_C_MASTER=192.168.1.XXX  # Your Mac IP
```

Save and exit (Ctrl+X, Y, Enter)

---

### Step 1.3: Install Python Dependencies

**On ALL 3 devices:**

```bash
source /opt/smart_grid_venv/bin/activate
cd ~/smart_grid_project

pip install --upgrade pip
pip install -r requirements.txt

# Verify installation
python -c "import pyspark; print(pyspark.__version__)"  # Should show 3.4.1
python -c "import kafka; print('Kafka OK')"
python -c "import happybase; print('HappyBase OK')"
```

---

## 📦 PHASE 2: Download and Prepare Datasets

### Step 2.1: Download Datasets (Mac - Cluster C)

```bash
# Create data directories
sudo mkdir -p /opt/smart_grid_data/{london,pjm,uci}
sudo chown -R $USER:$USER /opt/smart_grid_data

cd /opt/smart_grid_data
```

**Option A: Using Kaggle CLI (Recommended)**

```bash
# Install Kaggle CLI
pip install kaggle

# Setup Kaggle credentials
mkdir -p ~/.kaggle
# Download your kaggle.json from https://www.kaggle.com/settings
# (Your Account → API → Create New API Token)
mv ~/Downloads/kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json

# Download London Smart Meters (10 GB - takes 10-30 minutes)
kaggle datasets download -d jeanmidev/smart-meters-in-london \
  -p /opt/smart_grid_data/london --unzip

# Download PJM Hourly Energy (1 GB)
kaggle datasets download -d robikscube/hourly-energy-consumption \
  -p /opt/smart_grid_data/pjm --unzip
```

**Option B: Manual Download**

```bash
# 1. Visit these URLs in your browser:
#    London: https://www.kaggle.com/datasets/jeanmidev/smart-meters-in-london
#    PJM: https://www.kaggle.com/datasets/robikscube/hourly-energy-consumption
#
# 2. Download and extract to:
#    /opt/smart_grid_data/london/
#    /opt/smart_grid_data/pjm/
```

**Download UCI Dataset (Small - 20 MB):**

```bash
cd /opt/smart_grid_data/uci
wget https://archive.ics.uci.edu/ml/machine-learning-databases/00235/household_power_consumption.zip
unzip household_power_consumption.zip
rm household_power_consumption.zip
```

**Verify Downloads:**

```bash
ls -lh /opt/smart_grid_data/london/halfhourly_dataset/ | head -20
ls -lh /opt/smart_grid_data/pjm/*.csv
ls -lh /opt/smart_grid_data/uci/*.txt
```

You should see:
- London: ~5,500 CSV files in halfhourly_dataset/
- PJM: ~10 CSV files (AEP_hourly.csv, DOM_hourly.csv, etc.)
- UCI: household_power_consumption.txt

---

### Step 2.2: Create Sample Clean Data for Testing (Mac - Cluster C)

Before running the full pipeline, let's create a small sample for testing:

```bash
cd ~/smart_grid_project

# Create a sample CSV with 1000 rows for quick testing
cat > sample_clean.csv <<'EOF'
MAC000001,2012-01-01,00,Std,Adversity,0.234,0.145,12.3,5.1,0.82
MAC000001,2012-01-01,01,Std,Adversity,0.189,0.120,12.1,5.0,0.83
MAC000002,2012-01-01,00,ToU,Comfortable,0.456,0.280,12.3,5.1,0.82
MAC000002,2012-01-01,01,ToU,Comfortable,0.390,0.250,12.1,5.0,0.83
EOF

# Add 996 more rows (simulated)
python3 <<PYEOF
import random
from datetime import datetime, timedelta

meters = ['MAC000001', 'MAC000002', 'MAC000003', 'MAC000004', 'MAC000005']
tariffs = ['Std', 'ToU']
acorns = ['Adversity', 'Comfortable', 'Affluent']

with open('sample_clean.csv', 'a') as f:
    date = datetime(2012, 1, 1, 2, 0)
    for i in range(996):
        meter = random.choice(meters)
        tariff = random.choice(tariffs)
        acorn = random.choice(acorns)
        kwh = round(random.uniform(0.1, 2.5), 3)
        kwh_max = round(kwh * 0.6, 3)
        temp_high = round(random.uniform(8, 18), 1)
        temp_low = round(random.uniform(2, 10), 1)
        humidity = round(random.uniform(0.6, 0.9), 2)
        
        f.write(f"{meter},{date.strftime('%Y-%m-%d')},{date.strftime('%H')},{tariff},{acorn},{kwh},{kwh_max},{temp_high},{temp_low},{humidity}\n")
        
        date += timedelta(hours=1)
PYEOF

echo "Sample data created: sample_clean.csv"
head -20 sample_clean.csv
```

---

## 🗄️ PHASE 3: Create Database Schemas

### Step 3.1: Create HBase Tables (Windows 2 - Cluster B)

```bash
cd ~/smart_grid_project

# Start HBase shell and run the script
hbase shell create_tables.rb

# You should see:
# Dropped existing table: meter_readings (if it existed)
# Dropped existing table: anomaly_alerts (if it existed)
# Created table: meter_readings
# Created table: anomaly_alerts
# Created table: meter_state
```

**Verify HBase tables:**

```bash
echo "list" | hbase shell
# Should show: anomaly_alerts, meter_readings, meter_state

echo "describe 'meter_readings'" | hbase shell
# Should show table structure
```

---

### Step 3.2: Create Kafka Topics (Windows 1 - Cluster A)

```bash
cd ~/smart_grid_project

# Make script executable
chmod +x kafka_topics.sh

# Run the script
source cluster.env
bash kafka_topics.sh

# You should see:
# ==> Creating Kafka topics on broker: wsl-cluster-a:9092
# [OK] Topic 'meter-readings' created (6 partitions, RF=1)
# [OK] Topic 'anomaly-alerts' created (3 partitions, RF=1)
```

**Verify Kafka topics:**

```bash
source cluster.env
kafka-topics.sh --list --bootstrap-server $KAFKA_BROKER

# Should show:
# anomaly-alerts
# meter-readings

# Check topic details
kafka-topics.sh --describe --topic meter-readings --bootstrap-server $KAFKA_BROKER
```

---

### Step 3.3: Create HDFS Directories (Windows 2 - Cluster B)

```bash
# Create all required HDFS directories
hdfs dfs -mkdir -p /smart_grid/raw/london
hdfs dfs -mkdir -p /smart_grid/raw/pjm
hdfs dfs -mkdir -p /smart_grid/raw/uci
hdfs dfs -mkdir -p /smart_grid/clean/meter_readings
hdfs dfs -mkdir -p /smart_grid/parquet/meter_readings
hdfs dfs -mkdir -p /smart_grid/parquet/household_stats
hdfs dfs -mkdir -p /smart_grid/parquet/pjm_demand
hdfs dfs -mkdir -p /smart_grid/streaming/meter_readings
hdfs dfs -mkdir -p /smart_grid/models
hdfs dfs -mkdir -p /smart_grid/forecasts
hdfs dfs -mkdir -p /smart_grid/warehouse
hdfs dfs -mkdir -p /smart_grid/checkpoints/anomaly_detector
hdfs dfs -mkdir -p /hbase

# Set permissions
hdfs dfs -chmod -R 777 /smart_grid

# Verify
hdfs dfs -ls /smart_grid/
```

---

## 📤 PHASE 4: Upload Data to HDFS

### Step 4.1: Upload Sample Clean Data (Mac - Cluster C)

```bash
cd ~/smart_grid_project

# Upload sample data to HDFS
hdfs dfs -put sample_clean.csv /smart_grid/clean/meter_readings/

# Verify upload
hdfs dfs -ls /smart_grid/clean/meter_readings/
hdfs dfs -cat /smart_grid/clean/meter_readings/sample_clean.csv | head -10
```

---

### Step 4.2: Upload PJM Data (Mac - Cluster C)

```bash
# Upload PJM hourly demand CSVs
hdfs dfs -put /opt/smart_grid_data/pjm/*.csv /smart_grid/raw/pjm/

# Verify
hdfs dfs -ls /smart_grid/raw/pjm/
```

---

### Step 4.3: Convert CSV to Parquet (Mac - Cluster C)

Now we'll use Spark to convert the CSV data to optimized Parquet format:

```bash
cd ~/smart_grid_project
source /opt/smart_grid_venv/bin/activate
source cluster.env

# Run the HDFS loader
spark-submit \
  --master spark://mac-cluster-c:7077 \
  --executor-memory 4g \
  --num-executors 2 \
  --driver-memory 2g \
  hdfs_loader.py

# This will:
# 1. Read CSV from /smart_grid/clean/meter_readings/
# 2. Convert to Parquet
# 3. Partition by year/month
# 4. Write to /smart_grid/parquet/meter_readings/
#
# Expected output:
# [HDFS_LOADER] Loading meter readings from Pig ETL output...
# [HDFS_LOADER]   Loaded 1,000 meter reading rows
# [HDFS_LOADER]   Writing Parquet to: hdfs://wsl-cluster-b:9000/smart_grid/parquet/meter_readings
# [HDFS_LOADER]   [OK] Meter readings Parquet written
```

**Verify Parquet files:**

```bash
hdfs dfs -ls -R /smart_grid/parquet/meter_readings/
# Should show partitioned structure: year=2012/month=01/
```

---

## 🗃️ PHASE 5: Create Hive Tables

### Step 5.1: Run Hive Table Creation Script (Mac - Cluster C)

```bash
cd ~/smart_grid_project

# Run the Hive DDL script
hive -f create_tables.hql

# Expected output:
# OK
# Time taken: X seconds
# 
# Tables created:
# meter_readings
# household_stats
# pjm_demand
# streamed_readings
# demand_forecasts
```

---

### Step 5.2: Verify Hive Tables (Mac - Cluster C)

```bash
# Open Hive CLI
hive

# Run these commands:
USE smart_grid;
SHOW TABLES;

# You should see:
# meter_readings
# household_stats
# pjm_demand
# streamed_readings
# demand_forecasts
# v_hourly_anomaly_counts
# v_top_anomaly_meters
# v_forecast_accuracy
# v_consumption_by_acorn
# v_daily_summary

# Check row counts
SELECT COUNT(*) FROM meter_readings;
# Should show 1000 (our sample data)

SELECT COUNT(*) FROM pjm_demand;
# Should show number of PJM rows

# Exit Hive
exit;
```

---

## 🤖 PHASE 6: Train ML Forecasting Model

### Step 6.1: Train the Model (Mac - Cluster C)

```bash
cd ~/smart_grid_project
source /opt/smart_grid_venv/bin/activate

# Train the GBT demand forecasting model
spark-submit \
  --master spark://mac-cluster-c:7077 \
  --executor-memory 4g \
  --num-executors 2 \
  --driver-memory 2g \
  train_forecast_model.py

# This takes 5-10 minutes
# Expected output:
# [TRAIN_MODEL] === Demand Forecast Model Training ===
# [TRAIN_MODEL] Loading PJM demand data...
# [TRAIN_MODEL]   Rows loaded: 145,366
# [TRAIN_MODEL] Engineering features...
# [TRAIN_MODEL]   Train rows : 116,293
# [TRAIN_MODEL]   Test rows  : 29,073
# [TRAIN_MODEL] Training GBT model (this may take 5–10 minutes)...
# [TRAIN_MODEL]   [OK] Model trained
# [TRAIN_MODEL] === Model Evaluation Metrics ===
# [TRAIN_MODEL]   RMSE  : 150.2345 MW
# [TRAIN_MODEL]   MAE   : 110.5678 MW
# [TRAIN_MODEL]   R²    : 0.9712
# [TRAIN_MODEL] Saving model to: hdfs://wsl-cluster-b:9000/smart_grid/models/gbt_demand_forecast
# [TRAIN_MODEL]   [OK] Model saved
```

**Verify model was saved:**

```bash
hdfs dfs -ls /smart_grid/models/gbt_demand_forecast/
# Should show model metadata and stages/
```

---

## 🚀 PHASE 7: Start the Live Streaming Pipeline

Now we'll start the real-time data pipeline. You'll need **3 terminals**:

---

### Terminal 1: Start Kafka Producer (Windows 1 - Cluster A)

```bash
cd ~/smart_grid_project
source /opt/smart_grid_venv/bin/activate
source cluster.env

# Start the producer (simulates live meter readings)
python producer.py \
  --data sample_clean.csv \
  --speed 10 \
  --loop

# Expected output:
# [PRODUCER] Smart Grid Kafka Producer starting...
# [PRODUCER]   Broker  : wsl-cluster-a:9092
# [PRODUCER]   Topic   : meter-readings
# [PRODUCER]   Data    : sample_clean.csv
# [PRODUCER]   Speed   : 10s / day
# [PRODUCER] === Starting replay run #1 ===
# [PRODUCER] Day    1 [2012-01-01] —  24 msgs sent in 0.12s — sleeping 9.88s
# [PRODUCER] Day    2 [2012-01-02] —  24 msgs sent in 0.11s — sleeping 9.89s
# ...

# Leave this running! It will loop through the sample data
```

**Verify messages are being sent:**

In a new terminal on Windows 1:

```bash
source cluster.env

# Consume a few messages to verify
kafka-console-consumer.sh \
  --bootstrap-server $KAFKA_BROKER \
  --topic meter-readings \
  --from-beginning \
  --max-messages 5

# You should see JSON messages like:
# {"meter_id":"MAC000001","ts":"2012-01-01T00:00:00","kwh_hourly":0.234,...}
```

---

### Terminal 2: Start Spark Streaming Job (Windows 1 - Cluster A)

**IMPORTANT:** Open a NEW terminal/tab on Windows 1

```bash
cd ~/smart_grid_project
source /opt/smart_grid_venv/bin/activate
source cluster.env

# Start the Spark Streaming anomaly detector
spark-submit \
  --master spark://wsl-cluster-a:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  --executor-memory 3g \
  --driver-memory 2g \
  --conf spark.sql.streaming.checkpointLocation=hdfs://wsl-cluster-b:9000/smart_grid/checkpoints/anomaly_detector \
  anomaly_detector.py

# Expected output:
# [ANOMALY_DETECTOR] === Smart Grid Anomaly Detector Starting ===
# [ANOMALY_DETECTOR]   Kafka broker  : wsl-cluster-a:9092
# [ANOMALY_DETECTOR]   Input topic   : meter-readings
# [ANOMALY_DETECTOR]   Output topic  : anomaly-alerts
# [ANOMALY_DETECTOR]   HDFS output   : hdfs://wsl-cluster-b:9000/smart_grid/streaming/meter_readings
# [ANOMALY_DETECTOR]   Z threshold   : 3.0
# [ANOMALY_DETECTOR]   [OK] HDFS sink started
# [ANOMALY_DETECTOR]   [OK] HBase sink started
# [ANOMALY_DETECTOR]   [OK] Kafka alert sink started
# [ANOMALY_DETECTOR] === All sinks started. Streaming in progress... ===
#
# -------------------------------------------
# Batch: 0
# -------------------------------------------
# +----------+-------------------+-----------+-------+-------------+----------+--------+
# |meter_id  |ts                 |kwh_hourly |z_score|anomaly_type |severity  |acorn...|
# +----------+-------------------+-----------+-------+-------------+----------+--------+
# ...

# Leave this running! It processes the stream in real-time
```

**What's happening:**
- Reads messages from Kafka `meter-readings` topic
- Computes rolling statistics per meter (Welford's algorithm)
- Calculates Z-scores to detect anomalies
- Writes ALL readings to HDFS (Parquet)
- Writes ANOMALIES to HBase
- Publishes ANOMALIES back to Kafka `anomaly-alerts` topic

---

### Terminal 3: Start HBase Thrift API (Windows 2 - Cluster B)

**IMPORTANT:** Open a terminal on Windows 2

```bash
cd ~/smart_grid_project
source /opt/smart_grid_venv/bin/activate
source cluster.env

# Start the Flask API that exposes HBase to Superset
python hbase_api.py

# Expected output:
# [HBase API] Starting HBase API on port 5050
# [HBase API] HBase host: wsl-cluster-b
#  * Serving Flask app 'hbase_api'
#  * Running on http://0.0.0.0:5050

# Leave this running!
```

**Verify API is working:**

In a new terminal:

```bash
# Test the API
curl http://wsl-cluster-b:5050/health

# Should return:
# {"status":"ok","hbase":"connected"}

# Get recent alerts
curl http://wsl-cluster-b:5050/api/alerts/recent?limit=5

# Should return JSON with anomaly alerts (if any detected)
```

---

## 📈 PHASE 8: Generate Demand Forecasts

### Step 8.1: Run Forecast Job (Mac - Cluster C)

**IMPORTANT:** Open a terminal on Mac

```bash
cd ~/smart_grid_project
source /opt/smart_grid_venv/bin/activate
source cluster.env

# Generate 24-hour ahead forecast
spark-submit \
  --master spark://mac-cluster-c:7077 \
  --executor-memory 4g \
  --driver-memory 2g \
  run_forecast.py \
  --zone AEP \
  --hours 24

# Expected output:
# [RUN_FORECAST] === Forecast Run: 2024-XX-XXTXX:XX:XX.XXXXXX | Zone: AEP ===
# [RUN_FORECAST] History rows for zone AEP: 32,456
# [RUN_FORECAST] Building 24-hour forecast window from 2013-08-01 12:00:00
# [RUN_FORECAST]   Forecast window rows: 24
# [RUN_FORECAST] Loading model from: hdfs://wsl-cluster-b:9000/smart_grid/models/gbt_demand_forecast
# [RUN_FORECAST]   Forecast Parquet saved to: hdfs://wsl-cluster-b:9000/smart_grid/forecasts/AEP
# [RUN_FORECAST]   Forecast written to Hive table: smart_grid.demand_forecasts
# [RUN_FORECAST] === Forecast Sample (next 24h) ===
# +-------------------+-------------+
# |ts                 |predicted_mw |
# +-------------------+-------------+
# |2013-08-01 13:00:00|15234.56     |
# |2013-08-01 14:00:00|15890.12     |
# ...
```

---

## 🎯 PHASE 9: Verify Everything is Working

### Step 9.1: Check HDFS for Streaming Data

```bash
# On any device
hdfs dfs -ls -R /smart_grid/streaming/meter_readings/

# You should see partitioned Parquet files being written
# year=2012/month=01/part-xxxxx.parquet
```

---

### Step 9.2: Check HBase for Anomaly Alerts

```bash
# On Windows 2 (Cluster B)
echo "scan 'anomaly_alerts', {LIMIT => 10}" | hbase shell

# You should see rows with anomaly data
```

---

### Step 9.3: Check Kafka for Anomaly Alerts

```bash
# On Windows 1 (Cluster A)
source cluster.env

kafka-console-consumer.sh \
  --bootstrap-server $KAFKA_BROKER \
  --topic anomaly-alerts \
  --from-beginning \
  --max-messages 5

# You should see JSON messages with detected anomalies
```

---

### Step 9.4: Query Hive for Streaming Results

```bash
# On Mac (Cluster C)
hive

USE smart_grid;

-- Check how many readings have been processed
SELECT COUNT(*) FROM streamed_readings;

-- Check anomaly counts
SELECT 
  anomaly_type, 
  COUNT(*) as count,
  AVG(severity) as avg_severity
FROM streamed_readings
WHERE is_anomaly = TRUE
GROUP BY anomaly_type;

-- View recent anomalies
SELECT 
  meter_id, 
  ts, 
  kwh_hourly, 
  z_score, 
  anomaly_type,
  severity
FROM streamed_readings
WHERE is_anomaly = TRUE
ORDER BY ts DESC
LIMIT 10;
```

---

## 🎨 PHASE 10: Set Up Superset Dashboard (Optional)

### Step 10.1: Install Superset (Mac - Cluster C)

```bash
# Activate Python environment
source /opt/smart_grid_venv/bin/activate

# Install Superset with Hive support
pip install apache-superset
pip install pyhive[hive]
pip install thrift thrift-sasl sasl

# Initialize Superset database
export SUPERSET_SECRET_KEY="smart_grid_secret_$(openssl rand -hex 16)"
superset db upgrade

# Create admin user
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@smartgrid.local \
  --password admin123

# Initialize Superset
superset init

# Start Superset
superset run -h 0.0.0.0 -p 8088 --with-threads --reload &
```

---

### Step 10.2: Configure Superset Database Connections

1. **Open Superset in browser:** http://mac-cluster-c:8088
2. **Login:** admin / admin123
3. **Add Hive Connection:**
   - Settings → Database Connections → + Database
   - Database Type: Apache Hive
   - Display Name: SmartGrid Hive
   - SQLAlchemy URI: `hive://mac-cluster-c:10000/smart_grid`
   - Test Connection → Save

4. **Add Datasets:**
   - Datasets → + Dataset
   - Database: SmartGrid Hive
   - Schema: smart_grid
   - Table: meter_readings (repeat for all tables)

5. **Create Charts:**
   - Use the views we created (v_hourly_anomaly_counts, etc.)
   - Create line charts, pie charts, tables
   - Add to a new Dashboard

---

## ✅ VERIFICATION CHECKLIST

At this point, you should have:

- [x] **3 terminals running continuously:**
  - Terminal 1 (Windows 1): Kafka Producer
  - Terminal 2 (Windows 1): Spark Streaming Job
  - Terminal 3 (Windows 2): HBase API

- [x] **Data flowing through pipeline:**
  - Kafka → Spark Streaming → HDFS + HBase + Kafka

- [x] **Data queryable in:**
  - HDFS: Parquet files in /smart_grid/streaming/
  - HBase: Anomalies in anomaly_alerts table
  - Hive: streamed_readings table has rows

- [x] **ML Model:**
  - Trained and saved in HDFS
  - Can generate forecasts on demand

- [x] **Optional: Superset Dashboard:**
  - Running on port 8088
  - Connected to Hive
  - Displaying real-time metrics

---

## 🔄 Daily Operations

**To start the pipeline each day:**

```bash
# 1. Start all clusters (in order: B → C → A)
ssh wsl-cluster-b && bash start_cluster_b.sh
ssh mac-cluster-c && bash start_cluster_c.sh
ssh wsl-cluster-a && bash start_cluster_a.sh

# 2. Wait 2 minutes for everything to initialize

# 3. Start the 3 terminals:
# Terminal 1: python producer.py --loop
# Terminal 2: spark-submit ... anomaly_detector.py
# Terminal 3: python hbase_api.py

# 4. Optional: Generate daily forecast
spark-submit run_forecast.py --zone AEP --hours 24
```

**To stop the pipeline:**

```bash
# 1. Ctrl+C on all 3 terminals
# 2. Stop clusters (reverse order: A → C → B)
ssh wsl-cluster-a && stop-all.sh && kafka-server-stop.sh && zookeeper-server-stop.sh
ssh mac-cluster-c && stop-all.sh && pkill -f Hive
ssh wsl-cluster-b && stop-hbase.sh && stop-dfs.sh
```

---

## 🎉 You're Done!

Your Smart Grid Analytics Platform is now fully operational!

**What you have:**
- Real-time anomaly detection (Z-score based)
- Distributed storage (HDFS + HBase)
- SQL analytics (Hive)
- ML-based demand forecasting (Spark MLlib)
- Live data visualization (optional Superset)

**Next steps:**
- Process the FULL London dataset (10 GB)
- Set up automated forecasting (cron job)
- Build custom Superset dashboards
- Experiment with different anomaly thresholds
- Add more data sources
