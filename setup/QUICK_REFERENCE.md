# Smart Grid Platform - Quick Reference Card
## For Mac + 2× Windows (WSL2) Setup

---

## 🚀 Daily Startup Sequence

**Start in this order:**

```bash
# 1. Windows 2 (Cluster B - Storage) - START FIRST!
ssh wsl-cluster-b
bash start_cluster_b.sh

# 2. Mac (Cluster C - Analytics)
ssh mac-cluster-c
bash start_cluster_c.sh

# 3. Windows 1 (Cluster A - Streaming)
ssh wsl-cluster-a
bash start_cluster_a.sh

# 4. Verify all clusters are healthy
bash check_cluster_health.sh
```

---

## 📊 Web UIs

| Service | URL | Login |
|---------|-----|-------|
| HDFS NameNode | http://wsl-cluster-b:9870 | None |
| HBase Master | http://wsl-cluster-b:16010 | None |
| Spark Master (A) | http://wsl-cluster-a:8080 | None |
| Spark Master (C) | http://mac-cluster-c:8080 | None |
| Superset | http://mac-cluster-c:8088 | admin/admin123 |

**Note for WSL2:** If you can't access from Windows browser, add port forwarding:
```powershell
# In PowerShell as Admin on Windows host:
netsh interface portproxy add v4tov4 listenport=9870 listenaddress=0.0.0.0 connectport=9870 connectaddress=<WSL_IP>
```

---

## 🛠️ Common Commands

### HDFS Operations
```bash
# List files
hdfs dfs -ls /smart_grid/

# Check cluster health
hdfs dfsadmin -report

# Create directory
hdfs dfs -mkdir -p /smart_grid/test

# Upload file
hdfs dfs -put local_file.csv /smart_grid/raw/

# View file
hdfs dfs -cat /smart_grid/clean/meter_readings/part-00000
```

### HBase Operations
```bash
# Open HBase shell
hbase shell

# List tables
> list

# Describe table
> describe 'meter_readings'

# Scan table (limit 10 rows)
> scan 'anomaly_alerts', {LIMIT => 10}

# Count rows
> count 'anomaly_alerts'

# Exit
> exit
```

### Kafka Operations
```bash
# List topics
kafka-topics.sh --list --bootstrap-server wsl-cluster-a:9092

# Describe topic
kafka-topics.sh --describe --topic meter-readings --bootstrap-server wsl-cluster-a:9092

# Consume messages (from beginning)
kafka-console-consumer.sh \
  --bootstrap-server wsl-cluster-a:9092 \
  --topic meter-readings \
  --from-beginning \
  --max-messages 10

# Check consumer groups
kafka-consumer-groups.sh --list --bootstrap-server wsl-cluster-a:9092
```

### Hive Operations
```bash
# Open Hive CLI
hive

# Or use Beeline (preferred)
beeline -u jdbc:hive2://mac-cluster-c:10000

# List databases
> show databases;

# Use smart_grid database
> use smart_grid;

# Show tables
> show tables;

# Query
> select count(*) from meter_readings;
```

### Spark Operations
```bash
# Submit Spark job
spark-submit \
  --master spark://mac-cluster-c:7077 \
  --executor-memory 2g \
  --num-executors 2 \
  your_script.py

# Open Spark shell
spark-shell --master spark://mac-cluster-c:7077

# PySpark shell
pyspark --master spark://mac-cluster-c:7077

# Check running applications
curl http://mac-cluster-c:8080/json/ | jq
```

---

## 🎯 Running the Pipeline

### 1. Start Kafka Producer (Windows 1)
```bash
cd /path/to/project
source /opt/smart_grid_venv/bin/activate
python kafka/producer.py --speed 10 --loop
```

### 2. Start Spark Streaming Job (Windows 1)
```bash
spark-submit \
  --master spark://wsl-cluster-a:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  --executor-memory 3g \
  --conf spark.sql.streaming.checkpointLocation=hdfs://wsl-cluster-b:9000/smart_grid/checkpoints/anomaly_detector \
  spark/streaming/anomaly_detector.py
```

### 3. Run Hourly Forecast (Mac - via cron or manually)
```bash
spark-submit \
  --master spark://mac-cluster-c:7077 \
  --executor-memory 4g \
  spark/batch/run_forecast.py --zone AEP --hours 24
```

---

## 🔧 Troubleshooting

### Cannot connect to HDFS
```bash
# Check NameNode is running
ssh wsl-cluster-b
jps | grep NameNode

# Check network connectivity
ping wsl-cluster-b

# Check /etc/hosts has correct entry
cat /etc/hosts | grep wsl-cluster-b

# Try to access directly
hdfs dfs -ls hdfs://wsl-cluster-b:9000/
```

### Kafka topics not created
```bash
# Manually create topics
kafka-topics.sh \
  --create \
  --if-not-exists \
  --bootstrap-server wsl-cluster-a:9092 \
  --topic meter-readings \
  --partitions 6 \
  --replication-factor 1
```

### HBase won't start
```bash
# Check HDFS is running first
hdfs dfsadmin -report

# Check /hbase directory exists
hdfs dfs -ls /hbase

# Check Java version
java -version  # Must be 1.8.x

# Check HBase logs
tail -f /opt/hbase/logs/hbase-*-master-*.log
```

### Spark job fails with "Connection refused"
```bash
# Verify Spark master is running
curl http://wsl-cluster-a:8080

# Check if worker registered
curl http://wsl-cluster-a:8080/json/ | jq '.aliveworkers'

# Restart Spark
stop-all.sh
start-all.sh
```

### WSL IP keeps changing
```bash
# Get current WSL IP
ip addr show eth0 | grep "inet " | awk '{print $2}' | cut -d/ -f1

# Update /etc/hosts on all machines
sudo nano /etc/hosts

# Or run update_cluster_config.sh again
```

---

## 🛑 Shutdown Sequence

**Shut down in REVERSE order:**

```bash
# 1. Stop Spark Streaming job (Ctrl+C on Windows 1)
# 2. Stop Kafka producer (Ctrl+C on Windows 1)

# 3. Stop Cluster A (Windows 1)
ssh wsl-cluster-a
stop-all.sh  # Spark
kafka-server-stop.sh
zookeeper-server-stop.sh

# 4. Stop Cluster C (Mac)
ssh mac-cluster-c
stop-all.sh  # Spark
pkill -f HiveServer2
pkill -f HiveMetaStore

# 5. Stop Cluster B (Windows 2) - LAST!
ssh wsl-cluster-b
stop-hbase.sh
pkill -f "hbase thrift"
stop-dfs.sh
```

---

## 📝 Important File Locations

### Configuration Files
```
/opt/cluster.env                     # Cluster IPs and settings
/opt/smart_grid_env.sh              # Environment variables
/opt/hadoop/etc/hadoop/             # Hadoop configs
/opt/spark/conf/                    # Spark configs
/opt/hbase/conf/                    # HBase configs
/opt/hive/conf/                     # Hive configs
/opt/kafka/config/                  # Kafka configs
```

### Log Files
```
# Hadoop
/opt/hadoop/logs/

# Spark
/opt/spark/logs/

# HBase
/opt/hbase/logs/

# Kafka
/opt/kafka/logs/

# Hive
/tmp/hive-metastore.log
/tmp/hiveserver2.log

# Custom services
/tmp/hbase-thrift.log
/tmp/zookeeper.log
/tmp/kafka.log
```

### Data Directories
```
# HDFS (on Cluster B)
/opt/hadoop_data/namenode/
/opt/hadoop_data/datanode/

# HBase (on Cluster B)
/opt/hbase_data/

# Kafka (on Cluster A)
/opt/kafka_data/logs/

# Local datasets (before HDFS upload)
/opt/smart_grid_data/london/
/opt/smart_grid_data/pjm/
/opt/smart_grid_data/uci/
```

---

## 🆘 Emergency Recovery

### Full Cluster Reset
```bash
# WARNING: This deletes ALL data!

# On Cluster B (Storage)
stop-dfs.sh
stop-hbase.sh
rm -rf /opt/hadoop_data/*
rm -rf /opt/hbase_data/*
hdfs namenode -format -force
start-dfs.sh
start-hbase.sh

# On Cluster A (Streaming)
kafka-server-stop.sh
zookeeper-server-stop.sh
rm -rf /opt/kafka_data/logs/*
# Restart Kafka
```

### Recreate HDFS Directories
```bash
hdfs dfs -mkdir -p /smart_grid/{raw,clean,parquet,streaming,models,forecasts,warehouse,checkpoints}
hdfs dfs -mkdir -p /hbase
hdfs dfs -chmod -R 777 /smart_grid
```

### Recreate HBase Tables
```bash
hbase shell /path/to/create_tables.rb
```

### Recreate Kafka Topics
```bash
bash /path/to/kafka_topics.sh
```

---

## 📞 Quick Help

**Can't remember which cluster runs what?**
- **Windows 1**: Kafka, Spark Streaming
- **Windows 2**: HDFS, HBase
- **Mac**: Hive, Pig, Spark Batch, Superset

**Services won't start?**
1. Check Java version: `java -version` (must be 1.8.x)
2. Check /etc/hosts has all 3 IPs
3. Check ports aren't already in use: `netstat -tulpn | grep <PORT>`
4. Check logs in /opt/*/logs/

**Still stuck?**
Run: `bash check_cluster_health.sh` for full diagnostics
