#!/usr/bin/env bash
# ============================================================
#  start_cluster_a.sh
#  Startup script for Cluster A (Windows 1 WSL2)
#  Streaming Layer: Kafka + Spark Streaming
# ============================================================

set -euo pipefail

echo "========================================="
echo "  Starting Cluster A (Streaming Layer)"
echo "========================================="

# Source environment
source /opt/smart_grid_env.sh

# 1. Start Zookeeper for Kafka
echo "[1/4] Starting Zookeeper..."
nohup $KAFKA_HOME/bin/zookeeper-server-start.sh \
  $KAFKA_HOME/config/zookeeper.properties \
  > /tmp/zookeeper.log 2>&1 &

sleep 5
echo "  ✓ Zookeeper started (port 2181)"

# 2. Start Kafka Broker
echo "[2/4] Starting Kafka Broker..."
nohup $KAFKA_HOME/bin/kafka-server-start.sh \
  $KAFKA_HOME/config/server.properties \
  > /tmp/kafka.log 2>&1 &

sleep 10
echo "  ✓ Kafka Broker started (port 9092)"

# 3. Create Kafka Topics
echo "[3/4] Creating Kafka topics..."
$KAFKA_HOME/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic meter-readings \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=86400000

$KAFKA_HOME/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic anomaly-alerts \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000

echo "  ✓ Topics created: meter-readings, anomaly-alerts"

# 4. Start Spark Master and Worker
echo "[4/4] Starting Spark..."
$SPARK_HOME/sbin/start-master.sh
sleep 3
$SPARK_HOME/sbin/start-worker.sh spark://wsl-cluster-a:7077

echo "  ✓ Spark Master started (port 7077, Web UI: 8080)"

echo ""
echo "========================================="
echo "  Cluster A Ready!"
echo "========================================="
echo "  Kafka Broker: wsl-cluster-a:9092"
echo "  Spark Master: spark://wsl-cluster-a:7077"
echo "  Spark Web UI: http://wsl-cluster-a:8080"
echo ""
echo "Logs:"
echo "  Zookeeper: /tmp/zookeeper.log"
echo "  Kafka:     /tmp/kafka.log"
echo "  Spark:     $SPARK_HOME/logs/"
echo ""
echo "Next: Start producer with 'python producer.py'"
