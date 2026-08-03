#!/usr/bin/env bash
# ============================================================
#  start_cluster_c.sh
#  Startup script for Cluster C (Mac)
#  Analytics Layer: Hive + Spark + Pig
# ============================================================

set -euo pipefail

echo "========================================="
echo "  Starting Cluster C (Analytics Layer)"
echo "========================================="

# Source environment
source /opt/smart_grid_env.sh

# 1. Start Hive Metastore
echo "[1/3] Starting Hive Metastore..."
nohup $HIVE_HOME/bin/hive --service metastore \
  > /tmp/hive-metastore.log 2>&1 &

sleep 5
echo "  ✓ Hive Metastore started (port 9083)"

# 2. Start HiveServer2
echo "[2/3] Starting HiveServer2..."
nohup $HIVE_HOME/bin/hive --service hiveserver2 \
  > /tmp/hiveserver2.log 2>&1 &

sleep 5
echo "  ✓ HiveServer2 started (port 10000)"

# 3. Start Spark Master and Worker
echo "[3/3] Starting Spark..."
$SPARK_HOME/sbin/start-master.sh
sleep 3
$SPARK_HOME/sbin/start-worker.sh spark://mac-cluster-c:7077

echo "  ✓ Spark Master started (port 7077, Web UI: 8080)"

echo ""
echo "========================================="
echo "  Cluster C Ready!"
echo "========================================="
echo "  Hive Metastore:   thrift://mac-cluster-c:9083"
echo "  HiveServer2:      mac-cluster-c:10000"
echo "  Spark Master:     spark://mac-cluster-c:7077"
echo "  Spark Web UI:     http://mac-cluster-c:8080"
echo ""
echo "Logs:"
echo "  Hive Metastore:  /tmp/hive-metastore.log"
echo "  HiveServer2:     /tmp/hiveserver2.log"
echo "  Spark:           $SPARK_HOME/logs/"
echo ""
echo "Test Hive: beeline -u jdbc:hive2://localhost:10000"
echo "Test Spark: spark-shell --master spark://mac-cluster-c:7077"
