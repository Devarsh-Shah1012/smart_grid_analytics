#!/usr/bin/env bash
# ============================================================
#  start_cluster_b.sh
#  Startup script for Cluster B (Windows 2 WSL2)
#  Storage Layer: HDFS + HBase
# ============================================================

set -euo pipefail

echo "========================================="
echo "  Starting Cluster B (Storage Layer)"
echo "========================================="

# Source environment
source /opt/smart_grid_env.sh

# 1. Start HDFS
echo "[1/4] Starting HDFS..."
$HADOOP_HOME/sbin/start-dfs.sh

# Wait for NameNode to be ready
echo "  Waiting for NameNode..."
for i in {1..30}; do
  if nc -z localhost 9000 2>/dev/null; then
    echo "  ✓ HDFS NameNode ready (port 9000, Web UI: 9870)"
    break
  fi
  sleep 2
done

# 2. Create HDFS directories (idempotent)
echo "[2/4] Creating HDFS directory structure..."
hdfs dfs -mkdir -p /smart_grid/raw/london 2>/dev/null || true
hdfs dfs -mkdir -p /smart_grid/raw/pjm 2>/dev/null || true
hdfs dfs -mkdir -p /smart_grid/raw/uci 2>/dev/null || true
hdfs dfs -mkdir -p /smart_grid/clean/meter_readings 2>/dev/null || true
hdfs dfs -mkdir -p /smart_grid/parquet/meter_readings 2>/dev/null || true
hdfs dfs -mkdir -p /smart_grid/streaming/meter_readings 2>/dev/null || true
hdfs dfs -mkdir -p /smart_grid/models 2>/dev/null || true
hdfs dfs -mkdir -p /smart_grid/forecasts 2>/dev/null || true
hdfs dfs -mkdir -p /smart_grid/warehouse 2>/dev/null || true
hdfs dfs -mkdir -p /smart_grid/checkpoints 2>/dev/null || true
hdfs dfs -mkdir -p /hbase 2>/dev/null || true
hdfs dfs -chmod -R 777 /smart_grid 2>/dev/null || true

echo "  ✓ HDFS directories created"

# 3. Start HBase
echo "[3/4] Starting HBase..."
$HBASE_HOME/bin/start-hbase.sh

sleep 10
echo "  ✓ HBase Master started (port 16000, Web UI: 16010)"

# 4. Start HBase Thrift Server
echo "[4/4] Starting HBase Thrift server..."
nohup $HBASE_HOME/bin/hbase thrift start -p 9090 \
  > /tmp/hbase-thrift.log 2>&1 &

sleep 3
echo "  ✓ HBase Thrift started (port 9090)"

echo ""
echo "========================================="
echo "  Cluster B Ready!"
echo "========================================="
echo "  HDFS NameNode:    hdfs://wsl-cluster-b:9000"
echo "  HDFS Web UI:      http://wsl-cluster-b:9870"
echo "  HBase Master:     wsl-cluster-b:16000"
echo "  HBase Web UI:     http://wsl-cluster-b:16010"
echo "  HBase Thrift:     wsl-cluster-b:9090"
echo ""
echo "Logs:"
echo "  HDFS:        $HADOOP_HOME/logs/"
echo "  HBase:       $HBASE_HOME/logs/"
echo "  Thrift:      /tmp/hbase-thrift.log"
echo ""
echo "Verify HDFS: hdfs dfsadmin -report"
echo "Verify HBase: echo 'status' | hbase shell"
