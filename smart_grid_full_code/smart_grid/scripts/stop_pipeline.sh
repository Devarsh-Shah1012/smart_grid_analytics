#!/usr/bin/env bash
# ============================================================
#  scripts/stop_pipeline.sh
#  Gracefully shuts down all Smart Grid platform services.
#  Run on Cluster A master.
# ============================================================

set -euo pipefail
source "$(dirname "$0")/../config/cluster.env"

HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
SPARK_HOME=${SPARK_HOME:-/opt/spark}
KAFKA_HOME=${KAFKA_HOME:-/opt/kafka}
HBASE_HOME=${HBASE_HOME:-/opt/hbase}
HIVE_HOME=${HIVE_HOME:-/opt/hive}

log() { echo "[$(date '+%H:%M:%S')] $*"; }
ok()  { echo "[$(date '+%H:%M:%S')] ✅ $*"; }

log "=== Stopping Smart Grid Platform ==="

# ── Kill Streaming jobs first ─────────────────────────────────
log "Stopping Spark Streaming jobs..."
ssh "${CLUSTER_A_MASTER}" \
    "yarn application -list 2>/dev/null | grep SmartGrid | awk '{print \$1}' \
     | xargs -I{} yarn application -kill {} 2>/dev/null || true"
ok "Streaming jobs signalled"

# ── Stop Kafka ────────────────────────────────────────────────
log "Stopping Kafka Broker..."
ssh "${CLUSTER_A_MASTER}" \
    "${KAFKA_HOME}/bin/kafka-server-stop.sh 2>/dev/null || true"
sleep 3

log "Stopping Kafka Zookeeper..."
ssh "${CLUSTER_A_MASTER}" \
    "${KAFKA_HOME}/bin/zookeeper-server-stop.sh 2>/dev/null || true"
ok "Kafka stopped"

# ── Stop Spark ────────────────────────────────────────────────
log "Stopping Spark (Cluster A)..."
ssh "${CLUSTER_A_MASTER}" "${SPARK_HOME}/sbin/stop-all.sh 2>/dev/null || true"

log "Stopping Spark (Cluster C)..."
ssh "${CLUSTER_C_MASTER}" "${SPARK_HOME}/sbin/stop-all.sh 2>/dev/null || true"
ok "Spark stopped"

# ── Stop Hive ─────────────────────────────────────────────────
log "Stopping HiveServer2 and Metastore..."
ssh "${CLUSTER_C_MASTER}" \
    "pkill -f 'HiveServer2' 2>/dev/null || true; \
     pkill -f 'HiveMetaStore' 2>/dev/null || true"
ok "Hive stopped"

# ── Stop HBase Thrift + API ───────────────────────────────────
log "Stopping HBase Thrift server and Flask API..."
ssh "${CLUSTER_B_MASTER}" \
    "pkill -f 'hbase thrift' 2>/dev/null || true; \
     pkill -f 'hbase_api.py' 2>/dev/null || true"
ok "HBase Thrift/API stopped"

# ── Stop HBase ────────────────────────────────────────────────
log "Stopping HBase (Cluster B)..."
ssh "${CLUSTER_B_MASTER}" "${HBASE_HOME}/bin/stop-hbase.sh"
ok "HBase stopped"

# ── Stop YARN ────────────────────────────────────────────────
log "Stopping YARN..."
ssh "${CLUSTER_B_MASTER}" "${HADOOP_HOME}/sbin/stop-yarn.sh"
ok "YARN stopped"

# ── Stop HDFS ────────────────────────────────────────────────
log "Stopping HDFS..."
ssh "${CLUSTER_B_MASTER}" "${HADOOP_HOME}/sbin/stop-dfs.sh"
ok "HDFS stopped"

echo ""
echo "✅ All Smart Grid services stopped."
