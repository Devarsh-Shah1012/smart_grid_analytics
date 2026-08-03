#!/usr/bin/env bash
# ============================================================
#  scripts/start_pipeline.sh
#  Starts all Smart Grid platform services in the correct order.
#  Run on Cluster A master (it SSH-es to other clusters).
#
#  Usage: bash scripts/start_pipeline.sh [--skip-kafka]
# ============================================================

set -euo pipefail
source "$(dirname "$0")/../config/cluster.env"

SKIP_KAFKA=false
[[ "${1:-}" == "--skip-kafka" ]] && SKIP_KAFKA=true

HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
SPARK_HOME=${SPARK_HOME:-/opt/spark}
KAFKA_HOME=${KAFKA_HOME:-/opt/kafka}
HBASE_HOME=${HBASE_HOME:-/opt/hbase}
HIVE_HOME=${HIVE_HOME:-/opt/hive}

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "[$(date '+%H:%M:%S')] ✅ $*"; }
wait_port() {
    local host=$1 port=$2 name=$3 retries=30
    log "Waiting for ${name} on ${host}:${port}..."
    for i in $(seq 1 $retries); do
        nc -z "$host" "$port" 2>/dev/null && { ok "${name} is up"; return 0; }
        sleep 2
    done
    echo "⚠️  ${name} did not come up on ${host}:${port} after ${retries} attempts"
    return 1
}

# ── STEP 1: Start HDFS (Cluster B) ───────────────────────────
log "=== STEP 1: Starting HDFS (Cluster B) ==="
ssh "${CLUSTER_B_MASTER}" "${HADOOP_HOME}/sbin/start-dfs.sh"
wait_port "$CLUSTER_B_MASTER" 9000 "HDFS NameNode"
wait_port "$CLUSTER_B_MASTER" 9870 "HDFS Web UI"
ok "HDFS started"

# ── STEP 2: Start YARN (Cluster B) ───────────────────────────
log "=== STEP 2: Starting YARN ResourceManager ==="
ssh "${CLUSTER_B_MASTER}" "${HADOOP_HOME}/sbin/start-yarn.sh"
wait_port "$CLUSTER_B_MASTER" 8088 "YARN ResourceManager"
ok "YARN started"

# ── STEP 3: Start Zookeeper for HBase (Cluster B) ────────────
log "=== STEP 3: Starting Zookeeper (HBase, Cluster B) ==="
ssh "${CLUSTER_B_MASTER}" "${HBASE_HOME}/bin/hbase zookeeper start &"
wait_port "$CLUSTER_B_MASTER" 2181 "HBase Zookeeper"
ok "HBase Zookeeper started"

# ── STEP 4: Start HBase (Cluster B) ──────────────────────────
log "=== STEP 4: Starting HBase (Cluster B) ==="
ssh "${CLUSTER_B_MASTER}" "${HBASE_HOME}/bin/start-hbase.sh"
wait_port "$CLUSTER_B_MASTER" 16000 "HBase Master"
wait_port "$CLUSTER_B_MASTER" 16010 "HBase Web UI"
ok "HBase started"

# ── STEP 5: Start Hive Metastore (Cluster C) ─────────────────
log "=== STEP 5: Starting Hive Metastore (Cluster C) ==="
ssh "${CLUSTER_C_MASTER}" \
    "nohup ${HIVE_HOME}/bin/hive --service metastore \
     > /var/log/hive-metastore.log 2>&1 &"
wait_port "$CLUSTER_C_MASTER" 9083 "Hive Metastore"
ok "Hive Metastore started"

# ── STEP 6: Start HiveServer2 (Cluster C) ────────────────────
log "=== STEP 6: Starting HiveServer2 (Cluster C) ==="
ssh "${CLUSTER_C_MASTER}" \
    "nohup ${HIVE_HOME}/bin/hive --service hiveserver2 \
     > /var/log/hiveserver2.log 2>&1 &"
wait_port "$CLUSTER_C_MASTER" 10000 "HiveServer2"
ok "HiveServer2 started"

# ── STEP 7: Start Spark (Cluster A — streaming) ───────────────
log "=== STEP 7: Starting Spark (Cluster A) ==="
ssh "${CLUSTER_A_MASTER}" "${SPARK_HOME}/sbin/start-all.sh"
wait_port "$CLUSTER_A_MASTER" 7077 "Spark Master (A)"
wait_port "$CLUSTER_A_MASTER" 8080 "Spark Web UI (A)"
ok "Spark Cluster A started"

# ── STEP 8: Start Spark (Cluster C — batch/analytics) ─────────
log "=== STEP 8: Starting Spark (Cluster C) ==="
ssh "${CLUSTER_C_MASTER}" "${SPARK_HOME}/sbin/start-all.sh"
wait_port "$CLUSTER_C_MASTER" 7077 "Spark Master (C)"
ok "Spark Cluster C started"

# ── STEP 9: Start Zookeeper for Kafka (Cluster A) ────────────
if [[ "$SKIP_KAFKA" == "false" ]]; then
    log "=== STEP 9: Starting Zookeeper for Kafka (Cluster A) ==="
    ssh "${CLUSTER_A_MASTER}" \
        "nohup ${KAFKA_HOME}/bin/zookeeper-server-start.sh \
         ${KAFKA_HOME}/config/zookeeper.properties \
         > /var/log/zookeeper-kafka.log 2>&1 &"
    wait_port "$CLUSTER_A_MASTER" 2181 "Kafka Zookeeper"
    ok "Kafka Zookeeper started"

    # ── STEP 10: Start Kafka Broker ──────────────────────────
    log "=== STEP 10: Starting Kafka Broker (Cluster A) ==="
    sleep 3
    ssh "${CLUSTER_A_MASTER}" \
        "nohup ${KAFKA_HOME}/bin/kafka-server-start.sh \
         ${KAFKA_HOME}/config/server.properties \
         > /var/log/kafka-broker.log 2>&1 &"
    wait_port "$CLUSTER_A_MASTER" 9092 "Kafka Broker"
    ok "Kafka Broker started"

    # ── STEP 11: Create Kafka topics ─────────────────────────
    log "=== STEP 11: Creating Kafka topics ==="
    bash "$(dirname "$0")/../config/kafka_topics.sh"
    ok "Kafka topics created"
fi

# ── STEP 12: Start HBase Thrift server (for happybase) ───────
log "=== STEP 12: Starting HBase Thrift server (Cluster B) ==="
ssh "${CLUSTER_B_MASTER}" \
    "nohup ${HBASE_HOME}/bin/hbase thrift start -p 9090 \
     > /var/log/hbase-thrift.log 2>&1 &"
wait_port "$CLUSTER_B_MASTER" 9090 "HBase Thrift"
ok "HBase Thrift server started"

# ── STEP 13: Start HBase Flask API (for Superset) ────────────
log "=== STEP 13: Starting HBase Flask API (Cluster B) ==="
ssh "${CLUSTER_B_MASTER}" \
    "nohup python3 /opt/smart_grid/dashboard/hbase_api.py \
     > /var/log/hbase_api.log 2>&1 &"
wait_port "$CLUSTER_B_MASTER" 5050 "HBase API"
ok "HBase API started"

# ── Summary ───────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║       Smart Grid Platform — All Services UP          ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  HDFS NameNode UI : http://${CLUSTER_B_MASTER}:9870        ║"
echo "║  YARN UI          : http://${CLUSTER_B_MASTER}:8088        ║"
echo "║  HBase Master UI  : http://${CLUSTER_B_MASTER}:16010       ║"
echo "║  Spark UI (A)     : http://${CLUSTER_A_MASTER}:8080        ║"
echo "║  Spark UI (C)     : http://${CLUSTER_C_MASTER}:8080        ║"
echo "║  HBase API        : http://${CLUSTER_B_MASTER}:5050        ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "Next steps:"
echo "  1. python kafka/producer.py &"
echo "  2. spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \\"
echo "       spark/streaming/anomaly_detector.py"
echo "  3. spark-submit spark/batch/run_forecast.py"
