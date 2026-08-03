#!/usr/bin/env bash
# ============================================================
#  check_cluster_health.sh
#  Comprehensive health check for all cluster components
# ============================================================

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================="
echo "  Smart Grid Cluster Health Check"
echo "========================================="
echo ""

# Load config if exists
if [ -f /opt/cluster.env ]; then
    source /opt/cluster.env
fi

# Helper functions
check_port() {
    local host=$1
    local port=$2
    local service=$3
    
    if nc -z -w 2 "$host" "$port" 2>/dev/null; then
        echo -e "${GREEN}✓${NC} $service ($host:$port)"
        return 0
    else
        echo -e "${RED}✗${NC} $service ($host:$port) - NOT REACHABLE"
        return 1
    fi
}

check_process() {
    local process=$1
    if pgrep -f "$process" >/dev/null; then
        echo -e "${GREEN}✓${NC} $process is running"
        return 0
    else
        echo -e "${RED}✗${NC} $process is NOT running"
        return 1
    fi
}

check_hdfs_dir() {
    local path=$1
    if hdfs dfs -test -d "$path" 2>/dev/null; then
        echo -e "${GREEN}✓${NC} HDFS directory exists: $path"
        return 0
    else
        echo -e "${YELLOW}!${NC} HDFS directory missing: $path"
        return 1
    fi
}

# Detect cluster type
CLUSTER_TYPE=""
if pgrep -f "kafka.Kafka" >/dev/null; then
    CLUSTER_TYPE="A (Streaming)"
elif pgrep -f "NameNode" >/dev/null; then
    CLUSTER_TYPE="B (Storage)"
elif pgrep -f "HiveMetaStore" >/dev/null; then
    CLUSTER_TYPE="C (Analytics)"
fi

echo "Detected Cluster: $CLUSTER_TYPE"
echo ""

# Check Java
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Java Environment"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if command -v java &>/dev/null; then
    JAVA_VER=$(java -version 2>&1 | head -1 | cut -d'"' -f2)
    if [[ "$JAVA_VER" == 1.8.* ]]; then
        echo -e "${GREEN}✓${NC} Java 8 installed: $JAVA_VER"
    else
        echo -e "${YELLOW}!${NC} Java version: $JAVA_VER (should be 1.8.x)"
    fi
    
    if [ -n "${JAVA_HOME:-}" ]; then
        echo -e "${GREEN}✓${NC} JAVA_HOME set: $JAVA_HOME"
    else
        echo -e "${RED}✗${NC} JAVA_HOME not set"
    fi
else
    echo -e "${RED}✗${NC} Java not found in PATH"
fi
echo ""

# Check Network Connectivity
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Network Connectivity"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for host in wsl-cluster-a wsl-cluster-b mac-cluster-c; do
    if ping -c 1 -W 2 "$host" >/dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} $host is reachable"
    else
        echo -e "${RED}✗${NC} $host is NOT reachable"
    fi
done
echo ""

# Check Core Services
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Core Services (All Clusters)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# HDFS (should be accessible from all)
if [ -n "${CLUSTER_B_MASTER:-}" ]; then
    check_port "$CLUSTER_B_MASTER" 9000 "HDFS NameNode"
    check_port "$CLUSTER_B_MASTER" 9870 "HDFS Web UI"
fi

# HBase
if [ -n "${CLUSTER_B_MASTER:-}" ]; then
    check_port "$CLUSTER_B_MASTER" 16000 "HBase Master"
    check_port "$CLUSTER_B_MASTER" 9090 "HBase Thrift"
fi

# Kafka
if [ -n "${CLUSTER_A_MASTER:-}" ]; then
    check_port "$CLUSTER_A_MASTER" 2181 "Zookeeper (Kafka)"
    check_port "$CLUSTER_A_MASTER" 9092 "Kafka Broker"
fi

# Hive
if [ -n "${CLUSTER_C_MASTER:-}" ]; then
    check_port "$CLUSTER_C_MASTER" 9083 "Hive Metastore"
    check_port "$CLUSTER_C_MASTER" 10000 "HiveServer2"
fi

# Spark
if [ -n "${CLUSTER_A_MASTER:-}" ]; then
    check_port "$CLUSTER_A_MASTER" 7077 "Spark Master (A)"
fi
if [ -n "${CLUSTER_C_MASTER:-}" ]; then
    check_port "$CLUSTER_C_MASTER" 7077 "Spark Master (C)"
fi

echo ""

# Local Process Checks
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Local Processes (This Device)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Hadoop processes
if command -v jps &>/dev/null; then
    JPS_OUTPUT=$(jps)
    
    for proc in NameNode DataNode ResourceManager NodeManager; do
        if echo "$JPS_OUTPUT" | grep -q "$proc"; then
            echo -e "${GREEN}✓${NC} $proc"
        fi
    done
    
    # HBase processes
    for proc in HMaster HRegionServer; do
        if echo "$JPS_OUTPUT" | grep -q "$proc"; then
            echo -e "${GREEN}✓${NC} $proc"
        fi
    done
    
    # Kafka processes
    if echo "$JPS_OUTPUT" | grep -q "Kafka"; then
        echo -e "${GREEN}✓${NC} Kafka"
    fi
    if echo "$JPS_OUTPUT" | grep -q "QuorumPeerMain"; then
        echo -e "${GREEN}✓${NC} Zookeeper"
    fi
fi

# Hive processes
check_process "HiveMetaStore" 2>/dev/null || true
check_process "HiveServer2" 2>/dev/null || true

# Spark processes
check_process "org.apache.spark.deploy.master.Master" 2>/dev/null || true
check_process "org.apache.spark.deploy.worker.Worker" 2>/dev/null || true

echo ""

# HDFS Checks (if applicable)
if command -v hdfs &>/dev/null && [ -n "${CLUSTER_B_MASTER:-}" ]; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "HDFS Status"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    if hdfs dfsadmin -report 2>/dev/null | head -20; then
        echo ""
        echo "HDFS Directory Structure:"
        check_hdfs_dir "/smart_grid"
        check_hdfs_dir "/smart_grid/raw"
        check_hdfs_dir "/smart_grid/clean"
        check_hdfs_dir "/smart_grid/parquet"
        check_hdfs_dir "/hbase"
    else
        echo -e "${RED}✗${NC} Unable to connect to HDFS"
    fi
    echo ""
fi

# HBase Checks (if applicable)
if command -v hbase &>/dev/null && pgrep -f "HMaster" >/dev/null; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "HBase Status"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    if echo "status" | hbase shell 2>/dev/null | grep -q "active"; then
        echo -e "${GREEN}✓${NC} HBase is active"
        
        echo ""
        echo "HBase Tables:"
        echo "list" | hbase shell 2>/dev/null | grep -E "^(meter_readings|anomaly_alerts|meter_state)" || echo "No tables found"
    else
        echo -e "${RED}✗${NC} HBase is not responding"
    fi
    echo ""
fi

# Kafka Checks (if applicable)
if command -v kafka-topics.sh &>/dev/null && [ -n "${KAFKA_BROKER:-}" ]; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Kafka Status"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    if kafka-topics.sh --list --bootstrap-server "${KAFKA_BROKER}" 2>/dev/null; then
        echo ""
    else
        echo -e "${RED}✗${NC} Unable to connect to Kafka broker"
    fi
    echo ""
fi

# Resource Usage
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "System Resources"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Memory
if command -v free &>/dev/null; then
    free -h | grep -E "^(Mem|Swap):"
elif command -v vm_stat &>/dev/null; then
    # Mac
    echo "Memory (Mac):"
    vm_stat | perl -ne '/page size of (\d+)/ and $size=$1; /Pages\s+([^:]+)[^\d]+(\d+)/ and printf("%-16s % 16.2f MB\n", "$1:", $2 * $size / 1048576);'
fi

echo ""

# Disk space
echo "Disk Space:"
df -h | grep -E "(Filesystem|/opt|/tmp|/home)" | head -5

echo ""
echo "========================================="
echo "  Health Check Complete"
echo "========================================="
echo ""
echo "Web UIs:"
echo "  HDFS:        http://${CLUSTER_B_MASTER:-wsl-cluster-b}:9870"
echo "  HBase:       http://${CLUSTER_B_MASTER:-wsl-cluster-b}:16010"
echo "  Spark (A):   http://${CLUSTER_A_MASTER:-wsl-cluster-a}:8080"
echo "  Spark (C):   http://${CLUSTER_C_MASTER:-mac-cluster-c}:8080"
echo ""
