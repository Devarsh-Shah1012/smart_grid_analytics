#!/usr/bin/env bash
# ============================================================
#  update_cluster_config.sh
#  Interactive script to update cluster.env with actual IPs
# ============================================================

set -euo pipefail

echo "========================================="
echo "  Smart Grid Cluster Configuration"
echo "========================================="
echo ""

# Detect current device
if [[ "$OSTYPE" == "darwin"* ]]; then
    DEVICE_TYPE="Mac"
    IP_CMD="ifconfig | grep 'inet ' | grep -v 127.0.0.1 | head -1 | awk '{print \$2}'"
elif [[ -f /proc/version ]] && grep -qi microsoft /proc/version; then
    DEVICE_TYPE="WSL2"
    IP_CMD="ip addr show eth0 | grep 'inet ' | awk '{print \$2}' | cut -d/ -f1"
else
    DEVICE_TYPE="Linux"
    IP_CMD="hostname -I | awk '{print \$1}'"
fi

echo "Detected device type: $DEVICE_TYPE"
echo ""

# Get current IP
CURRENT_IP=$(eval "$IP_CMD")
echo "Current IP address: $CURRENT_IP"
echo ""

# Ask for cluster assignment
echo "Which cluster is this device?"
echo "  1) Cluster A - Streaming (Kafka + Spark Streaming)"
echo "  2) Cluster B - Storage (HDFS + HBase)"
echo "  3) Cluster C - Analytics (Hive + Spark + Pig)"
echo ""
read -p "Enter choice [1-3]: " CLUSTER_CHOICE

case $CLUSTER_CHOICE in
    1)
        CLUSTER_NAME="Cluster A"
        CLUSTER_VAR="CLUSTER_A_MASTER"
        HOSTNAME_VAR="wsl-cluster-a"
        ;;
    2)
        CLUSTER_NAME="Cluster B"
        CLUSTER_VAR="CLUSTER_B_MASTER"
        HOSTNAME_VAR="wsl-cluster-b"
        ;;
    3)
        CLUSTER_NAME="Cluster C"
        CLUSTER_VAR="CLUSTER_C_MASTER"
        HOSTNAME_VAR="mac-cluster-c"
        ;;
    *)
        echo "Invalid choice. Exiting."
        exit 1
        ;;
esac

echo ""
echo "Selected: $CLUSTER_NAME"
echo "Hostname: $HOSTNAME_VAR"
echo ""

# Ask for other cluster IPs
echo "Enter IP addresses for the OTHER clusters:"
echo ""

if [ "$CLUSTER_CHOICE" != "1" ]; then
    read -p "Cluster A IP (Streaming): " CLUSTER_A_IP
else
    CLUSTER_A_IP=$CURRENT_IP
fi

if [ "$CLUSTER_CHOICE" != "2" ]; then
    read -p "Cluster B IP (Storage): " CLUSTER_B_IP
else
    CLUSTER_B_IP=$CURRENT_IP
fi

if [ "$CLUSTER_CHOICE" != "3" ]; then
    read -p "Cluster C IP (Analytics): " CLUSTER_C_IP
else
    CLUSTER_C_IP=$CURRENT_IP
fi

echo ""
echo "========================================="
echo "  Configuration Summary"
echo "========================================="
echo "Cluster A (Streaming): $CLUSTER_A_IP"
echo "Cluster B (Storage):   $CLUSTER_B_IP"
echo "Cluster C (Analytics): $CLUSTER_C_IP"
echo ""

read -p "Is this correct? [y/N]: " CONFIRM
if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi

# Create cluster.env
CONFIG_FILE="/opt/cluster.env"

echo ""
echo "Creating $CONFIG_FILE..."

sudo tee $CONFIG_FILE > /dev/null <<EOF
# ============================================================
#  cluster.env
#  Smart Grid Platform Cluster Configuration
#  Generated: $(date)
# ============================================================

# ── Cluster Hosts ────────────────────────────────────────────
CLUSTER_A_MASTER=$CLUSTER_A_IP
CLUSTER_B_MASTER=$CLUSTER_B_IP
CLUSTER_C_MASTER=$CLUSTER_C_IP

# ── Kafka ────────────────────────────────────────────────────
KAFKA_BROKER=\${CLUSTER_A_MASTER}:9092
ZOOKEEPER_HOST=\${CLUSTER_A_MASTER}:2181
KAFKA_TOPIC_METERS=meter-readings
KAFKA_TOPIC_ALERTS=anomaly-alerts

# ── HDFS ─────────────────────────────────────────────────────
HDFS_NAMENODE=hdfs://\${CLUSTER_B_MASTER}:9000
HDFS_RAW_PATH=/smart_grid/raw/meter_readings
HDFS_CLEAN_PATH=/smart_grid/clean/meter_readings
HDFS_MODELS_PATH=/smart_grid/models
HDFS_FORECASTS_PATH=/smart_grid/forecasts

# ── HBase ────────────────────────────────────────────────────
HBASE_MASTER=\${CLUSTER_B_MASTER}
HBASE_ZOOKEEPER=\${CLUSTER_B_MASTER}:2181
HBASE_TABLE_READINGS=meter_readings
HBASE_TABLE_ALERTS=anomaly_alerts

# ── Hive ─────────────────────────────────────────────────────
HIVE_METASTORE=thrift://\${CLUSTER_C_MASTER}:9083
HIVE_DATABASE=smart_grid

# ── Spark ────────────────────────────────────────────────────
SPARK_MASTER_A=spark://\${CLUSTER_A_MASTER}:7077
SPARK_MASTER_C=spark://\${CLUSTER_C_MASTER}:7077
SPARK_EXECUTOR_MEMORY=3g
SPARK_DRIVER_MEMORY=2g

# ── Streaming Parameters ─────────────────────────────────────
STREAM_TRIGGER_SECONDS=30
ANOMALY_ZSCORE_THRESHOLD=3.0
ROLLING_WINDOW_DAYS=7
KAFKA_REPLAY_SPEED_SECONDS=10

# ── Local Data Directory ─────────────────────────────────────
LOCAL_DATA_DIR=/opt/smart_grid_data
EOF

sudo chmod 644 $CONFIG_FILE

echo "  ✓ Created $CONFIG_FILE"

# Update /etc/hosts
echo ""
echo "Updating /etc/hosts..."

sudo tee -a /etc/hosts > /dev/null <<EOF

# Smart Grid Cluster Hosts (added $(date))
$CLUSTER_A_IP    wsl-cluster-a
$CLUSTER_B_IP    wsl-cluster-b
$CLUSTER_C_IP    mac-cluster-c
EOF

echo "  ✓ Updated /etc/hosts"

# Test connectivity
echo ""
echo "Testing connectivity to other clusters..."

if [ "$CLUSTER_CHOICE" != "1" ]; then
    if ping -c 1 -W 2 wsl-cluster-a >/dev/null 2>&1; then
        echo "  ✓ Cluster A (wsl-cluster-a) reachable"
    else
        echo "  ✗ Cluster A (wsl-cluster-a) NOT reachable"
    fi
fi

if [ "$CLUSTER_CHOICE" != "2" ]; then
    if ping -c 1 -W 2 wsl-cluster-b >/dev/null 2>&1; then
        echo "  ✓ Cluster B (wsl-cluster-b) reachable"
    else
        echo "  ✗ Cluster B (wsl-cluster-b) NOT reachable"
    fi
fi

if [ "$CLUSTER_CHOICE" != "3" ]; then
    if ping -c 1 -W 2 mac-cluster-c >/dev/null 2>&1; then
        echo "  ✓ Cluster C (mac-cluster-c) reachable"
    else
        echo "  ✗ Cluster C (mac-cluster-c) NOT reachable"
    fi
fi

echo ""
echo "========================================="
echo "  Configuration Complete!"
echo "========================================="
echo ""
echo "Next steps:"
echo "  1. Copy this script to the other 2 devices"
echo "  2. Run it on each device to set up their configs"
echo "  3. Exchange SSH keys between all devices"
echo "  4. Run the appropriate start script:"
echo ""
echo "     Cluster A: bash start_cluster_a.sh"
echo "     Cluster B: bash start_cluster_b.sh"
echo "     Cluster C: bash start_cluster_c.sh"
echo ""
