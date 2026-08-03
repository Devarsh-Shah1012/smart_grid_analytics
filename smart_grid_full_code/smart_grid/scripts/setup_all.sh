#!/usr/bin/env bash
# ============================================================
#  scripts/setup_all.sh
#  One-shot setup script for the Smart Grid platform.
#  Run on each cluster's master node as appropriate.
#
#  Usage:
#    bash scripts/setup_all.sh [cluster_a|cluster_b|cluster_c|all]
#
#  Prerequisites:
#    - Java 8 or 11 installed on all nodes
#    - SSH passwordless access between all nodes
#    - cluster.env edited with correct IPs
# ============================================================

set -euo pipefail
source "$(dirname "$0")/../config/cluster.env"

TARGET=${1:-all}
INSTALL_DIR=/opt
HADOOP_VERSION=3.3.6
SPARK_VERSION=3.4.1
KAFKA_VERSION=3.5.1
HBASE_VERSION=2.5.5
HIVE_VERSION=3.1.3
PIG_VERSION=0.17.0
SCALA_VERSION=2.12

log() { echo "[$(date '+%H:%M:%S')] $*"; }
ok()  { echo "[$(date '+%H:%M:%S')] ✅ $*"; }
err() { echo "[$(date '+%H:%M:%S')] ❌ $*"; exit 1; }

# ── Java check ────────────────────────────────────────────────
check_java() {
    if ! command -v java &>/dev/null; then
        err "Java not found. Install Java 11: sudo apt install openjdk-11-jdk"
    fi
    JAVA_VER=$(java -version 2>&1 | head -1)
    ok "Java: ${JAVA_VER}"
}

# ── Download helper ───────────────────────────────────────────
download_if_missing() {
    local url=$1 dest=$2
    if [[ ! -f "$dest" ]]; then
        log "Downloading: $(basename $dest)"
        wget -q --show-progress -O "$dest" "$url" || err "Download failed: $url"
    else
        log "Already exists: $(basename $dest)"
    fi
}

# ============================================================
# HADOOP INSTALL
# ============================================================
install_hadoop() {
    log "=== Installing Apache Hadoop ${HADOOP_VERSION} ==="
    local TGZ=/tmp/hadoop.tar.gz
    download_if_missing \
        "https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
        "$TGZ"

    tar -xzf "$TGZ" -C "$INSTALL_DIR"
    ln -sfn "${INSTALL_DIR}/hadoop-${HADOOP_VERSION}" "${INSTALL_DIR}/hadoop"

    # Environment
    cat >> /etc/environment <<EOF
HADOOP_HOME=${INSTALL_DIR}/hadoop
PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin
JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
EOF
    export HADOOP_HOME="${INSTALL_DIR}/hadoop"

    # Configure core-site.xml
    cat > "${HADOOP_HOME}/etc/hadoop/core-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${CLUSTER_B_MASTER}:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/opt/hadoop_data/tmp</value>
  </property>
</configuration>
EOF

    # Configure hdfs-site.xml
    cat > "${HADOOP_HOME}/etc/hadoop/hdfs-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/opt/hadoop_data/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/opt/hadoop_data/datanode</value>
  </property>
  <property>
    <name>dfs.blocksize</name>
    <value>134217728</value>
  </property>
  <property>
    <name>dfs.namenode.http-address</name>
    <value>${CLUSTER_B_MASTER}:9870</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
</configuration>
EOF

    # Configure yarn-site.xml
    cat > "${HADOOP_HOME}/etc/hadoop/yarn-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>${CLUSTER_B_MASTER}</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>8192</value>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>8192</value>
  </property>
  <property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
  </property>
</configuration>
EOF

    # Workers file
    cat > "${HADOOP_HOME}/etc/hadoop/workers" <<EOF
${CLUSTER_B_WORKER1}
${CLUSTER_B_WORKER2}
EOF

    # Create data directories
    mkdir -p /opt/hadoop_data/{tmp,namenode,datanode}
    chmod -R 755 /opt/hadoop_data

    ok "Hadoop ${HADOOP_VERSION} installed"
}

# ============================================================
# SPARK INSTALL
# ============================================================
install_spark() {
    log "=== Installing Apache Spark ${SPARK_VERSION} ==="
    local TGZ=/tmp/spark.tar.gz
    download_if_missing \
        "https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
        "$TGZ"

    tar -xzf "$TGZ" -C "$INSTALL_DIR"
    ln -sfn "${INSTALL_DIR}/spark-${SPARK_VERSION}-bin-hadoop3" "${INSTALL_DIR}/spark"

    export SPARK_HOME="${INSTALL_DIR}/spark"

    # spark-defaults.conf
    cat > "${SPARK_HOME}/conf/spark-defaults.conf" <<EOF
spark.master                     spark://${CLUSTER_A_MASTER}:7077
spark.executor.memory            ${SPARK_EXECUTOR_MEMORY}
spark.driver.memory              ${SPARK_DRIVER_MEMORY}
spark.sql.adaptive.enabled       true
spark.sql.warehouse.dir          hdfs://${CLUSTER_B_MASTER}:9000/smart_grid/warehouse
spark.hadoop.hive.metastore.uris ${HIVE_METASTORE}
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.sql.parquet.compression.codec snappy
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://${CLUSTER_B_MASTER}:9000/spark-logs
EOF

    # spark-env.sh
    cat > "${SPARK_HOME}/conf/spark-env.sh" <<EOF
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
export HADOOP_CONF_DIR=${INSTALL_DIR}/hadoop/etc/hadoop
export SPARK_MASTER_HOST=${CLUSTER_A_MASTER}
export SPARK_WORKER_MEMORY=${SPARK_EXECUTOR_MEMORY}
export SPARK_WORKER_CORES=4
EOF

    # Workers
    cat > "${SPARK_HOME}/conf/workers" <<EOF
${CLUSTER_A_WORKER1}
${CLUSTER_A_WORKER2}
EOF

    ok "Spark ${SPARK_VERSION} installed"
}

# ============================================================
# KAFKA INSTALL
# ============================================================
install_kafka() {
    log "=== Installing Apache Kafka ${KAFKA_VERSION} ==="
    local TGZ=/tmp/kafka.tar.gz
    download_if_missing \
        "https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
        "$TGZ"

    tar -xzf "$TGZ" -C "$INSTALL_DIR"
    ln -sfn "${INSTALL_DIR}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" "${INSTALL_DIR}/kafka"

    export KAFKA_HOME="${INSTALL_DIR}/kafka"

    # server.properties
    cat > "${KAFKA_HOME}/config/server.properties" <<EOF
broker.id=0
listeners=PLAINTEXT://${CLUSTER_A_MASTER}:9092
advertised.listeners=PLAINTEXT://${CLUSTER_A_MASTER}:9092
log.dirs=/opt/kafka_data/logs
num.partitions=6
default.replication.factor=2
log.retention.hours=24
log.segment.bytes=1073741824
zookeeper.connect=${CLUSTER_A_MASTER}:2181
auto.create.topics.enable=false
EOF

    mkdir -p /opt/kafka_data/logs
    ok "Kafka ${KAFKA_VERSION} installed"
}

# ============================================================
# HBASE INSTALL
# ============================================================
install_hbase() {
    log "=== Installing Apache HBase ${HBASE_VERSION} ==="
    local TGZ=/tmp/hbase.tar.gz
    download_if_missing \
        "https://downloads.apache.org/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz" \
        "$TGZ"

    tar -xzf "$TGZ" -C "$INSTALL_DIR"
    ln -sfn "${INSTALL_DIR}/hbase-${HBASE_VERSION}" "${INSTALL_DIR}/hbase"

    export HBASE_HOME="${INSTALL_DIR}/hbase"

    # hbase-site.xml
    cat > "${HBASE_HOME}/conf/hbase-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>hbase.rootdir</name>
    <value>hdfs://${CLUSTER_B_MASTER}:9000/hbase</value>
  </property>
  <property>
    <name>hbase.cluster.distributed</name>
    <value>true</value>
  </property>
  <property>
    <name>hbase.zookeeper.quorum</name>
    <value>${CLUSTER_B_MASTER}</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.dataDir</name>
    <value>/opt/zookeeper_data/hbase</value>
  </property>
  <property>
    <name>hbase.master</name>
    <value>${CLUSTER_B_MASTER}:16000</value>
  </property>
  <property>
    <name>hbase.regionserver.wal.codec</name>
    <value>org.apache.hadoop.hbase.regionserver.wal.WALCellCodec</value>
  </property>
  <property>
    <name>hbase.master.ui.readonly</name>
    <value>false</value>
  </property>
</configuration>
EOF

    # regionservers
    cat > "${HBASE_HOME}/conf/regionservers" <<EOF
${CLUSTER_B_WORKER1}
${CLUSTER_B_WORKER2}
EOF

    mkdir -p /opt/zookeeper_data/hbase
    ok "HBase ${HBASE_VERSION} installed"
}

# ============================================================
# HIVE INSTALL
# ============================================================
install_hive() {
    log "=== Installing Apache Hive ${HIVE_VERSION} ==="
    local TGZ=/tmp/hive.tar.gz
    download_if_missing \
        "https://downloads.apache.org/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz" \
        "$TGZ"

    tar -xzf "$TGZ" -C "$INSTALL_DIR"
    ln -sfn "${INSTALL_DIR}/apache-hive-${HIVE_VERSION}-bin" "${INSTALL_DIR}/hive"

    export HIVE_HOME="${INSTALL_DIR}/hive"

    # hive-site.xml
    cat > "${HIVE_HOME}/conf/hive-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=/opt/hive_metastore;create=true</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.apache.derby.jdbc.EmbeddedDriver</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>hdfs://${CLUSTER_B_MASTER}:9000/smart_grid/warehouse</value>
  </property>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://${CLUSTER_C_MASTER}:9083</value>
  </property>
  <property>
    <name>hive.server2.thrift.port</name>
    <value>10000</value>
  </property>
  <property>
    <name>hive.server2.thrift.bind.host</name>
    <value>${CLUSTER_C_MASTER}</value>
  </property>
  <property>
    <name>hive.exec.dynamic.partition</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.exec.dynamic.partition.mode</name>
    <value>nonstrict</value>
  </property>
  <property>
    <name>hive.stats.autogather</name>
    <value>true</value>
  </property>
</configuration>
EOF

    # Initialise Hive metastore schema
    "${HIVE_HOME}/bin/schematool" -initSchema -dbType derby || true

    ok "Hive ${HIVE_VERSION} installed"
}

# ============================================================
# PIG INSTALL
# ============================================================
install_pig() {
    log "=== Installing Apache Pig ${PIG_VERSION} ==="
    local TGZ=/tmp/pig.tar.gz
    download_if_missing \
        "https://downloads.apache.org/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz" \
        "$TGZ"

    tar -xzf "$TGZ" -C "$INSTALL_DIR"
    ln -sfn "${INSTALL_DIR}/pig-${PIG_VERSION}" "${INSTALL_DIR}/pig"

    export PIG_HOME="${INSTALL_DIR}/pig"

    cat >> /etc/environment <<EOF
PIG_HOME=${INSTALL_DIR}/pig
PIG_CLASSPATH=${INSTALL_DIR}/hadoop/etc/hadoop
PATH=\$PATH:\$PIG_HOME/bin
EOF

    ok "Pig ${PIG_VERSION} installed"
}

# ============================================================
# HDFS INIT
# ============================================================
init_hdfs() {
    log "=== Initialising HDFS ==="
    export HADOOP_HOME="${INSTALL_DIR}/hadoop"

    # Format namenode (first time only)
    if [[ ! -d /opt/hadoop_data/namenode/current ]]; then
        "${HADOOP_HOME}/bin/hdfs" namenode -format -nonInteractive
        ok "HDFS NameNode formatted"
    else
        log "NameNode already formatted — skipping"
    fi

    # Start HDFS
    "${HADOOP_HOME}/sbin/start-dfs.sh"
    sleep 5

    # Create HDFS directory structure
    "${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p \
        /smart_grid/raw/london \
        /smart_grid/raw/pjm \
        /smart_grid/raw/uci \
        /smart_grid/clean/meter_readings \
        /smart_grid/clean/household_stats \
        /smart_grid/parquet/meter_readings \
        /smart_grid/parquet/household_stats \
        /smart_grid/parquet/pjm_demand \
        /smart_grid/streaming/meter_readings \
        /smart_grid/models \
        /smart_grid/forecasts \
        /smart_grid/reports \
        /smart_grid/warehouse \
        /smart_grid/checkpoints \
        /spark-logs \
        /hbase

    "${HADOOP_HOME}/bin/hdfs" dfs -chmod -R 777 /smart_grid
    ok "HDFS directory structure created"
}

# ============================================================
# Python dependencies
# ============================================================
install_python_deps() {
    log "=== Installing Python dependencies ==="
    pip3 install -q \
        pyspark==3.4.1 \
        kafka-python==2.0.2 \
        happybase==1.2.0 \
        flask==2.3.3 \
        flask-cors==4.0.0 \
        pandas==2.0.3 \
        numpy==1.24.4 \
        python-dotenv==1.0.0
    ok "Python dependencies installed"
}

# ============================================================
# MAIN
# ============================================================
check_java

case "$TARGET" in
    cluster_a)
        log "Setting up Cluster A (Streaming)"
        install_hadoop
        install_spark
        install_kafka
        install_python_deps
        ;;
    cluster_b)
        log "Setting up Cluster B (Storage)"
        install_hadoop
        install_hbase
        init_hdfs
        ;;
    cluster_c)
        log "Setting up Cluster C (Analytics)"
        install_hadoop
        install_spark
        install_hive
        install_pig
        install_python_deps
        ;;
    all)
        log "Setting up ALL components (single-node dev mode)"
        install_hadoop
        install_spark
        install_kafka
        install_hbase
        install_hive
        install_pig
        install_python_deps
        init_hdfs
        ;;
    *)
        err "Unknown target: $TARGET. Use cluster_a|cluster_b|cluster_c|all"
        ;;
esac

ok "=== Setup complete for target: ${TARGET} ==="
echo ""
echo "Next steps:"
echo "  1. bash scripts/start_pipeline.sh"
echo "  2. bash data/download_datasets.sh"
echo "  3. pig -f pig/etl_clean.pig"
echo "  4. spark-submit spark/batch/hdfs_loader.py"
echo "  5. hive -f hive/create_tables.hql"
echo "  6. hbase shell hbase/create_tables.rb"
echo "  7. spark-submit spark/batch/train_forecast_model.py"
echo "  8. python kafka/producer.py &"
echo "  9. spark-submit spark/streaming/anomaly_detector.py"
