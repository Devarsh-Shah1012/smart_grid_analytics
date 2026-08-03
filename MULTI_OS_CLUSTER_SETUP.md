# Smart Grid Platform - Multi-OS Cluster Setup Guide
## Mac + 2× Windows (WSL2) | Java 8

---

## Table of Contents
1. [Environment Overview](#environment-overview)
2. [Prerequisites](#prerequisites)
3. [Network Configuration](#network-configuration)
4. [Java 8 Installation](#java-8-installation)
5. [Hadoop & Ecosystem Setup](#hadoop--ecosystem-setup)
6. [Cluster Assignment Strategy](#cluster-assignment-strategy)
7. [Step-by-Step Setup](#step-by-step-setup)
8. [Troubleshooting](#troubleshooting)

---

## Environment Overview

**Device Assignment:**
- **Mac**: Cluster C (Analytics) - Hive, Pig, Spark Batch, Superset
- **Windows 1 (WSL2)**: Cluster A (Streaming) - Kafka, Zookeeper, Spark Streaming
- **Windows 2 (WSL2)**: Cluster B (Storage) - HDFS, HBase, Thrift API

**Why this assignment?**
- Mac typically has better performance for development tools (Superset, Jupyter)
- Cluster B (HDFS/HBase) is most resource-intensive → dedicate one machine
- Kafka on Windows WSL2 works well with proper network config

---

## Prerequisites

### All Devices

1. **Disk Space**: At least 50 GB free per device
2. **RAM**: Minimum 8 GB (16 GB recommended)
3. **Network**: All devices on same network, able to ping each other
4. **User**: Same username on all devices (recommended for SSH simplicity)

### Mac-Specific

```bash
# Install Homebrew (if not already installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install essential tools
brew install wget
brew install coreutils
brew install openssh
```

### Windows WSL2-Specific

**Enable WSL2 on both Windows machines:**

```powershell
# Run in PowerShell as Administrator
wsl --install -d Ubuntu-22.04
wsl --set-default-version 2

# Verify WSL2 is running
wsl --list --verbose
```

**Inside each WSL2 Ubuntu:**

```bash
# Update package manager
sudo apt update && sudo apt upgrade -y

# Install essential tools
sudo apt install -y wget curl ssh openssh-server net-tools vim
sudo apt install -y build-essential python3 python3-pip

# Enable SSH server
sudo service ssh start
sudo systemctl enable ssh
```

---

## Network Configuration

### Step 1: Find IP Addresses

**On Mac:**
```bash
ifconfig | grep "inet " | grep -v 127.0.0.1
# Note the 192.168.x.x or 10.0.x.x address
```

**On Windows WSL2 (both machines):**
```bash
ip addr show eth0 | grep "inet " | awk '{print $2}' | cut -d/ -f1
# Note this IP - it's your WSL IP
```

**Important WSL2 Networking Note:**
- WSL2 uses a virtual network adapter
- IP addresses may change after reboot
- You'll need to use Windows host IP for external access
- Find Windows host IP: `ipconfig` in PowerShell (look for Ethernet/WiFi adapter)

### Step 2: Set Static IPs (Recommended)

**For WSL2 (do this on both Windows machines):**

Create `/etc/wsl.conf`:
```bash
sudo tee /etc/wsl.conf > /dev/null <<EOF
[network]
generateResolvConf = false
hostname = wsl-cluster-a  # or wsl-cluster-b

[boot]
systemd = true
EOF
```

**Restart WSL** (run in PowerShell):
```powershell
wsl --shutdown
wsl
```

### Step 3: Configure Hosts File

**On ALL devices** (Mac and both WSL2), edit `/etc/hosts`:

```bash
sudo nano /etc/hosts
```

Add these entries (replace with YOUR actual IPs):
```
# Smart Grid Cluster Hosts
192.168.1.100    mac-cluster-c      # Mac IP
192.168.1.101    wsl-cluster-a      # Windows 1 WSL IP
192.168.1.102    wsl-cluster-b      # Windows 2 WSL IP
```

**Test connectivity from each device:**
```bash
ping mac-cluster-c
ping wsl-cluster-a
ping wsl-cluster-b
```

---

## Java 8 Installation

### On Mac

```bash
# Install OpenJDK 8 via Homebrew
brew tap adoptopenjdk/openjdk
brew install --cask adoptopenjdk8

# Verify installation
java -version
# Should show: openjdk version "1.8.0_xxx"

# Set JAVA_HOME
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)' >> ~/.zshrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.zshrc
source ~/.zshrc

# Verify JAVA_HOME
echo $JAVA_HOME
```

### On Windows WSL2 (both machines)

```bash
# Install OpenJDK 8
sudo apt install -y openjdk-8-jdk

# Verify installation
java -version
javac -version

# Set JAVA_HOME in .bashrc
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc

# Verify
echo $JAVA_HOME
```

---

## Hadoop & Ecosystem Setup

### Download All Components (do this on ALL devices)

```bash
# Create installation directory
sudo mkdir -p /opt
sudo chown $USER:$USER /opt
cd /opt

# Hadoop 3.3.6
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
ln -s hadoop-3.3.6 hadoop

# Spark 3.4.1
wget https://downloads.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
tar -xzf spark-3.4.1-bin-hadoop3.tgz
ln -s spark-3.4.1-bin-hadoop3 spark

# Kafka 3.5.1 (only on Windows 1 - Cluster A)
wget https://downloads.apache.org/kafka/3.5.1/kafka_2.12-3.5.1.tgz
tar -xzf kafka_2.12-3.5.1.tgz
ln -s kafka_2.12-3.5.1 kafka

# HBase 2.5.5 (only on Windows 2 - Cluster B)
wget https://downloads.apache.org/hbase/2.5.5/hbase-2.5.5-bin.tar.gz
tar -xzf hbase-2.5.5-bin.tar.gz
ln -s hbase-2.5.5 hbase

# Hive 3.1.3 (only on Mac - Cluster C)
wget https://downloads.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
tar -xzf apache-hive-3.1.3-bin.tar.gz
ln -s apache-hive-3.1.3-bin hive

# Pig 0.17.0 (only on Mac - Cluster C)
wget https://downloads.apache.org/pig/pig-0.17.0/pig-0.17.0.tar.gz
tar -xzf pig-0.17.0.tar.gz
ln -s pig-0.17.0 pig

# Clean up tarballs
rm *.tar.gz *.tgz
```

---

## Cluster Assignment Strategy

```
┌─────────────────────────────────────────────────────────────┐
│                    CLUSTER TOPOLOGY                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  WINDOWS 1 (WSL2) - Cluster A - Streaming Layer            │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  │
│  • Kafka Broker (port 9092)                                │
│  • Zookeeper for Kafka (port 2181)                         │
│  • Spark Master (port 7077)                                │
│  • Spark Streaming Job (anomaly_detector.py)               │
│                                                             │
│  WINDOWS 2 (WSL2) - Cluster B - Storage Layer              │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  │
│  • HDFS NameNode (port 9000, 9870)                         │
│  • YARN ResourceManager (port 8088)                        │
│  • HBase Master (port 16000, 16010)                        │
│  • HBase Thrift Server (port 9090)                         │
│  • HBase Zookeeper (port 2181)                             │
│  • Flask API for Superset (port 5050)                      │
│                                                             │
│  MAC - Cluster C - Analytics Layer                         │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  │
│  • Spark Master (port 7077)                                │
│  • Hive Metastore (port 9083)                              │
│  • HiveServer2 (port 10000)                                │
│  • Pig (ETL jobs)                                          │
│  • Spark MLlib (training, forecasting)                     │
│  • Apache Superset (port 8088)                             │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Step-by-Step Setup

### STEP 1: Configure Environment Variables (ALL DEVICES)

Create `/opt/smart_grid_env.sh` on each device:

```bash
sudo tee /opt/smart_grid_env.sh > /dev/null <<'EOF'
#!/bin/bash
# Smart Grid Platform Environment Variables

# Java
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64  # WSL
# export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)  # Mac (uncomment)

# Hadoop
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop

# Spark
export SPARK_HOME=/opt/spark
export SPARK_CONF_DIR=$SPARK_HOME/conf

# HBase (only on Cluster B - Windows 2)
export HBASE_HOME=/opt/hbase
export HBASE_CONF_DIR=$HBASE_HOME/conf

# Kafka (only on Cluster A - Windows 1)
export KAFKA_HOME=/opt/kafka

# Hive (only on Cluster C - Mac)
export HIVE_HOME=/opt/hive
export HIVE_CONF_DIR=$HIVE_HOME/conf

# Pig (only on Cluster C - Mac)
export PIG_HOME=/opt/pig
export PIG_CLASSPATH=$HADOOP_CONF_DIR

# PATH
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export PATH=$HBASE_HOME/bin:$HIVE_HOME/bin:$PIG_HOME/bin:$KAFKA_HOME/bin:$PATH

# Cluster IPs (UPDATE THESE WITH YOUR ACTUAL IPs)
export CLUSTER_A_MASTER=wsl-cluster-a
export CLUSTER_B_MASTER=wsl-cluster-b
export CLUSTER_C_MASTER=mac-cluster-c
EOF

# Make executable
sudo chmod +x /opt/smart_grid_env.sh

# Source it in your shell profile
echo 'source /opt/smart_grid_env.sh' >> ~/.bashrc  # WSL
# echo 'source /opt/smart_grid_env.sh' >> ~/.zshrc  # Mac

source /opt/smart_grid_env.sh
```

### STEP 2: Configure Hadoop (ALL DEVICES)

**Create `$HADOOP_HOME/etc/hadoop/hadoop-env.sh`:**

```bash
cat >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh <<'EOF'

# Java path
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64  # WSL
# export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)  # Mac

# Hadoop heap size
export HADOOP_HEAPSIZE=2048
export HADOOP_NAMENODE_OPTS="-Xmx2g"
export HADOOP_DATANODE_OPTS="-Xmx1g"
EOF
```

**Configure `core-site.xml`:**

```bash
cat > $HADOOP_HOME/etc/hadoop/core-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://wsl-cluster-b:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/opt/hadoop_data/tmp</value>
  </property>
  <property>
    <name>hadoop.proxyuser.root.hosts</name>
    <value>*</value>
  </property>
  <property>
    <name>hadoop.proxyuser.root.groups</name>
    <value>*</value>
  </property>
</configuration>
EOF
```

**Configure `hdfs-site.xml`:**

```bash
cat > $HADOOP_HOME/etc/hadoop/hdfs-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
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
    <name>dfs.namenode.http-address</name>
    <value>wsl-cluster-b:9870</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
</configuration>
EOF
```

**Create Hadoop data directories:**

```bash
sudo mkdir -p /opt/hadoop_data/{tmp,namenode,datanode}
sudo chown -R $USER:$USER /opt/hadoop_data
chmod -R 755 /opt/hadoop_data
```

### STEP 3: SSH Setup for Password-less Access

**On ALL devices:**

```bash
# Generate SSH key (if you don't have one)
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa

# View your public key
cat ~/.ssh/id_rsa.pub
```

**Exchange keys between all machines:**

```bash
# On each device, add the public keys of OTHER devices to authorized_keys
# Example: From Mac, add Windows 1 & 2 keys:
cat >> ~/.ssh/authorized_keys <<EOF
# Windows 1 public key here
# Windows 2 public key here
EOF

chmod 600 ~/.ssh/authorized_keys
chmod 700 ~/.ssh

# Test SSH from Mac to both Windows machines:
ssh $USER@wsl-cluster-a
ssh $USER@wsl-cluster-b
```

### STEP 4: Device-Specific Configuration

---

#### **WINDOWS 1 (Cluster A - Streaming)**

**Kafka Configuration:**

```bash
# Edit server.properties
nano /opt/kafka/config/server.properties
```

Update these lines:
```properties
broker.id=0
listeners=PLAINTEXT://wsl-cluster-a:9092
advertised.listeners=PLAINTEXT://wsl-cluster-a:9092
log.dirs=/opt/kafka_data/logs
zookeeper.connect=wsl-cluster-a:2181
num.partitions=6
default.replication.factor=1
auto.create.topics.enable=false
```

**Create Kafka data directory:**
```bash
mkdir -p /opt/kafka_data/logs
```

**Spark Configuration:**

```bash
cat > /opt/spark/conf/spark-defaults.conf <<'EOF'
spark.master                     spark://wsl-cluster-a:7077
spark.executor.memory            3g
spark.driver.memory              2g
spark.sql.warehouse.dir          hdfs://wsl-cluster-b:9000/smart_grid/warehouse
spark.hadoop.hive.metastore.uris thrift://mac-cluster-c:9083
spark.serializer                 org.apache.spark.serializer.KryoSerializer
EOF

cat > /opt/spark/conf/spark-env.sh <<'EOF'
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_MASTER_HOST=wsl-cluster-a
export SPARK_WORKER_MEMORY=3g
export SPARK_WORKER_CORES=2
EOF
```

---

#### **WINDOWS 2 (Cluster B - Storage)**

**Format HDFS NameNode (FIRST TIME ONLY):**

```bash
hdfs namenode -format -force
```

**HBase Configuration:**

```bash
cat > /opt/hbase/conf/hbase-site.xml <<'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>hbase.rootdir</name>
    <value>hdfs://wsl-cluster-b:9000/hbase</value>
  </property>
  <property>
    <name>hbase.cluster.distributed</name>
    <value>false</value>
  </property>
  <property>
    <name>hbase.zookeeper.quorum</name>
    <value>wsl-cluster-b</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.dataDir</name>
    <value>/opt/hbase_data/zookeeper</value>
  </property>
  <property>
    <name>hbase.unsafe.stream.capability.enforce</name>
    <value>false</value>
  </property>
</configuration>
EOF

# Create HBase data directory
mkdir -p /opt/hbase_data/zookeeper
```

**Set HBase environment:**

```bash
cat >> /opt/hbase/conf/hbase-env.sh <<'EOF'
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HBASE_MANAGES_ZK=true
EOF
```

---

#### **MAC (Cluster C - Analytics)**

**Hive Configuration:**

```bash
cat > /opt/hive/conf/hive-site.xml <<'EOF'
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
    <value>hdfs://wsl-cluster-b:9000/smart_grid/warehouse</value>
  </property>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://mac-cluster-c:9083</value>
  </property>
  <property>
    <name>hive.server2.thrift.port</name>
    <value>10000</value>
  </property>
  <property>
    <name>hive.server2.thrift.bind.host</name>
    <value>mac-cluster-c</value>
  </property>
</configuration>
EOF

# Initialize Hive metastore schema
/opt/hive/bin/schematool -initSchema -dbType derby
```

**Spark Configuration:**

```bash
cat > /opt/spark/conf/spark-defaults.conf <<'EOF'
spark.master                     spark://mac-cluster-c:7077
spark.executor.memory            4g
spark.driver.memory              2g
spark.sql.warehouse.dir          hdfs://wsl-cluster-b:9000/smart_grid/warehouse
spark.hadoop.hive.metastore.uris thrift://mac-cluster-c:9083
EOF

cat > /opt/spark/conf/spark-env.sh <<'EOF'
export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_MASTER_HOST=mac-cluster-c
export SPARK_WORKER_MEMORY=4g
export SPARK_WORKER_CORES=4
EOF
```

### STEP 5: Install Python Dependencies (ALL DEVICES)

```bash
# Create virtual environment
python3 -m venv /opt/smart_grid_venv
source /opt/smart_grid_venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install \
  pyspark==3.4.1 \
  kafka-python==2.0.2 \
  happybase==1.2.0 \
  flask==2.3.3 \
  flask-cors==4.0.0 \
  pandas==2.0.3 \
  numpy==1.24.4 \
  python-dotenv==1.0.0

# Add to shell profile
echo 'source /opt/smart_grid_venv/bin/activate' >> ~/.bashrc  # WSL
# echo 'source /opt/smart_grid_venv/bin/activate' >> ~/.zshrc  # Mac
```

---

## Starting the Cluster

### Start Order (IMPORTANT!)

```bash
# 1. Windows 2 (Cluster B) - Start HDFS first
ssh wsl-cluster-b
start-dfs.sh
# Wait 30 seconds for NameNode to be ready

# 2. Windows 2 - Start HBase
start-hbase.sh
hbase thrift start -p 9090 &

# 3. Mac (Cluster C) - Start Hive Metastore
ssh mac-cluster-c
nohup hive --service metastore > /tmp/hive-metastore.log 2>&1 &
nohup hive --service hiveserver2 > /tmp/hiveserver2.log 2>&1 &

# 4. Windows 1 (Cluster A) - Start Kafka
ssh wsl-cluster-a
nohup /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties > /tmp/zookeeper.log 2>&1 &
sleep 5
nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties > /tmp/kafka.log 2>&1 &

# 5. Start Spark on both clusters
ssh wsl-cluster-a && /opt/spark/sbin/start-all.sh
ssh mac-cluster-c && /opt/spark/sbin/start-all.sh
```

### Verification Commands

```bash
# Check HDFS
hdfs dfsadmin -report

# Check HBase
echo "status" | hbase shell

# Check Kafka topics
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server wsl-cluster-a:9092

# Check Spark Masters
# Windows 1: http://wsl-cluster-a:8080
# Mac: http://mac-cluster-c:8080

# Check HDFS Web UI
# http://wsl-cluster-b:9870
```

---

## Troubleshooting

### WSL2 Port Forwarding (if you can't access web UIs from host Windows)

**In PowerShell (as Administrator):**

```powershell
# Forward HDFS Web UI
netsh interface portproxy add v4tov4 listenport=9870 listenaddress=0.0.0.0 connectport=9870 connectaddress=<WSL_IP>

# Forward HBase Web UI
netsh interface portproxy add v4tov4 listenport=16010 listenaddress=0.0.0.0 connectport=16010 connectaddress=<WSL_IP>

# Forward Spark Web UI
netsh interface portproxy add v4tov4 listenport=8080 listenaddress=0.0.0.0 connectport=8080 connectaddress=<WSL_IP>

# View all forwards
netsh interface portproxy show all
```

### Common Issues

**1. "Connection refused" when accessing HDFS from Mac/Windows 1:**
- Verify `/etc/hosts` has correct entries on ALL machines
- Check firewall isn't blocking ports
- Ensure HDFS is actually running: `jps` on Cluster B should show `NameNode`

**2. Kafka can't bind to port 9092 (WSL2):**
```bash
# Kill any existing Kafka processes
pkill -9 -f kafka
# Wait 10 seconds
sleep 10
# Start again
/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
```

**3. HBase won't start:**
```bash
# Check Java version
java -version  # Must be 1.8.x

# Check HDFS is accessible
hdfs dfs -ls /

# Check ZooKeeper data directory permissions
ls -la /opt/hbase_data/zookeeper
```

**4. WSL IP keeps changing:**
Create a script to update `/etc/hosts` automatically:

```bash
#!/bin/bash
# update_hosts.sh
MY_IP=$(ip addr show eth0 | grep "inet " | awk '{print $2}' | cut -d/ -f1)
sudo sed -i "s/^[0-9.]* wsl-cluster-a/$MY_IP wsl-cluster-a/" /etc/hosts
echo "Updated hosts file with IP: $MY_IP"
```

**5. Mac can't SSH to WSL:**
- Ensure SSH server is running on WSL: `sudo service ssh status`
- Start if needed: `sudo service ssh start`
- Check port 22 is open: `sudo ufw allow 22` (if using firewall)

---

## Next Steps

Once all services are running:

1. **Download datasets** (on Mac - Cluster C):
   ```bash
   bash /path/to/download_datasets.sh
   ```

2. **Create HBase tables** (on Windows 2 - Cluster B):
   ```bash
   hbase shell /path/to/create_tables.rb
   ```

3. **Create Kafka topics** (on Windows 1 - Cluster A):
   ```bash
   bash /path/to/kafka_topics.sh
   ```

4. **Load data to HDFS** (on Mac - Cluster C):
   ```bash
   spark-submit /path/to/hdfs_loader.py
   ```

5. **Start streaming pipeline**:
   - Producer (Windows 1): `python producer.py`
   - Streaming job (Windows 1): `spark-submit anomaly_detector.py`

---

## Performance Tips

- **WSL2 Memory**: Limit WSL memory in `.wslconfig` (Windows host):
  ```ini
  # C:\Users\<YourUser>\.wslconfig
  [wsl2]
  memory=8GB
  processors=4
  ```

- **Hadoop Heap**: If devices have <8GB RAM, reduce heap sizes in `hadoop-env.sh`

- **Spark Executor Memory**: Adjust based on available RAM minus 2GB for OS

---

## Quick Reference Card

```
CLUSTER A (Windows 1 WSL2)     CLUSTER B (Windows 2 WSL2)     CLUSTER C (Mac)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━   ━━━━━━━━━━━━━━━━━━━━━━
Start Services:               Start Services:               Start Services:
1. Kafka ZK                   1. HDFS (start-dfs.sh)        1. Spark (start-all.sh)
2. Kafka Broker               2. HBase (start-hbase.sh)     2. Hive Metastore
3. Spark (start-all.sh)       3. Thrift (hbase thrift)      3. HiveServer2

Web UIs:                      Web UIs:                      Web UIs:
Spark: :8080                  HDFS: :9870                   Spark: :8080
                              HBase: :16010                 Hive: :10002

Log Locations:                Log Locations:                Log Locations:
/opt/kafka/logs/              /opt/hadoop/logs/             /opt/spark/logs/
/opt/spark/logs/              /opt/hbase/logs/              /opt/hive/logs/
```

---

**You're now ready to run the Smart Grid Analytics Platform across your 3-device cluster!** 🚀
