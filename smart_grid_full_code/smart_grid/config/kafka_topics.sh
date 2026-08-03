#!/usr/bin/env bash
# ============================================================
#  config/kafka_topics.sh
#  Creates all required Kafka topics.
#  Run on Cluster A master node.
# ============================================================

set -euo pipefail
source "$(dirname "$0")/cluster.env"

KAFKA_BIN=${KAFKA_HOME:-/opt/kafka}/bin

echo "==> Creating Kafka topics on broker: ${KAFKA_BROKER}"

# ── meter-readings topic ─────────────────────────────────────
# 6 partitions → allows 6 parallel Spark streaming tasks
# Replication factor 2 → fault tolerance across 2 brokers
${KAFKA_BIN}/kafka-topics.sh \
  --create \
  --if-not-exists \
  --bootstrap-server "${KAFKA_BROKER}" \
  --topic "${KAFKA_TOPIC_METERS}" \
  --partitions 6 \
  --replication-factor 2 \
  --config retention.ms=86400000 \
  --config compression.type=lz4

echo "  [OK] Topic '${KAFKA_TOPIC_METERS}' created (6 partitions, RF=2)"

# ── anomaly-alerts topic ─────────────────────────────────────
# 3 partitions → lower throughput, alerts are a subset of readings
${KAFKA_BIN}/kafka-topics.sh \
  --create \
  --if-not-exists \
  --bootstrap-server "${KAFKA_BROKER}" \
  --topic "${KAFKA_TOPIC_ALERTS}" \
  --partitions 3 \
  --replication-factor 2 \
  --config retention.ms=604800000

echo "  [OK] Topic '${KAFKA_TOPIC_ALERTS}' created (3 partitions, RF=2)"

# ── List all topics ──────────────────────────────────────────
echo ""
echo "==> All topics on broker:"
${KAFKA_BIN}/kafka-topics.sh \
  --list \
  --bootstrap-server "${KAFKA_BROKER}"

echo ""
echo "==> Topic details:"
${KAFKA_BIN}/kafka-topics.sh \
  --describe \
  --bootstrap-server "${KAFKA_BROKER}" \
  --topic "${KAFKA_TOPIC_METERS}"
