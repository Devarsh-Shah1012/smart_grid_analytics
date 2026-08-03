#!/usr/bin/env python3
"""
kafka/producer.py
=================
Smart Meter Kafka Producer
--------------------------
Replays the London Smart Meters dataset as a live stream,
simulating real-time meter readings being published to a
Kafka topic.

Replay logic:
  - 1 simulated day is compressed into REPLAY_SPEED_SECONDS real seconds.
  - All readings from a simulated day are sent in one burst,
    then the producer sleeps until the next simulated day.
  - This makes 2 years of data (730 days) replay in ~2 hours
    at the default speed of 10 seconds/day.

Usage:
  python kafka/producer.py --speed 10 --date-from 2012-01-01

Message Format (JSON):
  {
    "meter_id":     "MAC000002",
    "ts":           "2012-01-01T01:00:00",
    "kwh_hourly":   0.234,
    "kwh_max":      0.145,
    "reading_date": "2012-01-01",
    "hour":         "01",
    "tariff_type":  "Std",
    "acorn_group":  "Adversity",
    "temp_high_c":  12.3,
    "temp_low_c":   5.1,
    "humidity":     0.82,
    "simulated":    true
  }
"""

import os
import csv
import json
import time
import logging
import argparse
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────
KAFKA_BROKER    = os.getenv("KAFKA_BROKER",      "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC_METERS","meter-readings")
CLEAN_DATA_PATH = os.getenv("CLEAN_DATA_PATH",   "data/sample_clean.csv")
REPLAY_SPEED    = int(os.getenv("KAFKA_REPLAY_SPEED_SECONDS", "10"))

# ── Argument Parser ───────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Smart Meter Kafka Producer")
    p.add_argument("--broker",     default=KAFKA_BROKER,    help="Kafka broker address")
    p.add_argument("--topic",      default=KAFKA_TOPIC,     help="Kafka topic name")
    p.add_argument("--data",       default=CLEAN_DATA_PATH, help="Path to clean CSV (Pig output)")
    p.add_argument("--speed",      type=int, default=REPLAY_SPEED,
                   help="Seconds per simulated day (default: 10)")
    p.add_argument("--date-from",  default=None,
                   help="Start date filter YYYY-MM-DD (optional)")
    p.add_argument("--date-to",    default=None,
                   help="End date filter YYYY-MM-DD (optional)")
    p.add_argument("--loop",       action="store_true",
                   help="Loop replay indefinitely (for demo mode)")
    p.add_argument("--inject-anomalies", action="store_true", default=True,
                   help="Randomly inject synthetic anomalies for demo")
    return p.parse_args()

# ── Kafka Producer Factory ────────────────────────────────────
def create_producer(broker: str) -> KafkaProducer:
    """Create and return a configured KafkaProducer."""
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",              # Wait for all replicas to acknowledge
        retries=5,
        batch_size=16384,        # 16 KB batch — good throughput/latency balance
        linger_ms=10,            # Wait 10ms to fill batches
        compression_type="lz4",  # Fast compression
        max_block_ms=60000,
    )
    log.info(f"Connected to Kafka broker: {broker}")
    return producer

# ── Anomaly Injection ─────────────────────────────────────────
def maybe_inject_anomaly(record: dict, inject: bool) -> dict:
    """
    With small probability, artificially inflate a reading
    to simulate energy theft, meter fault, or crypto mining.
    These synthetic anomalies are labelled for validation.
    """
    if not inject:
        return record
    
    r = random.random()
    
    if r < 0.003:
        # Sudden spike (3x–10x normal value) → possible fault / high-draw device
        multiplier = random.uniform(3.0, 10.0)
        record["kwh_hourly"] = round(record["kwh_hourly"] * multiplier, 4)
        record["injected_anomaly"] = "sudden_spike"
        log.debug(f"[INJECT] Spike on {record['meter_id']} → {record['kwh_hourly']} kWh")
    
    elif r < 0.006:
        # Flatline (near-zero) → meter failure or disconnection
        record["kwh_hourly"] = 0.001
        record["kwh_max"]    = 0.001
        record["injected_anomaly"] = "flatline"
    
    elif r < 0.008:
        # Sustained elevation (2x normal) → energy theft baseline shift
        record["kwh_hourly"] = round(record["kwh_hourly"] * 2.2, 4)
        record["injected_anomaly"] = "sustained_elevation"
    
    else:
        record["injected_anomaly"] = "none"
    
    return record

# ── CSV Row → Dict ────────────────────────────────────────────
def parse_row(row: list) -> dict | None:
    """
    Parse a CSV row from Pig ETL output.
    Expected columns (in order from Pig STORE):
      meter_id, reading_date, hour, tariff_type, acorn_group,
      kwh_hourly, kwh_max, temp_high_c, temp_low_c, humidity
    Returns None if parsing fails.
    """
    try:
        meter_id, reading_date, hour, tariff_type, acorn_group, \
            kwh_hourly, kwh_max, temp_high_c, temp_low_c, humidity = row[:10]
        
        ts_str = f"{reading_date}T{hour.zfill(2)}:00:00"
        
        return {
            "meter_id":     meter_id.strip(),
            "ts":           ts_str,
            "kwh_hourly":   float(kwh_hourly),
            "kwh_max":      float(kwh_max) if kwh_max else 0.0,
            "reading_date": reading_date.strip(),
            "hour":         hour.strip().zfill(2),
            "tariff_type":  tariff_type.strip(),
            "acorn_group":  acorn_group.strip(),
            "temp_high_c":  float(temp_high_c) if temp_high_c else 0.0,
            "temp_low_c":   float(temp_low_c)  if temp_low_c  else 0.0,
            "humidity":     float(humidity)    if humidity    else 0.0,
            "simulated":    True,
            "injected_anomaly": "none",
            "ingest_time":  datetime.utcnow().isoformat(),
        }
    except (ValueError, IndexError) as e:
        log.debug(f"Skipping malformed row: {row} — {e}")
        return None

# ── Delivery Callbacks ────────────────────────────────────────
def on_send_success(metadata):
    log.debug(f"  ✓ {metadata.topic}[{metadata.partition}] offset={metadata.offset}")

def on_send_error(excp):
    log.error(f"  ✗ Kafka send error: {excp}")

# ── Main Replay Loop ──────────────────────────────────────────
def replay(args):
    producer = create_producer(args.broker)
    
    total_sent        = 0
    total_errors      = 0
    total_anomalies   = 0
    run_count         = 0

    try:
        while True:
            run_count += 1
            log.info(f"=== Starting replay run #{run_count} ===")
            log.info(f"  Data file : {args.data}")
            log.info(f"  Speed     : {args.speed} seconds / simulated day")
            log.info(f"  Topic     : {args.topic}")
            
            current_day   = None
            day_batch     = []
            day_count     = 0

            with open(args.data, "r", newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                
                for row in reader:
                    if not row or len(row) < 10:
                        continue
                    
                    record = parse_row(row)
                    if record is None:
                        continue
                    
                    # ── Date filtering ────────────────────────
                    if args.date_from and record["reading_date"] < args.date_from:
                        continue
                    if args.date_to and record["reading_date"] > args.date_to:
                        continue
                    
                    # ── Day boundary detection ────────────────
                    if current_day is None:
                        current_day = record["reading_date"]
                    
                    if record["reading_date"] != current_day:
                        # Flush current day's batch
                        _flush_day(
                            producer, args.topic, day_batch, current_day,
                            day_count, args.speed, args.inject_anomalies
                        )
                        total_sent      += len(day_batch)
                        total_anomalies += sum(
                            1 for r in day_batch if r.get("injected_anomaly","none") != "none"
                        )
                        
                        day_batch   = []
                        current_day = record["reading_date"]
                        day_count  += 1
                    
                    # ── Inject anomaly & buffer ───────────────
                    record = maybe_inject_anomaly(record, args.inject_anomalies)
                    day_batch.append(record)
            
            # Flush last day
            if day_batch:
                _flush_day(
                    producer, args.topic, day_batch, current_day,
                    day_count, args.speed, args.inject_anomalies
                )
                total_sent += len(day_batch)
            
            log.info(f"=== Run #{run_count} complete ===")
            log.info(f"    Messages sent      : {total_sent:,}")
            log.info(f"    Injected anomalies : {total_anomalies:,}")
            
            if not args.loop:
                break
            
            log.info("Looping replay in 5 seconds...")
            time.sleep(5)
    
    except KeyboardInterrupt:
        log.info("Producer interrupted by user.")
    
    finally:
        producer.flush(timeout=30)
        producer.close()
        log.info(f"Producer shut down. Total messages sent: {total_sent:,}")


def _flush_day(producer, topic, batch, date_str, day_num, speed, inject):
    """Send all records for one simulated day, then sleep."""
    start = time.time()
    
    for record in batch:
        try:
            producer.send(
                topic,
                key=record["meter_id"],
                value=record
            ).add_callback(on_send_success).add_errback(on_send_error)
        except KafkaError as e:
            log.error(f"Failed to send record: {e}")
    
    producer.flush(timeout=10)
    elapsed = time.time() - start
    sleep_time = max(0.0, speed - elapsed)
    
    log.info(
        f"Day {day_num:4d} [{date_str}] — "
        f"{len(batch):6,} msgs sent in {elapsed:.2f}s — "
        f"sleeping {sleep_time:.2f}s"
    )
    
    if sleep_time > 0:
        time.sleep(sleep_time)


# ── Entry Point ───────────────────────────────────────────────
if __name__ == "__main__":
    args = parse_args()
    log.info("Smart Grid Kafka Producer starting...")
    log.info(f"  Broker  : {args.broker}")
    log.info(f"  Topic   : {args.topic}")
    log.info(f"  Data    : {args.data}")
    log.info(f"  Speed   : {args.speed}s / day")
    log.info(f"  Loop    : {args.loop}")
    log.info(f"  Anomaly : {args.inject_anomalies}")
    replay(args)
