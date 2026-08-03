# ============================================================
#  hbase/create_tables.rb
#  HBase Shell script — creates all tables required by the
#  Smart Grid platform.
#
#  Run: hbase shell hbase/create_tables.rb
# ============================================================

# ── Disable & drop existing tables (idempotent re-run) ───────
['meter_readings', 'anomaly_alerts'].each do |tbl|
  if exists(tbl)
    disable tbl
    drop    tbl
    puts "Dropped existing table: #{tbl}"
  end
end

# ============================================================
# TABLE 1: meter_readings
# Stores the most recent N readings per meter for fast streaming
# state warm-start and dashboard per-meter lookups.
#
# Row key design:
#   {meter_id}_{reverse_epoch_seconds}
#   e.g. MAC000002_99996754321
#   Reverse epoch → newest rows come first in region scan
#
# Column families:
#   cf:reading  — energy consumption data
#   cf:meta     — meter metadata (tariff, Acorn group)
# ============================================================
create 'meter_readings',
  {
    NAME => 'cf',
    VERSIONS => 1,
    COMPRESSION => 'SNAPPY',
    DATA_BLOCK_ENCODING => 'FAST_DIFF',
    BLOOMFILTER => 'ROW',
    TTL => 2592000       # 30 days in seconds — auto-expire old readings
  },
  SPLITS => (0..9).map { |i| "MAC#{i}" }   # Pre-split for better distribution

puts "Created table: meter_readings"

# ============================================================
# TABLE 2: anomaly_alerts
# Stores all detected anomaly events.
# Supports fast per-meter lookup and global recent-alerts scan.
#
# Row key design:
#   {meter_id}_{reverse_epoch}
#   → Scanning a meter's row prefix gives newest alerts first
#
# Column families:
#   cf  — all alert fields in one family for simplicity
# ============================================================
create 'anomaly_alerts',
  {
    NAME => 'cf',
    VERSIONS => 3,              # Keep 3 versions (update history)
    COMPRESSION => 'SNAPPY',
    DATA_BLOCK_ENCODING => 'FAST_DIFF',
    BLOOMFILTER => 'ROW',
    TTL => 7776000              # 90 days retention for alerts
  },
  SPLITS => (0..9).map { |i| "MAC#{i}" }

puts "Created table: anomaly_alerts"

# ============================================================
# TABLE 3: meter_state
# Latest rolling statistics per meter.
# Updated by Spark Streaming on every micro-batch.
# Provides warm-start state for Spark streaming job restarts.
#
# Row key: {meter_id}  (single row per meter, overwritten)
# ============================================================
create 'meter_state',
  {
    NAME => 'stats',
    VERSIONS => 1,
    COMPRESSION => 'SNAPPY',
    BLOOMFILTER => 'ROW',
    IN_MEMORY => 'true'         # Keep in block cache — small table, frequently read
  }

puts "Created table: meter_state"

# ── Verify tables ────────────────────────────────────────────
puts "\n=== Tables created ==="
list

puts "\n=== Table descriptions ==="
describe 'meter_readings'
describe 'anomaly_alerts'
describe 'meter_state'

puts "\n=== Setup complete ==="
