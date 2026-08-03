#!/usr/bin/env python3
"""
hbase/hbase_utils.py
=====================
Python HBase client helper module using happybase (Thrift API).

Provides:
  - HBaseClient class with context manager
  - Functions to read/write meter readings and anomaly alerts
  - Utility functions for the dashboard (Superset HTTP API calls
    can use these via a Flask microservice if needed)

Install: pip install happybase
"""

import json
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
import happybase

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────
HBASE_HOST            = "localhost"
HBASE_PORT            = 9090        # Thrift port
TABLE_METER_READINGS  = "meter_readings"
TABLE_ANOMALY_ALERTS  = "anomaly_alerts"
TABLE_METER_STATE     = "meter_state"

# Reverse epoch base for descending row key ordering
REVERSE_EPOCH_BASE    = 99_999_999_999


def reverse_epoch(ts: datetime) -> str:
    """Convert timestamp to reverse-epoch string for descending row ordering."""
    return str(REVERSE_EPOCH_BASE - int(ts.timestamp()))


def make_row_key(meter_id: str, ts: datetime) -> bytes:
    """Build a HBase row key: {meter_id}_{reverse_epoch}"""
    return f"{meter_id}_{reverse_epoch(ts)}".encode()


# ── HBase Client ─────────────────────────────────────────────
class HBaseClient:
    """
    Context manager wrapper around happybase.Connection.
    
    Usage:
        with HBaseClient() as client:
            alerts = client.get_recent_alerts("MAC000002", limit=10)
    """
    
    def __init__(self, host: str = HBASE_HOST, port: int = HBASE_PORT):
        self.host = host
        self.port = port
        self.conn: Optional[happybase.Connection] = None
    
    def __enter__(self) -> "HBaseClient":
        self.conn = happybase.Connection(self.host, port=self.port)
        self.conn.open()
        return self
    
    def __exit__(self, *args):
        if self.conn:
            self.conn.close()
    
    def _table(self, name: str) -> happybase.Table:
        return self.conn.table(name)
    
    # ── Write Operations ──────────────────────────────────────
    
    def put_alert(self, alert: Dict[str, Any]) -> None:
        """Write a single anomaly alert to HBase."""
        ts = datetime.fromisoformat(alert["ts"])
        row_key = make_row_key(alert["meter_id"], ts)
        
        table = self._table(TABLE_ANOMALY_ALERTS)
        table.put(row_key, {
            b"cf:meter_id":      str(alert.get("meter_id",    "")).encode(),
            b"cf:ts":            str(alert.get("ts",          "")).encode(),
            b"cf:kwh_hourly":    str(alert.get("kwh_hourly",  0)).encode(),
            b"cf:z_score":       str(round(alert.get("z_score", 0), 3)).encode(),
            b"cf:anomaly_type":  str(alert.get("anomaly_type","")).encode(),
            b"cf:severity":      str(round(alert.get("severity", 0), 2)).encode(),
            b"cf:acorn_group":   str(alert.get("acorn_group", "")).encode(),
            b"cf:tariff_type":   str(alert.get("tariff_type","")).encode(),
            b"cf:status":        b"open",
            b"cf:created_at":    datetime.utcnow().isoformat().encode(),
        })
    
    def put_alerts_batch(self, alerts: List[Dict[str, Any]]) -> int:
        """Write a batch of anomaly alerts. Returns number written."""
        table = self._table(TABLE_ANOMALY_ALERTS)
        written = 0
        
        with table.batch(batch_size=500) as b:
            for alert in alerts:
                try:
                    ts = datetime.fromisoformat(alert["ts"])
                    row_key = make_row_key(alert["meter_id"], ts)
                    b.put(row_key, {
                        b"cf:meter_id":      str(alert.get("meter_id",    "")).encode(),
                        b"cf:ts":            str(alert.get("ts",          "")).encode(),
                        b"cf:kwh_hourly":    str(alert.get("kwh_hourly",  0)).encode(),
                        b"cf:z_score":       str(round(float(alert.get("z_score", 0)), 3)).encode(),
                        b"cf:anomaly_type":  str(alert.get("anomaly_type","")).encode(),
                        b"cf:severity":      str(round(float(alert.get("severity", 0)), 2)).encode(),
                        b"cf:acorn_group":   str(alert.get("acorn_group", "")).encode(),
                        b"cf:status":        b"open",
                        b"cf:created_at":    datetime.utcnow().isoformat().encode(),
                    })
                    written += 1
                except Exception as e:
                    log.error(f"Failed to write alert {alert.get('meter_id')}: {e}")
        
        return written
    
    def update_meter_state(self, meter_id: str, stats: Dict[str, float]) -> None:
        """Update rolling stats for a meter in meter_state table."""
        table = self._table(TABLE_METER_STATE)
        table.put(meter_id.encode(), {
            b"stats:count":            str(stats.get("count",          0)).encode(),
            b"stats:rolling_mean":     str(round(stats.get("rolling_mean",   0), 5)).encode(),
            b"stats:rolling_stddev":   str(round(stats.get("rolling_stddev", 0), 5)).encode(),
            b"stats:consecutive_high": str(stats.get("consecutive_high", 0)).encode(),
            b"stats:last_updated":     datetime.utcnow().isoformat().encode(),
        })
    
    # ── Read Operations ───────────────────────────────────────
    
    def get_recent_alerts(
        self,
        meter_id: str,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Get the N most recent alerts for a specific meter.
        Because row key = meter_id + reverse_epoch, scanning the
        meter's prefix returns newest alerts first.
        """
        table  = self._table(TABLE_ANOMALY_ALERTS)
        prefix = meter_id.encode()
        results = []
        
        for row_key, data in table.scan(row_prefix=prefix, limit=limit):
            results.append(_decode_row(row_key, data))
        
        return results
    
    def get_all_open_alerts(self, limit: int = 200) -> List[Dict[str, Any]]:
        """
        Full table scan for open anomaly alerts.
        Used by dashboard 'Live Alert Feed' panel.
        Filtered server-side by status column.
        """
        table   = self._table(TABLE_ANOMALY_ALERTS)
        results = []
        
        filter_str = "SingleColumnValueFilter('cf', 'status', =, 'binary:open')"
        
        for row_key, data in table.scan(
            filter=filter_str,
            limit=limit,
            columns=[b"cf"]
        ):
            row = _decode_row(row_key, data)
            results.append(row)
        
        return results
    
    def get_meter_state(self, meter_id: str) -> Optional[Dict[str, Any]]:
        """Get current rolling stats for a specific meter."""
        table = self._table(TABLE_METER_STATE)
        data  = table.row(meter_id.encode())
        
        if not data:
            return None
        
        return {
            "meter_id":          meter_id,
            "count":             int(_d(data, "stats:count", 0)),
            "rolling_mean":      float(_d(data, "stats:rolling_mean", 0)),
            "rolling_stddev":    float(_d(data, "stats:rolling_stddev", 0)),
            "consecutive_high":  int(_d(data, "stats:consecutive_high", 0)),
            "last_updated":      _d(data, "stats:last_updated", ""),
        }
    
    def resolve_alert(self, meter_id: str, ts: datetime) -> bool:
        """Mark an alert as resolved (e.g., after operator review)."""
        row_key = make_row_key(meter_id, ts)
        table   = self._table(TABLE_ANOMALY_ALERTS)
        
        try:
            table.put(row_key, {
                b"cf:status":      b"resolved",
                b"cf:resolved_at": datetime.utcnow().isoformat().encode(),
            })
            return True
        except Exception as e:
            log.error(f"Failed to resolve alert {meter_id} @ {ts}: {e}")
            return False
    
    def count_alerts_by_type(self) -> Dict[str, int]:
        """
        Scan anomaly_alerts table and count by anomaly_type.
        Returns dict: {anomaly_type: count}
        Used by dashboard pie chart.
        NOTE: Full scan — use sparingly; cache the result.
        """
        table   = self._table(TABLE_ANOMALY_ALERTS)
        counts  = {}
        
        for _, data in table.scan(columns=[b"cf:anomaly_type"]):
            atype = data.get(b"cf:anomaly_type", b"unknown").decode()
            counts[atype] = counts.get(atype, 0) + 1
        
        return counts
    
    def get_top_meters_by_alerts(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Return top N meters by alert count.
        Scans full table and aggregates in Python.
        For large datasets, use Hive view v_top_anomaly_meters instead.
        """
        table  = self._table(TABLE_ANOMALY_ALERTS)
        counts: Dict[str, int] = {}
        
        for row_key, _ in table.scan(columns=[]):
            meter_id = row_key.decode().split("_")[0]
            counts[meter_id] = counts.get(meter_id, 0) + 1
        
        sorted_meters = sorted(counts.items(), key=lambda x: -x[1])[:limit]
        return [{"meter_id": m, "alert_count": c} for m, c in sorted_meters]


# ── Helpers ───────────────────────────────────────────────────
def _decode_row(row_key: bytes, data: dict) -> Dict[str, Any]:
    """Convert raw HBase row to clean Python dict."""
    return {
        "row_key":     row_key.decode(errors="replace"),
        "meter_id":    _d(data, "cf:meter_id",     ""),
        "ts":          _d(data, "cf:ts",            ""),
        "kwh_hourly":  float(_d(data, "cf:kwh_hourly",  0)),
        "z_score":     float(_d(data, "cf:z_score",     0)),
        "anomaly_type":_d(data, "cf:anomaly_type",  ""),
        "severity":    float(_d(data, "cf:severity",    0)),
        "acorn_group": _d(data, "cf:acorn_group",   ""),
        "tariff_type": _d(data, "cf:tariff_type",   ""),
        "status":      _d(data, "cf:status",        ""),
        "created_at":  _d(data, "cf:created_at",    ""),
    }


def _d(data: dict, key: str, default) -> Any:
    """Safely decode a byte-key column from HBase row dict."""
    raw = data.get(key.encode() if isinstance(key, str) else key)
    if raw is None:
        return default
    try:
        return raw.decode("utf-8")
    except Exception:
        return default


# ── CLI Demo ──────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO,
        format="%(asctime)s [HBase] %(levelname)s — %(message)s")
    
    command = sys.argv[1] if len(sys.argv) > 1 else "alerts"
    
    with HBaseClient() as client:
        
        if command == "alerts":
            meter = sys.argv[2] if len(sys.argv) > 2 else None
            if meter:
                results = client.get_recent_alerts(meter, limit=10)
                log.info(f"Recent alerts for {meter}:")
            else:
                results = client.get_all_open_alerts(limit=20)
                log.info("All open alerts:")
            
            for r in results:
                log.info(f"  {r['ts']:25s} {r['meter_id']:12s} "
                         f"{r['anomaly_type']:25s} severity={r['severity']:.2f} "
                         f"z={r['z_score']:.2f}")
        
        elif command == "state":
            meter = sys.argv[2] if len(sys.argv) > 2 else "MAC000002"
            state = client.get_meter_state(meter)
            if state:
                log.info(f"Meter state for {meter}: {json.dumps(state, indent=2)}")
            else:
                log.warning(f"No state found for {meter}")
        
        elif command == "counts":
            counts = client.count_alerts_by_type()
            log.info("Alert counts by type:")
            for k, v in sorted(counts.items(), key=lambda x: -x[1]):
                log.info(f"  {k:30s} : {v:,}")
        
        elif command == "top":
            top = client.get_top_meters_by_alerts(limit=10)
            log.info("Top meters by alert count:")
            for i, row in enumerate(top, 1):
                log.info(f"  {i:2d}. {row['meter_id']:15s} {row['alert_count']:,} alerts")
        
        else:
            print("Usage: python hbase_utils.py [alerts|state|counts|top] [meter_id]")
