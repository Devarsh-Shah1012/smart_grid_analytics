#!/usr/bin/env python3
"""
dashboard/hbase_api.py
=======================
Lightweight Flask REST API that exposes HBase data to Superset.
Superset cannot query HBase directly; this bridge API converts
HBase row scans into JSON that Superset's REST datasource can read.

Run on Cluster B:
  pip install flask flask-cors happybase
  python dashboard/hbase_api.py

Endpoints:
  GET /api/alerts/recent?limit=50          — Most recent open alerts
  GET /api/alerts/meter/<meter_id>?limit=20 — Alerts for one meter
  GET /api/alerts/counts                   — Count by anomaly type
  GET /api/meters/top?limit=20             — Top meters by alert count
  GET /api/meters/state/<meter_id>         — Current rolling stats
  GET /health                              — Health check
"""

import os
import logging
from flask import Flask, jsonify, request, abort
from flask_cors import CORS
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from hbase.hbase_utils import HBaseClient

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [HBase API] %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Allow Superset (different host) to call this API

HBASE_HOST = os.getenv("HBASE_MASTER", "localhost")
DEFAULT_LIMIT = 50
MAX_LIMIT = 500


def _limit_param():
    try:
        v = int(request.args.get("limit", DEFAULT_LIMIT))
        return min(v, MAX_LIMIT)
    except ValueError:
        return DEFAULT_LIMIT


@app.route("/health")
def health():
    """Health check — also tests HBase connectivity."""
    try:
        with HBaseClient(HBASE_HOST) as client:
            # Try to list tables as connectivity test
            client.conn.tables()
        return jsonify({"status": "ok", "hbase": "connected"})
    except Exception as e:
        log.error(f"Health check failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 503


@app.route("/api/alerts/recent")
def recent_alerts():
    """
    Returns the most recent anomaly alerts across all meters.
    Superset Live Alert Feed panel calls this endpoint.
    """
    limit = _limit_param()
    
    try:
        with HBaseClient(HBASE_HOST) as client:
            alerts = client.get_all_open_alerts(limit=limit)
        
        return jsonify({
            "status":  "ok",
            "count":   len(alerts),
            "results": alerts,
        })
    except Exception as e:
        log.error(f"/api/alerts/recent error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/alerts/meter/<meter_id>")
def meter_alerts(meter_id: str):
    """Returns recent alerts for a specific meter. Used for drill-down."""
    limit = _limit_param()
    
    # Basic input validation
    if not meter_id.replace("-", "").isalnum():
        abort(400, "Invalid meter_id format")
    
    try:
        with HBaseClient(HBASE_HOST) as client:
            alerts = client.get_recent_alerts(meter_id, limit=limit)
        
        return jsonify({
            "status":   "ok",
            "meter_id": meter_id,
            "count":    len(alerts),
            "results":  alerts,
        })
    except Exception as e:
        log.error(f"/api/alerts/meter/{meter_id} error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/alerts/counts")
def alert_counts():
    """
    Returns count of alerts grouped by anomaly_type.
    Powers the Pie Chart panel in Superset.
    Response format Superset expects: list of {label, value} objects.
    """
    try:
        with HBaseClient(HBASE_HOST) as client:
            counts = client.count_alerts_by_type()
        
        chart_data = [
            {"anomaly_type": k, "count": v}
            for k, v in sorted(counts.items(), key=lambda x: -x[1])
        ]
        
        return jsonify({
            "status":  "ok",
            "results": chart_data,
        })
    except Exception as e:
        log.error(f"/api/alerts/counts error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/meters/top")
def top_meters():
    """Returns top N meters by alert count."""
    limit = _limit_param()
    
    try:
        with HBaseClient(HBASE_HOST) as client:
            top = client.get_top_meters_by_alerts(limit=limit)
        
        return jsonify({
            "status":  "ok",
            "count":   len(top),
            "results": top,
        })
    except Exception as e:
        log.error(f"/api/meters/top error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/meters/state/<meter_id>")
def meter_state(meter_id: str):
    """Returns current rolling statistics for a meter."""
    if not meter_id.replace("-", "").isalnum():
        abort(400, "Invalid meter_id format")
    
    try:
        with HBaseClient(HBASE_HOST) as client:
            state = client.get_meter_state(meter_id)
        
        if state is None:
            return jsonify({
                "status":   "not_found",
                "meter_id": meter_id,
            }), 404
        
        return jsonify({"status": "ok", "result": state})
    
    except Exception as e:
        log.error(f"/api/meters/state/{meter_id} error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/stats/summary")
def summary_stats():
    """
    Returns high-level platform statistics for the KPI cards.
    Combines data from multiple HBase tables.
    """
    try:
        with HBaseClient(HBASE_HOST) as client:
            counts    = client.count_alerts_by_type()
            top_meters = client.get_top_meters_by_alerts(limit=5)
        
        total_alerts = sum(counts.values())
        
        return jsonify({
            "status": "ok",
            "result": {
                "total_open_alerts":    total_alerts,
                "spike_count":          counts.get("sudden_spike",        0),
                "sustained_count":      counts.get("sustained_elevation",  0),
                "flatline_count":       counts.get("flatline",             0),
                "top_offending_meter":  top_meters[0]["meter_id"] if top_meters else "N/A",
            }
        })
    except Exception as e:
        log.error(f"/api/stats/summary error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.getenv("API_PORT", 5050))
    log.info(f"Starting HBase API on port {port}")
    log.info(f"HBase host: {HBASE_HOST}")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
