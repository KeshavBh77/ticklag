"""
Generates the three provisioned Grafana dashboards as JSON. Building them in
code (rather than hand-writing 600 lines of JSON) guarantees consistent
structure and valid panel grid positions.

Run once: python grafana/build_dashboards.py
Writes into grafana/dashboards/.
"""
import json
import os

OUT = os.path.join(os.path.dirname(__file__), "dashboards")
os.makedirs(OUT, exist_ok=True)

TS = "TimescaleDB"
PROM = "Prometheus"


def ts_target(sql, ref="A"):
    return {
        "refId": ref,
        "format": "time_series",
        "rawSql": sql,
        "datasource": {"type": "postgres", "uid": "${DS_TIMESCALEDB}"},
    }


def prom_target(expr, ref="A", legend=""):
    return {
        "refId": ref,
        "expr": expr,
        "legendFormat": legend,
        "datasource": {"type": "prometheus", "uid": "${DS_PROMETHEUS}"},
    }


def panel(title, targets, gridPos, ptype="timeseries", datasource_type="postgres", unit=None):
    p = {
        "title": title,
        "type": ptype,
        "gridPos": gridPos,
        "targets": targets,
        "datasource": {"type": datasource_type,
                       "uid": "${DS_TIMESCALEDB}" if datasource_type == "postgres" else "${DS_PROMETHEUS}"},
        "fieldConfig": {"defaults": {}, "overrides": []},
    }
    if unit:
        p["fieldConfig"]["defaults"]["unit"] = unit
    return p


def dashboard(title, uid, panels, templating=None):
    return {
        "title": title,
        "uid": uid,
        "schemaVersion": 39,
        "version": 1,
        "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "timezone": "browser",
        "panels": panels,
        "templating": {"list": templating or []},
        "__inputs": [],
    }


# ---------- 1. Market Data ----------
market = dashboard(
    "Ticklag: Market Data", "ticklag-market",
    [
        panel("Live price by symbol (1m avg)", [ts_target(
            "SELECT bucket AS time, symbol AS metric, avg_price AS value "
            "FROM ticks_1m WHERE $__timeFilter(bucket) ORDER BY bucket")],
            {"h": 9, "w": 16, "x": 0, "y": 0}, unit="currencyUSD"),
        panel("Tick throughput (count/min by symbol)", [ts_target(
            "SELECT bucket AS time, symbol AS metric, tick_count AS value "
            "FROM ticks_1m WHERE $__timeFilter(bucket) ORDER BY bucket")],
            {"h": 9, "w": 8, "x": 16, "y": 0}),
        panel("Price anomalies (z>3) — last 15m", [ts_target(
            "SELECT event_time AS time, symbol, price, zscore "
            "FROM ticks WHERE is_anomaly AND $__timeFilter(event_time) ORDER BY event_time DESC LIMIT 100")],
            {"h": 8, "w": 24, "x": 0, "y": 9}, ptype="table"),
    ],
)

# ---------- 2. Sensor Fleet ----------
fleet = dashboard(
    "Ticklag: Sensor Fleet", "ticklag-fleet",
    [
        panel("Avg sensor value by machine & type (1m)", [ts_target(
            "SELECT bucket AS time, machine_id || '/' || sensor_type AS metric, avg_value AS value "
            "FROM sensor_readings_1m WHERE $__timeFilter(bucket) ORDER BY bucket")],
            {"h": 9, "w": 16, "x": 0, "y": 0}),
        panel("Anomaly count per machine (1m)", [ts_target(
            "SELECT bucket AS time, machine_id AS metric, anomaly_count AS value "
            "FROM sensor_readings_1m WHERE $__timeFilter(bucket) ORDER BY bucket")],
            {"h": 9, "w": 8, "x": 16, "y": 0}),
        panel("Recent sensor anomalies (z>3)", [ts_target(
            "SELECT event_time AS time, machine_id, sensor_id, sensor_type, value, zscore "
            "FROM sensor_readings WHERE is_anomaly AND $__timeFilter(event_time) "
            "ORDER BY event_time DESC LIMIT 100")],
            {"h": 8, "w": 24, "x": 0, "y": 9}, ptype="table"),
    ],
)

# ---------- 3. Pipeline Health ----------
health = dashboard(
    "Ticklag: Pipeline Health", "ticklag-health",
    [
        panel("Events/sec by stream", [prom_target(
            "rate(ticklag_events_total[1m])", legend="{{stream}}")],
            {"h": 8, "w": 12, "x": 0, "y": 0}, datasource_type="prometheus", unit="ops"),
        panel("Consumer lag (Redpanda)", [prom_target(
            "sum by (group) (redpanda_kafka_consumer_group_lag_sum)", legend="{{group}}")],
            {"h": 8, "w": 12, "x": 12, "y": 0}, datasource_type="prometheus"),
        panel("DB write latency p99", [prom_target(
            "histogram_quantile(0.99, sum(rate(ticklag_db_write_seconds_bucket[1m])) by (le, table))",
            legend="{{table}} p99")],
            {"h": 8, "w": 12, "x": 0, "y": 8}, datasource_type="prometheus", unit="s"),
        panel("Anomalies/sec", [prom_target(
            "rate(ticklag_anomalies_total[1m])", legend="{{stream}}")],
            {"h": 8, "w": 6, "x": 12, "y": 8}, datasource_type="prometheus"),
        panel("Dead-lettered events", [prom_target(
            "ticklag_dlq_total", legend="{{stream}}")],
            {"h": 8, "w": 6, "x": 18, "y": 8}, datasource_type="prometheus", ptype="stat"),
    ],
)

# templating inputs so the provisioned datasource UIDs resolve
for d in (market, fleet, health):
    d["templating"]["list"] = [
        {"name": "DS_TIMESCALEDB", "type": "datasource", "query": "postgres",
         "current": {"text": "TimescaleDB", "value": "TimescaleDB"}},
        {"name": "DS_PROMETHEUS", "type": "datasource", "query": "prometheus",
         "current": {"text": "Prometheus", "value": "Prometheus"}},
    ]

for name, d in [("market_data.json", market), ("sensor_fleet.json", fleet), ("pipeline_health.json", health)]:
    with open(os.path.join(OUT, name), "w") as f:
        json.dump(d, f, indent=2)
    print("wrote", name)
