"""
Faust stream processor for Ticklag.

Consumes both topics, maintains per-symbol and per-sensor rolling stats,
flags z-score anomalies (|z|>3), and writes batched into TimescaleDB.
Exposes Prometheus metrics (events/sec, write latency, anomaly count) on
:8000 for the pipeline-health dashboard.

What Faust is doing under the hood (so you can defend it):
  - Faust is a Python stream processor that uses Kafka consumer groups.
    Each `@app.agent` is an async coroutine consuming a topic; Faust runs
    partitions across workers and commits offsets. Consumer lag (our headline
    metric) is Kafka's record of how far behind the committed offset is from
    the log end -- that's what Prometheus scrapes.
  - Tables (app.Table) are RocksDB-backed, changelog-replicated state. We use
    plain in-process dicts for rolling stats instead, because our state is
    small (50 sensors + 5 symbols) and we want the Welford objects, not
    serialized table values -- simpler and faster for this scale. Say this
    if asked why we're not using Faust tables.

Async batched DB writes: we buffer rodocker-compose up -dws and flush every BATCH_SIZE or
FLUSH_INTERVAL, whichever first. Per-row INSERTs would make the DB the
bottleneck almost immediately; batching is the single biggest throughput
lever here.
"""
import json
import os
import time

import asyncpg
import faust
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from windowing import RollingStats, zscore, is_anomaly

BROKER = os.environ.get("REDPANDA_BROKER", "redpanda:9092")
PG_DSN = os.environ.get("PG_DSN", "postgresql://ticklag:ticklag@timescaledb:5432/ticklag")
Z_THRESHOLD = float(os.environ.get("Z_THRESHOLD", "3.0"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "500"))
FLUSH_INTERVAL = float(os.environ.get("FLUSH_INTERVAL", "1.0"))
WINDOW_MAXLEN = int(os.environ.get("WINDOW_MAXLEN", "200"))  # cap rolling-stat memory

app = faust.App(
    "ticklag",
    broker=f"kafka://{BROKER}",
    value_serializer="raw",  # we json.loads manually -> full control over malformed data
    consumer_auto_offset_reset="latest",
)

stock_topic = app.topic("stock-ticks", value_type=bytes)
sensor_topic = app.topic("sensor-readings", value_type=bytes)

# ---- Prometheus metrics ----
EVENTS = Counter("ticklag_events_total", "events processed", ["stream"])
ANOMALIES = Counter("ticklag_anomalies_total", "anomalies detected", ["stream"])
WRITE_LATENCY = Histogram("ticklag_db_write_seconds", "batch write latency", ["table"])
BATCH_ROWS = Gauge("ticklag_last_batch_rows", "rows in last flushed batch", ["table"])
DLQ = Counter("ticklag_dlq_total", "malformed events dead-lettered", ["stream"])

# ---- in-process rolling state ----
symbol_stats = {}   # symbol -> RollingStats (rolling, capped via reset)
sensor_stats = {}   # sensor_id -> RollingStats

# ---- write buffers ----
_tick_buffer = []
_sensor_buffer = []
_pool = None


async def get_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(PG_DSN, min_size=2, max_size=8)
    return _pool


def _rolling(stats_map, key):
    s = stats_map.get(key)
    if s is None or s.count >= WINDOW_MAXLEN:
        # reset once we hit the cap so the baseline tracks recent behavior
        # (concept drift) instead of averaging over all history forever
        s = RollingStats()
        stats_map[key] = s
    return s


@app.agent(stock_topic)
async def process_ticks(stream):
    async for raw in stream:
        try:
            rec = json.loads(raw)
            symbol = rec["symbol"]
            price = float(rec["price"])
            event_time = int(rec["event_time"])
        except (json.JSONDecodeError, KeyError, ValueError, TypeError):
            DLQ.labels(stream="ticks").inc()
            continue

        stats = _rolling(symbol_stats, symbol)
        z = zscore(price, stats)
        anomaly = abs(z) > Z_THRESHOLD
        stats.update(price)

        EVENTS.labels(stream="ticks").inc()
        if anomaly:
            ANOMALIES.labels(stream="ticks").inc()

        _tick_buffer.append((event_time, symbol, price, rec.get("volume", 0.0), z, anomaly))
        if len(_tick_buffer) >= BATCH_SIZE:
            await flush_ticks()


@app.agent(sensor_topic)
async def process_sensors(stream):
    async for raw in stream:
        try:
            rec = json.loads(raw)
            machine = rec["machine_id"]
            sensor = rec["sensor_id"]
            stype = rec["sensor_type"]
            value = float(rec["value"])
            event_time = int(rec["event_time"])
        except (json.JSONDecodeError, KeyError, ValueError, TypeError):
            DLQ.labels(stream="sensors").inc()
            continue

        stats = _rolling(sensor_stats, sensor)
        z = zscore(value, stats)
        anomaly = abs(z) > Z_THRESHOLD
        stats.update(value)

        EVENTS.labels(stream="sensors").inc()
        if anomaly:
            ANOMALIES.labels(stream="sensors").inc()

        _sensor_buffer.append((event_time, machine, sensor, stype, value, z, anomaly))
        if len(_sensor_buffer) >= BATCH_SIZE:
            await flush_sensors()


async def flush_ticks():
    if not _tick_buffer:
        return
    rows = _tick_buffer.copy()
    _tick_buffer.clear()
    pool = await get_pool()
    with WRITE_LATENCY.labels(table="ticks").time():
        async with pool.acquire() as conn:
            await conn.executemany(
                "INSERT INTO ticks (event_time, symbol, price, volume, zscore, is_anomaly) "
                "VALUES (to_timestamp($1/1000.0), $2, $3, $4, $5, $6)",
                rows,
            )
    BATCH_ROWS.labels(table="ticks").set(len(rows))


async def flush_sensors():
    if not _sensor_buffer:
        return
    rows = _sensor_buffer.copy()
    _sensor_buffer.clear()
    pool = await get_pool()
    with WRITE_LATENCY.labels(table="sensor_readings").time():
        async with pool.acquire() as conn:
            await conn.executemany(
                "INSERT INTO sensor_readings "
                "(event_time, machine_id, sensor_id, sensor_type, value, zscore, is_anomaly) "
                "VALUES (to_timestamp($1/1000.0), $2, $3, $4, $5, $6, $7)",
                rows,
            )
    BATCH_ROWS.labels(table="sensor_readings").set(len(rows))


@app.timer(interval=FLUSH_INTERVAL)
async def periodic_flush():
    # time-based flush so low-traffic periods still persist promptly
    # (otherwise a half-full buffer could sit for a long time)
    await flush_ticks()
    await flush_sensors()


@app.on_before_shutdown
async def drain(app_):
    await flush_ticks()
    await flush_sensors()


# Prometheus HTTP endpoint. Faust has its own web server but exposing a
# dedicated metrics port keeps the scrape config trivial.
start_http_server(8000)
