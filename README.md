# Ticklag — Real-Time Data Streaming Pipeline

A real-time data pipeline I built to learn production-grade stream processing. The name comes from stock "ticks" + Kafka "consumer lag" — the two things I ended up obsessing over while building this.

It ingests live stock market data and synthetic industrial IoT telemetry, runs windowed anomaly detection, stores everything in a time-series database, and visualizes it in Grafana — all running locally with one command, completely free.

## What it does

Two data feeds → Redpanda (Kafka-compatible broker) → Faust stream processor → TimescaleDB → Grafana dashboards, with Prometheus tracking pipeline health in real time.

- **Live market feed:** AAPL, MSFT, GOOGL, TSLA, NVDA via Finnhub websocket (free tier)
- **Synthetic IoT fleet:** 5 machines × 10 sensors emitting at configurable Hz, with injected anomalies for the detector to catch
- **Anomaly detection:** rolling z-score (Welford's algorithm) flags readings > 3 standard deviations — computed before folding in the new point so an outlier can't hide itself by inflating its own baseline
- **Storage:** TimescaleDB hypertables + continuous 1-minute aggregates so dashboard queries stay fast as data grows

## Architecture

```
  ┌─────────────────┐     ┌──────────────────┐
  │ Finnhub WS      │     │ Synthetic IoT    │
  │ (AAPL,MSFT,...) │     │ 5 machines ×     │
  │                 │     │ 10 sensors       │
  └────────┬────────┘     └────────┬─────────┘
           │ key=symbol            │ key=machine_id
           ▼                       ▼
      ┌─────────────────────────────────┐
      │        Redpanda (Kafka API)     │
      │  stock-ticks(5p)  sensor-...(5p)│
      └───────────────┬─────────────────┘
                      │
                      ▼
        ┌──────────────────────────────┐
        │  Faust stream processor      │
        │  • rolling mean/stddev       │
        │  • z-score anomaly (|z|>3)   │
        │  • tumbling + sliding windows│
        │  • late-arrival/watermark    │
        │  • batched async DB writes   │
        └───────┬───────────────┬──────┘
                │               │ /metrics :8000
                ▼               ▼
     ┌──────────────────┐  ┌──────────────┐
     │  TimescaleDB     │  │  Prometheus  │
     │  hypertables +   │  │  scrapes lag,│
     │  continuous aggs │  │  throughput  │
     └────────┬─────────┘  └──────┬───────┘
              │                   │
              ▼                   ▼
          ┌───────────────────────────┐
          │        Grafana OSS        │
          │  Market · Fleet · Health  │
          └───────────────────────────┘
```

## Repo layout

```
ticklag/
├── docker-compose.yml          all services wired together, one-command up
├── .env.example                copy to .env, add your free Finnhub key
├── producers/
│   ├── iot_fleet.py            pure generator logic (unit-tested separately)
│   ├── iot_producer.py         Kafka wiring, configurable EMIT_HZ for load testing
│   ├── finnhub_producer.py     websocket ingest, reconnect logic, dead-letter handling
│   └── Dockerfile
├── stream/
│   ├── windowing.py            Welford stats + z-score math (unit-tested)
│   ├── faust_app.py            Faust agents, batched DB writes, Prometheus metrics
│   └── Dockerfile
├── db/init.sql                 hypertable setup + continuous aggregates
├── prometheus/prometheus.yml   scrape config
├── grafana/
│   ├── provisioning/           auto-loads datasources + dashboards on startup
│   ├── build_dashboards.py     generates the dashboard JSON programmatically
│   └── dashboards/*.json       3 provisioned dashboards
├── load/loadtest.py            ramps EMIT_HZ, measures the break point
└── tests/test_logic.py         10 unit tests
```

## Setup

### Prerequisites
- Docker + Docker Compose
- Free Finnhub API key from https://finnhub.io (no card required)

### Run it

```bash
git clone https://github.com/KeshavBh77/ticklag.git
cd ticklag
cp .env.example .env
# open .env and paste your Finnhub key
docker-compose up --build
```

That's it. Health checks handle startup order automatically — the stream processor waits for the DB and topics before starting.

### Open the dashboards
- **Grafana:** http://localhost:3000 (admin / admin) → Dashboards → Ticklag folder
- **Prometheus:** http://localhost:9090
- **Raw metrics:** http://localhost:8000

> Market Data dashboard shows live data during US market hours (Mon-Fri 9:30am-4pm ET). Sensor Fleet runs 24/7.

### Run the tests
```bash
pip install pytest
python -m pytest tests/ -v
```
10 tests — covers the anomaly injector, z-score math, Welford variance, sliding window forgetting, and watermark late-arrival detection.

## Design decisions I had to think through

**Why partition by symbol / machine_id?**
Kafka preserves ordering within a partition. The stream processor builds rolling stats per symbol and per sensor — if events for the same symbol arrived out of order, the window state would be wrong. Keying by symbol/machine_id means all events for a given key always go to the same partition, preserving the order that matters.

**Why Welford's algorithm instead of tracking sum and sum-of-squares?**
The naive approach (variance = E[x²] - E[x]²) loses floating point precision on long streams when the two large numbers nearly cancel. Welford's computes variance incrementally with one pass, O(1) per event, and stays numerically stable. The difference matters when you're processing millions of readings.

**Why compute z-score before adding the new point?**
If you fold the outlier in first and then compute the z-score against the updated baseline, the outlier partially inflates its own mean and stddev — making it look less extreme than it is. Computing before-add means the baseline reflects only the historical window, not the point being scored. There's a unit test specifically for this.

**Why batch DB writes instead of per-row INSERTs?**
Per-row INSERTs saturate Postgres almost immediately at any meaningful throughput. Buffering to 500 rows and flushing with `executemany` over an asyncpg pool is the single biggest throughput lever in the whole pipeline — the load test shows exactly where this matters.

**Why acks=1 on the producers instead of acks=all?**
A dropped tick or sensor reading is acceptable for this workload — it's telemetry, not a financial transaction. `acks=all` would wait for all replicas to confirm before returning, adding latency for a durability guarantee this use case doesn't need.

## Error handling

- **Websocket reconnect:** exponential backoff with jitter so a flapping Finnhub connection doesn't become a tight reconnect loop hammering their servers
- **Dead-letter queue:** malformed messages go to `stock-ticks-dlq` and increment a Prometheus counter — the consumer loop never crashes on bad input
- **Backpressure:** producer buffers are bounded; when the pipeline can't keep up, consumer lag grows visibly in Prometheus rather than silently dropping data
- **Graceful shutdown:** SIGTERM/SIGINT flush the write buffers before exiting

## Load test results

Run this yourself to get real numbers on your hardware:
```bash
pip install requests
python load/loadtest.py --stages 5,10,25,50,100 --seconds 60
```

| EMIT_HZ | events/sec | lag growth | p99 write | verdict |
|--------:|-----------:|-----------:|----------:|---------|
| 5       | _____      | _____      | _____ ms  | stable  |
| 10      | _____      | _____      | _____ ms  | stable  |
| 25      | _____      | _____      | _____ ms  | _____   |
| 50      | _____      | _____      | _____ ms  | _____   |
| 100     | _____      | _____      | _____ ms  | _____   |

**Ceiling:** EMIT_HZ=____ (~____ events/sec)
**Bottleneck identified:** ____
**Fix:** ____

## Known limitations

- Rolling-stat state lives in-process — fine for 55 keys, would need RocksDB-backed Faust tables to survive worker restarts at larger scale
- Single Redpanda broker, replication factor 1 — no fault tolerance, intentional for a local dev setup
- Grafana has no auth beyond admin/admin — don't expose it publicly without changing this

## Tech stack

Python · Redpanda · Faust · TimescaleDB · Grafana OSS · Prometheus · Docker Compose · confluent-kafka · asyncpg
