"""
Finnhub websocket -> Redpanda `stock-ticks` producer.

Partitioning choice (defend this in interview):
  key = symbol. Kafka hashes the key to pick a partition, so all ticks for
  a given symbol land on the same partition and stay strictly ordered.
  Ordering per symbol matters because the stream processor computes rolling
  stats per symbol -- out-of-order ticks within a symbol would corrupt the
  window. Cross-symbol ordering is irrelevant, so 5 partitions across 5
  symbols also gives us clean parallelism.

Error handling that's actually here (not happy-path):
  - websocket auto-reconnect with exponential backoff + jitter
  - malformed / non-trade messages -> dead-letter topic, never crash the loop
  - producer send failures -> logged, retried by the client's internal buffer
"""
import json
import os
import random
import signal
import time

import websocket  # websocket-client
from confluent_kafka import Producer

FINNHUB_TOKEN = os.environ.get("FINNHUB_API_KEY", "")
BROKER = os.environ.get("REDPANDA_BROKER", "redpanda:9092")
SYMBOLS = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]
TOPIC = "stock-ticks"
DLQ_TOPIC = "stock-ticks-dlq"

_running = True


def _shutdown(signum, frame):
    global _running
    _running = False


signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)


def make_producer():
    # linger.ms batches sends for throughput; acks=1 balances durability vs latency.
    # A tick feed can tolerate acks=1 (not financial-transaction-critical); acks=all
    # would add latency for durability we don't need here. Say this if asked.
    return Producer({
        "bootstrap.servers": BROKER,
        "linger.ms": 20,
        "batch.size": 32 * 1024,
        "acks": "1",
        "compression.type": "lz4",
        "queue.buffering.max.messages": 200000,  # backpressure buffer
    })


def _delivery_report(err, msg):
    if err is not None:
        # send failure -> log; the client already retried internally.
        print(f"[finnhub] delivery failed: {err}", flush=True)


def handle_message(producer, raw):
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        producer.produce(DLQ_TOPIC, value=raw.encode("utf-8"))
        return

    if msg.get("type") != "trade":
        return  # ping/subscribe-ack etc -- ignore, not an error

    for trade in msg.get("data", []):
        try:
            record = {
                "symbol": trade["s"],
                "price": float(trade["p"]),
                "volume": float(trade.get("v", 0)),
                "event_time": int(trade["t"]),  # finnhub gives epoch ms
            }
        except (KeyError, ValueError, TypeError):
            producer.produce(DLQ_TOPIC, value=json.dumps(trade).encode("utf-8"))
            continue

        producer.produce(
            TOPIC,
            key=record["symbol"].encode("utf-8"),  # partition by symbol
            value=json.dumps(record).encode("utf-8"),
            callback=_delivery_report,
        )
    producer.poll(0)  # serve delivery callbacks without blocking


def run():
    if not FINNHUB_TOKEN:
        raise SystemExit("FINNHUB_API_KEY not set. Get a free key at finnhub.io and put it in .env")

    producer = make_producer()
    backoff = 1.0

    while _running:
        try:
            ws = websocket.create_connection(
                f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}", timeout=10
            )
            for sym in SYMBOLS:
                ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
            print(f"[finnhub] connected, subscribed to {SYMBOLS}", flush=True)
            backoff = 1.0  # reset backoff on a successful connect

            while _running:
                raw = ws.recv()
                if not raw:
                    raise ConnectionError("empty frame -- socket closed")
                handle_message(producer, raw)

        except Exception as e:
            # reconnect with capped exponential backoff + jitter so a flapping
            # upstream doesn't turn into a tight reconnect loop hammering finnhub
            wait = min(backoff, 30) + random.uniform(0, 1)
            print(f"[finnhub] disconnected ({e}); reconnecting in {wait:.1f}s", flush=True)
            time.sleep(wait)
            backoff *= 2
        finally:
            try:
                ws.close()
            except Exception:
                pass

    producer.flush(10)
    print("[finnhub] shut down cleanly", flush=True)


if __name__ == "__main__":
    run()
