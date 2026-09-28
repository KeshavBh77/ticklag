"""
Synthetic IoT fleet -> Redpanda `sensor-readings` producer.

Imports the pure generator from iot_fleet.py (unit-tested separately) and
handles only the Kafka wiring + rate control here.

Partitioning choice: key = machine_id. All readings from one machine stay
ordered on one partition, which the stream processor needs for per-machine
windowing. 5 machines -> 5 partitions -> even spread.

EMIT_HZ is the knob the load generator ramps. It's readings-PER-SENSOR per
second; total msg/sec = EMIT_HZ * (num machines) * (sensors emitting).
At 50 sensors and EMIT_HZ=10 that's ~500 msg/s baseline; the load test
pushes this up until the pipeline breaks.
"""
import json
import os
import signal
import time

from confluent_kafka import Producer

from iot_fleet import IoTFleet, reading_to_dict

BROKER = os.environ.get("REDPANDA_BROKER", "redpanda:9092")
TOPIC = "sensor-readings"
EMIT_HZ = float(os.environ.get("EMIT_HZ", "10"))  # readings per sensor per second

_running = True


def _shutdown(signum, frame):
    global _running
    _running = False


signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)


def make_producer():
    return Producer({
        "bootstrap.servers": BROKER,
        "linger.ms": 10,
        "batch.size": 64 * 1024,
        "acks": "1",
        "compression.type": "lz4",
        "queue.buffering.max.messages": 500000,
    })


def run():
    producer = make_producer()
    fleet = IoTFleet()
    interval = 1.0 / EMIT_HZ
    print(f"[iot] emitting at {EMIT_HZ} Hz per sensor -> topic {TOPIC}", flush=True)

    emitted = 0
    last_report = time.time()

    while _running:
        loop_start = time.time()

        for reading in fleet.tick():
            producer.produce(
                TOPIC,
                key=reading.machine_id.encode("utf-8"),  # partition by machine
                value=json.dumps(reading_to_dict(reading)).encode("utf-8"),
            )
            emitted += 1
        producer.poll(0)

        # throughput heartbeat once a second (visible in docker logs)
        now = time.time()
        if now - last_report >= 1.0:
            print(f"[iot] {emitted} msgs in last {now - last_report:.1f}s", flush=True)
            emitted = 0
            last_report = now

        # rate control: sleep the remainder of the interval, or don't sleep at
        # all if we're already behind (that's the load-test's "breaking" signal)
        elapsed = time.time() - loop_start
        sleep_for = interval - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)

    producer.flush(10)
    print("[iot] shut down cleanly", flush=True)


if __name__ == "__main__":
    run()
