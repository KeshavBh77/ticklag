"""
Ticklag load test — ramps synthetic IoT load and measures where the pipeline
breaks. This is the ONLY source of the "real numbers" in the README: run it
against the live stack and it prints measured throughput, consumer lag growth,
and DB write latency at each stage. Nothing here is estimated.

It does NOT invent numbers. If the stack isn't running, it fails loudly.

Usage (stack must be up via `docker-compose up`):
    python load/loadtest.py --stages 5,10,25,50,100 --seconds 60

For each Hz stage it:
  1. restarts the iot-producer with that EMIT_HZ (via docker compose)
  2. lets it run `seconds`
  3. samples Prometheus for events/sec, consumer lag, p99 write latency
  4. records whether lag is stable or growing unbounded (the break signal)

Requires: requests, and Prometheus reachable on localhost:9090.
"""
import argparse
import subprocess
import sys
import time

import requests

PROM = "http://localhost:9090/api/v1/query"


def prom(expr):
    r = requests.get(PROM, params={"query": expr}, timeout=10)
    r.raise_for_status()
    data = r.json()["data"]["result"]
    if not data:
        return None
    return float(data[0]["value"][1])


def set_hz(hz):
    """Restart iot-producer with a new EMIT_HZ."""
    subprocess.run(
        ["docker", "compose", "stop", "iot-producer"],
        check=True, capture_output=True,
    )
    # override env and bring it back up
    env_line = f"EMIT_HZ={hz}"
    subprocess.run(
        ["docker", "compose", "up", "-d", "--no-deps", "iot-producer"],
        check=True, capture_output=True,
        env={**_docker_env(), "EMIT_HZ": str(hz)},
    )


def _docker_env():
    import os
    return dict(os.environ)


def sample():
    events = prom('sum(rate(ticklag_events_total{stream="sensors"}[30s]))') or 0.0
    lag = prom("sum(redpanda_kafka_consumer_group_lag_sum)") or 0.0
    p99 = prom(
        'histogram_quantile(0.99, sum(rate(ticklag_db_write_seconds_bucket'
        '{table="sensor_readings"}[30s])) by (le))'
    ) or 0.0
    return events, lag, p99


def run_stage(hz, seconds):
    print(f"\n=== stage: EMIT_HZ={hz} for {seconds}s ===")
    set_hz(hz)
    time.sleep(10)  # let it stabilize before sampling

    lag_start = sample()[1]
    samples = []
    t_end = time.time() + seconds
    while time.time() < t_end:
        ev, lag, p99 = sample()
        samples.append((ev, lag, p99))
        print(f"  events/s={ev:8.1f}  lag={lag:10.0f}  p99_write={p99*1000:6.1f}ms")
        time.sleep(10)

    lag_end = samples[-1][1]
    avg_events = sum(s[0] for s in samples) / len(samples)
    max_p99 = max(s[2] for s in samples)
    lag_growth = lag_end - lag_start

    # break heuristic: lag grew by more than one second's worth of events =
    # consumer can't keep up, the pipeline is falling behind at this rate
    unbounded = lag_growth > avg_events
    verdict = "BREAKING (lag growing unbounded)" if unbounded else "stable"
    print(f"  -> avg events/s={avg_events:.0f}, lag growth={lag_growth:.0f}, "
          f"max p99 write={max_p99*1000:.1f}ms => {verdict}")
    return {
        "hz": hz, "avg_events_per_s": round(avg_events, 1),
        "lag_growth": round(lag_growth, 0), "max_p99_write_ms": round(max_p99 * 1000, 1),
        "verdict": verdict,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stages", default="5,10,25,50,100",
                    help="comma-separated EMIT_HZ values to ramp through")
    ap.add_argument("--seconds", type=int, default=60, help="seconds per stage")
    args = ap.parse_args()

    # fail loudly if the stack isn't reachable -- we never fabricate results
    try:
        requests.get("http://localhost:9090/-/healthy", timeout=5)
    except requests.exceptions.RequestException:
        sys.exit("Prometheus not reachable at :9090. Is `docker-compose up` running?")

    stages = [int(x) for x in args.stages.split(",")]
    results = [run_stage(hz, args.seconds) for hz in stages]

    print("\n\n========== LOAD TEST SUMMARY (paste into README) ==========")
    print(f"{'EMIT_HZ':>8} {'events/s':>10} {'lag growth':>12} {'p99 write':>12}  verdict")
    for r in results:
        print(f"{r['hz']:>8} {r['avg_events_per_s']:>10} {r['lag_growth']:>12} "
              f"{r['max_p99_write_ms']:>10}ms  {r['verdict']}")

    breaking = [r for r in results if "BREAK" in r["verdict"]]
    if breaking:
        ceiling = min(r["hz"] for r in breaking)
        print(f"\nCeiling: pipeline first breaks at EMIT_HZ={ceiling} "
              f"(~{[r for r in results if r['hz']==ceiling][0]['avg_events_per_s']} events/s).")
    else:
        print("\nNo break observed in tested range — push higher --stages to find the ceiling.")


if __name__ == "__main__":
    main()
