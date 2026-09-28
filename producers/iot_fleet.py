"""
Synthetic IoT fleet generator: 5 machines x sensors emitting at a
configurable rate, with injected anomalies. Pure logic here (no Kafka)
so it can be unit-tested in isolation; the Kafka wiring lives in
iot_producer.py which imports this.


"""
import math
import random
import time
from dataclasses import dataclass, asdict

MACHINES = [f"machine-{i:02d}" for i in range(1, 6)]

# 10 sensors per machine, types rotated across the 10 as the prompt specifies
SENSOR_TYPES = ["temperature", "vibration", "pressure"]
SENSORS_PER_MACHINE = 10

# baseline (mean, stddev) per sensor type in its natural units
BASELINES = {
    "temperature": (65.0, 2.0),   # deg C
    "vibration": (0.5, 0.1),      # mm/s RMS
    "pressure": (100.0, 3.0),     # kPa
}


def sensor_type_for_index(idx: int) -> str:
    # rotate temperature/vibration/pressure across the 10 sensors deterministically
    return SENSOR_TYPES[idx % len(SENSOR_TYPES)]


@dataclass
class Reading:
    machine_id: str
    sensor_id: str
    sensor_type: str
    value: float
    event_time: int  # epoch ms
    anomaly_injected: bool  # ground-truth label, for validating the detector


class AnomalyState:
    """
    Tracks which (machine, sensor) is currently in an injected-anomaly episode
    and what kind. Episodes last a random number of readings so drift/spike
    anomalies persist across a window rather than being single-point blips.
    """
    def __init__(self):
        self.active = {}  # key -> (kind, remaining_readings)

    def maybe_start(self, key, prob=0.0008):
        if key in self.active:
            return
        if random.random() < prob:
            kind = random.choice(["temp_spike", "vibration_drift", "dropout"])
            duration = random.randint(20, 80)
            self.active[key] = (kind, duration)

    def apply(self, key, sensor_type, value):
        """Returns (possibly-modified value or None for dropout, injected_flag)."""
        if key not in self.active:
            return value, False
        kind, remaining = self.active[key]
        remaining -= 1
        if remaining <= 0:
            del self.active[key]
        else:
            self.active[key] = (kind, remaining)

        if kind == "dropout":
            return None, True  # sensor drops out -> emit nothing
        if kind == "temp_spike" and sensor_type == "temperature":
            return value + random.uniform(15, 30), True
        if kind == "vibration_drift" and sensor_type == "vibration":
            # drift grows as the episode progresses
            return value + random.uniform(0.4, 1.2), True
        # anomaly kind doesn't match this sensor type -> pass through unmodified
        return value, False


class IoTFleet:
    def __init__(self, seed=None):
        if seed is not None:
            random.seed(seed)
        self.anomaly = AnomalyState()

    def _base_value(self, sensor_type, t):
        mean, std = BASELINES[sensor_type]
        # gentle diurnal-ish oscillation + gaussian noise
        oscillation = 0.5 * std * math.sin(t / 30.0)
        return mean + oscillation + random.gauss(0, std)

    def tick(self):
        """Generate one full sweep of all machines x sensors. Yields Reading objects."""
        now_ms = int(time.time() * 1000)
        t = time.time()
        for machine in MACHINES:
            for s_idx in range(SENSORS_PER_MACHINE):
                sensor_type = sensor_type_for_index(s_idx)
                sensor_id = f"{machine}-s{s_idx:02d}-{sensor_type}"
                key = (machine, sensor_id)

                self.anomaly.maybe_start(key)
                base = self._base_value(sensor_type, t)
                value, injected = self.anomaly.apply(key, sensor_type, base)

                if value is None:  # dropout episode -> skip emission entirely
                    continue

                yield Reading(
                    machine_id=machine,
                    sensor_id=sensor_id,
                    sensor_type=sensor_type,
                    value=round(value, 4),
                    event_time=now_ms,
                    anomaly_injected=injected,
                )


def reading_to_dict(r: Reading) -> dict:
    return asdict(r)
