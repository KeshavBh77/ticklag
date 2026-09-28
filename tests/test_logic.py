import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "producers"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "stream"))

import math
import pytest
    from iot_fleet import IoTFleet, sensor_type_for_index, MACHINES, SENSORS_PER_MACHINE
    from windowing import RollingStats, zscore, is_anomaly, SlidingWindow, watermark_is_late


# ---------- IoT generator ----------

def test_sensor_type_rotation_is_deterministic():
    assert sensor_type_for_index(0) == "temperature"
    assert sensor_type_for_index(1) == "vibration"
    assert sensor_type_for_index(2) == "pressure"
    assert sensor_type_for_index(3) == "temperature"


def test_tick_emits_readings_for_fleet():
    fleet = IoTFleet(seed=1)
    readings = list(fleet.tick())
    # up to 5 machines x 10 sensors = 50, minus any dropout episodes
    assert len(readings) <= len(MACHINES) * SENSORS_PER_MACHINE
    assert len(readings) > 0
    for r in readings:
        assert r.machine_id in MACHINES
        assert r.sensor_type in ("temperature", "vibration", "pressure")
        assert isinstance(r.value, float)
        assert r.event_time > 0


def test_event_time_is_epoch_ms():
    fleet = IoTFleet(seed=2)
    r = next(iter(fleet.tick()))
    # epoch ms should be a 13-digit number in the current era
    assert r.event_time > 1_600_000_000_000


def test_injected_anomalies_eventually_appear():
    """Over enough ticks, the injector should produce at least one labeled anomaly."""
    fleet = IoTFleet(seed=42)
    seen_injected = False
    for _ in range(2000):
        for r in fleet.tick():
            if r.anomaly_injected:
                seen_injected = True
                break
        if seen_injected:
            break
    assert seen_injected, "no injected anomaly in 2000 ticks -- injector may be broken"


# ---------- Welford rolling stats ----------

def test_welford_matches_numpy_mean_and_var():
    import statistics
    data = [10, 12, 23, 23, 16, 23, 21, 16]
    s = RollingStats()
    for x in data:
        s.update(x)
    assert s.mean == pytest.approx(statistics.mean(data))
    assert s.variance == pytest.approx(statistics.variance(data))  # sample variance


def test_zscore_zero_without_enough_history():
    s = RollingStats()
    s.update(5.0)
    assert zscore(100.0, s) == 0.0  # only 1 point -> no baseline


def test_zscore_flags_clear_outlier():
    s = RollingStats()
    for x in [10, 10.1, 9.9, 10.05, 9.95, 10.0]:
        s.update(x)
    assert is_anomaly(50.0, s, threshold=3.0) is True
    assert is_anomaly(10.02, s, threshold=3.0) is False


def test_zscore_computed_before_adding_point():
    """An outlier must not inflate its own baseline and hide itself."""
    s = RollingStats()
    for x in [10, 10.2, 9.8, 10.1, 9.9]:  # small nonzero variance
        s.update(x)
    z_before = zscore(20.0, s)
    stddev_before = s.stddev
    s.update(20.0)  # now fold the outlier in
    z_after = zscore(20.0, s)  # recompute against the now-contaminated baseline
    # computing before adding gives a much larger (more detectable) z-score
    assert abs(z_before) > abs(z_after)
    assert abs(z_before) > 3.0


# ---------- Sliding window ----------

def test_sliding_window_forgets_old_values():
    w = SlidingWindow(size=3)
    for x in [1, 2, 3, 4, 5]:
        w.add(x)
    assert w.values == [3, 4, 5]
    assert w.stats().mean == pytest.approx(4.0)


# ---------- Watermark / late arrival ----------

def test_watermark_late_detection():
    watermark = 1_000_000
    allowed = 5_000
    assert watermark_is_late(990_000, watermark, allowed) is True   # 10s late > 5s grace
    assert watermark_is_late(998_000, watermark, allowed) is False  # 2s late, within grace
