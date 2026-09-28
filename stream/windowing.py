"""
Windowed aggregation + z-score anomaly math as PURE functions.

Kept separate from the Faust app so the statistics can be unit-tested
deterministically without spinning up Kafka. faust_app.py imports these.


"""
from dataclasses import dataclass, field
import math


@dataclass
class RollingStats:
    """Welford online mean/variance over a bounded count of recent points."""
    count: int = 0
    mean: float = 0.0
    m2: float = 0.0  # sum of squares of differences from the current mean

    def update(self, x: float):
        self.count += 1
        delta = x - self.mean
        self.mean += delta / self.count
        delta2 = x - self.mean
        self.m2 += delta * delta2

    @property
    def variance(self) -> float:
        if self.count < 2:
            return 0.0
        return self.m2 / (self.count - 1)  # sample variance

    @property
    def stddev(self) -> float:
        return math.sqrt(self.variance)


def zscore(x: float, stats: RollingStats) -> float:
    """
    z-score of x against the stats accumulated SO FAR (before x is added).
    Returns 0.0 when there isn't enough history or variance is zero --
    we don't flag anomalies until the baseline is established.
    """
    if stats.count < 2 or stats.stddev == 0:
        return 0.0
    return (x - stats.mean) / stats.stddev


def is_anomaly(x: float, stats: RollingStats, threshold: float = 3.0) -> bool:
    return abs(zscore(x, stats)) > threshold


@dataclass
class SlidingWindow:
    """
    Fixed-size sliding window of the most recent N values, with rolling
    stats recomputed over exactly those N. Used where we want the baseline
    to forget old data (concept drift) rather than accumulate forever.
    """
    size: int
    values: list = field(default_factory=list)

    def add(self, x: float):
        self.values.append(x)
        if len(self.values) > self.size:
            self.values.pop(0)

    def stats(self) -> RollingStats:
        s = RollingStats()
        for v in self.values:
            s.update(v)
        return s


def watermark_is_late(event_time_ms: int, watermark_ms: int, allowed_lateness_ms: int) -> bool:
    """
    True if this event is later than our watermark by more than the allowed
    grace period -- i.e. it belongs to a window we've already closed and
    should be routed to late-handling (dead-letter or a correction path),
    not silently folded into the current window.
    """
    return event_time_ms < (watermark_ms - allowed_lateness_ms)
