"""Clocks.

A session gap is 30 minutes. Generating a realistic session at wall speed means
waiting 30 minutes to see one end. That is fine for a demo and useless for a test,
so simulated time is separable from wall time here.

`speedup` is simulated seconds per wall second. At 1.0 the two agree.
"""

import time
from datetime import datetime, timedelta, timezone


class RealClock:
    """Wall time. speedup is fixed at 1.0 and sleeping is real sleeping."""

    speedup = 1.0

    def __init__(self, start: datetime | None = None):
        self._start = start or datetime.now(timezone.utc)
        self._wall_start = time.monotonic()

    def now(self) -> datetime:
        return self._start + timedelta(seconds=time.monotonic() - self._wall_start)

    def sim_elapsed(self) -> float:
        return time.monotonic() - self._wall_start

    def sleep_until_sim(self, sim_seconds: float) -> float:
        """Block until `sim_seconds` have passed since the start. Returns how long it
        actually slept, which is negative-clamped to zero when we are already late."""
        target = self._wall_start + sim_seconds
        gap = target - time.monotonic()
        if gap > 0:
            time.sleep(gap)
            return gap
        return 0.0


class VirtualClock:
    """Simulated time that advances faster than wall time.

    Two things use this. Tests, at a speedup high enough that no sleeping happens.
    And a backfill run, where you want an hour of session behaviour in a minute.

    The wall pacing is still honoured, so a run at speedup 60 and rate 10 emits
    600 events per wall second. Set speedup high enough and the pacer stops
    sleeping entirely, which is the tell that the generator is the bottleneck
    rather than the schedule.
    """

    def __init__(self, start: datetime | None = None, speedup: float = 60.0):
        if speedup <= 0:
            raise ValueError(f"speedup must be positive, got {speedup}")
        self.speedup = float(speedup)
        self._start = start or datetime.now(timezone.utc)
        self._wall_start = time.monotonic()
        self._sim = 0.0

    def now(self) -> datetime:
        return self._start + timedelta(seconds=self._sim)

    def sim_elapsed(self) -> float:
        return self._sim

    def sleep_until_sim(self, sim_seconds: float) -> float:
        self._sim = max(self._sim, sim_seconds)
        target = self._wall_start + sim_seconds / self.speedup
        gap = target - time.monotonic()
        if gap > 0:
            time.sleep(gap)
            return gap
        return 0.0


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
