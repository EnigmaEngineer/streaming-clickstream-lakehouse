"""The producer loop.

Rate control, late-event injection and duplicate emission. Everything the day 3
streaming job has to survive is created here on purpose and with a knob on it.

Run:
    python -m generator.produce --rate 200 --seconds 5 --sink null --report
"""

import argparse
import json
import random
import sys
from dataclasses import dataclass, field, asdict
from datetime import timedelta

from generator.arrivals import VisitPool
from generator.clock import RealClock, VirtualClock, iso
from generator.events import build_event
from generator.population import Population, crc32_partition
from generator.session import SessionModel
from generator.sinks import JsonlSink, MemorySink, NullSink


@dataclass
class Config:
    rate: float = 100.0          # events per second of simulated time
    seconds: float = 5.0         # simulated seconds to run for
    speedup: float = 1.0         # simulated seconds per wall second
    seed: int = 7
    users: int = 5000
    alpha: float = 1.0           # 0 is uniform, higher is a heavier tail
    late_rate: float = 0.05      # share of events given a lateness
    late_median_s: float = 12.0  # median of the lognormal lateness, in seconds
    late_sigma: float = 1.4      # spread of it, in log space
    dup_rate: float = 0.01       # share of events emitted a second time
    dup_delay_s: float = 3.0     # how far behind the original the copy arrives
    partitions: int = 6          # only used for the skew report
    pool: int = 150              # concurrent visits, see generator/arrivals.py
    visit_events: float = 6.0    # median events in one visit


@dataclass
class Stats:
    emitted: int = 0
    originals: int = 0
    duplicates: int = 0
    duplicates_pending: int = 0  # scheduled, due after the run ended, never sent
    late_events: int = 0
    sessions_started: int = 0
    visits_started: int = 0
    visits_ended: int = 0
    mean_occupancy: float = 0.0
    admissions_deflected: int = 0
    boundaries_gap_rule_misses: int = 0
    distinct_users: int = 0
    wall_seconds: float = 0.0
    sim_seconds: float = 0.0
    slept_seconds: float = 0.0
    lateness_samples: list = field(default_factory=list)
    user_counts: dict = field(default_factory=dict)

    def summary(self) -> dict:
        out = {k: v for k, v in asdict(self).items() if k not in ("lateness_samples", "user_counts")}
        out["achieved_sim_rate"] = round(self.emitted / self.sim_seconds, 2) if self.sim_seconds else 0.0
        out["achieved_wall_rate"] = round(self.emitted / self.wall_seconds, 2) if self.wall_seconds else 0.0
        if self.lateness_samples:
            s = sorted(self.lateness_samples)
            out["lateness_p50_s"] = round(s[len(s) // 2], 3)
            out["lateness_p95_s"] = round(s[int(len(s) * 0.95)], 3)
            out["lateness_max_s"] = round(s[-1], 3)
        return out


def _lateness(rng: random.Random, cfg: Config) -> float:
    """Lognormal, so most late events are a little late and a few are very late.

    The parameter given is the median rather than the mean, because the mean of a
    lognormal is dragged around by the tail and is not the number anyone means when
    they say "about ten seconds late".
    """
    import math

    mu = math.log(cfg.late_median_s)
    return math.exp(rng.gauss(mu, cfg.late_sigma))


def run(cfg: Config, sink, clock=None) -> Stats:
    rng = random.Random(cfg.seed)
    clock = clock or (RealClock() if cfg.speedup == 1.0 else VirtualClock(speedup=cfg.speedup))
    pop = Population(cfg.users, alpha=cfg.alpha, seed=cfg.seed)
    pool = VisitPool(pop, size=cfg.pool, median_events=cfg.visit_events)
    sessions = SessionModel()
    stats = Stats()

    import time

    wall_start = time.monotonic()
    pending: list[tuple[float, str, dict]] = []  # (due_sim, key, value)
    total = int(cfg.rate * cfg.seconds)

    for n in range(total):
        target_sim = n / cfg.rate
        stats.slept_seconds += clock.sleep_until_sim(target_sim)
        sim_now = clock.sim_elapsed()

        # Anything queued as a duplicate and now due goes out before the new event,
        # so the ordering the consumer sees is the ordering time implies.
        while pending and pending[0][0] <= sim_now:
            _, key, value = pending.pop(0)
            # Stamped at emission, not at scheduling. A copy carrying the original's
            # ingest_ts would be indistinguishable from a broken clock rather than
            # from a retry.
            value["ingest_ts"] = iso(clock.now())
            sink.send(key, value)
            stats.emitted += 1
            stats.duplicates += 1

        lateness = _lateness(rng, cfg) if rng.random() < cfg.late_rate else 0.0
        if lateness:
            stats.late_events += 1
            stats.lateness_samples.append(lateness)

        user_id, first_of_visit = pool.next_user(rng)
        event_time = clock.now() - timedelta(seconds=lateness)
        parts = sessions.step(user_id, event_time, rng, new_visit=first_of_visit)
        if parts["new_session"] and not parts["gap_rule_new_session"]:
            # A visit the gap rule cannot see, because the same user came back inside
            # thirty minutes. Day 3 will merge these two into one session and this is
            # the count of how often that is going to happen.
            stats.boundaries_gap_rule_misses += 1
        event = build_event(user_id, parts, clock.now(), timedelta(seconds=lateness))
        value = event.to_dict()

        sink.send(user_id, value)
        stats.emitted += 1
        stats.originals += 1
        stats.user_counts[user_id] = stats.user_counts.get(user_id, 0) + 1

        if rng.random() < cfg.dup_rate:
            # Same event_id, later ingest_ts. A retry the broker accepted twice.
            pending.append((sim_now + cfg.dup_delay_s, user_id, dict(value)))

    # Whatever is still queued is NOT emitted. The first version of this flushed the
    # queue at the end, which stamped every one of them with the final clock reading
    # and produced a burst of copies inside one millisecond. Day 3 would have read
    # that as a dedupe spike caused by the pipeline. The count is reported instead,
    # so a short run is explainable rather than quietly wrong. The share left behind
    # is roughly dup_delay_s over seconds, so it goes away on a long run.
    stats.duplicates_pending = len(pending)

    sink.flush()
    stats.wall_seconds = time.monotonic() - wall_start
    stats.sim_seconds = max(clock.sim_elapsed(), total / cfg.rate)
    stats.sessions_started = sessions.sessions_started
    stats.visits_started = pool.visits_started
    stats.visits_ended = pool.visits_ended
    stats.mean_occupancy = round(pool.mean_occupancy, 2)
    stats.admissions_deflected = pool.deflected
    stats.distinct_users = len(stats.user_counts)
    return stats


def partition_skew(stats: Stats, partitions: int) -> dict:
    load = [0] * partitions
    for user_id, n in stats.user_counts.items():
        load[crc32_partition(user_id, partitions)] += n
    even = sum(load) / partitions if partitions else 0
    return {
        "partitions": partitions,
        "load": load,
        "max_over_even": round(max(load) / even, 3) if even else 0.0,
        "min_over_even": round(min(load) / even, 3) if even else 0.0,
    }


def build_sink(name: str, path: str | None):
    if name == "null":
        return NullSink()
    if name == "memory":
        return MemorySink()
    if name == "stdout":
        return JsonlSink(sys.stdout)
    if name == "file":
        if not path:
            raise ValueError("--out is required with --sink file")
        return JsonlSink(open(path, "w", encoding="utf-8"))
    if name == "kafka":
        from generator.sinks import KafkaSink  # noqa: PLC0415

        raise NotImplementedError(
            "the kafka sink exists but has never been run against a broker, see day 3"
        )
    raise ValueError(f"unknown sink {name!r}")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="clickstream event generator")
    p.add_argument("--rate", type=float, default=100.0)
    p.add_argument("--seconds", type=float, default=5.0)
    p.add_argument("--speedup", type=float, default=1.0)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--users", type=int, default=5000)
    p.add_argument("--alpha", type=float, default=1.0)
    p.add_argument("--late-rate", type=float, default=0.05)
    # Day 3 needs to push the lateness tail past the thirty minute session gap. Below
    # the gap, an online rule that tracks the newest event time and an offline session
    # window agree exactly, because no late event can ever bridge a gap it is shorter
    # than. Above it they must diverge, and that is a claim worth being able to run.
    p.add_argument("--late-median-s", type=float, default=12.0)
    p.add_argument("--late-sigma", type=float, default=1.4)
    p.add_argument("--dup-rate", type=float, default=0.01)
    p.add_argument("--partitions", type=int, default=6)
    p.add_argument("--pool", type=int, default=150, help="concurrent visits")
    p.add_argument("--visit-events", type=float, default=6.0, help="median events per visit")
    p.add_argument("--sink", default="null", choices=["null", "memory", "stdout", "file", "kafka"])
    p.add_argument("--out", default=None)
    p.add_argument("--report", action="store_true", help="print a stats block to stderr")
    a = p.parse_args(argv)

    cfg = Config(
        rate=a.rate,
        seconds=a.seconds,
        speedup=a.speedup,
        seed=a.seed,
        users=a.users,
        alpha=a.alpha,
        late_rate=a.late_rate,
        late_median_s=a.late_median_s,
        late_sigma=a.late_sigma,
        dup_rate=a.dup_rate,
        partitions=a.partitions,
        pool=a.pool,
        visit_events=a.visit_events,
    )
    sink = build_sink(a.sink, a.out)
    stats = run(cfg, sink)
    sink.close()
    if a.report:
        block = stats.summary()
        block["skew"] = partition_skew(stats, cfg.partitions)
        print(json.dumps(block, indent=2), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
