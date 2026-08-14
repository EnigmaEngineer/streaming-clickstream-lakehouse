"""Everything the README quotes about the generator comes from here.

Run it and the numbers move. That is the point. A figure written into prose by hand
cannot be contradicted by a later change, and this program has been bitten by that
twice on another repo.

    python scripts/measure_generator.py
"""

import json
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from generator.clock import RealClock, VirtualClock  # noqa: E402
from generator.population import Population  # noqa: E402
from generator.produce import Config, partition_skew, run  # noqa: E402
from generator.sinks import MemorySink, NullSink  # noqa: E402


def ceiling_throughput(seconds: float = 2.0) -> dict:
    """How fast the generator goes with the pacer effectively switched off.

    A throwaway pass runs first. The first pass of a process pays for imports and for
    the population build, and charging that to the measurement is a mistake this
    program has made three times on other repos.
    """
    cfg = Config(rate=200000.0, seconds=0.2, users=5000, alpha=1.0)
    run(cfg, NullSink(), clock=VirtualClock(speedup=1e12))  # warmup, discarded

    runs = []
    for _ in range(5):
        cfg = Config(rate=200000.0, seconds=seconds, users=5000, alpha=1.0)
        sink = NullSink()
        t0 = time.monotonic()
        stats = run(cfg, sink, clock=VirtualClock(speedup=1e12))
        runs.append(stats.emitted / (time.monotonic() - t0))
    return {
        "events": int(200000 * seconds),
        "median_events_per_wall_second": int(statistics.median(runs)),
        "min": int(min(runs)),
        "max": int(max(runs)),
        "passes": len(runs),
    }


def rate_accuracy(targets=(50.0, 500.0, 5000.0), seconds: float = 2.0) -> list:
    """Ask for a rate at wall speed and see what comes out."""
    out = []
    for target in targets:
        cfg = Config(rate=target, seconds=seconds, users=5000, alpha=1.0)
        stats = run(cfg, NullSink(), clock=RealClock())
        achieved = stats.emitted / stats.wall_seconds
        out.append(
            {
                "target_per_s": target,
                "achieved_per_s": round(achieved, 1),
                "error_pct": round(100 * (achieved - target) / target, 2),
                "slept_s": round(stats.slept_seconds, 3),
                "wall_s": round(stats.wall_seconds, 3),
            }
        )
    return out


def skew_by_alpha(alphas=(0.0, 0.6, 1.0, 1.4), partitions: int = 6) -> list:
    out = []
    for alpha in alphas:
        cfg = Config(rate=2000.0, seconds=30.0, users=5000, alpha=alpha, seed=21)
        stats = run(cfg, NullSink(), clock=VirtualClock(speedup=1e12))
        s = partition_skew(stats, partitions)
        pop = Population(5000, alpha=alpha, seed=21)
        out.append(
            {
                "alpha": alpha,
                "events": stats.emitted,
                "distinct_users": stats.distinct_users,
                "top_50_user_share": round(pop.share_of_top(50), 4),
                "busiest_partition_over_even": s["max_over_even"],
                "quietest_partition_over_even": s["min_over_even"],
            }
        )
    return out


def lateness_profile(late_rate: float = 0.08) -> dict:
    cfg = Config(rate=2000.0, seconds=60.0, users=5000, alpha=1.0, late_rate=late_rate, seed=31)
    stats = run(cfg, NullSink(), clock=VirtualClock(speedup=1e12))
    s = sorted(stats.lateness_samples)
    over = lambda t: round(sum(1 for x in s if x > t) / len(s), 4)
    return {
        "events": stats.emitted,
        "late_events": stats.late_events,
        "realised_late_share": round(stats.late_events / stats.emitted, 4),
        "p50_s": round(s[len(s) // 2], 2),
        "p95_s": round(s[int(len(s) * 0.95)], 2),
        "p99_s": round(s[int(len(s) * 0.99)], 2),
        "max_s": round(s[-1], 2),
        "share_over_60s": over(60),
        "share_over_300s": over(300),
    }


def session_shape(pool: int = 150) -> dict:
    """What the session model actually produces. Day 3 scores against this.

    `gap_rule_misses` is the number that matters. The pipeline recovers sessions from
    a thirty minute inactivity gap. The generator knows the real boundary, because a
    visit either ended or it did not. Every miss is a pair of real sessions day 3 will
    report as one, and no amount of Spark tuning recovers them.
    """
    cfg = Config(
        rate=500.0, seconds=1800.0, users=2000, alpha=1.0, seed=41, dup_rate=0.0, pool=pool
    )
    sink = MemorySink()
    stats = run(cfg, sink, clock=VirtualClock(speedup=1e12))
    per_session: dict[str, int] = {}
    converted = set()
    types: dict[str, int] = {}
    for _, v in sink.records:
        per_session[v["session_hint"]] = per_session.get(v["session_hint"], 0) + 1
        types[v["event_type"]] = types.get(v["event_type"], 0) + 1
        if v["event_type"] == "checkout":
            converted.add(v["session_hint"])
    lengths = sorted(per_session.values())
    return {
        "events": stats.emitted,
        "concurrent_visits": pool,
        "visits_started": stats.visits_started,
        "mean_occupancy": stats.mean_occupancy,
        "admissions_deflected": stats.admissions_deflected,
        "sessions": len(per_session),
        "distinct_users": stats.distinct_users,
        "median_events_per_session": lengths[len(lengths) // 2],
        "p95_events_per_session": lengths[int(len(lengths) * 0.95)],
        "single_event_sessions_pct": round(100 * sum(1 for x in lengths if x == 1) / len(lengths), 1),
        "converting_sessions_pct": round(100 * len(converted) / len(per_session), 2),
        "gap_rule_misses": stats.boundaries_gap_rule_misses,
        "gap_rule_miss_pct_of_sessions": round(
            100 * stats.boundaries_gap_rule_misses / len(per_session), 2
        ),
        "event_type_mix": {k: round(v / stats.emitted, 4) for k, v in sorted(types.items())},
    }


def gap_rule_by_population(arms=((2000, 1.0), (20000, 1.0), (100000, 1.0), (300000, 1.0), (300000, 0.0))) -> list:
    """The thirty minute rule only works when users are scarce relative to traffic.

    The obvious prediction is that a visitor comes back after about
    `users * visit_events / rate` seconds, because that is how long the pool takes to
    work through everybody else. That expression is in the table and it is WRONG,
    which is why it is labelled `uniform` and reported next to the measurement rather
    than instead of it. It holds only when every user is equally likely.

    Under a heavy tail the average return gap is not the one that matters. Most visits
    belong to the busiest few users, and those people come back in seconds no matter
    how large the population is. The last arm is the control. Same 300,000 users at
    alpha 0, where the prediction is supposed to hold.
    """
    rate, seconds, visit_events, pool = 500.0, 600.0, 6.0, 150
    out = []
    for users, alpha in arms:
        cfg = Config(
            rate=rate,
            seconds=seconds,
            users=users,
            alpha=alpha,
            seed=51,
            dup_rate=0.0,
            pool=pool,
            visit_events=visit_events,
        )
        stats = run(cfg, NullSink(), clock=VirtualClock(speedup=1e12))
        out.append(
            {
                "users": users,
                "alpha": alpha,
                "uniform_predicted_return_gap_s": round(users * visit_events / rate, 1),
                "sessions": stats.sessions_started,
                "gap_rule_misses": stats.boundaries_gap_rule_misses,
                "gap_rule_miss_pct": round(
                    100 * stats.boundaries_gap_rule_misses / stats.sessions_started, 2
                ),
            }
        )
    return out


def main() -> int:
    report = {
        "measured_on": time.strftime("%Y-%m-%d"),
        "python": sys.version.split()[0],
        "ceiling_throughput": ceiling_throughput(),
        "rate_accuracy": rate_accuracy(),
        "skew_by_alpha": skew_by_alpha(),
        "lateness_profile": lateness_profile(),
        "session_shape": session_shape(),
        # A denser site means a user comes back sooner, so the gap rule misses more.
        "session_shape_busier_site": session_shape(pool=600),
        "gap_rule_by_population": gap_rule_by_population(),
    }
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
