"""Where the end to end lag goes, as arithmetic.

The blueprint asks for a dashboard showing p50 and p95 event to query lag. Taking
that literally on this repo would produce a lie, and it is worth saying why before
any number appears.

The corpus is a replay. Every event carries a timestamp the generator chose, and the
job reads the whole set minutes after the files were written. So `now() - event_ts`
is the age of the corpus. A dashboard publishing it would be reporting how long ago
the generator ran, in the voice of a service level objective. That is the 07-28 rule
from the previous project. Do not implement a metric whose subject the repo does not
have.

What is real here is the lag the design imposes, and it decomposes into three terms.

    ingest lag       event_ts to ingest_ts. The generator's injected lateness.
    emission delay   how long append mode makes a finished session wait.
    processing       what Spark spent, read off the query progress.

This module holds the parts that are arithmetic over numbers already in hand. No
Spark, so `tests.run_all` covers it and a clone with nothing installed can check the
headline figure. The DataFrame measurements live in stream/latency.py, which needs
the 300 MB install.
"""

_UNITS = {
    "millisecond": 0.001,
    "milliseconds": 0.001,
    "ms": 0.001,
    "second": 1.0,
    "seconds": 1.0,
    "minute": 60.0,
    "minutes": 60.0,
    "hour": 3600.0,
    "hours": 3600.0,
    "day": 86400.0,
    "days": 86400.0,
}


def parse_duration(text: str) -> float:
    """A Spark interval string to seconds.

    Deliberately strict. A bare number raises rather than defaulting to seconds,
    because the whole point of this module is that one number decides the headline
    and a silent unit guess is how it would be wrong by sixty.
    """
    if not isinstance(text, str):
        raise TypeError(f"duration must be a string, got {type(text).__name__}")
    parts = text.strip().split()
    if len(parts) != 2:
        raise ValueError(f"cannot read {text!r} as a duration, want a number and a unit")
    number, unit = parts
    if unit.lower() not in _UNITS:
        raise ValueError(f"unknown duration unit {unit!r} in {text!r}")
    return float(number) * _UNITS[unit.lower()]


def emission_floor_s(gap: str, watermark: str) -> float:
    """The soonest append mode may emit a session, measured from its last event.

    A session window ends one gap after its last event. Append mode emits a window
    once the watermark has passed the window end, and the watermark trails the newest
    event time by the delay. So emission needs the stream's newest event time to reach
    `last_event + gap + watermark`.

    On a live stream, where event time tracks the clock, that is wall time and this is
    a floor nothing can go under. Not a tuning target. Not a measurement of this
    machine. It is what the operator's own definition costs, and at the repo defaults
    it is 1,920 seconds.
    """
    return parse_duration(gap) + parse_duration(watermark)


def first_event_floor_s(gap: str, watermark: str, session_duration_s: float) -> float:
    """The same floor for the FIRST event of a session rather than the last.

    The first event waits out the whole session on top of the structural delay. So
    the floor is a property of the session and not a constant, and reporting only the
    constant would understate every session longer than zero seconds. Both go in the
    budget.
    """
    if session_duration_s < 0:
        raise ValueError(f"session duration cannot be negative, got {session_duration_s}")
    return emission_floor_s(gap, watermark) + float(session_duration_s)


def quantiles(values, qs=(0.5, 0.95, 0.99)) -> dict:
    """Exact quantiles over a list, linearly interpolated.

    Two things about this are deliberate.

    No sketch. The 08-01 lesson on the previous project was that an estimator whose
    answer depends on the order rows arrive in cannot carry a number a later run will
    compare against, and every figure here is meant to be compared.

    Interpolated, and specifically interpolated the way Spark's `percentile` does it,
    which is `p * (n - 1)` and then a linear blend of the two neighbours. This started
    out nearest rank and that was a real defect rather than a style choice.
    `lag_budget` adds a p50 measured here to a p50 measured by Spark, and two
    definitions of the median summed in one line is a silent inconsistency nobody
    would ever look for. tests/test_latency.py pushes identical values through both
    and asserts they agree.

    An empty input returns an empty dict rather than zeros. A zero here would read as
    a fast pipeline.
    """
    xs = sorted(float(v) for v in values)
    if not xs:
        return {}
    out = {}
    for q in qs:
        if not 0.0 < q <= 1.0:
            raise ValueError(f"quantile must be in (0, 1], got {q}")
        pos = q * (len(xs) - 1)
        lo = int(pos)
        hi = min(lo + 1, len(xs) - 1)
        out[f"p{int(q * 100)}"] = xs[lo] + (pos - lo) * (xs[hi] - xs[lo])
    return out


def batch_latency(progress) -> dict:
    """What Spark spent, off `query.recentProgress`.

    `triggerExecution` is the whole batch including the wait for input, and `addBatch`
    is the part that ran the plan. Both are reported. Summing triggerExecution across
    a bounded run and calling it throughput would charge the run for the time Spark
    spent finding out there were no more files.

    The first batch is kept and reported separately rather than discarded. The 08-02
    lesson is that whether a first observation is an outlier depends on the task, and
    the 08-12 one is that a warmup charged to whichever arm ran first is how a
    comparison gets rigged. Here there is one arm, so the honest move is to publish
    both the first batch and the rest.
    """
    batches = list(progress or [])
    if not batches:
        return {"batches": 0}
    trigger, add, rows = [], [], []
    for b in batches:
        d = b.get("durationMs", {}) or {}
        trigger.append(float(d.get("triggerExecution", 0)))
        add.append(float(d.get("addBatch", 0)))
        rows.append(int(b.get("numInputRows", 0)))
    total_ms = sum(trigger)
    total_rows = sum(rows)
    return {
        "batches": len(batches),
        "input_rows": total_rows,
        "trigger_total_ms": round(total_ms, 1),
        "trigger_first_ms": round(trigger[0], 1),
        "trigger_rest_ms": quantiles(trigger[1:]) if len(trigger) > 1 else {},
        "add_batch_ms": quantiles(add),
        "rows_per_second": round(total_rows / (total_ms / 1000.0), 1) if total_ms else None,
    }


def lag_budget(gap: str, watermark: str, ingest_p50_s: float, processing_p50_ms: float) -> dict:
    """The three terms side by side, with each one's share of the total.

    This is the whole point of day 5. The two terms anyone would tune are the two that
    do not matter, and the term nobody thinks of as latency is the entire number.

    `processing_p50_ms` is a per batch figure and it is charged here as if one batch
    carried one session, which flatters it. Even flattered it does not register.
    """
    floor = emission_floor_s(gap, watermark)
    proc = float(processing_p50_ms) / 1000.0
    ingest = float(ingest_p50_s)
    total = floor + proc + ingest
    terms = {"ingest_lag_s": ingest, "emission_delay_s": floor, "processing_s": proc}
    return {
        "terms": {k: round(v, 4) for k, v in terms.items()},
        "share": {k: round(v / total, 6) for k, v in terms.items()},
        "total_s": round(total, 4),
        "goal_s": 60.0,
        "meets_goal": total <= 60.0,
        # The blueprint's own goal line is "under a minute of end to end lag". The
        # ratio is how far the design it also specifies is from that, and it is a
        # fact about the two lines contradicting each other rather than about Spark.
        "over_goal_by": round(total / 60.0, 2),
    }
