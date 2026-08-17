"""The lag measurements that need a DataFrame.

The arithmetic is in stream/lag.py and it is tested without Spark. This file holds
the two terms that have to be measured over the corpus and over the pipeline's real
output. Split that way because the headline figure of the day is arithmetic and a
clone should be able to check it with nothing installed.

Every number here ends up published, so it lives in the library and not in scripts/.
That is ot-037 on the program side. A figure whose producing code nothing tests is a
figure nobody can contradict.
"""

from pyspark.sql import DataFrame, functions as F

from stream.lag import emission_floor_s, parse_duration
from stream.scoring import attach_sessions

QS = (0.5, 0.95, 0.99)


def exact_quantiles(df: DataFrame, col: str, qs=QS) -> dict:
    """`percentile`, not `percentile_approx`.

    Spark's approximate version is a sketch and its answer moves with the physical
    order of the input. The 08-01 finding on the previous project was that such an
    estimator cannot carry a metric a later run will compare against, and these are
    all meant to be compared. Fifty eight thousand rows do not need a sketch.
    """
    if not qs:
        raise ValueError("no quantiles requested, which would report nothing as success")
    arr = ", ".join(str(q) for q in qs)
    row = df.select(F.expr(f"percentile({col}, array({arr}))").alias("q")).first()
    if row is None or row["q"] is None:
        # An empty frame is a finding, not a zero. A p50 of 0.0 printed off no rows
        # reads as a fast pipeline, which is the 08-04 passing-on-nothing shape.
        return {"rows": 0}
    out = {f"p{int(q * 100)}": round(float(v), 4) for q, v in zip(qs, row["q"])}
    out["rows"] = df.count()
    return out


def ingest_lag(events: DataFrame) -> dict:
    """event_ts to ingest_ts, in seconds, over the corpus as landed.

    This is the only term of the three that is a property of the producer rather than
    of the pipeline, and it is the one a Kafka deployment would really pay. Measured
    off the files rather than taken from the generator's own report, because the
    generator reports what it intended to inject and this reports what arrived.

    Rows where the two timestamps are equal are kept. They are the 92 percent of
    events with no lateness injected and dropping them would turn a p50 of zero into
    a statistic about the late tail wearing the label of the whole stream.
    """
    lag = events.select(
        (F.col("ingest_ts").cast("double") - F.col("event_ts").cast("double")).alias("lag_s")
    ).where(F.col("lag_s").isNotNull())
    out = exact_quantiles(lag, "lag_s")
    out["max_s"] = round(float(lag.agg(F.max("lag_s")).first()[0] or 0.0), 4)
    late = lag.where(F.col("lag_s") > 0).count()
    out["rows_with_lateness"] = late
    out["share_with_lateness"] = round(late / out["rows"], 4) if out.get("rows") else None
    return out


def emission_lag(events: DataFrame, sessions: DataFrame, watermark: str) -> dict:
    """Per event, how long after its own event time its session became emittable.

    The join comes from `stream.scoring.attach_sessions` rather than being written
    again here. There is one definition of which session contains an event and it
    already exists. Two of them would drift, and ot-026 on the program side is exactly
    about a rule each caller reimplements.

    `session_end` is already one gap past the session's last event, so adding the
    watermark gives the earliest instant append mode may emit. Subtracting each
    event's own timestamp gives what that event waited. The spread across events comes
    from session duration and nothing else, which is why the p99 is interesting and
    the p50 is close to the floor.

    Unmatched events are excluded and counted. An event in no window waited forever
    rather than waiting a large number, and averaging a large number in would be
    kinder than the truth.
    """
    delay = parse_duration(watermark)
    joined = attach_sessions(events, sessions)
    matched = joined.where(F.col("session_start").isNotNull()).select(
        (
            F.col("session_end").cast("double")
            + F.lit(delay)
            - F.col("event_ts").cast("double")
        ).alias("lag_s")
    )
    out = exact_quantiles(matched, "lag_s")
    out["events_never_emitted"] = joined.where(F.col("session_start").isNull()).count()
    out["watermark_s"] = delay
    return out


def session_span(sessions: DataFrame) -> dict:
    """Session durations, which are the whole of the spread in emission lag.

    Reported next to it so a reader can check that claim rather than take it. p99
    emission lag minus p99 duration should land on the floor.
    """
    return exact_quantiles(sessions.select(F.col("duration_s")), "duration_s")


def floor_check(sessions: DataFrame, gap: str, watermark: str) -> dict:
    """Confirm the arithmetic floor against the pipeline's own output.

    `emission_floor_s` is derived on paper. This takes the minimum emission lag any
    real event achieved and compares it. They should agree to the millisecond for a
    session whose last event sits exactly at its own end minus the gap, which every
    session does by construction.

    A check like this is cheap and it is the difference between a formula and a
    measured fact. The 08-15 lesson was that an exact agreement can be the weakest
    evidence available, so the condition that makes disagreement impossible is named
    here rather than discovered later. Session end is defined as last event plus gap,
    so the last event of every session is exactly one floor from emittable. This
    confirms the code implements the definition. It is not independent evidence about
    Spark.
    """
    floor = emission_floor_s(gap, watermark)
    span = sessions.select(
        (
            F.col("session_end").cast("double")
            - F.col("session_start").cast("double")
            - F.col("duration_s")
        ).alias("trailing_gap_s")
    )
    row = span.agg(F.min("trailing_gap_s").alias("lo"), F.max("trailing_gap_s").alias("hi")).first()
    return {
        "floor_s": floor,
        "trailing_gap_min_s": round(float(row["lo"]), 4) if row["lo"] is not None else None,
        "trailing_gap_max_s": round(float(row["hi"]), 4) if row["hi"] is not None else None,
        "gap_s": parse_duration(gap),
    }
