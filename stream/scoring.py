"""Score recovered sessions against the generator's ground truth.

`session_hint` is what really happened. It increments when a visit starts, which is
a fact the generator knows and the pipeline cannot see. The pipeline guesses at the
same thing with a thirty minute inactivity gap. This measures the distance between
them.

Nothing in stream/sessionize.py knows this module exists. The scorer joins the raw
events back onto the sessions the pipeline emitted rather than asking the pipeline
to carry a truth column, because a pipeline with a measurement flag on it is not the
pipeline being measured.

This lives in the library rather than in scripts/ on purpose. Every number it
produces ends up published, and ot-037 on the program side is about figures whose
producing code nothing tests. tests/test_scoring.py runs against it.
"""

from pyspark.sql import DataFrame, functions as F


def attach_sessions(events: DataFrame, sessions: DataFrame) -> DataFrame:
    """Put each raw event inside the recovered session that contains it.

    Session windows for one user never overlap, because a window ends one gap after
    its last event and the next window starts later than that. So this join is one
    row in and one row out. A second match would mean the pipeline emitted
    overlapping windows, which is worth failing on rather than quietly summing.
    """
    e = events.alias("e")
    s = sessions.alias("s")
    cond = (
        (F.col("e.user_id") == F.col("s.user_id"))
        & (F.col("e.event_ts") >= F.col("s.session_start"))
        & (F.col("e.event_ts") < F.col("s.session_end"))
    )
    return e.join(s, cond, "left").select(
        F.col("e.event_id"),
        F.col("e.user_id"),
        F.col("e.event_ts"),
        F.col("e.session_hint"),
        F.col("s.session_start"),
        F.col("s.session_end"),
    )


def score(events: DataFrame, sessions: DataFrame) -> dict:
    """The comparison.

    Two ways to be wrong and they are not symmetric.

    A MERGE is two real visits landing in one recovered session. That is the failure
    the thirty minute rule makes on a heavy tailed population, because a busy user
    comes back before the gap opens. It is counted as boundaries lost, which is the
    number of extra visits sharing a recovered session.

    A SPLIT is one real visit coming out as two recovered sessions. It needs a gap
    of over thirty minutes inside a single visit, so it should be rare here and its
    being rare is a claim worth checking rather than assuming.
    """
    joined = attach_sessions(events, sessions).cache()
    total_events = joined.count()
    unmatched = joined.where(F.col("session_start").isNull()).count()
    matched = joined.where(F.col("session_start").isNotNull())

    true_sessions = events.select("session_hint").distinct().count()
    recovered = sessions.count()

    per_recovered = matched.groupBy("user_id", "session_start").agg(
        F.countDistinct("session_hint").alias("hints")
    )
    merged_sessions = per_recovered.where(F.col("hints") > 1).count()
    boundaries_lost = per_recovered.agg(F.sum(F.col("hints") - 1)).first()[0] or 0

    # Distinct on the pair and not on the start. Two users can open a session at the
    # same instant, and counting starts alone scored that as one window. The
    # generator embeds the user id in session_hint so it never bit on real data,
    # which is exactly why it needed a fixture to find it.
    per_hint = matched.groupBy("session_hint").agg(
        F.countDistinct(F.struct("user_id", "session_start")).alias("windows")
    )
    split_hints = per_hint.where(F.col("windows") > 1).count()
    hints_scored = per_hint.count()

    joined.unpersist()
    return {
        "events_joined": total_events,
        "events_unmatched": unmatched,
        "true_sessions_in_corpus": true_sessions,
        "true_sessions_scored": hints_scored,
        "recovered_sessions": recovered,
        "merged_recovered_sessions": merged_sessions,
        "boundaries_lost_to_merge": int(boundaries_lost),
        "boundary_miss_rate": round(boundaries_lost / hints_scored, 4) if hints_scored else 0.0,
        "split_true_sessions": split_hints,
        "split_rate": round(split_hints / hints_scored, 4) if hints_scored else 0.0,
    }


def truth_features(events: DataFrame) -> DataFrame:
    """The same four features, computed per real visit instead of per recovered one.

    Deduplicated on `event_id` first. The corpus carries planted duplicates and a
    truth side that counted them would report a higher event count than really
    happened, which would flatter the pipeline by shrinking the gap this function
    exists to measure.
    """
    deduped = events.dropDuplicates(["event_id"])
    agg = deduped.groupBy("session_hint").agg(
        F.count("*").alias("event_count"),
        F.size(F.collect_set("page")).alias("page_depth"),
        (F.max("event_ts").cast("double") - F.min("event_ts").cast("double")).alias("duration_s"),
        F.max(F.when(F.col("event_type") == "checkout", 1).otherwise(0)).alias("converted"),
    )
    return agg.withColumn(
        "bounce", F.when((F.col("page_depth") == 1) & (F.col("event_count") == 1), 1).otherwise(0)
    )


FEATURES = ["converted", "bounce", "event_count", "page_depth", "duration_s"]


def feature_bias(events: DataFrame, sessions: DataFrame) -> dict:
    """How far the recovered features sit from the real ones.

    Day 3 measured the boundary miss rate and left it as a number about
    sessionization. This is what that number does to the columns anyone would
    actually put on a dashboard. A merged session inherits the union of two visits,
    so a conversion anywhere in the pair marks the whole thing and a bounce in either
    half stops being a bounce.

    Reported as truth, recovered and the ratio. The ratio is the number worth
    quoting, since the levels are properties of generator/session.py and the
    distortion is a property of the thirty minute rule.
    """
    truth = truth_features(events)
    out: dict = {}
    t = truth.agg(*[F.avg(c).alias(c) for c in FEATURES]).first()
    r = sessions.agg(*[F.avg(c).alias(c) for c in FEATURES]).first()
    for c in FEATURES:
        tv, rv = float(t[c]), float(r[c])
        out[c] = {
            "truth": round(tv, 4),
            "recovered": round(rv, 4),
            "ratio": round(rv / tv, 3) if tv else None,
        }
    out["true_sessions"] = truth.count()
    out["recovered_sessions"] = sessions.count()
    return out
