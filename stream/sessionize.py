"""Parse and watermark and dedupe and session windows.

This is the whole of day 3. Four steps and three of them have a trap in them.

The order is fixed and it is not arbitrary. The watermark has to be set before the
dedupe, because the dedupe uses it to decide when it may forget an event_id. And it
has to be set before the session grouping, because append mode has no other way to
know a session is finished.
"""

from pyspark.sql import DataFrame, functions as F

from stream.schema import EVENT_SCHEMA, cast_times

# Thirty minutes of inactivity ends a session. Standard, and it is a guess at the
# truth rather than the truth. docs/event-schema.md explains why they differ.
DEFAULT_GAP = "30 minutes"

# How late an event may be before the pipeline stops waiting for it. Day 2 measured
# the generator's injected lateness at a median of 12.07s and a p99 of 297s. The
# longest was 2494s. So this number looks like a decision about which end of that
# tail to pay for, and day 3 measured that it is not. See the README. It is a
# default rather than a recommendation. scripts/watermark_sweep.py makes it a choice.
DEFAULT_WATERMARK = "2 minutes"


def parse(raw: DataFrame, value_col: str = "value") -> DataFrame:
    """String payload to typed columns.

    `raw` is whatever the source produced with one column holding a JSON string. The
    file source and the Kafka source disagree about what that column is called and
    about nothing else, which is why the reader is the only thing that knows which
    source is in use.
    """
    return cast_times(
        raw.select(F.from_json(F.col(value_col).cast("string"), EVENT_SCHEMA).alias("e")).select("e.*")
    )


def watermark(df: DataFrame, delay: str = DEFAULT_WATERMARK) -> DataFrame:
    return df.withWatermark("event_ts", delay)


def dedupe(df: DataFrame) -> DataFrame:
    """Drop repeat event_ids, with bounded state.

    `dropDuplicates(["event_id"])` is what most examples use and it keeps every
    event_id it has ever seen, forever, because event_id is not the watermark column
    and Spark has nothing to expire the entry against. On a stream that never ends
    that is a memory leak with a plausible-looking line of code in front of it.

    `dropDuplicates(["event_id", "event_ts"])` does bound the state, and it only
    catches a copy carrying the same event time. A retry that gets re-stamped on the
    way through passes straight through it.

    `dropDuplicatesWithinWatermark` bounds the state on the watermark alone and does
    not care whether the copy's event time moved. That is the one that survives both
    cases, and it is 3.5 or newer only.

    Worth being clear about what today's data proves. The generator's copy carries
    the ORIGINAL event_ts and only re-stamps ingest_ts, so all three of these catch
    it. The case that separates them is not in the corpus.
    """
    return df.dropDuplicatesWithinWatermark(["event_id"])


def session_windows(df: DataFrame, gap: str = DEFAULT_GAP) -> DataFrame:
    """Collapse events into sessions, one row per user per session.

    Day 3 stops at boundaries and a count. Duration and page depth and bounce and
    the conversion flag are day 4. Putting them here would be building tomorrow's
    blueprint line on top of today's.

    There is deliberately no hook here for the scorer to hang a truth column on.
    scripts/score_sessions.py joins the raw events back onto these windows instead,
    so the thing being scored is the pipeline's real output and not a variant of it
    built with an extra argument.
    """
    return (
        df.groupBy(F.col("user_id"), F.session_window(F.col("event_ts"), gap).alias("sw"))
        .agg(F.count("*").alias("event_count"))
        .select(
            F.col("user_id"),
            F.col("sw.start").alias("session_start"),
            F.col("sw.end").alias("session_end"),
            F.col("event_count"),
        )
    )


def build_sessions(raw: DataFrame, gap: str = DEFAULT_GAP, delay: str = DEFAULT_WATERMARK) -> DataFrame:
    """The one door.

    Everything that turns raw payloads into sessions goes through here. ot-026 on the
    program side is about a rule that each caller has to remember to apply, and the
    answer that worked on the last project was to leave callers no second route in.
    The job, the tests and the scorer all call this.
    """
    return session_windows(dedupe(watermark(parse(raw), delay)), gap)
