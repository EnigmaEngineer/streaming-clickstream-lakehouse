"""Checks for the streaming pipeline.

These need pyspark and they are slow, so they live behind tests/run_spark.py rather
than in the standard library suite. Nothing here is skipped when pyspark is missing.
It fails, loudly, because a suite that reports a pass on zero executed checks is the
failure mode I keep finding in my own tooling.

Every fixture below is built so the rule under test has something to choose between.
A session fixture with one event per user cannot test a boundary rule, and a dedupe
fixture with no repeat cannot test a dedupe. That lesson cost three surviving mutants
on another project of mine.
"""

import json
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import functions as F

from stream.job import read_file_source, spark_session
from stream.schema import cast_times, parse_failures
from stream.sessionize import build_sessions, dedupe, parse, session_windows, watermark

BASE = datetime(2026, 8, 15, 12, 0, 0, tzinfo=timezone.utc)
FLUSH_USER = "u_flushT"
_spark = None


def spark():
    global _spark
    if _spark is None:
        _spark = spark_session(app="tests", shuffle_partitions=2)
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


def iso(offset_s: float) -> str:
    t = BASE + timedelta(seconds=offset_s)
    return t.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def record(event_id, user_id, at_s, hint="s_x_001", late_s=0.0, page="/", event_type="page_view"):
    return {
        "event_id": event_id,
        "user_id": user_id,
        "session_hint": hint,
        "event_type": event_type,
        "page": page,
        "referrer": None,
        "device": "mobile",
        "country": "US",
        "event_ts": iso(at_s),
        "ingest_ts": iso(at_s + late_s),
    }


def as_raw(rows):
    """Rows as the file source would hand them over, one JSON string per line."""
    return spark().createDataFrame([(json.dumps(r),) for r in rows], "value string")


def written(rows, name, flush=True):
    """Write the fixture as the file source expects it.

    `flush` appends a sentinel event days after everything else, in its own shard so
    it lands in the last micro batch. Without it a bounded run emits nothing at all,
    because append mode holds a session window until the watermark passes its end
    and a run that has stopped reading files never advances the watermark again.
    The first version of these checks had no sentinel and every streaming check
    asserted against an empty list.
    """
    d = Path(tempfile.mkdtemp(prefix=f"clk-{name}-"))
    with open(d / "shard-00.json", "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")
    if flush:
        with open(d / "zzz-flush.json", "w", encoding="utf-8") as fh:
            fh.write(json.dumps(record("flush", FLUSH_USER, 86400 * 3)) + "\n")
    return d


def streaming_raw(rows, name, flush=True):
    """A real streaming DataFrame. `withWatermark` is a no op on a batch DataFrame
    and Spark drops it from the plan, so anything asserting on watermark or dedupe
    behaviour has to come through here."""
    return read_file_source(spark(), str(written(rows, name, flush)), 1)


def stream_sessions(rows, name, gap="30 minutes", delay="2 minutes"):
    """Run the real pipeline over a real streaming source and return the sessions,
    with the sentinel's own session removed."""
    src = written(rows, name)
    out = Path(tempfile.mkdtemp(prefix=f"clk-out-{name}-")) / "sessions"
    ckpt = Path(tempfile.mkdtemp(prefix=f"clk-ck-{name}-")) / "ckpt"
    raw = read_file_source(spark(), str(src), 1)
    q = (
        build_sessions(raw, gap=gap, delay=delay)
        .writeStream.outputMode("append")
        .format("parquet")
        .option("path", str(out))
        .option("checkpointLocation", str(ckpt))
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
    df = spark().read.parquet(str(out)).where(F.col("user_id") != FLUSH_USER).collect()
    shutil.rmtree(src, ignore_errors=True)
    return df


def feat(rows):
    """Sessions with features, over a batch source.

    build_sessions cannot be used here. dropDuplicatesWithinWatermark is refused on a
    batch DataFrame, and the dedupe is not what these checks are about. The one check
    that proves the features survive the real streaming path is
    check_features_survive_the_streaming_path below.
    """
    return session_windows(watermark(parse(as_raw(rows))), "30 minutes")


def check_parse_types_the_two_timestamps():
    df = parse(as_raw([record("a", "u_1", 0)]))
    types = dict(df.dtypes)
    assert types["event_ts"] == "timestamp", types
    assert types["ingest_ts"] == "timestamp", types
    assert types["event_id"] == "string", types


def check_lateness_is_ingest_minus_event():
    df = parse(as_raw([record("a", "u_1", 0, late_s=7.5)]))
    got = df.select("lateness_s").first()[0]
    assert abs(got - 7.5) < 1e-6, got


def check_a_bad_timestamp_becomes_null_and_is_not_dropped():
    bad = record("a", "u_1", 0)
    bad["event_ts"] = "15/08/2026 12:00:00"
    df = parse(as_raw([bad, record("b", "u_1", 1)]))
    assert df.count() == 2, "a row with an unreadable timestamp must be kept, not dropped"
    assert parse_failures(df).count() == 1


def check_the_declared_timestamp_format_is_actually_enforced():
    """TS_FORMAT names milliseconds. A generator that stopped writing them would be
    a silent producer change, so it should come back as a parse failure rather than
    as a timestamp Spark guessed at.

    The first version of this check asserted that casting an already cast timestamp
    yields null, on the theory that it would catch a second parse site. It does not.
    to_timestamp on a timestamp column succeeds and returns it unchanged, so that
    check passed for a reason unrelated to what it claimed and it was removed.
    """
    coarse = record("a", "u_1", 0)
    coarse["event_ts"] = "2026-08-15T12:00:00Z"
    df = parse(as_raw([coarse]))
    assert parse_failures(df).count() == 1, df.collect()


def check_two_events_inside_the_gap_are_one_session():
    rows = [record("a", "u_1", 0), record("b", "u_1", 600)]
    df = watermark(parse(as_raw(rows)))
    out = session_windows(df, "30 minutes").collect()
    assert len(out) == 1, out
    assert out[0]["event_count"] == 2, out


def check_a_gap_wider_than_the_threshold_splits_the_session():
    rows = [record("a", "u_1", 0), record("b", "u_1", 1801)]
    df = watermark(parse(as_raw(rows)))
    out = session_windows(df, "30 minutes").collect()
    assert len(out) == 2, out
    assert sorted(r["event_count"] for r in out) == [1, 1], out


def check_the_gap_boundary_is_exclusive_not_inclusive():
    """Exactly one gap apart is still one session. One second more is two. Without
    both halves a rule that is off by one in either direction passes."""
    same = session_windows(watermark(parse(as_raw([record("a", "u_1", 0), record("b", "u_1", 1800)]))), "30 minutes")
    apart = session_windows(watermark(parse(as_raw([record("a", "u_1", 0), record("b", "u_1", 1801)]))), "30 minutes")
    assert same.count() == 1, "1800s apart should merge"
    assert apart.count() == 2, "1801s apart should split"


def check_two_users_never_share_a_session():
    rows = [record("a", "u_1", 0), record("b", "u_2", 10)]
    out = session_windows(watermark(parse(as_raw(rows))), "30 minutes").collect()
    assert len(out) == 2, out
    assert {r["user_id"] for r in out} == {"u_1", "u_2"}, out


def check_out_of_order_events_still_land_in_one_session():
    """The generator emits an event whose event_ts is behind one already sent. A
    session window is defined on event time and must not care about arrival order."""
    rows = [record("a", "u_1", 600), record("b", "u_1", 0), record("c", "u_1", 300)]
    out = session_windows(watermark(parse(as_raw(rows))), "30 minutes").collect()
    assert len(out) == 1, out
    assert out[0]["event_count"] == 3, out


def check_a_repeated_event_id_is_counted_once():
    """The fixture carries a real collision. Two rows share event_id and a third does
    not, so a dedupe that drops everything and a dedupe that drops nothing both fail."""
    rows = [record("a", "u_1", 0), record("a", "u_1", 0), record("b", "u_1", 60)]
    out = stream_sessions(rows, "dedupe")
    assert len(out) == 1, out
    assert out[0]["event_count"] == 2, f"expected the copy to be dropped, got {out}"


def check_dedupe_keeps_the_same_id_from_a_different_user_apart():
    """event_id is globally unique in the generator. This guards against the dedupe
    key quietly growing a user id, which would stop it catching the duplicate the
    producer actually emits."""
    rows = [record("a", "u_1", 0), record("a", "u_2", 0)]
    out = stream_sessions(rows, "dedupe-users")
    assert sum(r["event_count"] for r in out) == 1, (
        f"one event_id means one event no matter which user carried it, got {out}"
    )


def check_build_sessions_is_the_same_as_the_steps_in_order():
    """The one door. If someone reorders build_sessions so the dedupe runs before the
    watermark, this is what notices."""
    rows = [record("a", "u_1", 0), record("b", "u_1", 60)]
    composed = build_sessions(streaming_raw(rows, "plan", flush=False))
    plan = composed._jdf.queryExecution().analyzed().toString()
    assert "EventTimeWatermark" in plan, plan[:400]
    # The plan prints leaves last, so the operator applied first appears lower down.
    # The watermark must sit below the dedupe, because the dedupe expires its state
    # against it.
    assert plan.index("DeduplicateWithinWatermark") < plan.index("EventTimeWatermark"), plan[:600]


def check_session_output_carries_no_truth_column():
    """session_hint is ground truth and the pipeline must never emit it. If it did,
    the scorer would be grading a pipeline that had been handed the answer.

    The assertion is on the exact set rather than on the absence of one name, and it
    earned that. The first draft of the feature list carried a
    `distinct_hints` column, which is a count of the truth column and would have
    walked straight past an `assert "session_hint" not in cols`. This is also why the
    set is written out rather than derived from SESSION_COLUMNS. Deriving it would
    make the two agree by construction and check nothing.
    """
    cols = set(build_sessions(as_raw([record("a", "u_1", 0)])).columns)
    assert cols == {
        "user_id",
        "session_start",
        "session_end",
        "event_count",
        "page_depth",
        "duration_s",
        "converted",
        "bounce",
        "window_span_s",
    }, cols
    assert not any("hint" in c for c in cols), cols


def check_session_end_is_one_gap_past_the_last_event():
    rows = [record("a", "u_1", 0), record("b", "u_1", 120)]
    out = session_windows(watermark(parse(as_raw(rows))), "30 minutes").collect()[0]
    span = (out["session_end"] - out["session_start"]).total_seconds()
    assert abs(span - (120 + 1800)) < 1e-6, span


def check_duration_is_the_event_span_and_not_the_window_span():
    """The easiest wrong number in the project. A session window ends one gap after
    its last event, so the window span carries the whole gap on every row."""
    rows = [record("a", "u_1", 0), record("b", "u_1", 120)]
    out = feat(rows).collect()[0]
    assert abs(out["duration_s"] - 120.0) < 1e-6, out["duration_s"]
    assert abs(out["window_span_s"] - 1920.0) < 1e-6, out["window_span_s"]
    # And the difference is exactly the gap, which is what makes it so easy to miss.
    assert abs((out["window_span_s"] - out["duration_s"]) - 1800.0) < 1e-6, out


def check_page_depth_counts_distinct_pages_not_events():
    rows = [
        record("a", "u_1", 0, page="/"),
        record("b", "u_1", 10, page="/"),
        record("c", "u_1", 20, page="/p/1"),
    ]
    out = feat(rows).collect()[0]
    assert out["event_count"] == 3, out
    assert out["page_depth"] == 2, out


def check_a_single_event_on_a_single_page_is_a_bounce():
    out = feat([record("a", "u_1", 0, page="/")]).collect()[0]
    assert out["bounce"] == 1, out


def check_two_events_on_one_page_is_not_a_bounce():
    """The control for the rule above. Without it the bounce flag could be reading
    page_depth alone, and both halves of the rule would look enforced."""
    rows = [record("a", "u_1", 0, page="/"), record("b", "u_1", 5, page="/")]
    out = feat(rows).collect()[0]
    assert out["page_depth"] == 1 and out["event_count"] == 2, out
    assert out["bounce"] == 0, out


def check_one_event_on_one_page_of_two_users_does_not_collapse():
    rows = [record("a", "u_1", 0, page="/"), record("b", "u_2", 0, page="/")]
    out = feat(rows).collect()
    assert len(out) == 2, out
    assert all(r["bounce"] == 1 for r in out), out


def check_conversion_is_set_by_any_checkout_in_the_session():
    rows = [
        record("a", "u_1", 0, event_type="page_view"),
        record("b", "u_1", 30, event_type="checkout"),
        record("c", "u_1", 60, event_type="click"),
    ]
    out = feat(rows).collect()[0]
    assert out["converted"] == 1, out


def check_a_session_with_no_checkout_is_not_converted():
    rows = [record("a", "u_1", 0, event_type="page_view"), record("b", "u_1", 30, event_type="click")]
    out = feat(rows).collect()[0]
    assert out["converted"] == 0, out


def check_features_survive_the_streaming_path():
    """The batch helper above skips the dedupe, so one check has to run the real
    thing. A feature that only works when the dedupe is absent would otherwise ship."""
    rows = [
        record("a", "u_1", 0, page="/", event_type="page_view"),
        record("a", "u_1", 0, page="/", event_type="page_view"),
        record("b", "u_1", 90, page="/checkout", event_type="checkout"),
    ]
    out = stream_sessions(rows, "features")
    assert len(out) == 1, out
    row = out[0]
    assert row["event_count"] == 2, row
    assert row["page_depth"] == 2, row
    assert row["converted"] == 1, row
    assert abs(row["duration_s"] - 90.0) < 1e-6, row
    assert row["bounce"] == 0, row
