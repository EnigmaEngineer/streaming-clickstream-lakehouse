"""Checks for the lag measurements that need a DataFrame.

Hand built fixtures with an answer worked out by hand. Measuring the measurer against
the pipeline would be comparing two things built the same way, and the 08-12 lesson is
that such a comparison cannot catch a defect they both carry.
"""

from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F

from stream.latency import emission_lag, exact_quantiles, floor_check, ingest_lag, session_span

from tests.test_sessionize import spark

BASE = datetime(2026, 8, 17, 12, 0, 0, tzinfo=timezone.utc)


def at(s):
    return BASE + timedelta(seconds=s)


def events(rows):
    """An event id and a user id. Then an event offset and an ingest offset and a hint."""
    return spark().createDataFrame(
        [(e, u, at(ev), at(ing), h) for e, u, ev, ing, h in rows],
        "event_id string, user_id string, event_ts timestamp, ingest_ts timestamp, "
        "session_hint string",
    )


def sessions(rows):
    """A user id and a start offset. Then an end offset and a duration in seconds."""
    return spark().createDataFrame(
        [(u, at(a), at(b), float(d)) for u, a, b, d in rows],
        "user_id string, session_start timestamp, session_end timestamp, duration_s double",
    )


def check_exact_quantiles_over_a_known_column():
    df = spark().createDataFrame([(float(x),) for x in range(1, 101)], "v double")
    got = exact_quantiles(df, "v")
    assert got["p50"] == 50.5, got
    assert got["rows"] == 100, got


def check_the_python_and_spark_quantiles_agree_on_the_same_values():
    """The two implementations meet inside `lag_budget`, which adds a p50 Spark
    measured to a p50 Python measured. Until this check existed one was nearest rank
    and the other interpolated, and that sum was quietly mixing two definitions of the
    median. This is the 08-07 rule. When one module compares results two ways, assert
    that the two ways agree.

    Fixtures chosen so interpolation actually bites. An even count and a list whose
    quantiles fall between values, because a nearest rank implementation agrees with
    an interpolated one on any input where the answer lands on a value.
    """
    from stream.lag import quantiles as py_quantiles  # noqa: PLC0415

    for values in ([100.0, 300.0], [0.0, 10.0, 25.0, 40.0], [1.5, 2.5, 9.0]):
        df = spark().createDataFrame([(v,) for v in values], "v double")
        spark_side = exact_quantiles(df, "v", qs=(0.5, 0.95))
        py_side = py_quantiles(values, qs=(0.5, 0.95))
        for key in ("p50", "p95"):
            assert abs(spark_side[key] - round(py_side[key], 4)) < 1e-6, (
                values,
                key,
                spark_side,
                py_side,
            )


def check_exact_quantiles_on_an_empty_frame_report_zero_rows_not_zero_lag():
    """The 08-04 passing-on-nothing shape. A p50 of 0.0 off no rows would be published
    as a fast pipeline, so the absence has to be visible."""
    df = spark().createDataFrame([(1.0,)], "v double").where(F.col("v") < 0)
    got = exact_quantiles(df, "v")
    assert got == {"rows": 0}, got


def check_asking_for_no_quantiles_is_refused():
    df = spark().createDataFrame([(1.0,)], "v double")
    try:
        exact_quantiles(df, "v", qs=())
    except ValueError:
        return
    raise AssertionError("an empty quantile list should not have reported success")


def check_ingest_lag_measures_the_gap_between_the_two_timestamps():
    """Two on time events and two late ones. The on time rows stay in, because
    dropping them turns a p50 of zero into a statistic about the late tail only."""
    e = events(
        [
            ("a", "u_1", 0, 0, "h1"),
            ("b", "u_1", 10, 15, "h1"),
            ("c", "u_1", 20, 25, "h1"),
            ("d", "u_1", 30, 130, "h1"),
        ]
    )
    got = ingest_lag(e)
    assert got["rows"] == 4, got
    assert got["max_s"] == 100.0, got
    assert got["rows_with_lateness"] == 3, got
    assert got["share_with_lateness"] == 0.75, got
    # Lags are 0 and 5 and 5 and 100. The two middle values are equal so the median is
    # 5 under either quantile definition, which is what this fixture wants. The first
    # version used a zero in place of the second 5 and still expected 5. That is the
    # nearest rank answer and it is not Spark's.
    assert got["p50"] == 5.0, got


def check_emission_lag_of_the_last_event_is_the_floor():
    """One session, one event. session_end is one gap past the event, so the event's
    wait is exactly gap plus watermark and nothing else."""
    e = events([("a", "u_1", 0, 0, "h1")])
    s = sessions([("u_1", 0, 1800, 0.0)])
    got = emission_lag(e, s, "2 minutes")
    assert got["rows"] == 1, got
    assert got["p50"] == 1920.0, got
    assert got["watermark_s"] == 120.0, got


def check_the_first_event_of_a_long_session_waits_the_session_out_too():
    """Two events 600 seconds apart. The later one waits the floor and the earlier one
    waits the floor plus 600. A model that only reported the constant would score both
    at 1920 and the p99 would be meaningless."""
    e = events([("a", "u_1", 0, 0, "h1"), ("b", "u_1", 600, 600, "h1")])
    s = sessions([("u_1", 0, 2400, 600.0)])
    got = emission_lag(e, s, "2 minutes")
    # The two lags are 2520 for the first event and 1920 for the last. Their
    # interpolated median is 2220, which is the assertion worth making, because it can
    # only come out right if both values are present and different. A model that
    # reported the constant floor for every event would give 1920 here.
    assert got["p50"] == 2220.0, got
    assert got["p99"] == 2514.0, got


def check_the_watermark_really_moves_the_emission_lag():
    """Without this the watermark term could be dropped from the sum and every check
    above would still pass, since 1920 also happens to be reachable from the gap and a
    two minute session."""
    e = events([("a", "u_1", 0, 0, "h1")])
    s = sessions([("u_1", 0, 1800, 0.0)])
    assert emission_lag(e, s, "10 seconds")["p50"] == 1810.0
    assert emission_lag(e, s, "30 minutes")["p50"] == 3600.0


def check_an_event_in_no_session_is_counted_not_averaged_in():
    """An event no window ever claimed waited forever. Folding a large finite number
    into the quantiles would be kinder than the truth."""
    e = events([("a", "u_1", 0, 0, "h1"), ("b", "u_1", 99999, 99999, "h2")])
    s = sessions([("u_1", 0, 1800, 0.0)])
    got = emission_lag(e, s, "2 minutes")
    assert got["events_never_emitted"] == 1, got
    assert got["rows"] == 1, got
    assert got["p99"] == 1920.0, got


def check_session_span_reads_the_duration_column():
    """Three sessions rather than two, so the median lands on a value and does not
    depend on how the quantile interpolates."""
    s = sessions([("u_1", 0, 1800, 10.0), ("u_2", 0, 1800, 50.0), ("u_3", 0, 1800, 90.0)])
    got = session_span(s)
    assert got["rows"] == 3, got
    assert got["p50"] == 50.0, got


def check_the_floor_check_recovers_the_gap_from_the_pipeline_output():
    """session_end minus session_start minus duration_s should be exactly the gap on
    every row. This is what turns the paper floor into a fact about the emitted data."""
    s = sessions([("u_1", 0, 1800, 0.0), ("u_2", 100, 2500, 600.0)])
    got = floor_check(s, "30 minutes", "2 minutes")
    assert got["floor_s"] == 1920.0, got
    assert got["gap_s"] == 1800.0, got
    assert got["trailing_gap_min_s"] == 1800.0, got
    assert got["trailing_gap_max_s"] == 1800.0, got


def check_the_floor_check_reports_a_disagreement_rather_than_hiding_it():
    """A session whose trailing gap is not the configured gap means the window
    definition and the floor formula disagree. It must show as a range, because a
    check that only returned the formula's own answer would agree with itself."""
    s = sessions([("u_1", 0, 1800, 0.0), ("u_2", 0, 900, 0.0)])
    got = floor_check(s, "30 minutes", "2 minutes")
    assert got["trailing_gap_min_s"] == 900.0, got
    assert got["trailing_gap_max_s"] == 1800.0, got
