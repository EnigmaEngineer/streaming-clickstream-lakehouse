"""Checks for the lag arithmetic.

No Spark here on purpose. The headline number of day 5 is a sum of two parsed
durations, and a clone with nothing installed should be able to falsify it.

Fixtures build the collision in rather than hoping for it, per the 08-02 lesson. A
duration test whose only case is "30 minutes" cannot catch a unit table that maps
every key to sixty.
"""

from stream.lag import (
    batch_latency,
    emission_floor_s,
    first_event_floor_s,
    lag_budget,
    parse_duration,
    quantiles,
)


def check_durations_parse_across_every_unit():
    """One case per unit. With only minutes in the fixture, a units table that
    returned 60.0 for everything would pass."""
    assert parse_duration("30 minutes") == 1800.0
    assert parse_duration("2 minutes") == 120.0
    assert parse_duration("1 minute") == 60.0
    assert parse_duration("10 seconds") == 10.0
    assert parse_duration("1 second") == 1.0
    assert parse_duration("2 hours") == 7200.0
    assert parse_duration("500 ms") == 0.5
    assert parse_duration("1 day") == 86400.0


def check_a_bare_number_is_refused_rather_than_assumed_to_be_seconds():
    """The whole module rests on this function and a silent unit guess is how the
    headline figure would be wrong by a factor of sixty."""
    for bad in ("30", "", "   ", "30minutes", "30 fortnights", "thirty minutes 5"):
        try:
            parse_duration(bad)
        except ValueError:
            continue
        raise AssertionError(f"{bad!r} should not have parsed")


def check_a_non_string_duration_raises_typeerror_not_valueerror():
    """A caller passing 1800 rather than '30 minutes' is a different mistake from a
    caller passing nonsense, and float(1800) would have parsed happily if the split
    had been reached. The 08-02 lesson is to assert on the specific failure."""
    for bad in (1800, None, ["30 minutes"]):
        try:
            parse_duration(bad)
        except TypeError:
            continue
        raise AssertionError(f"{bad!r} should have raised TypeError")


def check_the_emission_floor_is_the_gap_plus_the_watermark():
    assert emission_floor_s("30 minutes", "2 minutes") == 1920.0
    # Both terms move it. A floor that only read the gap would pass the row above.
    assert emission_floor_s("30 minutes", "30 minutes") == 3600.0
    assert emission_floor_s("2 minutes", "2 minutes") == 240.0
    assert emission_floor_s("1 minute", "10 seconds") == 70.0


def check_the_first_event_floor_adds_the_session_duration():
    assert first_event_floor_s("30 minutes", "2 minutes", 0.0) == 1920.0
    assert first_event_floor_s("30 minutes", "2 minutes", 45.5) == 1965.5


def check_a_negative_session_duration_is_refused():
    """It would silently shrink the floor below the structural minimum, which is the
    one thing the floor is for."""
    try:
        first_event_floor_s("30 minutes", "2 minutes", -1.0)
    except ValueError:
        return
    raise AssertionError("a negative duration should not have been accepted")


def check_quantiles_on_a_known_list():
    """101 values from 0, so every requested quantile lands on a value and the
    interpolation cannot hide behind a rounding coincidence."""
    got = quantiles(list(range(0, 101)))
    assert got["p50"] == 50.0, got
    assert got["p95"] == 95.0, got
    assert got["p99"] == 99.0, got


def check_quantiles_interpolate_between_neighbours():
    """The property that makes this agree with Spark's percentile. A nearest rank
    implementation returns 100 or 300 here and never 200, and the two definitions
    were summed in one budget line until this check existed."""
    got = quantiles([100.0, 300.0], qs=(0.5,))
    assert got["p50"] == 200.0, got
    assert quantiles([0.0, 10.0], qs=(0.25,))["p25"] == 2.5


def check_quantiles_of_an_empty_list_return_nothing_rather_than_zero():
    """A p50 of 0.0 off no rows reads as a fast pipeline. This is the 08-04 shape,
    where a check that can pass on zero inputs eventually is pointed at zero."""
    assert quantiles([]) == {}


def check_a_quantile_outside_the_range_is_refused():
    for bad in (0.0, 1.5, -0.2):
        try:
            quantiles([1, 2, 3], qs=(bad,))
        except ValueError:
            continue
        raise AssertionError(f"q={bad} should have raised")


def check_batch_latency_over_a_hand_built_progress_list():
    """Three batches with known timings. The first is slow, which is what a real run
    looks like, and the rest quantiles must exclude it."""
    progress = [
        {"numInputRows": 10, "durationMs": {"triggerExecution": 1000, "addBatch": 800}},
        {"numInputRows": 20, "durationMs": {"triggerExecution": 100, "addBatch": 60}},
        {"numInputRows": 30, "durationMs": {"triggerExecution": 300, "addBatch": 200}},
    ]
    got = batch_latency(progress)
    assert got["batches"] == 3, got
    assert got["input_rows"] == 60, got
    assert got["trigger_total_ms"] == 1400.0, got
    assert got["trigger_first_ms"] == 1000.0, got
    # 100 and 300 with the first batch removed, interpolated to 200. If the first
    # batch were still in the list the three values 100, 300 and 1000 would give a
    # p50 of 300, so this separates the two.
    assert got["trigger_rest_ms"]["p50"] == 200.0, got
    assert got["add_batch_ms"]["p50"] == 200.0, got
    assert got["rows_per_second"] == round(60 / 1.4, 1), got


def check_batch_latency_on_no_batches_reports_zero_batches_and_no_rates():
    """A rows per second figure off an empty run would be a division by zero or a
    fabricated number, and either one published is worse than the absence."""
    got = batch_latency([])
    assert got == {"batches": 0}, got
    assert batch_latency(None) == {"batches": 0}


def check_a_single_batch_run_reports_no_rest_quantiles():
    """With one batch there is no "rest", and reporting the first batch as the typical
    one is the 08-12 warmup mistake."""
    got = batch_latency([{"numInputRows": 5, "durationMs": {"triggerExecution": 50, "addBatch": 40}}])
    assert got["trigger_rest_ms"] == {}, got
    assert got["trigger_first_ms"] == 50.0, got


def check_the_budget_is_dominated_by_the_emission_delay():
    """The day's finding, pinned. At the repo defaults the structural term is over 99
    percent of the total and the goal in the blueprint is unreachable."""
    got = lag_budget("30 minutes", "2 minutes", ingest_p50_s=0.0, processing_p50_ms=200.0)
    assert got["terms"]["emission_delay_s"] == 1920.0, got
    assert got["share"]["emission_delay_s"] > 0.999, got
    assert got["meets_goal"] is False, got
    assert got["over_goal_by"] == 32.0, got


def check_a_short_gap_can_meet_the_goal_and_the_flag_moves():
    """Without this the meets_goal flag could be hard coded False and every check
    above would still pass. That is the 08-13 lesson about a test whose subject is
    'nothing changes'."""
    got = lag_budget("20 seconds", "10 seconds", ingest_p50_s=1.0, processing_p50_ms=500.0)
    assert got["total_s"] == 31.5, got
    assert got["meets_goal"] is True, got
    assert got["over_goal_by"] == 0.53, got


def check_the_ingest_and_processing_terms_really_reach_the_total():
    """Both are tiny at the defaults, so a budget that dropped one entirely would
    round to the same share. Checked on a total where they are not tiny."""
    got = lag_budget("10 seconds", "10 seconds", ingest_p50_s=3.0, processing_p50_ms=2000.0)
    assert got["terms"]["ingest_lag_s"] == 3.0, got
    assert got["terms"]["processing_s"] == 2.0, got
    assert got["total_s"] == 25.0, got
    assert abs(sum(got["share"].values()) - 1.0) < 1e-9, got
