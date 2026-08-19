"""Checks for the scorer.

Every published session number comes out of stream/scoring.py, so it gets the same
treatment as the pipeline. The fixtures are hand built with a known answer, because
scoring the scorer against the pipeline would be comparing two things built the same
way and a defect they both carry would pass.
"""

from datetime import datetime, timedelta, timezone

from stream.scoring import attach_sessions, feature_bias, score, truth_features

from tests.test_sessionize import spark

BASE = datetime(2026, 8, 15, 12, 0, 0, tzinfo=timezone.utc)


def at(s):
    return BASE + timedelta(seconds=s)


def events(rows):
    """Each row is an event id and a user id and an offset in seconds and a hint."""
    return spark().createDataFrame(
        [(e, u, at(s), h) for e, u, s, h in rows],
        "event_id string, user_id string, event_ts timestamp, session_hint string",
    )


def sessions(rows):
    """Each row is a user id and a start offset and an end offset and a count."""
    return spark().createDataFrame(
        [(u, at(a), at(b), n) for u, a, b, n in rows],
        "user_id string, session_start timestamp, session_end timestamp, event_count int",
    )


def check_every_event_lands_in_exactly_one_session():
    e = events([("a", "u_1", 0, "h1"), ("b", "u_1", 10, "h1")])
    s = sessions([("u_1", 0, 1800, 2)])
    got = attach_sessions(e, s)
    assert got.count() == 2, got.collect()
    assert got.where("session_start is null").count() == 0


def check_an_event_outside_every_window_is_reported_not_hidden():
    e = events([("a", "u_1", 0, "h1"), ("b", "u_1", 9999, "h2")])
    s = sessions([("u_1", 0, 1800, 1)])
    out = score(e, s)
    assert out["events_unmatched"] == 1, out
    assert out["events_joined"] == 2, out


def check_two_visits_in_one_window_count_as_one_lost_boundary():
    """The merge case. Two true sessions, one recovered session, so exactly one
    boundary was lost. This is the number the README publishes."""
    e = events([("a", "u_1", 0, "h1"), ("b", "u_1", 10, "h2")])
    s = sessions([("u_1", 0, 1800, 2)])
    out = score(e, s)
    assert out["true_sessions_scored"] == 2, out
    assert out["recovered_sessions"] == 1, out
    assert out["boundaries_lost_to_merge"] == 1, out
    assert out["merged_recovered_sessions"] == 1, out
    assert out["boundary_miss_rate"] == 0.5, out
    assert out["split_true_sessions"] == 0, out


def check_three_visits_in_one_window_count_as_two_lost_boundaries():
    """The rule is hints minus one and not a flag. With only the two visit fixture
    above, an implementation that counted merged windows would score identically."""
    e = events([("a", "u_1", 0, "h1"), ("b", "u_1", 10, "h2"), ("c", "u_1", 20, "h3")])
    s = sessions([("u_1", 0, 1800, 3)])
    out = score(e, s)
    assert out["boundaries_lost_to_merge"] == 2, out
    assert out["merged_recovered_sessions"] == 1, out


def check_one_visit_across_two_windows_is_a_split_not_a_merge():
    e = events([("a", "u_1", 0, "h1"), ("b", "u_1", 5000, "h1")])
    s = sessions([("u_1", 0, 1800, 1), ("u_1", 5000, 6800, 1)])
    out = score(e, s)
    assert out["split_true_sessions"] == 1, out
    assert out["boundaries_lost_to_merge"] == 0, out
    assert out["recovered_sessions"] == 2, out


def check_a_clean_recovery_scores_zero_on_both():
    e = events([("a", "u_1", 0, "h1"), ("b", "u_2", 0, "h2")])
    s = sessions([("u_1", 0, 1800, 1), ("u_2", 0, 1800, 1)])
    out = score(e, s)
    assert out["boundaries_lost_to_merge"] == 0, out
    assert out["split_true_sessions"] == 0, out
    assert out["boundary_miss_rate"] == 0.0, out


def check_the_same_hint_under_two_users_is_not_pooled():
    """session_hint embeds the user id in the generator, so this cannot happen there.
    It is checked anyway, because the scorer groups by hint alone in one place and by
    user and window in another, and those two disagreeing is a silent wrong answer."""
    e = events([("a", "u_1", 0, "h"), ("b", "u_2", 0, "h")])
    s = sessions([("u_1", 0, 1800, 1), ("u_2", 0, 1800, 1)])
    out = score(e, s)
    assert out["recovered_sessions"] == 2, out
    assert out["boundaries_lost_to_merge"] == 0, out
    # One hint spanning two windows is a split by the scorer's own definition. This
    # fixture is the reminder that the definition is per hint and not per user and
    # hint, which matters the moment a generator stops embedding the user in it.
    assert out["split_true_sessions"] == 1, out


def check_a_window_boundary_is_start_inclusive_and_end_exclusive():
    """Adjacent windows must not both claim the same event. If the join used <= on
    the end, the event at 1800 would land in both and every count would inflate."""
    e = events([("a", "u_1", 1800, "h1")])
    s = sessions([("u_1", 0, 1800, 1), ("u_1", 1800, 3600, 1)])
    got = attach_sessions(e, s)
    assert got.count() == 1, got.collect()
    row = got.first()
    # Compared against the event's own timestamp rather than against at(1800). Both
    # come back from Spark the same way, so this cannot fail on a timezone
    # representation difference the way the first version of this check did.
    assert row["session_start"] == row["event_ts"], got.collect()


def rich_events(rows):
    """Events carrying the columns the feature side needs.

    Each row holds an event id and a user id. Then an offset in seconds and a hint
    and a page and an event type.
    """
    return spark().createDataFrame(
        [(e, u, at(s), h, p, t) for e, u, s, h, p, t in rows],
        "event_id string, user_id string, event_ts timestamp, session_hint string, "
        "page string, event_type string",
    )


def feature_sessions(rows):
    """Recovered sessions carrying the feature columns.

    Each row holds a user id and a start and an end. Then event_count and page_depth
    and duration_s and converted and bounce.
    """
    return spark().createDataFrame(
        [(u, at(a), at(b), n, d, dur, c, bo) for u, a, b, n, d, dur, c, bo in rows],
        "user_id string, session_start timestamp, session_end timestamp, event_count int, "
        "page_depth int, duration_s double, converted int, bounce int",
    )


def check_truth_features_ignore_planted_duplicates():
    """The corpus carries repeat event_ids. Counting them on the truth side would
    inflate the real event count and shrink the very gap feature_bias measures."""
    e = rich_events(
        [
            ("a", "u_1", 0, "h1", "/", "page_view"),
            ("a", "u_1", 0, "h1", "/", "page_view"),
            ("b", "u_1", 10, "h1", "/cart", "add_to_cart"),
        ]
    )
    got = {r["session_hint"]: r for r in truth_features(e).collect()}
    assert got["h1"]["event_count"] == 2, got["h1"]
    assert got["h1"]["page_depth"] == 2, got["h1"]


def check_a_merged_session_pulls_conversion_up_and_bounce_down():
    """The feature bias finding, on a fixture where the right answer is known by hand.

    Two real visits. One is a single page bounce that never converts. The other
    converts. Merged into one recovered session they read as one converting,
    non-bouncing session, so conversion goes from a half to one and bounce from a
    half to zero.
    """
    e = rich_events(
        [
            ("a", "u_1", 0, "h1", "/", "page_view"),
            ("b", "u_1", 60, "h2", "/p/1", "page_view"),
            ("c", "u_1", 70, "h2", "/checkout", "checkout"),
        ]
    )
    s = feature_sessions([("u_1", 0, 1800, 3, 3, 70.0, 1, 0)])
    out = feature_bias(e, s)
    assert out["true_sessions"] == 2 and out["recovered_sessions"] == 1, out
    assert out["converted"]["truth"] == 0.5 and out["converted"]["recovered"] == 1.0, out["converted"]
    assert out["converted"]["ratio"] == 2.0, out["converted"]
    assert out["bounce"]["truth"] == 0.5 and out["bounce"]["recovered"] == 0.0, out["bounce"]


def check_an_unmerged_corpus_shows_no_bias():
    """The control. Without it the bias table could be reporting a distortion that
    every input produces, including a perfect one."""
    e = rich_events(
        [
            ("a", "u_1", 0, "h1", "/", "page_view"),
            ("b", "u_1", 10, "h1", "/p/1", "page_view"),
        ]
    )
    s = feature_sessions([("u_1", 0, 1800, 2, 2, 10.0, 0, 0)])
    out = feature_bias(e, s)
    for col in ("converted", "bounce", "event_count", "page_depth", "duration_s"):
        assert out[col]["ratio"] in (1.0, None), (col, out[col])
