"""Checks for the scorer.

Every published session number comes out of stream/scoring.py, so it gets the same
treatment as the pipeline. The fixtures are hand built with a known answer, because
scoring the scorer against the pipeline would be comparing two things built the same
way and a defect they both carry would pass.
"""

from datetime import datetime, timedelta, timezone

from stream.scoring import attach_sessions, score

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
