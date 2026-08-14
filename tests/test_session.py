import random
from datetime import datetime, timedelta, timezone

from generator.session import SessionModel

T0 = datetime(2026, 8, 14, 9, 0, tzinfo=timezone.utc)


def _model():
    return SessionModel(), random.Random(5)


def check_a_new_visit_starts_a_new_session_even_with_no_gap():
    # The visit is the truth. A user who leaves and comes back two minutes later has
    # had two visits, and the gap rule cannot see the second one.
    m, rng = _model()
    a = m.step("u_aaa111", T0, rng, new_visit=True)
    b = m.step("u_aaa111", T0 + timedelta(minutes=2), rng, new_visit=True)
    assert a["session_hint"] != b["session_hint"], (a["session_hint"], b["session_hint"])
    assert b["new_session"] is True
    assert b["gap_rule_new_session"] is False, "the gap rule should have missed this one"
    assert m.sessions_started == 2


def check_a_gap_longer_than_thirty_minutes_starts_a_new_session_on_its_own():
    m, rng = _model()
    a = m.step("u_aaa111", T0, rng, new_visit=True)
    b = m.step("u_aaa111", T0 + timedelta(minutes=30, seconds=1), rng, new_visit=False)
    assert a["session_hint"] != b["session_hint"], (a["session_hint"], b["session_hint"])
    assert b["new_session"] is True
    assert b["gap_rule_new_session"] is True


def check_a_gap_of_exactly_thirty_minutes_does_not():
    # The boundary is strictly greater than the gap. Worth pinning, because a change
    # to >= here moves every session count in the project by a little.
    m, rng = _model()
    a = m.step("u_aaa111", T0, rng, new_visit=True)
    b = m.step("u_aaa111", T0 + timedelta(minutes=30), rng, new_visit=False)
    assert a["session_hint"] == b["session_hint"]
    assert b["new_session"] is False
    assert b["gap_rule_new_session"] is False


def check_two_users_get_two_sessions_not_one():
    m, rng = _model()
    m.step("u_aaa111", T0, rng, new_visit=True)
    m.step("u_bbb222", T0, rng, new_visit=True)
    assert m.sessions_started == 2
    assert len(m.states) == 2


def check_referrer_is_the_previous_page_inside_a_session():
    m, rng = _model()
    a = m.step("u_ccc333", T0, rng, new_visit=True)
    b = m.step("u_ccc333", T0 + timedelta(seconds=20), rng, new_visit=False)
    assert b["referrer"] == a["page"], (b["referrer"], a["page"])


def check_the_first_event_of_a_session_never_refers_to_a_page_on_this_site():
    m, rng = _model()
    for i in range(200):
        parts = m.step(f"u_{i:06x}", T0, rng, new_visit=True)
        ref = parts["referrer"]
        assert ref is None or ref.startswith("http"), ref


def check_checkout_never_happens_without_a_cart():
    # The funnel is the whole reason this module exists. An independent draw would
    # produce a checkout from a user who had seen nothing.
    m = SessionModel()
    rng = random.Random(9)
    now = T0
    seen_checkout = 0
    for i in range(4000):
        uid = f"u_{i % 40:06x}"
        parts = m.step(uid, now, rng, new_visit=(i < 40))
        st = m.states[uid]
        if parts["event_type"] == "checkout":
            seen_checkout += 1
            assert st.in_cart, uid
        if parts["event_type"] == "add_to_cart":
            assert st.seen_product, uid
        now += timedelta(seconds=5)
    assert seen_checkout > 0, "no checkout in 4000 events, the funnel never fires"


def check_a_late_event_does_not_pull_the_last_seen_time_backwards():
    # This is what makes an out-of-order arrival harmless to the session boundary.
    # Without the max, a late event would reset the clock and the next in-order event
    # would look like it had opened a new session.
    m, rng = _model()
    m.step("u_ddd444", T0 + timedelta(minutes=20), rng, new_visit=True)
    m.step("u_ddd444", T0, rng, new_visit=False)  # arrives late, happened twenty minutes earlier
    assert m.states["u_ddd444"].last_event == T0 + timedelta(minutes=20)
    after = m.step("u_ddd444", T0 + timedelta(minutes=40), rng, new_visit=False)
    assert after["new_session"] is False, "a late event opened a spurious session"


def check_country_is_stable_across_sessions_and_device_is_not_required_to_be():
    m, rng = _model()
    a = m.step("u_eee555", T0, rng, new_visit=True)
    b = m.step("u_eee555", T0 + timedelta(hours=4), rng, new_visit=True)
    assert a["country"] == b["country"], (a["country"], b["country"])
    assert b["device"] in ("desktop", "mobile", "tablet")


def check_depth_counts_within_a_session_and_resets_with_it():
    m, rng = _model()
    m.step("u_fff666", T0, rng, new_visit=True)
    second = m.step("u_fff666", T0 + timedelta(seconds=30), rng, new_visit=False)
    assert second["depth"] == 2, second["depth"]
    fresh = m.step("u_fff666", T0 + timedelta(hours=2), rng, new_visit=True)
    assert fresh["depth"] == 1, fresh["depth"]
