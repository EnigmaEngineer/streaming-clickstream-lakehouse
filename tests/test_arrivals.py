import random

from generator.arrivals import VisitPool
from generator.population import Population


def _pool(**over):
    kwargs = {"size": 20, "median_events": 5.0}
    kwargs.update(over)
    return VisitPool(Population(500, alpha=1.0, seed=2), **kwargs), random.Random(17)


def check_the_pool_is_full_when_somebody_is_picked():
    # After the call it can be one short, because the visit just picked may have run
    # out and left. The invariant is about the moment of the pick.
    p, rng = _pool()
    for _ in range(2000):
        p.next_user(rng)
        assert p.size - 1 <= len(p.visits) <= p.size, len(p.visits)


def check_a_user_is_never_in_the_pool_twice():
    # Two concurrent visits by one user would interleave into one stream of events
    # with no gap between them, which is the bug this whole module exists to fix.
    p, rng = _pool()
    for _ in range(3000):
        p.next_user(rng)
        ids = [v[0] for v in p.visits]
        assert len(ids) == len(set(ids)), ids


def check_exactly_one_first_event_per_visit():
    p, rng = _pool()
    firsts = sum(1 for _ in range(5000) if p.next_user(rng)[1])
    # Every visit that started has flagged its first event, except the ones sitting in
    # the pool that the random pick has not reached yet.
    unpicked = sum(1 for v in p.visits if v[2])
    assert unpicked > 0, "no unpicked visit left, so this check proved nothing"
    assert firsts == p.visits_started - unpicked, (firsts, p.visits_started, unpicked)


def check_visits_end():
    # The whole defect on the first build was that nobody ever left.
    p, rng = _pool()
    for _ in range(5000):
        p.next_user(rng)
    assert p.visits_ended > 500, p.visits_ended
    # Every visit either ended or is still open. Written against the live pool rather
    # than against the target, because the target is not always reached.
    assert p.visits_started == p.visits_ended + len(p.visits)


def check_a_longer_median_visit_produces_fewer_visits():
    short, rng_a = _pool(median_events=3.0)
    long, rng_b = _pool(median_events=20.0)
    for _ in range(6000):
        short.next_user(rng_a)
        long.next_user(rng_b)
    assert short.visits_ended > long.visits_ended * 2, (short.visits_ended, long.visits_ended)


def check_a_pool_too_big_for_the_population_is_refused_up_front():
    # The first build clamped it to the population size instead. That pool can never
    # be filled, because admission draws until it finds somebody idle and nobody is.
    try:
        VisitPool(Population(30, alpha=0.0, seed=1), size=500)
    except ValueError as e:
        assert "half of the population" in str(e), str(e)
    else:
        raise AssertionError("a pool larger than the population was accepted")
    # Exactly half is the largest that works.
    VisitPool(Population(30, alpha=0.0, seed=1), size=15)


def check_bad_arguments_are_refused():
    pop = Population(50, seed=1)
    for kwargs in ({"size": 0}, {"median_events": 0.5}):
        try:
            VisitPool(pop, **kwargs)
        except ValueError:
            pass
        else:
            raise AssertionError(f"accepted {kwargs}")


def check_a_pool_it_cannot_fill_reports_the_shortfall():
    # A steep tail means the busiest users are almost always here, so a draw for
    # somebody new keeps returning somebody old. The pool runs short and both the
    # deflection count and the achieved occupancy say so. Running short in silence
    # would change the session length distribution with nothing in the output to
    # show it.
    p = VisitPool(Population(400, alpha=2.5, seed=1), size=200, median_events=4.0)
    rng = random.Random(3)
    for _ in range(4000):
        p.next_user(rng)
    assert p.deflected > 0, "no deflection at alpha 2.5, the check proved nothing"
    assert p.mean_occupancy < p.size, (p.mean_occupancy, p.size)


def check_a_flat_population_fills_the_pool_and_deflects_nothing():
    # The control for the check above. Without it a deflection count of zero would be
    # indistinguishable from a counter that is never incremented.
    p = VisitPool(Population(4000, alpha=0.0, seed=1), size=100, median_events=4.0)
    rng = random.Random(3)
    for _ in range(4000):
        p.next_user(rng)
    assert p.deflected == 0, p.deflected
    assert p.mean_occupancy > p.size - 1, (p.mean_occupancy, p.size)
