import random

from generator.population import (
    Population,
    crc32_partition,
    model_divergence,
    murmur2,
    murmur2_partition,
    partition_disagreements,
    partition_load,
)

# Measured against a real broker on 2026-08-16. Kafka 3.7.1 in KRaft mode with six
# partitions and librdkafka 2.15.0. The columns are the key and the partition the
# librdkafka producer put it on and the partition the Java console producer chose for
# the same key. These came off the broker. Nothing in this repo computed them. Full
# map in the day 4 audit and scripts/probe_partitioner.py regenerates them.
MEASURED = [
    ("u_004ae5", 2, 1),
    ("u_03983c", 3, 2),
    ("u_0870e1", 3, 4),
    ("u_09e469", 3, 1),
    ("u_0a5d2f", 0, 2),
    ("u_0ff18e", 5, 4),
    ("u_101fbc", 4, 2),
    ("u_11af92", 5, 1),
]


def check_alpha_zero_is_uniform():
    p = Population(100, alpha=0.0, seed=1)
    assert abs(p.share_of_top(10) - 0.10) < 1e-9, p.share_of_top(10)


def check_heavier_alpha_concentrates_traffic():
    flat = Population(5000, alpha=0.5, seed=1)
    steep = Population(5000, alpha=1.2, seed=1)
    assert steep.share_of_top(50) > flat.share_of_top(50) * 2, (
        steep.share_of_top(50),
        flat.share_of_top(50),
    )


def check_sampled_share_tracks_the_analytic_share():
    # The analytic share is what the weights say. This is the only check that the
    # sampler and the weights are describing the same population.
    p = Population(2000, alpha=1.0, seed=3)
    rng = random.Random(11)
    counts: dict[str, int] = {}
    for _ in range(60000):
        uid = p.pick(rng)
        counts[uid] = counts.get(uid, 0) + 1
    top = sorted(counts.values(), reverse=True)[:20]
    observed = sum(top) / 60000
    expected = p.share_of_top(20)
    assert abs(observed - expected) < 0.02, (observed, expected)


def check_user_ids_are_unique():
    # 24 bits over 5000 users collides with probability well above a coin flip, and a
    # collision would merge two users and look like a heavier tail than was asked for.
    p = Population(5000, alpha=1.0, seed=2)
    assert len(set(p.user_ids)) == 5000


def check_population_ids_do_not_move_when_the_traffic_seed_moves():
    # The ids come from their own generator. If they did not, comparing two runs at
    # different seeds would be comparing two different populations.
    assert Population(50, seed=4).user_ids == Population(50, alpha=0.3, seed=4).user_ids


def check_bad_arguments_are_refused():
    for kwargs in ({"size": 0}, {"size": 10, "alpha": -0.1}):
        try:
            Population(**kwargs)
        except ValueError:
            pass
        else:
            raise AssertionError(f"accepted {kwargs}")


def check_partitioning_is_stable_and_in_range():
    for uid in ("u_3f9a21", "u_000000", "u_ffffff"):
        a = crc32_partition(uid, 6)
        assert a == crc32_partition(uid, 6)
        assert 0 <= a < 6, a


def check_partition_count_of_one_puts_everything_on_partition_zero():
    assert crc32_partition("u_3f9a21", 1) == 0


def check_partition_load_conserves_events():
    counts = {f"u_{i:06x}": i + 1 for i in range(200)}
    load = partition_load(counts, 6)
    assert sum(load) == sum(counts.values()), (sum(load), sum(counts.values()))
    assert len(load) == 6


def check_both_models_reproduce_what_the_broker_did():
    """The whole point of the day 4 probe, frozen so it cannot quietly rot.

    Both halves matter. crc32 has to match the librdkafka column and murmur2 has to
    match the Java column. Checking only the model this repo uses would leave the
    claim about the other client resting on nothing again.
    """
    for key, via_librdkafka, via_java in MEASURED:
        assert crc32_partition(key, 6) == via_librdkafka, (key, crc32_partition(key, 6), via_librdkafka)
        assert murmur2_partition(key, 6) == via_java, (key, murmur2_partition(key, 6), via_java)


def check_the_two_models_really_do_disagree_on_these_keys():
    """Without this the check above passes if the two functions are the same function.

    Every measured pair here has the two clients on different partitions, so a
    murmur2_partition that had accidentally been written as crc32 would fail. That is
    the control, and this program has been bitten enough times by a green check that
    could not have gone red.
    """
    for key, via_librdkafka, via_java in MEASURED:
        assert via_librdkafka != via_java, key


def check_disagreements_are_counted_not_rated():
    """One wrong key out of six is still wrong. `agrees` is not a threshold.

    Six keys and not two. A two key fixture cannot tell "zero disagreements" apart
    from "fewer than half disagree", because one out of two fails both readings. A
    mutant that turned the flag into a majority vote survived that fixture on
    2026-08-16 and this is what killed it.
    """
    observed = {k: i % 6 for i, k in enumerate("abcdef")}
    perfect = partition_disagreements(observed, 6, lambda k, p: observed[k])
    assert perfect["disagreed"] == 0 and perfect["agrees"] is True
    one_off = partition_disagreements(observed, 6, lambda k, p: observed[k] + (k == "b"))
    assert one_off["disagreed"] == 1, one_off
    assert one_off["agrees"] is False, "a single disagreement is a disagreement"
    assert one_off["examples"][0]["key"] == "b"


def check_a_comparison_over_nothing_is_refused():
    """A checker that passes on zero inputs is the defect this program has now found
    in three of its own tools. This one raises instead."""
    for bad in ({}, None):
        try:
            partition_disagreements(bad or {}, 6)
        except ValueError:
            continue
        raise AssertionError(f"accepted {bad!r}")


def check_divergence_rate_sits_near_the_independence_line():
    keys = Population(200, alpha=1.0, seed=0).user_ids
    d = model_divergence(keys, 6)
    assert d["keys"] == 200
    # Two unrelated hashes over six partitions collide about one time in six, so the
    # rate should sit near 0.833. A rate near 0 would mean the two models are the same
    # function and a rate of 1.0 would mean something is forcing them apart.
    assert 0.7 < d["rate"] < 0.95, d
    assert d["chance_rate_if_independent"] == round(5 / 6, 4)


def check_murmur2_masks_the_sign_bit_rather_than_taking_an_absolute_value():
    """Java's toPositive is `& 0x7fffffff`. Writing it as abs() looks equivalent and is
    not, and it only diverges on keys whose hash has the top bit set.

    The assertion is that at least one real user id in a 400 user population is such a
    key. A check that merely looped over keys asserting the masked answer equals the
    masked answer would pass against the abs() version too.
    """
    hot = [k for k in Population(400, alpha=1.0, seed=3).user_ids if murmur2(k.encode("utf-8")) >= 0x80000000]
    assert hot, "no key in the sample had the top bit set, so this check proved nothing"
    diverged = 0
    for key in hot:
        h = murmur2(key.encode("utf-8"))
        assert murmur2_partition(key, 6) == (h & 0x7FFFFFFF) % 6
        if abs(h - 0x100000000) % 6 != murmur2_partition(key, 6):
            diverged += 1
    assert diverged, f"abs() agreed with the mask on all {len(hot)} negative hashes, so this proved nothing"
