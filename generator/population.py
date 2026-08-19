"""User population with a heavy tail.

`docs/decisions.md` has claimed from the start that the generator has a heavy-tail user
distribution, so that hot partitions can be observed rather than argued about. It did
not. This is that claim being made true.

Weights are Zipf-like, w(i) = 1 / (i + 1) ** alpha over a fixed user list. alpha 0 is
uniform. alpha near 1 puts most of the traffic on a handful of users.
"""

import bisect
import random
import zlib


class Population:
    def __init__(self, size: int, alpha: float = 1.0, seed: int = 0):
        if size < 1:
            raise ValueError(f"population size must be at least 1, got {size}")
        if alpha < 0:
            raise ValueError(f"alpha must not be negative, got {alpha}")
        self.size = size
        self.alpha = float(alpha)
        # Ids are drawn from a seeded generator of their own, so changing the traffic
        # seed does not change who exists.
        id_rng = random.Random(seed)
        self.user_ids = [f"u_{id_rng.getrandbits(24):06x}" for _ in range(size)]
        # Duplicate ids are possible at 24 bits and would silently merge two users
        # into one, which would look like a hotter tail than was asked for.
        if len(set(self.user_ids)) != size:
            seen, out = set(), []
            for uid in self.user_ids:
                while uid in seen:
                    uid = f"u_{id_rng.getrandbits(24):06x}"
                seen.add(uid)
                out.append(uid)
            self.user_ids = out
        self.weights = [1.0 / (i + 1) ** self.alpha for i in range(size)]
        total = sum(self.weights)
        self._cum = []
        running = 0.0
        for w in self.weights:
            running += w / total
            self._cum.append(running)
        self._cum[-1] = 1.0

    def pick(self, rng: random.Random) -> str:
        return self.user_ids[bisect.bisect_left(self._cum, rng.random())]

    def share_of_top(self, k: int) -> float:
        """Expected share of traffic taken by the k busiest users. Analytic, from the
        weights, not from a sample. Useful as the thing a sample gets checked against."""
        k = max(0, min(k, self.size))
        return sum(self.weights[:k]) / sum(self.weights)


def crc32_partition(key: str, partitions: int) -> int:
    """Partition a key the way librdkafka's default consistent partitioner does.

    This matters and it is easy to get wrong. librdkafka hashes with CRC32. The Java
    producer hashes with murmur2. The same key on the same topic lands on a different
    partition depending on which client wrote the record. Nothing here depends on the two agreeing, but
    a README sentence saying "keyed by user_id so a user's events stay together"
    is only true within one client library.
    """
    if partitions < 1:
        raise ValueError(f"partitions must be at least 1, got {partitions}")
    return zlib.crc32(key.encode("utf-8")) % partitions


def murmur2(data: bytes) -> int:
    """Kafka's Java client hash, ported. Returns the unsigned 32 bit value.

    This is not a general murmur2. It is the exact variant in
    org.apache.kafka.common.utils.Utils, seed 0x9747b28c, and it is here so that the
    claim "the Java producer partitions differently" can be a prediction rather than
    a shrug. A port nobody checked against the real thing would be worth nothing, so
    scripts/probe_partitioner.py runs the console producer and compares.
    """
    m = 0x5BD1E995
    h = (0x9747B28C ^ len(data)) & 0xFFFFFFFF
    whole = len(data) // 4 * 4
    for i in range(0, whole, 4):
        k = data[i] | (data[i + 1] << 8) | (data[i + 2] << 16) | (data[i + 3] << 24)
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> 24
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    tail = len(data) - whole
    if tail == 3:
        h ^= data[whole + 2] << 16
    if tail >= 2:
        h ^= data[whole + 1] << 8
    if tail >= 1:
        h ^= data[whole]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h


def murmur2_partition(key: str, partitions: int) -> int:
    """Partition a key the way the Kafka Java producer's default partitioner does.

    Java's `toPositive` masks the sign bit rather than taking an absolute value, so
    it is `& 0x7fffffff` and not `abs`. Those differ on Integer.MIN_VALUE and that
    difference is the kind of thing that shows up as one user out of a million on
    the wrong partition.
    """
    if partitions < 1:
        raise ValueError(f"partitions must be at least 1, got {partitions}")
    return (murmur2(key.encode("utf-8")) & 0x7FFFFFFF) % partitions


def partition_load(counts: dict[str, int], partitions: int) -> list[int]:
    """Fold per-user event counts onto partitions."""
    load = [0] * partitions
    for user_id, n in counts.items():
        load[crc32_partition(user_id, partitions)] += n
    return load


def partition_disagreements(observed: dict[str, int], partitions: int, predictor=crc32_partition) -> dict:
    """Check a partitioner model against what a broker really did.

    `observed` maps a key to the partition a record carrying that key landed on.
    For a long time nothing ever supplied that map, so `crc32_partition` was a
    claim about librdkafka read off its source rather than measured.

    The verdict is the disagreement count and not the agreement count. A function
    that agrees on 199 of 200 keys is wrong, and reporting 99.5 percent is how that
    gets missed. So `agrees` is only true at zero disagreements.

    `partitions_touched` is reported both ways because a partitioner can agree on
    every key it was shown and still be wrong about a partition no key reached.
    """
    if partitions < 1:
        raise ValueError(f"partitions must be at least 1, got {partitions}")
    if not observed:
        # A comparison over nothing is a failure mode I keep running into. Say
        # so rather than returning a clean-looking zero.
        raise ValueError("nothing to compare, observed is empty")

    disagreed = []
    for key, landed in sorted(observed.items()):
        predicted = predictor(key, partitions)
        if predicted != landed:
            disagreed.append({"key": key, "predicted": predicted, "landed": landed})

    return {
        "model": getattr(predictor, "__name__", str(predictor)),
        "keys_checked": len(observed),
        "disagreed": len(disagreed),
        "agrees": len(disagreed) == 0,
        "examples": disagreed[:5],
        "partitions_touched_observed": len(set(observed.values())),
        "partitions_touched_predicted": len({predictor(k, partitions) for k in observed}),
    }


def model_divergence(keys: list[str], partitions: int) -> dict:
    """How often the two client libraries send the same key to different partitions.

    This is the number the README's "keyed by user_id so a user's events stay
    together" sentence lives or dies on. With p partitions two independent hashes
    agree by chance about 1/p of the time, so the interesting figure is how close
    the measured rate sits to that rather than the raw count.
    """
    if not keys:
        raise ValueError("nothing to compare, keys is empty")
    differ = sum(1 for k in keys if crc32_partition(k, partitions) != murmur2_partition(k, partitions))
    return {
        "keys": len(keys),
        "different_partition": differ,
        "rate": round(differ / len(keys), 4),
        "chance_rate_if_independent": round(1.0 - 1.0 / partitions, 4),
    }
