"""User population with a heavy tail.

`docs/decisions.md` has claimed since day 1 that the generator has a heavy-tail user
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


def partition_load(counts: dict[str, int], partitions: int) -> list[int]:
    """Fold per-user event counts onto partitions."""
    load = [0] * partitions
    for user_id, n in counts.items():
        load[crc32_partition(user_id, partitions)] += n
    return load
