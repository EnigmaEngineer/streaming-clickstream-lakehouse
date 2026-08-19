"""Who is on the site right now.

The first build of this sampled a user from the whole population on every event. That
looked reasonable and it was wrong. With 2000 users and 500 events per second, every
user gets an event every four seconds forever, so no inactivity gap ever reaches
thirty minutes and no session ever ends. The measurement said it plainly: 2000 users,
2000 sessions, a median of 110 events each and a conversion rate of 100 percent.

Real traffic is not a uniform draw over everyone who has ever visited. It is a small
set of people on the site at once, each of whom leaves. That is what this models.

A visit holds a user for a budgeted number of events and then releases them. Session
length is therefore a property of the visit and not of the clock, and the thirty minute
gap rule becomes something the pipeline has to *recover* rather than something the
generator hands it.
"""

import math
import random


class VisitPool:
    """A fixed number of concurrent visits.

    `size` is how many people are on the site at once. With a rate of R events per
    second, a visitor sees an event roughly every size / R seconds, so the pair of
    knobs sets the think time without a third one.
    """

    def __init__(self, population, size: int = 150, median_events: float = 6.0, sigma: float = 0.9):
        if size < 1:
            raise ValueError(f"pool size must be at least 1, got {size}")
        if median_events < 1:
            raise ValueError(f"median_events must be at least 1, got {median_events}")
        # Not clamped. A pool that quietly shrinks to fit changes the session length
        # distribution with nothing in the output to say so, and admission is
        # rejection sampling, so a pool near the population size cannot be filled at
        # all. Half is a working limit rather than a derived one.
        if size > population.size // 2:
            raise ValueError(
                f"pool size {size} is more than half of the population "
                f"{population.size}, so most visitors would always be on the site"
            )
        self.population = population
        self.size = size
        self.median_events = float(median_events)
        self.sigma = float(sigma)
        self.visits: list[list] = []  # [user_id, remaining, is_first_event]
        self.visits_started = 0
        self.visits_ended = 0
        self.deflected = 0        # draws that landed on somebody already on the site
        self.occupancy_sum = 0    # so the achieved concurrency can be reported
        self.occupancy_n = 0

    @property
    def mean_occupancy(self) -> float:
        """`size` is a target and not a guarantee. At a steep alpha the site cannot
        hold that many distinct people at once, and this is what it really held."""
        return self.occupancy_sum / self.occupancy_n if self.occupancy_n else 0.0

    def _budget(self, rng: random.Random) -> int:
        # Lognormal, because the number of pages in a visit has a long right tail. Most
        # people look at a couple of things. A few read the entire catalogue.
        return max(1, int(math.exp(rng.gauss(math.log(self.median_events), self.sigma))))

    def _admit(self, rng: random.Random) -> bool:
        """Try to bring one more person onto the site. False means the draw kept
        landing on somebody already here."""
        active = {v[0]: i for i, v in enumerate(self.visits)}
        for _ in range(20):
            uid = self.population.pick(rng)
            if uid not in active:
                self.visits.append([uid, self._budget(rng), True])
                self.visits_started += 1
                return True
        # A heavy tail and a concurrency target pull against each other. At alpha 1.4
        # the fifty busiest users take 86 percent of the traffic, so a draw for
        # "somebody new" almost always returns a person who is already here. The
        # honest reading of that draw is not a second simultaneous visit by the same
        # person. It is that this person is still browsing, so their current visit
        # gets longer.
        self.visits[active[uid]][1] += self._budget(rng)
        self.deflected += 1
        return False

    def next_user(self, rng: random.Random) -> tuple[str, bool]:
        """One event's worth of activity. Fills the pool, picks somebody in it, and
        decrements their budget. The flag says whether this is that visit's first
        event, which is the true session boundary."""
        attempts = 0
        while len(self.visits) < self.size and attempts < self.size:
            attempts += 1
            if not self._admit(rng):
                break  # saturated for the moment, try again on the next event
        self.occupancy_sum += len(self.visits)
        self.occupancy_n += 1
        i = rng.randrange(len(self.visits))
        visit = self.visits[i]
        first = visit[2]
        visit[2] = False
        visit[1] -= 1
        if visit[1] <= 0:
            self.visits.pop(i)
            self.visits_ended += 1
        return visit[0], first
