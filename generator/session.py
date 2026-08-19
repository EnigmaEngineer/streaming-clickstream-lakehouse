"""Per-user session state.

The first version drew every event type independently, so a checkout could arrive from a user who
had never seen a product page. Sessionization scored against that is scored against
noise. This walks each user through a small funnel instead, and it keeps the previous
page so `referrer` chains within a visit.

`session_hint` is ground truth for scoring the sessionization. The pipeline never
reads it.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta

SESSION_GAP = timedelta(minutes=30)

# Where a visit comes from. None is a direct visit and is also what the schema says
# `referrer` holds on the first event of a session.
ENTRY_REFERRERS = [None, "https://www.google.com/", "https://news.ycombinator.com/", "https://t.co/"]
ENTRY_WEIGHTS = [0.42, 0.40, 0.10, 0.08]

LISTING_PAGES = ["/", "/search", "/category/audio", "/category/wearables"]

DEVICES = ["desktop", "mobile", "tablet"]
DEVICE_WEIGHTS = [0.44, 0.48, 0.08]
COUNTRIES = ["US", "IN", "GB", "DE", "CA", "BR", "AU"]
COUNTRY_WEIGHTS = [0.38, 0.19, 0.11, 0.10, 0.09, 0.08, 0.05]


@dataclass
class UserState:
    session_seq: int = 0
    last_event: datetime | None = None
    last_page: str | None = None
    depth: int = 0
    seen_product: bool = False
    in_cart: bool = False
    converted: bool = False
    # Country is a property of the user and does not move. Device is a property of the
    # visit, because the same person browses on a phone and buys on a laptop.
    country: str | None = None
    device: str | None = None


class SessionModel:
    """Holds one `UserState` per user seen so far.

    Memory grows with the number of distinct users, which is bounded by the population
    size. At a million users this would want eviction. It does not have it.
    """

    def __init__(self, gap: timedelta = SESSION_GAP):
        self.gap = gap
        self.states: dict[str, UserState] = {}
        self.sessions_started = 0

    def step(self, user_id: str, now: datetime, rng, new_visit: bool = True) -> dict:
        """Advance one user by one event and return the parts of the event that depend
        on session state.

        The truth about where a session begins is `new_visit`, which the visit pool
        owns. The thirty minute gap rule is computed alongside it and reported as
        `gap_rule_new_session`, because that is what the job recovers with and the
        two disagreeing is a number worth having before the streaming job exists.
        """
        st = self.states.setdefault(user_id, UserState())
        if st.country is None:
            st.country = rng.choices(COUNTRIES, weights=COUNTRY_WEIGHTS, k=1)[0]
        gap_says_new = st.last_event is None or (now - st.last_event) > self.gap
        new_session = new_visit or gap_says_new
        if new_session:
            st.device = rng.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0]
            st.session_seq += 1
            st.depth = 0
            st.last_page = None
            st.seen_product = False
            st.in_cart = False
            st.converted = False
            self.sessions_started += 1
            referrer = rng.choices(ENTRY_REFERRERS, weights=ENTRY_WEIGHTS, k=1)[0]
        else:
            referrer = st.last_page

        event_type, page = self._next_action(st, rng)
        st.depth += 1
        st.last_page = page
        # `now` here is the true event time, which is `ingest_ts` minus the injected
        # lateness. A late event can therefore land behind one already recorded, so
        # the last-seen time only ever moves forward.
        st.last_event = now if st.last_event is None else max(st.last_event, now)
        return {
            "session_hint": f"s_{user_id[2:]}_{st.session_seq:03d}",
            "event_type": event_type,
            "page": page,
            "referrer": referrer,
            "device": st.device,
            "country": st.country,
            "depth": st.depth,
            "new_session": new_session,
            "gap_rule_new_session": gap_says_new,
        }

    def _next_action(self, st: UserState, rng) -> tuple[str, str]:
        """The funnel. A user cannot check out without a cart and cannot fill a cart
        without having seen a product. Everything else is browsing."""
        if st.in_cart and not st.converted and rng.random() < 0.35:
            st.converted = True
            return "checkout", "/checkout"
        if st.seen_product and not st.in_cart and rng.random() < 0.18:
            st.in_cart = True
            return "add_to_cart", "/cart"
        roll = rng.random()
        if roll < 0.30 and st.depth > 0:
            # Scrolling happens on whatever page you are already on.
            return "scroll", st.last_page or "/"
        if roll < 0.55:
            page = f"/product/{rng.randint(1000, 9999)}"
            st.seen_product = True
            return "click", page
        return "page_view", rng.choice(LISTING_PAGES)
