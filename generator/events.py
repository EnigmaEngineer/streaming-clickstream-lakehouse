"""Event model and generation helpers.

Day 1: schema and a single-event factory. The producer loop, rate control and
late-event injection land on day 2.
"""

import random
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta

EVENT_TYPES = ["page_view", "click", "scroll", "add_to_cart", "checkout"]
DEVICES = ["desktop", "mobile", "tablet"]
COUNTRIES = ["US", "IN", "GB", "DE", "CA", "BR", "AU"]

# Rough funnel weights. Most traffic is browsing, very little converts.
EVENT_WEIGHTS = [0.55, 0.25, 0.13, 0.05, 0.02]

PAGES = [
    "/",
    "/search",
    "/category/audio",
    "/category/wearables",
    "/product/{pid}",
    "/cart",
    "/checkout",
]


@dataclass
class ClickEvent:
    event_id: str
    user_id: str
    session_hint: str
    event_type: str
    page: str
    referrer: str | None
    device: str
    country: str
    event_ts: str
    ingest_ts: str

    def to_dict(self):
        return asdict(self)


def utc_now():
    return datetime.now(timezone.utc)


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def make_user_id(rng: random.Random) -> str:
    return f"u_{rng.getrandbits(24):06x}"


def pick_page(rng: random.Random) -> str:
    page = rng.choice(PAGES)
    return page.format(pid=rng.randint(1000, 9999)) if "{pid}" in page else page


def make_event(
    rng: random.Random,
    user_id: str,
    session_seq: int,
    referrer: str | None = None,
    lateness: timedelta = timedelta(0),
) -> ClickEvent:
    """One event. `lateness` shifts event_ts into the past without moving ingest_ts,
    which is how we simulate an event that took a while to reach us."""
    now = utc_now()
    return ClickEvent(
        event_id=str(uuid.uuid4()),
        user_id=user_id,
        session_hint=f"s_{user_id[2:]}_{session_seq:03d}",
        event_type=rng.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0],
        page=pick_page(rng),
        referrer=referrer,
        device=rng.choice(DEVICES),
        country=rng.choice(COUNTRIES),
        event_ts=iso(now - lateness),
        ingest_ts=iso(now),
    )


# TODO(day 2): user population with a heavy tail so a few users generate most of the
# traffic. Need this to see whether user_id partitioning creates a hot partition.
# TODO(day 2): duplicate emission at a configurable rate to exercise the dedupe path.
