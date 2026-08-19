"""Event model.

The first `make_event` drew event type, page, device and country independently
on every call. That is gone. A checkout arriving from a user who had never opened a
product page made the sessionization score meaningless, because there was no
structure in the stream to recover. `session.SessionModel` owns those fields now and
this module only assembles the record.
"""

import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta

from generator.clock import iso

EVENT_TYPES = ["page_view", "click", "scroll", "add_to_cart", "checkout"]


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


def build_event(
    user_id: str,
    parts: dict,
    now: datetime,
    lateness: timedelta = timedelta(0),
    event_id: str | None = None,
) -> ClickEvent:
    """Assemble one event.

    `lateness` moves `event_ts` into the past and leaves `ingest_ts` alone, which is
    how an event that took a while to reach us is represented. Passing `event_id`
    re-emits an existing event, which is how a duplicate is made.
    """
    return ClickEvent(
        event_id=event_id or str(uuid.uuid4()),
        user_id=user_id,
        session_hint=parts["session_hint"],
        event_type=parts["event_type"],
        page=parts["page"],
        referrer=parts["referrer"],
        device=parts["device"],
        country=parts["country"],
        event_ts=iso(now - lateness),
        ingest_ts=iso(now),
    )
