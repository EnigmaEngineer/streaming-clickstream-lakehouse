"""Harness step, not part of the pipeline.

Append mode emits a session window only once the watermark has passed its end. A
bounded run over a fixed set of files stops when the files run out, so the last
watermark reading is whatever the final batch produced and every session still
inside its thirty minute gap never closes. On the first full run of day 3 that left
2117 sessions emitted and 34481 events sitting in state with nothing to push them
out.

A real stream does not have this problem, because more data keeps arriving. An
offline run does, and the honest fix is to say so and then hand the stream one event
far enough in the future to advance the watermark past everything.

The sentinel carries a user_id nothing else uses so its own session is easy to drop
before scoring. It is a fake event and it is labelled as one here rather than
explained in a footnote later.

    python -m scripts.flush_shard --dir /tmp/events --hours 6
"""

import argparse
import glob
import json
import os
import uuid
from datetime import datetime, timedelta, timezone

SENTINEL_USER = "u_flush0"


def max_event_ts(directory: str) -> datetime:
    newest = None
    for path in glob.glob(os.path.join(directory, "*.json")):
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                ts = json.loads(line)["event_ts"]
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                if newest is None or dt > newest:
                    newest = dt
    if newest is None:
        raise ValueError(f"no events found under {directory!r}, nothing to flush past")
    return newest


def write(directory: str, hours: float) -> str:
    at = max_event_ts(directory) + timedelta(hours=hours)
    stamp = at.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    record = {
        "event_id": str(uuid.uuid4()),
        "user_id": SENTINEL_USER,
        "session_hint": "s_flush0_001",
        "event_type": "page_view",
        "page": "/",
        "referrer": None,
        "device": "desktop",
        "country": "US",
        "event_ts": stamp,
        "ingest_ts": stamp,
    }
    # zzz so it sorts last, because the file source reads in name order within a
    # modification time and a flush that arrives first flushes nothing.
    out = os.path.join(directory, "zzz-flush.json")
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(record, separators=(",", ":")) + "\n")
    return out


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="write a watermark advancing sentinel event")
    p.add_argument("--dir", required=True)
    p.add_argument("--hours", type=float, default=6.0)
    a = p.parse_args(argv)
    print(write(a.dir, a.hours))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
