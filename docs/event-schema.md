# Event schema

One topic, `clickstream.events`, one event type. Keeping it to a single topic for now.
Splitting by event type is a day-6 problem if partition skew shows up.

Key: `user_id`. That guarantees all events for a user land on the same partition, which
Spark needs for session windows to work without a shuffle on every micro-batch.

## Payload

| Field | Type | Notes |
|---|---|---|
| `event_id` | uuid string | Dedupe key. Generator intentionally emits some twice. |
| `user_id` | string | `u_` + 6 hex. Partition key. |
| `session_hint` | string | Generator's own session id. **Not** used by the pipeline. It exists so I can score my sessionization against ground truth. |
| `event_type` | enum | `page_view`, `click`, `scroll`, `add_to_cart`, `checkout` |
| `page` | string | Path, e.g. `/product/8821` |
| `referrer` | string \| null | Null on the first event of a visit |
| `device` | enum | `desktop`, `mobile`, `tablet` |
| `country` | string | ISO-2 |
| `event_ts` | ISO-8601 UTC | When it happened |
| `ingest_ts` | ISO-8601 UTC | When the generator sent it. Difference is the injected lateness. |

Example:

```json
{
  "event_id": "6f1c3a0e-2b7d-4a11-9f0e-71c2a4d55b3a",
  "user_id": "u_3f9a21",
  "session_hint": "s_3f9a21_004",
  "event_type": "page_view",
  "page": "/product/8821",
  "referrer": "/search?q=headphones",
  "device": "mobile",
  "country": "US",
  "event_ts": "2026-07-24T18:02:11.482Z",
  "ingest_ts": "2026-07-24T18:02:11.611Z"
}
```

## Why two timestamps

`event_ts` drives watermarking. `ingest_ts` is only there so I can measure how late an
event actually arrived and compare that against what the watermark decided to drop.
Without both, "we dropped 0.4% of events" is a number with no explanation attached.

## Session definition

A session ends after 30 minutes of inactivity. Standard, and it matches what
`session_hint` does in the generator, so the two are comparable.

Edge case I have not decided yet: a session that spans a watermark boundary. Spark will
emit the window when the watermark passes it, but a very late event belonging to that
session arrives after the state is dropped. Options are to widen the watermark (more
memory, more latency) or accept the loss and count it. Day 6 problem, noted in
`docs/decisions.md`.
