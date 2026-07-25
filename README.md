# Streaming Clickstream Lakehouse

Kafka to Spark Structured Streaming to Snowflake, with session windows and exactly-once
writes. Built to handle the parts of streaming that actually break: late events,
duplicates, and reruns.

```bash
./scripts/setup.sh          # kafka + minio up, topic created
python -m generator.produce # day 2
```

## Architecture

```
  generator          Kafka                 Spark Structured Streaming            Snowflake
 ┌──────────┐    ┌───────────┐   ┌──────────────────────────────────────┐   ┌──────────────┐
 │ synthetic│───▶│clickstream│──▶│ parse → dedupe → watermark → session │──▶│ sessions     │
 │ events   │    │ .events   │   │ window → feature extraction          │   │ (MERGE)      │
 │ + late   │    │ 6 parts   │   └──────────────────────────────────────┘   └──────────────┘
 │ + dupes  │    │ key=user  │                    │
 └──────────┘    └───────────┘                    ▼
                                            checkpoint dir
                                         (offsets + session state)
```

Key is `user_id` so all of a user's events land on one partition. Session windows need
that to keep state local.

## Status

Day 1 of 7. Compose stack, event schema, and the generator's event model are in.
Producer loop is next.

- [x] Day 1: compose stack, event schema, decisions doc
- [ ] Day 2: producer with rate control, late events, duplicates
- [ ] Day 3: streaming job. parse / dedupe / watermark / sessionize
- [ ] Day 4: feature extraction, Snowflake MERGE sink
- [ ] Day 5: latency and throughput metrics
- [ ] Day 6: failure testing, replay, duplicate verification
- [ ] Day 7: benchmarks and writeup

## Notes

Single-node Kafka with replication factor 1. Fine for a laptop, not a production config.
MinIO stands in for S3 so the staging path works without an AWS account.

Design decisions and open questions are in `docs/decisions.md`.
