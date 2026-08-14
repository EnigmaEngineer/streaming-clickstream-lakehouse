# Streaming Clickstream Lakehouse

Kafka to Spark Structured Streaming to Snowflake, with session windows and exactly-once
writes. Built to handle the parts of streaming that actually break: late events,
duplicates, and reruns.

```bash
./scripts/setup.sh                                        # kafka + minio up, topic created
python -m generator.produce --rate 200 --seconds 5 --report
python tests/run_all.py
python scripts/measure_generator.py                       # every number below
```

The generator and the test suite need nothing installed. Standard library only.

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
that to keep state local. That guarantee holds within one client library and not
across two. librdkafka hashes a key with CRC32 and the Java producer uses murmur2, so
the same key lands on a different partition depending on who wrote it.

## The generator

`generator/` builds the traffic. Every knob exists because day 3 has to survive it.

| Module | What it owns |
|---|---|
| `clock.py` | Wall time or simulated time. A thirty minute session gap in 1.8 seconds. |
| `population.py` | Who exists, and a Zipf weight so a few users take most of the traffic. |
| `arrivals.py` | Who is on the site right now. Visits start, run for a budget, and end. |
| `session.py` | The funnel, the referrer chain, and the ground truth session boundary. |
| `events.py` | Assembles the record. |
| `produce.py` | Rate control, late-event injection, duplicate emission. |
| `sinks.py` | jsonl, stdout, memory, null, and an untested Kafka path. |

## Measured on 2026-08-14

Every figure here comes out of `scripts/measure_generator.py` on this machine on that
date. Re-run it and they move. Sandbox speed varies by about 1.8x between days, so
treat the ratios as the durable part.

**Throughput ceiling.** 37,226 events per wall second with the pacer switched off,
median of five passes after a discarded warmup, range 37,012 to 37,327.

**Rate control.** Ask for 50, 500 or 5,000 events per second and the run comes back
1.10, 0.21 and 0.11 percent high. The error shrinks as the rate rises, because the
overshoot is a fixed per-sleep cost divided by more events.

**Partition skew at six partitions.** Heavier tail, busier partition.

| alpha | share of traffic taken by the top 50 users | busiest partition over even | quietest |
|---|---|---|---|
| 0.0 | 1.0% | 1.071 | 0.949 |
| 0.6 | 13.7% | 1.129 | 0.893 |
| 1.0 | 49.5% | 1.274 | 0.804 |
| 1.4 | 85.5% | 1.212 | 0.786 |

The 1.4 row is lower than the 1.0 row and that is not noise. At alpha 1.4 the visit
pool cannot keep 150 distinct people on the site, so the busiest users spend their
draws extending a visit they are already in rather than starting new ones. The
generator reports that as `admissions_deflected` rather than hiding it.

**Lateness.** At an 8 percent injection rate the realised share is 7.88 percent over
121,123 events. Median 12.07 seconds. p95 118 seconds and p99 297 seconds. The longest was
2,494 seconds. 12.3 percent of late events are more than a minute behind. A watermark of one
minute therefore drops about 1 percent of the whole stream, which is the tradeoff day 3
has to price.

**Session shape.** 900,000 events, 105,730 sessions, 2,000 users. Median 5 events per
session and p95 26. A single event is 11.3 percent of them and 32.7 percent
contain a checkout.

## The thing day 2 got wrong

The first build of the generator sampled a user from the whole population on every
event. It looked fine. Then the measurement said 2,000 users and 2,000 sessions, a
median of 110 events each, and a conversion rate of 100 percent.

Nobody ever left. With 2,000 users and 500 events per second, every user gets an event
every four seconds forever, so no inactivity gap ever reaches thirty minutes and no
session ever ends. The ground truth column the whole project scores against was one
session per user.

`arrivals.py` is the fix. A visit holds a user for a budgeted number of events and then
releases them, so a session ends because the visit ended and not because the clock ran
out.

## The number day 3 has to live with

The pipeline recovers sessions from a thirty minute inactivity gap. The generator knows
where the real boundary was. They disagree, and the disagreement is not small.

| users | alpha | uniform prediction for the return gap | sessions | boundaries the gap rule misses |
|---|---|---|---|---|
| 2,000 | 1.0 | 24 s | 35,279 | 94.4% |
| 20,000 | 1.0 | 240 s | 35,434 | 72.4% |
| 100,000 | 1.0 | 1,200 s | 35,347 | 57.9% |
| 300,000 | 1.0 | 3,600 s | 35,511 | 50.7% |
| 300,000 | 0.0 | 3,600 s | 35,664 | **5.6%** |

The obvious prediction is that a visitor returns after `users * visit_events / rate`
seconds. That column is in the table because it is wrong. At 300,000 users it predicts
a return gap of an hour, twice the threshold, and half the boundaries still vanish.

The last row is the control. Same population and same rate, with flat weights in
place of the Zipf tail. The miss rate falls from 50.7 percent to 5.6 percent. The cause is the
tail and not the population size. Most visits belong to the busiest few users, and
those people come back in seconds however many other users exist.

So a session count off this pipeline is a lower bound, and how much of a lower bound
depends on the shape of the user distribution rather than on anything the streaming job
does. Day 3 measures it against `session_hint` rather than assuming it away.

## Status

Day 2 of 7.

- [x] Day 1: compose stack, event schema, decisions doc
- [x] Day 2: producer with rate control. Late events, duplicates and a visit model
- [ ] Day 3: streaming job. parse / dedupe / watermark / sessionize
- [ ] Day 4: feature extraction, Snowflake MERGE sink
- [ ] Day 5: latency and throughput metrics
- [ ] Day 6: failure testing, replay, duplicate verification
- [ ] Day 7: benchmarks and writeup

## Limitations, today

- **The Kafka sink has never touched a broker.** `build_sink("kafka", ...)` raises
  rather than returning something that looks like it works. Day 3.
- **The visit pool holds one state per user seen** and never evicts. At a few million
  users that is a memory problem.
- **A duplicate is always a whole-record copy.** A real retry storm also produces
  partial and reordered writes, and none of that is modelled.
- **The event mix is hand chosen.** The funnel probabilities came from what looks
  plausible, not from a real site. Any conclusion about conversion rates is a
  conclusion about `session.py`.
- **`admissions_deflected` changes the visit length distribution** when it fires, and
  the report says how often rather than correcting for it.

Design decisions and open questions are in `docs/decisions.md`.
