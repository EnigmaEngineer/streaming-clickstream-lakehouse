# Streaming Clickstream Lakehouse

Kafka to Spark Structured Streaming to Snowflake, with session windows and exactly-once
writes. Built to handle the parts of streaming that actually break: late events,
duplicates, and reruns.

```bash
./scripts/setup.sh                                        # kafka + minio up, topic created
python -m generator.produce --rate 200 --seconds 5 --report
python -m tests.run_all                                   # 47 checks, no install needed
python -m tests.run_spark                                 # 22 checks, needs pyspark
python scripts/measure_generator.py                       # every generator number below
```

The generator and `tests/run_all.py` need nothing installed. Standard library only.
`stream/` and `tests/run_spark.py` need pyspark 3.5 or newer. Both suites are real
and both have to pass. Neither skips when its dependency is missing.

## Architecture

```
  generator          Kafka                 Spark Structured Streaming            Snowflake
 ┌──────────┐    ┌───────────┐   ┌──────────────────────────────────────┐   ┌──────────────┐
 │ synthetic│───▶│clickstream│──▶│ parse → watermark → dedupe → session │──▶│ sessions     │
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

`stream/` reads it back.

| Module | What it owns |
|---|---|
| `schema.py` | The declared wire schema and the one place a timestamp is built. |
| `sessionize.py` | parse, watermark, dedupe, session windows, and the door they compose into. |
| `job.py` | Source, sink, and the query progress numbers pulled off the running query. |
| `scoring.py` | The comparison against `session_hint`. Nothing in the pipeline imports it. |

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
2,494 seconds. 12.3 percent of late events are more than a minute behind.

This block used to end with a sentence saying a one minute watermark would therefore
drop about 1 percent of the stream. Day 3 measured it and dropped zero. The sentence
was a prediction written in the voice of a measurement and it is corrected below.

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

## The streaming job

`stream/` is the pipeline. Four steps, and the order of them is not free.

```bash
pip install -r requirements.txt
python -m generator.produce --rate 8 --seconds 7200 --speedup 500000 \
    --sink file --out /tmp/all.jsonl
mkdir -p /tmp/events && (cd /tmp/events && split -n l/12 -d --additional-suffix=.json /tmp/all.jsonl shard-)
python -m scripts.flush_shard --dir /tmp/events --hours 6
python -m stream.job --source file --path /tmp/events --out /tmp/sessions \
    --checkpoint /tmp/ckpt --available-now --files-per-trigger 1 --progress
python -m scripts.score_sessions --events /tmp/events --sessions /tmp/sessions
python -m tests.run_spark
```

`build_sessions` in `stream/sessionize.py` is the only route from a payload to a
session. The job, the scorer and the tests all call it, so none of them can drift
into testing a pipeline that is not the one that runs.

**Why the watermark is set before the dedupe.** `dropDuplicates(["event_id"])` is
what most examples use and it keeps every event_id it has ever seen. event_id is not
the watermark column, so Spark has nothing to expire the entry against and the state
grows for as long as the stream runs. `dropDuplicatesWithinWatermark` bounds that
state on the watermark instead. Worth being honest about what this corpus proves.
The generator's duplicate carries the original `event_ts` and only re-stamps
`ingest_ts`, so all three available dedupe forms catch it. The case that separates
them is a retry whose event time moved, and that shape is not in the data.

### The watermark is not what admits late data

The obvious expectation is that a two minute watermark against a lateness tail
reaching seventeen minutes throws away a lot of events. It throws away none.

Measured on 2026-08-15 over 58,182 events in 12 shards, one shard per trigger, on
Spark 3.5.6 and Java 11. `dropped` is `numRowsDroppedByWatermark` read off the query
progress rather than counted by hand.

| watermark | session gap | dropped by session window | dropped by dedupe | sessions out | events covered |
|---|---|---|---|---|---|
| 10 seconds | 30 minutes | 0 | 2 | 3,252 | 57,598 |
| 2 minutes | 30 minutes | 0 | 0 | 3,252 | 57,600 |
| 30 minutes | 30 minutes | 0 | 0 | 3,252 | 57,600 |
| 10 seconds | 2 minutes | 0 | 2 | 5,850 | 57,598 |
| 2 minutes | 2 minutes | 0 | 0 | 5,852 | 57,600 |

The corpus holds 58,181 rows carrying 57,600 distinct `event_id`, so 581 are the
duplicates the generator emitted on purpose. Every row where nothing was dropped
covers exactly 57,600 events. The dedupe removed all 581 copies and lost nothing. The
two rows at a 10 second watermark cover 57,598, which is the 2 the dedupe reported
refusing. Both sides of that reconcile to the unit and neither is a spot check.

The last row is worth a second look. Same 2 minute gap as the row above it and 5,852
sessions against 5,850. A wider watermark holds state longer, so two sessions that the
narrower setting closed and never reopened stayed open long enough to absorb a later
event. That is the watermark changing the answer without dropping anything.

A plain count of rows sitting below the watermark at the start of their batch says
98 rows at a 10 second watermark and 25 at 2 minutes. That calculation is in the
day 3 audit and it runs in Python with no Spark in it. Spark dropped 2.

The reason is the session gap. A session window ends one gap after its last event,
so a late event is only refused when even the new session it would open has already
closed. Add the gap to each row's event time and the same independent calculation
predicts **zero** at a 30 minute gap, which is what Spark did at all three watermark
settings. At a 2 minute gap it predicts 23 and Spark still dropped 0, with and
without the dedupe operator in front, so the gap aware model is an upper bound and
not the whole rule. What loosens it further was not chased and is written down
rather than guessed at.

The practical version. On a session windowed pipeline the watermark is buying output
latency and state size, not late data. The gap is doing the admission. Tuning the
watermark to protect against data loss is tuning the wrong number.

### Scoring against ground truth

`session_hint` is the real boundary and the pipeline never reads it.
`stream/scoring.py` joins the raw events back onto the emitted windows, so what gets
graded is the pipeline's actual output and not a variant carrying a truth column.

| corpus | max lateness | gap rule misses, per the generator | boundaries lost, per Spark | splits | events dropped |
|---|---|---|---|---|---|
| seed 7 | 1,040 s | 3,770 | 3,770 | 0 | 0 |
| seed 11 | 2,035 s | 3,721 | 3,723 | 0 | 1 |
| seed 7, fat tail | 80,211 s | 3,691 | 3,770 | 142 | 885 |

Row one is the day 2 prediction reproduced against the real Spark job, exactly, on
7,022 true sessions against 3,252 recovered. Boundary miss rate 0.5369.

Rows two and three are the check on whether that exactness means anything. It is
conditional. The generator's rule walks events in arrival order and tracks the
newest event time it has seen. Spark sorts by event time and does not care about
arrival order. Those two can only disagree when a late event lands inside a gap
wider than the threshold, which needs lateness above the gap. At 1,040 seconds
against an 1,800 second gap that is arithmetically impossible and the two agree to
the unit. At 2,035 seconds two events manage it. At 80,211 seconds the answers come
apart by 79 boundaries, 142 true sessions get split and 885 events are refused
outright.

Spark returning 3,770 on rows one and three is a coincidence of the shared seed
rather than a pattern. Row two, on a different seed, returns 3,723.

## Status

Day 3 of 7.

- [x] Day 1: compose stack, event schema, decisions doc
- [x] Day 2: producer with rate control. Late events, duplicates and a visit model
- [x] Day 3: streaming job. parse / dedupe / watermark / sessionize
- [ ] Day 4: feature extraction, Snowflake MERGE sink
- [ ] Day 5: latency and throughput metrics
- [ ] Day 6: failure testing, replay, duplicate verification
- [ ] Day 7: benchmarks and writeup

## Limitations, today

- **No broker has been involved at any point.** The producer's Kafka sink raises
  rather than pretending, and the job's Kafka source is written and unrun. Day 3
  sessionized from a file source, which exercises the same watermark and dedupe and
  session window code and proves nothing about offsets or partition assignment.
  `crc32_partition` still computes librdkafka's partitioner in Python with nothing
  to check it against.
- **A bounded run leaves sessions in state.** Append mode emits a window once the
  watermark passes its end, and a run that stops reading files never advances the
  watermark again. The first full run emitted 2,117 sessions and left 34,481 events
  sitting in state. `scripts/flush_shard.py` is a harness step that pushes one
  sentinel event days into the future to close them. A real stream does not need it.
  Any number here that came from a flushed run is a number about a bounded rerun.
- **The gap aware model of late data admission is an upper bound.** It predicts the
  30 minute gap result exactly and over-predicts at a 2 minute gap. Something further
  loosens the filter and it has not been identified.
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
