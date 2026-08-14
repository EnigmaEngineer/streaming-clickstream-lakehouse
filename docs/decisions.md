# Decisions

Running notes on things I chose and why. Mostly so I remember the reasoning in three weeks.

## Kafka in KRaft mode, no Zookeeper

Fewer containers, and Zookeeper is deprecated for this. Single node, replication factor 1.
Not production config, and the README says so.

## MinIO instead of S3

The staging step writes Parquet before loading to Snowflake. MinIO speaks the S3 API, so
the code path is identical and nothing needs an AWS account to run. Swapping to real S3
is an endpoint change.

## Snowflake is the only paid dependency

Everything else runs locally. For anyone without a Snowflake trial there is a
`--sink=duckdb` fallback planned for day 4 so the pipeline still demonstrates the MERGE
logic end to end. DuckDB does not have identical MERGE semantics, so the fallback is
for demonstration, not for the benchmark numbers.

## user_id as the partition key

Session windows need all of a user's events together. Partitioning by `user_id` means
Spark can keep session state local. Risk is hot partitions if one user is very active.
The generator has a heavy-tail user distribution specifically so I can see whether that
actually bites.

Measured on day 2 at six partitions. At alpha 1.0 the busiest partition carries 1.274
times an even share. Not nothing, and not the disaster the phrase "hot partition"
suggests, because CRC32 over five thousand keys spreads the heavy users around rather
than stacking them.

Worth knowing and easy to miss: librdkafka partitions a key with CRC32 and the Java
producer uses murmur2. The co-location guarantee holds inside one client library. A
Java producer and a Python producer writing the same key send it to two different
partitions.

## The visit pool, added day 2

Day 2's first build drew a user from the whole population on every event. That gives
every user an event every few seconds forever, so no thirty minute gap ever opens and
no session ever ends. The measurement caught it. 2,000 users produced exactly 2,000
sessions with a median of 110 events each.

`generator/arrivals.py` replaces the draw. A fixed number of visits are open at once.
A visit holds a user for a budgeted number of events and then releases them, so the
session boundary is an event in the model rather than an accident of the clock.

Two consequences worth writing down. The pool size and the rate together set the think
time, so there is no third knob for it. And a heavy tail fights the pool, because the
busiest users are almost always already on the site. When the admission draw keeps
returning somebody who is here, their current visit gets extended instead. That is
counted as `admissions_deflected` rather than silently shrinking the pool.

## The gap rule is a lower bound, and the tail is why

The pipeline will recover sessions from thirty minutes of inactivity. The generator
knows the truth. Measured on day 2, at 300,000 users and a Zipf alpha of 1.0, the gap
rule misses 50.7 percent of real session boundaries. At the same population with flat
weights it misses 5.6 percent.

So the miss rate is a property of the user distribution and not of the streaming job.
No watermark setting recovers a boundary that was never visible in the data. Day 3
reports the number rather than pretending the recovered count is the real one.

## Open questions

- Late events arriving after session state is evicted. Widen the watermark or count the
  loss? Need to see the actual distribution first.
- Whether to dedupe on `event_id` in the stream or at the sink. Stream dedupe needs state
  and a time bound. Sink dedupe is simpler but means the intermediate is dirty.
  Leaning toward stream dedupe with a 1-hour bound, measured on day 3.
