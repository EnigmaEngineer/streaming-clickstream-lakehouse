# Decisions

Running notes on things I chose and why. Mostly so I remember the reasoning in three weeks.

Two sections below are marked as corrected. The README was kept current every day and
this file was not, so it ended up carrying a claim the README had already retracted and two
open questions the work had already answered. Leaving the wrong version visible
next to the right one is more useful than a silent edit.

## Kafka in KRaft mode, no Zookeeper

Fewer moving parts, and Zookeeper is deprecated for this. Single node, replication
factor 1. Not production config, and the README says so.

Two bring-up paths exist and only one of them has run. `docker/docker-compose.yml` needs
a Docker daemon. `scripts/bootstrap-local.sh` starts the same broker on the JVM directly
and is the one every measurement here came off. Listeners are pinned to `127.0.0.1`
because Kafka resolves its own address through `getLocalHost()` and `/etc/hosts` is not
writable on the machine this runs on.

## MinIO instead of S3, corrected

The original note said the staging step writes Parquet to MinIO before loading to
Snowflake, so the code path would be identical to S3 and swapping over would be an
endpoint change.

None of that was ever built. Search the whole repo for `s3` or `minio` or `boto` or
`aws` and you get one hit. It is a comment. The sink writes Parquet to a local path and
loads it into DuckDB from there. MinIO sat in the compose file the whole time, pulling an
image and holding a volume that nothing ever connected to. It is gone now.

`.env.example` went the same way. Nothing in this repo reads an environment variable.
There is no `os.environ` and no `getenv` anywhere in it. Every setting is a command line
flag. A file listing eight environment variables, five of them Snowflake credentials,
was asking a cloner to configure something no code would read.

## Snowflake is the only paid dependency

Everything else runs locally. `--sink duckdb` was planned from the start and landed later,
so the pipeline demonstrates the MERGE end to end without an account.

DuckDB and Snowflake do not have identical MERGE semantics. `warehouse/sql.py` holds both
statements and says which have run. DuckDB, all of them. Snowflake, none, ever.

## user_id as the partition key, corrected

The original note said session windows need all of a user's events together and that
partitioning by `user_id` lets Spark keep session state local. **The second half is
wrong.** Spark shuffles by the grouping key on its own. The physical plan for the session
window aggregation carries `Exchange hashpartitioning(user_id, 8)`, so whatever the
broker did with the key is undone before the aggregation sees it.

Keying still buys per user ordering at the broker, which is real and is not what the
sentence claimed. That claim sat unchecked in a README for most of the project. `explain`
answers it in one command.

Skew measured at six partitions. At alpha 1.0 the busiest partition carries
1.274 times an even share. Not nothing, and not the disaster the phrase "hot partition"
suggests, because CRC32 over five thousand keys spreads the heavy users around rather
than stacking them.

Worth knowing and easy to miss. librdkafka partitions a key with CRC32 and the Java
producer uses murmur2. I put the same 200 keys through both clients and they
disagreed on 169. The co-location guarantee holds inside one client library and across
two it holds for nothing.

## The visit pool

The first build drew a user from the whole population on every event. That gives
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

The pipeline recovers sessions from thirty minutes of inactivity. The generator knows the
truth. At 300,000 users and a Zipf alpha of 1.0 the gap rule misses
50.7 percent of real session boundaries. At the same population with flat weights it
misses 5.6 percent.

So the miss rate is a property of the user distribution and not of the streaming job. No
watermark setting recovers a boundary that was never visible in the data. The job reports
the number rather than pretending the recovered count is the real one.

## The two open questions, answered

Both were written before any code and both sat here long after the answers had been
published in the README.

**Late events arriving after session state is evicted.** Neither. The question assumed
the watermark decides admission and it does not. A session window ends one gap after its
last event, so a late row is only refused once the window it would open has already
closed. Measured over 58,182 rows. Watermarks of 10 seconds, 2 minutes and 30
minutes all dropped zero at a 30 minute gap. The watermark buys output latency and state
size. The gap does the admitting.

**Dedupe in the stream or at the sink.** In the stream, and the bound comes from
`dropDuplicatesWithinWatermark` rather than from a fixed hour. `dropDuplicates` keeps
every `event_id` it has ever seen, and `event_id` is not the watermark column, so Spark
has nothing to expire an entry against. Worth being honest that this corpus does not
separate the three available forms, because its duplicate carries the original
`event_ts`. The case that separates them is a retry whose event time moved.
