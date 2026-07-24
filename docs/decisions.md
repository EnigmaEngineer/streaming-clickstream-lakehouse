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
Spark can keep session state local. Risk is hot partitions if one user is very active —
the generator has a heavy-tail user distribution specifically so I can see whether this
actually bites.

## Open questions

- Late events arriving after session state is evicted. Widen the watermark or count the
  loss? Need to see the actual distribution first.
- Whether to dedupe on `event_id` in the stream or at the sink. Stream dedupe needs state
  and a time bound. Sink dedupe is simpler but means the intermediate is dirty.
  Leaning toward stream dedupe with a 1-hour bound, measured on day 3.
