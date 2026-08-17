"""The streaming job.

Reads a clickstream and writes one row per session. One source and one sink with one
transformation in between. That transformation lives in sessionize.py so that the
tests and the scorer run the same code the job does.

Run it against a directory of JSONL shards:

    python -m stream.job --source file --path /tmp/events --out /tmp/sessions \\
        --checkpoint /tmp/ckpt --available-now --files-per-trigger 1 --progress

Against a broker:

    python -m stream.job --source kafka --brokers 127.0.0.1:9092 \\
        --topic clickstream.events --out /tmp/sessions --checkpoint /tmp/ckpt

The kafka path needs the spark-sql-kafka connector on the classpath. See the README
for which of these two has actually been run.
"""

import argparse
import json
import sys

from pyspark.sql import SparkSession

from stream.sessionize import DEFAULT_GAP, DEFAULT_WATERMARK, build_sessions


def spark_session(app: str = "clickstream-sessionize", shuffle_partitions: int = 8) -> SparkSession:
    """Spark defaults to 200 shuffle partitions. On two cores and a session window
    keyed by user that is 200 mostly empty state stores per micro batch, and the
    scheduling cost of them dominates everything else the job does."""
    return (
        SparkSession.builder.appName(app)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def read_file_source(spark: SparkSession, path: str, files_per_trigger: int | None):
    """One JSON object per line, read as text.

    Text rather than the JSON reader on purpose. The JSON reader would parse and
    infer in the source, which puts the schema decision somewhere the tests cannot
    reach. Reading the line and calling from_json in sessionize.parse keeps parsing
    inside the code under test, and it makes the file source and the Kafka source
    hand the pipeline the same shape.
    """
    reader = spark.readStream.format("text")
    if files_per_trigger:
        # The knob that makes an offline run behave like a stream. One file per
        # trigger means the watermark advances between batches instead of the whole
        # corpus arriving as one batch with one final watermark.
        reader = reader.option("maxFilesPerTrigger", str(files_per_trigger))
    return reader.load(path)


def read_kafka_source(spark: SparkSession, brokers: str, topic: str, starting: str = "earliest"):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", starting)
        .load()
    )


def progress_summary(query) -> dict:
    """What the run actually did, pulled off the query rather than counted by hand.

    `numRowsDroppedByWatermark` is the number this job exists to expose. It is
    reported per stateful operator and there are two of them here. The dedupe and
    the session grouping are kept apart rather than summed, because adding them
    would double count an event the dedupe already refused.
    """
    batches = query.recentProgress
    ops: dict[str, dict] = {}
    rows_in = 0
    for b in batches:
        rows_in += b.get("numInputRows", 0)
        for i, op in enumerate(b.get("stateOperators", [])):
            name = op.get("operatorName", f"op{i}")
            slot = ops.setdefault(name, {"dropped_by_watermark": 0, "state_rows": 0, "memory_bytes": 0})
            slot["dropped_by_watermark"] += op.get("numRowsDroppedByWatermark", 0)
            # State rows and memory are a level, not a total, so the last batch wins.
            slot["state_rows"] = op.get("numRowsTotal", 0)
            slot["memory_bytes"] = op.get("memoryUsedBytes", 0)
    return {"batches": len(batches), "input_rows": rows_in, "state_operators": ops}


def duckdb_sink(db_path: str, batches: list):
    """A foreachBatch function that merges each micro batch into DuckDB.

    Two things about this are honest limitations rather than design.

    It runs on the driver and collects the batch there, because a DuckDB file cannot
    be written by several executors at once. At session-row volume that is fine and at
    real volume it is not. The Snowflake shape this rehearses writes staged Parquet
    from the executors and merges from the stage, which is why warehouse/sql.py has a
    stage table in it rather than a plain INSERT.

    It reopens the connection per batch. Structured Streaming can re-execute a batch
    after a failure, and a connection held open across that is a connection holding a
    half applied transaction. Reopening costs a few milliseconds and removes the
    question. Exactly once here comes from the MERGE being idempotent on the session
    key, not from the sink being clever.
    """
    import duckdb  # noqa: PLC0415

    from warehouse.merge import apply_batch, ensure_tables  # noqa: PLC0415
    from warehouse.sql import SESSION_COLUMNS  # noqa: PLC0415

    def sink(df, epoch_id: int) -> None:
        rows = [tuple(r[c] for c in SESSION_COLUMNS) for r in df.select(*SESSION_COLUMNS).collect()]
        con = duckdb.connect(db_path)
        try:
            ensure_tables(con)
            out = apply_batch(con, rows)
        finally:
            con.close()
        out["epoch"] = epoch_id
        batches.append(out)

    return sink


def run(args) -> dict:
    spark = spark_session(shuffle_partitions=args.shuffle_partitions)
    spark.sparkContext.setLogLevel(args.log_level)

    if args.source == "file":
        raw = read_file_source(spark, args.path, args.files_per_trigger)
    else:
        raw = read_kafka_source(spark, args.brokers, args.topic)

    sessions = build_sessions(raw, gap=args.gap, delay=args.watermark)

    merged: list = []
    writer = sessions.writeStream.outputMode("append").option("checkpointLocation", args.checkpoint)
    if args.sink == "duckdb":
        writer = writer.foreachBatch(duckdb_sink(args.duckdb, merged))
    else:
        writer = writer.format("parquet").option("path", args.out)
    query = writer.trigger(availableNow=True).start() if args.available_now else writer.start()

    if args.available_now:
        query.awaitTermination()
    else:
        query.awaitTermination(args.seconds)
        query.stop()

    summary = progress_summary(query)
    # The raw per batch progress, kept beside the summary rather than instead of it.
    # stream/lag.py reads the durationMs blocks and progress_summary throws them away,
    # and a second query run to recover them would be a second measurement of a
    # different run.
    summary["raw_progress"] = [dict(b) for b in query.recentProgress]
    summary["gap"] = args.gap
    summary["watermark"] = args.watermark
    summary["sink"] = args.sink
    if args.sink == "duckdb":
        summary["merge_batches"] = merged
        summary["rows_landed"] = merged[-1]["rows_after"] if merged else 0
    spark.stop()
    return summary


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="sessionize a clickstream")
    p.add_argument("--source", default="file", choices=["file", "kafka"])
    p.add_argument("--path", help="directory of JSONL shards, for --source file")
    p.add_argument("--brokers", default="127.0.0.1:9092")
    p.add_argument("--topic", default="clickstream.events")
    p.add_argument("--out", help="parquet output directory, for --sink parquet")
    p.add_argument("--checkpoint", required=True)
    p.add_argument("--sink", default="parquet", choices=["parquet", "duckdb"])
    p.add_argument("--duckdb", help="duckdb file, for --sink duckdb")
    p.add_argument("--gap", default=DEFAULT_GAP)
    p.add_argument("--watermark", default=DEFAULT_WATERMARK)
    p.add_argument("--files-per-trigger", type=int, default=0)
    p.add_argument("--shuffle-partitions", type=int, default=8)
    p.add_argument("--available-now", action="store_true", help="drain what is there and stop")
    p.add_argument("--seconds", type=float, default=60.0, help="run length when not --available-now")
    p.add_argument("--log-level", default="ERROR")
    p.add_argument("--progress", action="store_true", help="print the query progress summary")
    p.add_argument("--progress-json", help="write the raw per batch progress here, for scripts.latency_report")
    a = p.parse_args(argv)

    if a.source == "file" and not a.path:
        p.error("--path is required with --source file")
    if a.sink == "parquet" and not a.out:
        p.error("--out is required with --sink parquet")
    if a.sink == "duckdb" and not a.duckdb:
        p.error("--duckdb is required with --sink duckdb")

    summary = run(a)
    if a.progress_json:
        with open(a.progress_json, "w", encoding="utf-8") as fh:
            json.dump(summary["raw_progress"], fh)
    if a.progress:
        # raw_progress is 14 batches of nested timing dicts. Printing it here would
        # bury the summary it sits beside, so it goes to a file or nowhere.
        print(json.dumps({k: v for k, v in summary.items() if k != "raw_progress"}, indent=2), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
