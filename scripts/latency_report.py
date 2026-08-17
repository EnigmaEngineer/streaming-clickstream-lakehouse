"""Print the lag budget. Formatting only.

The arithmetic is in stream/lag.py and the measurements are in stream/latency.py, both
of which have tests. This file reads paths and prints JSON. ot-037 on the program side
is about figures whose producing code nothing tests, and the split is the answer to it.

    python -m scripts.latency_report --events /tmp/events --sessions /tmp/sessions \\
        --progress /tmp/progress.json
"""

import argparse
import json
import sys

from pyspark.sql import functions as F

from scripts.flush_shard import SENTINEL_USER
from stream.job import spark_session
from stream.lag import batch_latency, first_event_floor_s, lag_budget
from stream.latency import emission_lag, floor_check, ingest_lag, session_span
from stream.sessionize import DEFAULT_GAP, DEFAULT_WATERMARK


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="report where the end to end lag goes")
    p.add_argument("--events", required=True, help="directory of JSONL shards")
    p.add_argument("--sessions", required=True, help="parquet written by stream.job")
    p.add_argument("--progress", help="json file holding query.recentProgress, optional")
    p.add_argument("--gap", default=DEFAULT_GAP)
    p.add_argument("--watermark", default=DEFAULT_WATERMARK)
    p.add_argument("--keep-sentinel", action="store_true")
    a = p.parse_args(argv)

    spark = spark_session(app="latency-report")
    spark.sparkContext.setLogLevel("ERROR")
    events = spark.read.json(a.events)
    events = events.withColumn("event_ts", F.to_timestamp("event_ts")).withColumn(
        "ingest_ts", F.to_timestamp("ingest_ts")
    )
    sessions = spark.read.parquet(a.sessions)
    if not a.keep_sentinel:
        # The flush sentinel is a fake event six hours past the corpus. Left in, it
        # owns the largest emission lag in the run and the p99 becomes a statistic
        # about the harness.
        events = events.where(F.col("user_id") != SENTINEL_USER)
        sessions = sessions.where(F.col("user_id") != SENTINEL_USER)

    ing = ingest_lag(events)
    emit = emission_lag(events, sessions, a.watermark)
    spans = session_span(sessions)
    out = {
        "gap": a.gap,
        "watermark": a.watermark,
        "ingest_lag_s": ing,
        "emission_lag_s": emit,
        "session_duration_s": spans,
        "floor_check": floor_check(sessions, a.gap, a.watermark),
    }

    proc = {}
    if a.progress:
        with open(a.progress, encoding="utf-8") as fh:
            proc = batch_latency(json.load(fh))
        out["processing"] = proc

    # p50 of the batch plan time is the fairest per session charge available, and it
    # is generous. See lag_budget.
    add_p50 = (proc.get("add_batch_ms") or {}).get("p50", 0.0)
    out["budget"] = lag_budget(a.gap, a.watermark, ing.get("p50", 0.0), add_p50)
    out["budget"]["first_event_floor_p95_s"] = first_event_floor_s(
        a.gap, a.watermark, spans.get("p95", 0.0)
    )

    print(json.dumps(out, indent=2), file=sys.stdout)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
