"""Print the session scorecard. Formatting only.

The arithmetic is in stream/scoring.py, which has tests. This file reads two paths
and prints JSON. That split is deliberate and it is the ot-037 rule on the program
side, which is that a script producing a published figure should not be the only
place that figure's logic lives.

    python -m scripts.score_sessions --events /tmp/events --sessions /tmp/sessions
"""

import argparse
import json
import sys

from pyspark.sql import functions as F

from scripts.flush_shard import SENTINEL_USER
from stream.job import spark_session
from stream.scoring import feature_bias, score


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="score recovered sessions against session_hint")
    p.add_argument("--events", required=True, help="directory of JSONL shards")
    p.add_argument("--sessions", required=True, help="parquet written by stream.job")
    p.add_argument("--keep-sentinel", action="store_true")
    p.add_argument("--features", action="store_true", help="also print the feature bias table")
    a = p.parse_args(argv)

    spark = spark_session(app="score-sessions")
    spark.sparkContext.setLogLevel("ERROR")
    events = spark.read.json(a.events)
    events = events.withColumn("event_ts", F.to_timestamp("event_ts"))
    sessions = spark.read.parquet(a.sessions)
    if not a.keep_sentinel:
        events = events.where(F.col("user_id") != SENTINEL_USER)
        sessions = sessions.where(F.col("user_id") != SENTINEL_USER)

    out = {"scorecard": score(events, sessions)}
    if a.features:
        out["feature_bias"] = feature_bias(events, sessions)
    print(json.dumps(out, indent=2), file=sys.stdout)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
