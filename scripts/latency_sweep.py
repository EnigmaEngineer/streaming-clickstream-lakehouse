"""Sweep the session gap and report what each setting costs in lag and in accuracy.

The gap is the only knob that moves the emission floor, and it moves the boundary miss
rate at the same time and in the opposite direction. A short gap emits sooner and
splits real visits. A long gap waits and merges them. That is the tradeoff this
project is actually about and day 3 measured only one half of it.

Resumable on purpose. Each arm is a fresh SparkSession over 58 thousand events and
takes about thirty seconds, and the shell this runs in kills a call at roughly 178
seconds. So results append to a JSONL file and an arm already present is skipped. A
sweep that cannot be finished in one call has to survive being run in pieces.

    python -m scripts.latency_sweep --path /tmp/events --work /tmp/sweep \\
        --gaps "1 minute,5 minutes" --out /tmp/sweep/rows.jsonl
"""

import argparse
import json
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace

from pyspark.sql import functions as F

from scripts.flush_shard import SENTINEL_USER
from stream.job import run, spark_session
from stream.lag import batch_latency, emission_floor_s
from stream.scoring import score
from stream.sessionize import DEFAULT_WATERMARK


def done_gaps(path: Path) -> set:
    if not path.exists():
        return set()
    out = set()
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                out.add(json.loads(line)["gap"])
    return out


def one(path: str, work: Path, gap: str, watermark: str, files_per_trigger: int) -> dict:
    tag = gap.replace(" ", "")
    out_dir = work / f"sessions-{tag}"
    ckpt = work / f"ckpt-{tag}"
    for d in (out_dir, ckpt):
        shutil.rmtree(d, ignore_errors=True)
    args = SimpleNamespace(
        source="file",
        path=path,
        brokers=None,
        topic=None,
        out=str(out_dir),
        checkpoint=str(ckpt),
        sink="parquet",
        duckdb=None,
        gap=gap,
        watermark=watermark,
        files_per_trigger=files_per_trigger,
        shuffle_partitions=8,
        available_now=True,
        seconds=0.0,
        log_level="ERROR",
        # crash_batch and crash_point arrived on day 6. Same shape as the day 4
        # omission above, and this time tests/test_structural.py failed on the first
        # run after run() started reading them.
        crash_batch=None,
        crash_point="after-merge",
    )
    summary = run(args)
    ops = summary["state_operators"]
    row = {
        "gap": gap,
        "watermark": watermark,
        "emission_floor_s": emission_floor_s(gap, watermark),
        "input_rows": summary["input_rows"],
        "dropped_by_session_window": ops.get("defaultName", {}).get("dropped_by_watermark", 0),
        "dropped_by_dedupe": ops.get("dedupeWithinWatermark", {}).get("dropped_by_watermark", 0),
        "processing": batch_latency(summary["raw_progress"]),
        "sessions_out": str(out_dir),
    }

    # Scoring needs its own session, because run() stopped the one it built. Reusing a
    # stopped session is the kind of failure that looks like a data problem.
    spark = spark_session(app=f"score-{tag}")
    spark.sparkContext.setLogLevel("ERROR")
    events = spark.read.json(path).withColumn("event_ts", F.to_timestamp("event_ts"))
    events = events.where(F.col("user_id") != SENTINEL_USER)
    sessions = spark.read.parquet(str(out_dir)).where(F.col("user_id") != SENTINEL_USER)
    card = score(events, sessions)
    spark.stop()
    row["recovered_sessions"] = card["recovered_sessions"]
    row["true_sessions"] = card["true_sessions_scored"]
    row["boundary_miss_rate"] = card["boundary_miss_rate"]
    row["split_rate"] = card["split_rate"]
    row["events_unmatched"] = card["events_unmatched"]
    return row


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="sweep the session gap for lag against accuracy")
    p.add_argument("--path", required=True)
    p.add_argument("--work", required=True)
    p.add_argument("--out", required=True, help="jsonl, appended to, arms already present are skipped")
    p.add_argument("--gaps", required=True, help="comma separated, e.g. '1 minute,5 minutes'")
    p.add_argument("--watermark", default=DEFAULT_WATERMARK)
    p.add_argument("--files-per-trigger", type=int, default=1)
    a = p.parse_args(argv)

    work = Path(a.work)
    work.mkdir(parents=True, exist_ok=True)
    out = Path(a.out)
    already = done_gaps(out)
    wanted = [g.strip() for g in a.gaps.split(",") if g.strip()]
    if not wanted:
        raise ValueError("no gaps requested, which would exit 0 having measured nothing")

    for gap in wanted:
        if gap in already:
            print(f"skip {gap}, already in {out}", file=sys.stderr, flush=True)
            continue
        row = one(a.path, work, gap, a.watermark, a.files_per_trigger)
        with out.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row) + "\n")
        print(json.dumps({k: v for k, v in row.items() if k != "processing"}), file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
