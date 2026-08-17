"""Run the job across watermark and gap settings and print what each one cost.

The point of this is one question. If events arrive up to seventeen minutes late and
the watermark is two minutes, how many events does the pipeline throw away?

The answer on this corpus is none, and the reason is not the watermark. See the
README. This script is what turns that from a claim into a table.

Every run is a fresh checkpoint, because a checkpoint carries the watermark and the
state store from the previous configuration and reusing one would score the new
setting against the old setting's state.

    python -m scripts.watermark_sweep --path /tmp/events --work /tmp/sweep
"""

import argparse
import json
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace

from stream.job import run

# Watermark, session gap. The pairs are chosen so that one dimension moves at a time,
# with a final row where the gap is smaller than the lateness tail.
COMBOS = [
    ("10 seconds", "30 minutes"),
    ("2 minutes", "30 minutes"),
    ("30 minutes", "30 minutes"),
    ("10 seconds", "2 minutes"),
    ("2 minutes", "2 minutes"),
]


def one(path: str, work: Path, watermark: str, gap: str, files_per_trigger: int) -> dict:
    tag = f"{watermark}-{gap}".replace(" ", "")
    out = work / f"sessions-{tag}"
    ckpt = work / f"ckpt-{tag}"
    for d in (out, ckpt):
        shutil.rmtree(d, ignore_errors=True)
    args = SimpleNamespace(
        source="file",
        path=path,
        brokers=None,
        topic=None,
        out=str(out),
        checkpoint=str(ckpt),
        # sink and duckdb arrived on day 4 and this namespace did not get them, so
        # every run of this script since raised AttributeError inside stream.job.run.
        # Nothing caught it because nothing runs scripts/. tests/test_structural.py
        # now compares this call against the attributes run() really reads.
        sink="parquet",
        duckdb=None,
        gap=gap,
        watermark=watermark,
        files_per_trigger=files_per_trigger,
        shuffle_partitions=8,
        available_now=True,
        seconds=0.0,
        log_level="ERROR",
    )
    summary = run(args)
    ops = summary["state_operators"]
    return {
        "watermark": watermark,
        "gap": gap,
        "input_rows": summary["input_rows"],
        "dropped_by_session_window": ops.get("defaultName", {}).get("dropped_by_watermark", 0),
        "dropped_by_dedupe": ops.get("dedupeWithinWatermark", {}).get("dropped_by_watermark", 0),
        "dedupe_state_rows_at_end": ops.get("dedupeWithinWatermark", {}).get("state_rows", 0),
        "session_state_rows_at_end": ops.get("defaultName", {}).get("state_rows", 0),
        "out": str(out),
    }


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="sweep watermark and session gap")
    p.add_argument("--path", required=True)
    p.add_argument("--work", required=True)
    p.add_argument("--files-per-trigger", type=int, default=1)
    a = p.parse_args(argv)

    work = Path(a.work)
    work.mkdir(parents=True, exist_ok=True)
    rows = []
    for watermark, gap in COMBOS:
        # A SparkSession per combo. getOrCreate would hand back the first one and
        # every later row would silently run on the first row's config.
        row = one(a.path, work, watermark, gap, a.files_per_trigger)
        rows.append(row)
        print(json.dumps(row), file=sys.stderr, flush=True)
    print(json.dumps(rows, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
