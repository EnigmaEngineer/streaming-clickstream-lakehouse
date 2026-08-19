"""The day 7 pass. Every figure this README published, re-measured, side by side.

    python scripts/reproduction_report.py                     # json
    python scripts/reproduction_report.py --chart docs/reproduction.png

The table below is a record rather than a computation. `published` is what the README
said before day 7 and the day it was written. `measured` is what the command in
`source` returned on 2026-08-19, on the corpus rebuilt from the command in the README's
"The streaming job" section. Every one was run in this session and the raw outputs are
in the day 7 audit.

Two things this script is deliberately not. It does not run the pipeline, because a
report that re-derives its own inputs would report agreement with itself. And it does
not decide anything, because `stream/repro.py` holds the rule and has tests over it.
This file reads a table and formats it.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from stream.repro import COUNTED, TIMING, Figure, report  # noqa: E402

FIGURES = [
    # --- generator, seeded, so these are the ones with no excuse ---
    Figure("generator sessions", 105730, 105730, COUNTED, "scripts/measure_generator.py"),
    Figure("gap rule miss pct, 300k alpha 1.0", 50.74, 50.74, COUNTED, "scripts/measure_generator.py"),
    Figure("gap rule miss pct, 300k alpha 0.0", 5.58, 5.58, COUNTED, "scripts/measure_generator.py"),
    Figure("busiest partition over even, alpha 1.0", 1.274, 1.274, COUNTED, "scripts/measure_generator.py"),
    Figure("realised late share", 0.0788, 0.0788, COUNTED, "scripts/measure_generator.py"),
    Figure("generator ceiling, events/s", 37226.0, 39193.0, TIMING, "scripts/measure_generator.py"),
    # --- the streaming run ---
    Figure("input rows", 58182, 58182, COUNTED, "stream.job --progress"),
    Figure("batches", 14, 14, COUNTED, "stream.job --progress"),
    Figure("dropped by session window", 0, 0, COUNTED, "stream.job --progress"),
    Figure("dropped by dedupe", 0, 0, COUNTED, "stream.job --progress"),
    # --- scoring against ground truth ---
    Figure("true sessions", 7022, 7022, COUNTED, "scripts.score_sessions"),
    Figure("recovered sessions", 3252, 3252, COUNTED, "scripts.score_sessions"),
    Figure("boundaries lost to merge", 3770, 3770, COUNTED, "scripts.score_sessions"),
    Figure("boundary miss rate", 0.5369, 0.5369, COUNTED, "scripts.score_sessions"),
    Figure("split true sessions", 0, 0, COUNTED, "scripts.score_sessions"),
    # --- the lag budget ---
    Figure("ingest lag p95, s", 3.0, 3.0, COUNTED, "scripts.latency_report"),
    Figure("ingest lag p99, s", 37.391, 37.391, COUNTED, "scripts.latency_report"),
    Figure("rows with lateness", 3562, 3562, COUNTED, "scripts.latency_report"),
    Figure("emission floor, s", 1920.0, 1920.0, COUNTED, "scripts.latency_report"),
    Figure("trailing gap min, s", 1800.0, 1800.0, COUNTED, "scripts.latency_report"),
    Figure("emission lag p50, s", 2548.75, 2548.75, COUNTED, "scripts.latency_report"),
    Figure("over the 60 s goal by", 32.02, 32.02, COUNTED, "scripts.latency_report"),
    Figure("add_batch p50, ms", 1107.5, 1069.0, TIMING, "scripts.latency_report"),
    Figure("add_batch p95, ms", 2435.9, 2600.0, TIMING, "scripts.latency_report"),
    Figure("add_batch p99, ms", 3828.0, 4253.6, TIMING, "scripts.latency_report"),
]


def chart(rep: dict, path: str) -> None:
    import matplotlib  # noqa: PLC0415

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt  # noqa: PLC0415

    rows = sorted(rep["figures"], key=lambda r: (r["kind"], r["ratio"]))
    names = [r["name"] for r in rows]
    ratios = [r["ratio"] for r in rows]
    # A count that reproduced sits exactly on 1.0 and would be invisible as a bar, so
    # the marker carries the meaning and the line is the reference.
    colours = ["#2a6f97" if r["kind"] == COUNTED else "#c1666b" for r in rows]

    fig, ax = plt.subplots(figsize=(9, 8))
    ax.axvline(1.0, color="#444", lw=1)
    ax.scatter(ratios, range(len(rows)), c=colours, s=70, zorder=3)
    handles = [
        plt.Line2D([], [], marker="o", ls="", color="#2a6f97", label="counted, must be 1.000"),
        plt.Line2D([], [], marker="o", ls="", color="#c1666b", label="timing, expected to move"),
    ]
    ax.legend(handles=handles, loc="lower right", fontsize=8, framealpha=0.9)
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels(names, fontsize=8)
    ax.set_xlabel("day 7 measurement divided by the figure the README published")
    ax.set_title(
        f"{rep['counted']} counted quantities, all exactly 1.000. "
        f"{rep['moved']} timings moved.",
        fontsize=10,
    )
    ax.set_xlim(0.9, 1.15)
    ax.grid(axis="x", alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, dpi=140)
    print(f"wrote {path}", file=sys.stderr)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--chart", help="png path, optional")
    args = p.parse_args()

    rep = report(FIGURES)
    if args.chart:
        chart(rep, args.chart)
    print(json.dumps(rep, indent=2))
    return 0 if rep["clean"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
