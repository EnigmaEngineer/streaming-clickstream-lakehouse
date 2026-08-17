"""Draw the two charts day 5 is about. Reads files, draws, writes a PNG.

No arithmetic here. Both inputs are produced by scripts/latency_report.py and
scripts/latency_sweep.py, whose numbers come out of stream/lag.py and
stream/latency.py, and those have tests. A chart script that computed anything would
be a figure with no test behind it, which is ot-037 on the program side.

    python -m scripts.latency_dashboard --report /tmp/lat.json \\
        --sweep /tmp/sweep.jsonl --out docs/latency-budget.png
"""

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


def load_sweep(path):
    rows = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    if not rows:
        raise ValueError(f"no sweep rows in {path}, nothing to draw")
    return sorted(rows, key=lambda r: r["emission_floor_s"])


def draw(report: dict, sweep: list, out: str) -> str:
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))

    terms = report["budget"]["terms"]
    names = ["ingest lag", "emission delay", "processing"]
    keys = ["ingest_lag_s", "emission_delay_s", "processing_s"]
    # A log axis has no place to put a zero, and the first version of this drew the
    # zero term as a 1 ms bar so it would appear. That is a bar representing a quantity
    # that is not there. A zero term gets no bar and says so in its label instead.
    drawn = [terms[k] if terms[k] > 0 else 0.0 for k in keys]
    bars = ax1.barh(names, drawn, color=["#7f8c8d", "#c0392b", "#7f8c8d"])
    ax1.set_xscale("log")
    ax1.set_xlabel("seconds, log scale")
    ax1.set_title("Where the lag goes, at a 30 minute gap")
    goal = report["budget"]["goal_s"]
    ax1.axvline(goal, color="#2c3e50", linestyle="--", linewidth=1)
    ax1.text(goal * 1.1, 2.35, f"blueprint goal, {goal:.0f}s", fontsize=8, color="#2c3e50")
    biggest = max(drawn)
    for bar, k in zip(bars, keys):
        value = terms[k]
        label = f"{value:.4g}s  ({report['budget']['share'][k] * 100:.2f}%)"
        if value <= 0:
            label = "0s, no bar to draw on a log axis"
        ax1.text(
            (bar.get_width() * 1.15) if value > 0 else biggest * 1e-4,
            bar.get_y() + bar.get_height() / 2,
            label,
            va="center",
            fontsize=8,
        )
    ax1.set_xlim(left=biggest * 1e-4, right=biggest * 40)

    floors = [r["emission_floor_s"] for r in sweep]
    ax2.plot(floors, [r["boundary_miss_rate"] for r in sweep], "o-", color="#c0392b", label="merged, boundaries lost")
    ax2.plot(floors, [r["split_rate"] for r in sweep], "s-", color="#2980b9", label="split, real visits cut in two")
    for r in sweep:
        ax2.annotate(
            r["gap"],
            (r["emission_floor_s"], r["boundary_miss_rate"]),
            textcoords="offset points",
            xytext=(4, 6),
            fontsize=8,
        )
    ax2.set_xscale("log")
    ax2.set_xlabel("emission floor, seconds, log scale")
    ax2.set_ylabel("share of true sessions")
    ax2.set_title("Buying latency costs accuracy, in both directions")
    ax2.legend(fontsize=8)
    ax2.grid(alpha=0.3)

    Path(out).parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(out, dpi=130)
    return out


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="draw the latency budget and the gap tradeoff")
    p.add_argument("--report", required=True, help="json from scripts.latency_report")
    p.add_argument("--sweep", required=True, help="jsonl from scripts.latency_sweep")
    p.add_argument("--out", required=True)
    a = p.parse_args(argv)
    with open(a.report, encoding="utf-8") as fh:
        report = json.load(fh)
    print(draw(report, load_sweep(a.sweep), a.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
