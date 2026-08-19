"""Does a figure this repo published still come out of the code that produced it.

Day 7 re-ran every measurement in the README rather than trusting the prose, because
this project has twice found a number sitting in a sentence after the thing that
produced it had moved. The classification below is the rule that pass used.

It lives under `stream/` because it is standard library only, like `lag.py`, and a
clone can run it before installing anything. It has nothing to do with the streaming
job and there is no better package for it.

The rule is not "close enough". A counted quantity either reproduces exactly or the
repo has a problem, and a timing is expected to move because the machine is not the
same machine. Mixing the two under one tolerance is how a real regression gets filed
as noise.
"""

from __future__ import annotations

from dataclasses import dataclass

# A count, a row total, a rate derived from counts. Anything the seed determines.
COUNTED = "counted"
# Wall clock. Sandbox speed has been measured varying by about 1.8x between days.
TIMING = "timing"

REPRODUCED = "reproduced"
MOVED = "moved"
BROKEN = "broken"


@dataclass(frozen=True)
class Figure:
    name: str
    published: float
    measured: float
    kind: str
    source: str  # the command that produces it, so a reader can re-run one row


def ratio(fig: Figure) -> float:
    if fig.published == 0:
        # A published zero has no ratio. Day 3 published zero drops on two operators and
        # day 7 measured zero again, which is a reproduction and not a division.
        return 1.0 if fig.measured == 0 else float("inf")
    return fig.measured / fig.published


def classify(fig: Figure) -> str:
    """Exact for a count. Anything else for a timing.

    A counted quantity that moves at all is BROKEN, not MOVED. There is no tolerance
    band here on purpose. The seeds are fixed, so a difference of one row means a
    difference in the code, and the interesting cases in this program have all been
    small. 254,346 against 254,952 on another repo was 0.24 percent and it was a real
    defect that had been published four times.
    """
    if fig.kind not in (COUNTED, TIMING):
        raise ValueError(f"unknown kind {fig.kind!r} for {fig.name!r}")
    if fig.kind == COUNTED:
        return REPRODUCED if fig.measured == fig.published else BROKEN
    return REPRODUCED if fig.measured == fig.published else MOVED


def report(figures: list[Figure]) -> dict:
    """Summarise a day 7 style pass.

    Refuses an empty list. A checker that reports clean having looked at nothing is a
    defect this program has now found in three separate tools, and the answer each time
    was to make "nothing to check" a finding rather than a pass.
    """
    if not figures:
        raise ValueError("no figures to check, refusing to report a clean pass on none")
    rows = []
    for f in figures:
        rows.append(
            {
                "name": f.name,
                "kind": f.kind,
                "published": f.published,
                "measured": f.measured,
                "ratio": round(ratio(f), 6),
                "verdict": classify(f),
                "source": f.source,
            }
        )
    verdicts = [r["verdict"] for r in rows]
    return {
        "figures": rows,
        "checked": len(rows),
        "counted": sum(1 for f in figures if f.kind == COUNTED),
        "reproduced": verdicts.count(REPRODUCED),
        "moved": verdicts.count(MOVED),
        "broken": verdicts.count(BROKEN),
        "clean": BROKEN not in verdicts,
    }
