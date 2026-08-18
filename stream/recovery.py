"""What a checkpoint says happened, and what a restart is therefore going to redo.

Standard library only. Nothing here imports pyspark or duckdb, so a clone can read a
checkpoint and answer "what would a restart replay" without installing either. The
Spark side writes these directories and this module only reads them.

A Structured Streaming checkpoint keeps two logs that matter here.

    offsets/<n>   written BEFORE batch n runs. This is the plan.
    commits/<n>   written AFTER batch n's sink returns. This is the receipt.

So a crash leaves at most one batch with a plan and no receipt, and that batch is the
one a restart re-runs. Everything in this module is a way of asking which batch that
is and what it cost.

The functions refuse rather than return empty when there is nothing to read. A
checkpoint directory that does not exist and a checkpoint with no batches in it are
different from a clean run, and a reader that answers "nothing uncommitted" to both
is the shape of check this program keeps catching.
"""

import json
import os


class UnreadableCheckpoint(Exception):
    """The directory is missing, or it holds no offsets log.

    Separate from an empty result on purpose. `checkpoint_state` on a path that was
    never a checkpoint would otherwise report zero planned batches and zero
    uncommitted ones, which reads exactly like a clean shutdown.
    """


def _batch_ids(directory: str) -> list[int]:
    """Numeric filenames only.

    The log directories also carry `.<n>.crc` files, and those start with a dot and
    are not batches. An int() filter is enough and a suffix filter would not be,
    because the crc name is the batch id with dots around it.
    """
    if not os.path.isdir(directory):
        return []
    out = []
    for name in os.listdir(directory):
        if name.isdigit():
            out.append(int(name))
    return sorted(out)


def checkpoint_state(path: str) -> dict:
    """Which batches were planned, which were committed, which are neither.

    `uncommitted` is the answer a restart acts on. In normal operation it is empty or
    it holds exactly one batch id. More than one means something wrote offsets ahead
    of the sink, which this pipeline does not do, so it is reported rather than
    asserted away.
    """
    offsets_dir = os.path.join(path, "offsets")
    if not os.path.isdir(offsets_dir):
        raise UnreadableCheckpoint(f"no offsets log under {path!r}")
    planned = _batch_ids(offsets_dir)
    if not planned:
        raise UnreadableCheckpoint(f"offsets log under {path!r} holds no batches")
    committed = _batch_ids(os.path.join(path, "commits"))
    uncommitted = [b for b in planned if b not in set(committed)]
    return {
        "planned": planned,
        "committed": committed,
        "uncommitted": uncommitted,
        "last_committed": max(committed) if committed else None,
        "next_batch": max(planned) + 1 if not uncommitted else min(uncommitted),
    }


def source_offsets(path: str, batch_id: int) -> list:
    """The per source position recorded for one batch.

    The file is line delimited. Line 0 is a version marker, line 1 is a metadata
    object, and everything after that is one source's offset. For Kafka that is a
    topic to partition to offset map. For the file source it is a log ordinal, which
    is far less interesting and is still the thing a restart resumes from.
    """
    f = os.path.join(path, "offsets", str(batch_id))
    if not os.path.isfile(f):
        raise UnreadableCheckpoint(f"no offsets entry for batch {batch_id} under {path!r}")
    with open(f, encoding="utf-8") as fh:
        lines = [ln.strip() for ln in fh if ln.strip()]
    # Every offset Spark writes is JSON. The file source writes {"logOffset": n} and
    # Kafka writes a topic to partition map. A source built on LongOffset writes a
    # bare number, which is also JSON and parses to an int.
    #
    # The first draft of this wrapped the parse in a try and kept an unparseable line
    # as text. There is no such line, so that branch could not be reached and the
    # test written for it asserted the wrong thing. A JSONDecodeError now escapes,
    # which is the right answer for a corrupt log. This module refuses rather than
    # guesses everywhere else and a half read offset is the worst place to start.
    return [json.loads(ln) for ln in lines[2:]]


def kafka_position(offsets: list) -> dict:
    """Flatten a Kafka source offset into partition to offset.

    Returns an empty dict when the source is not Kafka, and the caller is expected to
    know which it asked for. Written this way because the file source's offset is a
    scalar and pretending it has partitions would invent structure.
    """
    out: dict[str, int] = {}
    for entry in offsets:
        if not isinstance(entry, dict):
            continue
        for topic, parts in entry.items():
            if not isinstance(parts, dict):
                continue
            for part, off in parts.items():
                out[f"{topic}:{part}"] = off
    return out


def replay_scope(before: dict, after: dict) -> dict:
    """What the restart re-ran, from the two checkpoint states around it.

    `redone` is the batch ids that were planned before the restart and got planned
    again. On this pipeline that is the uncommitted batch and nothing else. `added`
    is the work the restart did that the first run never reached.
    """
    before_planned = set(before["planned"])
    after_planned = set(after["planned"])
    redone = sorted(b for b in before["uncommitted"] if b in after_planned)
    added = sorted(after_planned - before_planned)
    return {
        "redone": redone,
        "added": added,
        "resumed_at": before["next_batch"],
        "finished_at": max(after["planned"]) if after_planned else None,
    }


# The verdicts. Named rather than boolean, because "not identical" covers three very
# different failures and a replay test that only says pass or fail hides which one
# happened.
IDENTICAL = "identical"
ROWS_GAINED = "rows_gained"
ROWS_LOST = "rows_lost"
CELLS_DIFFER = "cells_differ"


def compare(baseline: str, arm: str) -> str:
    """Compare two `warehouse.merge.table_fingerprint` values.

    The fingerprint is "<count>:<md5 of the ordered rows>". Splitting it lets a
    difference in row count be told apart from a difference in a cell, and those two
    are the duplicate case and the wrong value case. A single equality check would
    call both of them "not idempotent" and leave the reader to guess.
    """
    b_count, b_hash = baseline.split(":", 1)
    a_count, a_hash = arm.split(":", 1)
    if baseline == arm:
        return IDENTICAL
    if int(a_count) > int(b_count):
        return ROWS_GAINED
    if int(a_count) < int(b_count):
        return ROWS_LOST
    if a_hash != b_hash:
        return CELLS_DIFFER
    # Same count and same hash but not equal is not reachable through the string
    # above. Left as a raise rather than a fourth verdict, because reaching it means
    # the fingerprint format changed and a silent answer would be worse.
    raise ValueError(f"cannot compare {baseline!r} against {arm!r}")
