"""Checks over stream/recovery.py.

No Spark here. The fixtures are checkpoint shaped directories built by hand, which is
the only way to test the crash case without crashing something. A real checkpoint from
today's run is compared against these in tests/test_replay.py, which needs pyspark.

The fixture rule from 2026-08-02 applies hard in this file. A checkpoint with one
batch in it cannot test any rule about choosing between batches, so every fixture
below has at least three.
"""

import json
import os
import tempfile

from stream import recovery


def _ckpt(planned, committed, offset_lines=None):
    """Build a checkpoint directory. Returns the path.

    Deliberately takes the two lists separately rather than deriving commits from
    offsets. The whole point of this module is the case where they disagree, so a
    helper that kept them in step would make half the tests unwritable.
    """
    root = tempfile.mkdtemp(prefix="ckpt-")
    os.makedirs(os.path.join(root, "offsets"))
    os.makedirs(os.path.join(root, "commits"))
    for b in planned:
        body = offset_lines.get(b) if offset_lines else None
        lines = ["v1", '{"batchWatermarkMs":0}'] + (body or ["17"])
        with open(os.path.join(root, "offsets", str(b)), "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
        # Spark writes a crc beside every log entry and it is not a batch.
        with open(os.path.join(root, "offsets", f".{b}.crc"), "wb") as fh:
            fh.write(b"\x00\x01")
    for b in committed:
        with open(os.path.join(root, "commits", str(b)), "w", encoding="utf-8") as fh:
            fh.write('v1\n{"nextBatchWatermarkMs":0}\n')
    return root


def check_a_clean_shutdown_has_nothing_uncommitted():
    st = recovery.checkpoint_state(_ckpt([0, 1, 2, 3], [0, 1, 2, 3]))
    assert st["uncommitted"] == [], st
    assert st["last_committed"] == 3
    assert st["next_batch"] == 4


def check_a_crash_leaves_the_last_planned_batch_uncommitted():
    st = recovery.checkpoint_state(_ckpt([0, 1, 2, 3], [0, 1, 2]))
    assert st["uncommitted"] == [3], st
    assert st["last_committed"] == 2
    assert st["next_batch"] == 3


def check_a_gap_in_the_commit_log_is_reported_and_not_smoothed():
    # Not something this pipeline produces. If it ever appears, the reader has to say
    # so rather than answer with the highest committed id and look tidy.
    st = recovery.checkpoint_state(_ckpt([0, 1, 2, 3], [0, 2, 3]))
    assert st["uncommitted"] == [1], st
    assert st["next_batch"] == 1


def check_crc_files_are_not_counted_as_batches():
    st = recovery.checkpoint_state(_ckpt([0, 1, 2], [0, 1, 2]))
    assert st["planned"] == [0, 1, 2], st


def check_a_missing_directory_raises_rather_than_reporting_a_clean_run():
    try:
        recovery.checkpoint_state("/tmp/definitely-not-a-checkpoint-3f9a")
    except recovery.UnreadableCheckpoint:
        return
    raise AssertionError("a path that is not a checkpoint reported a state")


def check_an_offsets_log_with_no_batches_raises():
    root = tempfile.mkdtemp(prefix="ckpt-")
    os.makedirs(os.path.join(root, "offsets"))
    os.makedirs(os.path.join(root, "commits"))
    try:
        recovery.checkpoint_state(root)
    except recovery.UnreadableCheckpoint:
        return
    raise AssertionError("an empty offsets log reported a state")


def check_source_offsets_skips_the_version_and_metadata_lines():
    body = {2: [json.dumps({"clickstream.events": {"0": 400, "1": 380}})]}
    root = _ckpt([0, 1, 2], [0, 1], offset_lines=body)
    off = recovery.source_offsets(root, 2)
    assert off == [{"clickstream.events": {"0": 400, "1": 380}}], off


def check_the_file_source_offset_shape_reads_back():
    # Copied from a real checkpoint written by today's run, offsets/3 line 3.
    body = {1: [json.dumps({"logOffset": 3})]}
    root = _ckpt([0, 1, 2], [0, 1], offset_lines=body)
    assert recovery.source_offsets(root, 1) == [{"logOffset": 3}]


def check_a_bare_number_offset_parses_rather_than_raising():
    # LongOffset writes the number and nothing else. It is still JSON.
    root = _ckpt([0, 1, 2], [0, 1, 2])
    assert recovery.source_offsets(root, 1) == [17], recovery.source_offsets(root, 1)


def check_a_corrupt_offsets_line_raises_rather_than_being_kept_as_text():
    # The branch this replaces used to swallow it. A half read offset log is the
    # worst possible thing to answer a replay question from.
    root = _ckpt([0, 1, 2], [0, 1])
    with open(os.path.join(root, "offsets", "2"), "w", encoding="utf-8") as fh:
        fh.write("v1\n{}\nnot json at all\n")
    try:
        recovery.source_offsets(root, 2)
    except json.JSONDecodeError:
        return
    raise AssertionError("a corrupt offsets line was read without complaint")


def check_source_offsets_on_a_batch_that_was_never_planned_raises():
    root = _ckpt([0, 1, 2], [0, 1, 2])
    try:
        recovery.source_offsets(root, 9)
    except recovery.UnreadableCheckpoint:
        return
    raise AssertionError("read an offset for a batch that does not exist")


def check_kafka_position_flattens_topic_and_partition():
    off = [{"clickstream.events": {"0": 400, "1": 380, "2": 12}}]
    assert recovery.kafka_position(off) == {
        "clickstream.events:0": 400,
        "clickstream.events:1": 380,
        "clickstream.events:2": 12,
    }


def check_kafka_position_on_a_file_source_offset_is_empty_not_wrong():
    assert recovery.kafka_position(["17"]) == {}


def check_replay_scope_names_the_batch_that_was_redone():
    before = recovery.checkpoint_state(_ckpt([0, 1, 2, 3], [0, 1, 2]))
    after = recovery.checkpoint_state(_ckpt([0, 1, 2, 3, 4, 5], [0, 1, 2, 3, 4, 5]))
    sc = recovery.replay_scope(before, after)
    assert sc["redone"] == [3], sc
    assert sc["added"] == [4, 5], sc
    assert sc["resumed_at"] == 3
    assert sc["finished_at"] == 5


def check_replay_scope_after_a_clean_stop_redoes_nothing():
    before = recovery.checkpoint_state(_ckpt([0, 1, 2], [0, 1, 2]))
    after = recovery.checkpoint_state(_ckpt([0, 1, 2, 3], [0, 1, 2, 3]))
    sc = recovery.replay_scope(before, after)
    assert sc["redone"] == [], sc
    assert sc["added"] == [3], sc


def check_compare_separates_a_duplicate_from_a_wrong_value():
    # Same shape as the fingerprint warehouse.merge builds: count, colon, md5.
    assert recovery.compare("768:aaa", "768:aaa") == recovery.IDENTICAL
    assert recovery.compare("768:aaa", "769:bbb") == recovery.ROWS_GAINED
    assert recovery.compare("768:aaa", "700:bbb") == recovery.ROWS_LOST
    assert recovery.compare("768:aaa", "768:bbb") == recovery.CELLS_DIFFER


def check_compare_does_not_call_a_row_loss_identical():
    # The failure this is really guarding. A replay that silently drops rows has the
    # same row count as nothing and a different one from the baseline, and an
    # equality check that returned a bool would report it the same as a duplicate.
    assert recovery.compare("768:aaa", "767:aaa") == recovery.ROWS_LOST
