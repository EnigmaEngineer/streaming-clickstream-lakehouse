"""Checks over the crash injection in stream/job.py.

`crash_at` needs no Spark to exercise and it lives in a module that imports pyspark,
so it is tested here rather than in the no-install suite. The fake sink below records
what it was handed, which is the only way to tell "crashed before the merge" apart
from "crashed after it" without a warehouse.

The 2026-08-02 fixture rule applies. A fixture with one batch in it cannot test a rule
about which batch to die on, so every check below runs at least four.
"""

from stream.job import InjectedCrash, crash_at


class Recorder:
    def __init__(self):
        self.seen = []

    def __call__(self, df, epoch_id):
        self.seen.append(epoch_id)


def _drive(fn, batches=6):
    """Feed batch ids until one of them raises. Returns the id that did."""
    for i in range(batches):
        try:
            fn(None, i)
        except InjectedCrash:
            return i
    return None


def check_crashing_after_the_merge_lets_that_batch_reach_the_sink():
    rec = Recorder()
    died = _drive(crash_at(rec, 3, "after-merge"))
    assert died == 3, died
    # The whole point. Batch 3 landed in the warehouse and then the job died, so the
    # commit file was never written and the restart redoes a batch that is already
    # there. That is the case the MERGE has to absorb.
    assert rec.seen == [0, 1, 2, 3], rec.seen


def check_crashing_before_the_merge_leaves_that_batch_unwritten():
    rec = Recorder()
    died = _drive(crash_at(rec, 3, "before-merge"))
    assert died == 3, died
    assert rec.seen == [0, 1, 2], rec.seen


def check_the_two_crash_points_really_differ():
    # Written because the pair is easy to get wrong in a way that still passes each
    # check on its own. If both raised in the same place, both tests above would
    # still be satisfiable by editing one expected list.
    after, before = Recorder(), Recorder()
    _drive(crash_at(after, 2, "after-merge"))
    _drive(crash_at(before, 2, "before-merge"))
    assert len(after.seen) == len(before.seen) + 1, (after.seen, before.seen)


def check_a_batch_the_crash_is_not_aimed_at_passes_through():
    rec = Recorder()
    died = _drive(crash_at(rec, 99, "after-merge"), batches=5)
    assert died is None, died
    assert rec.seen == [0, 1, 2, 3, 4], rec.seen


def check_crashing_at_batch_zero_is_allowed():
    # Guards the falsy-integer mistake. `if batch` rather than `if batch is None`
    # somewhere upstream would make batch 0 unreachable and nothing else would say so.
    rec = Recorder()
    died = _drive(crash_at(rec, 0, "before-merge"))
    assert died == 0, died
    assert rec.seen == [], rec.seen


def check_an_unknown_crash_point_is_refused_at_wrap_time():
    # At wrap time rather than at batch time. A typo that only surfaces on the batch
    # it was aimed at would waste a whole run before saying anything.
    try:
        crash_at(Recorder(), 3, "after-commit")
    except ValueError as e:
        assert "after-commit" in str(e), str(e)
        return
    raise AssertionError("an unknown crash point was accepted")
