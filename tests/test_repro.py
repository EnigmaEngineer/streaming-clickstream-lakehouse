"""Checks for the day 7 reproduction rule.

The point of the module under test is that a count and a timing get different
treatment. So every fixture here builds a case where the two rules disagree. A
fixture where both rules give the same verdict cannot tell whether the split exists,
which is the 08-02 lesson from another repo in this program.
"""

from stream.repro import (
    BROKEN,
    COUNTED,
    MOVED,
    REPRODUCED,
    TIMING,
    Figure,
    classify,
    ratio,
    report,
)

SRC = "stream.job --progress"


def _fig(published, measured, kind, name="x"):
    return Figure(name=name, published=published, measured=measured, kind=kind, source=SRC)


def check_a_count_that_moves_by_one_row_is_broken_and_a_timing_is_not():
    """The whole module is this one distinction. Same numbers on both sides, so a
    classify that ignored `kind` would return the same verdict twice and fail here."""
    assert classify(_fig(58182, 58183, COUNTED)) == BROKEN
    assert classify(_fig(58182, 58183, TIMING)) == MOVED


def check_an_exact_match_reproduces_under_both_kinds():
    assert classify(_fig(3252, 3252, COUNTED)) == REPRODUCED
    assert classify(_fig(1069.0, 1069.0, TIMING)) == REPRODUCED


def check_there_is_no_tolerance_band_hiding_in_the_count_rule():
    """0.24 percent is the size of the real defect this rule exists for. A relative
    tolerance of even a thousandth would pass it."""
    assert classify(_fig(254952, 254346, COUNTED)) == BROKEN
    assert classify(_fig(1.0, 1.0000001, COUNTED)) == BROKEN


def check_a_published_zero_reproduces_rather_than_dividing():
    """Day 3 published zero rows dropped by both operators and day 7 measured zero
    again. That is the most common shape in this repo and it has no ratio."""
    z = _fig(0, 0, COUNTED, name="dropped_by_watermark")
    assert ratio(z) == 1.0
    assert classify(z) == REPRODUCED
    assert ratio(_fig(0, 3, COUNTED)) == float("inf")
    assert classify(_fig(0, 3, COUNTED)) == BROKEN


def check_an_unknown_kind_raises_rather_than_defaulting_to_the_lenient_rule():
    """A typo in `kind` must not silently buy the timing treatment, which is the
    treatment that never reports a problem."""
    try:
        classify(_fig(1, 2, "count"))
    except ValueError as e:
        assert "count" in str(e), str(e)
    else:
        raise AssertionError("a misspelled kind was accepted")


def check_report_refuses_an_empty_list():
    """Three tools in this program have reported a clean pass having checked nothing.
    Making the empty case a finding is the fix that worked each time."""
    try:
        report([])
    except ValueError as e:
        assert "refusing" in str(e), str(e)
    else:
        raise AssertionError("reported a clean pass over zero figures")


def check_report_counts_each_verdict_and_only_a_broken_count_makes_it_dirty():
    """A timing that moved must not make the pass dirty, and a count that moved must.
    Both are in the same fixture so a report that keyed off `ratio != 1` would fail.

    The split is deliberately three to one rather than one to one. A fixture with the
    same number of each cannot tell `kind == COUNTED` from `kind == TIMING`, and a
    mutant that swapped them survived this check until 2026-08-19. That number is the
    headline of the chart in the README, so the swap would have been published.
    """
    figs = [
        _fig(58182, 58182, COUNTED, "input_rows"),
        _fig(3252, 3252, COUNTED, "sessions"),
        _fig(0, 0, COUNTED, "dropped"),
        _fig(37226.0, 39193.0, TIMING, "ceiling"),
    ]
    r = report(figs)
    assert r["checked"] == 4, r
    assert r["counted"] == 3, r
    assert r["reproduced"] == 3, r
    assert r["moved"] == 1, r
    assert r["broken"] == 0, r
    assert r["clean"] is True, r

    dirty = report(figs + [_fig(3252, 3251, COUNTED, "sessions_again")])
    assert dirty["counted"] == 4, dirty
    assert dirty["broken"] == 1, dirty
    assert dirty["clean"] is False, dirty
    assert dirty["moved"] == 1, dirty


def check_the_report_carries_the_command_so_one_row_can_be_rechecked():
    """A table of figures with no way back to what made them is the defect this
    module is about, one level up."""
    r = report([_fig(3252, 3252, COUNTED, "sessions")])
    assert r["figures"][0]["source"] == SRC, r
    assert r["figures"][0]["name"] == "sessions", r
