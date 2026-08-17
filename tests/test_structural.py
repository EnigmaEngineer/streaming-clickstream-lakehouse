"""Checks for the source reading detector, and then the real repo run through it.

Half of these point the detector at fixtures with a known answer. The other half point
it at this repo, which is the part that would have caught the day 4 breakage.

The fixture half exists because a check that reads code cannot be tested from inside
itself. That was the 08-10 lesson on the previous project, where the equivalent
detector's mutant survived and the behaviour had to be shown by hand.
"""

from pathlib import Path

from tests.structural import args_attributes_read, missing_namespace_fields, namespace_calls

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = Path(__file__).resolve().parent / "fixtures"
JOB = ROOT / "stream" / "job.py"
CALLERS = [ROOT / "scripts" / "watermark_sweep.py", ROOT / "scripts" / "latency_sweep.py"]


def check_the_detector_finds_the_attributes_read_off_the_parameter():
    got = args_attributes_read(FIXTURES / "fake_job.py")
    assert got == {"source", "path", "topic", "sink"}, got


def check_an_assigned_attribute_is_not_treated_as_a_requirement():
    """fake_job.run sets args.result. Requiring it would force every caller to
    pre-populate an output, and the check would then fail on correct code."""
    assert "result" not in args_attributes_read(FIXTURES / "fake_job.py")


def check_a_function_with_no_parameters_raises_rather_than_returning_nothing():
    """An empty requirement set makes every caller complete, so this is the
    passing-on-nothing shape and it has to be a finding."""
    try:
        args_attributes_read(FIXTURES / "fake_job.py", func_name="takes_nothing")
    except ValueError:
        return
    raise AssertionError("a zero argument function should not have produced requirements")


def check_a_missing_function_name_raises():
    try:
        args_attributes_read(FIXTURES / "fake_job.py", func_name="not_here")
    except ValueError:
        return
    raise AssertionError("an absent function should not have reported an empty set")


def check_both_spellings_of_simplenamespace_are_found():
    """caller_complete uses types.SimpleNamespace and caller_missing_sink uses the bare
    name. A detector knowing only one would report clean on the other, which is the
    worse direction to be wrong in."""
    dotted = namespace_calls(FIXTURES / "caller_complete.py")
    bare = namespace_calls(FIXTURES / "caller_missing_sink.py")
    assert dotted == [{"source", "path", "topic", "sink"}], dotted
    assert bare == [{"source", "path", "topic"}], bare


def check_the_planted_defect_is_reported():
    got = missing_namespace_fields(FIXTURES / "fake_job.py", [FIXTURES / "caller_missing_sink.py"])
    assert len(got) == 1, got
    assert list(got.values()) == [["sink"]], got


def check_the_complete_caller_is_clean():
    """The control. Without it the detector could be firing on every input, and the
    10-08 lesson is that a run whose control also looks killed is worthless."""
    got = missing_namespace_fields(FIXTURES / "fake_job.py", [FIXTURES / "caller_complete.py"])
    assert got == {}, got


def check_being_given_no_scripts_raises_rather_than_passing():
    try:
        missing_namespace_fields(FIXTURES / "fake_job.py", [])
    except ValueError:
        return
    raise AssertionError("zero scripts should not have reported success")


def check_a_file_with_no_namespace_calls_raises_rather_than_passing():
    """The hardest case for any validator is the input that gives it nothing to do.
    fake_job.py builds a SimpleNamespace as a return value, so the file used here is
    one that builds none at all."""
    try:
        missing_namespace_fields(FIXTURES / "fake_job.py", [ROOT / "stream" / "lag.py"])
    except ValueError:
        return
    raise AssertionError("a file with no namespace calls should not have reported clean")


def check_every_hand_built_namespace_in_this_repo_is_complete():
    """The real check. This is the one that would have caught day 4.

    scripts/watermark_sweep.py built a namespace without `sink` from the moment the
    job grew that option, so every run of it raised AttributeError for a day while the
    README kept publishing the table it produces.
    """
    got = missing_namespace_fields(JOB, CALLERS)
    assert got == {}, got


def check_the_job_really_reads_the_options_the_callers_supply():
    """Guards the other direction. If args_attributes_read came back with a small set
    for some parsing reason, the check above would pass on an incomplete caller. These
    four are the ones that have actually caused trouble."""
    required = args_attributes_read(JOB)
    for name in ("sink", "source", "gap", "watermark"):
        assert name in required, (name, sorted(required))
