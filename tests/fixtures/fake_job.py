"""A fixture. Never imported by anything and never run.

Stands in for stream/job.py so the detector in tests/structural.py can be pointed at a
file whose answer is known. Lines carrying a deliberate defect are marked `# DEFECT`
rather than written down as line numbers somewhere else, so editing this file cannot
move a defect out from under an assertion.
"""

from types import SimpleNamespace


def run(args):
    """Reads four attributes. One of them is assigned rather than read."""
    if args.source == "file":
        path = args.path
    else:
        path = args.topic
    sink = args.sink
    # Assigned, not read. A detector counting this as a requirement would demand that
    # every caller pre-populate an output field.
    args.result = 1
    return SimpleNamespace(path=path, sink=sink)


def takes_nothing():
    """No parameters at all. args_attributes_read must raise on this rather than
    return an empty set, since an empty requirement set makes any caller complete."""
    return 1
