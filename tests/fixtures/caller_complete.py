"""A fixture. Never imported and never run.

A caller that supplies every attribute fake_job.run reads. The control. Without it the
detector could be reporting a problem on every input, and a check that always fires is
as useless as one that never does.

Uses the dotted form on purpose, because a detector that only matched the bare name
would report clean on this file for the wrong reason.
"""

import types


def build():
    return types.SimpleNamespace(source="file", path="/tmp/x", topic=None, sink="parquet")
