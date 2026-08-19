"""A fixture. Never imported and never run.

This is a real defect from this repo, reduced. The namespace carries everything except `sink`,
which is exactly the shape scripts/watermark_sweep.py sat in for a while before it
was fixed. AttributeError at run time and nothing in the suite noticed.
"""

from types import SimpleNamespace


def build():
    # DEFECT: sink is missing and fake_job.run reads it.
    return SimpleNamespace(source="file", path="/tmp/x", topic=None)
