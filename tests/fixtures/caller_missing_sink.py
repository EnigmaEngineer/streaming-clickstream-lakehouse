"""A fixture. Never imported and never run.

This is the real day 4 defect, reduced. The namespace carries everything except `sink`,
which is exactly the shape scripts/watermark_sweep.py was in from 2026-08-16 until it
was fixed. AttributeError at run time and nothing in the suite noticed.
"""

from types import SimpleNamespace


def build():
    # DEFECT: sink is missing and fake_job.run reads it.
    return SimpleNamespace(source="file", path="/tmp/x", topic=None)
