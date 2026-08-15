"""Runner for the checks that need Spark.

Separate from run_all.py because these take about a minute and need a 300 MB
install, and run_all.py is the suite a clone can run with nothing installed. Both
are real and both have to pass before a commit. Neither is optional.

There is no skip path. If pyspark is missing this fails on the import, which is the
point. A suite that reports success having executed nothing is the exact defect this
program has now found in three of its own tools.

    python -m tests.run_spark
"""

import importlib
import sys
import traceback
from pathlib import Path

MODULES = [
    "tests.test_sessionize",
    "tests.test_scoring",
]


def run() -> int:
    root = Path(__file__).resolve().parent.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    passed, failed = 0, []
    for name in MODULES:
        # A module that will not import is a failure to report, not a crash. On
        # 2026-08-15 a syntax error in one test file killed the runner after another
        # module's checks had already passed, and the exit was a traceback rather
        # than a count. The same shape took down a job on another repo here.
        try:
            mod = importlib.import_module(name)
        except Exception:
            failed.append((name, traceback.format_exc()))
            continue
        checks = [f for f in dir(mod) if f.startswith("check_")]
        if not checks:
            failed.append((name, "module defines no checks"))
            continue
        for check in sorted(checks):
            try:
                getattr(mod, check)()
                passed += 1
            except Exception:
                failed.append((f"{name}.{check}", traceback.format_exc()))

    session = sys.modules.get("tests.test_sessionize")
    if session is not None and session._spark is not None:
        session._spark.stop()

    for name, why in failed:
        print(f"FAIL {name}\n{why}")
    if not passed and not failed:
        print("no checks ran, which is a failure and not a pass")
        return 2
    print(f"{passed} passed, {len(failed)} failed, across {len(MODULES)} modules")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(run())
