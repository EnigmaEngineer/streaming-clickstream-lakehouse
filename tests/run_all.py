"""Test runner.

Plain functions named check_*, no pytest. The reason is in `docs/decisions.md`: on
another repo in this program two pytest-style files entered a project whose runner
loops over scripts, and one of them exited 0 having executed zero assertions.

A module that defines no checks is a failure here, not a pass.
"""

import importlib
import sys
import traceback
from pathlib import Path

MODULES = [
    "tests.test_clock",
    "tests.test_population",
    "tests.test_arrivals",
    "tests.test_session",
    "tests.test_produce",
]


def run() -> int:
    root = Path(__file__).resolve().parent.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    passed, failed = 0, []
    for name in MODULES:
        # An import error is a reportable failure, not a reason to abandon the run
        # and print a traceback where a count should be. Added 2026-08-15 after the
        # Spark runner did exactly that.
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

    for name, why in failed:
        print(f"FAIL {name}\n{why}")
    print(f"{passed} passed, {len(failed)} failed, across {len(MODULES)} modules")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(run())
