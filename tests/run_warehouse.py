"""Runner for the checks that need duckdb.

Third runner in this repo and each one is named for the install it needs. run_all is
the standard library. run_spark needs pyspark. This needs duckdb, which is a 21 MB
wheel rather than a 300 MB one, so it is cheap to run and there was no reason to fold
it into the slow suite.

No skip path. A missing duckdb fails on the import.

    python -m tests.run_warehouse
"""

import importlib
import sys
import traceback
from pathlib import Path

MODULES = ["tests.test_merge"]


def collect(mod) -> list[str]:
    """Check functions defined in this module, not ones it imported.

    Found on 2026-08-16 the moment warehouse/merge.py grew a function called
    `check_batch`. The runner picked it up off the import, called it with no
    arguments, and reported a failure in a test file that did not have one. The
    reverse case is worse and was sitting right behind it. An imported check that
    happens to take no arguments would have run twice and passed twice.
    """
    out = []
    for name in dir(mod):
        if not name.startswith("check_"):
            continue
        fn = getattr(mod, name)
        if callable(fn) and getattr(fn, "__module__", None) == mod.__name__:
            out.append(name)
    return sorted(out)


def run() -> int:
    root = Path(__file__).resolve().parent.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    passed, failed = 0, []
    for name in MODULES:
        try:
            mod = importlib.import_module(name)
        except Exception:
            failed.append((name, traceback.format_exc()))
            continue
        checks = collect(mod)
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
    if not passed and not failed:
        print("no checks ran, which is a failure and not a pass")
        return 2
    print(f"{passed} passed, {len(failed)} failed, across {len(MODULES)} modules")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(run())
