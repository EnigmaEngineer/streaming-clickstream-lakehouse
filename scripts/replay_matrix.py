"""Kill the job, start it again, and check what the warehouse looks like afterwards.

Harness. It shells out to `stream.job`, reads the checkpoint with `stream.recovery`
and the table with `warehouse.merge`, and writes one JSON file per arm. Every verdict
it prints comes from `recovery.compare`, which is library code with tests over it.
This file decides nothing.

Each arm is a separate invocation because a Spark session costs about eighteen
seconds to build and the sandbox kills a shell call at about three minutes. Run them
one at a time and then read the results together.

    python -m scripts.replay_matrix --arm clean         --corpus /tmp/c6 --work /tmp/rm
    python -m scripts.replay_matrix --arm crash-after   --corpus /tmp/c6 --work /tmp/rm
    python -m scripts.replay_matrix --arm crash-before  --corpus /tmp/c6 --work /tmp/rm
    python -m scripts.replay_matrix --arm fresh-checkpoint --corpus /tmp/c6 --work /tmp/rm
    python -m scripts.replay_matrix --arm wiped-warehouse  --corpus /tmp/c6 --work /tmp/rm
    python -m scripts.replay_matrix --report --work /tmp/rm

`clean` has to run first. The other four are compared against its fingerprint and
there is nothing to compare against until it exists.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys

from stream.recovery import UnreadableCheckpoint, checkpoint_state, compare, replay_scope

ARMS = ["clean", "crash-after", "crash-before", "fresh-checkpoint", "wiped-warehouse"]


def fingerprint(db_path: str) -> str:
    import duckdb  # noqa: PLC0415

    from warehouse.merge import ensure_tables, table_fingerprint  # noqa: PLC0415

    con = duckdb.connect(db_path)
    try:
        ensure_tables(con)
        return table_fingerprint(con)
    finally:
        con.close()


def duplicate_keys(db_path: str) -> int:
    """Count merge keys appearing more than once.

    DuckDB enforces the primary key, so this can only ever be zero on the DuckDB
    path and it is measured anyway. Snowflake accepts the same declaration and does
    not enforce it, so on the dialect this repo is rehearsing for, the number is not
    free. Measuring it here means the check exists when the sink moves.
    """
    import duckdb  # noqa: PLC0415

    from warehouse.sql import MERGE_KEY  # noqa: PLC0415

    con = duckdb.connect(db_path)
    try:
        cols = ", ".join(MERGE_KEY)
        row = con.execute(
            f"SELECT count(*) FROM (SELECT {cols} FROM sessions GROUP BY {cols} HAVING count(*) > 1)"
        ).fetchone()
        return row[0]
    finally:
        con.close()


def job(work: str, corpus: str, ckpt: str, db: str, tag: str, crash=None, point="after-merge") -> dict:
    """One `stream.job` run as a child process.

    A subprocess rather than an in process call, because the arms that crash have to
    lose the driver as well as the query. An exception caught inside this process
    would leave a warm SparkSession and a Python heap that a restarted job would
    never have.
    """
    out = os.path.join(work, f"summary-{tag}.json")
    cmd = [
        sys.executable, "-m", "stream.job",
        "--source", "file", "--path", corpus,
        "--available-now", "--files-per-trigger", "1",
        "--sink", "duckdb", "--duckdb", db,
        "--checkpoint", ckpt,
        "--summary-json", out,
    ]
    if crash is not None:
        cmd += ["--crash-batch", str(crash), "--crash-point", point]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    summary = {}
    if os.path.isfile(out):
        with open(out, encoding="utf-8") as fh:
            summary = json.load(fh)
    return {
        "exit_code": proc.returncode,
        "summary": summary,
        "stderr_tail": proc.stderr.strip().splitlines()[-3:] if proc.stderr else [],
    }


def state_or_none(ckpt: str) -> dict | None:
    try:
        return checkpoint_state(ckpt)
    except UnreadableCheckpoint:
        return None


def run_arm(name: str, corpus: str, work: str, crash_batch: int) -> dict:
    results = os.path.join(work, "results")
    os.makedirs(results, exist_ok=True)
    ckpt = os.path.join(work, name, "ckpt")
    db = os.path.join(work, name, "w.duckdb")
    os.makedirs(os.path.join(work, name), exist_ok=True)

    rec: dict = {"arm": name}

    if name == "clean":
        rec["runs"] = [job(work, corpus, ckpt, db, "clean")]
        rec["checkpoint"] = checkpoint_state(ckpt)

    elif name in ("crash-after", "crash-before"):
        point = "after-merge" if name == "crash-after" else "before-merge"
        first = job(work, corpus, ckpt, db, name + "-1", crash=crash_batch, point=point)
        mid = checkpoint_state(ckpt)
        rec["crashed_exit_code"] = first["exit_code"]
        rec["fingerprint_at_crash"] = fingerprint(db)
        rec["checkpoint_at_crash"] = mid
        second = job(work, corpus, ckpt, db, name + "-2")
        after = checkpoint_state(ckpt)
        rec["runs"] = [first, second]
        rec["checkpoint"] = after
        rec["replay"] = replay_scope(mid, after)

    elif name == "fresh-checkpoint":
        # The warehouse from the clean arm, and a checkpoint that has never seen it.
        # Every batch reprocesses. If the table comes out the same, the checkpoint is
        # not what makes this pipeline idempotent.
        src = os.path.join(work, "clean", "w.duckdb")
        shutil.copyfile(src, db)
        shutil.rmtree(ckpt, ignore_errors=True)
        rec["runs"] = [job(work, corpus, ckpt, db, name)]
        rec["checkpoint"] = checkpoint_state(ckpt)

    elif name == "wiped-warehouse":
        # The mirror. The checkpoint from the clean arm and an empty warehouse, which
        # is what a restore from backup looks like to a job that was never told.
        shutil.rmtree(ckpt, ignore_errors=True)
        shutil.copytree(os.path.join(work, "clean", "ckpt"), ckpt)
        if os.path.exists(db):
            os.remove(db)
        rec["runs"] = [job(work, corpus, ckpt, db, name)]
        rec["checkpoint"] = state_or_none(ckpt)

    else:
        raise ValueError(f"unknown arm {name!r}, have {ARMS}")

    rec["fingerprint"] = fingerprint(db)
    rec["duplicate_keys"] = duplicate_keys(db)
    with open(os.path.join(results, f"{name}.json"), "w", encoding="utf-8") as fh:
        json.dump(rec, fh, indent=2, default=str)
    return rec


def report(work: str) -> dict:
    results = os.path.join(work, "results")
    base_path = os.path.join(results, "clean.json")
    if not os.path.isfile(base_path):
        raise SystemExit("no clean.json, run the clean arm first")
    with open(base_path, encoding="utf-8") as fh:
        base = json.load(fh)

    rows = []
    for name in ARMS:
        p = os.path.join(results, f"{name}.json")
        if not os.path.isfile(p):
            # A missing arm is reported rather than skipped. A table with four rows
            # where five were expected looks complete.
            rows.append({"arm": name, "verdict": "NOT RUN"})
            continue
        with open(p, encoding="utf-8") as fh:
            r = json.load(fh)
        rows.append(
            {
                "arm": name,
                "fingerprint": r["fingerprint"],
                "rows": int(r["fingerprint"].split(":", 1)[0]),
                "verdict": compare(base["fingerprint"], r["fingerprint"]),
                "duplicate_keys": r["duplicate_keys"],
                "replay": r.get("replay"),
                "fingerprint_at_crash": r.get("fingerprint_at_crash"),
            }
        )
    return {"baseline": base["fingerprint"], "arms": rows}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="crash the job and check the warehouse afterwards")
    p.add_argument("--arm", choices=ARMS)
    p.add_argument("--corpus")
    p.add_argument("--work", required=True)
    p.add_argument("--crash-batch", type=int, default=3)
    p.add_argument("--report", action="store_true")
    a = p.parse_args(argv)

    if a.report:
        print(json.dumps(report(a.work), indent=2))
        return 0
    if not a.arm or not a.corpus:
        p.error("--arm and --corpus are required unless --report")
    out = run_arm(a.arm, a.corpus, a.work, a.crash_batch)
    print(json.dumps({k: v for k, v in out.items() if k != "runs"}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
