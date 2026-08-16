"""Checks that run the MERGE against a real DuckDB.

These need duckdb, so they live behind tests/run_warehouse.py. There is no skip path
and no in-memory fake of a database. A fake would agree with whatever the code does.

Every fixture below gives the rule under test something to choose between. A merge
fixture with one row per key cannot test a merge.
"""

from datetime import datetime

import duckdb

from warehouse.merge import DuplicateKeysInBatch, apply_batch, ensure_tables, table_fingerprint

T0 = datetime(2026, 8, 16, 9, 0, 0)
T1 = datetime(2026, 8, 16, 9, 30, 0)
T2 = datetime(2026, 8, 16, 11, 0, 0)
LOAD_A = datetime(2026, 8, 16, 12, 0, 0)
LOAD_B = datetime(2026, 8, 16, 13, 0, 0)


def row(user_id="u_1", start=T0, events=3, converted=0):
    return (user_id, start, T1, events, 2, 120.0, converted, 0, 1800.0)


def fresh():
    con = duckdb.connect()
    ensure_tables(con)
    return con


def check_a_first_batch_inserts_everything():
    con = fresh()
    out = apply_batch(con, [row("u_1"), row("u_2")], loaded_at=LOAD_A)
    assert out["rows_after"] == 2, out
    assert out["inserted"] == 2 and out["updated"] == 0, out


def check_replaying_the_same_batch_changes_nothing():
    """The idempotency claim, checked on the table contents and not on the row count.
    A merge that dropped one row and inserted another would keep the count."""
    con = fresh()
    apply_batch(con, [row("u_1"), row("u_2")], loaded_at=LOAD_A)
    before = table_fingerprint(con)
    out = apply_batch(con, [row("u_1"), row("u_2")], loaded_at=LOAD_B)
    assert table_fingerprint(con) == before, (before, table_fingerprint(con))
    assert out["inserted"] == 0 and out["updated"] == 2, out


def check_a_later_version_of_a_session_overwrites_the_earlier_one():
    """The reason the sink merges instead of inserting. A replay from an earlier
    checkpoint re-emits a session with more events in it, and that row is the better
    one."""
    con = fresh()
    apply_batch(con, [row("u_1", events=3)], loaded_at=LOAD_A)
    apply_batch(con, [row("u_1", events=11, converted=1)], loaded_at=LOAD_B)
    got = con.execute("SELECT event_count, converted FROM sessions WHERE user_id='u_1'").fetchall()
    assert got == [(11, 1)], got


def check_the_fingerprint_ignores_the_load_time_and_nothing_else():
    con = fresh()
    apply_batch(con, [row("u_1", events=3)], loaded_at=LOAD_A)
    same_data_later = table_fingerprint(con)
    apply_batch(con, [row("u_1", events=3)], loaded_at=LOAD_B)
    assert table_fingerprint(con) == same_data_later
    apply_batch(con, [row("u_1", events=4)], loaded_at=LOAD_B)
    assert table_fingerprint(con) != same_data_later


def check_a_duplicate_key_inside_one_batch_never_reaches_the_database():
    """Refused by check_batch before the MERGE is sent. The point of catching it here
    is that the message names the key. A database refusing it would too, eventually,
    and by then the stage table has already been written."""
    con = fresh()
    try:
        apply_batch(con, [row("u_1", events=3), row("u_1", events=9)], loaded_at=LOAD_A)
    except DuplicateKeysInBatch:
        assert con.execute("SELECT count(*) FROM sessions").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM sessions_stage").fetchone()[0] == 0
        return
    raise AssertionError("a batch with a duplicate merge key was applied")


def check_the_database_would_have_refused_it_too():
    """The control for the check above. Without this, check_batch could be guarding
    against a thing DuckDB is perfectly happy with, and the guard would be theatre.

    Staging both rows by hand and running the MERGE has to fail. If this ever starts
    passing, the guard has become the only thing standing between a replay and a
    silently arbitrary winner, which is worth knowing.
    """
    con = fresh()
    from warehouse.sql import DIALECTS

    for r in (row("u_1", events=3), row("u_1", events=9)):
        con.execute("INSERT INTO sessions_stage VALUES (?,?,?,?,?,?,?,?,?,?)", [*r, LOAD_A])
    try:
        con.execute(DIALECTS["duckdb"]["merge"])
    except Exception as e:
        assert "u_1" in str(e) or "multiple" in str(e).lower() or "twice" in str(e).lower(), str(e)
        return
    raise AssertionError("duckdb accepted a merge with two source rows for one key")


def check_an_empty_batch_leaves_the_table_alone():
    con = fresh()
    apply_batch(con, [row("u_1")], loaded_at=LOAD_A)
    before = table_fingerprint(con)
    out = apply_batch(con, [], loaded_at=LOAD_B)
    assert table_fingerprint(con) == before
    assert out["batch_rows"] == 0 and out["inserted"] == 0


def check_two_batches_of_different_sessions_accumulate():
    con = fresh()
    apply_batch(con, [row("u_1", start=T0)], loaded_at=LOAD_A)
    apply_batch(con, [row("u_1", start=T2)], loaded_at=LOAD_B)
    assert con.execute("SELECT count(*) FROM sessions WHERE user_id='u_1'").fetchone()[0] == 2


def check_the_stage_is_empty_after_a_batch():
    """A stage left full would be merged again by the next batch, so the sink would
    replay the previous batch on every call and look idempotent while doing it."""
    con = fresh()
    apply_batch(con, [row("u_1"), row("u_2")], loaded_at=LOAD_A)
    assert con.execute("SELECT count(*) FROM sessions_stage").fetchone()[0] == 0
