"""Checks for the merge sink that need no database.

The dialect parity check is the one that earns its place. Two hand written SQL
dialects drift, and the way they drift is that somebody adds a statement to the one
they are running and not to the one they are not.
"""

from datetime import datetime

from warehouse.merge import DuplicateKeysInBatch, check_batch, dialect
from warehouse.sql import DIALECTS, MERGE_KEY, SESSION_COLUMNS

T0 = datetime(2026, 8, 16, 9, 0, 0)
T1 = datetime(2026, 8, 16, 9, 30, 0)


def row(user_id="u_1", start=T0, events=3):
    """One row in SESSION_COLUMNS order. Positional on purpose, because that is how
    the rows really arrive from Spark and a reordering should break something."""
    return (user_id, start, T1, events, 2, 120.0, 0, 0, 1800.0)


def check_every_dialect_defines_every_statement():
    names = {d: set(v) for d, v in DIALECTS.items()}
    first = next(iter(names.values()))
    for d, got in names.items():
        assert got == first, (d, sorted(got ^ first))
    assert "merge" in first and "create_target" in first


def check_no_dialect_ships_an_empty_statement():
    for d, statements in DIALECTS.items():
        for name, sql in statements.items():
            assert sql and sql.strip(), (d, name)


def check_the_merge_key_is_a_real_subset_of_the_columns():
    for k in MERGE_KEY:
        assert k in SESSION_COLUMNS, k
    assert len(set(MERGE_KEY)) == len(MERGE_KEY)


def check_the_merge_updates_every_non_key_column():
    """A column added to SESSION_COLUMNS and forgotten in the UPDATE clause would go
    stale on every row that is ever re-merged, and nothing else would notice."""
    merge = DIALECTS["duckdb"]["merge"]
    for c in SESSION_COLUMNS:
        if c in MERGE_KEY:
            continue
        assert f"{c} = s.{c}" in merge, c
    for k in MERGE_KEY:
        assert f"{k} = s.{k}," not in merge, f"{k} is the key and must not be updated"


def check_the_merge_overwrites_rather_than_skipping():
    """DO NOTHING would look idempotent and would pin the first version of a session
    forever, including a truncated one written before a replay finished it."""
    merge = DIALECTS["duckdb"]["merge"].upper()
    assert "WHEN MATCHED THEN UPDATE" in merge
    assert "DO NOTHING" not in merge


def check_an_unknown_dialect_is_refused():
    for bad in ("postgres", "", "DUCKDB"):
        try:
            dialect(bad)
        except ValueError:
            continue
        raise AssertionError(f"accepted dialect {bad!r}")


def check_a_batch_with_two_rows_for_one_key_is_refused():
    try:
        check_batch([row("u_1", T0), row("u_1", T0, events=9)])
    except DuplicateKeysInBatch as e:
        assert "u_1" in str(e), str(e)
        return
    raise AssertionError("a duplicate merge key was accepted")


def check_the_same_user_at_two_starts_is_not_a_duplicate():
    """The control for the check above. If this failed, the duplicate rule would be
    refusing a user with two sessions, which is most users."""
    check_batch([row("u_1", T0), row("u_1", T1)])


def check_two_users_at_one_start_is_not_a_duplicate():
    check_batch([row("u_1", T0), row("u_2", T0)])


def check_an_empty_batch_is_allowed_and_is_not_the_duplicate_case():
    check_batch([])
