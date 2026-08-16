"""Land a batch of sessions into the warehouse without creating duplicates.

The shape is stage then merge. A batch goes into `sessions_stage`, one MERGE moves it
into `sessions`, the stage is emptied. That is the same shape a Snowflake load takes
with a staged Parquet file in front of it, so the DuckDB path here is a rehearsal of
the real one rather than a different design.

ONE DOOR. `apply_batch` is the only function that writes to `sessions`. ot-026 on the
program side is about a rule each caller has to remember, and the answer that has
worked twice now is to leave callers no second route in. `stream/sessionize.py` does
the same for the read side.
"""

from datetime import datetime, timezone

from warehouse.sql import DIALECTS, MERGE_KEY, SESSION_COLUMNS


class DuplicateKeysInBatch(Exception):
    """Raised before the MERGE runs, not after it fails.

    A MERGE whose source holds two rows for one key has no defined answer. Snowflake
    raises on it by default and DuckDB refuses too, so nothing silently picks a winner
    and this class is not guarding against corruption. It exists so the error names
    the key and the count instead of arriving as a database message about
    nondeterminism.
    """


def dialect(name: str) -> dict:
    if name not in DIALECTS:
        raise ValueError(f"unknown dialect {name!r}, have {sorted(DIALECTS)}")
    return DIALECTS[name]


def ensure_tables(con, name: str = "duckdb") -> None:
    d = dialect(name)
    con.execute(d["create_target"])
    con.execute(d["create_stage"])


def check_batch(rows: list[tuple]) -> None:
    """Refuse a batch that would make the MERGE nondeterministic.

    Reads the key columns by position out of SESSION_COLUMNS rather than by name,
    because the rows arrive as tuples from Spark and a column reordering upstream
    should break here rather than merge on the wrong pair.
    """
    if not rows:
        # An empty batch is a real thing on a stream with a quiet minute in it, so it
        # is allowed. It is separated from the duplicate case on purpose.
        return
    idx = [SESSION_COLUMNS.index(c) for c in MERGE_KEY]
    seen: dict[tuple, int] = {}
    for row in rows:
        k = tuple(row[i] for i in idx)
        seen[k] = seen.get(k, 0) + 1
    dupes = {k: n for k, n in seen.items() if n > 1}
    if dupes:
        first = next(iter(dupes))
        raise DuplicateKeysInBatch(f"{len(dupes)} duplicate merge keys in a batch of {len(rows)}, first {first}")


def apply_batch(con, rows: list[tuple], name: str = "duckdb", loaded_at: datetime | None = None) -> dict:
    """Stage, merge, clear. Returns what changed.

    `before` and `after` are counted rather than inferred from the batch size. A merge
    that inserts nothing because every key already existed and a merge that inserted
    everything both return without complaint, and the row delta is the only thing that
    tells them apart.
    """
    d = dialect(name)
    check_batch(rows)
    stamp = loaded_at or datetime.now(timezone.utc).replace(tzinfo=None)

    before = con.execute("SELECT count(*) FROM sessions").fetchone()[0]
    con.execute(d["truncate_stage"])
    if rows:
        placeholders = ", ".join(["?"] * (len(SESSION_COLUMNS) + 1))
        con.executemany(
            f"INSERT INTO sessions_stage VALUES ({placeholders})",
            [(*row, stamp) for row in rows],
        )
        con.execute(d["merge"])
    after = con.execute("SELECT count(*) FROM sessions").fetchone()[0]
    con.execute(d["truncate_stage"])

    return {
        "batch_rows": len(rows),
        "rows_before": before,
        "rows_after": after,
        "inserted": after - before,
        "updated": len(rows) - (after - before),
        "dialect": name,
    }


def table_fingerprint(con) -> str:
    """A value that changes when any cell changes.

    `loaded_at` is left out on purpose. Two runs of the same batch write different
    load timestamps and the table is still the same table, so including it would make
    every idempotency check fail for the one reason that does not matter.
    """
    cols = ", ".join(SESSION_COLUMNS)
    row = con.execute(
        f"SELECT count(*), md5(string_agg(concat_ws('|', {cols}), '\n' ORDER BY {', '.join(MERGE_KEY)})) FROM sessions"
    ).fetchone()
    return f"{row[0]}:{row[1]}"
