"""The same four statements in two dialects.

There is no Snowflake account behind this repo. The standing decision on the program
side is to build against DuckDB, write the Snowflake statement beside it, and be loud
about which one has run. So every statement lives here twice and `DIALECTS` is what
`tests/test_warehouse.py` walks to check neither side has grown a statement the other
is missing.

WHAT HAS RUN. Every DuckDB statement below ran on duckdb 1.5.5. Not one
Snowflake statement has ever been sent to Snowflake. They are written from the docs
and they are unverified, which is a different thing from wrong and is not a better
thing. Treat them as a plan.
"""

SESSION_COLUMNS = [
    "user_id",
    "session_start",
    "session_end",
    "event_count",
    "page_depth",
    "duration_s",
    "converted",
    "bounce",
    "window_span_s",
]

# The merge key. A session window's start is the first event's timestamp and it does
# not move once the window closes, so the pair identifies a session for as long as the
# corpus and the gap stay put. Change the gap and every key changes with it, which is
# why scripts/merge_sessions.py reports the gap next to the row count.
MERGE_KEY = ["user_id", "session_start"]

_DUCKDB_CREATE = """
CREATE TABLE IF NOT EXISTS sessions (
    user_id        VARCHAR      NOT NULL,
    session_start  TIMESTAMP    NOT NULL,
    session_end    TIMESTAMP    NOT NULL,
    event_count    BIGINT       NOT NULL,
    page_depth     BIGINT       NOT NULL,
    duration_s     DOUBLE       NOT NULL,
    converted      INTEGER      NOT NULL,
    bounce         INTEGER      NOT NULL,
    window_span_s  DOUBLE       NOT NULL,
    loaded_at      TIMESTAMP    NOT NULL,
    PRIMARY KEY (user_id, session_start)
)
"""

# Snowflake has no enforced primary key. It accepts the declaration and does not check
# it, so the uniqueness this table needs comes from the MERGE and from nothing else.
# Writing PRIMARY KEY here and believing it is enforced is the mistake this comment
# exists to stop.
_SNOWFLAKE_CREATE = """
CREATE TABLE IF NOT EXISTS sessions (
    user_id        STRING       NOT NULL,
    session_start  TIMESTAMP_NTZ NOT NULL,
    session_end    TIMESTAMP_NTZ NOT NULL,
    event_count    NUMBER       NOT NULL,
    page_depth     NUMBER       NOT NULL,
    duration_s     FLOAT        NOT NULL,
    converted      NUMBER       NOT NULL,
    bounce         NUMBER       NOT NULL,
    window_span_s  FLOAT        NOT NULL,
    loaded_at      TIMESTAMP_NTZ NOT NULL
)
"""

_STAGE_CREATE = "CREATE TABLE IF NOT EXISTS sessions_stage AS SELECT * FROM sessions WHERE 1=0"

_SET = ", ".join(f"{c} = s.{c}" for c in SESSION_COLUMNS if c not in MERGE_KEY)
_INSERT_COLS = ", ".join([*SESSION_COLUMNS, "loaded_at"])
_INSERT_VALS = ", ".join([*(f"s.{c}" for c in SESSION_COLUMNS), "s.loaded_at"])
_ON = " AND ".join(f"t.{c} = s.{c}" for c in MERGE_KEY)

# WHEN MATCHED overwrites rather than skipping. A session can legitimately be emitted
# again with a higher event_count after a replay from an earlier checkpoint, and the
# later row is the more complete one. DO NOTHING would keep the truncated version
# forever and look perfectly idempotent while doing it.
_MERGE = f"""
MERGE INTO sessions AS t
USING sessions_stage AS s
ON {_ON}
WHEN MATCHED THEN UPDATE SET {_SET}, loaded_at = s.loaded_at
WHEN NOT MATCHED THEN INSERT ({_INSERT_COLS}) VALUES ({_INSERT_VALS})
"""

DIALECTS = {
    "duckdb": {
        "create_target": _DUCKDB_CREATE,
        "create_stage": _STAGE_CREATE,
        "truncate_stage": "DELETE FROM sessions_stage",
        "merge": _MERGE,
    },
    "snowflake": {
        "create_target": _SNOWFLAKE_CREATE,
        "create_stage": _STAGE_CREATE,
        "truncate_stage": "TRUNCATE TABLE sessions_stage",
        "merge": _MERGE,
    },
}
