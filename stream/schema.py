"""The wire schema, as Spark sees it.

Nothing here is inferred. A file source will happily infer a schema off the first
batch and then change its mind when a later batch has a null where the first one had
a string. Declaring it also means a field the generator stops sending shows up as a
column of nulls rather than as a missing column, which is the failure I would rather
debug.

Both timestamps arrive as ISO-8601 strings and are cast here rather than parsed by
the JSON reader. That puts the only place a bad timestamp can enter into one
function, so it is the only place that has to count them.
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType, StructField, StructType

# Matches docs/event-schema.md. The two timestamps are strings on the wire.
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("session_hint", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("page", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("device", StringType(), True),
        StructField("country", StringType(), True),
        StructField("event_ts", StringType(), False),
        StructField("ingest_ts", StringType(), False),
    ]
)

# The generator writes isoformat(timespec="milliseconds") with Z for UTC. Spark's
# default ISO parser handles that, but naming the pattern means a generator change
# breaks the cast loudly instead of silently producing nulls in a different timezone.
TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"


def cast_times(df: DataFrame) -> DataFrame:
    """Turn the two string timestamps into real timestamps.

    A row whose `event_ts` will not parse gets a null and is kept, not dropped. It is
    counted by `parse_failures` instead. Dropping it here would make a malformed
    producer look like a quiet dip in volume, and a dip in volume is the hardest
    thing on this list to notice.
    """
    return (
        df.withColumn("event_ts", F.to_timestamp("event_ts", TS_FORMAT))
        .withColumn("ingest_ts", F.to_timestamp("ingest_ts", TS_FORMAT))
        .withColumn("lateness_s", F.col("ingest_ts").cast("double") - F.col("event_ts").cast("double"))
    )


def parse_failures(df: DataFrame) -> DataFrame:
    """The rows `cast_times` could not read. Kept as a separate view so a caller has
    to ask for them rather than find them missing."""
    return df.where(F.col("event_ts").isNull() | F.col("ingest_ts").isNull())
