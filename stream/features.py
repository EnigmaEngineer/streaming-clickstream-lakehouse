"""Per-session features. Duration and page depth and bounce and conversion.

Day 4's blueprint line. Day 3 deliberately aggregated only a count so that its
numbers did not rest on work that had not been done yet.

The aggregations live here rather than inside `session_windows` because the window
is one decision and what gets measured inside it is another. `sessionize` still owns
the grouping and calls this for the expression list, so there is still one door.
"""

from pyspark.sql import Column, functions as F

# What counts as a conversion. One string in one place, because a funnel that grows a
# second terminal event later should break this line rather than quietly disagree with
# generator/session.py.
CONVERSION_EVENT = "checkout"


def aggregations() -> list[Column]:
    """The aggregate expressions applied inside each session window.

    `duration_s` is the span of real events and NOT the span of the window. Those are
    not the same thing and the difference is not small. A session window ends one gap
    after its last event, so `sw.end - sw.start` adds the full gap to every session on
    the topic. At the default thirty minute gap that inflates a two minute visit by a
    factor of sixteen. It is the easiest wrong number in this whole project to publish
    and it looks completely reasonable in a dashboard.

    `page_depth` is a set built and then measured, not a `countDistinct`. Spark refuses
    a distinct aggregate on a streaming DataFrame outright, with
    "Distinct aggregations are not supported on streaming DataFrames/Datasets", and it
    suggests `approx_count_distinct` in the error. That suggestion is wrong for this
    column. Page depth here runs from 1 to about 10, a sketch is an estimate where an
    exact answer is available, and a bounded program on this side already found that an
    approximate aggregate is the wrong thing to compare across runs. `collect_set` is
    exact and its state is one set of page strings per open session.
    """
    return [
        F.count("*").alias("event_count"),
        F.size(F.collect_set("page")).alias("page_depth"),
        # min and max over the real event times, not the window bounds.
        (F.max("event_ts").cast("double") - F.min("event_ts").cast("double")).alias("duration_s"),
        F.max(F.when(F.col("event_type") == CONVERSION_EVENT, 1).otherwise(0)).alias("converted"),
    ]


def derive(df):
    """Columns that are cheaper to compute after the aggregation than inside it.

    `bounce` is one page and one event. Both halves are needed. A visitor who lands,
    scrolls twice and leaves saw one page and is not a bounce by the count rule alone,
    and calling that engagement is generous. Reporting both parts means whoever
    disagrees with the definition can rebuild theirs from the columns.
    """
    return df.withColumn(
        "bounce",
        F.when((F.col("page_depth") == 1) & (F.col("event_count") == 1), 1).otherwise(0),
    )
