from datetime import datetime

from generator.clock import VirtualClock
from generator.produce import Config, run, partition_skew, build_sink
from generator.sinks import MemorySink, NullSink


def _run(**over):
    cfg = Config(rate=200.0, seconds=5.0, seed=13, users=500, **over)
    sink = MemorySink()
    stats = run(cfg, sink, clock=VirtualClock(speedup=1e9))
    return cfg, sink, stats


def check_the_generator_emits_the_number_of_events_the_rate_implies():
    cfg, sink, stats = _run()
    assert stats.originals == 1000, stats.originals
    assert stats.emitted == stats.originals + stats.duplicates
    assert sink.count == stats.emitted


def check_duplicates_carry_the_same_event_id_and_a_later_ingest_time():
    cfg, sink, stats = _run(dup_rate=0.2)
    assert stats.duplicates > 0, "no duplicates at a 20 percent rate"
    by_id: dict[str, list] = {}
    for _, v in sink.records:
        by_id.setdefault(v["event_id"], []).append(v)
    repeated = [v for v in by_id.values() if len(v) > 1]
    assert len(repeated) == stats.duplicates, (len(repeated), stats.duplicates)
    for pair in repeated:
        assert pair[0]["event_ts"] == pair[1]["event_ts"], "a copy moved the event time"
        assert pair[1]["ingest_ts"] > pair[0]["ingest_ts"], (
            pair[0]["ingest_ts"],
            pair[1]["ingest_ts"],
        )


def check_a_copy_due_after_the_run_ends_is_counted_and_not_flushed():
    # The first build flushed the queue at the end and stamped every copy with the
    # final clock reading, so a hundred of them landed inside one millisecond. That
    # is a pattern the streaming job would have to explain and the generator invented it.
    cfg, sink, stats = _run(dup_rate=0.3)
    assert stats.duplicates_pending > 0, "nothing was left pending, the check is idle"
    stamps = [v["ingest_ts"] for _, v in sink.records]
    last = max(stamps)
    assert stamps.count(last) < 5, f"{stamps.count(last)} events share the final stamp"


def check_no_duplicates_at_all_when_the_rate_is_zero():
    cfg, sink, stats = _run(dup_rate=0.0)
    ids = [v["event_id"] for _, v in sink.records]
    assert len(ids) == len(set(ids)), "duplicate emitted at dup_rate 0"
    assert stats.duplicates == 0


def check_a_late_event_has_an_event_time_behind_its_ingest_time():
    cfg, sink, stats = _run(late_rate=0.5, dup_rate=0.0)
    behind = [v for _, v in sink.records if v["event_ts"] < v["ingest_ts"]]
    assert len(behind) == stats.late_events, (len(behind), stats.late_events)
    assert stats.late_events > 300, stats.late_events
    # Nothing may arrive from the future.
    assert not [v for _, v in sink.records if v["event_ts"] > v["ingest_ts"]]


def check_lateness_is_off_entirely_at_a_rate_of_zero():
    cfg, sink, stats = _run(late_rate=0.0, dup_rate=0.0)
    assert stats.late_events == 0
    assert all(v["event_ts"] == v["ingest_ts"] for _, v in sink.records)


def check_the_key_is_the_user_id():
    # Session locality in the streaming job rests entirely on this.
    cfg, sink, stats = _run()
    assert all(k == v["user_id"] for k, v in sink.records)


def check_a_heavier_tail_makes_the_busiest_partition_busier():
    _, _, flat = _run(alpha=0.0)
    _, _, steep = _run(alpha=1.3)
    a = partition_skew(flat, 6)["max_over_even"]
    b = partition_skew(steep, 6)["max_over_even"]
    assert b > a, (b, a)


def check_skew_over_one_partition_is_exactly_even():
    _, _, stats = _run()
    s = partition_skew(stats, 1)
    assert s["max_over_even"] == 1.0 and s["min_over_even"] == 1.0, s


def check_the_run_is_reproducible_at_a_fixed_seed():
    a = _run()[1].records
    b = _run()[1].records
    assert [v["event_id"] for _, v in a] != [v["event_id"] for _, v in b], (
        "event ids are uuid4 and must not be reproducible"
    )
    strip = lambda rs: [(k, v["event_type"], v["page"], v["session_hint"]) for k, v in rs]
    assert strip(a) == strip(b), "the same seed produced different traffic"


def check_the_kafka_sink_refuses_without_a_broker_and_a_topic():
    # This used to assert NotImplementedError, because the sink had never run
    # against a broker and a path nobody has executed is worse than an absent one. It
    # has now run, so what is left to check is that it will not quietly produce into
    # the default topic on a caller who forgot to name one.
    for brokers, topic in (("", ""), ("127.0.0.1:9092", ""), ("", "clickstream.events")):
        try:
            build_sink("kafka", None, brokers, topic)
        except (ValueError, ImportError, ModuleNotFoundError) as e:
            assert "kafka" in str(e).lower() or "topic" in str(e).lower(), str(e)
        else:
            raise AssertionError(f"kafka sink built with brokers={brokers!r} topic={topic!r}")


def check_an_unknown_sink_name_is_refused():
    try:
        build_sink("snowflake", None)
    except ValueError as e:
        assert "snowflake" in str(e), str(e)
    else:
        raise AssertionError("an unknown sink name was accepted")


def check_a_file_sink_without_a_path_is_refused():
    try:
        build_sink("file", None)
    except ValueError as e:
        assert "--out" in str(e), str(e)
    else:
        raise AssertionError("file sink accepted a missing path")
