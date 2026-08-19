"""Where events go.

The Kafka import is inside the class on purpose. Everything except `KafkaSink` runs
with the standard library, so a clone with nothing installed can still run the
generator and the tests.
"""

import json
import sys


class JsonlSink:
    """One JSON object per line. Takes an already-open file, so the caller decides
    whether that is a real file or stdout."""

    def __init__(self, handle=None):
        self.handle = handle or sys.stdout
        self.count = 0

    def send(self, key: str, value: dict) -> None:
        self.handle.write(json.dumps(value, separators=(",", ":")) + "\n")
        self.count += 1

    def flush(self) -> None:
        self.handle.flush()

    def close(self) -> None:
        self.flush()


class NullSink:
    """Counts and discards. This is what the rate measurements run against, so that
    the number reported is the generator's cost and not the disk's."""

    def __init__(self):
        self.count = 0

    def send(self, key: str, value: dict) -> None:
        self.count += 1

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


class MemorySink:
    """Keeps everything. For tests only. It will happily exhaust memory on a long run."""

    def __init__(self):
        self.records: list[tuple[str, dict]] = []

    @property
    def count(self) -> int:
        return len(self.records)

    def send(self, key: str, value: dict) -> None:
        self.records.append((key, value))

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


class KafkaSink:
    """confluent-kafka producer.

    This has now run against a real broker, 5,446 events with zero delivery failures.
    It sat here untested for a long time before that, because claiming a path works
    without running it is the thing this repo is meant to avoid. The delivery callback
    is here so a failure is loud rather than silent.
    """

    def __init__(self, brokers: str, topic: str):
        from confluent_kafka import Producer  # noqa: PLC0415

        self.topic = topic
        self.failures = 0
        self.count = 0
        self._producer = Producer({"bootstrap.servers": brokers, "linger.ms": 20})

    def _on_delivery(self, err, msg):
        if err is not None:
            self.failures += 1

    def send(self, key: str, value: dict) -> None:
        import json as _json

        self._producer.produce(
            self.topic,
            key=key.encode("utf-8"),
            value=_json.dumps(value, separators=(",", ":")).encode("utf-8"),
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)
        self.count += 1

    def flush(self) -> None:
        self._producer.flush(10)

    def close(self) -> None:
        self.flush()
        if self.failures:
            raise RuntimeError(f"{self.failures} of {self.count} records failed delivery")
