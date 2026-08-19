"""Produce keyed records to a real broker and report where they landed.

This exists because `generator.population.crc32_partition` has claimed from the start
that it reproduces librdkafka's default partitioner, and nothing had ever checked it.
The README limitation said so plainly. This is the check.

It needs a broker. `scripts/setup.sh` brings one up locally. There is no offline mode
and no skip, because a partitioner probe that runs without a broker measures the same
Python function twice.

    python -m scripts.probe_partitioner --brokers 127.0.0.1:9092 --partitions 6

The verdict comes from `generator.population.partition_disagreements`, which is
library code with tests over it. This file produces records and formats output.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile

from generator.population import (
    Population,
    crc32_partition,
    model_divergence,
    murmur2_partition,
    partition_disagreements,
)


def create_topic(brokers: str, topic: str, partitions: int) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic  # noqa: PLC0415

    admin = AdminClient({"bootstrap.servers": brokers})
    existing = admin.list_topics(timeout=10).topics
    if topic in existing:
        have = len(existing[topic].partitions)
        if have != partitions:
            raise SystemExit(f"topic {topic} already exists with {have} partitions, wanted {partitions}")
        return
    for name, fut in admin.create_topics([NewTopic(topic, partitions, 1)]).items():
        fut.result(timeout=20)


def produce_keys(brokers: str, topic: str, keys: list[str]) -> dict[str, int]:
    """One record per key. The delivery report carries the partition the broker took.

    Reading the partition off the delivery report rather than off a consumer is
    deliberate. The consumer would tell you where a record was read from, which is
    the same answer one hop further away, and it needs a group and a rebalance to
    get there.
    """
    from confluent_kafka import Producer  # noqa: PLC0415

    landed: dict[str, int] = {}
    errors: list[str] = []

    def on_delivery(err, msg):
        if err is not None:
            errors.append(str(err))
            return
        landed[msg.key().decode("utf-8")] = msg.partition()

    producer = Producer({"bootstrap.servers": brokers, "linger.ms": 5})
    for key in keys:
        producer.produce(topic, key=key.encode("utf-8"), value=b"{}", on_delivery=on_delivery)
        producer.poll(0)
    remaining = producer.flush(30)
    if remaining:
        raise SystemExit(f"{remaining} records never got a delivery report")
    if errors:
        raise SystemExit(f"{len(errors)} delivery failures, first: {errors[0]}")
    return landed


def produce_keys_java(kafka_home: str, brokers: str, topic: str, keys: list[str]) -> None:
    """Same keys, through the Java client instead.

    `kafka-console-producer.sh` is the Java producer with its default partitioner, so
    this needs no code of ours on the write side at all. That is the point. A Java
    partitioner I wrote and then measured would be measuring my own port twice.
    """
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as fh:
        for key in keys:
            fh.write(f"{key}\t{{}}\n")
        path = fh.name
    cmd = [
        os.path.join(kafka_home, "bin", "kafka-console-producer.sh"),
        "--bootstrap-server", brokers,
        "--topic", topic,
        "--property", "parse.key=true",
        "--property", "key.separator=\t",
    ]
    with open(path) as stdin:
        proc = subprocess.run(cmd, stdin=stdin, capture_output=True, text=True, timeout=180)
    os.unlink(path)
    if proc.returncode != 0:
        raise SystemExit(f"console producer failed: {proc.stderr[-500:]}")


def read_back(brokers: str, topic: str, partitions: int, expected: int) -> dict[str, int]:
    """Read every partition to its end and record where each key is sitting.

    A consumer is the only way to see what the Java producer decided, since the
    delivery report stayed inside a JVM this process does not own.
    """
    from confluent_kafka import Consumer, TopicPartition  # noqa: PLC0415

    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": "probe-readback",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.assign([TopicPartition(topic, p, 0) for p in range(partitions)])
    landed: dict[str, int] = {}
    idle = 0
    while len(landed) < expected and idle < 40:
        msg = consumer.poll(0.5)
        if msg is None:
            idle += 1
            continue
        if msg.error():
            raise SystemExit(f"consume error: {msg.error()}")
        idle = 0
        if msg.key() is not None:
            landed[msg.key().decode("utf-8")] = msg.partition()
    consumer.close()
    return landed


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="check the partitioner models against a live broker")
    p.add_argument("--brokers", default="127.0.0.1:9092")
    p.add_argument("--topic", default="probe.partitioner")
    p.add_argument("--partitions", type=int, default=6)
    p.add_argument("--keys", type=int, default=200, help="how many distinct user ids to send")
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--client", default="librdkafka", choices=["librdkafka", "java", "both"])
    p.add_argument("--kafka-home", help="required for --client java, the unpacked kafka directory")
    p.add_argument("--dump", help="write the raw key to partition maps here")
    a = p.parse_args(argv)

    if a.client in ("java", "both") and not a.kafka_home:
        p.error("--kafka-home is required to drive the Java console producer")

    # Real user ids from the generator's own population, not made up strings. A
    # partitioner that agrees on "key0".."key9" and not on "u_3f9a21" is a
    # partitioner that agrees on the wrong alphabet.
    keys = Population(a.keys, alpha=1.0, seed=a.seed).user_ids
    out: dict = {
        "partitions": a.partitions,
        "librdkafka": __import__("confluent_kafka").libversion()[0],
        "divergence": model_divergence(keys, a.partitions),
        "arms": [],
    }
    failed = False
    raw: dict[str, dict[str, int]] = {}

    if a.client in ("librdkafka", "both"):
        topic = f"{a.topic}.rd"
        create_topic(a.brokers, topic, a.partitions)
        landed = produce_keys(a.brokers, topic, keys)
        if len(landed) != len(keys):
            raise SystemExit(f"produced {len(keys)} keys and got {len(landed)} delivery reports")
        raw["librdkafka"] = landed
        for model in (crc32_partition, murmur2_partition):
            v = partition_disagreements(landed, a.partitions, model)
            v.update({"topic": topic, "producer": "librdkafka"})
            out["arms"].append(v)
        failed = failed or not out["arms"][0]["agrees"]

    if a.client in ("java", "both"):
        topic = f"{a.topic}.java"
        create_topic(a.brokers, topic, a.partitions)
        produce_keys_java(a.kafka_home, a.brokers, topic, keys)
        landed = read_back(a.brokers, topic, a.partitions, len(keys))
        if len(landed) != len(keys):
            raise SystemExit(f"sent {len(keys)} keys and read back {len(landed)}")
        raw["java"] = landed
        for model in (murmur2_partition, crc32_partition):
            v = partition_disagreements(landed, a.partitions, model)
            v.update({"topic": topic, "producer": "java"})
            out["arms"].append(v)
            if model is murmur2_partition and not v["agrees"]:
                failed = True

    if a.dump:
        with open(a.dump, "w") as fh:
            json.dump({"partitions": a.partitions, "observed": raw}, fh, indent=2, sort_keys=True)

    print(json.dumps(out, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
