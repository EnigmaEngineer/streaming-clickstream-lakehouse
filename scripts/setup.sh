#!/usr/bin/env bash
# Brings Kafka up under Docker, waits for it, creates the topic.
#
# This needs a Docker daemon and it has never run on the machine every number in this
# repo came off. Use scripts/bootstrap-local.sh unless you specifically want the
# container. The MinIO service this used to start is gone. Nothing ever connected to it.
set -euo pipefail

cd "$(dirname "$0")/../docker"
docker compose up -d

echo "waiting for kafka..."
for i in $(seq 1 30); do
  if docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

docker compose exec -T kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic clickstream.events \
  --partitions 6 \
  --replication-factor 1

echo "topic ready. 6 partitions."
