#!/usr/bin/env bash
# Brings up Kafka and MinIO, waits for Kafka, creates the topic.
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
