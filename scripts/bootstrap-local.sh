#!/usr/bin/env bash
# Kafka in KRaft mode on the JVM directly. No Docker.
#
# This exists because scripts/setup.sh needs a Docker daemon and every measurement in
# this repo was taken on a machine that has none. A bring-up script nobody can run is
# not a bring-up script. This one has run.
#
# Everything lands under RUN_DIR so teardown.sh has one place to look.
#
#   ./scripts/bootstrap-local.sh
#   ./scripts/teardown.sh
#
# Set KAFKA_TARBALL to a tarball you already have and the download is skipped.

set -euo pipefail

KAFKA_VERSION="${KAFKA_VERSION:-3.7.1}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
RUN_DIR="${RUN_DIR:-/tmp/clickstream-local}"
TOPIC="${KAFKA_TOPIC:-clickstream.events}"
PARTITIONS="${KAFKA_PARTITIONS:-6}"
PORT="${KAFKA_PORT:-9092}"

NAME="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
TARBALL="${KAFKA_TARBALL:-${RUN_DIR}/${NAME}.tgz}"
HOME_DIR="${RUN_DIR}/${NAME}"

mkdir -p "$RUN_DIR"

if [ ! -d "$HOME_DIR" ]; then
  if [ ! -f "$TARBALL" ]; then
    echo "fetching kafka ${KAFKA_VERSION}"
    # archive.apache.org has served this between 0.33 and 3.0 MB/s on the same week.
    # -C - so a killed call resumes rather than starting over.
    curl -fSL -C - -o "$TARBALL" \
      "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${NAME}.tgz"
  fi
  tar xzf "$TARBALL" -C "$RUN_DIR"
fi

DATA_DIR="${RUN_DIR}/data"
CONF="${RUN_DIR}/server.properties"

# getLocalHost() is what Kafka calls to work out its own address, and /etc/hosts is not
# writable everywhere this runs. Pinning both listeners to the loopback address is the
# difference between a broker that starts and an UnknownHostException.
cat > "$CONF" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:${PORT},CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:${PORT}
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=${DATA_DIR}
num.partitions=${PARTITIONS}
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
EOF

if [ ! -f "${DATA_DIR}/meta.properties" ]; then
  CLUSTER_ID="$("${HOME_DIR}/bin/kafka-storage.sh" random-uuid)"
  "${HOME_DIR}/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$CONF" >/dev/null
  echo "formatted cluster ${CLUSTER_ID}"
fi

# 640M is enough for a single node carrying a few million small records. The default
# heap will not fit alongside a Spark driver on a two core box.
export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xmx640M -Xms256M}"

nohup "${HOME_DIR}/bin/kafka-server-start.sh" "$CONF" > "${RUN_DIR}/broker.log" 2>&1 &
echo $! > "${RUN_DIR}/broker.pid"
echo "broker starting, pid $(cat "${RUN_DIR}/broker.pid")"

# Every attempt pays its own JVM start, so this loop is slower than it looks. Six tries
# is roughly twenty to thirty wall seconds.
ready=0
for _ in $(seq 1 12); do
  if "${HOME_DIR}/bin/kafka-broker-api-versions.sh" \
      --bootstrap-server "127.0.0.1:${PORT}" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done

if [ "$ready" -ne 1 ]; then
  echo "broker did not come up. last 30 lines of ${RUN_DIR}/broker.log:" >&2
  tail -30 "${RUN_DIR}/broker.log" >&2
  exit 1
fi

"${HOME_DIR}/bin/kafka-topics.sh" --bootstrap-server "127.0.0.1:${PORT}" \
  --create --if-not-exists --topic "$TOPIC" \
  --partitions "$PARTITIONS" --replication-factor 1

echo "ready. topic ${TOPIC}, ${PARTITIONS} partitions, broker on 127.0.0.1:${PORT}"
echo "state in ${RUN_DIR}. tear it down with ./scripts/teardown.sh"
