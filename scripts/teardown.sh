#!/usr/bin/env bash
# Stops whatever is running and removes the state it left behind.
#
# Two bring-up paths exist, so this handles both and reports on each rather than
# guessing which one you used. It is safe to run when nothing is up.
#
#   ./scripts/teardown.sh              # broker and containers, keeps the data
#   ./scripts/teardown.sh --purge      # also removes logs, checkpoints and output
#
# --purge deletes RUN_DIR and the scratch paths the README commands write to. It does
# not touch the Kafka tarball, because re-downloading 120 MB to run the tests again is
# a bad default.

set -uo pipefail

RUN_DIR="${RUN_DIR:-/tmp/clickstream-local}"
PURGE=0
[ "${1:-}" = "--purge" ] && PURGE=1

# --- native broker, started by bootstrap-local.sh ---

PIDFILE="${RUN_DIR}/broker.pid"
if [ -f "$PIDFILE" ]; then
  PID="$(cat "$PIDFILE")"
  if kill -0 "$PID" 2>/dev/null; then
    kill "$PID" 2>/dev/null
    # Kafka flushes its log segments on the way out. Give it a moment before SIGKILL,
    # otherwise an unclean shutdown makes the next start replay the whole log.
    for _ in $(seq 1 15); do
      kill -0 "$PID" 2>/dev/null || break
      sleep 1
    done
    if kill -0 "$PID" 2>/dev/null; then
      echo "broker ${PID} ignored SIGTERM, sending SIGKILL"
      kill -9 "$PID" 2>/dev/null
    fi
    echo "native broker ${PID} stopped"
  else
    echo "native broker ${PID} was already gone"
  fi
  rm -f "$PIDFILE"
else
  echo "no native broker pid file, nothing to stop"
fi

# --- compose stack, started by setup.sh ---
# Kept for a machine with Docker on it. This repo's numbers were not taken there.

if command -v docker >/dev/null 2>&1; then
  COMPOSE_DIR="$(cd "$(dirname "$0")/../docker" && pwd)"
  if docker compose -f "${COMPOSE_DIR}/docker-compose.yml" ps -q 2>/dev/null | grep -q .; then
    docker compose -f "${COMPOSE_DIR}/docker-compose.yml" down -v
    echo "compose stack down, volumes removed"
  else
    echo "compose stack not running"
  fi
else
  echo "no docker on this machine, skipping the compose path"
fi

# --- state ---

if [ "$PURGE" -eq 1 ]; then
  rm -rf "${RUN_DIR}/data" "${RUN_DIR}/broker.log" "${RUN_DIR}/server.properties"
  # The paths the README's own commands write to. Named one by one on purpose. A
  # wildcard under /tmp in a script people run without reading it is how you lose
  # somebody else's work.
  rm -rf /tmp/events /tmp/sessions /tmp/ckpt /tmp/ckpt2 /tmp/all.jsonl /tmp/wh.duckdb \
         /tmp/progress.json /tmp/rm /tmp/c6 /tmp/all6.jsonl
  echo "purged broker state and the scratch paths from the README"
  echo "kept ${RUN_DIR}/*.tgz so the next bootstrap does not re-download"
else
  echo "state kept in ${RUN_DIR}. pass --purge to remove it"
fi
