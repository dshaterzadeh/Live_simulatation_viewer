#!/usr/bin/env bash
# One entry point for the whole prototype.
#
#   ./run.sh up        build if needed, start broker + publisher + sensors + frontend
#   ./run.sh down      stop and remove all four containers
#   ./run.sh restart   down + up
#   ./run.sh status    container states and the URLs to open
#   ./run.sh logs [service]   follow logs (default: publisher)
#   ./run.sh dev       host-side publisher + sensors from .venv against the
#                      Docker broker, for fast iteration without rebuilding
#
# Knobs, all optional:  DELAY=1.0  LOOP=1  SEED=42  SENSOR_DELAY=1.0
#   LOOP=0 ./run.sh up        single bounded pass instead of looping forever
#   SEED=42 ./run.sh up       reproducible sensor noise and faults
#
# Everything else (service definitions, ports, health ordering) is in
# docker-compose.yml; this script only translates the knobs into flags.

set -euo pipefail
cd "$(dirname "$0")"

PAGE="http://localhost:8002/mqtt_web_tester.html"

need_docker() {
  if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker is not running (or not installed). Start Docker Desktop and retry." >&2
    exit 1
  fi
}

# Translate LOOP / SEED into the flags the two Python CLIs take. The publisher
# only has --loop (absence = one pass); the simulator has --loop/--no-loop.
export_flags() {
  if [ "${LOOP:-1}" = "0" ]; then
    export LOOP_FLAG="" SENSOR_LOOP_FLAG="--no-loop"
  else
    export LOOP_FLAG="--loop" SENSOR_LOOP_FLAG="--loop"
  fi
  export SEED_FLAG="${SEED:+--seed $SEED}"
}

cmd_up() {
  need_docker
  export_flags
  docker compose up -d --build --remove-orphans
  echo
  cmd_status
}

cmd_down() {
  need_docker
  export_flags
  docker compose down --remove-orphans
}

cmd_status() {
  need_docker
  docker compose ps --format 'table {{.Service}}\t{{.Status}}\t{{.Ports}}' 2>/dev/null || docker compose ps
  echo
  echo "Dashboard : $PAGE  → click Connect"
  echo "Replay    : ${LOOP:-1} loop, DELAY=${DELAY:-1.0}s/step (speed is live from the dashboard)"
  echo "Logs      : ./run.sh logs [publisher|sensors|mosquitto|frontend]"
}

cmd_logs() {
  need_docker
  docker compose logs -f --tail=50 "${1:-publisher}"
}

# Host-side iteration: the broker stays in Docker (it never changes), the two
# Python processes run from .venv so an edit is a Ctrl+C and a re-run away.
cmd_dev() {
  need_docker
  if [ ! -x .venv/bin/python ]; then
    echo "❌ .venv/ missing. Create it: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
    exit 1
  fi
  export_flags
  docker compose up -d mosquitto frontend
  echo "Broker + frontend up. Starting publisher and sensors from .venv (Ctrl+C stops both)…"
  trap 'kill 0' EXIT INT TERM
  # shellcheck disable=SC2086
  .venv/bin/python sensor_simulator.py -f 20260623_baseline.hdf5 -t sensors \
      --delay "${SENSOR_DELAY:-0.3}" $SENSOR_LOOP_FLAG $SEED_FLAG &
  # shellcheck disable=SC2086
  .venv/bin/python hdf5_mqtt_publisher.py -f 20260623_baseline.hdf5 -t sim/coesi5 \
      --delay "${DELAY:-0.3}" $LOOP_FLAG
}

case "${1:-}" in
  up)      cmd_up ;;
  down)    cmd_down ;;
  restart) cmd_down; cmd_up ;;
  status)  cmd_status ;;
  logs)    cmd_logs "${2:-}" ;;
  dev)     cmd_dev ;;
  *)
    sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
