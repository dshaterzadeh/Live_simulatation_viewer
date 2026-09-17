#!/usr/bin/env bash
# One entry point for the whole prototype.
#
#   ./run.sh up        build if needed, start brokers + engine + bridge + sensors + frontend
#   ./run.sh down      stop and remove all six containers
#   ./run.sh restart   down + up
#   ./run.sh status    container states and the URLs to open
#   ./run.sh logs [service]   follow logs (default: engine)
#   ./run.sh dev       host-side HELICS broker + engine + bridge from .venv against
#                      the Docker Mosquitto, for fast iteration without rebuilding
#
# All configuration is in .env (copy .env.example); there are no defaults in
# this script. A variable exported in the shell wins over .env for one run:
#   PACE=0 ./run.sh up        free-run: as fast as the federation computes
#   LOOP=0 ./run.sh up        single bounded pass instead of looping forever
#   SEED=42 ./run.sh up       reproducible sensor noise and faults
#
# Everything else (service definitions, ports, health ordering) is in
# docker-compose.yml.

set -euo pipefail
cd "$(dirname "$0")"

if [ ! -f .env ]; then
  echo "❌ .env is missing. Create it from the template and edit as needed:  cp .env.example .env" >&2
  exit 1
fi
# Shell exports take precedence (as they do for docker compose): only fill in
# what the caller did not set.
while IFS='=' read -r k v; do
  [ -n "$k" ] && [ -z "${!k+x}" ] && export "$k=$v"
done < <(grep -v '^[[:space:]]*#' .env | grep '=' | sed 's/[[:space:]]*#.*$//')

PAGE="http://localhost:${FRONTEND_PORT:?FRONTEND_PORT is not set — see .env.example}/mqtt_web_tester.html"

need_docker() {
  if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker is not running (or not installed). Start Docker Desktop and retry." >&2
    exit 1
  fi
}

cmd_up() {
  need_docker
  docker compose up -d --build --remove-orphans
  echo
  cmd_status
}

cmd_down() {
  need_docker
  docker compose down --remove-orphans
}

cmd_status() {
  need_docker
  docker compose ps --format 'table {{.Service}}\t{{.Status}}\t{{.Ports}}' 2>/dev/null || docker compose ps
  echo
  echo "Dashboard : $PAGE  → click Connect"
  echo "Replay    : loop=${LOOP:?}, PACE=${PACE:?} sim s per real s (play/pause/step/pace are live from the dashboard)"
  echo "Logs      : ./run.sh logs [engine|bridge|helics-broker|sensors|mosquitto|frontend]"
}

cmd_logs() {
  need_docker
  docker compose logs -f --tail=50 "${1:-engine}"
}

# Host-side iteration: Mosquitto, sensors and the frontend stay in Docker (they
# never change while working on the federation); the HELICS broker, engine and
# bridge run from .venv so an edit is a Ctrl+C and a re-run away. They take no
# arguments: .env is in the environment, with its host-side addresses.
#
# The HELICS broker runs host-side too, not in Docker: a ZMQ core needs the
# broker to connect *back* to each federate, and a broker inside Docker
# Desktop's VM cannot reliably reach processes on the Mac. It is one line.
cmd_dev() {
  need_docker
  if [ ! -x .venv/bin/python ]; then
    echo "❌ .venv/ missing. Create it: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
    exit 1
  fi
  docker compose up -d mosquitto frontend sensors
  docker compose stop engine bridge helics-broker >/dev/null 2>&1 || true
  echo "Mosquitto + sensors + frontend up. Starting HELICS broker, bridge and engine from .venv (Ctrl+C stops all)…"
  trap 'kill 0' EXIT INT TERM
  .venv/bin/python helics_broker.py &
  sleep 1
  .venv/bin/python bridge.py &
  .venv/bin/python engine.py
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
