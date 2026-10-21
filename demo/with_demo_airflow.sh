#!/usr/bin/env bash
# Runs a command (make eval-live) with the worker and gateway reading the demo
# Airflow, then puts them back on AIRFLOW_BASE_URL from demo/.env, also on failure
# or Ctrl-C. Each restart waits until no investigation is running.
# Failures from the other Airflow during the run are investigated against the
# demo Airflow, so run it when that Airflow is quiet.
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=(docker compose --env-file demo/.env -f demo/docker-compose.yml)
export COMPOSE_PROFILES=demo
DEMO_AIRFLOW=http://airflow:8080
WORKER_PORT=$(sed -n 's/^WORKER_HOST_PORT=//p' demo/.env | tail -1)
WORKER_PORT=${WORKER_PORT:-8090}

wait_idle() {
  until curl -fsS "http://localhost:$WORKER_PORT/healthz" 2>/dev/null | grep -q '"investigating":0'; do
    echo "  waiting for the running investigation to finish..."
    sleep 10
  done
}

wait_ready() {
  until "${COMPOSE[@]}" exec -T openclaw-gateway curl -fsS localhost:18789/readyz 2>/dev/null | grep -q '"ready":true'; do
    sleep 3
  done
}

# $1: Airflow base URL for this start; empty = the value in demo/.env.
restart() {
  wait_idle
  if [ -n "$1" ]; then
    AIRFLOW_BASE_URL="$1" "${COMPOSE[@]}" up -d ingestion-worker openclaw-gateway
  else
    env -u AIRFLOW_BASE_URL "${COMPOSE[@]}" up -d ingestion-worker openclaw-gateway
  fi
  wait_ready
  echo "  worker reads $("${COMPOSE[@]}" exec -T ingestion-worker printenv AIRFLOW_BASE_URL)"
}

# The restore recreates both containers, and their logs go with them: keep the
# run's logs (retries, rate limits, fallbacks) in demo/.state/eval-live-logs/,
# the last 10 runs only.
LOG_DIR=demo/.state/eval-live-logs
save_logs() {
  local stamp
  stamp=$(date +%F-%H%M)
  mkdir -p "$LOG_DIR"
  for svc in openclaw-gateway ingestion-worker; do
    (umask 077; "${COMPOSE[@]}" logs --no-color --timestamps "$svc" > "$LOG_DIR/$stamp-$svc.log" 2>&1 || true)
  done
  echo "  logs saved: $LOG_DIR/$stamp-*.log"
  ls -1t "$LOG_DIR"/*.log | tail -n +21 | xargs rm -f
}

restore() {
  wait_idle
  save_logs || echo "  (could not save the logs)"
  echo "== restoring the worker and gateway to demo/.env"
  restart ""
}

echo "== pointing the worker and gateway at the demo Airflow ($DEMO_AIRFLOW)"
restart "$DEMO_AIRFLOW"
trap restore EXIT
"$@"
