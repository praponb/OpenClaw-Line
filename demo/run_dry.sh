#!/usr/bin/env bash
# Dry run: wait for the ingestion worker (DRY_RUN=1) to write the incident
# message, then run the investigator on it inside the gateway container and
# print the triage report to the terminal. No LINE credentials are needed.
set -euo pipefail

cd "$(dirname "$0")"
INCIDENTS=.state/incidents
SINCE_FILE=.state/.trigger-marker
TIMEOUT_S=${TIMEOUT_S:-240}

echo "Waiting up to ${TIMEOUT_S}s for the ingestion worker to dispatch an incident..."
deadline=$(( $(date +%s) + TIMEOUT_S ))
incident=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  incident=$(find "$INCIDENTS" -name '*.txt' -newer "$SINCE_FILE" 2>/dev/null | head -n 1 || true)
  [ -n "$incident" ] && break
  sleep 3
done
if [ -z "$incident" ]; then
  echo "No incident file appeared. Check: docker compose logs ingestion-worker airflow" >&2
  exit 1
fi

name=$(basename "$incident")
echo "Incident message: $incident"
echo "------------------------------------------------------------------"
cat "$incident"
echo "------------------------------------------------------------------"
echo "Running the investigator (fresh session)..."
docker compose exec -T openclaw-gateway openclaw agent \
  --agent pipeline-investigator \
  --session-key "dry-run-$(date +%s)" \
  --message-file "/incidents/$name"
