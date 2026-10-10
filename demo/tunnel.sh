#!/usr/bin/env bash
# Phase 3: expose the worker's LINE webhook listener through Cloudflare and
# register https://<host>/line/webhook as the LINE channel's webhook URL.
#   - CLOUDFLARE_TUNNEL_TOKEN set: named tunnel; PUBLIC_WEBHOOK_BASE_URL is its hostname.
#   - otherwise: quick tunnel with a random *.trycloudflare.com URL (changes on restart).
set -euo pipefail
cd "$(dirname "$0")"
compose() { docker compose --env-file .env -f docker-compose.yml "$@"; }
set -a; source .env; set +a

if [ -n "${CLOUDFLARE_TUNNEL_TOKEN:-}" ]; then
  : "${PUBLIC_WEBHOOK_BASE_URL:?set PUBLIC_WEBHOOK_BASE_URL (e.g. https://openclaw-feedback.example.com) in demo/.env}"
  compose --profile tunnel up -d tunnel
  base="$PUBLIC_WEBHOOK_BASE_URL"
else
  compose --profile tunnel-quick up -d tunnel-quick
  echo "Waiting for the quick tunnel URL..."
  base=""
  for _ in $(seq 1 30); do
    base="$(compose logs --no-log-prefix tunnel-quick 2>&1 | grep -oE 'https://[a-z0-9-]+\.trycloudflare\.com' | tail -1 || true)"
    [ -n "$base" ] && break
    sleep 2
  done
  [ -n "$base" ] || { echo "No trycloudflare.com URL in the tunnel logs" >&2; exit 1; }
fi

echo "Tunnel: $base"
python3 set_line_webhook.py "${base%/}/line/webhook"
