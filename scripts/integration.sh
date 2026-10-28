#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f .env ]]; then
  echo "missing .env; copy .env.example and set local credentials first" >&2
  exit 1
fi
set -a
source .env
set +a

if [[ "${SKIP_COMPOSE:-0}" != "1" ]]; then
  docker compose up -d --wait
fi
go run ./cmd/migrate
if command -v psql >/dev/null; then
  psql "$PG_DSN" -v ON_ERROR_STOP=1 -f scripts/seed_dev.sql >/dev/null
else
  docker compose exec -T postgres psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 < scripts/seed_dev.sql
fi
go run ./cmd/seed

TEST_REDIS_ADDR="$REDIS_ADDR" TEST_REDIS_PASSWORD="$REDIS_PASSWORD" go test -run '^TestApplyPositionAtomicallyTracksTransitions$' ./internal/pipeline

vehicle_id="route-test-$(date +%s)"
server_log="$(mktemp /tmp/fleet-tracking-ingest.XXXXXX.log)"
go run ./cmd/ingest >"$server_log" 2>&1 &
server_pid=$!

api_log="$(mktemp /tmp/fleet-tracking-api.XXXXXX.log)"
API_ADDR=127.0.0.1:9091 COOKIE_SECURE=false go run ./cmd/api >"$api_log" 2>&1 &
api_pid=$!

cleanup() {
  kill "$api_pid" "$server_pid" 2>/dev/null || true
  wait "$api_pid" 2>/dev/null || true
  wait "$server_pid" 2>/dev/null || true
  rm -f "$server_log" "$api_log"
}
trap cleanup EXIT

for _ in {1..30}; do
  if curl --fail --silent http://localhost:9090/readyz >/dev/null; then
    break
  fi
  sleep 1
done

if ! curl --fail --silent http://localhost:9090/readyz >/dev/null; then
  cat "$server_log" >&2
  exit 1
fi

go run ./cmd/routetest -vehicle "$vehicle_id"
sleep 2

if command -v psql >/dev/null; then
  positions=$(psql "$PG_DSN" -tAc "SELECT count(*) FROM position_history h JOIN vehicles v ON v.id = h.vehicle_id WHERE v.external_id = '$vehicle_id'")
  events=$(psql "$PG_DSN" -tAc "SELECT count(*) FROM geofence_events e JOIN vehicles v ON v.id = e.vehicle_id WHERE v.external_id = '$vehicle_id'")
else
  positions=$(docker compose exec -T postgres psql -U "$DB_USER" -d "$DB_NAME" -tAc \
    "SELECT count(*) FROM position_history h JOIN vehicles v ON v.id = h.vehicle_id WHERE v.external_id = '$vehicle_id'")
  events=$(docker compose exec -T postgres psql -U "$DB_USER" -d "$DB_NAME" -tAc \
    "SELECT count(*) FROM geofence_events e JOIN vehicles v ON v.id = e.vehicle_id WHERE v.external_id = '$vehicle_id'")
fi

if [[ "$positions" -lt 1 || "$events" -ne 2 ]]; then
  echo "persistence check failed: positions=$positions events=$events" >&2
  exit 1
fi

echo "PASS — integration: positions=$positions events=$events"
