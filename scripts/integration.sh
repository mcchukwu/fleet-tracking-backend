#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f .env ]]; then
  echo "missing .env; copy .env.example and set local credentials first" >&2
  exit 1
fi
if ! command -v migrate >/dev/null; then
  echo "missing migrate CLI" >&2
  exit 1
fi

set -a
source .env
set +a

docker compose up -d --wait
migrate -database "$PG_DSN" -path migrations up

TEST_REDIS_ADDR="$REDIS_ADDR" go test -run '^TestApplyPositionAtomicallyTracksTransitions$' ./internal/pipeline

vehicle_id="route-test-$(date +%s)"
server_log="$(mktemp /tmp/fleet-tracking-ingest.XXXXXX.log)"
go run ./cmd/ingest >"$server_log" 2>&1 &
server_pid=$!

cleanup() {
  kill "$server_pid" 2>/dev/null || true
  wait "$server_pid" 2>/dev/null || true
  rm -f "$server_log"
}
trap cleanup EXIT

for _ in {1..30}; do
  if curl --fail --silent http://localhost:9090/healthz >/dev/null; then
    break
  fi
  sleep 1
done

if ! curl --fail --silent http://localhost:9090/healthz >/dev/null; then
  cat "$server_log" >&2
  exit 1
fi

go run ./cmd/routetest -vehicle "$vehicle_id"
sleep 2

positions=$(docker compose exec -T postgres psql -U "$DB_USER" -d "$DB_NAME" -tAc \
  "SELECT count(*) FROM position_history h JOIN vehicles v ON v.id = h.vehicle_id WHERE v.external_id = '$vehicle_id'")
events=$(docker compose exec -T postgres psql -U "$DB_USER" -d "$DB_NAME" -tAc \
  "SELECT count(*) FROM geofence_events e JOIN vehicles v ON v.id = e.vehicle_id WHERE v.external_id = '$vehicle_id'")

if [[ "$positions" -lt 1 || "$events" -ne 2 ]]; then
  echo "persistence check failed: positions=$positions events=$events" >&2
  exit 1
fi

echo "PASS — integration: positions=$positions events=$events"
