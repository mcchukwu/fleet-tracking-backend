#!/usr/bin/env bash
set -euo pipefail

env_file="${ENV_FILE:-.env}"
if [[ ! -f "$env_file" ]]; then
  echo "missing $env_file; copy .env.example and set local credentials first" >&2
  exit 1
fi
set -a
source "$env_file"
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

sim_log="$(mktemp /tmp/fleet-tracking-sim.XXXXXX.log)"
go run ./cmd/simulator -ws-url ws://localhost:8080/ws/ingest -count 3 -interval 1s >"$sim_log" 2>&1 &
sim_pid=$!

cleanup() {
  kill "$sim_pid" "$api_pid" "$server_pid" 2>/dev/null || true
  wait "$sim_pid" 2>/dev/null || true
  wait "$api_pid" 2>/dev/null || true
  wait "$server_pid" 2>/dev/null || true
  rm -f "$server_log" "$api_log" "$sim_log"
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

for _ in {1..30}; do
  if curl --fail --silent http://localhost:9091/readyz >/dev/null; then
    break
  fi
  sleep 1
done

if ! curl --fail --silent http://localhost:9091/readyz >/dev/null; then
  cat "$api_log" >&2
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

jar="$(mktemp /tmp/fleet-tracking-api.XXXXXX.jar)"
login="$(curl --silent --cookie-jar "$jar" -X POST http://localhost:9091/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"dispatcher@demo.ftb","password":"DemoPass123!"}')"
csrf="$(printf '%s' "$login" | python3 -c 'import json,sys; print(json.load(sys.stdin)["csrf_token"])')"
vehicles="$(curl --silent --cookie "$jar" 'http://localhost:9091/api/v1/vehicles?limit=500' | \
  python3 -c 'import json,sys; print(len(json.load(sys.stdin)["data"]))')"
if [[ "$vehicles" -lt 20 ]]; then
  echo "api check failed: vehicles=$vehicles" >&2
  exit 1
fi

csv="$(mktemp /tmp/fleet-tracking-import.XXXXXX.csv)"
printf 'imei,plate,vehicle_type,driver_name,driver_phone\n' > "$csv"
printf 'integration-imei-1,INT-001-AA,truck,Integration Driver,08130000001\n' >> "$csv"
printf 'integration-imei-2,INT-002-AA,truck,Integration Driver,08130000001\n' >> "$csv"
imported="$(curl --silent --cookie "$jar" -X POST http://localhost:9091/api/v1/import/vehicles \
  -H "X-CSRF-Token: $csrf" -F "file=@$csv" | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["imported"])')"
rm -f "$csv"
if [[ "$imported" -ne 2 ]]; then
  echo "api import check failed: imported=$imported" >&2
  exit 1
fi

vehicle_id="$(curl --silent --cookie "$jar" 'http://localhost:9091/api/v1/vehicles?limit=1' | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["data"][0]["id"])')"
driver_id="$(curl --silent --cookie "$jar" -X POST http://localhost:9091/api/v1/drivers \
  -H "X-CSRF-Token: $csrf" -H 'Content-Type: application/json' \
  -d '{"name":"Integration Driver","phone":"08130000002"}' | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
driver_patch="$(curl --silent -o /dev/null -w '%{http_code}' --cookie "$jar" -X PATCH \
  "http://localhost:9091/api/v1/drivers/$driver_id" -H "X-CSRF-Token: $csrf" \
  -H 'Content-Type: application/json' -d '{"licence":"INT-0001"}')"
vehicle_patch="$(curl --silent -o /dev/null -w '%{http_code}' --cookie "$jar" -X PATCH \
  "http://localhost:9091/api/v1/vehicles/$vehicle_id" -H "X-CSRF-Token: $csrf" \
  -H 'Content-Type: application/json' -d '{"depot":"Integration Depot"}')"
if [[ "$driver_patch" != "204" || "$vehicle_patch" != "204" ]]; then
  echo "update check failed: driver=$driver_patch vehicle=$vehicle_patch" >&2
  exit 1
fi

sleep 2
ws="$(go run ./cmd/wsprobe -api-url http://localhost:9091 2>&1)"
if [[ "$ws" != ok* ]]; then
  echo "websocket check failed: $ws" >&2
  exit 1
fi

echo "PASS. integration: positions=$positions events=$events api_vehicles=$vehicles imported=$imported updates=ok ws=$ws"
