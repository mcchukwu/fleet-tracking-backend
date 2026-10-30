#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <dump> <database> [--force]" >&2
  exit 1
fi
dump="$1"
target="$2"
force="${3:-}"

if [[ ! "$target" =~ ^[A-Za-z0-9_]+$ ]]; then
  echo "database name must use letters, digits and underscores" >&2
  exit 1
fi

if [[ ! -f "$dump" ]]; then
  echo "dump not found: $dump" >&2
  exit 1
fi

cd "$(dirname "$0")/.."
env_file="${ENV_FILE:-.env}"
set -a
# shellcheck disable=SC1090
source "$env_file"
set +a

compose=(docker compose)
if [[ -n "${COMPOSE_PROJECT:-}" ]]; then
  compose+=(-p "$COMPOSE_PROJECT")
fi

exists="$("${compose[@]}" exec -T postgres psql -U "$DB_USER" -d postgres -tAc \
  "SELECT 1 FROM pg_database WHERE datname = '$target'")"
if [[ "$exists" == "1" ]]; then
  if [[ "$force" != "--force" ]]; then
    echo "database $target exists; pass --force to replace it" >&2
    exit 1
  fi
  "${compose[@]}" exec -T postgres dropdb -U "$DB_USER" --if-exists "$target"
fi

"${compose[@]}" exec -T postgres createdb -U "$DB_USER" "$target"
"${compose[@]}" exec -T postgres pg_restore -U "$DB_USER" -d "$target" --no-owner --no-privileges < "$dump"

vehicles="$("${compose[@]}" exec -T postgres psql -U "$DB_USER" -d "$target" -tAc 'SELECT count(*) FROM vehicles')"
positions="$("${compose[@]}" exec -T postgres psql -U "$DB_USER" -d "$target" -tAc 'SELECT count(*) FROM position_history')"
echo "restored $target: vehicles=$vehicles positions=$positions"
