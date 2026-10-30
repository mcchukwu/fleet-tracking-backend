#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
env_file="${ENV_FILE:-.env}"
if [[ ! -f "$env_file" ]]; then
  echo "missing $env_file" >&2
  exit 1
fi
set -a
# shellcheck disable=SC1090
source "$env_file"
set +a

compose=(docker compose)
if [[ -n "${COMPOSE_PROJECT:-}" ]]; then
  compose+=(-p "$COMPOSE_PROJECT")
fi

dir="${BACKUP_DIR:-./backups}"
retention="${BACKUP_RETENTION_DAYS:-14}"
mkdir -p "$dir"

file="$dir/${DB_NAME}-$(date -u +%Y%m%dT%H%M%SZ).dump"
"${compose[@]}" exec -T postgres pg_dump -U "$DB_USER" -d "$DB_NAME" -Fc > "$file"
echo "wrote $file"

find "$dir" -name '*.dump' -type f -mtime +"$retention" -delete
