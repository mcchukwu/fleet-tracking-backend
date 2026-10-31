#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <git-ref>" >&2
  exit 1
fi
ref="$1"

cd "$(dirname "$0")/.."
set -a
# shellcheck disable=SC1090
source .env
set +a

project="${COMPOSE_PROJECT:-ftb-demo}"
export COMPOSE_PROJECT="$project"
export COMPOSE_PARALLEL_LIMIT=1
previous="$(git rev-parse HEAD)"

echo "backing up before $ref"
./scripts/backup.sh || echo "backup skipped (database may not exist yet)"

git fetch --all --tags
git checkout --force "$ref"

echo "deploying $ref"
if ! docker compose -p "$project" --profile app --profile demo up -d --build; then
  echo "deploy failed; rolling back to $previous" >&2
  git checkout --force "$previous"
  docker compose -p "$project" --profile app --profile demo up -d --build || true
  exit 1
fi

sleep 10
curl_args=(-fsS)
if [[ "${CURL_INSECURE:-0}" == "1" ]]; then
  curl_args+=(-k)
fi
if ! curl "${curl_args[@]}" "https://${DOMAIN}/readyz" >/dev/null; then
  echo "health check failed; rolling back to $previous" >&2
  git checkout --force "$previous"
  docker compose -p "$project" --profile app --profile demo up -d --build || true
  exit 1
fi

echo "deployed $(git rev-parse --short HEAD)"
