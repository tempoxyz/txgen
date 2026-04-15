#!/usr/bin/env bash
# Apply the ClickHouse schema.
#
# Usage:
#   ./scripts/clickhouse/apply.sh <url> [user] [password]
#
# Examples:
#   ./scripts/clickhouse/apply.sh http://localhost:8123
#   ./scripts/clickhouse/apply.sh https://host.clickhouse.cloud:8443 default password
set -euo pipefail

URL="${1:?usage: apply.sh <url> [user] [password]}"
USER="${2:-}"
PASSWORD="${3:-}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

AUTH_HEADERS=()
if [[ -n "$USER" ]]; then
  AUTH_HEADERS+=(-H "X-ClickHouse-User: $USER")
fi
if [[ -n "$PASSWORD" ]]; then
  AUTH_HEADERS+=(-H "X-ClickHouse-Key: $PASSWORD")
fi

for f in "$SCRIPT_DIR"/[0-9]*.sql; do
  echo "Applying $(basename "$f")..."
  RESPONSE=$(curl -s -w "\n%{http_code}" "$URL/" --data-binary @"$f" "${AUTH_HEADERS[@]}" 2>&1)
  HTTP_CODE=$(echo "$RESPONSE" | tail -1)
  BODY=$(echo "$RESPONSE" | sed '$d')
  if [[ "$HTTP_CODE" != "200" ]]; then
    echo "FAILED (HTTP $HTTP_CODE): $(basename "$f")" >&2
    echo "$BODY" >&2
    exit 1
  fi
done

echo "Done. Tables:"
echo "SHOW TABLES LIKE 'txgen%'" | curl -sf "$URL/" --data-binary @- "${AUTH_HEADERS[@]}"
