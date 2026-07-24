#!/usr/bin/env bash
# Apply the ClickHouse schema.
#
# Usage:
#   ./scripts/clickhouse/apply.sh <url> [user] [password] [start]
#
# Examples:
#   ./scripts/clickhouse/apply.sh http://localhost:8123
#   ./scripts/clickhouse/apply.sh https://host.clickhouse.cloud:8443 default password
#   ./scripts/clickhouse/apply.sh https://host.clickhouse.cloud:8443 default password 006
set -euo pipefail

URL="${1:?usage: apply.sh <url> [user] [password] [start]}"
USER="${2:-}"
PASSWORD="${3:-}"
START="${4:-001}"

if [[ ! "$START" =~ ^[0-9]{3}$ ]]; then
  echo "start migration must be a three-digit prefix such as 006" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

AUTH_HEADERS=()
if [[ -n "$USER" ]]; then
  AUTH_HEADERS+=(-H "X-ClickHouse-User: $USER")
fi
if [[ -n "$PASSWORD" ]]; then
  AUTH_HEADERS+=(-H "X-ClickHouse-Key: $PASSWORD")
fi

for f in "$SCRIPT_DIR"/[0-9]*.sql; do
  MIGRATION="$(basename "$f")"
  if [[ "${MIGRATION%%_*}" < "$START" ]]; then
    continue
  fi
  echo "Applying $MIGRATION..."
  STATEMENT=0
  QUERY=""
  while IFS= read -r LINE || [[ -n "$LINE" ]]; do
    QUERY+="$LINE"$'\n'
    if [[ "$LINE" =~ \;[[:space:]]*$ ]]; then
      STATEMENT=$((STATEMENT + 1))
      RESPONSE=$(curl -s -w "\n%{http_code}" "$URL/" --data-binary "$QUERY" "${AUTH_HEADERS[@]}" 2>&1)
      HTTP_CODE=$(echo "$RESPONSE" | tail -1)
      BODY=$(echo "$RESPONSE" | sed '$d')
      if [[ "$HTTP_CODE" != "200" ]]; then
        echo "FAILED (HTTP $HTTP_CODE): $MIGRATION statement $STATEMENT" >&2
        echo "$BODY" >&2
        exit 1
      fi
      QUERY=""
    fi
  done < "$f"
  if [[ -n "${QUERY//[[:space:]]/}" ]]; then
    echo "FAILED: $MIGRATION has an unterminated SQL statement" >&2
    exit 1
  fi
done

echo "Done. Tables:"
echo "SHOW TABLES LIKE 'txgen%'" | curl -sf "$URL/" --data-binary @- "${AUTH_HEADERS[@]}"
