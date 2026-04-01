#!/usr/bin/env bash
# Integration test: generate transactions and verify they decode with cast.
#
# Usage: ./tests/decode-tx.sh <binary> <spec> [count]
set -euo pipefail

BIN="${1:?usage: $0 <binary> <spec> [count]}"
SPEC="${2:?usage: $0 <binary> <spec> [count]}"
COUNT="${3:-50}"

echo "=== $BIN -s $SPEC -n $COUNT ==="

OUTPUT=$(cargo run --quiet --bin "$BIN" -- generate -s "$SPEC" -n "$COUNT" --seed 42 2>/dev/null)

TOTAL=0
FAILED=0

while IFS= read -r raw; do
  TOTAL=$((TOTAL + 1))
  if ! cast decode-tx "$raw" >/dev/null 2>&1; then
    echo "FAIL: $raw"
    FAILED=$((FAILED + 1))
  fi
done < <(echo "$OUTPUT" | jq -r .raw)

echo "$((TOTAL - FAILED))/$TOTAL passed"

if [ "$FAILED" -gt 0 ]; then
  exit 1
fi
