#!/usr/bin/env bash
# Integration test: generate transactions and verify they decode with cast.
set -euo pipefail

FAILED=0
TOTAL=0

run_test() {
  local bin="$1" spec="$2" count="$3" label="$4"
  echo "=== $label: $bin -s $spec -n $count ==="

  local output
  if ! output=$(cargo run --quiet --bin "$bin" -- generate -s "$spec" -n "$count" --seed 42 2>/dev/null); then
    echo "FAIL: $bin generation failed"
    FAILED=$((FAILED + 1))
    TOTAL=$((TOTAL + 1))
    return
  fi

  local ok=0 fail=0
  while IFS= read -r raw; do
    TOTAL=$((TOTAL + 1))
    if cast decode-tx "$raw" >/dev/null 2>&1; then
      ok=$((ok + 1))
    else
      echo "FAIL: cast decode-tx failed for $raw"
      fail=$((fail + 1))
      FAILED=$((FAILED + 1))
    fi
  done < <(echo "$output" | jq -r .raw)

  echo "  $ok OK, $fail FAIL"
}

# Ethereum: all tx types (legacy, eip2930, eip1559)
run_test txgen-ethereum tests/specs/ethereum-all-types.yaml 30 "Ethereum all types"

# Tempo: all tx types (legacy, eip2930, eip1559, tempo, tempo+parallel, tempo+sponsored)
run_test txgen-tempo tests/specs/tempo-all-types.yaml 60 "Tempo all types"

echo ""
echo "=== Results: $((TOTAL - FAILED))/$TOTAL passed ==="

if [ "$FAILED" -gt 0 ]; then
  exit 1
fi
