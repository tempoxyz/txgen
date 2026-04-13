#!/usr/bin/env bash
# setup.sh — Start a fresh tempo node, fund accounts, generate transactions.
#
# Usage:
#   ./scripts/bench/setup.sh [OPTIONS]
#
# Options:
#   --spec <PATH>         Workload spec (default: examples/bench-spec.yaml)
#   --count <N>           Transactions to generate (default: 200000)
#   --genesis <PATH>      Genesis JSON (default: /tmp/txgen-localnet/genesis.json)
#   --tempo-bin <PATH>    Tempo binary (default: ~/.tempo/bin/tempo)
#   --block-time <DUR>    Dev mode block time (default: 500ms)
#   --gas-limit <N>       Builder gas limit (default: 3000000000)
#   --max-tasks <N>       Builder max tasks (default: 32)
#   --txpool-size <N>     Max txpool per sub-pool (default: 500000)
#   --seed <N>            RNG seed for tx generation (default: 99)
#   --datadir <PATH>      Explicit datadir (default: mktemp)
#
# Writes datadir path to /tmp/txgen-bench-datadir for other scripts.
set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────────────
SPEC="examples/bench-spec.yaml"
COUNT=200000
GENESIS="/tmp/txgen-localnet/genesis.json"
TEMPO_BIN="${TEMPO_BIN:-$HOME/.tempo/bin/tempo}"
BLOCK_TIME="500ms"
GAS_LIMIT="3000000000"
MAX_TASKS="32"
TXPOOL_SIZE="500000"
SEED="99"
DATADIR=""
FAUCET_KEY="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
FAUCET_AMOUNT="1000000000000"
FAUCET_ADDRS=("0x20c0000000000000000000000000000000000000" "0x20c0000000000000000000000000000000000001")

# ── Parse args ───────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --spec)         SPEC="$2"; shift 2 ;;
    --count)        COUNT="$2"; shift 2 ;;
    --genesis)      GENESIS="$2"; shift 2 ;;
    --tempo-bin)    TEMPO_BIN="$2"; shift 2 ;;
    --block-time)   BLOCK_TIME="$2"; shift 2 ;;
    --gas-limit)    GAS_LIMIT="$2"; shift 2 ;;
    --max-tasks)    MAX_TASKS="$2"; shift 2 ;;
    --txpool-size)  TXPOOL_SIZE="$2"; shift 2 ;;
    --seed)         SEED="$2"; shift 2 ;;
    --datadir)      DATADIR="$2"; shift 2 ;;
    -h|--help)      head -18 "$0" | tail -17; exit 0 ;;
    *)              echo "error: unknown option: $1" >&2; exit 1 ;;
  esac
done

# ── Validate ─────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

command -v "$TEMPO_BIN" >/dev/null 2>&1 || { echo "error: tempo not found: $TEMPO_BIN" >&2; exit 1; }
[[ -f "$GENESIS" ]] || { echo "error: genesis not found: $GENESIS" >&2; exit 1; }
[[ -f "$REPO_ROOT/$SPEC" ]] || [[ -f "$SPEC" ]] || { echo "error: spec not found: $SPEC" >&2; exit 1; }

BENCH_BIN="$REPO_ROOT/target/release/bench"
TXGEN_BIN="$REPO_ROOT/target/release/txgen-tempo"
[[ -x "$BENCH_BIN" ]] || { echo "error: bench not built (run: cargo build --release -p bench-cli)" >&2; exit 1; }
[[ -x "$TXGEN_BIN" ]] || { echo "error: txgen-tempo not built (run: cargo build --release -p txgen-tempo)" >&2; exit 1; }

# ── Kill old tempo ───────────────────────────────────────────────────
if pgrep -f "tempo node" >/dev/null 2>&1; then
  echo "Killing existing tempo..."
  pkill -f "tempo node" || true
  sleep 2
fi

# ── Create datadir ───────────────────────────────────────────────────
if [[ -z "$DATADIR" ]]; then
  DATADIR=$(mktemp -d "/tmp/txgen-bench.XXXXXX")
fi
mkdir -p "$DATADIR"
echo "$DATADIR" > /tmp/txgen-bench-datadir
echo "Datadir: $DATADIR"

# ── Patch genesis timestamp ──────────────────────────────────────────
python3 -c "
import json, time
g = json.load(open('$GENESIS'))
g['timestamp'] = hex(int(time.time()))
json.dump(g, open('$GENESIS', 'w'))
"
echo "Genesis timestamp patched"

# ── Start tempo ──────────────────────────────────────────────────────
FAUCET_FLAGS=()
FAUCET_FLAGS+=(--faucet.enabled)
FAUCET_FLAGS+=(--faucet.private-key "$FAUCET_KEY")
FAUCET_FLAGS+=(--faucet.amount "$FAUCET_AMOUNT")
for addr in "${FAUCET_ADDRS[@]}"; do
  FAUCET_FLAGS+=(--faucet.address "$addr")
done

echo "Starting tempo (block-time=$BLOCK_TIME, gas-limit=$GAS_LIMIT, max-tasks=$MAX_TASKS, txpool=$TXPOOL_SIZE)..."
"$TEMPO_BIN" node \
  --chain "$GENESIS" \
  --datadir "$DATADIR" \
  --dev \
  --dev.block-time "$BLOCK_TIME" \
  --builder.gaslimit "$GAS_LIMIT" \
  --builder.max-tasks "$MAX_TASKS" \
  --http \
  --http.api all \
  --rpc.max-connections 10000 \
  --metrics 127.0.0.1:9001 \
  --txpool.pending-max-count "$TXPOOL_SIZE" \
  --txpool.basefee-max-count "$TXPOOL_SIZE" \
  --txpool.queued-max-count "$TXPOOL_SIZE" \
  "${FAUCET_FLAGS[@]}" \
  --log.stdout.filter error \
  >"$DATADIR/tempo.log" 2>&1 &
TEMPO_PID=$!
echo "$TEMPO_PID" > "$DATADIR/tempo.pid"
echo "Tempo PID=$TEMPO_PID"

# Wait for RPC
for i in $(seq 1 60); do
  if curl -sf -X POST -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","method":"net_version","params":[],"id":1}' \
    http://localhost:8545 >/dev/null 2>&1; then
    echo "RPC ready after ${i}s"
    break
  fi
  [[ $i -eq 60 ]] && { echo "error: RPC not ready after 60s" >&2; exit 1; }
  sleep 1
done

# ── Fund accounts ────────────────────────────────────────────────────
cd "$REPO_ROOT"
ADDRESSES=$("$TXGEN_BIN" addresses -s "$SPEC" -f shell)
ADDR_COUNT=$(echo "$ADDRESSES" | wc -w)
echo "Funding $ADDR_COUNT accounts..."
echo "$ADDRESSES" | tr ' ' '\n' | xargs -P 50 -I{} \
  curl -sf -X POST -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"tempo_fundAddress","params":["{}"],"id":1}' \
  http://localhost:8545 -o /dev/null
echo "Funded. Waiting for txpool to drain..."
ZERO_COUNT=0
for i in $(seq 1 120); do
  pending=$(curl -sf -X POST -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","method":"txpool_status","params":[],"id":1}' \
    http://localhost:8545 2>/dev/null | python3 -c "import sys,json; print(int(json.load(sys.stdin)['result']['pending'],16))" 2>/dev/null || echo "?")
  if [[ "$pending" == "0" ]]; then
    ZERO_COUNT=$((ZERO_COUNT + 1))
    if [[ $ZERO_COUNT -ge 3 ]]; then
      echo "Txpool drained after ${i}s"
      break
    fi
  else
    ZERO_COUNT=0
  fi
  [[ $((i % 10)) -eq 0 ]] && echo "  pending: $pending"
  sleep 1
done

# ── Generate transactions ───────────────────────────────────────────
echo "Generating $COUNT transactions (seed=$SEED)..."
"$TXGEN_BIN" generate \
  -s "$SPEC" \
  -n "$COUNT" \
  --seed "$SEED" \
  --rpc http://localhost:8545 \
  -o "$DATADIR/txs.ndjson" 2>&1 | tail -1

echo ""
echo "=== Setup complete ==="
echo "  Datadir:  $DATADIR"
echo "  Tempo:    PID $TEMPO_PID"
echo "  Accounts: $ADDR_COUNT funded"
echo "  Txs:      $COUNT generated at $DATADIR/txs.ndjson"
echo "  Size:     $(du -h "$DATADIR/txs.ndjson" | cut -f1)"
