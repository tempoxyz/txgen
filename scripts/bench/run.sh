#!/usr/bin/env bash
# run.sh — End-to-end benchmark: setup node → run → plot.
#
# Usage:
#   ./scripts/bench/run.sh [OPTIONS]
#
# Modes:
#   send (default)   Generate transactions and send via RPC
#   replay           Extract blocks from an archive node and replay via Engine API
#
# Common options:
#   --mode <MODE>          Benchmark mode: send or replay (default: send)
#   --metrics-url <URL>    Prometheus metrics endpoint (default: http://127.0.0.1:9001/metrics)
#   --scrape-interval <N>  Metrics scrape interval in ms (default: 500)
#   --datadir <PATH>       Explicit datadir (default: mktemp)
#   --metadata <K=V>       Extra metadata key=value (repeatable)
#   --report <SPEC>        Additional report destination (repeatable)
#   --no-setup             Skip node start + account funding
#   --no-plot              Skip plot generation
#
# Send mode options:
#   --spec <PATH>          Workload spec (default: examples/bench-spec.yaml)
#   --count <N>            Transactions to generate (default: 200000)
#   --seed <N>             RNG seed for tx generation (default: 99)
#   --tps <N>              Target TPS (default: 5000)
#   --max-concurrent <N>   Max concurrent HTTP requests (default: 500)
#   --rpc-url <URL>        RPC endpoint (default: http://localhost:8545)
#   --drain-timeout <N>    Seconds to wait for pool drain (default: 300)
#
# Replay mode options:
#   --rpc-source <URL>     Archive node RPC for block extraction (required)
#   --engine <URL>         Engine API endpoint (default: http://localhost:8551)
#   --jwt-secret <PATH>    Path to JWT secret file (required)
#   --from <N>             Starting block number (required)
#   --to <N>               Ending block number (required)
#   --wait-for-persistence <POLICY>  always, never, or every:N (default: every:2)
#
# Node options (used with setup):
#   --genesis <PATH>       Genesis JSON (default: /tmp/txgen-localnet/genesis.json)
#   --tempo-bin <PATH>     Tempo binary (default: ~/.tempo/bin/tempo)
#   --block-time <DUR>     Dev mode block time (default: 500ms)
#   --gas-limit <N>        Builder gas limit (default: 3000000000)
#   --max-tasks <N>        Builder max tasks (default: 32)
#   --txpool-count <N>     Max txpool txs per sub-pool (default: 500000)
#   --txpool-size <N>      Max txpool size in MB per sub-pool (default: 20)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/lib.sh"

# ── Defaults ─────────────────────────────────────────────────────────

# Common
MODE="send"
METRICS_URL="http://127.0.0.1:9001/metrics"
SCRAPE_INTERVAL=500
DATADIR=""
EXTRA_METADATA=()
EXTRA_REPORTS=()
DO_SETUP=true
DO_PLOT=true

# Send mode
SPEC="examples/bench-spec.yaml"
COUNT=200000
SEED=99
TPS=5000
MAX_CONCURRENT=500
RPC="http://localhost:8545"
DRAIN_TIMEOUT=300

# Replay mode
RPC_SOURCE=""
ENGINE="http://localhost:8551"
JWT_SECRET=""
FROM_BLOCK=""
TO_BLOCK=""
WAIT_FOR_PERSISTENCE="every:2"

# Node
GENESIS="/tmp/txgen-localnet/genesis.json"
TEMPO_BIN="${TEMPO_BIN:-$HOME/.tempo/bin/tempo}"
BLOCK_TIME="500ms"
GAS_LIMIT="3000000000"
MAX_TASKS="32"
TXPOOL_COUNT="500000"
TXPOOL_SIZE="20"

# ── Parse args ───────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    # Common
    --mode)             MODE="$2"; shift 2 ;;
    --metrics-url)      METRICS_URL="$2"; shift 2 ;;
    --scrape-interval)  SCRAPE_INTERVAL="$2"; shift 2 ;;
    --datadir)          DATADIR="$2"; shift 2 ;;
    --metadata)         EXTRA_METADATA+=("$2"); shift 2 ;;
    --report)           EXTRA_REPORTS+=("$2"); shift 2 ;;
    --no-setup)         DO_SETUP=false; shift ;;
    --no-plot)          DO_PLOT=false; shift ;;
    # Send mode
    --spec)             SPEC="$2"; shift 2 ;;
    --count)            COUNT="$2"; shift 2 ;;
    --seed)             SEED="$2"; shift 2 ;;
    --tps)              TPS="$2"; shift 2 ;;
    --max-concurrent)   MAX_CONCURRENT="$2"; shift 2 ;;
    --rpc-url)          RPC="$2"; shift 2 ;;
    --drain-timeout)    DRAIN_TIMEOUT="$2"; shift 2 ;;
    # Replay mode
    --rpc-source)       RPC_SOURCE="$2"; shift 2 ;;
    --engine)           ENGINE="$2"; shift 2 ;;
    --jwt-secret)       JWT_SECRET="$2"; shift 2 ;;
    --from)             FROM_BLOCK="$2"; shift 2 ;;
    --to)               TO_BLOCK="$2"; shift 2 ;;
    --wait-for-persistence) WAIT_FOR_PERSISTENCE="$2"; shift 2 ;;
    # Node
    --genesis)          GENESIS="$2"; shift 2 ;;
    --tempo-bin)        TEMPO_BIN="$2"; shift 2 ;;
    --block-time)       BLOCK_TIME="$2"; shift 2 ;;
    --gas-limit)        GAS_LIMIT="$2"; shift 2 ;;
    --max-tasks)        MAX_TASKS="$2"; shift 2 ;;
    --txpool-count)     TXPOOL_COUNT="$2"; shift 2 ;;
    --txpool-size)      TXPOOL_SIZE="$2"; shift 2 ;;
    -h|--help)          head -45 "$0" | tail -44; exit 0 ;;
    *)                  echo "error: unknown option: $1" >&2; exit 1 ;;
  esac
done

# ── Validate ─────────────────────────────────────────────────────────

case "$MODE" in
  send) ;;
  replay)
    [[ -n "$RPC_SOURCE" ]]  || { echo "error: --rpc-source is required for replay mode" >&2; exit 1; }
    [[ -n "$JWT_SECRET" ]]  || { echo "error: --jwt-secret is required for replay mode" >&2; exit 1; }
    [[ -n "$FROM_BLOCK" ]]  || { echo "error: --from is required for replay mode" >&2; exit 1; }
    [[ -n "$TO_BLOCK" ]]    || { echo "error: --to is required for replay mode" >&2; exit 1; }
    [[ -f "$JWT_SECRET" ]]  || { echo "error: JWT secret not found: $JWT_SECRET" >&2; exit 1; }
    ;;
  *) echo "error: unknown mode: $MODE (expected: send, replay)" >&2; exit 1 ;;
esac

BENCH_BIN="$REPO_ROOT/target/release/bench"
TXGEN_BIN="$REPO_ROOT/target/release/txgen-tempo"
[[ -x "$BENCH_BIN" ]] || { echo "error: bench not built (run: cargo build --release -p bench-cli)" >&2; exit 1; }
[[ -x "$TXGEN_BIN" ]] || { echo "error: txgen-tempo not built (run: cargo build --release -p txgen-tempo)" >&2; exit 1; }

# ── Datadir ──────────────────────────────────────────────────────────

if [[ -z "$DATADIR" ]]; then
  DATADIR=$(mktemp -d "/tmp/txgen-bench.XXXXXX")
fi
mkdir -p "$DATADIR"
echo "$DATADIR" > /tmp/txgen-bench-datadir

# ── Setup ────────────────────────────────────────────────────────────

if [[ "$DO_SETUP" == true ]]; then
  echo "╔══════════════════════════════════════╗"
  echo "║           SETUP                      ║"
  echo "╚══════════════════════════════════════╝"

  [[ -f "$GENESIS" ]] || { echo "error: genesis not found: $GENESIS" >&2; exit 1; }
  command -v "$TEMPO_BIN" >/dev/null 2>&1 || { echo "error: tempo not found: $TEMPO_BIN" >&2; exit 1; }

  start_tempo "$GENESIS" "$DATADIR" "$BLOCK_TIME" "$GAS_LIMIT" \
    "$MAX_TASKS" "$TXPOOL_COUNT" "$TXPOOL_SIZE" "$TEMPO_BIN"
  wait_for_rpc "$RPC"

  if [[ "$MODE" == "send" ]]; then
    cd "$REPO_ROOT"
    fund_accounts "$TXGEN_BIN" "$SPEC" "$RPC"
  fi

  echo ""
fi

# ── Metadata + report flags ──────────────────────────────────────────

METADATA_FLAGS=(-m "mode=$MODE" -m "block_time=$BLOCK_TIME")

if [[ "$MODE" == "send" ]]; then
  METADATA_FLAGS+=(-m "tps=$TPS" -m "max_concurrent=$MAX_CONCURRENT")
  METADATA_FLAGS+=(-m "scrape_interval_ms=$SCRAPE_INTERVAL")
else
  METADATA_FLAGS+=(-m "from=$FROM_BLOCK" -m "to=$TO_BLOCK" -m "wait_for_persistence=$WAIT_FOR_PERSISTENCE")
fi

for kv in "${EXTRA_METADATA[@]+"${EXTRA_METADATA[@]}"}"; do
  METADATA_FLAGS+=(-m "$kv")
done

REPORT_FLAGS=(
  --report console
  --report "json:$DATADIR/report.json"
)
for spec in "${EXTRA_REPORTS[@]+"${EXTRA_REPORTS[@]}"}"; do
  REPORT_FLAGS+=(--report "$spec")
done

# ── Bench ────────────────────────────────────────────────────────────

echo "╔══════════════════════════════════════╗"
echo "║           BENCH ($MODE)              ║"
echo "╚══════════════════════════════════════╝"

START_TIME=$(date +%s)

if [[ "$MODE" == "send" ]]; then
  cd "$REPO_ROOT"
  echo "=== Generate $COUNT txs | bench send (tps=$TPS, max_concurrent=$MAX_CONCURRENT) ==="
  echo "  RPC:         $RPC"
  echo "  Metrics:     $METRICS_URL (interval=${SCRAPE_INTERVAL}ms)"
  echo ""

  "$TXGEN_BIN" generate \
    -s "$SPEC" \
    -n "$COUNT" \
    --seed "$SEED" \
    --rpc "$RPC" \
  | "$BENCH_BIN" send \
      --rpc-url "$RPC" \
      --tps "$TPS" \
      --max-concurrent "$MAX_CONCURRENT" \
      --metrics-url "$METRICS_URL" \
      --scrape-interval-ms "$SCRAPE_INTERVAL" \
      --drain-timeout "$DRAIN_TIMEOUT" \
      "${METADATA_FLAGS[@]}" \
      "${REPORT_FLAGS[@]}" 2>&1

else
  BLOCK_COUNT=$((TO_BLOCK - FROM_BLOCK + 1))
  echo "=== Extract $BLOCK_COUNT blocks | bench send-blocks ==="
  echo "  Source RPC:  $RPC_SOURCE"
  echo "  Engine:      $ENGINE"
  echo "  Range:       $FROM_BLOCK - $TO_BLOCK"
  echo "  Persistence: $WAIT_FOR_PERSISTENCE"
  echo "  Metrics:     $METRICS_URL (interval=${SCRAPE_INTERVAL}ms)"
  echo ""

  "$TXGEN_BIN" extract \
    --rpc "$RPC_SOURCE" \
    --from "$FROM_BLOCK" \
    --to "$TO_BLOCK" \
  | "$BENCH_BIN" send-blocks \
      --engine "$ENGINE" \
      --jwt-secret "$JWT_SECRET" \
      --wait-for-persistence "$WAIT_FOR_PERSISTENCE" \
      --metrics-url "$METRICS_URL" \
      --scrape-interval-ms "$SCRAPE_INTERVAL" \
      "${METADATA_FLAGS[@]}" \
      "${REPORT_FLAGS[@]}" 2>&1
fi

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo ""
echo "=== Bench complete in ${ELAPSED}s ==="

# ── Teardown ─────────────────────────────────────────────────────────

stop_tempo "$DATADIR"

echo ""
echo "=== Run complete ==="
echo "  Total time:   ${ELAPSED}s"
echo "  Report:       $DATADIR/report.json"

print_summary "$DATADIR/report.json"

# ── Plot ─────────────────────────────────────────────────────────────

if [[ "$DO_PLOT" == true ]]; then
  echo ""
  echo "╔══════════════════════════════════════╗"
  echo "║           PLOT                       ║"
  echo "╚══════════════════════════════════════╝"
  uv run --with matplotlib python3 "$SCRIPT_DIR/plot.py" "$DATADIR"
fi
