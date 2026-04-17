#!/usr/bin/env bash
# run.sh — Run bench with built-in metrics scraping.
#
# Usage:
#   ./scripts/bench/run.sh [OPTIONS]
#
# Options:
#   --tps <N>              Target TPS (default: 5000)
#   --max-concurrent <N>   Max concurrent HTTP requests (default: 500)
#   --rpc-url <URL>        RPC endpoint (default: http://localhost:8545)
#   --metrics-url <URL>    Prometheus metrics endpoint (default: http://127.0.0.1:9001/metrics)
#   --scrape-interval <N>  Metrics scrape interval in ms (default: 500)
#   --datadir <PATH>       Datadir (default: read from /tmp/txgen-bench-datadir)
#   --input <PATH>         Tx file (default: $DATADIR/txs.ndjson)
#   --metadata <K=V>       Extra metadata key=value (repeatable)
#   --report <SPEC>        Additional report destination (repeatable)
#   --drain-timeout <N>    Seconds to wait for pool drain (default: 300, 0 to disable)
#
# Outputs:
#   $DATADIR/report.json   Bench report (includes scraped metrics as samples)
set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────────────
TPS=5000
MAX_CONCURRENT=500
RPC="http://localhost:8545"
METRICS_URL="http://127.0.0.1:9001/metrics"
SCRAPE_INTERVAL=500
DATADIR=""
INPUT=""
EXTRA_METADATA=()
EXTRA_REPORTS=()
DRAIN_TIMEOUT=300

# ── Parse args ───────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --tps)              TPS="$2"; shift 2 ;;
    --max-concurrent)   MAX_CONCURRENT="$2"; shift 2 ;;
    --rpc-url)          RPC="$2"; shift 2 ;;
    --metrics-url)      METRICS_URL="$2"; shift 2 ;;
    --scrape-interval)  SCRAPE_INTERVAL="$2"; shift 2 ;;
    --datadir)          DATADIR="$2"; shift 2 ;;
    --input)            INPUT="$2"; shift 2 ;;
    --metadata)         EXTRA_METADATA+=("$2"); shift 2 ;;
    --report)           EXTRA_REPORTS+=("$2"); shift 2 ;;
    --drain-timeout)    DRAIN_TIMEOUT="$2"; shift 2 ;;
    -h|--help)          head -18 "$0" | tail -17; exit 0 ;;
    *)                  echo "error: unknown option: $1" >&2; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

if [[ -z "$DATADIR" ]]; then
  DATADIR=$(cat /tmp/txgen-bench-datadir 2>/dev/null) || { echo "error: no datadir (run setup.sh first or pass --datadir)" >&2; exit 1; }
fi
[[ -d "$DATADIR" ]] || { echo "error: datadir not found: $DATADIR" >&2; exit 1; }

if [[ -z "$INPUT" ]]; then
  INPUT="$DATADIR/txs.ndjson"
fi
[[ -f "$INPUT" ]] || { echo "error: input file not found: $INPUT" >&2; exit 1; }

BENCH_BIN="$REPO_ROOT/target/release/bench"
[[ -x "$BENCH_BIN" ]] || { echo "error: bench not built" >&2; exit 1; }

TX_COUNT=$(wc -l < "$INPUT")

echo "=== Bench: ${TX_COUNT} txs, tps=${TPS}, max_concurrent=${MAX_CONCURRENT} ==="
echo "  RPC:         $RPC"
echo "  Metrics:     $METRICS_URL (interval=${SCRAPE_INTERVAL}ms)"
echo "  Input:       $INPUT"
echo "  Datadir:     $DATADIR"
echo ""

# ── Build metadata flags ──────────────────────────────────────────────
METADATA_FLAGS=(
  -m "tps=$TPS"
  -m "max_concurrent=$MAX_CONCURRENT"
  -m "scrape_interval_ms=$SCRAPE_INTERVAL"
)
for kv in "${EXTRA_METADATA[@]}"; do
  METADATA_FLAGS+=(-m "$kv")
done

# ── Build report flags ────────────────────────────────────────────────
REPORT_FLAGS=(
  --report console
  --report "json:$DATADIR/report.json"
)
for spec in "${EXTRA_REPORTS[@]}"; do
  REPORT_FLAGS+=(--report "$spec")
done

# ── Run bench ────────────────────────────────────────────────────────
START_TIME=$(date +%s)

"$BENCH_BIN" send \
  --rpc-url "$RPC" \
  --tps "$TPS" \
  --max-concurrent "$MAX_CONCURRENT" \
  --input "$INPUT" \
  --metrics-url "$METRICS_URL" \
  --scrape-interval-ms "$SCRAPE_INTERVAL" \
  --drain-timeout "$DRAIN_TIMEOUT" \
  "${METADATA_FLAGS[@]}" \
  "${REPORT_FLAGS[@]}" 2>&1

END_TIME=$(date +%s)
SEND_ELAPSED=$((END_TIME - START_TIME))
echo ""
echo "=== Bench complete in ${SEND_ELAPSED}s ==="

TOTAL_ELAPSED=$SEND_ELAPSED

# ── Kill tempo ───────────────────────────────────────────────────────
if [[ -f "$DATADIR/tempo.pid" ]]; then
  TEMPO_PID=$(cat "$DATADIR/tempo.pid")
  if kill -0 "$TEMPO_PID" 2>/dev/null; then
    echo "Stopping tempo (PID $TEMPO_PID)..."
    kill "$TEMPO_PID"
    wait "$TEMPO_PID" 2>/dev/null || true
  fi
fi

echo ""
echo "=== Run complete ==="
echo "  Total time:   ${TOTAL_ELAPSED}s"
echo "  Report:       $DATADIR/report.json"

# ── Print summary from report ────────────────────────────────────────
python3 -c "
import json
r = json.load(open('$DATADIR/report.json'))
print()
print(f'  Sent:     {r[\"sent\"]}')
print(f'  Success:  {r[\"success\"]}')
print(f'  Failed:   {r[\"failed\"]}')
print(f'  Elapsed:  {r[\"elapsed_secs\"]:.1f}s')
print(f'  TPS:      {r[\"tps\"]:.0f}')
print(f'  p50:      {r[\"latency\"][\"p50_ms\"]:.2f}ms')
print(f'  p99:      {r[\"latency\"][\"p99_ms\"]:.2f}ms')
samples = len(r.get('samples', []))
blocks = len(r.get('blocks', []))
if samples:
    print(f'  Samples:  {samples}')
if blocks:
    print(f'  Blocks:   {blocks}')
" 2>/dev/null || true
