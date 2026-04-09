#!/usr/bin/env bash
# run.sh — Run bench with metrics scraping.
#
# Usage:
#   ./scripts/bench/run.sh [OPTIONS]
#
# Options:
#   --tps <N>              Target TPS (default: 5000)
#   --max-concurrent <N>   Max concurrent HTTP requests (default: 500)
#   --rpc-url <URL>        RPC endpoint (default: http://localhost:8545)
#   --datadir <PATH>       Datadir (default: read from /tmp/txgen-bench-datadir)
#   --input <PATH>         Tx file (default: $DATADIR/txs.ndjson)
#   --scrape-interval <S>  Metrics scrape interval (default: 0.3)
#   --drain-timeout <N>    Seconds to wait for pool drain (default: 300)
#
# Outputs:
#   $DATADIR/metrics.csv   Scraped prometheus metrics
#   $DATADIR/report.json   Bench report
set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────────────
TPS=5000
MAX_CONCURRENT=500
RPC="http://localhost:8545"
DATADIR=""
INPUT=""
SCRAPE_INTERVAL="0.5"
DRAIN_TIMEOUT=300

# ── Parse args ───────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --tps)              TPS="$2"; shift 2 ;;
    --max-concurrent)   MAX_CONCURRENT="$2"; shift 2 ;;
    --rpc-url)          RPC="$2"; shift 2 ;;
    --datadir)          DATADIR="$2"; shift 2 ;;
    --input)            INPUT="$2"; shift 2 ;;
    --scrape-interval)  SCRAPE_INTERVAL="$2"; shift 2 ;;
    --drain-timeout)    DRAIN_TIMEOUT="$2"; shift 2 ;;
    -h|--help)          head -16 "$0" | tail -15; exit 0 ;;
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

# ── Start scraper ────────────────────────────────────────────────────
rm -f "$DATADIR/stop_scrape"
python3 "$SCRIPT_DIR/scrape.py" "$DATADIR/metrics.ndjson" "$DATADIR/stop_scrape" "$SCRAPE_INTERVAL" &
SCRAPE_PID=$!

echo "=== Bench: ${TX_COUNT} txs, tps=${TPS}, max_concurrent=${MAX_CONCURRENT} ==="
echo "  RPC:     $RPC"
echo "  Input:   $INPUT"
echo "  Datadir: $DATADIR"
echo "  Scraper: PID $SCRAPE_PID (interval=${SCRAPE_INTERVAL}s)"
echo ""

# ── Run bench ────────────────────────────────────────────────────────
START_TIME=$(date +%s)

"$BENCH_BIN" send \
  --rpc-url "$RPC" \
  --tps "$TPS" \
  --max-concurrent "$MAX_CONCURRENT" \
  --input "$INPUT" \
  --report "json:$DATADIR/report.json" 2>&1

END_TIME=$(date +%s)
SEND_ELAPSED=$((END_TIME - START_TIME))
echo ""
echo "=== Send complete in ${SEND_ELAPSED}s ==="

# ── Wait for pool drain ─────────────────────────────────────────────
echo "Waiting for txpool to drain (timeout=${DRAIN_TIMEOUT}s)..."
ZERO_COUNT=0
for i in $(seq 1 "$DRAIN_TIMEOUT"); do
  pending=$(curl -sf -X POST -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","method":"txpool_status","params":[],"id":1}' \
    "$RPC" 2>/dev/null | python3 -c "import sys,json; print(int(json.load(sys.stdin)['result']['pending'],16))" 2>/dev/null || echo "?")
  if [[ "$pending" == "0" ]]; then
    ZERO_COUNT=$((ZERO_COUNT + 1))
    if [[ $ZERO_COUNT -ge 3 ]]; then
      echo "Pool drained after ${i}s (3 consecutive zero readings)"
      break
    fi
  else
    ZERO_COUNT=0
  fi
  [[ $((i % 10)) -eq 0 ]] && echo "  pending: $pending"
  sleep 1
done

# ── Stop scraper ─────────────────────────────────────────────────────
touch "$DATADIR/stop_scrape"
wait $SCRAPE_PID 2>/dev/null || true

TOTAL_ELAPSED=$(( $(date +%s) - START_TIME ))
METRIC_ROWS=$(wc -l < "$DATADIR/metrics.ndjson")

echo ""
echo "=== Run complete ==="
echo "  Total time:   ${TOTAL_ELAPSED}s"
echo "  Metrics rows: $METRIC_ROWS"
echo "  Report:       $DATADIR/report.json"
echo "  Metrics:      $DATADIR/metrics.ndjson"

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
" 2>/dev/null || true
