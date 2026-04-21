#!/usr/bin/env bash
# lib.sh — Shared functions for bench scripts.
#
# Source this file; do not execute directly.

# ── Node management ─────────────────────────────────────────────────

# Patch the genesis timestamp to the current time (required for dev mode).
patch_genesis() {
  local genesis="$1"
  python3 -c "
import json, time
g = json.load(open('$genesis'))
g['timestamp'] = hex(int(time.time()))
json.dump(g, open('$genesis', 'w'))
"
  echo "Genesis timestamp patched"
}

# Start a tempo dev node. Sets TEMPO_PID.
#
# Arguments: genesis chain_file datadir block_time gas_limit max_tasks
#            txpool_count txpool_size tempo_bin [extra_flags...]
start_tempo() {
  local genesis="$1" datadir="$2" block_time="$3" gas_limit="$4"
  local max_tasks="$5" txpool_count="$6" txpool_size="$7" tempo_bin="$8"
  shift 8
  local faucet_key="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
  local faucet_amount="1000000000000"
  local faucet_addrs=("0x20c0000000000000000000000000000000000000" "0x20c0000000000000000000000000000000000001")

  # Kill old instance
  if pgrep -f "tempo node" >/dev/null 2>&1; then
    echo "Killing existing tempo..."
    pkill -f "tempo node" || true
    sleep 2
  fi

  patch_genesis "$genesis"

  local faucet_flags=()
  faucet_flags+=(--faucet.enabled)
  faucet_flags+=(--faucet.private-key "$faucet_key")
  faucet_flags+=(--faucet.amount "$faucet_amount")
  for addr in "${faucet_addrs[@]}"; do
    faucet_flags+=(--faucet.address "$addr")
  done

  echo "Starting tempo (block-time=$block_time, gas-limit=$gas_limit, max-tasks=$max_tasks, txpool=${txpool_count}x${txpool_size}MB)..."
  "$tempo_bin" node \
    --chain "$genesis" \
    --datadir "$datadir" \
    --dev \
    --dev.block-time "$block_time" \
    --builder.gaslimit "$gas_limit" \
    --builder.max-tasks "$max_tasks" \
    --http \
    --http.api all \
    --rpc.max-connections 10000 \
    --metrics 127.0.0.1:9001 \
    --txpool.pending-max-count "$txpool_count" \
    --txpool.pending-max-size "$txpool_size" \
    --txpool.basefee-max-count "$txpool_count" \
    --txpool.basefee-max-size "$txpool_size" \
    --txpool.queued-max-count "$txpool_count" \
    --txpool.queued-max-size "$txpool_size" \
    "${faucet_flags[@]}" \
    --log.stdout.filter error \
    "$@" \
    >"$datadir/tempo.log" 2>&1 &
  TEMPO_PID=$!
  echo "$TEMPO_PID" > "$datadir/tempo.pid"
  echo "Tempo PID=$TEMPO_PID"
}

# Wait for the HTTP RPC to become available.
wait_for_rpc() {
  local url="${1:-http://localhost:8545}"
  for i in $(seq 1 60); do
    if curl -sf -X POST -H 'Content-Type: application/json' \
      -d '{"jsonrpc":"2.0","method":"net_version","params":[],"id":1}' \
      "$url" >/dev/null 2>&1; then
      echo "RPC ready after ${i}s"
      return 0
    fi
    [[ $i -eq 60 ]] && { echo "error: RPC not ready after 60s" >&2; return 1; }
    sleep 1
  done
}

# Fund accounts using tempo_fundAddress.
fund_accounts() {
  local txgen_bin="$1" spec="$2" rpc="$3"

  local addresses
  addresses=$("$txgen_bin" addresses -s "$spec" -f shell)
  local addr_count
  addr_count=$(echo "$addresses" | wc -w)
  echo "Funding $addr_count accounts..."
  echo "$addresses" | tr ' ' '\n' | xargs -P 50 -I{} \
    curl -sf -X POST -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","method":"tempo_fundAddress","params":["{}"],"id":1}' \
    "$rpc" -o /dev/null
  echo "Funded. Waiting for txpool to drain..."

  local zero_count=0
  for i in $(seq 1 120); do
    local pending
    pending=$(curl -sf -X POST -H 'Content-Type: application/json' \
      -d '{"jsonrpc":"2.0","method":"txpool_status","params":[],"id":1}' \
      "$rpc" 2>/dev/null | python3 -c "import sys,json; print(int(json.load(sys.stdin)['result']['pending'],16))" 2>/dev/null || echo "?")
    if [[ "$pending" == "0" ]]; then
      zero_count=$((zero_count + 1))
      if [[ $zero_count -ge 3 ]]; then
        echo "Txpool drained after ${i}s"
        return 0
      fi
    else
      zero_count=0
    fi
    [[ $((i % 10)) -eq 0 ]] && echo "  pending: $pending"
    sleep 1
  done
}

# Stop the tempo node if a PID file exists.
stop_tempo() {
  local datadir="$1"
  if [[ -f "$datadir/tempo.pid" ]]; then
    local pid
    pid=$(cat "$datadir/tempo.pid")
    if kill -0 "$pid" 2>/dev/null; then
      echo "Stopping tempo (PID $pid)..."
      kill "$pid"
      wait "$pid" 2>/dev/null || true
    fi
  fi
}

# ── Report ───────────────────────────────────────────────────────────

# Print a summary from a JSON report file.
print_summary() {
  local report="$1"
  python3 -c "
import json, sys
r = json.load(open('$report'))

# send mode fields
sent = r.get('sent')
if sent is not None:
    print(f'  Sent:     {sent}')
    print(f'  Success:  {r[\"success\"]}')
    print(f'  Failed:   {r[\"failed\"]}')
    print(f'  Elapsed:  {r[\"elapsed_secs\"]:.1f}s')
    print(f'  TPS:      {r[\"tps\"]:.0f}')
    lat = r.get('latency')
    if lat:
        print(f'  p50:      {lat[\"p50_ms\"]:.2f}ms')
        print(f'  p99:      {lat[\"p99_ms\"]:.2f}ms')

# block stats (both modes)
rs = r.get('run_stats')
if rs:
    print(f'  Blocks:   {rs[\"total_blocks\"]}')
    print(f'  Blocks/s: {rs[\"avg_blocks_per_second\"]:.1f}')
    print(f'  Avg TPS:  {rs[\"avg_tps\"]:.0f}')
    print(f'  Gas/s:    {rs[\"avg_gas_per_second\"]:.0f}')

samples = len(r.get('samples', []))
blocks = len(r.get('blocks', []))
if samples:
    print(f'  Samples:  {samples}')
" 2>/dev/null || true
}
