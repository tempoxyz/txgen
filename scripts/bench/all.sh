#!/usr/bin/env bash
# all.sh — End-to-end benchmark: setup → run → plot.
#
# Usage:
#   ./scripts/bench/all.sh [OPTIONS]
#
# All options are forwarded to setup.sh and run.sh as appropriate.
#
# Examples:
#   # Quick 200k run with defaults
#   ./scripts/bench/all.sh
#
#   # 500k txs at 5000 TPS
#   ./scripts/bench/all.sh --count 500000 --tps 5000
#
#   # Custom spec and block time
#   ./scripts/bench/all.sh --spec my-spec.yaml --block-time 1s --count 100000
#
# Options (setup):
#   --spec <PATH>         Workload spec (default: examples/bench-spec.yaml)
#   --count <N>           Transactions to generate (default: 200000)
#   --genesis <PATH>      Genesis JSON (default: /tmp/txgen-localnet/genesis.json)
#   --tempo-bin <PATH>    Tempo binary (default: ~/.tempo/bin/tempo)
#   --block-time <DUR>    Dev mode block time (default: 500ms)
#   --gas-limit <N>       Builder gas limit (default: 3000000000)
#   --max-tasks <N>       Builder max tasks (default: 32)
#   --txpool-size <N>     Max txpool per sub-pool (default: 500000)
#   --seed <N>            RNG seed (default: 99)
#
# Options (run):
#   --tps <N>             Target TPS (default: 5000)
#   --max-concurrent <N>  Max concurrent HTTP requests (default: 500)
#
# Options (control):
#   --no-plot             Skip plot generation
#   --no-setup            Skip setup (reuse existing node + txs)
#   --tmux                Run bench in tmux session instead of foreground
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Split args into setup/run/control
SETUP_ARGS=()
RUN_ARGS=()
NO_PLOT=false
NO_SETUP=false
USE_TMUX=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    # Control flags
    --no-plot)    NO_PLOT=true; shift ;;
    --no-setup)   NO_SETUP=true; shift ;;
    --tmux)       USE_TMUX=true; shift ;;
    # Setup-only flags
    --spec|--count|--genesis|--tempo-bin|--block-time|--gas-limit|--max-tasks|--txpool-size|--seed|--datadir)
      SETUP_ARGS+=("$1" "$2"); shift 2 ;;
    # Run-only flags
    --tps|--max-concurrent|--rpc-url|--metrics-url|--scrape-interval|--drain-timeout)
      RUN_ARGS+=("$1" "$2"); shift 2 ;;
    -h|--help)
      head -32 "$0" | tail -31; exit 0 ;;
    *)
      echo "error: unknown option: $1" >&2; exit 1 ;;
  esac
done

# ── Setup ────────────────────────────────────────────────────────────
if [[ "$NO_SETUP" == false ]]; then
  echo "╔══════════════════════════════════════╗"
  echo "║           SETUP                      ║"
  echo "╚══════════════════════════════════════╝"
  bash "$SCRIPT_DIR/setup.sh" "${SETUP_ARGS[@]}"
  echo ""
fi

DATADIR=$(cat /tmp/txgen-bench-datadir)

# ── Run ──────────────────────────────────────────────────────────────
echo "╔══════════════════════════════════════╗"
echo "║           BENCH                      ║"
echo "╚══════════════════════════════════════╝"

if [[ "$USE_TMUX" == true ]]; then
  tmux kill-session -t bench 2>/dev/null || true
  tmux new-session -d -s bench "bash '$SCRIPT_DIR/run.sh' ${RUN_ARGS[*]}"
  echo "Bench running in tmux session 'bench'"
  echo "  tmux attach -t bench    # watch progress"
  echo "  tmux capture-pane -t bench -p  # check output"
  echo ""
  echo "When done, run:"
  echo "  uv run --with matplotlib python3 $SCRIPT_DIR/plot.py"
else
  bash "$SCRIPT_DIR/run.sh" "${RUN_ARGS[@]}"

  # ── Plot ───────────────────────────────────────────────────────
  if [[ "$NO_PLOT" == false ]]; then
    echo ""
    echo "╔══════════════════════════════════════╗"
    echo "║           PLOT                       ║"
    echo "╚══════════════════════════════════════╝"
    uv run --with matplotlib python3 "$SCRIPT_DIR/plot.py" "$DATADIR"
  fi
fi
