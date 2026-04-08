# Bench Scripts

End-to-end benchmarking for a local Tempo dev node. Starts a node, funds
accounts, generates transactions, sends them with metrics scraping, waits
for the txpool to drain, and produces a 15-panel matplotlib dashboard.

## Setup

### 1. Install Tempo

```bash
# Install tempoup
curl -L https://tempo.xyz/install | bash

# Install Tempo (use a version with faucet + dev mode support)
tempoup --version 1.5.1
```

The binary is installed to `~/.tempo/bin/tempo`. Override with `$TEMPO_BIN`
or `--tempo-bin`.

### 2. Generate genesis

The bench spec uses 10,000 accounts from the test mnemonic. Generate a
genesis with enough pre-funded accounts from the **tempo** repo:

```bash
# In the tempo repo:
cargo run -p tempo-xtask -- generate-genesis \
  -a 11000 \
  --output /tmp/txgen-localnet \
  --no-dkg-in-genesis
```

This creates `/tmp/txgen-localnet/genesis.json` with chain ID 1337. The
`setup.sh` script automatically patches the genesis timestamp to the
current time before each node start (required for dev mode).

### 3. Build txgen binaries

```bash
# In the txgen repo:
cargo build --release -p bench-cli -p txgen-tempo
```

### 4. Other dependencies

- Python 3 (for `scrape.py` and `plot.py`)
- [uv](https://github.com/astral-sh/uv) (for running `plot.py` with matplotlib)
- `curl` (for RPC calls and metrics scraping)

## Quick Start

```bash
# Full run: setup node → generate 200k txs → send at 5000 TPS → plot
./scripts/bench/all.sh
```

## Examples

```bash
# 1M transactions, unlimited TPS
./scripts/bench/all.sh --count 1000000 --tps 0

# Custom block time and gas limit
./scripts/bench/all.sh --block-time 1s --gas-limit 1000000000

# Skip setup (reuse running node + existing txs)
./scripts/bench/all.sh --no-setup --tps 10000

# Run in tmux, plot later
./scripts/bench/all.sh --tmux --no-plot
uv run --with matplotlib python3 scripts/bench/plot.py

# Re-plot from existing data
uv run --with matplotlib python3 scripts/bench/plot.py /path/to/datadir
```

## Scripts

| Script       | Purpose                                                      |
|--------------|--------------------------------------------------------------|
| `setup.sh`   | Kill old node, patch genesis, start Tempo, fund accounts, generate txs |
| `run.sh`     | Start metrics scraper, run bench, wait for pool drain        |
| `scrape.py`  | Capture all Prometheus metrics to NDJSON (every 500ms)       |
| `plot.py`    | 15-panel matplotlib dashboard from scraped metrics           |
| `all.sh`     | Orchestrates setup → run → plot                             |

## Outputs

All outputs go to a temp datadir (path stored in `/tmp/txgen-bench-datadir`):

- `txs.ndjson` — generated transactions
- `metrics.ndjson` — scraped Prometheus metrics (one JSON object per sample)
- `report.json` — bench send report (sent/success/failed/latency)
- `bench_plots.png` — matplotlib dashboard
- `metric_keys.txt` — list of all available metric keys
- `tempo.log` — node logs

## Notes

- `--dev.block-time` and `--dev.block-max-transactions` are **mutually
  exclusive** — the node exits with code 2 if both are given.
- The default workload (`bench-spec.yaml`) sends TIP20 `transfer()` calls
  at ~50k gas each. With the default 3B gas limit and a ~420M payment gas
  limit, the builder caps at ~8,400 txs/block.
- The faucet funds accounts via `tempo_fundAddress` RPC. The ~20k
  "invalid_tx" skipped at startup in the metrics are from the funding
  phase (10k accounts × 2 token addresses) — this is expected.
