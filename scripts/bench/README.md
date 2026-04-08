# Bench Scripts

End-to-end benchmarking for a local Tempo dev node. Starts a node, funds
accounts, generates transactions, sends them with metrics scraping, waits
for the txpool to drain, and produces a 15-panel matplotlib dashboard.

## Prerequisites

- Tempo binary (`~/.tempo/bin/tempo` or `$TEMPO_BIN`)
- Genesis file at `/tmp/txgen-localnet/genesis.json` (from `tempo-xtask generate-genesis`)
- Built binaries: `cargo build --release -p bench-cli -p txgen-tempo`
- Python 3 (for scraping and plotting)
- [uv](https://github.com/astral-sh/uv) (for running plot.py with matplotlib)

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
