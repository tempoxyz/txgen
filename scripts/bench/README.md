# Bench Scripts

End-to-end benchmarking for a local Tempo dev node. Starts a node, funds
accounts, runs a benchmark pipeline, and produces a matplotlib dashboard.

Two modes are supported:
- **send** — Generate transactions and send via RPC (`txgen generate | bench send`)
- **replay** — Extract blocks from an archive node and replay via Engine API (`txgen extract | bench send-blocks`)

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
script automatically patches the genesis timestamp to the current time
before each node start (required for dev mode).

### 3. Build txgen binaries

```bash
# In the txgen repo:
cargo build --release -p bench-cli -p txgen-tempo
```

### 4. Other dependencies

- Python 3 (for `plot.py` summary report parsing)
- [uv](https://github.com/astral-sh/uv) (for running `plot.py` with matplotlib)
- `curl` (for RPC calls during account funding)

## Quick Start

```bash
# Send mode: start node → fund → generate 200k txs | send at 5000 TPS → plot
./scripts/bench/run.sh

# Replay mode: start node → extract blocks from archive → replay via Engine API → plot
./scripts/bench/run.sh --mode replay \
  --rpc-source http://archive:8545 \
  --jwt-secret /path/to/jwt.hex \
  --from 1000 --to 2000
```

## Examples

```bash
# 1M transactions, unlimited TPS
./scripts/bench/run.sh --count 1000000 --tps 0

# Generate workload transactions for five minutes
./scripts/bench/run.sh --duration 5m

# Custom block time and gas limit
./scripts/bench/run.sh --block-time 1s --gas-limit 1000000000

# Skip setup (reuse running node), just run the pipeline
./scripts/bench/run.sh --no-setup --tps 10000

# Custom metrics endpoint
./scripts/bench/run.sh --metrics-url http://127.0.0.1:9001/metrics

# Skip plot generation
./scripts/bench/run.sh --no-plot

# Replay with ClickHouse reporting
./scripts/bench/run.sh --mode replay \
  --rpc-source http://archive:8545 \
  --jwt-secret /path/to/jwt.hex \
  --from 20000000 --to 20000100 \
  --report clickhouse:https://host:8443 \
  --metadata scenario=replay-100 \
  --metadata platform=tempo \
  --metadata git-sha=abc123 \
  --metadata git-ref=main

# Re-plot from existing data
uv run --with matplotlib python3 scripts/bench/plot.py /path/to/datadir
```

## Scripts

| Script       | Purpose                                                      |
|--------------|--------------------------------------------------------------|
| `run.sh`     | Main entry point: setup → bench → teardown → plot            |
| `lib.sh`     | Shared functions (start/stop node, fund accounts, etc.)      |
| `plot.py`    | 15-panel matplotlib dashboard from report samples            |

## Outputs

All outputs go to a temp datadir (path stored in `/tmp/txgen-bench-datadir`):

- `report.json` — bench report with scraped metrics in the `samples` array
- `bench_plots.png` — matplotlib dashboard
- `metric_keys.txt` — list of all available metric keys
- `tempo.log` — node logs

## Metrics

Metrics scraping is handled by the `bench` binary itself via `--metrics-url`.
The scraper runs in-process alongside the benchmark, fetching the node's
Prometheus `/metrics` endpoint at the configured interval (default: 500ms).
All scraped samples are included in the JSON report's `samples` array.

In send mode, internal metrics (`txgen_transactions_sent_total`, etc.) are
snapshotted on the same interval. In replay mode, block submission counters
(`txgen_blocks_sent_total`, `txgen_blocks_success_total`,
`txgen_blocks_failed_total`) are snapshotted instead.

The `plot.py` script reads samples directly from `report.json` — no
separate metrics file is needed.

## Notes

- `--dev.block-time` and `--dev.block-max-transactions` are **mutually
  exclusive** — the node exits with code 2 if both are given.
- The default workload (`bench-spec.yaml`) sends TIP20 `transfer()` calls
  at ~50k gas each. With the default 3B gas limit and a ~420M payment gas
  limit, the builder caps at ~8,400 txs/block.
- The faucet funds accounts via `tempo_fundAddress` RPC. The ~20k
  "invalid_tx" skipped at startup in the metrics are from the funding
  phase (10k accounts × 2 token addresses) — this is expected.
- In replay mode, non-VALID engine statuses (INVALID, SYNCING) are fatal
  and cause a non-zero exit.
