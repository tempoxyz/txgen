# txgen

A chain-agnostic transaction generation tool for blockchain load testing and benchmarking.

## Overview

txgen generates signed, RLP-encoded transactions from YAML workload specifications. It outputs transactions as newline-delimited JSON (NDJSON) that can be piped to sending tools or saved for replay.

**Key features:**
- **Chain-agnostic**: Plugin architecture supports multiple chains (Ethereum, Tempo)
- **Deterministic**: Seed-based RNG for reproducible transaction generation
- **Flexible**: YAML specs with weighted template mixing, value generators, and account pools
- **Fast**: Generates transactions without network I/O

## Installation

```bash
cargo install --path crates/txgen-ethereum
cargo install --path crates/txgen-tempo
cargo install --path crates/bench-cli
```

Or build from source:

```bash
cargo build --release
```

## CLI Tools

The workspace provides three binaries: `txgen-ethereum` and `txgen-tempo` for transaction generation, and `bench` for benchmarking. Each txgen binary is a standalone chain-specific generator.

### `txgen-ethereum` / `txgen-tempo`

#### `generate`

Generate transactions from a workload spec.

```bash
# Generate 1000 Ethereum transactions
txgen-ethereum generate -s workload.yaml -n 1000

# Generate Tempo transactions with reproducible seed
txgen-tempo generate -s workload.yaml -n 1000 --seed 42

# Fetch nonces from chain before generating
txgen-ethereum generate -s workload.yaml -n 1000 --rpc http://localhost:8545

# Output to file
txgen-ethereum generate -s workload.yaml -n 1000 -o transactions.ndjson
```

| Flag | Description |
|------|-------------|
| `-s, --spec <PATH>` | Workload specification file (YAML) |
| `-n, --count <N>` | Number of transactions to generate |
| `-o, --output <PATH>` | Output file (default: stdout) |
| `--rpc <URL>` | RPC endpoint for fetching current nonces |
| `--rpc-rps <N>` | Rate limit for RPC requests per second (0 = unbounded) |
| `--seed <SEED>` | RNG seed for reproducibility |

**Required RPC methods:** `eth_getTransactionCount` (only when `--rpc` is provided)

#### `addresses`

List all addresses from a workload spec (useful for funding).

```bash
txgen-ethereum addresses -s workload.yaml
txgen-ethereum addresses -s workload.yaml -f json
txgen-ethereum addresses -s workload.yaml -f shell   # space-separated for xargs
```

| Flag | Description |
|------|-------------|
| `-s, --spec <PATH>` | Workload specification file (YAML) |
| `-f, --format <FMT>` | Output format: `plain`, `json`, `shell` (default: `plain`) |

**Required RPC methods:** None (offline)

#### `extract`

Extract raw RLP-encoded blocks from an archive node as NDJSON.

```bash
txgen-ethereum extract --rpc http://localhost:8545 --from 1000 --to 2000 -o blocks.ndjson
```

| Flag | Description |
|------|-------------|
| `--rpc <URL>` | RPC endpoint (archive node) |
| `--from <N>` | First block number (inclusive) |
| `--to <N>` | Last block number (inclusive) |
| `-o, --output <PATH>` | Output file (default: stdout) |
| `--buffer-size <N>` | Number of blocks to prefetch ahead (default: 20) |

**Required RPC methods:** `debug_getRawBlock`

### `bench`

#### `bench send`

Send pre-generated transactions from NDJSON file or stdin.

After sending completes, queries the node for per-block statistics (transaction count, gas used) and includes them in the report.

```bash
# From file
bench send --input transactions.ndjson --rpc-url http://localhost:8545 --tps 500

# From stdin (pipe from txgen)
txgen-ethereum generate -s workload.yaml -n 1000 | bench send --rpc-url http://localhost:8545

# With JSON report and metadata
bench send -i txs.ndjson --rpc-url http://localhost:8545 \
  --report json:report.json \
  -m build-sha=abcdef -m build-profile=perf
```

| Flag | Description |
|------|-------------|
| `-i, --input <PATH>` | Input NDJSON file (default: stdin) |
| `--rpc-url <URL>` | RPC endpoint URLs, comma-separated or repeated (default: `http://localhost:8545`) |
| `--tps <N>` | Target transactions per second (0 = unlimited) |
| `--max-concurrent <N>` | Maximum concurrent requests (default: 100) |
| `--timeout <DUR>` | Request timeout (default: 30s) |
| `--report <FORMAT>` | Report destinations, repeatable (see [Reporters](#reporters)) |
| `-m, --metadata <K=V>` | Metadata key=value pairs for the report, repeatable |
| `--metrics-url <URL>` | Prometheus endpoint to scrape during the run (see [Metrics Scraping](#metrics-scraping)) |
| `--scrape-interval-ms <N>` | Scrape interval in milliseconds (default: 500) |
| `--drain-timeout <N>` | Wait for txpool drain after sending, in seconds (default: 300, 0 to disable) |

**Required RPC methods:** `eth_sendRawTransaction`, `eth_getBlockByNumber`, `txpool_status` (for `--drain-timeout`)

#### `bench send-blocks`

Submit RLP-encoded blocks via reth Engine API.

```bash
bench send-blocks --engine http://localhost:8551 --jwt-secret /path/to/jwt.hex --input blocks.ndjson
```

| Flag | Description |
|------|-------------|
| `--engine <URL>` | Engine API endpoint |
| `--jwt-secret <PATH>` | Path to JWT secret file |
| `-i, --input <PATH>` | Input NDJSON file (default: stdin) |
| `--wait-for-persistence <POLICY>` | Persistence wait policy: `always`, `never`, or `every:N` (default: `every:2`) |
| `--report <FORMAT>` | Report destinations, repeatable (see [Reporters](#reporters)) |
| `-m, --metadata <K=V>` | Metadata key=value pairs for the report, repeatable |
| `--metrics-url <URL>` | Prometheus endpoint to scrape during the run (see [Metrics Scraping](#metrics-scraping)) |
| `--scrape-interval-ms <N>` | Scrape interval in milliseconds (default: 500) |

**Required RPC methods:** `reth_newPayload`, `reth_forkchoiceUpdated` (reth custom Engine API)

#### `bench replay`

Replay blocks from a source archive node via Engine API. Equivalent to `txgen extract ... | bench send-blocks ...` but avoids serialization overhead.

```bash
bench replay \
  --rpc-source http://archive:8545 \
  --engine http://localhost:8551 \
  --jwt-secret /path/to/jwt.hex \
  --from 1000 --to 2000
```

| Flag | Description |
|------|-------------|
| `--rpc-source <URL>` | Source RPC endpoint (archive node) |
| `--engine <URL>` | Engine API endpoint |
| `--jwt-secret <PATH>` | Path to JWT secret file |
| `--from <N>` | Starting block number |
| `--to <N>` | Ending block number |
| `--report <FORMAT>` | Report destinations, repeatable (see [Reporters](#reporters)) |
| `-m, --metadata <K=V>` | Metadata key=value pairs for the report, repeatable |
| `--metrics-url <URL>` | Prometheus endpoint to scrape during the run (see [Metrics Scraping](#metrics-scraping)) |
| `--scrape-interval-ms <N>` | Scrape interval in milliseconds (default: 500) |

**Required RPC methods:**
- Source RPC: `debug_getRawBlock`
- Engine API: `reth_newPayload`, `reth_forkchoiceUpdated` (reth custom Engine API)

#### `bench view`

Print an existing JSON report to the console.

```bash
bench view
bench view report.json
```

| Argument | Description |
|----------|-------------|
| `<INPUT>` | JSON report file (default: `report.json`) |

**Required RPC methods:** None (offline)

### Live Progress

During `bench send`, the console reporter displays a live progress line:

```
  Sent: 5000 | OK: 4800 | Fail: 50 | Inflight: 150/200 | Rate: 980/1000 tps
```

| Field | Description |
|-------|-------------|
| `Sent` | Total transactions submitted to the sender |
| `OK` | Successful RPC responses |
| `Fail` | Failed RPC responses |
| `Inflight` | Transactions sent but not yet resolved (current/`--max-concurrent`) |
| `Rate` | Actual send rate (current/`--tps` target) |

**Reading backpressure:**

- **Concurrency-bound**: `Inflight` is at or near `--max-concurrent`. The RPC endpoint can't keep up — responses are slow relative to the send rate. Increase `--max-concurrent` or reduce `--tps`.
- **Rate-limited**: `Rate` matches the `--tps` target and `Inflight` is well below `--max-concurrent`. The rate limiter is the bottleneck (working as intended).
- **Source-bound**: `Rate` is below `--tps` and `Inflight` is low. The transaction source (file I/O or stdin pipe) is the bottleneck.

### Reporters

Report destinations are specified with `--report` and can be repeated:

| Format | Description |
|--------|-------------|
| `console` | Print summary to stderr (default if no reporters specified) |
| `json:<path>` | Write JSON report to file |
| `clickhouse:<url>` | Push benchmark data to ClickHouse |

### Metrics Scraping

All bench commands support built-in Prometheus metrics scraping via `--metrics-url`. When enabled, a background scraper periodically fetches the node's `/metrics` endpoint and includes all samples in the JSON report.

```bash
# Scrape node metrics alongside the benchmark
bench send -i txs.ndjson --metrics-url http://127.0.0.1:9001/metrics --report json:report.json

# Custom scrape interval (default: 500ms)
bench send -i txs.ndjson --metrics-url http://127.0.0.1:9001/metrics --scrape-interval-ms 200
```

In `send` mode, internal txgen metrics (`txgen_transactions_sent_total`, `txgen_transactions_success_total`, etc.) are also snapshotted on the same interval and included alongside node metrics.

Metadata key=value pairs (`-m key=value`) are applied as labels to all samples, useful for tagging runs with build SHAs, profiles, or experiment IDs.

### ClickHouse Reporting

The ClickHouse reporter pushes benchmark results into three tables (`txgen_runs`, `txgen_blocks`, `txgen_metric_samples`). Block data is stored as factual chain data; metrics are stored as point-in-time scrape snapshots with no block attribution. It requires four metadata keys:

```bash
bench send -i txs.ndjson \
  --metrics-url http://127.0.0.1:9001/metrics \
  --report clickhouse:https://host:8443 \
  -m scenario=tip20-10k \
  -m platform=tempo \
  -m git-sha=abc123 \
  -m git-ref=main
```

| Required Metadata | Description |
|-------------------|-------------|
| `scenario` | Benchmark scenario name (e.g. `tip20-10k`) |
| `platform` | Target platform: `ethereum` or `tempo` |
| `git-sha` | Node commit SHA being benchmarked |
| `git-ref` | Node git branch/ref |

Authentication is configured via environment variables: `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE`. See [`scripts/clickhouse/README.md`](scripts/clickhouse/README.md) for schema setup and example queries.

The JSON report includes:
- `samples` — point-in-time metric snapshots (internal + node), stored as a time series
- `blocks` — factual chain data for each block in the run (tx count, gas used, etc.)

## Output Format

Transactions are output as NDJSON with two fields:

```json
{"raw":"0x02f86c01...","key":"0x3c44cdddb6a900fa2b585dd299e03d12fa4293bc"}
```

| Field | Description |
|-------|-------------|
| `raw` | RLP-encoded signed transaction (EIP-2718 envelope) |
| `key` | Scheduling key (20 bytes) for ordering |

**Scheduling rule:** Transactions with the same `key` must be sent sequentially (they share a nonce sequence). Different keys can be sent in parallel.

## Workload Specification

Workload specs are YAML files that define accounts, templates, and mix ratios.

### Structure

```yaml
# Chain ID for transaction signing
chain_id: 1

# Default gas configuration
gas:
  max_fee_per_gas: 1000000000      # 1 gwei
  max_priority_fee_per_gas: 1000000000

# Account pools derived from mnemonics
accounts:
  users:
    mnemonic: "${MNEMONIC}"  # Supports environment variable expansion
    range: [0, 100]          # Derive accounts 0-99
  
  deployer:
    mnemonic: "${MNEMONIC}"
    index: 0                 # Single account

# ABI artifacts for contract calls
artifacts:
  erc20: "./abis/ERC20.json"

# Transaction templates
templates:
  transfer:
    type: eip1559
    from:
      pool: users
      select: random
    to: "0x..."
    value: 1000
    gas_limit: 21000

# Weighted mix for generation
mix:
  - template: transfer
    weight: 100
```

### Account Selection

Accounts are selected from pools using `select`:

```yaml
from:
  pool: users
  select: random    # Random account from pool
```

### Value Generators

Dynamic values can be generated using expressions:

```yaml
# Uniform random in range
value:
  uniform: [1000, 10000]

# Random choice from list
to:
  choice:
    - "0x0000000000000000000000000000000000000001"
    - "0x0000000000000000000000000000000000000002"

# Account address from pool
to:
  pool: users
  select: random

# Random bytes
input:
  random_bytes: 32
```

### Contract Calls

Use the `call` field for ABI-encoded contract calls:

```yaml
templates:
  erc20_transfer:
    type: eip1559
    from:
      pool: users
      select: random
    gas_limit: 65000
    call:
      to: "0x..."           # Contract address
      abi: erc20            # Artifact name
      function: transfer    # Function name
      args:
        - "0x..."           # recipient
        - 1000000           # amount
      value: 0
```

## Supported Chains

### Ethereum (`txgen-ethereum`)

Standard Ethereum transaction types:

| Type | Description |
|------|-------------|
| `legacy` | Pre-EIP-1559 transactions |
| `eip2930` | Access list transactions |
| `eip1559` | Dynamic fee transactions |

### Tempo (`txgen-tempo`)

All Ethereum types plus Tempo-native transactions:

| Type | Description |
|------|-------------|
| `tempo` | Native 0x76 transactions with protocol, 2D, or expiring nonces |

**Tempo-specific fields:**

```yaml
templates:
  tempo_transfer:
    type: tempo
    from:
      pool: users
      select: random
    to: "0x..."
    value: 1000
    gas_limit: 21000
    max_fee_per_gas: 1000000000
    max_priority_fee_per_gas: 1000000000
    
    # Tempo-specific replay protection
    nonce_key: "42"              # 2D nonce lane (0 = protocol nonce)
    expiring_nonce: true         # TIP-1009 expiring nonce mode
    valid_for_secs: 25           # Relative expiry window, resolved at generation time
    valid_before: 1700100000     # Absolute expiry timestamp (alternative to valid_for_secs)
    fee_token: "0x..."           # Pay gas in stablecoin
    valid_after: 1700000000      # Scheduled: valid after timestamp
    
    # Batched calls
    calls:
      - to: "0x..."
        abi: erc20
        function: transfer
        args: ["0x...", 1000]
      - to: "0x..."
        abi: erc20
        function: approve
        args: ["0x...", 5000]
```

**2D nonces:** Using different `nonce_key` values allows transactions from the same sender to be sent in parallel without nonce conflicts.

**Expiring nonces:** Set `expiring_nonce: true` to generate TIP-1009 transactions. txgen will set `nonce_key = U256::MAX` and `nonce = 0` automatically. You must provide either:

- `valid_before`: an absolute Unix timestamp in seconds
- `valid_for_secs`: a relative TTL in seconds, resolved when the transaction is generated

`valid_for_secs` must be `<= 30`, matching Tempo's expiring nonce validity window.

For streamed benchmark pipelines such as `txgen-tempo generate | bench send`, txgen also applies a deterministic per-transaction fee bump before sponsor signing and sender signing. This guarantees that otherwise identical expiring transactions still produce unique signed payloads, avoiding hash-based replay collisions.

Recommended benchmark setting: `valid_for_secs: 25`. This matches `tempo-bench`'s default behavior and stays inside Tempo's 30-second protocol limit while leaving some propagation slack.

**Benchmarking caveat:** Expiring nonce transactions are still time-bounded by `valid_before <= now + 30s`. Streamed generation/send pipelines are practical because txgen builds and signs each transaction immediately before emitting it, but pre-generating a large expiring-tx file and replaying it later is still unsafe because many transactions will expire before submission.

**Prefetch caveat:** `txgen-tempo generate --rpc` only prefetches constant 2D nonce keys that are fixed in the spec. Dynamic/generated `nonce_key` values (`uniform`, `choice`, etc.) are resolved per transaction and are not prefetched automatically.

## RPC Methods

Summary of which RPC methods are required by each feature:

| RPC Method | Required By |
|------------|-------------|
| `eth_getTransactionCount` | `txgen-ethereum generate --rpc`, `txgen-tempo generate --rpc` |
| `eth_sendRawTransaction` | `bench send` |
| `eth_getBlockByNumber` | `bench send` (per-block stats collection) |
| `debug_getRawBlock` | `txgen extract`, `bench replay` (source RPC) |
| `reth_newPayload` | `bench send-blocks`, `bench replay` (engine) |
| `reth_forkchoiceUpdated` | `bench send-blocks`, `bench replay` (engine) |
| `txpool_status` | `bench send` (pool drain wait) |

> **Note:** `debug_*` methods require a node with the debug namespace enabled (typically archive nodes). `reth_*` methods are custom reth Engine API extensions.

## Examples

See the `examples/` directory:

- `simple.yaml` — Basic Ethereum transfers
- `tempo.yaml` — Tempo transactions with 2D and expiring nonces
- `tempo-mainnet-spam.yaml` — Tempo mainnet workload
- `bench-spec.yaml` — Bench workload specification
- `erc20.abi.json` — ERC-20 ABI artifact

```bash
# Run the simple example
txgen-ethereum generate -s examples/simple.yaml -n 10 --seed 42

# Run the Tempo example
txgen-tempo generate -s examples/tempo.yaml -n 10 --seed 42
```

## Architecture

```
txgen/
├── crates/
│   ├── txgen-core/       # Core library: spec parsing, account management, output
│   ├── txgen-cli/        # Shared CLI framework and NetworkAdapter trait
│   ├── txgen-ethereum/   # Ethereum binary: legacy, eip2930, eip1559
│   ├── txgen-tempo/      # Tempo binary: 0x76 + delegates to ethereum
│   ├── bench-core/       # Benchmarking: metrics, sender, reporters
│   └── bench-cli/        # Bench CLI binary (bench)
└── examples/             # Example workload specs
```

### NetworkAdapter Trait

Each chain binary implements the `NetworkAdapter` trait from `txgen-cli`:

```rust
pub trait NetworkAdapter: Send + Sync {
    type Template: DeserializeOwned + Send;
    type Network: Network;

    fn build_request(
        &self,
        template: Self::Template,
        ctx: &mut BuildContext<'_>,
    ) -> Result<TxRequest<<Self::Network as Network>::TransactionRequest>>;

    fn prefetch_nonces(/* ... */) -> impl Future<Output = Result<()>> { ... }
}
```

## License

MIT OR Apache-2.0
