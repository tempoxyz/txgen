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
cargo install --path crates/txgen-cli
cargo install --path crates/bench-cli
```

Or build from source:

```bash
cargo build --release
```

## CLI Tools

The workspace provides two binaries: `txgen` for transaction generation and `bench` for benchmarking.

### `txgen`

#### `txgen generate`

Generate transactions from a workload spec.

```bash
# Generate 1000 Ethereum transactions
txgen generate -s workload.yaml -c ethereum -n 1000

# Generate Tempo transactions with reproducible seed
txgen generate -s workload.yaml -c tempo -n 1000 --seed 42

# Fetch nonces from chain before generating
txgen generate -s workload.yaml -c ethereum -n 1000 --rpc http://localhost:8545

# Output to file
txgen generate -s workload.yaml -c ethereum -n 1000 -o transactions.ndjson
```

| Flag | Description |
|------|-------------|
| `-s, --spec <PATH>` | Workload specification file (YAML) |
| `-c, --chain <CHAIN>` | Chain plugin: `ethereum`, `tempo` |
| `-n, --count <N>` | Number of transactions to generate |
| `-o, --output <PATH>` | Output file (default: stdout) |
| `--rpc <URL>` | RPC endpoint for fetching current nonces |
| `--rpc-rps <N>` | Rate limit for RPC requests per second (0 = unbounded) |
| `--seed <SEED>` | RNG seed for reproducibility |

**Required RPC methods:** `eth_getTransactionCount` (only when `--rpc` is provided)

#### `txgen addresses`

List all addresses from a workload spec (useful for funding).

```bash
txgen addresses -s workload.yaml
txgen addresses -s workload.yaml -f json
txgen addresses -s workload.yaml -f shell   # space-separated for xargs
```

| Flag | Description |
|------|-------------|
| `-s, --spec <PATH>` | Workload specification file (YAML) |
| `-f, --format <FMT>` | Output format: `plain`, `json`, `shell` (default: `plain`) |

**Required RPC methods:** None (offline)

#### `txgen extract`

Extract raw RLP-encoded blocks from an archive node as NDJSON.

```bash
txgen extract --rpc http://localhost:8545 --from 1000 --to 2000 -o blocks.ndjson
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
txgen generate -s workload.yaml -c ethereum -n 1000 | bench send --rpc-url http://localhost:8545

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

**Required RPC methods:** `eth_sendRawTransaction`, `eth_getBlockByNumber`

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

**Required RPC methods:**
- Source RPC: `debug_getRawBlock`
- Engine API: `reth_newPayload`, `reth_forkchoiceUpdated` (reth custom Engine API)

#### `bench plot`

Generate PNG charts from a JSON report.

```bash
bench plot --input report.json --output ./charts
bench plot --input report.json -t throughput
```

| Flag | Description |
|------|-------------|
| `-i, --input <PATH>` | Input JSON report file |
| `-o, --output <PATH>` | Output directory for PNGs (default: `.`) |
| `-t, --plot-type <TYPE>` | `throughput`, `latency`, `cumulative`, `all` (default: `all`) |
| `--width <PX>` | Chart width (default: 1200) |
| `--height <PX>` | Chart height (default: 600) |

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
| `clickhouse:<url>` | Push time-series data to ClickHouse |

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

### Ethereum (`-c ethereum`)

Standard Ethereum transaction types:

| Type | Description |
|------|-------------|
| `legacy` | Pre-EIP-1559 transactions |
| `eip2930` | Access list transactions |
| `eip1559` | Dynamic fee transactions |

### Tempo (`-c tempo`)

All Ethereum types plus Tempo-native transactions:

| Type | Description |
|------|-------------|
| `tempo` | Native 0x76 transactions with parallel nonces |

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
    
    # Tempo-specific
    nonce_key: "42"              # Parallel nonce key (0 = protocol nonce)
    fee_token: "0x..."           # Pay gas in stablecoin
    valid_after: 1700000000      # Scheduled: valid after timestamp
    valid_before: 1700100000     # Scheduled: valid before timestamp
    
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

**Parallel nonces:** Using different `nonce_key` values allows transactions from the same sender to be sent in parallel without nonce conflicts.

## RPC Methods

Summary of which RPC methods are required by each feature:

| RPC Method | Required By |
|------------|-------------|
| `eth_getTransactionCount` | `txgen generate --rpc` |
| `eth_sendRawTransaction` | `bench send` |
| `eth_getBlockByNumber` | `bench send` (per-block stats collection) |
| `debug_getRawBlock` | `txgen extract`, `bench replay` (source RPC) |
| `reth_newPayload` | `bench send-blocks`, `bench replay` (engine) |
| `reth_forkchoiceUpdated` | `bench send-blocks`, `bench replay` (engine) |

> **Note:** `debug_*` methods require a node with the debug namespace enabled (typically archive nodes). `reth_*` methods are custom reth Engine API extensions.

## Examples

See the `examples/` directory:

- `simple.yaml` — Basic Ethereum transfers
- `tempo.yaml` — Tempo transactions with parallel nonces
- `tempo-mainnet-spam.yaml` — Tempo mainnet workload
- `erc20.abi.json` — ERC-20 ABI artifact

```bash
# Run the simple example
txgen generate -s examples/simple.yaml -c ethereum -n 10 --seed 42

# Run the Tempo example
txgen generate -s examples/tempo.yaml -c tempo -n 10 --seed 42
```

## Architecture

```
txgen/
├── crates/
│   ├── txgen-core/       # Core library: spec parsing, account management, output
│   ├── txgen-ethereum/   # Ethereum plugin: legacy, eip2930, eip1559
│   ├── txgen-tempo/      # Tempo plugin: 0x76 + delegates to ethereum
│   ├── txgen-cli/        # CLI binary (txgen)
│   ├── bench-core/       # Benchmarking: metrics, sender, reporters
│   └── bench-cli/        # Bench CLI binary (bench)
└── examples/             # Example workload specs
```

### Plugin Trait

Chains implement the `ChainPlugin` trait:

```rust
pub trait ChainPlugin: Send + Sync {
    type Template: DeserializeOwned;
    
    fn name(&self) -> &'static str;
    fn build(&self, template: Self::Template, ctx: &mut BuildContext) -> Result<GeneratedTx>;
}
```

## License

MIT OR Apache-2.0
