# txgen

A chain-agnostic transaction generation tool for blockchain load testing and benchmarking.

## Overview

txgen generates signed, RLP-encoded transactions from YAML workload specifications. It outputs transactions as newline-delimited JSON (NDJSON) that can be piped to sending tools or saved for later use.

For end-to-end workflow examples, see the [txgen Cookbook](COOKBOOK.md).

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

# Generate for a bounded wall-clock duration
txgen-tempo generate -s workload.yaml --duration 5m

# Fetch nonces from chain before generating
txgen-ethereum generate -s workload.yaml -n 1000 --rpc http://localhost:8545

# Output to file
txgen-ethereum generate -s workload.yaml -n 1000 -o transactions.ndjson
```

| Flag | Description |
|------|-------------|
| `-s, --spec <PATH>` | Workload specification file (YAML) |
| `-n, --count <N>` | Maximum number of workload transactions to generate; required unless `--duration` is set |
| `--duration <DUR>` | Maximum workload generation duration; setup is emitted first and excluded |
| `-o, --output <PATH>` | Output file (default: stdout) |
| `--rpc <URL>` | RPC endpoint for fetching current nonces |
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

Extract raw RLP-encoded blocks from an archive node as NDJSON. Use `--bal` to attach RLP-encoded block access lists for replaying EIP-7928/Amsterdam payloads.

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
| `--bal` | Include RLP-encoded block access lists in the `bal` field |

**Required RPC methods:** `debug_getRawBlock`; with `--bal`: `eth_getBlockAccessListByBlockNumber`

#### `extract-big-blocks`

Generate reth-bb-compatible big-block payloads as NDJSON. Use `--bal` to fetch and merge constituent block access lists into `merged_block_access_list`.

```bash
txgen-ethereum extract-big-blocks \
  --rpc http://archive:8545 \
  --from 910020 \
  --count 25 \
  --target-gas 2G \
  -o big-blocks.ndjson
```

| Flag | Description |
|------|-------------|
| `--rpc <URL>` | RPC endpoint (archive node) |
| `--from <N>` | First source block number |
| `--count <N>` | Number of synthetic big blocks to emit |
| `--target-gas <GAS>` | Target gas per big block; accepts `K`, `M`, `G` suffixes |
| `-o, --output <PATH>` | Output file (default: stdout) |
| `--buffer-size <N>` | Reserved for future prefetching compatibility (default: 20) |
| `--bal` | Include a merged RLP-encoded block access list for each synthetic big block |

**Required RPC methods:** `debug_getRawBlock`; with `--bal`: `eth_getBlockAccessListByBlockNumber`

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
| `--metrics-url <URL or NODE:URL,...>` | Prometheus endpoint(s) to scrape during the run (see [Metrics Scraping](#metrics-scraping)) |
| `--scrape-interval-ms <N>` | Scrape interval in milliseconds (default: 500) |
| `--metrics-align <TIMESTAMP>` | Align exported metric timestamps to a benchmark-start Unix timestamp, in seconds or milliseconds |
| `--skip-setup` | Ignore setup-phase transactions in the input stream |
| `--drain-timeout <N>` | Wait for txpool drain after sending, in seconds (default: 300, 0 to disable) |

**Required RPC methods:** `eth_sendRawTransaction`, `eth_getTransactionReceipt` (setup and inclusion waits), `eth_getBlockByNumber`, `txpool_status` (for `--drain-timeout`)

#### `bench send-blocks`

Submit RLP-encoded blocks or reth-bb big-block payloads via reth Engine API.

Raw-block inputs may include an optional `bal` field produced by `txgen extract --bal`; `bench send-blocks` forwards it to `reth_newPayload` alongside the block RLP. Big-block inputs use the current reth-bb `BigBlockData` format where each NDJSON line contains the constituent execution payloads in `env_switches`, plus `prior_block_hashes`, `block_number`, and optional `merged_block_access_list`.

```bash
bench send-blocks --engine http://localhost:8551 --jwt-secret /path/to/jwt.hex --input blocks.ndjson
bench send-blocks --engine http://localhost:8551 --jwt-secret /path/to/jwt.hex --input big-blocks.ndjson

# Exercise reorg paths by building synthetic side-fork blocks via testing_buildBlockV1.
bench send-blocks \
  --engine http://localhost:8551 \
  --jwt-secret /path/to/jwt.hex \
  --rpc http://localhost:8545 \
  --reorg 8 \
  --input blocks.ndjson
```

| Flag | Description |
|------|-------------|
| `--engine <URL>` | Engine API endpoint |
| `--jwt-secret <PATH>` | Path to JWT secret file |
| `-i, --input <PATH>` | Input NDJSON file (default: stdin) |
| `--wait-for-persistence <POLICY>` | Persistence wait policy: `always`, `never`, or `every:N` (default: `every:2`) |
| `--wait-time <DURATION>` | Minimum interval between block submissions. Accepts `100ms`, `2s`, or bare milliseconds like `400` |
| `--reorg [DEPTH]` | Build synthetic side-fork blocks and alternate forkchoice updates to exercise reorg paths. If `DEPTH` is omitted, defaults to `8`. Requires raw RLP block input |
| `--rpc <URL>` | Regular HTTP RPC endpoint for `testing_buildBlockV1` when `--reorg` is enabled (default: `http://localhost:8545`) |
| `--report <FORMAT>` | Report destinations, repeatable (see [Reporters](#reporters)) |
| `-m, --metadata <K=V>` | Metadata key=value pairs for the report, repeatable |
| `--metrics-url <URL or NODE:URL,...>` | Prometheus endpoint(s) to scrape during the run (see [Metrics Scraping](#metrics-scraping)) |
| `--scrape-interval-ms <N>` | Scrape interval in milliseconds (default: 500) |
| `--metrics-align <TIMESTAMP>` | Align exported metric timestamps to a benchmark-start Unix timestamp, in seconds or milliseconds |

For `send-blocks`, aggregate run rates use benchmark wall-clock duration. Per-block timestamps remain the original chain timestamps from the input. With `--reorg`, canonical block stats remain canonical-only, but the wall-clock duration includes synthetic fork block build/submission work.

**Required RPC methods:** `reth_newPayload`, `reth_forkchoiceUpdated` (reth custom Engine API). Big-block inputs require a `reth-bb` compatible node. `--reorg` additionally requires `testing_buildBlockV1` on the regular HTTP RPC endpoint, for example from a node started with `--http --http.api eth,testing`. `--rpc-url` and `--local-rpc-url` are accepted as backwards-compatible aliases for `--rpc`.

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
| `prometheus:<url>` | Push samples via Prometheus remote write (`/api/v1/write`) |

### Metrics Scraping

All bench commands support built-in Prometheus metrics scraping via `--metrics-url`. When enabled, one or more background scrapers periodically fetch node `/metrics` endpoints and include all samples in the JSON report.

```bash
# Scrape node metrics alongside the benchmark
bench send -i txs.ndjson --metrics-url http://127.0.0.1:9001/metrics --report json:report.json

# Custom scrape interval (default: 500ms)
bench send -i txs.ndjson --metrics-url http://127.0.0.1:9001/metrics --scrape-interval-ms 200

# Scrape multiple nodes and tag each scraped sample with node=<label>
bench send -i txs.ndjson --metrics-url a:http://node-a:9001/metrics,b:http://node-b:9001/metrics
```

For a single endpoint, pass the URL directly. For multiple endpoints, every comma-separated entry must use `node_label:URL`; the label is added to scraped Prometheus samples as `node=<node_label>`.

Internal txgen metrics are snapshotted on the same interval and included alongside node metrics. In `send` mode: `txgen_transactions_sent_total`, `txgen_transactions_success_total`, etc. In `send-blocks` mode: `txgen_blocks_sent_total`, `txgen_blocks_success_total`, `txgen_blocks_failed_total`.

Metadata key=value pairs (`-m key=value`) are applied as labels to all samples, useful for tagging runs with build SHAs, profiles, or experiment IDs.

Use `--metrics-align <TIMESTAMP>` to shift exported sample timestamps while preserving each sample's offset within the run. The timestamp is treated as benchmark start time and may be Unix seconds or milliseconds; after conversion to milliseconds, exported sample `unix_ms` values become `TIMESTAMP + offset_ms`.

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

### Prometheus Reporting

The Prometheus reporter forwards every sample in the unified time series (internal `txgen_*` counters plus scraped node Prometheus metrics) to a Prometheus-compatible endpoint via the `/api/v1/write` endpoint. The same endpoint works with VictoriaMetrics, Cortex, Thanos, etc. Samples are sent in Prometheus Remote Write format with their original `unix_ms` timestamps, so backfilling at the end of the run does not lose fidelity.

```bash
bench send -i txs.ndjson \
  --metrics-url http://127.0.0.1:9001/metrics \
  --report prometheus:http://prometheus:8428 \
  -m scenario=tip20-10k \
  -m platform=tempo \
  -m git-sha=abc123 \
  -m git-ref=main
```

Each `-m key=value` pair from `--metadata` is encoded as a label on every sample (perfect for `run_id`, `scenario`, `git_sha`, `platform`, …). Label keys are sanitized to match the Prometheus identifier rules (`-`, `.`, etc. become `_`).

Connection knobs are read from environment variables to keep secrets off the command line:

| Env var | Purpose |
|---------|---------|
| `PROMETHEUS_BEARER_TOKEN` | Sent as `Authorization: Bearer …` (e.g. for VM Cloud / vmauth) |
| `PROMETHEUS_USER` / `PROMETHEUS_PASSWORD` | HTTP basic auth credentials |
| `PROMETHEUS_TENANT_ID` | Cluster VM `accountID` query parameter |
| `PROMETHEUS_BATCH_SIZE` | Samples per HTTP request (default: `10000`) |
| `PROMETHEUS_TIMEOUT_SECS` | Per-request HTTP timeout in seconds (default: `60`) |

Example with auth:

```bash
PROMETHEUS_BEARER_TOKEN=$(cat ~/.prometheus-token) \
bench send -i txs.ndjson \
  --metrics-url http://127.0.0.1:9001/metrics \
  --report prometheus:https://prometheus.example.com \
  -m scenario=tip20-10k -m run_id=$(uuidgen)
```

## Output Format

Transactions are output as NDJSON with scheduling keys split by release policy:

```json
{
  "phase": "workload",
  "id": "transfer",
  "raw": "0x02f86c01...",
  "submission_keys": [
    "0x3c44cdddb6a900fa2b585dd299e03d12fa4293bc"
  ],
  "inclusion_keys": []
}
```

| Field | Description |
|-------|-------------|
| `phase` | `setup` or `workload`; missing phase is treated as `workload` by `bench` |
| `id` | Optional diagnostic identifier |
| `raw` | RLP-encoded signed transaction (EIP-2718 envelope) |
| `submission_keys` | 20-byte ordering constraints released after RPC submission succeeds |
| `inclusion_keys` | 20-byte ordering constraints released after the transaction is included in a block |

**Scheduling rule:** Transactions that share any scheduling key must be sent sequentially until that key's release condition is met. Normal transactions carry their natural nonce-lane key in `submission_keys`, because the chain enforces nonce order after admission. Sequence steps on the same nonce lane are submitted back-to-back; txgen only adds synthetic `inclusion_keys` at cross-lane sequence boundaries where nonce order cannot guarantee execution order.

## Workload Specification

Workload specs are YAML files that define accounts, transaction templates, optional transaction sequences, and mix ratios.

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

# ABI/deployment artifacts for contract calls and setup deploys
artifacts:
  erc20: "./abis/ERC20.json"
  token:
    abi: "./out/Token.sol/Token.json"
    bytecode: "./out/Token.sol/Token.json"

# Optional deterministic setup transactions emitted before workload txs
setup:
  steps:
    - id: token
      deploy:
        type: eip1559
        artifact: token
        from:
          pool: deployer
          select: { index: 0 }
        gas_limit: 5000000
        constructor_args: ["Benchmark Token", "BENCH"]

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

# Optional multi-transaction sequences
sequences:
  two_transfers:
    bindings:
      sender:
        account:
          pool: users
          select: random
      recipient:
        account:
          pool: users
          select: random
      amount:
        u256:
          uniform: [1, 100]
    steps:
      - template: transfer
        with:
          from: { var: sender.ref }
          to: { var: recipient.address }
          value: { var: amount }
      - template: transfer
        with:
          from: { var: sender.ref }
          to: { var: recipient.address }
          value: { var: amount }

# Weighted mix for generation
mix:
  - template: transfer
    weight: 90
  - sequence: two_transfers
    weight: 10
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

# Random address
to: random

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

### Setup Transactions

Use `setup.steps` for deterministic transactions that prepare the chain before the measured workload, such as contract deployments and mint/configuration calls. `txgen` emits all setup transactions first with `phase: "setup"`; workload transactions are emitted afterwards with `phase: "workload"`.

`bench send` treats the first workload transaction as a setup barrier: it waits for all setup transactions to be included, waits for the txpool to drain, resets benchmark timing/metrics, and only then sends workload transactions. Use `bench send --skip-setup` to ignore setup transactions when the target chain is already prepared.

```yaml
artifacts:
  token:
    abi: ./out/Token.sol/Token.json
    bytecode: ./out/Token.sol/Token.json

setup:
  steps:
    - id: token
      bindings:
        deployer:
          account: { pool: deployer, select: { index: 0 } }
      deploy:
        type: eip1559
        artifact: token
        from: { var: deployer.ref }
        gas_limit: 5000000
        constructor_args: ["Benchmark Token", "BENCH"]

    - id: mint_user0
      bindings:
        deployer:
          account: { pool: deployer, select: { index: 0 } }
        user:
          account: { pool: users, select: { index: 0 } }
      tx:
        type: eip1559
        from: { var: deployer.ref }
        gas_limit: 100000
        call:
          to: { var: setup.token.address }
          abi: token
          function: mint
          args:
            - { var: user.address }
            - 1000000000000000000000

templates:
  transfer_token:
    type: eip1559
    from: { pool: users, select: random }
    gas_limit: 65000
    call:
      to: { var: setup.token.address }
      abi: token
      function: transfer
      args:
        - { pool: users, select: random }
        - 1000000000000000000
```

Setup outputs are deterministic only. Supported references are:

| Reference | Description |
|-----------|-------------|
| `setup.<id>.address` | Contract address for deployment steps |
| `setup.<id>.tx_hash` | Signed transaction hash |
| `setup.<id>.sender` | Sender address |
| `setup.<id>.nonce` | Sender nonce used by the setup transaction |

Setup does not support using contract call return values, logs, or receipt fields as later inputs.

### Transaction Sequences

Use `sequences` when one generated workload item should emit multiple ordered transactions. Bindings are resolved once per sequence instance and can be referenced from any step with `{ var: ... }`.

```yaml
sequences:
  approve_then_transfer_from:
    bindings:
      owner:
        account: { pool: users, select: random }
      spender:
        account: { pool: users, select: random }
      recipient:
        account: { pool: users, select: random }
      amount:
        u256: { uniform: [1, 1000] }

    steps:
      - template: erc20_approve
        with:
          from: { var: owner.ref }
          call:
            args:
              - { var: spender.address }
              - { var: amount }

      - template: erc20_transfer_from
        with:
          from: { var: spender.ref }
          call:
            args:
              - { var: owner.address }
              - { var: recipient.address }
              - { var: amount }
```

Supported binding references:

| Binding | References |
|---------|------------|
| `account` | `<name>.ref`, `<name>.address` |
| `address` | `<name>` |
| `bytes32` | `<name>` |
| `abi_hash` | `<name>` |
| `u256` | `<name>` |
| `u64` | `<name>` |
| `string` | `<name>` |

Sequences also expose `{ var: chain_id }` as the top-level workload `chain_id` unless a binding named `chain_id` is defined.

Hash bindings can reference other sequence bindings and are resolved once per sequence instance. For deterministic IDs that contracts compute with `keccak256(abi.encode(...))` (such as Tempo MPP channel IDs), use `abi_hash`:

```yaml
bindings:
  payer:
    account: { pool: users, select: random }
  ephemeral_recipient:
    address: random
  salt:
    bytes32: { random_bytes: 32 }
  channel_id:
    abi_hash:
      types: [address, address, address, bytes32, address, address, uint256]
      values:
        - { var: payer.address } # payer
        - { var: payer.address } # payee
        - "0x20c0000000000000000000000000000000000000" # token
        - { var: salt }
        - "0x0000000000000000000000000000000000000000" # authorizedSigner
        - "0x0000000000000000000000000000000000000000" # channel contract
        - { var: chain_id }
```

Each emitted sequence step gets its natural nonce-lane key as a `submission_key`. Txgen then groups adjacent sequence steps by lane. Steps in the same lane rely on nonce order and can be submitted back-to-back. When a sequence crosses lanes, txgen adds a synthetic boundary key: the previous run releases it after inclusion, and the next run requires it as a `submission_key`. This preserves cross-lane ordering without receipt-gating same-lane nonce chains.

When set, `txgen generate -n` counts emitted transactions, not sequence instances. txgen never emits a partial sequence; if no remaining mix entry fits the remaining transaction budget or `--duration` elapses before the next workload item starts, generation stops early.

See `examples/sequence.yaml` for a small syntax example, `examples/tip20-sequence.yaml` for a Tempo TIP20 `approve -> transferFrom` sequence whose second transaction depends on the first, and `examples/tip20-mpp.yaml` for TIP20 transfers mixed with deterministic MPP channel `open -> close` sequences.

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
| `eth_getTransactionReceipt` | `bench send` (setup and inclusion waits) |
| `eth_getBlockByNumber` | `bench send` (per-block stats collection) |
| `debug_getRawBlock` | `txgen extract`, `txgen-ethereum extract-big-blocks` |
| `eth_getBlockAccessListByBlockNumber` | `txgen extract --bal`, `txgen-ethereum extract-big-blocks --bal` |
| `reth_newPayload` | `bench send-blocks` |
| `reth_forkchoiceUpdated` | `bench send-blocks` |
| `testing_buildBlockV1` | `bench send-blocks --reorg` |
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
