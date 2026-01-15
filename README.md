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
```

Or build from source:

```bash
cargo build --release
```

## Usage

```bash
# Generate 1000 Ethereum transactions
txgen generate -s workload.yaml -c ethereum -n 1000

# Generate Tempo transactions with reproducible seed
txgen generate -s workload.yaml -c tempo -n 1000 --seed 42

# Output to file
txgen generate -s workload.yaml -c ethereum -n 1000 -o transactions.ndjson
```

### Options

| Flag | Description |
|------|-------------|
| `-s, --spec <PATH>` | Workload specification file (YAML) |
| `-c, --chain <CHAIN>` | Chain plugin: `ethereum`, `tempo` |
| `-n, --count <N>` | Number of transactions to generate |
| `-o, --output <PATH>` | Output file (default: stdout) |
| `--seed <SEED>` | RNG seed for reproducibility |

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

## Examples

See the `examples/` directory:

- `simple.yaml` - Basic Ethereum transfers
- `tempo.yaml` - Tempo transactions with parallel nonces

```bash
# Run the simple example
txgen generate -s examples/simple.yaml -c ethereum -n 10 --seed 42

# Run the Tempo example
txgen generate -s examples/tempo.yaml -c tempo -n 10 --seed 42
```

## Metrics Collection

The `bench-core` library provides comprehensive metrics collection for benchmarking:

### Runtime Metrics

`MetricsCollector` tracks real-time statistics during transaction sending:

- **sent/success/failed** - Transaction counts
- **latency** - Per-transaction RPC response times (min, max, mean, p50, p95, p99)
- **elapsed** - Total benchmark duration

```rust
let metrics = MetricsCollector::new();
metrics.start().await;

// ... send transactions ...

let bench_metrics = metrics.finalize().await;
println!("TPS: {:.2}", bench_metrics.tps());
println!("Success rate: {:.1}%", bench_metrics.success_rate());
```

### Block Statistics

Post-run analysis of on-chain block data:

```rust
let block_stats = collect_block_stats(&provider, start_block, end_block).await?;

for block in &block_stats {
    println!("Block {}: {} txs, {} gas used", 
        block.number, block.tx_count, block.gas_used);
}
```

Each `BlockStats` includes:
- Block number, timestamp, tx count, success count
- Gas used/limit
- Block time (delta from previous block)

### Run Summary

Aggregate statistics computed from block data:

```rust
let run_stats = RunStats::from_blocks(&block_stats);

println!("Blocks {}-{}", run_stats.start_block, run_stats.end_block);
println!("Total txs: {}", run_stats.total_txs);
println!("Avg TPS: {:.2}", run_stats.avg_tps);
println!("Block time p50: {}ms", run_stats.block_time_p50_ms);
```

## Architecture

```
txgen/
├── crates/
│   ├── txgen-core/       # Core library: spec parsing, account management, output
│   ├── txgen-ethereum/   # Ethereum plugin: legacy, eip2930, eip1559
│   ├── txgen-tempo/      # Tempo plugin: 0x76 + delegates to ethereum
│   ├── txgen-cli/        # CLI binary
│   ├── bench-core/       # Benchmarking: metrics, sender, reporters
│   └── bench-cli/        # Bench CLI binary
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
