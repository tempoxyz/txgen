# txgen Cookbook

This cookbook collects common txgen and bench workflows. It assumes the release binaries are built:

```bash
cargo build --release -p txgen-ethereum -p txgen-tempo -p bench-cli

# Optional: put them on PATH, or use target/release/<binary> in the examples below.
export PATH="$PWD/target/release:$PATH"
```

Use `txgen-ethereum` for standard Ethereum transactions and `txgen-tempo` for Tempo workloads. `bench` is chain-agnostic for sending raw transactions and can also replay blocks through the reth Engine API.

## Table of contents

- [Generate synthetic transactions offline](#generate-synthetic-transactions-offline)
- [List and fund workload accounts](#list-and-fund-workload-accounts)
- [Send to destination-only address pools](#send-to-destination-only-address-pools)
- [Send a fixed number of synthetic transactions](#send-a-fixed-number-of-synthetic-transactions)
- [Run a timed stress test](#run-a-timed-stress-test)
- [Replay historical blocks through Engine API](#replay-historical-blocks-through-engine-api)
- [Replay blocks with Block Access Lists](#replay-blocks-with-block-access-lists)
- [Pace block replay](#pace-block-replay)
- [Build and replay big-block payloads](#build-and-replay-big-block-payloads)
- [Exercise reorg paths](#exercise-reorg-paths)
- [Use setup transactions and skip them later](#use-setup-transactions-and-skip-them-later)
- [Generate Tempo keychain TIP20 workloads](#generate-tempo-keychain-tip20-workloads)
- [Generate dependent transaction sequences](#generate-dependent-transaction-sequences)
- [Generate random fixed-size values](#generate-random-fixed-size-values)
- [Use Tempo parallel and expiring nonces](#use-tempo-parallel-and-expiring-nonces)
- [Scrape metrics and write reports](#scrape-metrics-and-write-reports)
- [Read a report](#read-a-report)
- [Troubleshooting throughput](#troubleshooting-throughput)

## Generate synthetic transactions offline

Generate signed NDJSON transactions from a workload spec without touching a node:

```bash
txgen-ethereum generate \
  --spec examples/simple.yaml \
  --count 1000 \
  --seed 42 \
  --output txs.ndjson
```

For Tempo:

```bash
txgen-tempo generate \
  --spec examples/tempo.yaml \
  --count 1000 \
  --seed 42 \
  --output tempo-txs.ndjson
```

Notes:
- `--seed` makes account/template selection reproducible.
- Without `--rpc`, txgen starts every nonce lane at nonce `0`.
- With `--rpc`, txgen fetches current chain nonces before generating.

## List and fund workload accounts

List every signer account address referenced by a workload spec. Destination-only `address_pools` are omitted:

```bash
txgen-tempo addresses --spec examples/bench-spec.yaml
```

Shell-friendly output is useful for faucet scripts:

```bash
txgen-tempo addresses --spec examples/bench-spec.yaml --format shell \
  | tr ' ' '\n' \
  | xargs -P 50 -I{} curl -sf -X POST \
      -H 'Content-Type: application/json' \
      -d '{"jsonrpc":"2.0","method":"tempo_fundAddress","params":["{}"],"id":1}' \
      http://localhost:8545 -o /dev/null
```

## Send to destination-only address pools

Use `address_pools` when you want workload transactions to target known existing users without making those users available as signers. Pools can be derived from a recipient mnemonic, fast deterministic state-bloat-style addresses, or literal addresses. Mnemonic-backed pools are derived lazily and cached on first use, so very large ranges do not add startup cost:

```yaml
accounts:
  senders:
    mnemonic: "${SENDER_MNEMONIC}"
    range: [0, 100]

address_pools:
  existing_users:
    mnemonic: "${RECIPIENT_MNEMONIC}"
    range: [0, 10000]
  state_bloat_users:
    fast:
      seed: "${STATE_BLOAT_SEED}"
      range: [10000, 1000000]
  known_recipients:
    addresses:
      - "0x0000000000000000000000000000000000000001"
      - "0x0000000000000000000000000000000000000002"

templates:
  transfer_to_existing_user:
    type: eip1559
    from: { pool: senders, select: random }
    to:
      address_pool:
        pool: existing_users
        select: random
    value: 1
    gas_limit: 21000
```

For ERC20-style calls, use the same generator in the ABI `address` argument:

```yaml
templates:
  erc20_transfer_to_existing_user:
    type: eip1559
    from: { pool: senders, select: random }
    gas_limit: 65000
    call:
      to: "0xToken..."
      abi: erc20
      function: transfer
      args:
        - address_pool:
            pool: existing_users
            select: random
        - 1000000
```

`address_pools` are only valid in address-valued positions like `to`, sequence `address` bindings, and ABI `address` arguments. They cannot be used for `from` or `sponsor`, and `txgen addresses` will not print them.

## Send a fixed number of synthetic transactions

Generate and send in one pipeline. This avoids writing a large NDJSON file:

```bash
txgen-tempo generate \
  --spec examples/bench-spec.yaml \
  --count 200000 \
  --seed 99 \
  --rpc http://localhost:8545 \
| bench send \
  --rpc-url http://localhost:8545 \
  --tps 5000 \
  --max-concurrent 500 \
  --drain-timeout 300 \
  --report console \
  --report json:report.json
```

Use `--tps 0` for unlimited send rate:

```bash
bench send --input txs.ndjson --rpc-url http://localhost:8545 --tps 0
```

By default, failed transaction submissions are retried forever. Use `--retries N` to cap retries, or `--retries 0` to disable retries entirely.

## Run a timed stress test

Generate workload transactions for a wall-clock duration instead of a count:

```bash
txgen-tempo generate \
  --spec examples/bench-spec.yaml \
  --duration 5m \
  --seed 99 \
  --rpc http://localhost:8545 \
| bench send \
  --rpc-url http://localhost:8545 \
  --tps 10000 \
  --max-concurrent 1000 \
  --report json:timed-report.json
```

Setup transactions are emitted before the timer starts. If both `--count` and `--duration` are provided, generation stops at whichever limit is reached first.

## Replay historical blocks through Engine API

Extract raw RLP blocks from an archive/debug RPC and submit them to a reth-compatible Engine API:

```bash
txgen-ethereum extract \
  --rpc http://archive:8545 \
  --from 20000000 \
  --to 20000100 \
| bench send-blocks \
  --engine http://localhost:8551 \
  --jwt-secret /path/to/jwt.hex \
  --wait-for-persistence never \
  --report console \
  --report json:replay-report.json
```

Required source RPC method: `debug_getRawBlock`.

## Replay blocks with Block Access Lists

For Amsterdam/EIP-7928 payloads, include block access lists (BALs) in the extracted stream. `txgen` fetches BALs from the source RPC, RLP-encodes them into each raw block line, and `bench send-blocks` forwards them to `reth_newPayload` alongside the block RLP:

```bash
txgen-ethereum extract \
  --rpc http://archive:8545 \
  --from 20000000 \
  --to 20000100 \
  --bal \
| bench send-blocks \
  --engine http://localhost:8551 \
  --jwt-secret /path/to/jwt.hex \
  --report json:bal-replay-report.json
```

Required source RPC methods: `debug_getRawBlock` and `eth_getBlockAccessListByBlockNumber`.

Big-block extraction supports BALs too. `txgen` fetches each constituent block's BAL, shifts and merges the access indexes the same way `reth-bench generate-big-block --bal` does, and writes the merged RLP bytes to `merged_block_access_list`:

```bash
txgen-ethereum extract-big-blocks \
  --rpc http://archive:8545 \
  --from 910020 \
  --count 25 \
  --target-gas 2G \
  --bal \
  --output big-blocks-with-bal.ndjson

bench send-blocks \
  --engine http://localhost:8551 \
  --jwt-secret /path/to/jwt.hex \
  --input big-blocks-with-bal.ndjson \
  --report json:big-block-bal-report.json
```

## Pace block replay

Add a minimum wall-clock interval between block submissions:

```bash
bench send-blocks \
  --engine http://localhost:8551 \
  --jwt-secret /path/to/jwt.hex \
  --input blocks.ndjson \
  --wait-time 500ms
```

`--wait-time` is measured around `reth_newPayload` + `reth_forkchoiceUpdated`; if processing takes longer than the interval, no extra sleep is added.

## Build and replay big-block payloads

Create reth-bb-compatible synthetic big blocks by merging transactions from source blocks until a target gas amount is reached:

```bash
txgen-ethereum extract-big-blocks \
  --rpc http://archive:8545 \
  --from 910020 \
  --count 25 \
  --target-gas 2G \
  --output big-blocks.ndjson

bench send-blocks \
  --engine http://localhost:8551 \
  --jwt-secret /path/to/jwt.hex \
  --input big-blocks.ndjson \
  --report json:big-block-report.json
```

`--target-gas` accepts bare units or `K`, `M`, `G` suffixes.

## Exercise reorg paths

`bench send-blocks --reorg DEPTH` first builds `DEPTH` synthetic side-fork blocks with `testing_buildBlockV1`, then resolves the side chain with the corresponding `DEPTH` canonical blocks. Use `--reorg-gap N` to submit `N` additional canonical blocks before starting the next synthetic side chain. The gap defaults to `0`, and synthetic side chains never overlap.

```bash
txgen-ethereum extract \
  --rpc http://archive:8545 \
  --from 20000000 \
  --to 20000100 \
| bench send-blocks \
  --engine http://localhost:8551 \
  --jwt-secret /path/to/jwt.hex \
  --rpc http://localhost:8545 \
  --reorg 8 \
  --reorg-gap 2 \
  --report json:reorg-report.json
```

Notes:
- Reorg mode only supports raw RLP block input, not big-block input.
- The regular HTTP RPC must expose `testing_buildBlockV1` (for example, a node started with `--http --http.api eth,testing`).
- Canonical block stats remain canonical-only; run wall-clock time includes synthetic fork work.

## Use setup transactions and skip them later

Specs can include deterministic `setup.steps` for deploys or initialization. `txgen` emits those first with `phase: setup`, and `bench send` waits for setup inclusion and txpool drain before starting measured workload metrics.

```bash
txgen-tempo generate --spec examples/tip20-mpp.yaml --count 10000 --rpc http://localhost:8545 \
| bench send --rpc-url http://localhost:8545 --report json:mpp-report.json
```

If the chain is already prepared, reuse the same generated stream but ignore setup transactions:

```bash
bench send --input txs-with-setup.ndjson --rpc-url http://localhost:8545 --skip-setup
```

## Generate Tempo keychain TIP20 workloads

Use `keychain_authorize_pool` in setup to authorize one deterministic access key per user, then sign measured workload transactions with the paired access key:

```bash
txgen-tempo generate \
  --spec tests/specs/tempo-keychain-tip20.yaml \
  --count 1000 \
  --seed 7 \
  --rpc http://localhost:8545 \
| bench send --rpc-url http://localhost:8545 --report json:keychain-report.json
```

Access keys are signing-only keys derived from the setup step's `access_keys` mnemonic/range. They do not become funded workload accounts and are not included in `txgen-tempo addresses`.

For inline provisioning traffic, use `auth.mode: key_authorization`. Each workload transaction carries a signed secp256k1 `key_authorization`, optional limits, and an optional TIP-1053 witness. Inline access keys can be derived from a separate mnemonic/range under `auth.access_key`; if omitted, txgen uses a public benchmark-only mnemonic starting at index `1000000`:

```bash
txgen-tempo generate \
  --spec tests/specs/tempo-inline-key-authorization-tip20.yaml \
  --count 1000 \
  --seed 7 \
| bench send --rpc-url http://localhost:8545 --report json:inline-key-auth-report.json
```

## Generate dependent transaction sequences

Use `sequences` when a workload item needs multiple ordered transactions, such as `approve -> transferFrom`:

```bash
txgen-tempo generate \
  --spec examples/tip20-sequence.yaml \
  --count 1000 \
  --seed 7 \
| bench send --rpc-url http://localhost:8545 --tps 1000
```

`--count` counts emitted transactions, not sequence instances. Txgen never emits a partial sequence; if the remaining budget cannot fit the selected sequence, generation stops early.

## Generate random fixed-size values

Use `random` when a template, ABI argument, or sequence binding should generate a fresh fixed-size value from the workload RNG. This is useful for fuzz-style traffic where recipients or identifiers do not need to come from a funded account pool.

```yaml
templates:
  random_eth_transfer:
    type: eip1559
    from: { pool: users, select: random }
    to: random
    value: 1
    gas_limit: 21000

  random_token_transfer:
    type: eip1559
    from: { pool: users, select: random }
    gas_limit: 65000
    call:
      to: "0x20c0000000000000000000000000000000000000"
      abi: erc20
      function: transfer
      args:
        - random
        - 1000000
```

Random sequence bindings are resolved once per sequence instance, then reused by every step that references them:

```yaml
sequences:
  transfer_twice_to_random_recipient:
    bindings:
      sender:
        account: { pool: users, select: random }
      recipient:
        address: random
      amount:
        u256: random
      salt:
        bytes32: random
    steps:
      - template: random_eth_transfer
        with:
          from: { var: sender.ref }
          to: { var: recipient }
          value: { var: amount }
      - template: random_eth_transfer
        with:
          from: { var: sender.ref }
          to: { var: recipient }
          value: { var: amount }
```

`random` is supported for fixed-size values such as `address`, `bytes32`, `u64`, `u128`, and `u256`. Use `random_bytes: <len>` for dynamically sized byte payloads.

## Use Tempo parallel and expiring nonces

Tempo supports multiple nonce lanes per sender with `nonce_key`, which improves throughput by avoiding a single per-account nonce bottleneck:

```yaml
nonce_key:
  uniform: [0, 100]
```

Expiring nonce transactions are useful for streamed benchmarks:

```yaml
expiring_nonce: true
valid_for_secs: 25
```

Caveats:
- `valid_for_secs` must be `<= 30`.
- Stream expiring transactions directly into `bench send`; do not pre-generate a large file for later replay because transactions may expire.
- `txgen-tempo generate --rpc` prefetches fixed nonce keys, but not dynamic/generated nonce keys.

## Scrape metrics and write reports

Scrape a node Prometheus endpoint during a transaction send run:

```bash
bench send \
  --input txs.ndjson \
  --rpc-url http://localhost:8545 \
  --metrics-url http://127.0.0.1:9001/metrics \
  --scrape-interval-ms 500 \
  --metadata scenario=tip20-10k \
  --metadata platform=tempo \
  --report console \
  --report json:report.json
```

Push to ClickHouse:

```bash
CLICKHOUSE_USER=default CLICKHOUSE_PASSWORD=secret CLICKHOUSE_DATABASE=benchmarks \
bench send --input txs.ndjson \
  --rpc-url http://localhost:8545 \
  --metrics-url http://127.0.0.1:9001/metrics \
  --report clickhouse:https://host:8443 \
  -m scenario=tip20-10k \
  -m platform=tempo \
  -m git-sha=abc123 \
  -m git-ref=main
```

Push samples via Prometheus remote write:

```bash
PROMETHEUS_BEARER_TOKEN=$(cat ~/.prometheus-token) \
bench send --input txs.ndjson \
  --rpc-url http://localhost:8545 \
  --metrics-url http://127.0.0.1:9001/metrics \
  --report prometheus:https://prometheus.example.com \
  -m scenario=tip20-10k \
  -m run_id=$(uuidgen)
```

## Read a report

Print a JSON report summary:

```bash
bench view report.json
```

## Troubleshooting throughput

The live `bench send` progress line contains:

```text
Sent | OK | Fail | Inflight current/max | Rate actual/target
```

Interpretation:
- `Inflight` near `--max-concurrent`: RPC is concurrency-bound; increase `--max-concurrent` or lower `--tps`.
- `Rate` matches target and `Inflight` is low: rate limiter is the bottleneck as intended.
- `Rate` below target and `Inflight` is low: source is likely bottlenecked; pre-generate to a file or use faster storage/stdin source.
