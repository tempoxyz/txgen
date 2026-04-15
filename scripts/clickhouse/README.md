# ClickHouse Schema

ClickHouse tables for storing benchmark results from txgen.

## Tables

### `txgen_runs`

One row per benchmark execution. Stores run identity, git info, and flexible key-value config/metadata maps.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | UUID | Unique run identifier |
| `started_at` | DateTime64(3) | Run start time |
| `finished_at` | DateTime64(3) | Run end time |
| `scenario_name` | String | Benchmark scenario (e.g. `tip20-10k`) |
| `platform` | String | `ethereum` or `tempo` |
| `mode` | String | `send`, `replay`, or `send-blocks` |
| `git_sha` | String | Node commit SHA |
| `git_ref` | String | Node branch/ref |
| `config` | Map(String, String) | Run config (tps, max_concurrent, etc.) |
| `metadata` | Map(String, String) | CI context (PR, workflow URL, etc.) |

### `txgen_blocks`

One row per block in a run. Contains block facts (tx count, gas) and timing data. Each block has a correlation window used to associate scraped metrics.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | UUID | Parent run |
| `block_index` | UInt32 | 0-based position in the run |
| `block_number` | UInt64 | Chain block number |
| `chain_timestamp` | UInt64? | Block timestamp (unix seconds) |
| `window_kind` | String | `precise` (replay) or `observed` (send) |
| `window_start_offset_ms` | UInt64 | Correlation window start (ms from run start) |
| `window_end_offset_ms` | UInt64 | Correlation window end (ms from run start) |
| `tx_count` | UInt32 | Transactions in the block |
| `gas_used` | UInt64 | Gas consumed |
| `gas_limit` | UInt64 | Block gas limit |
| `block_time_ms` | UInt64? | Inter-block time (send mode) |
| `new_payload_ms` | UInt64? | newPayload latency (engine mode) |
| `fcu_ms` | UInt64? | forkchoiceUpdated latency (engine mode) |
| `total_latency_ms` | UInt64? | Total execution latency (engine mode) |
| `payload_status` | String? | newPayload status (VALID, SYNCING, etc.) |
| `server_latency_us` | UInt64? | reth server-side execution time |
| `persistence_wait_us` | UInt64? | reth persistence wait time |
| `execution_cache_wait_us` | UInt64? | reth execution cache wait time |
| `sparse_trie_wait_us` | UInt64? | reth sparse trie wait time |

### `txgen_block_metrics`

One row per (block, metric, labels). Stores aggregated Prometheus samples correlated to each block's time window. No schema changes needed when adding new metrics.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | UUID | Parent run |
| `block_index` | UInt32 | Block position |
| `metric_name` | String | e.g. `reth_jemalloc_resident_bytes` |
| `labels_json` | String | Canonical JSON of Prometheus labels |
| `source` | String | `prometheus`, `txgen`, or `derived` |
| `sample_count` | UInt16 | Samples in the window |
| `first_value` | Float64 | First sample value |
| `last_value` | Float64 | Last sample value |
| `min_value` | Float64 | Minimum |
| `max_value` | Float64 | Maximum |
| `avg_value` | Float64 | Average |
| `delta_value` | Float64? | last - first (for counters) |

## Setup

Each table is in its own numbered SQL file (`001_*.sql`, `002_*.sql`, `003_*.sql`). Apply them all:

```bash
# Local (no auth)
./scripts/clickhouse/apply.sh http://localhost:8123

# ClickHouse Cloud
./scripts/clickhouse/apply.sh https://host.clickhouse.cloud:8443 default password
```

## Usage

Enable the ClickHouse reporter with `--report clickhouse:<url>` and the required metadata:

```bash
bench send -i txs.ndjson \
  --metrics-url http://127.0.0.1:9001/metrics \
  --report clickhouse:https://host:8443 \
  -m scenario=tip20-10k \
  -m platform=tempo \
  -m git-sha=abc123 \
  -m git-ref=main
```

### Authentication

Credentials and database are configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `CLICKHOUSE_USER` | ClickHouse user | *(none)* |
| `CLICKHOUSE_PASSWORD` | ClickHouse password | *(none)* |
| `CLICKHOUSE_DATABASE` | Database name | `default` |

```bash
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=secret
export CLICKHOUSE_DATABASE=benchmarks
```

Additional metadata is stored in the `metadata` map column:

```bash
-m pr-number=42 \
-m github-run-url=https://github.com/... \
-m grafana-url=https://grafana.example.com/...
```

Config-like keys (`tps`, `max_concurrent`, `chain_id`, `scrape_interval_ms`) are stored in the `config` map column.

## Example Queries

### Per-block execution profile

```sql
SELECT
    block_number, tx_count, gas_used, total_latency_ms,
    round(gas_used / (total_latency_ms / 1000.0) / 1e9, 3) AS ggas_per_sec
FROM txgen_blocks
WHERE run_id = '{run_id}'
ORDER BY block_index;
```

### Run summary (computed from blocks)

```sql
SELECT
    count() AS blocks,
    sum(tx_count) AS total_txs,
    sum(gas_used) AS total_gas,
    round(sum(gas_used) / (sum(total_latency_ms) / 1000.0) / 1e6, 2) AS mgas_per_sec,
    quantile(0.50)(total_latency_ms) AS p50_ms,
    quantile(0.95)(total_latency_ms) AS p95_ms,
    quantile(0.99)(total_latency_ms) AS p99_ms
FROM txgen_blocks
WHERE run_id = '{run_id}';
```

### Compare two runs (block-by-block)

```sql
SELECT
    f.block_number,
    f.total_latency_ms AS feature_ms,
    b.total_latency_ms AS baseline_ms,
    round(100.0 * (f.total_latency_ms - b.total_latency_ms) / b.total_latency_ms, 2) AS pct_change
FROM txgen_blocks f
JOIN txgen_blocks b ON f.block_number = b.block_number
WHERE f.run_id = '{feature_run}' AND b.run_id = '{baseline_run}'
ORDER BY f.block_index;
```

### Payment lane fill per block

```sql
SELECT
    b.block_number,
    m_payment.last_value AS payment_gas_used,
    m_limit.last_value AS payment_gas_limit,
    round(m_payment.last_value / m_limit.last_value * 100, 1) AS fill_pct
FROM txgen_blocks b
LEFT JOIN txgen_block_metrics m_payment
    ON b.run_id = m_payment.run_id AND b.block_index = m_payment.block_index
    AND m_payment.metric_name = 'reth_tempo_payload_builder_payment_gas_used_last'
LEFT JOIN txgen_block_metrics m_limit
    ON b.run_id = m_limit.run_id AND b.block_index = m_limit.block_index
    AND m_limit.metric_name = 'reth_tempo_payload_builder_payment_gas_limit_last'
WHERE b.run_id = '{run_id}'
ORDER BY b.block_index;
```

### Memory usage per block

```sql
SELECT
    b.block_number,
    round(m.max_value / 1e9, 2) AS peak_resident_gb
FROM txgen_blocks b
JOIN txgen_block_metrics m
    ON b.run_id = m.run_id AND b.block_index = m.block_index
    AND m.metric_name = 'reth_jemalloc_resident'
WHERE b.run_id = '{run_id}'
ORDER BY b.block_index;
```

### Filter by platform

```sql
SELECT run_id, scenario_name, started_at, git_ref
FROM txgen_runs
WHERE platform = 'tempo'
ORDER BY started_at DESC
LIMIT 10;
```
