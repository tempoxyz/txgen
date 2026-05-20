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
| `mode` | String | `send` or `send-blocks` |
| `git_sha` | String | Node commit SHA |
| `git_ref` | String | Node branch/ref |
| `config` | Map(String, String) | Run config (tps, max_concurrent, etc.) |
| `metadata` | Map(String, String) | CI context (PR, workflow URL, etc.) |

### `txgen_blocks`

One row per block in a run. Contains factual chain data.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | UUID | Parent run |
| `block_index` | UInt32 | 0-based position in the run |
| `block_number` | UInt64 | Chain block number |
| `chain_timestamp_ms` | UInt64? | Block timestamp (unix milliseconds) |
| `tx_count` | UInt32 | Transactions in the block |
| `gas_used` | UInt64 | Gas consumed |
| `gas_limit` | UInt64 | Block gas limit |
| `block_time_ms` | UInt64? | Inter-block time (ms) |
| `new_payload_ms` | UInt64? | Client-side `reth_newPayload` latency (ms, send-blocks only) |
| `forkchoice_updated_ms` | UInt64? | Client-side `reth_forkchoiceUpdated` latency (ms, send-blocks only) |
| `new_payload_server_latency_us` | UInt64? | Server-side execution latency (µs, send-blocks only) |
| `persistence_wait_us` | UInt64? | Server-side persistence wait (µs, send-blocks only) |
| `execution_cache_wait_us` | UInt64? | Server-side execution cache wait (µs, send-blocks only) |
| `sparse_trie_wait_us` | UInt64? | Server-side sparse trie wait (µs, send-blocks only) |

### `txgen_metric_samples`

Point-in-time metric snapshots. One row per scraped metric value. Metrics are stored as a time series with no block attribution.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | UUID | Parent run |
| `offset_ms` | UInt64 | Monotonic ms since run start |
| `unix_ms` | UInt64 | Wall-clock ms |
| `metric_name` | String | e.g. `reth_jemalloc_resident` |
| `labels_json` | String | Canonical JSON of Prometheus labels |
| `source` | String | `prometheus` or `txgen` |
| `value` | Float64 | Metric value |

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

The reporter generates `txgen_runs.run_id` automatically and adds it to report metadata as `clickhouse_run_id`, so JSON reports expose the ClickHouse run id. To choose that UUID before the insert, pass `-m clickhouse_run_id=<uuid>`.

### Authentication

Credentials and database are configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `CLICKHOUSE_USER` | ClickHouse user | *(none)* |
| `CLICKHOUSE_PASSWORD` | ClickHouse password | *(none)* |
| `CLICKHOUSE_DATABASE` | Database name | `default` |
| `CLICKHOUSE_SAMPLE_BATCH_SIZE` | Metric sample rows per insert | `50000` |

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

### Per-block stats

```sql
SELECT block_number, tx_count, gas_used, gas_limit, block_time_ms
FROM txgen_blocks
WHERE run_id = '{run_id}'
ORDER BY block_index;
```

### Run summary

```sql
SELECT
    count() AS blocks,
    sum(tx_count) AS total_txs,
    sum(gas_used) AS total_gas,
    quantile(0.50)(block_time_ms) AS block_time_p50_ms,
    quantile(0.99)(block_time_ms) AS block_time_p99_ms
FROM txgen_blocks
WHERE run_id = '{run_id}';
```

### Metric time series

```sql
SELECT offset_ms, value
FROM txgen_metric_samples
WHERE run_id = '{run_id}'
  AND metric_name = 'reth_jemalloc_resident'
ORDER BY offset_ms;
```

### Memory usage over time

```sql
SELECT
    offset_ms / 1000 AS seconds,
    round(value / 1e9, 2) AS resident_gb
FROM txgen_metric_samples
WHERE run_id = '{run_id}'
  AND metric_name = 'reth_jemalloc_resident'
ORDER BY offset_ms;
```

### Filter by platform

```sql
SELECT run_id, scenario_name, started_at, git_ref
FROM txgen_runs
WHERE platform = 'tempo'
ORDER BY started_at DESC
LIMIT 10;
```
