# ClickHouse Schema

ClickHouse tables for storing benchmark results from txgen.

## Tables

### `txgen_runs`

One row per benchmark or scenario execution. Stores run identity, git info, and flexible key-value config/metadata maps.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | UUID | Unique run identifier |
| `started_at` | DateTime64(3) | Run start time |
| `finished_at` | DateTime64(3) | Run end time |
| `scenario_name` | String | Benchmark scenario (e.g. `tip20-10k`) |
| `platform` | String | `ethereum` or `tempo` |
| `mode` | String | `send`, `send-blocks`, or `scenario` |
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

### `txgen_scenario_runs`

One aggregate row per finalized scenario run. Common identity, timestamps, scenario name, platform, revisions, configuration, and user metadata live in `txgen_runs` under the same `run_id`.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | UUID | Parent row in `txgen_runs` |
| `report_version` | UInt32 | Scenario report schema version |
| `requested_journeys` | UInt64? | Configured journey count; null for duration-only runs |
| `started_journeys` | UInt64 | Journeys that actually started |
| `completed_journeys` | UInt64 | Journeys that completed every required step |
| `failed_journeys` | UInt64 | Journeys that failed or timed out |
| `timed_out_journeys` | UInt64 | Failed journeys classified as timeouts |
| `elapsed_ms` | UInt64 | Total run elapsed time |
| `completed_journeys_per_second` | Float64 | Completed journeys divided by elapsed time |
| `maximum_in_flight` | UInt64 | Highest observed active-journey count |
| `latency_samples` | UInt64 | Completed-journey latency observations |
| `latency_min_ms` | Float64 | Minimum completed-journey latency |
| `latency_mean_ms` | Float64 | Mean completed-journey latency |
| `latency_p50_ms` | Float64 | P50 completed-journey latency |
| `latency_p95_ms` | Float64 | P95 completed-journey latency |
| `latency_p99_ms` | Float64 | P99 completed-journey latency |
| `latency_max_ms` | Float64 | Maximum completed-journey latency |

### `txgen_scenario_steps`

One aggregate row per expanded scenario step and run. A step that was never reached has zero successes, failures, and latency samples. Fragment provenance is null for ordinary inline steps.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | UUID | Parent row in `txgen_runs` |
| `step_index` | UInt64 | Zero-based expanded step position |
| `step_name` | String | Expanded save name or deterministic fallback |
| `chain` | String | Scenario chain alias |
| `kind` | String | `checkpoint`, `submit`, `wait_receipt`, or `wait_log` |
| `source_file` | String? | Fragment declaration file |
| `fragment` | String? | Fragment name |
| `instance_alias` | String? | Complete fragment-use alias |
| `local_step_name` | String? | Fragment-local step name |
| `local_step_index` | UInt64? | Zero-based position in the declaring fragment |
| `success` | UInt64 | Successful executions |
| `failed` | UInt64 | Failed executions |
| `latency_samples` | UInt64 | Attempted-step latency observations |
| `latency_min_ms` | Float64 | Minimum step latency |
| `latency_mean_ms` | Float64 | Mean step latency |
| `latency_p50_ms` | Float64 | P50 step latency |
| `latency_p95_ms` | Float64 | P95 step latency |
| `latency_p99_ms` | Float64 | P99 step latency |
| `latency_max_ms` | Float64 | Maximum step latency |

### `txgen_receipt_gas`

One row per confirmed transaction. The row retains receipt-level gas values exactly; it does not attribute gas to internal calls.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | UUID | Parent row in `txgen_runs` |
| `tx_hash` | String | Confirmed transaction hash |
| `sender` | String? | Sender address when known |
| `labels_json` | String | Canonical JSON of workload or scenario labels |
| `scenario_instance` | UInt64? | Scenario instance index when applicable |
| `success` | Bool | Receipt execution status |
| `block_number` | UInt64? | Inclusion block number when supplied by the receipt |
| `block_hash` | String? | Inclusion block hash when supplied by the receipt |
| `gas_used` | UInt256 | Outer transaction gas consumed |
| `effective_gas_price` | UInt256? | Effective gas price when supplied by the receipt |
| `fee_paid` | UInt256? | Exact `gas_used * effective_gas_price` when the fee field is present |

## Setup

The eight numbered migrations are applied in filename order. For a new database, apply the complete set:

```bash
# Local (no auth)
./scripts/clickhouse/apply.sh http://localhost:8123

# ClickHouse Cloud
./scripts/clickhouse/apply.sh https://host.clickhouse.cloud:8443 default password
```

Migration `005_rename_chain_timestamp.sql` is a one-way data conversion and must not be reapplied after it drops the old column. On an existing deployment where migrations through `005` are already present, start at `006`:

```bash
./scripts/clickhouse/apply.sh https://host.clickhouse.cloud:8443 default password 006
```

`apply.sh` sends migrations to the authenticated user's default database. It does not read `CLICKHOUSE_DATABASE`; when the runtime writer uses a non-default database, configure the migration user with the same default database or apply the numbered SQL files to that database through your normal deployment tooling.

Migrations `006_txgen_scenario_runs.sql` and `007_txgen_scenario_steps.sql` must be deployed before any scenario uses `--report clickhouse:<url>`. Migration `008_txgen_receipt_gas.sql` must be deployed before enabling granular receipt publication. In particular, deploy all three migrations before enabling that writer in Zones.

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

Scenario runs accept repeatable destinations. A bare path and `json:<path>` both create a JSON report; `clickhouse:<url>` publishes the same finalized report:

```bash
txgen-tempo scenario run \
  --scenario scenario.yaml \
  --count 100 \
  --report json:scenario-report.json \
  --report clickhouse:https://host:8443 \
  -m git-sha=abc123 \
  -m git-ref=main \
  -m github-run-url=https://github.com/example/actions/runs/123
```

The scenario runner derives `scenario`, `platform`, and `mode=scenario`; those metadata keys are reserved and conflicting user values are rejected. `git-sha` and `git-ref` remain required, and other `-m key=value` pairs are stored in `txgen_runs.metadata`. Every destination receives the same client-generated `run_id`, which is also present in the JSON report.

JSON files are finalized before ClickHouse publication. A ClickHouse failure is returned to the caller but does not remove JSON that has already been written. The writer requests synchronous acknowledgement for separate HTTP inserts of `txgen_scenario_steps`, `txgen_receipt_gas`, `txgen_scenario_runs`, and finally `txgen_runs`; scenario runs never require or insert `txgen_blocks`. The final `txgen_runs` insert is the visibility marker, so complete-run queries must begin with `txgen_runs` and inner-join the detail tables by `run_id`. This hides child rows left by an interrupted publication.

### Authentication

Credentials and database are configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `CLICKHOUSE_USER` | ClickHouse user | *(none)* |
| `CLICKHOUSE_PASSWORD` | ClickHouse password | *(none)* |
| `CLICKHOUSE_DATABASE` | Database name using letters, digits, and underscores | `default` |
| `CLICKHOUSE_SAMPLE_BATCH_SIZE` | Metric sample rows per insert | `50000` |

```bash
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=secret
export CLICKHOUSE_DATABASE=benchmarks
```

These variables configure runtime report writers. Migration authentication is passed as the second and third arguments to `apply.sh`, and migrations target that user's default database as described above.

Additional metadata is stored in the `metadata` map column:

```bash
-m pr-number=42 \
-m github-run-url=https://github.com/... \
-m grafana-url=https://grafana.example.com/...
```

Config-like keys (`tps`, `max_concurrent`, `chain_id`, `scrape_interval_ms`) are stored in the `config` map column.

## Example Queries

### Granular receipt gas

```sql
SELECT
    tx_hash,
    sender,
    labels_json,
    scenario_instance,
    success,
    block_number,
    gas_used,
    effective_gas_price,
    fee_paid
FROM txgen_receipt_gas
WHERE run_id = '{run_id}'
ORDER BY labels_json, scenario_instance, tx_hash;
```

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

### Scenario summary and steps

Begin at `txgen_runs` so only scenario reports whose final visibility marker was inserted are returned:

```sql
SELECT
    r.run_id,
    r.scenario_name,
    s.started_journeys,
    s.completed_journeys,
    s.failed_journeys,
    s.completed_journeys_per_second,
    s.latency_p50_ms,
    s.latency_p95_ms,
    s.latency_p99_ms
FROM txgen_runs AS r
INNER JOIN txgen_scenario_runs AS s USING (run_id)
WHERE r.mode = 'scenario'
ORDER BY r.started_at DESC;
```

```sql
SELECT
    step.step_index,
    step.step_name,
    step.chain,
    step.kind,
    step.success,
    step.failed,
    step.latency_p50_ms,
    step.latency_p95_ms,
    step.fragment,
    step.instance_alias,
    step.local_step_name
FROM txgen_runs AS r
INNER JOIN txgen_scenario_steps AS step USING (run_id)
WHERE r.run_id = '{run_id}'
  AND r.mode = 'scenario'
ORDER BY step.step_index;
```
