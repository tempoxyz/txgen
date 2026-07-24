-- Add scenario DAG identity and latency fields while retaining the original
-- latency columns for readers of report schema version 1.
ALTER TABLE txgen_scenario_runs
    ADD COLUMN IF NOT EXISTS client_observed_e2e_latency_samples UInt64 AFTER maximum_in_flight,
    ADD COLUMN IF NOT EXISTS client_observed_e2e_latency_min_ms Float64 AFTER client_observed_e2e_latency_samples,
    ADD COLUMN IF NOT EXISTS client_observed_e2e_latency_mean_ms Float64 AFTER client_observed_e2e_latency_min_ms,
    ADD COLUMN IF NOT EXISTS client_observed_e2e_latency_p50_ms Float64 AFTER client_observed_e2e_latency_mean_ms,
    ADD COLUMN IF NOT EXISTS client_observed_e2e_latency_p95_ms Float64 AFTER client_observed_e2e_latency_p50_ms,
    ADD COLUMN IF NOT EXISTS client_observed_e2e_latency_p99_ms Float64 AFTER client_observed_e2e_latency_p95_ms,
    ADD COLUMN IF NOT EXISTS client_observed_e2e_latency_max_ms Float64 AFTER client_observed_e2e_latency_p99_ms,
    ADD COLUMN IF NOT EXISTS observed_critical_path_latency_samples UInt64 AFTER client_observed_e2e_latency_max_ms,
    ADD COLUMN IF NOT EXISTS observed_critical_path_latency_min_ms Float64 AFTER observed_critical_path_latency_samples,
    ADD COLUMN IF NOT EXISTS observed_critical_path_latency_mean_ms Float64 AFTER observed_critical_path_latency_min_ms,
    ADD COLUMN IF NOT EXISTS observed_critical_path_latency_p50_ms Float64 AFTER observed_critical_path_latency_mean_ms,
    ADD COLUMN IF NOT EXISTS observed_critical_path_latency_p95_ms Float64 AFTER observed_critical_path_latency_p50_ms,
    ADD COLUMN IF NOT EXISTS observed_critical_path_latency_p99_ms Float64 AFTER observed_critical_path_latency_p95_ms,
    ADD COLUMN IF NOT EXISTS observed_critical_path_latency_max_ms Float64 AFTER observed_critical_path_latency_p99_ms;

ALTER TABLE txgen_scenario_steps
    ADD COLUMN IF NOT EXISTS step_id String AFTER step_index,
    ADD COLUMN IF NOT EXISTS depends_on Array(String) AFTER step_name,
    ADD COLUMN IF NOT EXISTS command_latency_samples UInt64 AFTER failed,
    ADD COLUMN IF NOT EXISTS command_latency_min_ms Float64 AFTER command_latency_samples,
    ADD COLUMN IF NOT EXISTS command_latency_mean_ms Float64 AFTER command_latency_min_ms,
    ADD COLUMN IF NOT EXISTS command_latency_p50_ms Float64 AFTER command_latency_mean_ms,
    ADD COLUMN IF NOT EXISTS command_latency_p95_ms Float64 AFTER command_latency_p50_ms,
    ADD COLUMN IF NOT EXISTS command_latency_p99_ms Float64 AFTER command_latency_p95_ms,
    ADD COLUMN IF NOT EXISTS command_latency_max_ms Float64 AFTER command_latency_p99_ms;

-- One aggregate row for each dependency edge or intra-step protocol relation.
-- Signed Float64 values retain cross-chain clock skew instead of clamping it.
CREATE TABLE IF NOT EXISTS txgen_scenario_causal_edges (
    run_id                                      UUID,
    relation                                    LowCardinality(String), -- dependency or submit_to_observation
    source_step_id                              String,
    destination_step_id                         String,
    source_milestone                            LowCardinality(String),
    destination_milestone                       LowCardinality(String),
    observed_latency_samples                    UInt64,
    observed_latency_min_ms                     Float64,
    observed_latency_mean_ms                    Float64,
    observed_latency_p50_ms                     Float64,
    observed_latency_p95_ms                     Float64,
    observed_latency_p99_ms                     Float64,
    observed_latency_max_ms                     Float64,
    chain_timestamp_delta_samples               UInt64,
    chain_timestamp_delta_min_ms                Float64,
    chain_timestamp_delta_mean_ms               Float64,
    chain_timestamp_delta_p50_ms                Float64,
    chain_timestamp_delta_p95_ms                Float64,
    chain_timestamp_delta_p99_ms                Float64,
    chain_timestamp_delta_max_ms                Float64,
    destination_observation_lag_samples         UInt64,
    destination_observation_lag_min_ms          Float64,
    destination_observation_lag_mean_ms         Float64,
    destination_observation_lag_p50_ms          Float64,
    destination_observation_lag_p95_ms          Float64,
    destination_observation_lag_p99_ms          Float64,
    destination_observation_lag_max_ms           Float64
)
ENGINE = MergeTree
ORDER BY (
    run_id,
    relation,
    source_step_id,
    destination_step_id,
    source_milestone,
    destination_milestone
);

-- Present only when instance sampling is enabled for a scenario run.
CREATE TABLE IF NOT EXISTS txgen_scenario_instance_traces (
    run_id                           UUID,
    scenario_instance                UInt64,
    started_at_unix_ms               UInt64,
    finished_at_unix_ms              UInt64,
    outcome                          LowCardinality(String),
    client_observed_e2e_latency_ms    Float64,
    observed_critical_path_latency_ms Float64,
    critical_path_step_ids            Array(String)
)
ENGINE = MergeTree
ORDER BY (run_id, scenario_instance);

-- One secret-free command-timing row for every step in a sampled instance,
-- including steps such as checkpoints that do not emit a protocol milestone.
CREATE TABLE IF NOT EXISTS txgen_scenario_trace_steps (
    run_id                 UUID,
    scenario_instance      UInt64,
    step_id                String,
    step_index             UInt64,
    step_name              String,
    kind                   LowCardinality(String),
    depends_on             Array(String),
    success                Bool,
    started_offset_ms      UInt64,
    finished_offset_ms     UInt64,
    command_latency_ms     Float64
)
ENGINE = MergeTree
ORDER BY (run_id, scenario_instance, step_index);

-- Secret-free protocol milestones belonging to sampled instances. Grouped
-- receipt events share one row and use aligned event_names/log_indices arrays.
CREATE TABLE IF NOT EXISTS txgen_scenario_trace_milestones (
    run_id                 UUID,
    scenario_instance      UInt64,
    step_id                String,
    step_index             UInt64,
    milestone_index        UInt16,
    chain                  LowCardinality(String),
    kind                   LowCardinality(String),
    run_offset_ms           UInt64,
    wall_time_unix_ms       UInt64,
    submitted_at_unix_ms    Nullable(UInt64),
    accepted_at_unix_ms     Nullable(UInt64),
    first_observed_at_unix_ms Nullable(UInt64),
    transaction_hash        Nullable(String),
    block_number           Nullable(UInt64),
    block_hash             Nullable(String),
    transaction_index      Nullable(UInt64),
    log_index              Nullable(UInt64),
    canonical_block_timestamp_ms Nullable(UInt64),
    confirmation_depth     UInt64,
    event_names            Array(String),
    log_indices            Array(UInt64)
)
ENGINE = MergeTree
ORDER BY (run_id, scenario_instance, step_index, milestone_index);
