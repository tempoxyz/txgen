CREATE TABLE IF NOT EXISTS txgen_scenario_steps (
    run_id                 UUID,                      -- parent row in txgen_runs
    step_index              UInt64,                    -- zero-based expanded step position
    step_name               String,                    -- expanded save name or deterministic fallback
    chain                   LowCardinality(String),    -- scenario chain alias
    kind                    LowCardinality(String),    -- checkpoint, submit, wait_receipt, or wait_log
    source_file             Nullable(String),          -- fragment declaration file; null for inline steps
    fragment                Nullable(String),          -- fragment name; null for inline steps
    instance_alias          Nullable(String),          -- fragment-use alias; null for inline steps
    local_step_name         Nullable(String),          -- fragment-local step name; null for inline steps
    local_step_index        Nullable(UInt64),          -- fragment-local step position; null for inline steps
    success                 UInt64,                    -- successful executions of this step
    failed                  UInt64,                    -- failed executions of this step
    latency_samples         UInt64,                    -- attempted-step latency sample count
    latency_min_ms          Float64,
    latency_mean_ms         Float64,
    latency_p50_ms          Float64,
    latency_p95_ms          Float64,
    latency_p99_ms          Float64,
    latency_max_ms          Float64
)
ENGINE = MergeTree
ORDER BY (run_id, step_index);
