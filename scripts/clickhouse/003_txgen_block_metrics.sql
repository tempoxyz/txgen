CREATE TABLE IF NOT EXISTS txgen_block_metrics (
    run_id                UUID,                     -- parent run
    block_index           UInt32,                   -- block position within the run
    block_number          UInt64,                   -- chain block number
    metric_name           LowCardinality(String),   -- metric name (e.g. 'reth_jemalloc_resident')
    labels_json           String,                   -- canonical JSON of Prometheus label map
    source                LowCardinality(String),   -- 'prometheus', 'txgen', or 'derived'
    sample_count          UInt16,                   -- number of samples in the block window
    first_value           Float64,                  -- first sample value in the window
    last_value            Float64,                  -- last sample value in the window
    min_value             Float64,                  -- minimum value in the window
    max_value             Float64,                  -- maximum value in the window
    avg_value             Float64,                  -- average value in the window
    delta_value           Nullable(Float64)         -- last - first (useful for counters)
)
ENGINE = MergeTree
ORDER BY (run_id, metric_name, block_index);
