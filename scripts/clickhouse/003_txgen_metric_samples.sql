CREATE TABLE IF NOT EXISTS txgen_metric_samples (
    run_id                UUID,                     -- parent run
    offset_ms             UInt64,                   -- monotonic ms since run start
    unix_ms               UInt64,                   -- wall-clock ms
    metric_name           LowCardinality(String),   -- metric name
    labels_json           String,                   -- canonical JSON of label map
    source                LowCardinality(String),   -- 'prometheus' or 'txgen'
    value                 Float64                   -- metric value
)
ENGINE = MergeTree
ORDER BY (run_id, metric_name, labels_json, offset_ms);
