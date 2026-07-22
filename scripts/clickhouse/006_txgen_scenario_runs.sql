CREATE TABLE IF NOT EXISTS txgen_scenario_runs (
    run_id                             UUID,              -- parent row in txgen_runs
    report_version                     UInt32,            -- scenario report schema version
    requested_journeys                 Nullable(UInt64),  -- configured count; null for duration-only runs
    started_journeys                   UInt64,            -- journeys that actually started
    completed_journeys                 UInt64,            -- journeys that completed every step
    failed_journeys                    UInt64,            -- journeys that failed or timed out
    timed_out_journeys                 UInt64,            -- failed journeys classified as timeouts
    elapsed_ms                         UInt64,            -- total scenario-run elapsed time
    completed_journeys_per_second      Float64,           -- completed journeys divided by elapsed time
    maximum_in_flight                  UInt64,            -- highest observed active-journey count
    latency_samples                    UInt64,            -- completed-journey latency sample count
    latency_min_ms                     Float64,
    latency_mean_ms                    Float64,
    latency_p50_ms                     Float64,
    latency_p95_ms                     Float64,
    latency_p99_ms                     Float64,
    latency_max_ms                     Float64
)
ENGINE = MergeTree
ORDER BY run_id;
