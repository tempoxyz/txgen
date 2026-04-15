CREATE TABLE IF NOT EXISTS txgen_runs (
    run_id               UUID,                     -- unique identifier for this benchmark run
    started_at           DateTime64(3, 'UTC'),      -- when the run started
    finished_at          DateTime64(3, 'UTC'),      -- when the run finished
    scenario_name        LowCardinality(String),    -- benchmark scenario name (e.g. 'tip20-10k')
    platform             LowCardinality(String),    -- target platform: 'ethereum' or 'tempo'
    mode                 LowCardinality(String),    -- bench subcommand: 'send', 'replay', or 'send-blocks'
    git_sha              String,                    -- node commit SHA being benchmarked
    git_ref              String,                    -- node git branch or ref
    config               Map(String, String),       -- run config (tps, max_concurrent, chain_id, etc.)
    metadata             Map(String, String)        -- CI context (pr_number, github_run_url, etc.)
)
ENGINE = MergeTree
ORDER BY (scenario_name, platform, started_at, run_id);
