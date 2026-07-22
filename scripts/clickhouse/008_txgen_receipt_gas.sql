CREATE TABLE IF NOT EXISTS txgen_receipt_gas (
    run_id                 UUID,                      -- parent row in txgen_runs
    tx_hash                String,                    -- confirmed transaction hash
    sender                 Nullable(String),          -- sender address when known
    labels_json            String,                    -- canonical JSON of workload/scenario labels
    scenario_instance      Nullable(UInt64),          -- scenario instance index when applicable
    success                Bool,                      -- receipt execution status
    block_number           Nullable(UInt64),          -- inclusion block number when supplied
    block_hash             Nullable(String),          -- inclusion block hash when supplied
    gas_used               UInt256,                   -- outer transaction gas consumed
    effective_gas_price    Nullable(UInt256),         -- receipt effective gas price when supplied
    fee_paid               Nullable(UInt256)          -- gas_used * effective_gas_price when supplied
)
ENGINE = MergeTree
ORDER BY (run_id, labels_json, tx_hash);
