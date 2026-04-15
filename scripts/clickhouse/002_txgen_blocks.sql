CREATE TABLE IF NOT EXISTS txgen_blocks (
    run_id                      UUID,                              -- parent run
    block_index                 UInt32,                            -- 0-based position within the run
    block_number                UInt64,                            -- chain block number
    chain_timestamp             Nullable(UInt64),                  -- block timestamp (unix seconds)
    window_kind                 LowCardinality(String),            -- 'precise' (replay) or 'observed' (send)
    window_start_offset_ms      UInt64,                            -- correlation window start (ms from run start)
    window_end_offset_ms        UInt64,                            -- correlation window end (ms from run start)
    tx_count                    UInt32,                            -- transactions in the block
    gas_used                    UInt64,                            -- gas consumed by the block
    gas_limit                   UInt64,                            -- block gas limit
    block_time_ms               Nullable(UInt64),                  -- inter-block time (send mode)
    new_payload_ms              Nullable(UInt64),                  -- newPayload latency (engine mode)
    fcu_ms                      Nullable(UInt64),                  -- forkchoiceUpdated latency (engine mode)
    total_latency_ms            Nullable(UInt64),                  -- total execution latency (engine mode)
    payload_status              LowCardinality(Nullable(String)),  -- newPayload status (VALID, SYNCING, etc.)
    server_latency_us           Nullable(UInt64),                  -- reth server-side execution time (µs)
    persistence_wait_us         Nullable(UInt64),                  -- reth persistence wait time (µs)
    execution_cache_wait_us     Nullable(UInt64),                  -- reth execution cache wait time (µs)
    sparse_trie_wait_us         Nullable(UInt64)                   -- reth sparse trie wait time (µs)
)
ENGINE = MergeTree
ORDER BY (run_id, block_index);
