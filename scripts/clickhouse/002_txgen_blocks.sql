CREATE TABLE IF NOT EXISTS txgen_blocks (
    run_id                UUID,                     -- parent run
    block_index           UInt32,                   -- 0-based position within the run
    block_number          UInt64,                   -- chain block number
    chain_timestamp       Nullable(UInt64),         -- block timestamp (unix seconds)
    tx_count              UInt32,                   -- transactions in the block
    gas_used              UInt64,                   -- gas consumed by the block
    gas_limit             UInt64,                   -- block gas limit
    block_time_ms         Nullable(UInt64)          -- inter-block time (ms)
)
ENGINE = MergeTree
ORDER BY (run_id, block_index);
