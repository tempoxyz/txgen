-- Add engine API timing columns to txgen_blocks (send-blocks mode).
-- Nullable because these are only populated in send-blocks mode.
ALTER TABLE txgen_blocks
    ADD COLUMN IF NOT EXISTS new_payload_ms        Nullable(UInt64) AFTER block_time_ms,
    ADD COLUMN IF NOT EXISTS forkchoice_updated_ms  Nullable(UInt64) AFTER new_payload_ms,
    ADD COLUMN IF NOT EXISTS new_payload_server_latency_us Nullable(UInt64) AFTER forkchoice_updated_ms,
    ADD COLUMN IF NOT EXISTS persistence_wait_us   Nullable(UInt64) AFTER new_payload_server_latency_us,
    ADD COLUMN IF NOT EXISTS execution_cache_wait_us Nullable(UInt64) AFTER persistence_wait_us,
    ADD COLUMN IF NOT EXISTS sparse_trie_wait_us   Nullable(UInt64) AFTER execution_cache_wait_us;
