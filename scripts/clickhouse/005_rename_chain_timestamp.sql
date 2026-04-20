-- Rename chain_timestamp (seconds) to chain_timestamp_ms (milliseconds).
-- Existing rows are converted from seconds to milliseconds.
ALTER TABLE txgen_blocks
    ADD COLUMN IF NOT EXISTS chain_timestamp_ms Nullable(UInt64) AFTER block_number;

ALTER TABLE txgen_blocks
    UPDATE chain_timestamp_ms = chain_timestamp * 1000
    WHERE chain_timestamp IS NOT NULL AND chain_timestamp_ms IS NULL;

ALTER TABLE txgen_blocks
    DROP COLUMN IF EXISTS chain_timestamp;
