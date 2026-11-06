-- Add submit-to-inclusion timing to granular receipt rows. Additive: existing
-- rows and readers keep working. Deploy before running a txgen that publishes
-- these columns from `bench send`. Scenario rows omit them.
--   submitted_at_unix_ms: load-generator wall time when submission started
--   block_timestamp_ms:   including block timestamp (Tempo millisecond precision)
--   inclusion_latency_ms: block_timestamp_ms - submitted_at_unix_ms, signed so
--                         clock skew between machines is retained, not clamped
ALTER TABLE txgen_receipt_gas
    ADD COLUMN IF NOT EXISTS submitted_at_unix_ms Nullable(UInt64) AFTER fee_paid,
    ADD COLUMN IF NOT EXISTS block_timestamp_ms Nullable(UInt64) AFTER submitted_at_unix_ms,
    ADD COLUMN IF NOT EXISTS inclusion_latency_ms Nullable(Int64) AFTER block_timestamp_ms;
