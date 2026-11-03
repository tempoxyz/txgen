-- Let a run appear on the perf dashboards (perf.tempo.xyz and the internal one)
-- even when it is not a scheduled nightly, such as a manual dispatch someone
-- wants to show. Additive: writers don't set it, so it stays false unless an
-- operator flags a run:
--   ALTER TABLE txgen_runs UPDATE force_display = true WHERE run_id = '<run_id>';
ALTER TABLE txgen_runs
    ADD COLUMN IF NOT EXISTS force_display Bool DEFAULT false
    COMMENT 'Show this run on perf dashboards even when it is not a scheduled nightly run';
