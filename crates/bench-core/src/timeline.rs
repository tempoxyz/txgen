//! Block timeline markers for metric ↔ block correlation.
//!
//! [`BlockMarker`] records when blocks are observed (send mode) or
//! submitted (replay mode), enabling reporters to aggregate scraped
//! node metrics over per-block time windows.

use serde::{Deserialize, Serialize};

/// A marker recording when a block was observed during `bench send`.
///
/// The `offset_ms` is an approximate arrival time from head-polling.
/// After the run, `collect_block_stats()` enriches with chain data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMarker {
    /// Block number.
    pub number: u64,
    /// Block timestamp from the chain header (if known).
    pub chain_timestamp: Option<u64>,
    /// Monotonic offset (ms) when the block was observed.
    pub offset_ms: u64,
}

/// A marker recording timing details for a replayed block via Engine API.
///
/// Provides precise sub-block timing windows for metric aggregation:
/// - Execution window: `[submit_start, fcu_done]`
/// - Inter-block gap: `[prev.fcu_done, this.submit_start]`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBlockMarker {
    /// Block number.
    pub number: u64,
    /// Block timestamp from the chain header.
    pub chain_timestamp: u64,
    /// Offset (ms) when `reth_newPayload` was called.
    pub submit_start_offset_ms: u64,
    /// Offset (ms) when `reth_newPayload` returned.
    pub new_payload_done_offset_ms: u64,
    /// Offset (ms) when `reth_forkchoiceUpdated` returned.
    pub fcu_done_offset_ms: u64,
}

impl ReplayBlockMarker {
    /// Total execution duration (newPayload + FCU) in milliseconds.
    pub fn execution_ms(&self) -> u64 {
        self.fcu_done_offset_ms
            .saturating_sub(self.submit_start_offset_ms)
    }

    /// newPayload duration in milliseconds.
    pub fn new_payload_ms(&self) -> u64 {
        self.new_payload_done_offset_ms
            .saturating_sub(self.submit_start_offset_ms)
    }

    /// forkchoiceUpdated duration in milliseconds.
    pub fn fcu_ms(&self) -> u64 {
        self.fcu_done_offset_ms
            .saturating_sub(self.new_payload_done_offset_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_marker_serde() {
        let marker = BlockMarker {
            number: 100,
            chain_timestamp: Some(1700000000),
            offset_ms: 500,
        };

        let json = serde_json::to_string(&marker).unwrap();
        let parsed: BlockMarker = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.number, 100);
        assert_eq!(parsed.chain_timestamp, Some(1700000000));
        assert_eq!(parsed.offset_ms, 500);
    }

    #[test]
    fn replay_marker_durations() {
        let marker = ReplayBlockMarker {
            number: 200,
            chain_timestamp: 1700000000,
            submit_start_offset_ms: 1000,
            new_payload_done_offset_ms: 1150,
            fcu_done_offset_ms: 1200,
        };

        assert_eq!(marker.execution_ms(), 200);
        assert_eq!(marker.new_payload_ms(), 150);
        assert_eq!(marker.fcu_ms(), 50);
    }

    #[test]
    fn replay_marker_serde() {
        let marker = ReplayBlockMarker {
            number: 300,
            chain_timestamp: 1700000012,
            submit_start_offset_ms: 5000,
            new_payload_done_offset_ms: 5100,
            fcu_done_offset_ms: 5120,
        };

        let json = serde_json::to_string(&marker).unwrap();
        let parsed: ReplayBlockMarker = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.number, 300);
        assert_eq!(parsed.fcu_done_offset_ms, 5120);
    }
}
