//! Block timeline markers for metric ↔ block correlation.
//!
//! [`BlockMarker`] records when blocks are observed (send mode) or
//! submitted (replay mode), enabling reporters to aggregate scraped
//! node metrics over per-block time windows.

use serde::{Deserialize, Serialize};

/// A marker recording when a block was observed or submitted.
///
/// In **send mode**, only `offset_ms` is set (approximate arrival time
/// from head-polling).
///
/// In **replay/send-blocks mode**, the optional Engine API timing fields
/// provide precise sub-block windows for metric aggregation:
/// - Execution window: `[offset_ms, fcu_done_offset_ms]`
/// - Inter-block gap: `[prev.fcu_done_offset_ms, this.offset_ms]`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMarker {
    /// Block number.
    pub number: u64,
    /// Block timestamp from the chain header (if known).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain_timestamp: Option<u64>,
    /// Monotonic offset (ms) when the block was observed (send mode)
    /// or when `reth_newPayload` was called (replay mode).
    pub offset_ms: u64,
    /// Offset (ms) when `reth_newPayload` returned (replay mode only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_payload_done_offset_ms: Option<u64>,
    /// Offset (ms) when `reth_forkchoiceUpdated` returned (replay mode only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fcu_done_offset_ms: Option<u64>,
}

impl BlockMarker {
    /// Total execution duration (newPayload + FCU) in milliseconds.
    ///
    /// Returns `None` if this is not a replay marker.
    pub fn execution_ms(&self) -> Option<u64> {
        self.fcu_done_offset_ms
            .map(|fcu| fcu.saturating_sub(self.offset_ms))
    }

    /// newPayload duration in milliseconds.
    ///
    /// Returns `None` if this is not a replay marker.
    pub fn new_payload_ms(&self) -> Option<u64> {
        self.new_payload_done_offset_ms
            .map(|np| np.saturating_sub(self.offset_ms))
    }

    /// forkchoiceUpdated duration in milliseconds.
    ///
    /// Returns `None` if this is not a replay marker.
    pub fn fcu_ms(&self) -> Option<u64> {
        match (self.new_payload_done_offset_ms, self.fcu_done_offset_ms) {
            (Some(np), Some(fcu)) => Some(fcu.saturating_sub(np)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_mode_marker_serde() {
        let marker = BlockMarker {
            number: 100,
            chain_timestamp: Some(1700000000),
            offset_ms: 500,
            new_payload_done_offset_ms: None,
            fcu_done_offset_ms: None,
        };

        let json = serde_json::to_string(&marker).unwrap();
        assert!(!json.contains("new_payload_done_offset_ms"));
        assert!(!json.contains("fcu_done_offset_ms"));

        let parsed: BlockMarker = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.number, 100);
        assert_eq!(parsed.chain_timestamp, Some(1700000000));
        assert_eq!(parsed.offset_ms, 500);
        assert!(parsed.new_payload_done_offset_ms.is_none());
        assert!(parsed.fcu_done_offset_ms.is_none());
    }

    #[test]
    fn send_mode_marker_has_no_durations() {
        let marker = BlockMarker {
            number: 100,
            chain_timestamp: None,
            offset_ms: 500,
            new_payload_done_offset_ms: None,
            fcu_done_offset_ms: None,
        };

        assert_eq!(marker.execution_ms(), None);
        assert_eq!(marker.new_payload_ms(), None);
        assert_eq!(marker.fcu_ms(), None);
    }

    #[test]
    fn replay_marker_durations() {
        let marker = BlockMarker {
            number: 200,
            chain_timestamp: Some(1700000000),
            offset_ms: 1000,
            new_payload_done_offset_ms: Some(1150),
            fcu_done_offset_ms: Some(1200),
        };

        assert_eq!(marker.execution_ms(), Some(200));
        assert_eq!(marker.new_payload_ms(), Some(150));
        assert_eq!(marker.fcu_ms(), Some(50));
    }

    #[test]
    fn replay_marker_serde() {
        let marker = BlockMarker {
            number: 300,
            chain_timestamp: Some(1700000012),
            offset_ms: 5000,
            new_payload_done_offset_ms: Some(5100),
            fcu_done_offset_ms: Some(5120),
        };

        let json = serde_json::to_string(&marker).unwrap();
        assert!(json.contains("new_payload_done_offset_ms"));
        assert!(json.contains("fcu_done_offset_ms"));

        let parsed: BlockMarker = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.number, 300);
        assert_eq!(parsed.fcu_done_offset_ms, Some(5120));
    }
}
