//! Types and provider extension for reth's custom Engine API.
//!
//! reth exposes two custom RPC methods for benchmarking:
//! - `reth_newPayload` — accepts either standard `ExecutionData` or raw RLP-encoded block bytes
//!   (`BlockRlp`), and returns [`RethPayloadStatus`] with server-side timing information.
//! - `reth_forkchoiceUpdated` — simplified forkchoice update with no payload attributes.
//!
//! These types mirror the definitions in reth's `reth-rpc-api` crate but are kept standalone
//! to avoid pulling in the full reth dependency tree.
//!
//! The [`RethApi`] trait provides a provider extension (like alloy's `DebugApi`) that is
//! automatically available on any `Provider`.

use alloy_network::Network;
use alloy_primitives::Bytes;
use alloy_provider::Provider;
use alloy_rpc_types_engine::{ForkchoiceState, ForkchoiceUpdated, PayloadStatus};
use alloy_transport::TransportResult;
use serde::{Deserialize, Serialize};

/// Input for `reth_newPayload`.
///
/// Accepts either standard execution data or raw RLP-encoded block bytes.
/// Uses `#[serde(untagged)]` to try deserialization in order.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RethNewPayloadInput {
    /// Raw RLP-encoded block bytes.
    BlockRlp(Bytes),
}

/// Response from `reth_newPayload` with server-side timing.
///
/// Extends the standard [`PayloadStatus`] with microsecond-precision timing
/// fields measured on the reth server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RethPayloadStatus {
    /// Standard payload status (flattened into the JSON object).
    #[serde(flatten)]
    pub status: PayloadStatus,

    /// Server-side execution latency in microseconds.
    #[serde(default)]
    pub latency_us: u64,

    /// Time spent waiting for persistence to complete, in microseconds.
    /// `None` when persistence wait was not requested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persistence_wait_us: Option<u64>,

    /// Time spent waiting for the execution cache lock, in microseconds.
    /// `None` when cache wait was not requested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_cache_wait_us: Option<u64>,

    /// Time spent waiting for the sparse trie lock, in microseconds.
    /// `None` when cache wait was not requested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sparse_trie_wait_us: Option<u64>,
}

/// Response from `reth_forkchoiceUpdated`.
///
/// This is the standard [`ForkchoiceUpdated`] type — reth's custom endpoint
/// does not extend the response, only simplifies the request (no payload
/// attributes).
pub type RethForkchoiceUpdated = ForkchoiceUpdated;

/// Provider extension for reth's custom Engine API methods.
///
/// Automatically implemented for any type that implements [`Provider`].
///
/// # Example
///
/// ```ignore
/// use bench_core::RethApi;
///
/// let status = provider.reth_new_payload(input).await?;
/// let fcu = provider.reth_forkchoice_updated(state).await?;
/// ```
#[async_trait::async_trait]
pub trait RethApi<N: Network>: Send + Sync {
    /// Submit a new payload via `reth_newPayload`.
    async fn reth_new_payload(
        &self,
        input: RethNewPayloadInput,
    ) -> TransportResult<RethPayloadStatus>;

    /// Submit a forkchoice update via `reth_forkchoiceUpdated`.
    async fn reth_forkchoice_updated(
        &self,
        forkchoice_state: ForkchoiceState,
    ) -> TransportResult<RethForkchoiceUpdated>;
}

#[async_trait::async_trait]
impl<N, P> RethApi<N> for P
where
    N: Network,
    P: Provider<N>,
{
    async fn reth_new_payload(
        &self,
        input: RethNewPayloadInput,
    ) -> TransportResult<RethPayloadStatus> {
        self.client().request("reth_newPayload", (input,)).await
    }

    async fn reth_forkchoice_updated(
        &self,
        forkchoice_state: ForkchoiceState,
    ) -> TransportResult<RethForkchoiceUpdated> {
        self.client()
            .request("reth_forkchoiceUpdated", (forkchoice_state,))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_rlp_roundtrip() {
        let input = RethNewPayloadInput::BlockRlp(Bytes::from(vec![0xf8, 0x70, 0x01]));
        let json = serde_json::to_string(&input).unwrap();
        let deserialized: RethNewPayloadInput = serde_json::from_str(&json).unwrap();

        match deserialized {
            RethNewPayloadInput::BlockRlp(bytes) => {
                assert_eq!(bytes.as_ref(), &[0xf8, 0x70, 0x01]);
            }
        }
    }

    #[test]
    fn test_reth_payload_status_deserialize_with_timing() {
        let json = r#"{
            "status": "VALID",
            "latestValidHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "latency_us": 12345,
            "persistence_wait_us": 5000,
            "execution_cache_wait_us": 2000,
            "sparse_trie_wait_us": 1000
        }"#;

        let status: RethPayloadStatus = serde_json::from_str(json).unwrap();
        assert_eq!(status.latency_us, 12345);
        assert_eq!(status.persistence_wait_us, Some(5000));
        assert_eq!(status.execution_cache_wait_us, Some(2000));
        assert_eq!(status.sparse_trie_wait_us, Some(1000));
    }

    #[test]
    fn test_reth_payload_status_deserialize_without_optional_timing() {
        let json = r#"{
            "status": "VALID",
            "latestValidHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "latency_us": 500
        }"#;

        let status: RethPayloadStatus = serde_json::from_str(json).unwrap();
        assert_eq!(status.latency_us, 500);
        assert_eq!(status.persistence_wait_us, None);
        assert_eq!(status.execution_cache_wait_us, None);
        assert_eq!(status.sparse_trie_wait_us, None);
    }

    #[test]
    fn test_reth_payload_status_deserialize_missing_latency() {
        let json = r#"{
            "status": "SYNCING"
        }"#;

        let status: RethPayloadStatus = serde_json::from_str(json).unwrap();
        assert_eq!(status.latency_us, 0);
        assert_eq!(status.persistence_wait_us, None);
    }
}
