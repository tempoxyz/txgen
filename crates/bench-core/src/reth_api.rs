//! Types and provider extension for reth's custom Engine API.
//!
//! reth exposes two custom RPC methods for benchmarking:
//! - `reth_newPayload` — accepts standard `ExecutionData`, reth-bb [`BigBlockData`], or raw
//!   RLP-encoded block bytes plus optional block access list bytes (`BlockRlp`), and returns
//!   [`RethPayloadStatus`] with server-side timing information.
//! - `reth_forkchoiceUpdated` — simplified forkchoice update with no payload attributes.
//!
//! These types mirror the definitions in reth's `reth-rpc-api` crate but are kept standalone
//! to avoid pulling in the full reth dependency tree.
//!
//! The [`RethApi`] trait provides a provider extension (like alloy's `DebugApi`) that is
//! automatically available on any `Provider`.

use alloy_network::Network;
use alloy_primitives::{Bytes, B256};
use alloy_provider::Provider;
use alloy_rpc_types_engine::{ExecutionData, ForkchoiceState, ForkchoiceUpdated, PayloadStatus};
use alloy_transport::TransportResult;
use serde::{ser::SerializeStruct, Deserialize, Deserializer, Serialize, Serializer};

/// Input for `reth_newPayload`.
///
/// Accepts standard execution data, reth-bb big-block data, or raw RLP-encoded block bytes.
/// Uses `#[serde(untagged)]` to try deserialization in order.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum RethNewPayloadInput {
    /// Standard execution data (payload + sidecar).
    ExecutionData(Box<ExecutionData>),
    /// reth-bb big-block data containing all constituent payloads.
    BigBlockData(Box<BigBlockData<ExecutionData>>),
    /// Raw RLP-encoded block bytes and optional RLP-encoded block access list bytes.
    BlockRlp {
        /// Raw RLP-encoded block bytes.
        block: Bytes,
        /// RLP-encoded block access list bytes.
        bal: Option<Bytes>,
    },
}

impl Serialize for RethNewPayloadInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::ExecutionData(data) => data.serialize(serializer),
            Self::BigBlockData(data) => data.serialize(serializer),
            Self::BlockRlp { block, bal: None } => block.serialize(serializer),
            Self::BlockRlp { block, bal: Some(bal) } => {
                let mut state = serializer.serialize_struct("BlockRlp", 2)?;
                state.serialize_field("block", block)?;
                state.serialize_field("bal", bal)?;
                state.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for RethNewPayloadInput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum RethNewPayloadInputSerde {
            ExecutionData(Box<ExecutionData>),
            BigBlockData(Box<BigBlockData<ExecutionData>>),
            BlockRlp {
                block: Bytes,
                #[serde(default)]
                bal: Option<Bytes>,
            },
            LegacyBlockRlp(Bytes),
        }

        Ok(match RethNewPayloadInputSerde::deserialize(deserializer)? {
            RethNewPayloadInputSerde::ExecutionData(data) => Self::ExecutionData(data),
            RethNewPayloadInputSerde::BigBlockData(data) => Self::BigBlockData(data),
            RethNewPayloadInputSerde::BlockRlp { block, bal } => Self::BlockRlp { block, bal },
            RethNewPayloadInputSerde::LegacyBlockRlp(block) => Self::BlockRlp { block, bal: None },
        })
    }
}

/// Big-block payload data for reth-bb.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BigBlockData<T> {
    /// Original execution payloads that form this big block, in execution order.
    pub env_switches: Vec<T>,
    /// Block number → real block hash for blocks covered by previous big blocks in a sequence.
    pub prior_block_hashes: Vec<(u64, B256)>,
    /// Synthetic block number assigned to this big block.
    pub block_number: u64,
    /// RLP-encoded merged block access list for this big block, if present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merged_block_access_list: Option<Bytes>,
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

/// Default persistence threshold matching reth's `DEFAULT_PERSISTENCE_THRESHOLD`.
pub const DEFAULT_PERSISTENCE_THRESHOLD: u64 = 2;

/// Policy for when to wait for persistence during `reth_newPayload`.
#[derive(Debug, Clone)]
pub enum WaitForPersistence {
    /// Always wait for persistence on every block.
    Always,
    /// Never wait for persistence.
    Never,
    /// Wait for persistence every N blocks.
    EveryN(u64),
}

impl WaitForPersistence {
    /// Returns the `wait_for_persistence` flag for a given block index (0-based).
    pub fn should_wait(&self, block_index: u64) -> Option<bool> {
        match self {
            Self::Always => Some(true),
            Self::Never => Some(false),
            Self::EveryN(n) => {
                if *n == 0 {
                    return Some(false);
                }
                Some((block_index + 1).is_multiple_of(*n))
            }
        }
    }
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
/// let status = provider.reth_new_payload(input, None).await?;
/// let fcu = provider.reth_forkchoice_updated(state).await?;
/// ```
#[async_trait::async_trait]
pub trait RethApi<N: Network>: Send + Sync {
    /// Submit a new payload via `reth_newPayload`.
    ///
    /// `wait_for_persistence` controls whether the server blocks until
    /// in-flight persistence completes before processing.
    async fn reth_new_payload(
        &self,
        input: RethNewPayloadInput,
        wait_for_persistence: Option<bool>,
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
        wait_for_persistence: Option<bool>,
    ) -> TransportResult<RethPayloadStatus> {
        self.client().request("reth_newPayload", (input, wait_for_persistence, None::<bool>)).await
    }

    async fn reth_forkchoice_updated(
        &self,
        forkchoice_state: ForkchoiceState,
    ) -> TransportResult<RethForkchoiceUpdated> {
        self.client().request("reth_forkchoiceUpdated", (forkchoice_state,)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_rlp_roundtrip() {
        let input = RethNewPayloadInput::BlockRlp {
            block: Bytes::from(vec![0xf8, 0x70, 0x01]),
            bal: Some(Bytes::from(vec![0xc0])),
        };
        let json = serde_json::to_string(&input).expect("block RLP input should serialize");
        let deserialized: RethNewPayloadInput =
            serde_json::from_str(&json).expect("serialized block RLP input should deserialize");

        match deserialized {
            RethNewPayloadInput::BlockRlp { block, bal } => {
                assert_eq!(block.as_ref(), &[0xf8, 0x70, 0x01]);
                assert_eq!(bal.as_ref().map(Bytes::as_ref), Some(&[0xc0][..]));
            }
            RethNewPayloadInput::ExecutionData(_) | RethNewPayloadInput::BigBlockData(_) => {
                panic!("expected BlockRlp variant")
            }
        }
    }

    #[test]
    fn test_block_rlp_without_bal_serializes_legacy_bytes() {
        let input =
            RethNewPayloadInput::BlockRlp { block: Bytes::from(vec![0xf8, 0x70, 0x01]), bal: None };

        let json = serde_json::to_string(&input)
            .expect("block RLP without BAL should serialize as legacy bytes");
        assert_eq!(json, r#""0xf87001""#);

        let deserialized: RethNewPayloadInput =
            serde_json::from_str(&json).expect("legacy block RLP should deserialize");

        match deserialized {
            RethNewPayloadInput::BlockRlp { block, bal } => {
                assert_eq!(block.as_ref(), &[0xf8, 0x70, 0x01]);
                assert_eq!(bal, None);
            }
            RethNewPayloadInput::ExecutionData(_) | RethNewPayloadInput::BigBlockData(_) => {
                panic!("expected BlockRlp variant")
            }
        }
    }

    #[test]
    fn test_block_rlp_deserializes_object_without_bal() {
        let json = r#"{"block":"0xf87001"}"#;
        let input: RethNewPayloadInput =
            serde_json::from_str(json).expect("block RLP object without BAL should deserialize");

        match input {
            RethNewPayloadInput::BlockRlp { block, bal } => {
                assert_eq!(block.as_ref(), &[0xf8, 0x70, 0x01]);
                assert_eq!(bal, None);
            }
            RethNewPayloadInput::ExecutionData(_) | RethNewPayloadInput::BigBlockData(_) => {
                panic!("expected BlockRlp variant")
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

        let status: RethPayloadStatus =
            serde_json::from_str(json).expect("valid status with timing should deserialize");
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

        let status: RethPayloadStatus = serde_json::from_str(json)
            .expect("valid status without optional timing should deserialize");
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

        let status: RethPayloadStatus =
            serde_json::from_str(json).expect("status without latency should deserialize");
        assert_eq!(status.latency_us, 0);
        assert_eq!(status.persistence_wait_us, None);
    }
}
