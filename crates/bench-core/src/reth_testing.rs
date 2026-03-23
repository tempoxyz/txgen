//! Types and client for reth's `testing_buildBlockV1` RPC.
//!
//! This RPC method instructs reth to build a block from the given payload
//! attributes and pre-supplied transactions. It returns the full execution
//! payload envelope.
//!
//! These types mirror the definitions in reth's `reth-rpc-api` crate but are
//! kept standalone to avoid pulling in the full reth dependency tree.

use alloy_primitives::{B256, Bytes};
use alloy_rpc_types_engine::{ExecutionPayloadEnvelopeV4, PayloadAttributes};
use serde::{Deserialize, Serialize};

/// Request body for `testing_buildBlockV1`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestingBuildBlockRequestV1 {
    /// Hash of the parent block to build on top of.
    pub parent_block_hash: B256,

    /// Payload attributes controlling the block (timestamp, fee recipient, etc.).
    pub payload_attributes: PayloadAttributes,

    /// Pre-supplied transactions to include in the block (RLP-encoded).
    pub transactions: Vec<Bytes>,

    /// Optional extra data to embed in the block header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra_data: Option<Bytes>,
}

/// Response from `testing_buildBlockV1`.
///
/// This is the standard [`ExecutionPayloadEnvelopeV4`] — reth returns a full
/// payload envelope containing the execution payload, blobs bundle, and
/// override flag.
pub type TestingBuildBlockResponse = ExecutionPayloadEnvelopeV4;

/// Thin client wrapper for `testing_buildBlockV1`.
///
/// Calls the `testing_buildBlockV1` RPC method on a connected reth node.
///
/// # Example
///
/// ```ignore
/// let response = build_block_v1(&rpc_client, request).await?;
/// ```
pub async fn build_block_v1(
    client: &alloy_rpc_client::RpcClient,
    request: TestingBuildBlockRequestV1,
) -> eyre::Result<TestingBuildBlockResponse> {
    let response: TestingBuildBlockResponse = client
        .request("testing_buildBlockV1", (request,))
        .await
        .map_err(|e| eyre::eyre!("testing_buildBlockV1 failed: {e}"))?;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;

    #[test]
    fn test_request_roundtrip() {
        let request = TestingBuildBlockRequestV1 {
            parent_block_hash: B256::ZERO,
            payload_attributes: PayloadAttributes {
                timestamp: 1_000_000,
                prev_randao: B256::ZERO,
                suggested_fee_recipient: Default::default(),
                withdrawals: Some(vec![]),
                parent_beacon_block_root: Some(B256::ZERO),
            },
            transactions: vec![Bytes::from(vec![0xf8, 0x70, 0x01])],
            extra_data: None,
        };

        let json = serde_json::to_string(&request).expect("serialize");
        let deserialized: TestingBuildBlockRequestV1 =
            serde_json::from_str(&json).expect("deserialize");

        assert_eq!(deserialized.parent_block_hash, request.parent_block_hash);
        assert_eq!(deserialized.transactions.len(), 1);
        assert_eq!(deserialized.extra_data, None);
    }

    #[test]
    fn test_request_with_extra_data() {
        let request = TestingBuildBlockRequestV1 {
            parent_block_hash: B256::ZERO,
            payload_attributes: PayloadAttributes {
                timestamp: 1_000_000,
                prev_randao: B256::ZERO,
                suggested_fee_recipient: Default::default(),
                withdrawals: Some(vec![]),
                parent_beacon_block_root: Some(B256::ZERO),
            },
            transactions: vec![],
            extra_data: Some(Bytes::from(vec![0xde, 0xad])),
        };

        let json = serde_json::to_string(&request).expect("serialize");
        assert!(json.contains("extraData"));

        let deserialized: TestingBuildBlockRequestV1 =
            serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized.extra_data, Some(Bytes::from(vec![0xde, 0xad])));
    }

    #[test]
    fn test_request_camel_case_fields() {
        let request = TestingBuildBlockRequestV1 {
            parent_block_hash: B256::ZERO,
            payload_attributes: PayloadAttributes {
                timestamp: 42,
                prev_randao: B256::ZERO,
                suggested_fee_recipient: Default::default(),
                withdrawals: None,
                parent_beacon_block_root: None,
            },
            transactions: vec![],
            extra_data: None,
        };

        let json = serde_json::to_string(&request).expect("serialize");
        assert!(json.contains("parentBlockHash"));
        assert!(json.contains("payloadAttributes"));
        assert!(!json.contains("parent_block_hash"));
    }
}
