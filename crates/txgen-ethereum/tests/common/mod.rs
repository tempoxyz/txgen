//! Mock JSON-RPC helpers shared by the scenario integration tests.
#![allow(dead_code)]

use alloy_primitives::{Address, B256};
use axum::Router;
use serde_json::{json, Value};
use tokio::{net::TcpListener, task::JoinHandle};

/// Serve `app` on an ephemeral local port and return its URL.
pub async fn serve(app: Router) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock RPC");
    let address = listener.local_addr().expect("mock RPC address");
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve mock RPC");
    });
    (format!("http://{address}"), server)
}

pub fn receipt_value(
    transaction_hash: B256,
    from: Address,
    block_number: u64,
    status: bool,
) -> Value {
    json!({
        "status": if status { "0x1" } else { "0x0" },
        "cumulativeGasUsed": "0x5208",
        "logs": [],
        "logsBloom": format!("0x{}", "00".repeat(256)),
        "type": "0x2",
        "transactionHash": transaction_hash,
        "transactionIndex": "0x0",
        "blockHash": B256::repeat_byte(0x55),
        "blockNumber": quantity(block_number),
        "gasUsed": "0x5208",
        "effectiveGasPrice": "0x1",
        "from": from,
        "to": Address::repeat_byte(0x22),
        "contractAddress": null
    })
}

pub fn block_value(number: u64, hash: B256) -> Value {
    json!({
        "hash": hash,
        "parentHash": B256::ZERO,
        "sha3Uncles": B256::ZERO,
        "miner": Address::ZERO,
        "stateRoot": B256::ZERO,
        "transactionsRoot": B256::ZERO,
        "receiptsRoot": B256::ZERO,
        "logsBloom": format!("0x{}", "00".repeat(256)),
        "difficulty": "0x0",
        "number": quantity(number),
        "gasLimit": "0x1c9c380",
        "gasUsed": "0x0",
        "timestamp": "0x0",
        "extraData": "0x",
        "mixHash": B256::ZERO,
        "nonce": "0x0000000000000000",
        "baseFeePerGas": "0x0",
        "transactions": [],
        "uncles": []
    })
}

pub fn quantity(value: u64) -> String {
    format!("0x{value:x}")
}
