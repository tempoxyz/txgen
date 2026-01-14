use alloy_primitives::{Address, U256};
use serde::Deserialize;
use txgen_core::{AccountRef, CallDef, GenValue};

/// Template for Ethereum transactions.
#[derive(Debug, Clone, Deserialize)]
pub struct EthereumTemplate {
    /// Transaction type: "legacy", "eip2930", "eip1559", "eip4844", "eip7702"
    #[serde(rename = "type")]
    pub tx_type: String,

    /// Sender account reference.
    pub from: AccountRef,

    /// Gas limit.
    pub gas_limit: u64,

    /// Value to transfer.
    #[serde(default)]
    pub value: GenValue<U256>,

    /// Recipient address (None for contract creation).
    #[serde(default)]
    pub to: Option<GenValue<Address>>,

    /// Contract call definition (alternative to raw `to`/`value`/`input`).
    #[serde(default)]
    pub call: Option<CallDef>,

    /// Gas price for legacy/eip2930 transactions.
    #[serde(default)]
    pub gas_price: Option<u128>,

    /// Max fee per gas for EIP-1559+ transactions.
    #[serde(default)]
    pub max_fee_per_gas: Option<u128>,

    /// Max priority fee per gas for EIP-1559+ transactions.
    #[serde(default)]
    pub max_priority_fee_per_gas: Option<u128>,
}
