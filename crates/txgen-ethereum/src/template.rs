use alloy_primitives::{Address, Bytes, U256};
use serde::Deserialize;
use txgen_core::{AccountRef, CallDef, GenValue};

/// Ethereum transaction type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EthTxType {
    Legacy,
    Eip2930,
    Eip1559,
}

/// Template for Ethereum transactions.
#[derive(Debug, Clone, Deserialize)]
pub struct EthereumTemplate {
    /// Transaction type.
    #[serde(rename = "type")]
    pub tx_type: EthTxType,

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

    /// Raw input data when not using `call`.
    #[serde(default)]
    pub input: Option<GenValue<Bytes>>,

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
