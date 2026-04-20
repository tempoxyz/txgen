use alloy_primitives::{Address, U256};
use serde::Deserialize;
use txgen_core::{AccountRef, CallDef, GenValue};

/// Tempo transaction type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TempoTxType {
    Legacy,
    Eip2930,
    Eip1559,
    Tempo,
}

/// Template for Tempo transactions.
///
/// Supports all EVM transaction types (legacy, eip2930, eip1559) plus
/// Tempo native 0x76 transactions with parallel nonces, sponsorship,
/// fee tokens, and batched calls.
#[derive(Debug, Clone, Deserialize)]
pub struct TempoTemplate {
    /// Transaction type.
    #[serde(rename = "type")]
    pub tx_type: TempoTxType,

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

    /// Nonce key for parallel nonces (Tempo 0x76).
    /// Key 0 is the protocol nonce, keys 1-N are user nonces for parallelization.
    #[serde(default)]
    pub nonce_key: Option<GenValue<U256>>,

    /// Use Tempo expiring nonce mode (TIP-1009).
    ///
    /// When enabled, txgen sets `nonce_key = U256::MAX` and `nonce = 0`
    /// automatically, and requires either `valid_before` or `valid_for_secs`.
    #[serde(default)]
    pub expiring_nonce: bool,

    /// Fee token address for paying gas in stablecoins (Tempo 0x76).
    #[serde(default)]
    pub fee_token: Option<Address>,

    /// Fee payer/sponsor account (Tempo 0x76).
    #[serde(default)]
    pub sponsor: Option<AccountRef>,

    /// Transaction valid after this timestamp (Tempo 0x76).
    #[serde(default)]
    pub valid_after: Option<u64>,

    /// Transaction valid before this timestamp (Tempo 0x76).
    #[serde(default)]
    pub valid_before: Option<u64>,

    /// Relative expiry window in seconds, resolved at generation time.
    ///
    /// Only used with `expiring_nonce: true`.
    #[serde(default)]
    pub valid_for_secs: Option<u64>,

    /// Batched calls (Tempo 0x76).
    #[serde(default)]
    pub calls: Option<Vec<CallDef>>,
}
