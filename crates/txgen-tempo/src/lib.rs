mod nonce;
mod template;

pub use nonce::{NONCE_PRECOMPILE, TempoNonceProvider, prefetch_parallel_nonces};
pub use txgen_cli::fetch_protocol_nonces;

use alloy_network::TransactionBuilder;
use alloy_primitives::{Address, Bytes, TxKind, U256, keccak256};
use alloy_signer::SignerSync;
use eyre::Result;
use tempo_alloy::TempoNetwork;
use tempo_alloy::rpc::TempoTransactionRequest;
use tempo_primitives::transaction::Call;
use txgen_cli::{GenerateContext, NetworkAdapter, TxRequest};
use txgen_core::BuildContext;

pub use template::TempoTemplate;

/// Tempo network adapter for transaction generation.
///
/// Supports all Ethereum transaction types (legacy, EIP-2930, EIP-1559)
/// plus Tempo native 0x76 transactions.
pub struct TempoAdapter;

impl NetworkAdapter for TempoAdapter {
    type Template = TempoTemplate;
    type Network = TempoNetwork;

    fn build_request(
        &self,
        template: Self::Template,
        ctx: &mut BuildContext<'_>,
    ) -> Result<TxRequest<TempoTransactionRequest>> {
        let selected = ctx.select_signer(&template.from)?;

        let nonce_key: U256 = if let Some(ref nk) = template.nonce_key {
            ctx.resolve_value(nk)?
        } else {
            U256::ZERO
        };

        let is_tempo = template.tx_type == "tempo";
        let scheduling_key = if is_tempo {
            compute_scheduling_key(selected.address, nonce_key)
        } else {
            selected.address.0.0
        };
        let nonce = ctx.next_nonce(scheduling_key);

        let (to, value, input, calls) = resolve_call_data(&template, is_tempo, ctx)?;

        let mut req = TempoTransactionRequest::default();
        req.set_chain_id(ctx.chain_id);
        req.set_nonce(nonce);
        req.set_gas_limit(template.gas_limit);

        match template.tx_type.as_str() {
            "tempo" => {
                req.set_max_fee_per_gas(
                    template.max_fee_per_gas.unwrap_or(ctx.gas.max_fee_per_gas),
                );
                req.set_max_priority_fee_per_gas(
                    template
                        .max_priority_fee_per_gas
                        .unwrap_or(ctx.gas.max_priority_fee_per_gas),
                );

                req.calls = calls;

                if !nonce_key.is_zero() {
                    req.set_nonce_key(nonce_key);
                }
                if let Some(fee_token) = template.fee_token {
                    req.set_fee_token(fee_token);
                }
                if let Some(valid_before) = template.valid_before {
                    req.set_valid_before(valid_before);
                }
                if let Some(valid_after) = template.valid_after {
                    req.set_valid_after(valid_after);
                }

                // Handle sponsor signing: build a temporary TempoTransaction to
                // compute the fee_payer_signature_hash, sign it, then set on the request.
                if let Some(ref sponsor_ref) = template.sponsor {
                    let temp_tx = req
                        .clone()
                        .build_aa()
                        .map_err(|e| eyre::eyre!("failed to build AA tx for sponsor: {e}"))?;

                    let sponsor = ctx.select_signer(sponsor_ref)?;
                    let sponsor_signer = ctx.accounts.get_by_index(&sponsor.pool, sponsor.index)?;
                    let fee_payer_hash = temp_tx.fee_payer_signature_hash(selected.address);
                    let fee_payer_sig = sponsor_signer.sign_hash_sync(&fee_payer_hash)?;
                    req.set_fee_payer_signature(fee_payer_sig);
                }
            }
            "legacy" => {
                req.set_gas_price(template.gas_price.unwrap_or(ctx.gas.max_fee_per_gas));
                if let TxKind::Call(addr) = to {
                    req.set_to(addr);
                }
                req.set_value(value);
                if !input.is_empty() {
                    req.set_input(input);
                }
            }
            "eip2930" => {
                req.set_gas_price(template.gas_price.unwrap_or(ctx.gas.max_fee_per_gas));
                req.set_access_list(Default::default());
                if let TxKind::Call(addr) = to {
                    req.set_to(addr);
                }
                req.set_value(value);
                if !input.is_empty() {
                    req.set_input(input);
                }
            }
            "eip1559" => {
                req.set_max_fee_per_gas(
                    template.max_fee_per_gas.unwrap_or(ctx.gas.max_fee_per_gas),
                );
                req.set_max_priority_fee_per_gas(
                    template
                        .max_priority_fee_per_gas
                        .unwrap_or(ctx.gas.max_priority_fee_per_gas),
                );
                if let TxKind::Call(addr) = to {
                    req.set_to(addr);
                }
                req.set_value(value);
                if !input.is_empty() {
                    req.set_input(input);
                }
            }
            other => eyre::bail!("unsupported transaction type: {}", other),
        }

        Ok(TxRequest {
            request: req,
            signer_pool: selected.pool,
            signer_index: selected.index,
            key: scheduling_key,
        })
    }

    async fn prefetch_nonces(&self, ctx: &mut GenerateContext, rpc: &str) -> Result<()> {
        use alloy_provider::{ProviderBuilder, network::Ethereum};
        use eyre::WrapErr;

        let provider = ProviderBuilder::<_, _, Ethereum>::new()
            .connect_http(rpc.parse().wrap_err("invalid RPC URL")?);

        let (accounts, nonces) = ctx.accounts_and_nonces();
        txgen_cli::fetch_protocol_nonces(accounts, nonces, rpc).await?;

        let (spec, accounts, nonces) = ctx.prefetch_state();
        prefetch_parallel_nonces(&provider, accounts, spec, nonces).await?;

        Ok(())
    }
}

pub(crate) fn compute_scheduling_key(sender: Address, nonce_key: U256) -> [u8; 20] {
    if nonce_key.is_zero() {
        sender.0.0
    } else {
        let mut data = [0u8; 52];
        data[..20].copy_from_slice(sender.as_slice());
        data[20..52].copy_from_slice(&nonce_key.to_be_bytes::<32>());
        let hash = keccak256(data);
        let mut key = [0u8; 20];
        key.copy_from_slice(&hash[..20]);
        key
    }
}

/// Resolve call data from the template into (to, value, input, calls).
///
/// For tempo transactions, the result is returned as `calls`; for EVM types
/// it is returned as `(to, value, input)`.
fn resolve_call_data(
    template: &TempoTemplate,
    is_tempo: bool,
    ctx: &mut BuildContext<'_>,
) -> Result<(TxKind, U256, Bytes, Vec<Call>)> {
    if let Some(ref call_defs) = template.calls {
        let mut calls = Vec::with_capacity(call_defs.len());
        for call_def in call_defs {
            let encoded = ctx.encode_call(call_def)?;
            calls.push(Call {
                to: TxKind::Call(encoded.to),
                value: encoded.value,
                input: encoded.input,
            });
        }
        Ok((TxKind::Create, U256::ZERO, Bytes::new(), calls))
    } else if let Some(ref call_def) = template.call {
        let encoded = ctx.encode_call(call_def)?;
        if is_tempo {
            Ok((
                TxKind::Create,
                U256::ZERO,
                Bytes::new(),
                vec![Call {
                    to: TxKind::Call(encoded.to),
                    value: encoded.value,
                    input: encoded.input,
                }],
            ))
        } else {
            Ok((
                TxKind::Call(encoded.to),
                encoded.value,
                encoded.input,
                Vec::new(),
            ))
        }
    } else {
        let to = ctx.resolve_to(&template.to)?;
        let value: U256 = ctx.resolve_value(&template.value)?;
        if is_tempo {
            Ok((
                TxKind::Create,
                U256::ZERO,
                Bytes::new(),
                vec![Call {
                    to,
                    value,
                    input: Bytes::new(),
                }],
            ))
        } else {
            Ok((to, value, Bytes::new(), Vec::new()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::SignableTransaction;
    use alloy_eips::eip2718::Encodable2718;
    use alloy_network::TxSignerSync;
    use alloy_primitives::Address;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use std::collections::HashMap;
    use tempo_primitives::TEMPO_TX_TYPE_ID;
    use txgen_core::{
        AccountManager, AccountPoolDef, AccountRef, ArtifactManager, GasConfig, GenValue,
        NonceTracker, SelectMode,
    };

    const TEST_MNEMONIC: &str = "test test test test test test test test test test test junk";

    fn sign_and_encode<A: NetworkAdapter>(
        adapter: &A,
        template: A::Template,
        ctx: &mut BuildContext<'_>,
    ) -> Bytes
    where
        <A::Network as alloy_network::Network>::UnsignedTx:
            SignableTransaction<alloy_primitives::Signature>,
        <A::Network as alloy_network::Network>::TxEnvelope: From<alloy_consensus::Signed<<A::Network as alloy_network::Network>::UnsignedTx>>
            + Encodable2718,
    {
        let tx_req = adapter.build_request(template, ctx).unwrap();
        let mut unsigned = tx_req.request.build_unsigned().unwrap();
        let signer = ctx
            .accounts
            .get_by_index(&tx_req.signer_pool, tx_req.signer_index)
            .unwrap();
        let sig = signer.sign_transaction_sync(&mut unsigned).unwrap();
        let signed = unsigned.into_signed(sig);
        let envelope = <A::Network as alloy_network::Network>::TxEnvelope::from(signed);
        Bytes::from(envelope.encoded_2718())
    }

    #[test]
    fn test_build_tempo_transfer() {
        let mut accounts_map = HashMap::new();
        accounts_map.insert(
            "users".to_string(),
            AccountPoolDef {
                mnemonic: TEST_MNEMONIC.to_string(),
                index: None,
                range: Some([0, 10]),
            },
        );
        let accounts = AccountManager::from_spec(&accounts_map).unwrap();
        let artifacts = ArtifactManager::empty();
        let gas = GasConfig::default();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);

        let template = TempoTemplate {
            tx_type: "tempo".to_string(),
            from: AccountRef {
                pool: "users".to_string(),
                select: SelectMode::Index(0),
            },
            gas_limit: 21000,
            value: GenValue::Literal(U256::from(1000)),
            to: Some(GenValue::Literal(Address::ZERO)),
            call: None,
            gas_price: None,
            max_fee_per_gas: Some(1_000_000_000),
            max_priority_fee_per_gas: Some(1_000_000_000),
            nonce_key: None,
            fee_token: None,
            sponsor: None,
            valid_after: None,
            valid_before: None,
            calls: None,
        };

        let raw = sign_and_encode(&TempoAdapter, template, &mut ctx);

        assert!(!raw.is_empty());
        assert_eq!(raw[0], TEMPO_TX_TYPE_ID);
    }

    #[test]
    fn test_build_tempo_with_parallel_nonce() {
        let mut accounts_map = HashMap::new();
        accounts_map.insert(
            "users".to_string(),
            AccountPoolDef {
                mnemonic: TEST_MNEMONIC.to_string(),
                index: None,
                range: Some([0, 10]),
            },
        );
        let accounts = AccountManager::from_spec(&accounts_map).unwrap();
        let artifacts = ArtifactManager::empty();
        let gas = GasConfig::default();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);

        let template = TempoTemplate {
            tx_type: "tempo".to_string(),
            from: AccountRef {
                pool: "users".to_string(),
                select: SelectMode::Index(0),
            },
            gas_limit: 21000,
            value: GenValue::Literal(U256::from(1000)),
            to: Some(GenValue::Literal(Address::ZERO)),
            call: None,
            gas_price: None,
            max_fee_per_gas: Some(1_000_000_000),
            max_priority_fee_per_gas: Some(1_000_000_000),
            nonce_key: Some(GenValue::Literal(U256::from(42))),
            fee_token: None,
            sponsor: None,
            valid_after: None,
            valid_before: None,
            calls: None,
        };

        let raw = sign_and_encode(&TempoAdapter, template, &mut ctx);

        assert!(!raw.is_empty());
        assert_eq!(raw[0], TEMPO_TX_TYPE_ID);
    }

    #[test]
    fn test_delegates_ethereum_types() {
        let mut accounts_map = HashMap::new();
        accounts_map.insert(
            "users".to_string(),
            AccountPoolDef {
                mnemonic: TEST_MNEMONIC.to_string(),
                index: None,
                range: Some([0, 10]),
            },
        );
        let accounts = AccountManager::from_spec(&accounts_map).unwrap();
        let artifacts = ArtifactManager::empty();
        let gas = GasConfig::default();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);

        let template = TempoTemplate {
            tx_type: "eip1559".to_string(),
            from: AccountRef {
                pool: "users".to_string(),
                select: SelectMode::Index(0),
            },
            gas_limit: 21000,
            value: GenValue::Literal(U256::from(1000)),
            to: Some(GenValue::Literal(Address::ZERO)),
            call: None,
            gas_price: None,
            max_fee_per_gas: Some(1_000_000_000),
            max_priority_fee_per_gas: Some(1_000_000_000),
            nonce_key: None,
            fee_token: None,
            sponsor: None,
            valid_after: None,
            valid_before: None,
            calls: None,
        };

        let raw = sign_and_encode(&TempoAdapter, template, &mut ctx);

        assert!(!raw.is_empty());
        assert_eq!(raw[0], 0x02);
    }

    #[test]
    fn test_scheduling_key_protocol_nonce() {
        let sender = Address::repeat_byte(0xab);
        let key = compute_scheduling_key(sender, U256::ZERO);
        assert_eq!(key, sender.0.0);
    }

    #[test]
    fn test_scheduling_key_parallel_nonce() {
        let sender = Address::repeat_byte(0xab);
        let key1 = compute_scheduling_key(sender, U256::from(1));
        let key2 = compute_scheduling_key(sender, U256::from(2));

        assert_ne!(key1, sender.0.0);
        assert_ne!(key1, key2);
    }
}
