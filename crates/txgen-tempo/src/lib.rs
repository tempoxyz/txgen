mod nonce;
mod template;
mod tempo_tx;

pub use nonce::{NONCE_PRECOMPILE, TempoNonceProvider, prefetch_parallel_nonces};
pub use txgen_ethereum::fetch_protocol_nonces;

use alloy_primitives::{Address, Bytes, TxKind, U256, keccak256};
use alloy_signer::SignerSync;
use eyre::Result;
use txgen_core::{BuildContext, ChainPlugin, GeneratedTx, SelectMode};
use txgen_ethereum::{EthereumPlugin, EthereumTemplate};

pub use template::TempoTemplate;
pub use tempo_tx::{Call, TEMPO_TX_TYPE_ID, TempoTransaction};

/// Tempo transaction generation plugin.
///
/// Supports all Ethereum transaction types (delegated to EthereumPlugin)
/// plus Tempo native 0x76 transactions.
#[derive(Debug, Default)]
pub struct TempoPlugin {
    ethereum: EthereumPlugin,
}

impl ChainPlugin for TempoPlugin {
    type Template = TempoTemplate;

    fn name(&self) -> &'static str {
        "tempo"
    }

    fn build(&self, template: Self::Template, ctx: &mut BuildContext<'_>) -> Result<GeneratedTx> {
        match template.tx_type.as_str() {
            "tempo" => self.build_tempo(template, ctx),
            _ => {
                let eth_template = convert_to_ethereum_template(&template)?;
                self.ethereum.build(eth_template, ctx)
            }
        }
    }
}

impl TempoPlugin {
    /// Build a Tempo transaction with async nonce fetching support.
    ///
    /// If a nonce provider is given, it will be used to fetch nonces for
    /// scheduling keys that haven't been seen before (including parallel lanes).
    pub async fn build_with_nonce_provider<P: txgen_core::NonceProvider>(
        &self,
        template: TempoTemplate,
        ctx: &mut BuildContext<'_>,
        nonce_provider: Option<&P>,
    ) -> Result<GeneratedTx> {
        match template.tx_type.as_str() {
            "tempo" => self.build_tempo_async(template, ctx, nonce_provider).await,
            _ => {
                let eth_template = convert_to_ethereum_template(&template)?;
                self.ethereum.build(eth_template, ctx)
            }
        }
    }

    async fn build_tempo_async<P: txgen_core::NonceProvider>(
        &self,
        template: TempoTemplate,
        ctx: &mut BuildContext<'_>,
        nonce_provider: Option<&P>,
    ) -> Result<GeneratedTx> {
        let (from_address, signer_pool, signer_idx) = {
            match template.from.select {
                SelectMode::Random => {
                    let signer = ctx.accounts.get_random(&template.from.pool, ctx.rng)?;
                    let addr = signer.address();
                    let pool = ctx.accounts.get_pool(&template.from.pool)?;
                    let idx = pool.iter().position(|s| s.address() == addr).unwrap_or(0);
                    (addr, template.from.pool.clone(), idx)
                }
                SelectMode::Index(idx) => {
                    let signer = ctx.accounts.get_by_index(&template.from.pool, idx)?;
                    (signer.address(), template.from.pool.clone(), idx)
                }
            }
        };

        let nonce_key: U256 = if let Some(ref nk) = template.nonce_key {
            let mut resolver = ctx.resolver();
            resolver.resolve_gen(nk)?
        } else {
            U256::ZERO
        };

        let scheduling_key = compute_scheduling_key(from_address, nonce_key);

        // Fetch nonce from provider if available and not yet tracked
        let nonce = if let Some(provider) = nonce_provider {
            ctx.nonces
                .next_with_provider(scheduling_key, from_address, nonce_key, provider)
                .await?
        } else {
            ctx.next_nonce(scheduling_key)
        };

        self.build_tempo_inner(
            template,
            ctx,
            from_address,
            signer_pool,
            signer_idx,
            nonce_key,
            nonce,
        )
    }

    fn build_tempo(
        &self,
        template: TempoTemplate,
        ctx: &mut BuildContext<'_>,
    ) -> Result<GeneratedTx> {
        let (from_address, signer_pool, signer_idx) = {
            match template.from.select {
                SelectMode::Random => {
                    let signer = ctx.accounts.get_random(&template.from.pool, ctx.rng)?;
                    let addr = signer.address();
                    let pool = ctx.accounts.get_pool(&template.from.pool)?;
                    let idx = pool.iter().position(|s| s.address() == addr).unwrap_or(0);
                    (addr, template.from.pool.clone(), idx)
                }
                SelectMode::Index(idx) => {
                    let signer = ctx.accounts.get_by_index(&template.from.pool, idx)?;
                    (signer.address(), template.from.pool.clone(), idx)
                }
            }
        };

        let nonce_key: U256 = if let Some(ref nk) = template.nonce_key {
            let mut resolver = ctx.resolver();
            resolver.resolve_gen(nk)?
        } else {
            U256::ZERO
        };

        let scheduling_key = compute_scheduling_key(from_address, nonce_key);

        let nonce = ctx.next_nonce(scheduling_key);

        self.build_tempo_inner(
            template,
            ctx,
            from_address,
            signer_pool,
            signer_idx,
            nonce_key,
            nonce,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build_tempo_inner(
        &self,
        template: TempoTemplate,
        ctx: &mut BuildContext<'_>,
        from_address: Address,
        signer_pool: String,
        signer_idx: usize,
        nonce_key: U256,
        nonce: u64,
    ) -> Result<GeneratedTx> {
        let scheduling_key = compute_scheduling_key(from_address, nonce_key);

        let calls = resolve_calls(&template, ctx)?;

        let max_fee_per_gas = template.max_fee_per_gas.unwrap_or(ctx.gas.max_fee_per_gas);
        let max_priority_fee_per_gas = template
            .max_priority_fee_per_gas
            .unwrap_or(ctx.gas.max_priority_fee_per_gas);

        let mut tx = TempoTransaction {
            chain_id: ctx.chain_id,
            fee_token: template.fee_token,
            max_priority_fee_per_gas,
            max_fee_per_gas,
            gas_limit: template.gas_limit,
            calls,
            nonce_key,
            nonce,
            fee_payer_signature: None,
            valid_before: template.valid_before,
            valid_after: template.valid_after,
        };

        let signer = ctx.accounts.get_by_index(&signer_pool, signer_idx)?;
        let sig_hash = tx.signature_hash();
        let sender_signature = signer.sign_hash_sync(&sig_hash)?;

        let raw = if let Some(ref sponsor_ref) = template.sponsor {
            let sponsor_signer = match sponsor_ref.select {
                SelectMode::Random => ctx.accounts.get_random(&sponsor_ref.pool, ctx.rng)?,
                SelectMode::Index(idx) => ctx.accounts.get_by_index(&sponsor_ref.pool, idx)?,
            };

            let fee_payer_hash = tx.fee_payer_signature_hash(from_address);
            let fee_payer_sig = sponsor_signer.sign_hash_sync(&fee_payer_hash)?;
            tx.fee_payer_signature = Some(fee_payer_sig);

            encode_signed_tempo_tx(&tx, sender_signature)
        } else {
            encode_signed_tempo_tx(&tx, sender_signature)
        };

        Ok(GeneratedTx {
            raw,
            key: scheduling_key,
        })
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

fn resolve_calls(template: &TempoTemplate, ctx: &mut BuildContext<'_>) -> Result<Vec<Call>> {
    if let Some(ref call_defs) = template.calls {
        let mut calls = Vec::with_capacity(call_defs.len());
        for call_def in call_defs {
            let artifacts = ctx.artifacts;
            let mut resolver = ctx.resolver();
            let encoded = call_def.encode(artifacts, &mut resolver)?;
            calls.push(Call {
                to: TxKind::Call(encoded.to),
                value: encoded.value,
                input: encoded.input,
            });
        }
        Ok(calls)
    } else if let Some(ref call_def) = template.call {
        let artifacts = ctx.artifacts;
        let mut resolver = ctx.resolver();
        let encoded = call_def.encode(artifacts, &mut resolver)?;
        Ok(vec![Call {
            to: TxKind::Call(encoded.to),
            value: encoded.value,
            input: encoded.input,
        }])
    } else {
        let mut resolver = ctx.resolver();
        let to = if let Some(ref to_gen) = template.to {
            TxKind::Call(resolver.resolve_gen(to_gen)?)
        } else {
            TxKind::Create
        };
        let value: U256 = resolver.resolve_gen(&template.value)?;
        Ok(vec![Call {
            to,
            value,
            input: Bytes::new(),
        }])
    }
}

fn encode_signed_tempo_tx(tx: &TempoTransaction, signature: alloy_primitives::Signature) -> Bytes {
    let signed = tx.clone().into_signed(signature);
    let mut encoded = Vec::new();
    signed.encode_2718(&mut encoded);
    Bytes::from(encoded)
}

fn convert_to_ethereum_template(template: &TempoTemplate) -> Result<EthereumTemplate> {
    Ok(EthereumTemplate {
        tx_type: template.tx_type.clone(),
        from: template.from.clone(),
        gas_limit: template.gas_limit,
        value: template.value.clone(),
        to: template.to.clone(),
        call: template.call.clone(),
        gas_price: template.gas_price,
        max_fee_per_gas: template.max_fee_per_gas,
        max_priority_fee_per_gas: template.max_priority_fee_per_gas,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use std::collections::HashMap;
    use txgen_core::{
        AccountManager, AccountPoolDef, AccountRef, ArtifactManager, GasConfig, GenValue,
        NonceTracker,
    };

    const TEST_MNEMONIC: &str = "test test test test test test test test test test test junk";

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

        let plugin = TempoPlugin::default();
        let tx = plugin.build(template, &mut ctx).unwrap();

        assert!(!tx.raw.is_empty());
        assert_eq!(tx.raw[0], 0x76);
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

        let plugin = TempoPlugin::default();
        let tx = plugin.build(template, &mut ctx).unwrap();

        assert!(!tx.raw.is_empty());
        assert_eq!(tx.raw[0], 0x76);
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

        let plugin = TempoPlugin::default();
        let tx = plugin.build(template, &mut ctx).unwrap();

        assert!(!tx.raw.is_empty());
        assert_eq!(tx.raw[0], 0x02);
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
