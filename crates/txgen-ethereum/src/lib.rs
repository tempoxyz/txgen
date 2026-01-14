mod template;

use alloy_consensus::{
    SignableTransaction, TxEip1559, TxEip2930, TxLegacy, transaction::RlpEcdsaEncodableTx,
};
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, Bytes, TxKind, U256};
use eyre::{Result, bail};
use txgen_core::{BuildContext, ChainPlugin, GenValue, GeneratedTx, SelectMode, ValueResolver};

pub use template::EthereumTemplate;

/// Ethereum transaction generation plugin.
///
/// Supports legacy, EIP-2930, and EIP-1559 transaction types.
#[derive(Debug, Default)]
pub struct EthereumPlugin;

impl ChainPlugin for EthereumPlugin {
    type Template = EthereumTemplate;

    fn name(&self) -> &'static str {
        "ethereum"
    }

    fn build(&self, template: Self::Template, ctx: &mut BuildContext<'_>) -> Result<GeneratedTx> {
        // Resolve the sender - get address and index first
        let (from_address, signer_pool, signer_idx) = {
            match template.from.select {
                SelectMode::Random => {
                    let signer = ctx.accounts.get_random(&template.from.pool, ctx.rng)?;
                    let addr = signer.address();
                    // Find the index - for random we need to scan
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

        // Scheduling key is the sender address
        let key: [u8; 20] = from_address.0.0;

        // Get nonce for this sender
        let nonce = ctx.next_nonce(key);

        // Resolve call data if present
        let (to, value, input) = resolve_call_data(&template, ctx)?;

        // Get signer again for signing
        let signer = ctx.accounts.get_by_index(&signer_pool, signer_idx)?;

        // Build and sign the transaction based on type
        let raw = match template.tx_type.as_str() {
            "legacy" => build_legacy(&template, ctx, nonce, to, value, input, signer)?,
            "eip2930" => build_eip2930(&template, ctx, nonce, to, value, input, signer)?,
            "eip1559" => build_eip1559(&template, ctx, nonce, to, value, input, signer)?,
            other => bail!("unsupported transaction type: {}", other),
        };

        Ok(GeneratedTx { raw, key })
    }
}

fn resolve_call_data(
    template: &EthereumTemplate,
    ctx: &mut BuildContext<'_>,
) -> Result<(TxKind, U256, Bytes)> {
    if let Some(ref call) = template.call {
        // Get artifacts reference before creating resolver
        let artifacts = ctx.artifacts;
        let mut resolver = ctx.resolver();
        let encoded = call.encode(artifacts, &mut resolver)?;
        Ok((TxKind::Call(encoded.to), encoded.value, encoded.input))
    } else {
        // Simple transfer or contract creation
        let mut resolver = ctx.resolver();
        let to = resolve_to(&template.to, &mut resolver)?;
        let value = resolver.resolve_gen(&template.value)?;
        Ok((to, value, Bytes::new()))
    }
}

fn resolve_to(to: &Option<GenValue<Address>>, resolver: &mut ValueResolver<'_>) -> Result<TxKind> {
    match to {
        Some(gen_value) => {
            let addr: Address = resolver.resolve_gen(gen_value)?;
            Ok(TxKind::Call(addr))
        }
        None => Ok(TxKind::Create),
    }
}

fn build_legacy(
    template: &EthereumTemplate,
    ctx: &BuildContext<'_>,
    nonce: u64,
    to: TxKind,
    value: U256,
    input: Bytes,
    signer: &txgen_core::EcdsaSigner,
) -> Result<Bytes> {
    let gas_price = template.gas_price.unwrap_or(ctx.gas.max_fee_per_gas);

    let tx = TxLegacy {
        chain_id: Some(ctx.chain_id),
        nonce,
        gas_price,
        gas_limit: template.gas_limit,
        to,
        value,
        input,
    };

    let signature = signer.sign_transaction_sync(&mut tx.clone())?;
    let signed = tx.into_signed(signature);

    let mut encoded = Vec::new();
    signed.tx().eip2718_encode(signed.signature(), &mut encoded);

    Ok(Bytes::from(encoded))
}

fn build_eip2930(
    template: &EthereumTemplate,
    ctx: &BuildContext<'_>,
    nonce: u64,
    to: TxKind,
    value: U256,
    input: Bytes,
    signer: &txgen_core::EcdsaSigner,
) -> Result<Bytes> {
    let gas_price = template.gas_price.unwrap_or(ctx.gas.max_fee_per_gas);

    let tx = TxEip2930 {
        chain_id: ctx.chain_id,
        nonce,
        gas_price,
        gas_limit: template.gas_limit,
        to,
        value,
        input,
        access_list: Default::default(),
    };

    let signature = signer.sign_transaction_sync(&mut tx.clone())?;
    let signed = tx.into_signed(signature);

    let mut encoded = Vec::new();
    signed.tx().eip2718_encode(signed.signature(), &mut encoded);

    Ok(Bytes::from(encoded))
}

fn build_eip1559(
    template: &EthereumTemplate,
    ctx: &BuildContext<'_>,
    nonce: u64,
    to: TxKind,
    value: U256,
    input: Bytes,
    signer: &txgen_core::EcdsaSigner,
) -> Result<Bytes> {
    let max_fee_per_gas = template.max_fee_per_gas.unwrap_or(ctx.gas.max_fee_per_gas);
    let max_priority_fee_per_gas = template
        .max_priority_fee_per_gas
        .unwrap_or(ctx.gas.max_priority_fee_per_gas);

    let tx = TxEip1559 {
        chain_id: ctx.chain_id,
        nonce,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        gas_limit: template.gas_limit,
        to,
        value,
        input,
        access_list: Default::default(),
    };

    let signature = signer.sign_transaction_sync(&mut tx.clone())?;
    let signed = tx.into_signed(signature);

    let mut encoded = Vec::new();
    signed.tx().eip2718_encode(signed.signature(), &mut encoded);

    Ok(Bytes::from(encoded))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use std::collections::HashMap;
    use txgen_core::{
        AccountManager, AccountPoolDef, AccountRef, ArtifactManager, GasConfig, NonceTracker,
    };

    const TEST_MNEMONIC: &str = "test test test test test test test test test test test junk";

    #[test]
    fn test_build_eip1559_transfer() {
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

        let template = EthereumTemplate {
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
        };

        let plugin = EthereumPlugin;
        let tx = plugin.build(template, &mut ctx).unwrap();

        // Verify we got a non-empty transaction
        assert!(!tx.raw.is_empty());
        // EIP-1559 transactions start with 0x02
        assert_eq!(tx.raw[0], 0x02);
    }
}
