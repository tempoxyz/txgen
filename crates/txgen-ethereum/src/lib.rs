mod nonces;
mod template;

pub use nonces::fetch_protocol_nonces;

use alloy_consensus::{
    SignableTransaction, TxEip1559, TxEip2930, TxLegacy, transaction::RlpEcdsaEncodableTx,
};
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, Bytes, TxKind, U256};
use eyre::{Result, bail};
use txgen_cli::{GenerateContext, NetworkAdapter};
use txgen_core::{BuildContext, GenValue, GeneratedTx, ValueResolver};

pub use template::EthereumTemplate;

/// Ethereum network adapter for transaction generation.
///
/// Supports legacy, EIP-2930, and EIP-1559 transaction types.
pub struct EthereumAdapter;

impl NetworkAdapter for EthereumAdapter {
    type Template = EthereumTemplate;

    fn build_tx(
        &self,
        template: Self::Template,
        ctx: &mut BuildContext<'_>,
    ) -> Result<GeneratedTx> {
        let selected = ctx.select_signer(&template.from)?;
        let key: [u8; 20] = selected.address.0.0;
        let nonce = ctx.next_nonce(key);
        let (to, value, input) = resolve_call_data(&template, ctx)?;
        let signer = ctx.accounts.get_by_index(&selected.pool, selected.index)?;

        let raw = match template.tx_type.as_str() {
            "legacy" => build_legacy(&template, ctx, nonce, to, value, input, signer)?,
            "eip2930" => build_eip2930(&template, ctx, nonce, to, value, input, signer)?,
            "eip1559" => build_eip1559(&template, ctx, nonce, to, value, input, signer)?,
            other => bail!("unsupported transaction type: {}", other),
        };

        Ok(GeneratedTx { raw, key })
    }

    #[allow(clippy::manual_async_fn)]
    fn prefetch_nonces<'a>(
        &'a self,
        ctx: &'a mut GenerateContext,
        rpc: &'a str,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        async move {
            let (accounts, nonces) = ctx.accounts_and_nonces();
            fetch_protocol_nonces(accounts, nonces, rpc).await
        }
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
        SelectMode,
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

        let adapter = EthereumAdapter;
        let tx = adapter.build_tx(template, &mut ctx).unwrap();

        // Verify we got a non-empty transaction
        assert!(!tx.raw.is_empty());
        // EIP-1559 transactions start with 0x02
        assert_eq!(tx.raw[0], 0x02);
    }
}
