mod template;

use alloy_network::{Ethereum, TransactionBuilder};
use alloy_primitives::{Bytes, TxKind, U256};
use alloy_rpc_types_eth::TransactionRequest;
use eyre::Result;
use txgen_cli::{NetworkAdapter, TxRequest};
use txgen_core::BuildContext;

pub use template::{EthTxType, EthereumTemplate};

/// Ethereum network adapter for transaction generation.
///
/// Supports legacy, EIP-2930, and EIP-1559 transaction types.
#[derive(Default)]
pub struct EthereumAdapter;

impl NetworkAdapter for EthereumAdapter {
    type Template = EthereumTemplate;
    type Network = Ethereum;
    type SignContext = ();

    fn network_name() -> &'static str {
        "ethereum"
    }

    fn build_request(
        &self,
        template: Self::Template,
        ctx: &mut BuildContext<'_>,
    ) -> Result<TxRequest<TransactionRequest>> {
        let selected = ctx.select_signer(&template.from)?;
        let key: [u8; 20] = selected.address.0 .0;
        let nonce = ctx.next_nonce(key);

        let (to, value, input) = resolve_call_data(&template, ctx)?;

        let mut req = TransactionRequest::default();
        req.set_chain_id(ctx.chain_id);
        req.set_nonce(nonce);
        req.set_gas_limit(template.gas_limit);

        req.set_kind(to);
        req.set_value(value);
        if !input.is_empty() {
            req.set_input(input);
        }

        match template.tx_type {
            EthTxType::Legacy => {
                req.set_gas_price(template.gas_price.unwrap_or(ctx.gas.max_fee_per_gas));
            }
            EthTxType::Eip2930 => {
                req.set_gas_price(template.gas_price.unwrap_or(ctx.gas.max_fee_per_gas));
                req.set_access_list(Default::default());
            }
            EthTxType::Eip1559 => {
                req.set_max_fee_per_gas(
                    template.max_fee_per_gas.unwrap_or(ctx.gas.max_fee_per_gas),
                );
                req.set_max_priority_fee_per_gas(
                    template.max_priority_fee_per_gas.unwrap_or(ctx.gas.max_priority_fee_per_gas),
                );
            }
        }

        Ok(TxRequest {
            request: req,
            signer_pool: selected.pool,
            signer_index: selected.index,
            key,
            sign_context: (),
        })
    }

    #[allow(clippy::manual_async_fn)]
    fn prepare_nonces<'a>(
        &'a self,
        _spec: &'a txgen_core::WorkloadSpec,
        accounts: &'a txgen_core::AccountManager,
        nonces: &'a mut txgen_core::NonceTracker,
        rpc: &'a str,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        async move { txgen_cli::fetch_pending_protocol_nonces(accounts, nonces, rpc).await }
    }
}

fn resolve_call_data(
    template: &EthereumTemplate,
    ctx: &mut BuildContext<'_>,
) -> Result<(TxKind, U256, Bytes)> {
    if let Some(ref call) = template.call {
        let encoded = ctx.encode_call(call)?;
        Ok((TxKind::Call(encoded.to), encoded.value, encoded.input))
    } else {
        let to = ctx.resolve_to(&template.to)?;
        let value = ctx.resolve_value(&template.value)?;
        let input = template
            .input
            .as_ref()
            .map(|input| ctx.resolve_value(input))
            .transpose()?
            .unwrap_or_default();
        Ok((to, value, input))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::SignableTransaction;
    use alloy_eips::eip2718::Encodable2718;
    use alloy_network::{NetworkTransactionBuilder, TxSignerSync};
    use rand::{rngs::StdRng, SeedableRng};
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
        time::Duration,
    };
    use txgen_cli::{materialize_setup, materialize_setup_online};
    use txgen_core::{
        AccountManager, AccountPoolDef, AccountRef, AddressPoolManager, ArtifactManager, GasConfig,
        GenValue, NonceTracker, SelectMode, WorkloadSpec,
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
            tx_type: EthTxType::Eip1559,
            from: AccountRef { pool: "users".to_string(), select: SelectMode::Index(0) },
            gas_limit: 21000,
            value: GenValue::Literal(U256::from(1000)),
            to: Some(GenValue::Literal(alloy_primitives::Address::ZERO)),
            input: None,
            call: None,
            gas_price: None,
            max_fee_per_gas: Some(1_000_000_000),
            max_priority_fee_per_gas: Some(1_000_000_000),
        };

        let adapter = EthereumAdapter;
        let tx_req = adapter.build_request(template, &mut ctx).unwrap();

        let mut unsigned = tx_req.request.build_unsigned().unwrap();
        let signer = ctx.accounts.get_by_index(&tx_req.signer_pool, tx_req.signer_index).unwrap();
        let sig = signer.sign_transaction_sync(&mut unsigned).unwrap();
        let signed = unsigned.into_signed(sig);
        let envelope =
            <alloy_consensus::TxEnvelope as From<alloy_consensus::Signed<_>>>::from(signed);
        let raw = Bytes::from(envelope.encoded_2718());

        // Verify we got a non-empty transaction
        assert!(!raw.is_empty());
        // EIP-1559 transactions start with 0x02
        assert_eq!(raw[0], 0x02);
    }

    #[tokio::test]
    async fn online_setup_matches_existing_generate_materialization() {
        let spec: WorkloadSpec = serde_yaml::from_str(&format!(
            r#"
chain_id: 1
accounts:
  users:
    mnemonic: "{TEST_MNEMONIC}"
    range: [0, 1]
setup:
  steps:
    - id: first
      tx:
        type: eip1559
        from: {{ pool: users, select: {{ index: 0 }} }}
        to: "0x0000000000000000000000000000000000000001"
        value: 1
        gas_limit: 21000
    - id: second
      tx:
        type: eip1559
        from: {{ pool: users, select: {{ index: 0 }} }}
        to: {{ var: setup.first.sender }}
        value: 2
        gas_limit: 21000
"#
        ))
        .unwrap();
        let accounts = AccountManager::from_spec(&spec.accounts).unwrap();
        let address_pools = AddressPoolManager::empty();
        let artifacts = ArtifactManager::empty();

        let generated = {
            let mut nonces = NonceTracker::new();
            let mut rng = StdRng::seed_from_u64(99);
            let mut context = BuildContext::new_with_address_pools(
                spec.chain_id,
                &spec.gas,
                &accounts,
                &address_pools,
                &artifacts,
                &mut nonces,
                &mut rng,
            );
            materialize_setup(&mut EthereumAdapter, &spec, &mut context).unwrap()
        };

        let observed = Arc::new(Mutex::new(Vec::new()));
        let online = {
            let mut nonces = NonceTracker::new();
            let mut rng = StdRng::seed_from_u64(99);
            let mut context = BuildContext::new_with_address_pools(
                spec.chain_id,
                &spec.gas,
                &accounts,
                &address_pools,
                &artifacts,
                &mut nonces,
                &mut rng,
            );
            materialize_setup_online(
                &mut EthereumAdapter,
                &spec,
                &mut context,
                Duration::from_secs(1),
                {
                    let observed = observed.clone();
                    move |transaction| {
                        let observed = observed.clone();
                        async move {
                            observed
                                .lock()
                                .unwrap()
                                .push(transaction.id.clone().expect("setup transaction id"));
                            Ok(transaction)
                        }
                    }
                },
            )
            .await
            .unwrap()
        };

        assert_eq!(
            *observed.lock().unwrap(),
            vec!["setup.first".to_string(), "setup.second".to_string()]
        );
        assert_eq!(generated.transactions.len(), online.transactions.len());
        for (generated, online) in generated.transactions.iter().zip(&online.transactions) {
            assert_eq!(generated.id, online.id);
            assert_eq!(generated.raw, online.raw);
            assert_eq!(generated.submission_keys, online.submission_keys);
            assert_eq!(generated.inclusion_keys, online.inclusion_keys);
        }
    }
}
