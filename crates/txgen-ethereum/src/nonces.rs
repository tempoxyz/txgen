use alloy_provider::{Provider, ProviderBuilder, network::Ethereum};
use eyre::{Result, WrapErr};
use txgen_core::{AccountManager, NonceTracker};

/// Fetch protocol nonces (nonce_key=0) for all accounts from an EVM RPC.
///
/// Uses `eth_getTransactionCount` to fetch the current nonce for each
/// account address and stores it in the tracker with the sender address
/// as the scheduling key.
pub async fn fetch_protocol_nonces(
    accounts: &AccountManager,
    nonces: &mut NonceTracker,
    rpc_url: &str,
) -> Result<()> {
    let provider = ProviderBuilder::<_, _, Ethereum>::new()
        .connect_http(rpc_url.parse().wrap_err("invalid RPC URL")?);

    for (pool_name, addresses) in accounts.all_addresses() {
        let total = addresses.len();
        for (idx, address) in addresses.iter().enumerate() {
            eprintln!(
                "fetching nonce for {}[{}/{}] ({})...",
                pool_name,
                idx + 1,
                total,
                address
            );

            let nonce = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                provider.get_transaction_count(*address),
            )
            .await
            .wrap_err_with(|| format!("timeout fetching nonce for {}[{}]", pool_name, idx))?
            .wrap_err_with(|| {
                format!(
                    "failed to fetch nonce for {}[{}] ({})",
                    pool_name, idx, address
                )
            })?;

            let scheduling_key = address.0.0;
            nonces.reset(scheduling_key, nonce);

            eprintln!(
                "fetched nonce for {}[{}/{}] ({}): {}",
                pool_name,
                idx + 1,
                total,
                address,
                nonce
            );
        }
    }

    Ok(())
}
