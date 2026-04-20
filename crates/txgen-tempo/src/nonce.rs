//! Tempo-specific nonce fetching from the nonce precompile.
//!
//! The nonce precompile is at address 0x4E4F4E4345000000000000000000000000000000
//! Storage layout: keccak256(abi.encode(account_address, nonce_key)) -> nonce (uint64)
//!
//! For protocol nonce (key 0), the nonce is stored in the account state directly.

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_provider::{Provider, network::Ethereum};
use eyre::{Result, WrapErr};
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};
use tempo_primitives::transaction::TEMPO_EXPIRING_NONCE_KEY;
use tokio::sync::Mutex;
use txgen_core::NonceProvider;

/// Tempo nonce precompile address (ASCII hex for "NONCE")
pub const NONCE_PRECOMPILE: Address = Address::new([
    0x4E, 0x4F, 0x4E, 0x43, 0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
]);

/// Tempo nonce provider that fetches nonces from the chain.
///
/// - Protocol nonce (key 0): Uses standard eth_getTransactionCount
/// - Expiring nonce (key U256::MAX): Always 0
/// - Parallel nonces (other non-zero keys): Query the nonce precompile storage
pub struct TempoNonceProvider<P> {
    provider: P,
    rate_limiter: Option<RateLimiter>,
}

impl<P> TempoNonceProvider<P>
where
    P: Provider<Ethereum> + Clone + Send + Sync,
{
    /// Create a new Tempo nonce provider (unbounded).
    pub fn new(provider: P) -> Self {
        Self {
            provider,
            rate_limiter: None,
        }
    }

    /// Create a new Tempo nonce provider with rate limiting.
    pub fn with_rate_limit(provider: P, requests_per_sec: u64) -> Self {
        Self {
            provider,
            rate_limiter: Some(RateLimiter::new(requests_per_sec)),
        }
    }

    /// Fetch the nonce for a given address and nonce_key.
    async fn fetch(&self, address: Address, nonce_key: U256) -> Result<u64> {
        if let Some(ref limiter) = self.rate_limiter {
            limiter.acquire().await;
        }

        if nonce_key.is_zero() {
            // Protocol nonce - use standard transaction count
            let nonce = self
                .provider
                .get_transaction_count(address)
                .await
                .wrap_err("failed to fetch protocol nonce")?;
            eprintln!("fetched lane nonce: {} lane=0 nonce={}", address, nonce);
            Ok(nonce)
        } else if nonce_key == TEMPO_EXPIRING_NONCE_KEY {
            eprintln!("fetched lane nonce: {} lane=expiring nonce=0", address);
            Ok(0)
        } else {
            // Parallel nonce - query nonce precompile storage
            let storage_key = compute_nonce_storage_key(address, nonce_key);
            let storage_value = self
                .provider
                .get_storage_at(NONCE_PRECOMPILE, storage_key)
                .await
                .wrap_err("failed to fetch parallel nonce from precompile")?;

            // Storage value is uint64, stored as U256
            let nonce = storage_value.to::<u64>();
            eprintln!(
                "fetched lane nonce: {} lane={} nonce={}",
                address, nonce_key, nonce
            );
            Ok(nonce)
        }
    }
}

/// Simple token bucket rate limiter.
struct RateLimiter {
    interval: Duration,
    last_token: Mutex<Instant>,
}

impl RateLimiter {
    fn new(tokens_per_sec: u64) -> Self {
        Self {
            interval: Duration::from_secs_f64(1.0 / tokens_per_sec as f64),
            last_token: Mutex::new(Instant::now()),
        }
    }

    async fn acquire(&self) {
        let mut last = self.last_token.lock().await;
        let now = Instant::now();
        let elapsed = now.duration_since(*last);

        if elapsed < self.interval {
            tokio::time::sleep(self.interval - elapsed).await;
        }

        *last = Instant::now();
    }
}

impl<P> NonceProvider for TempoNonceProvider<P>
where
    P: Provider<Ethereum> + Clone + Send + Sync + 'static,
{
    fn fetch_nonce(
        &self,
        address: Address,
        nonce_key: U256,
        _scheduling_key: [u8; 20],
    ) -> Pin<Box<dyn Future<Output = Result<u64>> + Send + '_>> {
        Box::pin(self.fetch(address, nonce_key))
    }
}

/// Prefetch parallel lane nonces for all (account, nonce_key) pairs found in templates.
///
/// Scans the spec templates for literal `nonce_key` values and fetches the current
/// nonce from the nonce precompile for each (sender, nonce_key) combination.
/// Protocol nonces (key=0) are skipped — use [`txgen_ethereum::fetch_protocol_nonces`]
/// for those.
pub async fn prefetch_parallel_nonces<P: Provider<Ethereum> + Clone + Send + Sync>(
    provider: &P,
    accounts: &txgen_core::AccountManager,
    spec: &txgen_core::WorkloadSpec,
    nonces: &mut txgen_core::NonceTracker,
) -> Result<()> {
    use crate::compute_parallel_scheduling_key;

    let nonce_keys = collect_prefetchable_parallel_nonce_keys(spec);

    if nonce_keys.is_empty() {
        return Ok(());
    }

    eprintln!(
        "prefetching nonces for {} parallel lane(s)...",
        nonce_keys.len()
    );

    for (pool_name, addresses) in accounts.all_addresses() {
        for address in addresses {
            for &nonce_key in &nonce_keys {
                let storage_key = compute_nonce_storage_key(address, nonce_key);
                let storage_value = provider
                    .get_storage_at(NONCE_PRECOMPILE, storage_key)
                    .await
                    .wrap_err_with(|| {
                        format!(
                            "failed to fetch parallel nonce for {} lane {} ({})",
                            pool_name, nonce_key, address
                        )
                    })?;

                let nonce = storage_value.to::<u64>();
                let scheduling_key = compute_parallel_scheduling_key(address, nonce_key);
                nonces.reset(scheduling_key, nonce);

                eprintln!(
                    "fetched lane nonce: {} lane={} nonce={}",
                    address, nonce_key, nonce
                );
            }
        }
    }

    Ok(())
}

/// Collect constant 2D nonce lanes that can be prefetched before generation.
///
/// Prefetch only makes sense for nonce keys that are already fixed in the spec.
/// Generated keys (`uniform`, `choice`, etc.) are resolved per transaction and
/// cannot be known ahead of time.
fn collect_prefetchable_parallel_nonce_keys(
    spec: &txgen_core::WorkloadSpec,
) -> std::collections::HashSet<U256> {
    use crate::TempoTemplate;
    use txgen_core::GenValue;

    let mut nonce_keys = std::collections::HashSet::new();

    for entry in &spec.mix {
        if let Some(value) = spec.templates.get(&entry.template)
            && let Ok(template) = serde_yaml::from_value::<TempoTemplate>(value.clone())
            && !template.expiring_nonce
            && let Some(GenValue::Literal(key)) = &template.nonce_key
            && !key.is_zero()
            && *key != TEMPO_EXPIRING_NONCE_KEY
        {
            nonce_keys.insert(*key);
        }
    }

    nonce_keys
}

/// Compute the storage key for a (address, nonce_key) pair in the nonce precompile.
/// Storage key = keccak256(abi.encode(address, nonce_key))
fn compute_nonce_storage_key(address: Address, nonce_key: U256) -> U256 {
    // abi.encode(address, uint256) = 32 bytes (address padded) + 32 bytes (nonce_key)
    let mut data = [0u8; 64];
    // Address is left-padded to 32 bytes
    data[12..32].copy_from_slice(address.as_slice());
    // nonce_key as big-endian U256
    data[32..64].copy_from_slice(&nonce_key.to_be_bytes::<32>());

    let hash: B256 = keccak256(data);
    U256::from_be_bytes(hash.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use txgen_core::WorkloadSpec;

    #[test]
    fn test_nonce_precompile_address() {
        // "NONCE" in ASCII hex
        assert_eq!(
            format!("{:?}", NONCE_PRECOMPILE).to_lowercase(),
            "0x4e4f4e4345000000000000000000000000000000"
        );
    }

    #[test]
    fn test_compute_storage_key() {
        let address = Address::repeat_byte(0xab);
        let nonce_key = U256::from(42);

        let key = compute_nonce_storage_key(address, nonce_key);

        // Should be deterministic
        let key2 = compute_nonce_storage_key(address, nonce_key);
        assert_eq!(key, key2);

        // Different nonce_key should give different storage key
        let key3 = compute_nonce_storage_key(address, U256::from(43));
        assert_ne!(key, key3);
    }

    #[test]
    fn test_collect_prefetchable_parallel_nonce_keys_skips_expiring_nonce_templates() {
        let spec = WorkloadSpec::parse(
            r#"
chain_id: 1
templates:
  expiring_flag:
    type: tempo
    from: { pool: users, select: random }
    to: "0x0000000000000000000000000000000000000001"
    gas_limit: 21000
    expiring_nonce: true
    valid_before: 1700000000
  expiring_reserved_key:
    type: tempo
    from: { pool: users, select: random }
    to: "0x0000000000000000000000000000000000000001"
    gas_limit: 21000
    nonce_key: "115792089237316195423570985008687907853269984665640564039457584007913129639935"
    valid_before: 1700000000
  parallel_lane:
    type: tempo
    from: { pool: users, select: random }
    to: "0x0000000000000000000000000000000000000001"
    gas_limit: 21000
    nonce_key: "42"
mix:
  - template: expiring_flag
    weight: 1
  - template: expiring_reserved_key
    weight: 1
  - template: parallel_lane
    weight: 1
"#,
        )
        .unwrap();

        let nonce_keys = collect_prefetchable_parallel_nonce_keys(&spec);
        assert_eq!(nonce_keys.len(), 1);
        assert!(nonce_keys.contains(&U256::from(42)));
    }
}
