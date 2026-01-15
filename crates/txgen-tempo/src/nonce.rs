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
/// - Parallel nonces (key != 0): Queries the nonce precompile storage
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
}
