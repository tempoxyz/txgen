//! Transaction sender with scheduling key ordering and rate limiting.
//!
//! Handles sending transactions to an RPC endpoint while:
//! - Respecting scheduling key ordering (same key = sequential, different key = parallel)
//! - Applying rate limiting

use crate::metrics::MetricsCollector;
use alloy_network::Ethereum;
use alloy_primitives::Bytes;
use alloy_provider::{DynProvider, Provider, ProviderBuilder};
use eyre::{Context, Result};
use rand::seq::IndexedRandom;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use txgen_core::GeneratedTx;

/// Configuration for the sender.
#[derive(Debug, Clone)]
pub struct SenderConfig {
    /// RPC endpoint URLs. Transactions are randomly distributed across these.
    pub rpc_urls: Vec<String>,
    /// Maximum transactions per second (0 = unlimited).
    pub rate_limit: u64,
    /// Maximum concurrent requests.
    pub max_concurrent: usize,
    /// Request timeout.
    pub timeout: Duration,
}

impl Default for SenderConfig {
    fn default() -> Self {
        Self {
            rpc_urls: vec!["http://localhost:8545".to_string()],
            rate_limit: 0,
            max_concurrent: 100,
            timeout: Duration::from_secs(30),
        }
    }
}

/// A transaction to be sent.
struct PendingTx {
    raw: Bytes,
    key: [u8; 20],
}

/// Transaction sender.
pub struct Sender {
    providers: Vec<DynProvider<Ethereum>>,
    metrics: Arc<MetricsCollector>,
    semaphore: Arc<Semaphore>,
    /// Per-key queues to ensure ordering.
    key_queues: HashMap<[u8; 20], mpsc::Sender<PendingTx>>,
    /// Worker task handles for awaiting completion.
    worker_handles: Vec<JoinHandle<()>>,
    /// Rate limiter tokens.
    rate_limiter: Option<Arc<RateLimiter>>,
}

impl Sender {
    /// Create a new sender.
    pub fn new(config: SenderConfig, metrics: Arc<MetricsCollector>) -> Result<Self> {
        let providers: Vec<DynProvider<Ethereum>> = config
            .rpc_urls
            .iter()
            .map(|url| {
                let url = url.parse().context("failed to parse RPC URL")?;
                Ok(ProviderBuilder::new().connect_http(url).erased())
            })
            .collect::<Result<_>>()?;

        let semaphore = Arc::new(Semaphore::new(config.max_concurrent));

        let rate_limiter = if config.rate_limit > 0 {
            Some(Arc::new(RateLimiter::new(config.rate_limit)))
        } else {
            None
        };

        Ok(Self {
            providers,
            metrics,
            semaphore,
            key_queues: HashMap::new(),
            worker_handles: Vec::new(),
            rate_limiter,
        })
    }

    /// Send a transaction.
    ///
    /// This respects scheduling key ordering: transactions with the same key
    /// are sent sequentially, while transactions with different keys can be
    /// sent in parallel.
    pub async fn send(&mut self, tx: GeneratedTx) -> Result<()> {
        // Apply rate limiting before enqueueing to provide backpressure
        // to the source reader. This makes the rate limit global rather
        // than per-key.
        if let Some(ref limiter) = self.rate_limiter {
            limiter.acquire().await;
        }

        let pending = PendingTx {
            raw: tx.raw,
            key: tx.key,
        };

        self.metrics.record_sent().await;

        // Get or create the queue for this key.
        let queue = match self.key_queues.entry(pending.key) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                let (sender, receiver) = mpsc::channel(1024);

                // Spawn a worker for this key.
                let providers = self.providers.clone();
                let metrics = self.metrics.clone();
                let semaphore = self.semaphore.clone();

                let handle = tokio::spawn(async move {
                    key_worker(receiver, providers, metrics, semaphore).await;
                });
                self.worker_handles.push(handle);

                e.insert(sender)
            }
        };

        if queue.send(pending).await.is_err() {
            tracing::warn!("Failed to enqueue transaction, worker channel closed");
            self.metrics.record_failure().await;
        }

        Ok(())
    }

    /// Wait for all pending transactions to complete.
    pub async fn flush(&mut self) {
        // Drop all senders to signal workers to stop.
        self.key_queues.clear();

        // Wait for all workers to finish processing.
        for handle in self.worker_handles.drain(..) {
            let _ = handle.await;
        }
    }
}

/// Maximum number of retries per transaction.
const MAX_RETRIES: u32 = 10;

/// Retry backoff delays (microseconds): 100µs, 500µs, 1ms, 5ms, 10ms, ...
const RETRY_BACKOFFS_US: [u64; 6] = [100, 500, 1_000, 5_000, 10_000, 50_000];

/// Worker that processes transactions for a single scheduling key.
async fn key_worker(
    mut receiver: mpsc::Receiver<PendingTx>,
    providers: Vec<DynProvider<Ethereum>>,
    metrics: Arc<MetricsCollector>,
    semaphore: Arc<Semaphore>,
) {
    while let Some(pending) = receiver.recv().await {
        // Acquire semaphore permit.
        let _permit = semaphore.acquire().await;

        // Pick a random provider for this request.
        // SAFETY: `providers` is guaranteed to be non-empty by construction.
        let provider = providers.choose(&mut rand::rng()).unwrap();

        let mut attempt = 0u32;
        loop {
            let start = Instant::now();

            match provider.send_raw_transaction(&pending.raw).await {
                Ok(_pending_tx) => {
                    let latency = start.elapsed();
                    if attempt > 0 {
                        tracing::debug!(attempts = attempt + 1, "Succeeded after retry");
                    }
                    metrics.record_success(latency).await;
                }
                Err(e) => {
                    attempt += 1;
                    if attempt <= MAX_RETRIES {
                        let backoff_idx = (attempt as usize - 1).min(RETRY_BACKOFFS_US.len() - 1);
                        let backoff = Duration::from_micros(RETRY_BACKOFFS_US[backoff_idx]);
                        tracing::debug!(
                            attempt,
                            error = %e,
                            backoff_us = backoff.as_micros() as u64,
                            "Retrying RPC error"
                        );
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    tracing::warn!(error = %e, attempts = attempt, "RPC request failed (exhausted retries)");
                    metrics.record_failure().await;
                }
            }
            break;
        }
    }
}

/// Token bucket rate limiter using scheduled times.
///
/// Tracks the *scheduled* next-token time rather than the last-wake time.
/// This eliminates throughput loss from sleep overshoot: if a sleep
/// overshoots by 500µs, subsequent tokens are issued immediately until
/// the schedule catches up.
struct RateLimiter {
    interval: Duration,
    next_token: tokio::sync::Mutex<Instant>,
}

impl RateLimiter {
    fn new(tokens_per_sec: u64) -> Self {
        Self {
            interval: Duration::from_secs_f64(1.0 / tokens_per_sec as f64),
            next_token: tokio::sync::Mutex::new(Instant::now()),
        }
    }

    async fn acquire(&self) {
        let mut next = self.next_token.lock().await;
        let now = Instant::now();

        if *next > now {
            tokio::time::sleep(*next - now).await;
        }

        // Advance from the scheduled time, not wall-clock, so we can
        // burst to catch up after sleep overshoot.
        *next = (*next).max(now) + self.interval;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sender_config_default() {
        let config = SenderConfig::default();
        assert_eq!(config.rpc_urls, vec!["http://localhost:8545"]);
        assert_eq!(config.rate_limit, 0);
        assert_eq!(config.max_concurrent, 100);
    }
}
