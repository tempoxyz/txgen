//! Transaction sender with scheduling key ordering and rate limiting.
//!
//! Handles sending transactions to an RPC endpoint while:
//! - Respecting scheduling key ordering (same key = sequential, different key = parallel)
//! - Applying rate limiting

use crate::metrics::MetricsCollector;
use alloy_network::Ethereum;
use alloy_primitives::Bytes;
use alloy_provider::{DynProvider, Provider};
use eyre::Result;
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
    /// Maximum transactions submitted per second (0 = unlimited).
    ///
    /// Controls throughput via a token bucket. Provides backpressure to the
    /// transaction source before enqueueing.
    pub rate_limit: u64,
    /// Maximum number of RPC requests in flight simultaneously.
    ///
    /// Controls parallelism via a semaphore. Limits how many connections are
    /// open at once, independently of the rate limit.
    pub max_concurrent: usize,
}

impl Default for SenderConfig {
    fn default() -> Self {
        Self {
            rate_limit: 0,
            max_concurrent: 100,
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
    pub fn new(
        providers: Vec<DynProvider<Ethereum>>,
        config: SenderConfig,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent));

        let rate_limiter = if config.rate_limit > 0 {
            Some(Arc::new(RateLimiter::new(config.rate_limit)))
        } else {
            None
        };

        Self {
            providers,
            metrics,
            semaphore,
            key_queues: HashMap::new(),
            worker_handles: Vec::new(),
            rate_limiter,
        }
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

        let start = Instant::now();
        match provider.send_raw_transaction(&pending.raw).await {
            Ok(_pending_tx) => {
                metrics.record_success(start.elapsed()).await;
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to send transaction");
                metrics.record_failure().await;
            }
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
        assert_eq!(config.rate_limit, 0);
        assert_eq!(config.max_concurrent, 100);
    }
}
