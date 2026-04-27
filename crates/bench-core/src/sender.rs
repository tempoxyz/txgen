//! Transaction sender with scheduling key ordering and rate limiting.
//!
//! Handles sending transactions to an RPC endpoint while:
//! - Respecting scheduling key ordering (shared key = sequential, disjoint keys = parallel)
//! - Applying rate limiting

use crate::metrics::MetricsCollector;
use alloy_network::AnyNetwork;
use alloy_primitives::Bytes;
use alloy_provider::{DynProvider, Provider};
use eyre::Result;
use rand::seq::IndexedRandom;
use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, Semaphore},
    task::JoinHandle,
};
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
        Self { rate_limit: 0, max_concurrent: 100 }
    }
}

/// A transaction to be sent.
struct PendingTx {
    raw: Bytes,
    scheduling_keys: Vec<[u8; 20]>,
}

/// Transaction sender.
pub struct Sender {
    providers: Vec<DynProvider<AnyNetwork>>,
    metrics: Arc<MetricsCollector>,
    semaphore: Arc<Semaphore>,
    /// Transactions waiting for all of their scheduling keys to become free.
    pending: VecDeque<PendingTx>,
    /// Scheduling keys currently held by dispatched transactions.
    active_keys: HashSet<[u8; 20]>,
    completion_tx: mpsc::UnboundedSender<Vec<[u8; 20]>>,
    completion_rx: mpsc::UnboundedReceiver<Vec<[u8; 20]>>,
    /// Worker task handles for awaiting completion.
    worker_handles: Vec<JoinHandle<()>>,
    /// Rate limiter tokens.
    rate_limiter: Option<Arc<RateLimiter>>,
}

impl Sender {
    /// Create a new sender.
    pub fn new(
        providers: Vec<DynProvider<AnyNetwork>>,
        config: SenderConfig,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent));

        let rate_limiter = if config.rate_limit > 0 {
            Some(Arc::new(RateLimiter::new(config.rate_limit)))
        } else {
            None
        };

        let (completion_tx, completion_rx) = mpsc::unbounded_channel();

        Self {
            providers,
            metrics,
            semaphore,
            pending: VecDeque::new(),
            active_keys: HashSet::new(),
            completion_tx,
            completion_rx,
            worker_handles: Vec::new(),
            rate_limiter,
        }
    }

    /// Send a transaction.
    ///
    /// This respects scheduling key ordering: transactions that share any key
    /// are sent sequentially, while transactions with disjoint key sets can be
    /// sent in parallel.
    pub async fn send(&mut self, tx: GeneratedTx) -> Result<()> {
        // Apply rate limiting before enqueueing to provide backpressure
        // to the source reader. This makes the rate limit global rather
        // than per-key.
        if let Some(ref limiter) = self.rate_limiter {
            limiter.acquire().await;
        }

        self.drain_completions();

        let scheduling_keys = normalize_keys(tx.scheduling_keys)?;
        self.pending.push_back(PendingTx { raw: tx.raw, scheduling_keys });
        self.schedule_ready();

        Ok(())
    }

    /// Wait for all pending transactions to complete.
    pub async fn flush(&mut self) {
        self.drain_completions();
        self.schedule_ready();

        while !self.pending.is_empty() || !self.active_keys.is_empty() {
            match self.completion_rx.recv().await {
                Some(keys) => {
                    self.release_keys(&keys);
                    self.drain_completions();
                    self.schedule_ready();
                }
                None => break,
            }
        }

        for handle in self.worker_handles.drain(..) {
            let _ = handle.await;
        }
    }

    fn drain_completions(&mut self) {
        while let Ok(keys) = self.completion_rx.try_recv() {
            self.release_keys(&keys);
        }
    }

    fn release_keys(&mut self, keys: &[[u8; 20]]) {
        for key in keys {
            self.active_keys.remove(key);
        }
    }

    fn schedule_ready(&mut self) {
        let mut blocked_keys = self.active_keys.clone();
        let mut index = 0;

        while index < self.pending.len() {
            let is_ready =
                self.pending[index].scheduling_keys.iter().all(|key| !blocked_keys.contains(key));

            if is_ready {
                let pending = self.pending.remove(index).expect("pending index exists");
                for key in &pending.scheduling_keys {
                    self.active_keys.insert(*key);
                    blocked_keys.insert(*key);
                }
                self.dispatch(pending);
            } else {
                for key in &self.pending[index].scheduling_keys {
                    blocked_keys.insert(*key);
                }
                index += 1;
            }
        }
    }

    fn dispatch(&mut self, pending: PendingTx) {
        let providers = self.providers.clone();
        let metrics = self.metrics.clone();
        let semaphore = self.semaphore.clone();
        let completion_tx = self.completion_tx.clone();

        let handle = tokio::spawn(async move {
            submit_tx(pending, providers, metrics, semaphore, completion_tx).await;
        });
        self.worker_handles.push(handle);
    }
}

fn normalize_keys(keys: Vec<[u8; 20]>) -> Result<Vec<[u8; 20]>> {
    if keys.is_empty() {
        eyre::bail!("scheduling_keys must not be empty");
    }

    let mut normalized = Vec::with_capacity(keys.len());
    for key in keys {
        if !normalized.contains(&key) {
            normalized.push(key);
        }
    }
    Ok(normalized)
}

async fn submit_tx(
    pending: PendingTx,
    providers: Vec<DynProvider<AnyNetwork>>,
    metrics: Arc<MetricsCollector>,
    semaphore: Arc<Semaphore>,
    completion_tx: mpsc::UnboundedSender<Vec<[u8; 20]>>,
) {
    let completion_keys = pending.scheduling_keys.clone();

    match semaphore.acquire().await {
        Ok(_permit) => {
            metrics.record_sent();

            // Pick a random provider for this request.
            // SAFETY: `providers` is guaranteed to be non-empty by construction.
            let provider = providers.choose(&mut rand::rng()).unwrap();

            let start = Instant::now();
            match provider.send_raw_transaction(&pending.raw).await {
                Ok(_pending_tx) => {
                    metrics.record_success(start.elapsed());
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to send transaction");
                    metrics.record_failure();
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to acquire concurrency permit");
            metrics.record_failure();
        }
    }

    let _ = completion_tx.send(completion_keys);
}

/// Token-budget rate limiter.
///
/// Instead of sleeping per-token (which breaks at high TPS due to timer
/// resolution), this tracks how many tokens *should* have been issued by
/// now based on elapsed time. Tokens are granted instantly while the
/// budget allows; the caller only sleeps when it gets ahead of schedule.
struct RateLimiter {
    rate: f64,
    start: Instant,
    state: tokio::sync::Mutex<RateLimiterState>,
}

struct RateLimiterState {
    issued: u64,
}

impl RateLimiter {
    fn new(tokens_per_sec: u64) -> Self {
        Self {
            rate: tokens_per_sec as f64,
            start: Instant::now(),
            state: tokio::sync::Mutex::new(RateLimiterState { issued: 0 }),
        }
    }

    async fn acquire(&self) {
        let mut state = self.state.lock().await;

        // The time at which this token *should* be issued.
        let expected = Duration::from_secs_f64(state.issued as f64 / self.rate);
        let elapsed = self.start.elapsed();

        if expected > elapsed {
            tokio::time::sleep(expected - elapsed).await;
        }

        state.issued += 1;
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
