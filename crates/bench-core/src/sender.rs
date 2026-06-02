//! Transaction sender with scheduling key ordering and rate limiting.
//!
//! Handles sending transactions to an RPC endpoint while:
//! - Respecting scheduling key ordering (shared key = sequential, disjoint keys = parallel)
//! - Applying rate limiting

use crate::metrics::MetricsCollector;
use alloy_network::{primitives::ReceiptResponse, AnyNetwork};
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
    sync::{mpsc, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
};
use txgen_core::{dedup_scheduling_keys, GeneratedTx, SchedulingKey, TxPhase};

/// Configuration for the sender.
#[derive(Debug, Clone)]
pub struct SenderConfig {
    /// Maximum transactions submitted per second (0 = unlimited).
    ///
    /// Controls RPC submission throughput via a token bucket. Provides
    /// backpressure to the transaction source when submissions cannot keep up.
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

type SchedulingKeys = Vec<SchedulingKey>;

type KeySets = (SchedulingKeys, SchedulingKeys);

const RECEIPT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(300);
const PENDING_BACKLOG_FACTOR: usize = 4;

/// A transaction to be sent.
struct PendingTx {
    phase: TxPhase,
    id: Option<String>,
    raw: Bytes,
    submission_keys: SchedulingKeys,
    inclusion_keys: SchedulingKeys,
}

impl PendingTx {
    fn scheduling_keys(&self) -> impl Iterator<Item = &SchedulingKey> {
        self.submission_keys.iter().chain(self.inclusion_keys.iter())
    }
}

/// Transaction sender.
pub struct Sender {
    providers: Vec<DynProvider<AnyNetwork>>,
    metrics: Arc<MetricsCollector>,
    semaphore: Arc<Semaphore>,
    /// Transactions waiting for all of their scheduling keys to become free.
    pending: VecDeque<PendingTx>,
    /// Scheduling keys currently held by dispatched transactions.
    active_keys: HashSet<SchedulingKey>,
    completion_tx: mpsc::UnboundedSender<SchedulingKeys>,
    completion_rx: mpsc::UnboundedReceiver<SchedulingKeys>,
    /// Worker tasks for awaiting completion and reaping completed task state.
    worker_tasks: JoinSet<()>,
    /// Rate limiter tokens.
    rate_limiter: Option<Arc<RateLimiter>>,
    /// Maximum number of transactions buffered internally before applying
    /// backpressure to the transaction source.
    max_buffered: usize,
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

        let max_buffered = max_buffered_transactions(&config);
        let (completion_tx, completion_rx) = mpsc::unbounded_channel();

        Self {
            providers,
            metrics,
            semaphore,
            pending: VecDeque::new(),
            active_keys: HashSet::new(),
            completion_tx,
            completion_rx,
            worker_tasks: JoinSet::new(),
            rate_limiter,
            max_buffered,
        }
    }

    /// Send a transaction.
    ///
    /// This respects scheduling key ordering: transactions that share any key
    /// are sent sequentially, while transactions with disjoint key sets can be
    /// sent in parallel.
    pub async fn send(&mut self, tx: GeneratedTx) -> Result<()> {
        self.wait_for_buffer_capacity().await;

        let GeneratedTx { phase, id, raw, submission_keys, inclusion_keys } = tx;
        let (submission_keys, inclusion_keys) =
            normalize_key_sets(submission_keys, inclusion_keys)?;
        self.pending.push_back(PendingTx { phase, id, raw, submission_keys, inclusion_keys });
        self.pump().await;

        Ok(())
    }

    /// Wait for all pending transactions to complete.
    pub async fn flush(&mut self) {
        self.pump().await;

        while !self.pending.is_empty() || !self.active_keys.is_empty() {
            match self.completion_rx.recv().await {
                Some(keys) => {
                    self.release_keys(&keys);
                    self.pump().await;
                }
                None => break,
            }
        }

        while let Some(result) = self.worker_tasks.join_next().await {
            if let Err(err) = result {
                tracing::warn!(%err, "sender worker task failed");
            }
        }
    }

    fn drain_completions(&mut self) {
        while let Ok(keys) = self.completion_rx.try_recv() {
            self.release_keys(&keys);
        }
        self.reap_worker_tasks();
    }

    fn reap_worker_tasks(&mut self) {
        while let Some(result) = self.worker_tasks.try_join_next() {
            if let Err(err) = result {
                tracing::warn!(%err, "sender worker task failed");
            }
        }
    }

    fn release_keys(&mut self, keys: &[SchedulingKey]) {
        for key in keys {
            self.active_keys.remove(key);
        }
    }

    async fn wait_for_buffer_capacity(&mut self) {
        while self.pending.len() >= self.max_buffered {
            self.pump().await;
            if self.pending.len() < self.max_buffered {
                break;
            }

            match self.completion_rx.recv().await {
                Some(keys) => self.release_keys(&keys),
                None => break,
            }
        }
    }

    async fn pump(&mut self) {
        loop {
            self.drain_completions();

            let Some(mut index) = self.next_ready_index() else {
                break;
            };

            let permit = match self.semaphore.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    let Ok(permit) = self.semaphore.clone().acquire_owned().await else {
                        break;
                    };
                    self.drain_completions();
                    let Some(next_index) = self.next_ready_index() else {
                        drop(permit);
                        break;
                    };
                    index = next_index;
                    permit
                }
            };

            if let Some(limiter) = &self.rate_limiter &&
                let Some(delay) = limiter.try_acquire_or_delay().await
            {
                drop(permit);
                tokio::time::sleep(delay).await;
                continue;
            }

            let pending = self.pending.remove(index).expect("pending index exists");
            self.activate_keys(&pending);
            self.dispatch(pending, permit);
        }
    }

    fn next_ready_index(&self) -> Option<usize> {
        let mut blocked_keys = self.active_keys.clone();

        for (index, pending) in self.pending.iter().enumerate() {
            let is_blocked = pending.scheduling_keys().any(|key| blocked_keys.contains(key));

            if is_blocked {
                for key in pending.scheduling_keys() {
                    blocked_keys.insert(*key);
                }
            } else {
                return Some(index);
            }
        }

        None
    }

    fn activate_keys(&mut self, pending: &PendingTx) {
        for key in pending.scheduling_keys() {
            self.active_keys.insert(*key);
        }
    }

    fn dispatch(&mut self, pending: PendingTx, permit: OwnedSemaphorePermit) {
        let providers = self.providers.clone();
        let metrics = self.metrics.clone();
        let completion_tx = self.completion_tx.clone();

        self.worker_tasks.spawn(async move {
            submit_tx(pending, providers, metrics, permit, completion_tx).await;
        });
    }
}

fn max_buffered_transactions(config: &SenderConfig) -> usize {
    config.max_concurrent.saturating_mul(PENDING_BACKLOG_FACTOR).max(1)
}

fn normalize_key_sets(
    submission_keys: SchedulingKeys,
    inclusion_keys: SchedulingKeys,
) -> Result<KeySets> {
    let inclusion_keys = dedup_scheduling_keys(inclusion_keys);
    let mut submission_keys = dedup_scheduling_keys(submission_keys);

    // If a key appears in both sets, keep the stricter release policy.
    submission_keys.retain(|key| !inclusion_keys.contains(key));

    if submission_keys.is_empty() && inclusion_keys.is_empty() {
        eyre::bail!("transactions must have at least one submission or inclusion key");
    }

    Ok((submission_keys, inclusion_keys))
}

async fn submit_tx(
    pending: PendingTx,
    providers: Vec<DynProvider<AnyNetwork>>,
    metrics: Arc<MetricsCollector>,
    permit: OwnedSemaphorePermit,
    completion_tx: mpsc::UnboundedSender<SchedulingKeys>,
) {
    let release_all_keys = || {
        let mut keys = pending.submission_keys.clone();
        keys.extend_from_slice(&pending.inclusion_keys);
        keys
    };

    metrics.record_sent();

    // Pick a random provider for this request.
    // SAFETY: `providers` is guaranteed to be non-empty by construction.
    let provider = providers.choose(&mut rand::rng()).unwrap();

    let start = Instant::now();
    let tx_hash = match provider.send_raw_transaction(&pending.raw).await {
        Ok(pending_tx) => {
            metrics.record_success(start.elapsed());
            *pending_tx.tx_hash()
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to send transaction");
            metrics.record_failure();
            release_keys(&completion_tx, release_all_keys());
            drop(permit);
            return;
        }
    };

    release_keys(&completion_tx, pending.submission_keys);
    drop(permit);

    if pending.inclusion_keys.is_empty() && pending.phase != TxPhase::Setup {
        return;
    }

    match wait_for_receipt(provider, tx_hash).await {
        Ok(true) => {}
        Ok(false) => {
            tracing::error!(id = pending.id.as_deref(), phase = ?pending.phase, %tx_hash, "Transaction reverted");
            metrics.record_failure();
        }
        Err(e) => {
            tracing::error!(error = %e, %tx_hash, "Failed waiting for transaction receipt");
            metrics.record_failure();
        }
    }

    release_keys(&completion_tx, pending.inclusion_keys);
}

fn release_keys(completion_tx: &mpsc::UnboundedSender<SchedulingKeys>, keys: SchedulingKeys) {
    if !keys.is_empty() {
        let _ = completion_tx.send(keys);
    }
}

async fn wait_for_receipt(
    provider: &DynProvider<AnyNetwork>,
    tx_hash: alloy_primitives::TxHash,
) -> Result<bool> {
    let deadline = tokio::time::Instant::now() + RECEIPT_TIMEOUT;

    loop {
        if let Some(receipt) = provider.get_transaction_receipt(tx_hash).await? {
            return Ok(receipt.status());
        }

        if tokio::time::Instant::now() >= deadline {
            eyre::bail!("timed out waiting for transaction receipt");
        }

        tokio::time::sleep(RECEIPT_POLL_INTERVAL).await;
    }
}

const RATE_LIMITER_TICK: Duration = Duration::from_millis(1);

/// Millisecond-batched rate limiter.
///
/// This deliberately does not accumulate catch-up credit. If submissions stall
/// behind RPC concurrency or source backpressure, only one tick worth of credit
/// is retained. That keeps high-rate runs from collapsing into large sawtooth
/// bursts without requiring a sub-millisecond timer wakeup per transaction.
struct RateLimiter {
    tokens_per_tick: f64,
    max_tokens: f64,
    state: tokio::sync::Mutex<RateLimiterState>,
}

struct RateLimiterState {
    tokens: f64,
    next_refill: Instant,
}

impl RateLimiter {
    fn new(tokens_per_sec: u64) -> Self {
        let tokens_per_tick = tokens_per_sec as f64 * RATE_LIMITER_TICK.as_secs_f64();
        let max_tokens = tokens_per_tick.ceil().max(1.0);

        Self {
            tokens_per_tick,
            max_tokens,
            state: tokio::sync::Mutex::new(RateLimiterState {
                tokens: max_tokens,
                next_refill: Instant::now() + RATE_LIMITER_TICK,
            }),
        }
    }

    async fn try_acquire_or_delay(&self) -> Option<Duration> {
        let mut state = self.state.lock().await;
        state.refill(self.tokens_per_tick, self.max_tokens);

        if state.tokens >= 1.0 {
            state.tokens -= 1.0;
            None
        } else {
            Some(state.next_refill.saturating_duration_since(Instant::now()))
        }
    }
}

impl RateLimiterState {
    fn refill(&mut self, tokens_per_tick: f64, max_tokens: f64) {
        let now = Instant::now();
        if now < self.next_refill {
            return;
        }

        let ticks = elapsed_ticks(now, self.next_refill, RATE_LIMITER_TICK);
        self.tokens = (self.tokens + tokens_per_tick * ticks as f64).min(max_tokens);
        self.next_refill += multiply_duration(RATE_LIMITER_TICK, ticks);
    }
}

fn elapsed_ticks(now: Instant, next_refill: Instant, tick: Duration) -> u64 {
    let tick_nanos = tick.as_nanos().max(1);
    let ticks = now.duration_since(next_refill).as_nanos() / tick_nanos + 1;
    ticks.min(u64::MAX as u128) as u64
}

fn multiply_duration(duration: Duration, multiplier: u64) -> Duration {
    let nanos = duration.as_nanos().saturating_mul(multiplier as u128);
    Duration::from_nanos(nanos.min(u64::MAX as u128) as u64)
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

    #[test]
    fn test_rate_limiter_batches_one_tick_of_tokens() {
        let limiter = RateLimiter::new(50_000);
        assert_eq!(limiter.tokens_per_tick, 50.0);
        assert_eq!(limiter.max_tokens, 50.0);
    }

    #[tokio::test]
    async fn test_rate_limiter_exhausts_tick_batch_before_waiting() {
        let limiter = RateLimiter::new(50_000);

        for _ in 0..50 {
            assert_eq!(limiter.try_acquire_or_delay().await, None);
        }

        let delay = limiter.try_acquire_or_delay().await.expect("batch should be exhausted");
        assert!(delay <= Duration::from_millis(1), "delay should be bounded by tick: {delay:?}");
    }

    #[tokio::test]
    async fn test_rate_limiter_does_not_accumulate_large_catch_up_burst() {
        let limiter = RateLimiter::new(50_000);

        {
            let mut state = limiter.state.lock().await;
            state.tokens = 0.0;
            state.next_refill = Instant::now() - Duration::from_secs(1);
        }

        for _ in 0..50 {
            assert_eq!(limiter.try_acquire_or_delay().await, None);
        }

        assert!(
            limiter.try_acquire_or_delay().await.is_some(),
            "stalled limiter should retain at most one tick of credit"
        );
    }

    #[test]
    fn test_max_buffered_transactions_uses_existing_sender_knobs() {
        let config = SenderConfig { rate_limit: 10_000, max_concurrent: 100 };
        assert_eq!(max_buffered_transactions(&config), 400);

        let config = SenderConfig { rate_limit: 100_000, max_concurrent: 100 };
        assert_eq!(max_buffered_transactions(&config), 400);
    }
}
