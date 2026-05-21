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
    /// Maximum number of retries after the initial submit attempt.
    ///
    /// `None` retries forever. `Some(0)` submits once without retrying.
    pub retries: Option<u64>,
}

impl Default for SenderConfig {
    fn default() -> Self {
        Self { rate_limit: 0, max_concurrent: 100, retries: None }
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
    /// Maximum number of retries after the initial submit attempt.
    retries: Option<u64>,
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

        let max_buffered = max_buffered_transactions(&config, rate_limiter.as_deref());
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
            retries: config.retries,
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

            let Some(index) = self.next_ready_index() else {
                break;
            };

            let Ok(permit) = self.semaphore.clone().try_acquire_owned() else {
                break;
            };

            if let Some(limiter) = &self.rate_limiter
                && let Some(delay) = limiter.try_acquire_or_delay().await
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
        let retries = self.retries;

        self.worker_tasks.spawn(async move {
            submit_tx(pending, providers, metrics, permit, completion_tx, retries).await;
        });
    }
}

fn max_buffered_transactions(config: &SenderConfig, limiter: Option<&RateLimiter>) -> usize {
    let concurrency_buffer = config.max_concurrent.saturating_mul(PENDING_BACKLOG_FACTOR).max(1);
    let burst_buffer =
        limiter.map(|l| (l.burst_capacity.ceil() as usize).saturating_mul(2)).unwrap_or(0);
    concurrency_buffer.max(burst_buffer)
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
    retries: Option<u64>,
) {
    let release_all_keys = || {
        let mut keys = pending.submission_keys.clone();
        keys.extend_from_slice(&pending.inclusion_keys);
        keys
    };

    // Pick a random provider for this request.
    // SAFETY: `providers` is guaranteed to be non-empty by construction.
    let provider = providers.choose(&mut rand::rng()).unwrap();

    let start = Instant::now();
    let mut attempt = 0;
    let tx_hash = loop {
        metrics.record_sent();
        match provider.send_raw_transaction(&pending.raw).await {
            Ok(pending_tx) => {
                metrics.record_success(start.elapsed());
                break *pending_tx.tx_hash();
            }
            Err(e) if should_retry(attempt, retries) => {
                attempt += 1;
                tracing::warn!(error = %e, attempt, "Failed to send transaction, retrying");
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            Err(e) => {
                tracing::warn!(error = %e, attempt, "Failed to send transaction");
                metrics.record_failure();
                drop(permit);
                release_keys(&completion_tx, release_all_keys());
                return;
            }
        }
    };

    drop(permit);

    release_keys(&completion_tx, pending.submission_keys);

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

fn should_retry(attempt: u64, retries: Option<u64>) -> bool {
    retries.is_none_or(|retries| attempt < retries)
}

fn retry_delay(attempt: u64) -> Duration {
    let millis = 100_u64.saturating_mul(2_u64.saturating_pow(attempt.min(5) as u32));
    Duration::from_millis(millis.min(1_000))
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

const RATE_LIMITER_MAX_BURST: Duration = Duration::from_millis(10);

/// Token-bucket rate limiter with a bounded burst budget.
///
/// A pure cumulative scheduler catches up all missed tokens after startup or
/// source stalls, which can create visible send-rate spikes. This limiter still
/// batches enough tokens to avoid sub-millisecond sleeps at high TPS, but caps
/// accumulated credit to a small time window.
struct RateLimiter {
    rate: f64,
    burst_capacity: f64,
    state: tokio::sync::Mutex<RateLimiterState>,
}

struct RateLimiterState {
    tokens: f64,
    last_refill: Instant,
}

impl RateLimiter {
    fn new(tokens_per_sec: u64) -> Self {
        let rate = tokens_per_sec as f64;
        let burst_capacity = (rate * RATE_LIMITER_MAX_BURST.as_secs_f64()).max(1.0);

        Self {
            rate,
            burst_capacity,
            state: tokio::sync::Mutex::new(RateLimiterState {
                tokens: burst_capacity,
                last_refill: Instant::now(),
            }),
        }
    }

    async fn try_acquire_or_delay(&self) -> Option<Duration> {
        let mut state = self.state.lock().await;
        state.refill(self.rate, self.burst_capacity);

        if state.tokens >= 1.0 {
            state.tokens -= 1.0;
            None
        } else {
            let missing_tokens = 1.0 - state.tokens;
            Some(Duration::from_secs_f64(missing_tokens / self.rate))
        }
    }
}

impl RateLimiterState {
    fn refill(&mut self, rate: f64, burst_capacity: f64) {
        let now = Instant::now();
        let new_tokens = now.duration_since(self.last_refill).as_secs_f64() * rate;
        self.tokens = (self.tokens + new_tokens).min(burst_capacity);
        self.last_refill = now;
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

    #[test]
    fn test_rate_limiter_burst_capacity_is_bounded() {
        let limiter = RateLimiter::new(10_000);
        assert_eq!(limiter.burst_capacity, 100.0);
    }

    #[tokio::test]
    async fn test_rate_limiter_does_not_accumulate_unbounded_catch_up_credit() {
        let limiter = RateLimiter::new(1_000);
        assert_eq!(limiter.burst_capacity, 10.0);

        {
            let mut state = limiter.state.lock().await;
            state.tokens = 0.0;
            state.last_refill = Instant::now() - Duration::from_secs(1);
        }

        assert_eq!(limiter.try_acquire_or_delay().await, None);

        let state = limiter.state.lock().await;
        assert!(state.tokens <= 9.0, "tokens: {}", state.tokens);
        assert!(state.tokens > 8.0, "tokens: {}", state.tokens);
    }

    #[test]
    fn test_max_buffered_transactions_uses_existing_sender_knobs() {
        let config = SenderConfig { rate_limit: 10_000, max_concurrent: 100, retries: None };
        let limiter = RateLimiter::new(config.rate_limit);
        assert_eq!(max_buffered_transactions(&config, Some(&limiter)), 400);

        let config = SenderConfig { rate_limit: 100_000, max_concurrent: 100, retries: None };
        let limiter = RateLimiter::new(config.rate_limit);
        assert_eq!(max_buffered_transactions(&config, Some(&limiter)), 2_000);
    }
}
