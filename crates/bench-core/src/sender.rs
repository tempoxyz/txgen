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
    sync::{mpsc, Semaphore},
    task::JoinHandle,
};
use txgen_core::{dedup_scheduling_keys, GeneratedTx, LateSignSpec, SchedulingKey, TxPhase};

/// A signer that materializes raw transaction bytes from a deferred-signing
/// envelope just before submission.
///
/// Implementations are network-specific (e.g. Tempo expiring nonces) and
/// dispatch on [`LateSignSpec::format`].
pub trait LateSigner: Send + Sync {
    /// Sign the deferred envelope, returning the RLP-encoded EIP-2718 envelope
    /// to submit. Called inside the sender's dispatch path, immediately before
    /// `eth_sendRawTransaction`.
    fn sign(&self, spec: &LateSignSpec) -> Result<Bytes>;
}

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

type SchedulingKeys = Vec<SchedulingKey>;

type KeySets = (SchedulingKeys, SchedulingKeys);

const RECEIPT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(300);

/// A transaction to be sent.
struct PendingTx {
    phase: TxPhase,
    id: Option<String>,
    raw: Bytes,
    late_sign: Option<LateSignSpec>,
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
    /// Worker task handles for awaiting completion.
    worker_handles: Vec<JoinHandle<()>>,
    /// Rate limiter tokens.
    rate_limiter: Option<Arc<RateLimiter>>,
    /// Optional signer for transactions carrying a [`LateSignSpec`] envelope.
    late_signer: Option<Arc<dyn LateSigner>>,
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
            late_signer: None,
        }
    }

    /// Register a late signer for transactions whose `raw` bytes are
    /// materialized just before submission (e.g. Tempo expiring nonces).
    ///
    /// Returns the modified sender to allow builder-style chaining.
    pub fn with_late_signer(mut self, signer: Arc<dyn LateSigner>) -> Self {
        self.late_signer = Some(signer);
        self
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

        let GeneratedTx { phase, id, raw, late_sign, submission_keys, inclusion_keys } = tx;
        let (submission_keys, inclusion_keys) =
            normalize_key_sets(submission_keys, inclusion_keys)?;
        self.pending.push_back(PendingTx {
            phase,
            id,
            raw,
            late_sign,
            submission_keys,
            inclusion_keys,
        });
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

    fn release_keys(&mut self, keys: &[SchedulingKey]) {
        for key in keys {
            self.active_keys.remove(key);
        }
    }

    fn schedule_ready(&mut self) {
        let mut blocked_keys = self.active_keys.clone();
        let mut index = 0;

        while index < self.pending.len() {
            let is_blocked =
                self.pending[index].scheduling_keys().any(|key| blocked_keys.contains(key));

            if is_blocked {
                for key in self.pending[index].scheduling_keys() {
                    blocked_keys.insert(*key);
                }
                index += 1;
            } else {
                let pending = self.pending.remove(index).expect("pending index exists");
                for key in pending.scheduling_keys() {
                    self.active_keys.insert(*key);
                    blocked_keys.insert(*key);
                }
                self.dispatch(pending);
            }
        }
    }

    fn dispatch(&mut self, pending: PendingTx) {
        let providers = self.providers.clone();
        let metrics = self.metrics.clone();
        let semaphore = self.semaphore.clone();
        let completion_tx = self.completion_tx.clone();
        let late_signer = self.late_signer.clone();

        let handle = tokio::spawn(async move {
            submit_tx(pending, providers, metrics, semaphore, completion_tx, late_signer).await;
        });
        self.worker_handles.push(handle);
    }
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
    semaphore: Arc<Semaphore>,
    completion_tx: mpsc::UnboundedSender<SchedulingKeys>,
    late_signer: Option<Arc<dyn LateSigner>>,
) {
    let release_all_keys = || {
        let mut keys = pending.submission_keys.clone();
        keys.extend_from_slice(&pending.inclusion_keys);
        keys
    };

    let Ok(permit) = semaphore.acquire().await else {
        tracing::warn!("Failed to acquire concurrency permit");
        metrics.record_failure();
        release_keys(&completion_tx, release_all_keys());
        return;
    };

    metrics.record_sent();

    // Materialize the raw bytes from the late-sign envelope (if any) here,
    // immediately before submission. This minimizes the gap between signing
    // any time-sensitive fields (e.g. Tempo `valid_before`) and the RPC
    // accepting the tx.
    let raw = match resolve_raw(&pending, late_signer.as_deref()) {
        Ok(raw) => raw,
        Err(e) => {
            tracing::error!(
                id = pending.id.as_deref(),
                phase = ?pending.phase,
                error = %e,
                "Failed to materialize raw bytes from late-sign envelope",
            );
            metrics.record_failure();
            release_keys(&completion_tx, release_all_keys());
            return;
        }
    };

    // Pick a random provider for this request.
    // SAFETY: `providers` is guaranteed to be non-empty by construction.
    let provider = providers.choose(&mut rand::rng()).unwrap();

    let start = Instant::now();
    let tx_hash = match provider.send_raw_transaction(&raw).await {
        Ok(pending_tx) => {
            metrics.record_success(start.elapsed());
            *pending_tx.tx_hash()
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to send transaction");
            metrics.record_failure();
            release_keys(&completion_tx, release_all_keys());
            return;
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

fn release_keys(completion_tx: &mpsc::UnboundedSender<SchedulingKeys>, keys: SchedulingKeys) {
    if !keys.is_empty() {
        let _ = completion_tx.send(keys);
    }
}

/// Materialize the RLP-encoded EIP-2718 envelope to submit.
///
/// If `pending.late_sign` is set, this invokes the registered [`LateSigner`]
/// (and errors if none is registered). Otherwise it returns the pre-signed
/// `pending.raw` bytes.
fn resolve_raw(pending: &PendingTx, late_signer: Option<&dyn LateSigner>) -> Result<Bytes> {
    if let Some(spec) = &pending.late_sign {
        let signer = late_signer.ok_or_else(|| {
            eyre::eyre!(
                "transaction has `late_sign` envelope (format `{}`) but sender has no late signer registered",
                spec.format
            )
        })?;
        signer.sign(spec)
    } else {
        Ok(pending.raw.clone())
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
