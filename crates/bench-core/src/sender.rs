//! Transaction sender with scheduling key ordering and rate limiting.
//!
//! Handles sending transactions to an RPC endpoint while:
//! - Respecting scheduling key ordering (shared key = sequential, disjoint keys = parallel)
//! - Applying rate limiting

use crate::{metrics::MetricsCollector, RequestAuthProvider, RpcRequestContext};
use alloy_network::{primitives::ReceiptResponse, AnyNetwork, AnyTransactionReceipt};
use alloy_primitives::{Address, Bytes, TxHash};
use alloy_provider::{DynProvider, Provider};
use eyre::{Context, Result};
use rand::seq::IndexedRandom;
use reqwest::header::HeaderMap;
use std::{
    collections::{HashSet, VecDeque},
    fmt,
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

/// A submission RPC provider and the identity exposed to request authentication.
#[derive(Clone)]
pub struct RpcEndpoint {
    identity: Arc<str>,
    provider: DynProvider<AnyNetwork>,
}

impl RpcEndpoint {
    /// Create an endpoint with an opaque identity and provider.
    pub fn new(identity: impl Into<Arc<str>>, provider: DynProvider<AnyNetwork>) -> Self {
        Self { identity: identity.into(), provider }
    }

    /// Return this endpoint's authentication identity.
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// Return the Alloy provider for this endpoint.
    pub fn provider(&self) -> &DynProvider<AnyNetwork> {
        &self.provider
    }
}

impl fmt::Debug for RpcEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcEndpoint").field("identity", &self.identity).finish_non_exhaustive()
    }
}

/// A transaction to be sent.
struct PendingTx {
    queue_id: u64,
    phase: TxPhase,
    id: Option<String>,
    raw: Bytes,
    sender: Option<Address>,
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
    endpoints: Vec<RpcEndpoint>,
    request_auth: Option<Arc<dyn RequestAuthProvider>>,
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
    /// Monotonic identity used to remove a newly queued transaction if
    /// dispatch preparation reports an error.
    next_queue_id: u64,
    /// Authentication failures discovered after the `send` call's own
    /// transaction was already dispatched. These are reported by `flush`.
    deferred_errors: VecDeque<eyre::Report>,
}

impl Sender {
    /// Create a new sender.
    pub fn new(
        providers: Vec<DynProvider<AnyNetwork>>,
        config: SenderConfig,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        let endpoints = providers
            .into_iter()
            .enumerate()
            .map(|(index, provider)| RpcEndpoint::new(format!("rpc-{index}"), provider))
            .collect();
        Self::new_with_request_auth(endpoints, config, metrics, None)
    }

    /// Create a sender with request-scoped authentication.
    pub fn new_with_request_auth(
        endpoints: Vec<RpcEndpoint>,
        config: SenderConfig,
        metrics: Arc<MetricsCollector>,
        request_auth: Option<Arc<dyn RequestAuthProvider>>,
    ) -> Self {
        assert!(!endpoints.is_empty(), "sender requires at least one RPC endpoint");
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent));

        let rate_limiter = if config.rate_limit > 0 {
            Some(Arc::new(RateLimiter::new(config.rate_limit)))
        } else {
            None
        };

        let max_buffered = max_buffered_transactions(&config, rate_limiter.as_deref());
        let (completion_tx, completion_rx) = mpsc::unbounded_channel();

        Self {
            endpoints,
            request_auth,
            metrics,
            semaphore,
            pending: VecDeque::new(),
            active_keys: HashSet::new(),
            completion_tx,
            completion_rx,
            worker_tasks: JoinSet::new(),
            rate_limiter,
            max_buffered,
            next_queue_id: 0,
            deferred_errors: VecDeque::new(),
        }
    }

    /// Send a transaction.
    ///
    /// This respects scheduling key ordering: transactions that share any key
    /// are sent sequentially, while transactions with disjoint key sets can be
    /// sent in parallel.
    pub async fn send(&mut self, tx: GeneratedTx) -> Result<()> {
        if let Some(error) = self.deferred_errors.pop_front() {
            // A previous call dispatched its own transaction before discovering
            // an older authentication failure. Report that failure before
            // accepting more work, and cancel everything that has not reached
            // the wire so a missing nonce cannot be skipped implicitly.
            self.deferred_errors.clear();
            self.pending.clear();
            return Err(error);
        }

        self.wait_for_buffer_capacity().await?;

        let queue_id = self.next_queue_id;
        self.next_queue_id = self
            .next_queue_id
            .checked_add(1)
            .ok_or_else(|| eyre::eyre!("sender transaction queue identity overflowed"))?;
        let GeneratedTx { phase, id, raw, sender, submission_keys, inclusion_keys } = tx;
        let (submission_keys, inclusion_keys) =
            normalize_key_sets(submission_keys, inclusion_keys)?;
        self.pending.push_back(PendingTx {
            queue_id,
            phase,
            id,
            raw,
            sender,
            submission_keys,
            inclusion_keys,
        });
        if let Err(failure) = self.pump().await {
            let current_is_pending =
                self.pending.iter().any(|pending| pending.queue_id == queue_id);
            self.pending.clear();

            if failure.queue_id == queue_id {
                return Err(failure.error);
            }

            // `pump` removes the transaction whose authentication failed. If
            // the error came from an older queued transaction, also retract the
            // transaction accepted by this call so retrying it cannot later
            // produce a duplicate submission.
            if current_is_pending {
                return Err(failure.error);
            }

            // The transaction accepted by this call is already on the wire, so
            // returning `Err` would invite an unsafe retry. Defer the unrelated
            // older failure until `flush` instead.
            self.deferred_errors.push_back(failure.error);
        }

        Ok(())
    }

    /// Wait for all pending transactions to complete.
    pub async fn flush(&mut self) -> Result<()> {
        let mut first_error = self.deferred_errors.pop_front();
        self.deferred_errors.clear();
        if first_error.is_some() {
            self.pending.clear();
        } else if let Err(failure) = self.pump().await {
            self.pending.clear();
            first_error = Some(failure.error);
        }

        while !self.pending.is_empty() || !self.active_keys.is_empty() {
            match self.completion_rx.recv().await {
                Some(keys) => {
                    self.release_keys(&keys);
                    if first_error.is_none() &&
                        let Err(failure) = self.pump().await
                    {
                        self.pending.clear();
                        first_error = Some(failure.error);
                    }
                }
                None => break,
            }
        }

        while let Some(result) = self.worker_tasks.join_next().await {
            if let Err(err) = result {
                tracing::warn!(%err, "sender worker task failed");
            }
        }

        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
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

    async fn wait_for_buffer_capacity(&mut self) -> Result<()> {
        while self.pending.len() >= self.max_buffered {
            if let Err(failure) = self.pump().await {
                self.pending.clear();
                return Err(failure.error);
            }
            if self.pending.len() < self.max_buffered {
                break;
            }

            match self.completion_rx.recv().await {
                Some(keys) => self.release_keys(&keys),
                None => break,
            }
        }
        Ok(())
    }

    async fn pump(&mut self) -> std::result::Result<(), DispatchPreparationError> {
        loop {
            self.drain_completions();

            let Some(index) = self.next_ready_index() else {
                break;
            };

            let Ok(permit) = self.semaphore.clone().try_acquire_owned() else {
                break;
            };

            if let Some(limiter) = &self.rate_limiter &&
                let Some(delay) = limiter.try_acquire_or_delay().await
            {
                drop(permit);
                tokio::time::sleep(delay).await;
                continue;
            }

            // Resolve authentication immediately before dispatch so queued
            // transactions observe a freshly reloaded sender map. Do this while
            // the transaction is still queued so any error is propagated and no
            // HTTP request is made.
            let endpoint = self
                .endpoints
                .choose(&mut rand::rng())
                .expect("sender has at least one endpoint")
                .clone();
            let pending = self.pending.get(index).expect("pending index exists");
            let id = pending.id.as_deref().unwrap_or("<unnamed>").to_string();
            let submission_headers = match self
                .headers_for(&endpoint, "eth_sendRawTransaction", pending.sender, None)
                .wrap_err_with(|| format!("failed to authenticate transaction {id}"))
            {
                Ok(headers) => headers,
                Err(error) => {
                    // A failed `send`/`flush` must not leave this transaction
                    // queued for an implicit later submission.
                    let pending = self.pending.remove(index).expect("pending index exists");
                    return Err(DispatchPreparationError { queue_id: pending.queue_id, error });
                }
            };

            let pending = self.pending.remove(index).expect("pending index exists");
            self.activate_keys(&pending);
            self.dispatch(pending, endpoint, submission_headers, permit);
        }

        Ok(())
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

    fn headers_for(
        &self,
        endpoint: &RpcEndpoint,
        method: &str,
        sender: Option<Address>,
        tx_hash: Option<TxHash>,
    ) -> Result<HeaderMap> {
        let headers = match &self.request_auth {
            Some(auth) => auth.headers_for(&RpcRequestContext {
                endpoint: endpoint.identity(),
                method,
                sender,
                tx_hash,
            }),
            None => Ok(HeaderMap::new()),
        }?;
        Ok(mark_headers_sensitive(headers))
    }

    fn dispatch(
        &mut self,
        pending: PendingTx,
        endpoint: RpcEndpoint,
        submission_headers: HeaderMap,
        permit: OwnedSemaphorePermit,
    ) {
        let metrics = self.metrics.clone();
        let completion_tx = self.completion_tx.clone();
        let request_auth = self.request_auth.clone();

        self.worker_tasks.spawn(async move {
            submit_tx(
                pending,
                endpoint,
                submission_headers,
                request_auth,
                metrics,
                permit,
                completion_tx,
            )
            .await;
        });
    }
}

struct DispatchPreparationError {
    queue_id: u64,
    error: eyre::Report,
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
    endpoint: RpcEndpoint,
    submission_headers: HeaderMap,
    request_auth: Option<Arc<dyn RequestAuthProvider>>,
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

    let start = Instant::now();
    let tx_hash = match send_raw_transaction(&endpoint, &pending.raw, submission_headers).await {
        Ok(tx_hash) => {
            metrics.record_success(start.elapsed());
            tx_hash
        }
        Err(e) => {
            if request_auth.is_some() {
                tracing::warn!("Failed to send authenticated transaction");
            } else {
                tracing::warn!(error = %e, "Failed to send transaction");
            }
            metrics.record_failure();
            drop(permit);
            release_keys(&completion_tx, release_all_keys());
            return;
        }
    };

    drop(permit);

    release_keys(&completion_tx, pending.submission_keys);

    if pending.inclusion_keys.is_empty() && pending.phase != TxPhase::Setup {
        return;
    }

    match wait_for_receipt(&endpoint, pending.sender, tx_hash, request_auth.as_deref()).await {
        Ok(true) => {}
        Ok(false) => {
            tracing::error!(id = pending.id.as_deref(), phase = ?pending.phase, %tx_hash, "Transaction reverted");
            metrics.record_failure();
        }
        Err(e) => {
            if request_auth.is_some() {
                tracing::error!(%tx_hash, "Failed waiting for authenticated transaction receipt");
            } else {
                tracing::error!(error = %e, %tx_hash, "Failed waiting for transaction receipt");
            }
            metrics.record_failure();
        }
    }

    release_keys(&completion_tx, pending.inclusion_keys);
}

async fn send_raw_transaction(
    endpoint: &RpcEndpoint,
    raw: &Bytes,
    headers: HeaderMap,
) -> alloy_transport::TransportResult<TxHash> {
    let encoded = format!("0x{}", hex::encode(raw));
    endpoint
        .provider()
        .client()
        .request::<_, TxHash>("eth_sendRawTransaction", (encoded,))
        .map_meta(|mut meta| {
            meta.headers_mut().extend(headers);
            meta
        })
        .await
}

fn release_keys(completion_tx: &mpsc::UnboundedSender<SchedulingKeys>, keys: SchedulingKeys) {
    if !keys.is_empty() {
        let _ = completion_tx.send(keys);
    }
}

async fn wait_for_receipt(
    endpoint: &RpcEndpoint,
    sender: Option<Address>,
    tx_hash: TxHash,
    request_auth: Option<&dyn RequestAuthProvider>,
) -> Result<bool> {
    let deadline = tokio::time::Instant::now() + RECEIPT_TIMEOUT;

    loop {
        let headers = match request_auth {
            Some(auth) => auth
                .headers_for(&RpcRequestContext {
                    endpoint: endpoint.identity(),
                    method: "eth_getTransactionReceipt",
                    sender,
                    tx_hash: Some(tx_hash),
                })
                .map(mark_headers_sensitive)?,
            None => HeaderMap::new(),
        };
        let receipt = endpoint
            .provider()
            .client()
            .request::<_, Option<AnyTransactionReceipt>>("eth_getTransactionReceipt", (tx_hash,))
            .map_meta(|mut meta| {
                meta.headers_mut().extend(headers);
                meta
            })
            .await?;
        if let Some(receipt) = receipt {
            return Ok(receipt.status());
        }

        if tokio::time::Instant::now() >= deadline {
            eyre::bail!("timed out waiting for transaction receipt");
        }

        tokio::time::sleep(RECEIPT_POLL_INTERVAL).await;
    }
}

fn mark_headers_sensitive(mut headers: HeaderMap) -> HeaderMap {
    for value in headers.values_mut() {
        value.set_sensitive(true);
    }
    headers
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
        let config = SenderConfig { rate_limit: 10_000, max_concurrent: 100 };
        let limiter = RateLimiter::new(config.rate_limit);
        assert_eq!(max_buffered_transactions(&config, Some(&limiter)), 400);

        let config = SenderConfig { rate_limit: 100_000, max_concurrent: 100 };
        let limiter = RateLimiter::new(config.rate_limit);
        assert_eq!(max_buffered_transactions(&config, Some(&limiter)), 2_000);
    }

    #[test]
    fn authentication_headers_are_marked_sensitive_at_the_request_boundary() {
        let mut headers = HeaderMap::new();
        headers.insert("x-test-auth", "fixture-sensitive-value".parse().unwrap());

        let headers = mark_headers_sensitive(headers);

        assert!(!format!("{headers:?}").contains("fixture-sensitive-value"));
    }
}
