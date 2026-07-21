//! Transaction sender with scheduling key ordering and rate limiting.
//!
//! Handles sending transactions to an RPC endpoint while:
//! - Respecting scheduling key ordering (shared key = sequential, disjoint keys = parallel)
//! - Applying rate limiting

use crate::metrics::MetricsCollector;
use alloy_network::{primitives::ReceiptResponse, AnyNetwork};
use alloy_primitives::{keccak256, BlockHash, Bytes, TxHash};
use alloy_provider::{DynProvider, Provider};
use eyre::{bail, Result, WrapErr};
use futures::future::try_join_all;
use rand::seq::IndexedRandom;
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
const FAILURE_HEAD_QUERY_TIMEOUT: Duration = Duration::from_secs(2);
const PENDING_BACKLOG_FACTOR: usize = 4;

/// An RPC endpoint used for transaction submission and receipt queries.
///
/// Keeping the URL next to the erased provider makes failures attributable to
/// the endpoint that produced them.
#[derive(Clone)]
pub struct RpcEndpoint {
    url: Arc<str>,
    provider: DynProvider<AnyNetwork>,
}

impl RpcEndpoint {
    /// Create an endpoint from its configured URL and provider.
    pub fn new(url: impl Into<String>, provider: DynProvider<AnyNetwork>) -> Self {
        Self { url: Arc::from(url.into()), provider }
    }

    /// The configured RPC URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The provider connected to this endpoint.
    pub fn provider(&self) -> &DynProvider<AnyNetwork> {
        &self.provider
    }
}

impl fmt::Debug for RpcEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcEndpoint").field("url", &self.url).finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SetupReceiptCheckpoint {
    tx_hash: TxHash,
    block_number: u64,
    block_hash: BlockHash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReceiptObservation {
    status: bool,
    block_number: Option<u64>,
    block_hash: Option<BlockHash>,
}

#[derive(Debug, PartialEq, Eq)]
struct ProviderHeadDiagnostic {
    block_number: Option<u64>,
    error: Option<String>,
}

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
    providers: Vec<RpcEndpoint>,
    metrics: Arc<MetricsCollector>,
    semaphore: Arc<Semaphore>,
    /// Transactions waiting for all of their scheduling keys to become free.
    pending: VecDeque<PendingTx>,
    /// Scheduling keys currently held by dispatched transactions.
    active_keys: HashSet<SchedulingKey>,
    completion_tx: mpsc::UnboundedSender<SchedulingKeys>,
    completion_rx: mpsc::UnboundedReceiver<SchedulingKeys>,
    setup_receipt_tx: mpsc::UnboundedSender<SetupReceiptCheckpoint>,
    setup_receipt_rx: mpsc::UnboundedReceiver<SetupReceiptCheckpoint>,
    /// Worker tasks for awaiting completion and reaping completed task state.
    worker_tasks: JoinSet<()>,
    /// Rate limiter tokens.
    rate_limiter: Option<Arc<RateLimiter>>,
    /// Maximum number of transactions buffered internally before applying
    /// backpressure to the transaction source.
    max_buffered: usize,
}

impl Sender {
    /// Create a new sender from providers without configured URL labels.
    pub fn new(
        providers: Vec<DynProvider<AnyNetwork>>,
        config: SenderConfig,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        let endpoints = providers
            .into_iter()
            .enumerate()
            .map(|(index, provider)| RpcEndpoint::new(format!("provider[{index}]"), provider))
            .collect();
        Self::new_with_endpoints(endpoints, config, metrics)
    }

    /// Create a new sender from providers paired with their configured URLs.
    pub fn new_with_endpoints(
        providers: Vec<RpcEndpoint>,
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
        let (setup_receipt_tx, setup_receipt_rx) = mpsc::unbounded_channel();

        Self {
            providers,
            metrics,
            semaphore,
            pending: VecDeque::new(),
            active_keys: HashSet::new(),
            completion_tx,
            completion_rx,
            setup_receipt_tx,
            setup_receipt_rx,
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

    /// Wait until every configured endpoint has imported the successful setup
    /// receipts observed during this sender's most recent flush.
    ///
    /// A matching transaction hash alone is not sufficient: every endpoint
    /// must report success and the same inclusion block number and hash. This
    /// prevents workload submission through a lagging or divergent endpoint.
    /// `expected_setup_txs` is also checked so a missing worker checkpoint
    /// cannot silently weaken the barrier.
    pub async fn wait_for_setup_convergence(&mut self, expected_setup_txs: u64) -> Result<()> {
        let mut checkpoints = Vec::new();
        while let Ok(checkpoint) = self.setup_receipt_rx.try_recv() {
            checkpoints.push(checkpoint);
        }
        if checkpoints.len() as u64 != expected_setup_txs {
            bail!(
                "setup receipt checkpoint count mismatch: expected {expected_setup_txs}, observed {}",
                checkpoints.len()
            );
        }

        tracing::info!(
            setup_txs = checkpoints.len(),
            rpc_endpoints = self.providers.len(),
            "Waiting for setup state on all RPC endpoints"
        );

        wait_for_all_provider_receipts(
            &self.providers,
            &checkpoints,
            RECEIPT_TIMEOUT,
            RECEIPT_POLL_INTERVAL,
        )
        .await?;

        tracing::info!(
            setup_txs = checkpoints.len(),
            rpc_endpoints = self.providers.len(),
            "Setup state converged on all RPC endpoints"
        );
        Ok(())
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
        let setup_receipt_tx = self.setup_receipt_tx.clone();

        self.worker_tasks.spawn(async move {
            submit_tx(pending, providers, metrics, permit, completion_tx, setup_receipt_tx).await;
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
    providers: Vec<RpcEndpoint>,
    metrics: Arc<MetricsCollector>,
    permit: OwnedSemaphorePermit,
    completion_tx: mpsc::UnboundedSender<SchedulingKeys>,
    setup_receipt_tx: mpsc::UnboundedSender<SetupReceiptCheckpoint>,
) {
    let release_all_keys = || {
        let mut keys = pending.submission_keys.clone();
        keys.extend_from_slice(&pending.inclusion_keys);
        keys
    };

    metrics.record_sent();

    // Pick a random provider for this request.
    // SAFETY: `providers` is guaranteed to be non-empty by construction.
    let endpoint = providers.choose(&mut rand::rng()).unwrap();
    let local_tx_hash = keccak256(&pending.raw);

    let start = Instant::now();
    let tx_hash = match endpoint.provider().send_raw_transaction(&pending.raw).await {
        Ok(pending_tx) => {
            metrics.record_success(start.elapsed());
            *pending_tx.tx_hash()
        }
        Err(e) => {
            let error = e.to_string();
            metrics.record_failure();
            release_keys(&completion_tx, release_all_keys());

            // A rejected transaction has no receipt or canonical inclusion
            // block. Query the same endpoint immediately after rejection to
            // capture the approximate head against which it was validated.
            // Retain the permit so diagnostic requests remain bounded by the
            // configured maximum RPC concurrency if failures arrive in bulk.
            let head = query_provider_head(endpoint).await;
            drop(permit);
            tracing::warn!(
                error = %error,
                rpc_url = endpoint.url(),
                tx_hash = %local_tx_hash,
                id = pending.id.as_deref(),
                phase = ?pending.phase,
                validation_head = head.block_number,
                validation_head_error = head.error.as_deref(),
                "Failed to send transaction"
            );
            return;
        }
    };

    if tx_hash != local_tx_hash {
        tracing::warn!(
            rpc_url = endpoint.url(),
            %local_tx_hash,
            provider_tx_hash = %tx_hash,
            "RPC endpoint returned a transaction hash that differs from the locally computed hash"
        );
    }

    drop(permit);

    release_keys(&completion_tx, pending.submission_keys);

    if pending.inclusion_keys.is_empty() && pending.phase != TxPhase::Setup {
        return;
    }

    match wait_for_receipt(endpoint, tx_hash).await {
        Ok(receipt) if !receipt.status => {
            tracing::error!(
                id = pending.id.as_deref(),
                phase = ?pending.phase,
                rpc_url = endpoint.url(),
                %tx_hash,
                block_number = receipt.block_number,
                block_hash = ?receipt.block_hash,
                "Transaction reverted"
            );
            metrics.record_failure();
        }
        Ok(receipt) => {
            if pending.phase == TxPhase::Setup {
                match (receipt.block_number, receipt.block_hash) {
                    (Some(block_number), Some(block_hash)) => {
                        let _ = setup_receipt_tx.send(SetupReceiptCheckpoint {
                            tx_hash,
                            block_number,
                            block_hash,
                        });
                    }
                    _ => {
                        tracing::error!(
                            id = pending.id.as_deref(),
                            phase = ?pending.phase,
                            rpc_url = endpoint.url(),
                            %tx_hash,
                            block_number = receipt.block_number,
                            block_hash = ?receipt.block_hash,
                            "Successful setup receipt is missing its inclusion block identity"
                        );
                        metrics.record_failure();
                    }
                }
            }
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                rpc_url = endpoint.url(),
                %tx_hash,
                id = pending.id.as_deref(),
                phase = ?pending.phase,
                "Failed waiting for transaction receipt"
            );
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

async fn query_provider_head(endpoint: &RpcEndpoint) -> ProviderHeadDiagnostic {
    match tokio::time::timeout(FAILURE_HEAD_QUERY_TIMEOUT, endpoint.provider().get_block_number())
        .await
    {
        Ok(Ok(block_number)) => {
            ProviderHeadDiagnostic { block_number: Some(block_number), error: None }
        }
        Ok(Err(error)) => {
            ProviderHeadDiagnostic { block_number: None, error: Some(error.to_string()) }
        }
        Err(_) => ProviderHeadDiagnostic {
            block_number: None,
            error: Some(format!(
                "head query timed out after {}s",
                FAILURE_HEAD_QUERY_TIMEOUT.as_secs()
            )),
        },
    }
}

async fn wait_for_receipt(endpoint: &RpcEndpoint, tx_hash: TxHash) -> Result<ReceiptObservation> {
    let deadline = tokio::time::Instant::now() + RECEIPT_TIMEOUT;

    loop {
        if let Some(receipt) =
            endpoint.provider().get_transaction_receipt(tx_hash).await.wrap_err_with(|| {
                format!(
                    "failed to query transaction receipt from rpc_url={} tx_hash={tx_hash}",
                    endpoint.url()
                )
            })?
        {
            return Ok(ReceiptObservation {
                status: receipt.status(),
                block_number: receipt.block_number(),
                block_hash: receipt.block_hash(),
            });
        }

        if tokio::time::Instant::now() >= deadline {
            bail!(
                "timed out waiting for transaction receipt from rpc_url={} tx_hash={tx_hash}",
                endpoint.url()
            );
        }

        tokio::time::sleep(RECEIPT_POLL_INTERVAL).await;
    }
}

async fn wait_for_all_provider_receipts(
    endpoints: &[RpcEndpoint],
    checkpoints: &[SetupReceiptCheckpoint],
    timeout: Duration,
    poll_interval: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;

    try_join_all(endpoints.iter().map(|endpoint| async move {
        for checkpoint in checkpoints {
            wait_for_matching_setup_receipt(endpoint, checkpoint, deadline, poll_interval).await?;
        }
        Ok::<(), eyre::Report>(())
    }))
    .await?;

    Ok(())
}

async fn wait_for_matching_setup_receipt(
    endpoint: &RpcEndpoint,
    expected: &SetupReceiptCheckpoint,
    deadline: tokio::time::Instant,
    poll_interval: Duration,
) -> Result<()> {
    loop {
        let receipt =
            endpoint.provider().get_transaction_receipt(expected.tx_hash).await.wrap_err_with(
                || {
                    format!(
                        "failed to query setup receipt from rpc_url={} tx_hash={}",
                        endpoint.url(),
                        expected.tx_hash
                    )
                },
            )?;

        if let Some(receipt) = receipt {
            if !receipt.status() {
                bail!(
                    "setup receipt reverted on rpc_url={} tx_hash={} block_number={:?} block_hash={:?}",
                    endpoint.url(),
                    expected.tx_hash,
                    receipt.block_number(),
                    receipt.block_hash()
                );
            }

            let observed_number = receipt.block_number();
            let observed_hash = receipt.block_hash();
            if observed_number != Some(expected.block_number) ||
                observed_hash != Some(expected.block_hash)
            {
                bail!(
                    "setup receipt block mismatch on rpc_url={} tx_hash={}: expected block_number={} block_hash={}, observed block_number={observed_number:?} block_hash={observed_hash:?}",
                    endpoint.url(),
                    expected.tx_hash,
                    expected.block_number,
                    expected.block_hash
                );
            }

            return Ok(());
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            bail!(
                "timed out waiting for setup state on rpc_url={} tx_hash={} expected_block_number={} expected_block_hash={}",
                endpoint.url(),
                expected.tx_hash,
                expected.block_number,
                expected.block_hash
            );
        }

        tokio::time::sleep(poll_interval.min(deadline.saturating_duration_since(now))).await;
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
    use alloy_provider::ProviderBuilder;
    use alloy_transport::mock::Asserter;
    use serde_json::{json, Value};
    use std::{
        io::{self, Write},
        sync::Mutex,
    };
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    struct CapturedLogWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedLogWriter(self.0.clone())
        }
    }

    impl CapturedLogs {
        fn contents(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    fn mock_endpoint(url: &str) -> (RpcEndpoint, Asserter) {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new_with_network::<AnyNetwork>()
            .connect_mocked_client(asserter.clone())
            .erased();
        (RpcEndpoint::new(url, provider), asserter)
    }

    fn mock_receipt(
        tx_hash: TxHash,
        block_number: u64,
        block_hash: BlockHash,
        status: bool,
    ) -> Value {
        json!({
            "type": "0x0",
            "status": if status { "0x1" } else { "0x0" },
            "cumulativeGasUsed": "0x5208",
            "logsBloom": format!("0x{}", "00".repeat(256)),
            "logs": [],
            "transactionHash": tx_hash,
            "transactionIndex": "0x0",
            "blockHash": block_hash,
            "blockNumber": format!("0x{block_number:x}"),
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x1",
            "from": "0x0000000000000000000000000000000000000001",
            "to": "0x0000000000000000000000000000000000000002",
            "contractAddress": null
        })
    }

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

    #[tokio::test]
    async fn setup_barrier_waits_for_a_lagging_provider_receipt() {
        let tx_hash = TxHash::repeat_byte(0x11);
        let block_hash = BlockHash::repeat_byte(0x22);
        let checkpoint = SetupReceiptCheckpoint { tx_hash, block_number: 42, block_hash };
        let receipt = mock_receipt(tx_hash, 42, block_hash, true);

        let (synced, synced_rpc) = mock_endpoint("http://synced.example");
        synced_rpc.push_success(&receipt);

        let (lagging, lagging_rpc) = mock_endpoint("http://lagging.example");
        lagging_rpc.push_success(&Value::Null);
        lagging_rpc.push_success(&receipt);

        wait_for_all_provider_receipts(
            &[synced, lagging],
            &[checkpoint],
            Duration::from_secs(1),
            Duration::from_millis(1),
        )
        .await
        .unwrap();

        assert!(synced_rpc.read_q().is_empty());
        assert!(lagging_rpc.read_q().is_empty());
    }

    #[tokio::test]
    async fn setup_barrier_requires_matching_block_identity_from_every_provider() {
        let tx_hash = TxHash::repeat_byte(0x33);
        let expected_hash = BlockHash::repeat_byte(0x44);
        let checkpoint =
            SetupReceiptCheckpoint { tx_hash, block_number: 100, block_hash: expected_hash };

        let (synced, synced_rpc) = mock_endpoint("http://synced.example");
        synced_rpc.push_success(&mock_receipt(tx_hash, 100, expected_hash, true));

        let (divergent, divergent_rpc) = mock_endpoint("http://divergent.example");
        divergent_rpc.push_success(&mock_receipt(tx_hash, 100, BlockHash::repeat_byte(0x55), true));

        let error = wait_for_all_provider_receipts(
            &[synced, divergent],
            &[checkpoint],
            Duration::from_secs(1),
            Duration::from_millis(1),
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(error.contains("http://divergent.example"), "{error}");
        assert!(error.contains("setup receipt block mismatch"), "{error}");
    }

    #[tokio::test]
    async fn setup_barrier_rejects_a_missing_receipt_checkpoint() {
        let (endpoint, _) = mock_endpoint("http://setup.example");
        let metrics = MetricsCollector::new(crate::clock::RunClock::new());
        let mut sender =
            Sender::new_with_endpoints(vec![endpoint], SenderConfig::default(), metrics);

        let error = sender.wait_for_setup_convergence(1).await.unwrap_err().to_string();

        assert!(error.contains("expected 1, observed 0"), "{error}");
    }

    #[test]
    fn send_failure_logs_provider_hash_and_head() {
        let (endpoint, rpc) = mock_endpoint("http://rejecting.example");
        rpc.push_failure_msg("transaction rejected");
        rpc.push_success(&123u64);

        let raw = Bytes::from_static(&[0x01, 0x02, 0x03]);
        let expected_hash = keccak256(&raw);
        let pending = PendingTx {
            phase: TxPhase::Workload,
            id: Some("rejected-workload".to_string()),
            raw,
            submission_keys: Vec::new(),
            inclusion_keys: Vec::new(),
        };
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(logs.clone())
            .finish();
        let dispatcher = tracing::Dispatch::new(subscriber);

        let metrics = tracing::dispatcher::with_default(&dispatcher, || {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(
                async {
                    let metrics = MetricsCollector::new(crate::clock::RunClock::new());
                    let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
                    let (completion_tx, _) = mpsc::unbounded_channel();
                    let (setup_receipt_tx, _) = mpsc::unbounded_channel();

                    submit_tx(
                        pending,
                        vec![endpoint],
                        metrics.clone(),
                        permit,
                        completion_tx,
                        setup_receipt_tx,
                    )
                    .await;
                    metrics
                },
            )
        });

        assert!(rpc.read_q().is_empty(), "head-number response was not consumed");
        assert_eq!(metrics.counts(), (1, 0, 1));
        let output = logs.contents();
        assert!(output.contains("rpc_url=\"http://rejecting.example\""), "{output}");
        assert!(output.contains(&format!("tx_hash={expected_hash}")), "{output}");
        assert!(output.contains("validation_head=123"), "{output}");
    }
}
