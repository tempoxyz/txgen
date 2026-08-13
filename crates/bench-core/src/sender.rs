//! Transaction sender with scheduling key ordering and rate limiting.
//!
//! Handles sending transactions to an RPC endpoint while:
//! - Respecting scheduling key ordering (shared key = sequential, disjoint keys = parallel)
//! - Applying rate limiting

use crate::{
    metrics::MetricsCollector,
    receipt_metrics::{ReceiptCollectorHandle, ReceiptMetricLabels},
    RequestAuthProvider, RpcRequestContext,
};
use alloy_network::{primitives::ReceiptResponse, AnyNetwork, AnyTransactionReceipt};
use alloy_primitives::{keccak256, Address, Bytes, TxHash, U256};
use alloy_provider::{DynProvider, Provider};
use alloy_transport::RpcError;
use eyre::{Context, Result};
use rand::seq::IndexedRandom;
use reqwest::header::HeaderMap;
use std::{
    collections::{HashSet, VecDeque},
    fmt,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant, SystemTime},
};
use tokio::{
    sync::{mpsc, Notify, OwnedSemaphorePermit, Semaphore},
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

impl SenderConfig {
    /// Validate configuration shared by transaction submission clients.
    pub fn validate(&self) -> Result<()> {
        if self.max_concurrent == 0 {
            eyre::bail!("max_concurrent must be greater than zero");
        }

        Ok(())
    }
}

/// Result returned after an RPC endpoint accepts a raw transaction.
#[derive(Debug, Clone, Copy)]
pub struct RpcSubmission {
    /// Hash returned by `eth_sendRawTransaction`.
    pub tx_hash: TxHash,
    /// Time spent awaiting RPC acceptance, measured with a monotonic clock.
    pub acceptance_latency: Duration,
    /// Wall-clock time at which the RPC submission started.
    pub submitted_at: SystemTime,
}

/// Receipt fields retained for gas reporting without losing whether the RPC
/// response actually supplied a fee field.
#[derive(Debug)]
pub struct RpcReceiptDetails {
    /// Fully decoded transaction receipt.
    pub receipt: AnyTransactionReceipt,
    /// Gas consumed by the outer transaction receipt.
    pub gas_used: U256,
    /// Effective gas price when supplied by the RPC response.
    pub effective_gas_price: Option<U256>,
}

/// Point at which an individual RPC submission failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcSubmitFailureKind {
    /// The request was not sent, so its nonce can be safely reused.
    BeforeSend,
    /// The node returned a JSON-RPC rejection.
    Rejected,
    /// The transport failed after dispatch and acceptance is unknown.
    Ambiguous,
}

/// Classified failure from [`RpcSubmitter::submit_classified`].
#[derive(Debug)]
pub struct RpcSubmitError {
    kind: RpcSubmitFailureKind,
    timed_out: bool,
    diagnostic: String,
}

impl RpcSubmitError {
    /// Submission failure category used for nonce recovery.
    pub const fn kind(&self) -> RpcSubmitFailureKind {
        self.kind
    }

    /// Whether the submission failed because its caller-supplied deadline elapsed.
    pub const fn is_timeout(&self) -> bool {
        self.timed_out
    }

    fn before_send(error: impl std::fmt::Display) -> Self {
        Self {
            kind: RpcSubmitFailureKind::BeforeSend,
            timed_out: false,
            diagnostic: error.to_string(),
        }
    }

    fn deadline(kind: RpcSubmitFailureKind, diagnostic: &'static str) -> Self {
        Self { kind, timed_out: true, diagnostic: diagnostic.to_string() }
    }

    fn from_transport(error: alloy_transport::TransportError, redact: bool) -> Self {
        let kind = classify_transport_failure(&error);
        let diagnostic = if redact {
            "authenticated RPC submission failed".to_string()
        } else {
            error.to_string()
        };
        Self { kind, timed_out: false, diagnostic }
    }
}

fn classify_transport_failure(error: &alloy_transport::TransportError) -> RpcSubmitFailureKind {
    match error {
        RpcError::ErrorResp(_) => RpcSubmitFailureKind::Rejected,
        RpcError::UnsupportedFeature(_) | RpcError::LocalUsageError(_) | RpcError::SerError(_) => {
            RpcSubmitFailureKind::BeforeSend
        }
        RpcError::NullResp | RpcError::DeserError { .. } | RpcError::Transport(_) => {
            RpcSubmitFailureKind::Ambiguous
        }
    }
}

fn submission_may_have_been_accepted(error: &alloy_transport::TransportError) -> bool {
    if classify_transport_failure(error) == RpcSubmitFailureKind::Ambiguous {
        return true;
    }
    let RpcError::ErrorResp(payload) = error else { return false };
    known_transaction_error(&payload.message)
}

fn known_transaction_error(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("already known")
        || message.starts_with("known transaction")
        || message.contains("transaction already known")
        || message.contains("already imported")
}

impl std::fmt::Display for RpcSubmitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.diagnostic)
    }
}

impl std::error::Error for RpcSubmitError {}

/// Cloneable client for submitting individual transactions through bench's
/// concurrency and transaction-rate controls.
///
/// Clones share the same semaphore and token bucket. Separately constructed
/// clients have independent rate limits, allowing scenario-instance start rate
/// limiting to remain separate from transaction submission rate limiting.
/// Generated transactions are ordered using the same submission and inclusion
/// key semantics as [`Sender`]. Raw submissions bypass key ordering.
#[derive(Clone)]
pub struct RpcSubmitter {
    endpoints: Arc<[RpcEndpoint]>,
    request_auth: Option<Arc<dyn RequestAuthProvider>>,
    semaphore: Arc<Semaphore>,
    rate_limiter: Option<Arc<RateLimiter>>,
    ordering: Arc<RpcOrdering>,
}

impl RpcSubmitter {
    /// Create an RPC submitter backed by one or more interchangeable providers.
    pub fn new(providers: Vec<DynProvider<AnyNetwork>>, config: SenderConfig) -> Result<Self> {
        let endpoints = providers
            .into_iter()
            .enumerate()
            .map(|(index, provider)| RpcEndpoint::new(format!("rpc-{index}"), provider))
            .collect();
        Self::new_with_request_auth(endpoints, config, None)
    }

    /// Create an RPC submitter with request-scoped authentication.
    pub fn new_with_request_auth(
        endpoints: Vec<RpcEndpoint>,
        config: SenderConfig,
        request_auth: Option<Arc<dyn RequestAuthProvider>>,
    ) -> Result<Self> {
        if endpoints.is_empty() {
            eyre::bail!("at least one RPC provider is required");
        }
        config.validate()?;

        let rate_limiter =
            (config.rate_limit > 0).then(|| Arc::new(RateLimiter::new(config.rate_limit)));

        Ok(Self {
            endpoints: endpoints.into(),
            request_auth,
            semaphore: Arc::new(Semaphore::new(config.max_concurrent)),
            rate_limiter,
            ordering: Arc::new(RpcOrdering::default()),
        })
    }

    /// Submit a generated transaction and wait until an RPC endpoint accepts it.
    pub async fn submit(&self, tx: &GeneratedTx) -> Result<RpcSubmission> {
        self.submit_classified(tx).await.map_err(Into::into)
    }

    /// Submit a generated transaction while retaining whether an error happened
    /// before dispatch, was an RPC rejection, or had an ambiguous transport outcome.
    pub async fn submit_classified(
        &self,
        tx: &GeneratedTx,
    ) -> std::result::Result<RpcSubmission, RpcSubmitError> {
        self.submit_classified_inner(tx, None).await
    }

    /// Submit a generated transaction before an absolute deadline.
    ///
    /// Expiry while waiting for ordering, rate, or concurrency capacity is
    /// classified as [`RpcSubmitFailureKind::BeforeSend`]. Expiry after the raw
    /// RPC starts is classified as [`RpcSubmitFailureKind::Ambiguous`].
    pub async fn submit_classified_until(
        &self,
        tx: &GeneratedTx,
        deadline: tokio::time::Instant,
    ) -> std::result::Result<RpcSubmission, RpcSubmitError> {
        self.submit_classified_inner(tx, Some(deadline)).await
    }

    async fn submit_classified_inner(
        &self,
        tx: &GeneratedTx,
        deadline: Option<tokio::time::Instant>,
    ) -> std::result::Result<RpcSubmission, RpcSubmitError> {
        let order = self
            .ordering
            .clone()
            .enqueue(tx.submission_keys.clone(), tx.inclusion_keys.clone())
            .map_err(RpcSubmitError::before_send)?;
        let mut order = match deadline {
            Some(deadline) => {
                tokio::time::timeout_at(deadline, order.acquire()).await.map_err(|_| {
                    RpcSubmitError::deadline(
                        RpcSubmitFailureKind::BeforeSend,
                        "submission deadline elapsed before dispatch",
                    )
                })?
            }
            None => order.acquire().await,
        };
        let permit = match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, self.acquire_permit())
                .await
                .map_err(|_| {
                    RpcSubmitError::deadline(
                        RpcSubmitFailureKind::BeforeSend,
                        "submission deadline elapsed before dispatch",
                    )
                })?
                .map_err(RpcSubmitError::before_send)?,
            None => self.acquire_permit().await.map_err(RpcSubmitError::before_send)?,
        };

        let expected_hash = keccak256(&tx.raw);
        let endpoint = self.endpoint_for_hash(expected_hash);
        let headers = self
            .headers_for(&endpoint, "eth_sendRawTransaction", tx.sender, None)
            .map_err(RpcSubmitError::before_send)?;
        let redact = self.request_auth.is_some();
        let submission = match deadline {
            Some(deadline) => {
                tokio::time::timeout_at(deadline, submit_raw_rpc(&endpoint, &tx.raw, headers))
                    .await
                    .map_err(|_| {
                        RpcSubmitError::deadline(
                            RpcSubmitFailureKind::Ambiguous,
                            "submission deadline elapsed after RPC dispatch; acceptance is unknown",
                        )
                    })?
                    .map_err(|error| RpcSubmitError::from_transport(error, redact))?
            }
            None => submit_raw_rpc(&endpoint, &tx.raw, headers)
                .await
                .map_err(|error| RpcSubmitError::from_transport(error, redact))?,
        };
        drop(permit);

        order.release_submission_keys();
        if let Some(inclusion_release) = order.take_inclusion_keys() {
            let submitter = self.clone();
            let sender = tx.sender;
            let tx_hash = submission.tx_hash;
            tokio::spawn(async move {
                let _ = submitter.wait_for_receipt(sender, tx_hash).await;
                drop(inclusion_release);
            });
        }

        Ok(submission)
    }

    /// Submit raw EIP-2718 transaction bytes and wait for RPC acceptance.
    pub async fn submit_raw(&self, raw: &Bytes) -> Result<RpcSubmission> {
        let _permit = self.acquire_permit().await?;

        let endpoint = self.endpoint_for_hash(keccak256(raw));
        let headers = self.headers_for(&endpoint, "eth_sendRawTransaction", None, None)?;
        submit_raw_rpc(&endpoint, raw, headers)
            .await
            .map_err(|error| rpc_request_error(error, self.request_auth.is_some(), "submission"))
    }

    /// Fetch a transaction receipt through a sender-authenticated endpoint.
    pub async fn get_transaction_receipt(
        &self,
        sender: Option<Address>,
        tx_hash: TxHash,
    ) -> Result<Option<AnyTransactionReceipt>> {
        Ok(self
            .get_transaction_receipt_details(sender, tx_hash)
            .await?
            .map(|details| details.receipt))
    }

    /// Fetch a transaction receipt while preserving optional fee-field
    /// presence from the raw RPC response.
    pub async fn get_transaction_receipt_details(
        &self,
        sender: Option<Address>,
        tx_hash: TxHash,
    ) -> Result<Option<RpcReceiptDetails>> {
        let endpoint = self.endpoint_for_hash(tx_hash);
        let headers =
            self.headers_for(&endpoint, "eth_getTransactionReceipt", sender, Some(tx_hash))?;
        let value = endpoint
            .provider()
            .client()
            .request::<_, Option<serde_json::Value>>("eth_getTransactionReceipt", (tx_hash,))
            .map_meta(|mut meta| {
                meta.headers_mut().extend(headers);
                meta
            })
            .await
            .map_err(|error| {
                rpc_request_error(error, self.request_auth.is_some(), "receipt lookup")
            })?;

        let details = value.map(decode_receipt_details).transpose()?;
        if let Some(details) = &details
            && details.receipt.transaction_hash() != tx_hash
        {
            eyre::bail!("receipt lookup returned a different transaction hash");
        }
        Ok(details)
    }

    /// Check whether a transaction is known through a sender-authenticated endpoint.
    pub async fn transaction_exists(
        &self,
        sender: Option<Address>,
        tx_hash: TxHash,
    ) -> Result<bool> {
        let endpoint = self.endpoint_for_hash(tx_hash);
        let headers =
            self.headers_for(&endpoint, "eth_getTransactionByHash", sender, Some(tx_hash))?;
        endpoint
            .provider()
            .client()
            .request::<_, Option<serde_json::Value>>("eth_getTransactionByHash", (tx_hash,))
            .map_meta(|mut meta| {
                meta.headers_mut().extend(headers);
                meta
            })
            .await
            .map(|transaction| transaction.is_some())
            .map_err(|error| {
                rpc_request_error(error, self.request_auth.is_some(), "transaction lookup")
            })
    }

    /// Validate submission authentication for a sender without dispatching an
    /// RPC request.
    ///
    /// Every configured endpoint is checked because authentication providers
    /// may use endpoint identity when selecting credentials.
    pub fn validate_submission_auth(&self, sender: Option<Address>) -> Result<()> {
        for endpoint in self.endpoints.iter() {
            self.headers_for(endpoint, "eth_sendRawTransaction", sender, None)?;
        }
        Ok(())
    }

    fn endpoint_for_hash(&self, tx_hash: TxHash) -> RpcEndpoint {
        // SAFETY: construction rejects an empty endpoint list.
        let mut prefix = [0u8; 8];
        prefix.copy_from_slice(&tx_hash[..8]);
        let index =
            (u64::from_be_bytes(prefix) % u64::try_from(self.endpoints.len()).unwrap()) as usize;
        self.endpoints[index].clone()
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

    async fn wait_for_receipt(&self, sender: Option<Address>, tx_hash: TxHash) -> Result<bool> {
        let deadline = tokio::time::Instant::now() + RECEIPT_TIMEOUT;

        loop {
            if let Some(receipt) = self.get_transaction_receipt(sender, tx_hash).await? {
                return Ok(receipt.status());
            }
            if tokio::time::Instant::now() >= deadline {
                eyre::bail!("timed out waiting for transaction receipt");
            }
            tokio::time::sleep(RECEIPT_POLL_INTERVAL).await;
        }
    }

    async fn acquire_permit(&self) -> Result<OwnedSemaphorePermit> {
        loop {
            let permit = self
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| eyre::eyre!("RPC submitter semaphore closed"))?;

            if let Some(limiter) = &self.rate_limiter
                && let Some(delay) = limiter.try_acquire_or_delay().await
            {
                drop(permit);
                tokio::time::sleep(delay).await;
                continue;
            }

            return Ok(permit);
        }
    }
}

#[derive(Default)]
struct RpcOrdering {
    state: StdMutex<RpcOrderingState>,
    notify: Notify,
}

#[derive(Default)]
struct RpcOrderingState {
    next_id: u64,
    pending: VecDeque<RpcPendingOrder>,
    active_keys: HashSet<SchedulingKey>,
}

struct RpcPendingOrder {
    id: u64,
    submission_keys: SchedulingKeys,
    inclusion_keys: SchedulingKeys,
}

impl RpcPendingOrder {
    fn scheduling_keys(&self) -> impl Iterator<Item = &SchedulingKey> {
        self.submission_keys.iter().chain(self.inclusion_keys.iter())
    }
}

impl RpcOrdering {
    fn enqueue(
        self: Arc<Self>,
        submission_keys: SchedulingKeys,
        inclusion_keys: SchedulingKeys,
    ) -> Result<RpcOrderTicket> {
        let (submission_keys, inclusion_keys) =
            normalize_key_sets(submission_keys, inclusion_keys)?;
        let mut state = self.state.lock().expect("RPC ordering mutex poisoned");
        let id = state.next_id;
        state.next_id = state.next_id.wrapping_add(1);
        state.pending.push_back(RpcPendingOrder {
            id,
            submission_keys: submission_keys.clone(),
            inclusion_keys: inclusion_keys.clone(),
        });
        drop(state);
        self.notify.notify_waiters();

        Ok(RpcOrderTicket { ordering: self, id, submission_keys, inclusion_keys, acquired: false })
    }

    fn try_acquire(&self, id: u64) -> bool {
        let mut state = self.state.lock().expect("RPC ordering mutex poisoned");
        let Some(index) = next_ready_order_index(&state.pending, &state.active_keys) else {
            return false;
        };
        if state.pending[index].id != id {
            return false;
        }

        let pending = state.pending.remove(index).expect("pending RPC order exists");
        state.active_keys.extend(pending.scheduling_keys().copied());
        true
    }

    fn cancel_pending(&self, id: u64) {
        let mut state = self.state.lock().expect("RPC ordering mutex poisoned");
        if let Some(index) = state.pending.iter().position(|pending| pending.id == id) {
            state.pending.remove(index);
        }
        drop(state);
        self.notify.notify_waiters();
    }

    fn release(&self, keys: &[SchedulingKey]) {
        let mut state = self.state.lock().expect("RPC ordering mutex poisoned");
        for key in keys {
            state.active_keys.remove(key);
        }
        drop(state);
        self.notify.notify_waiters();
    }
}

fn next_ready_order_index(
    pending: &VecDeque<RpcPendingOrder>,
    active_keys: &HashSet<SchedulingKey>,
) -> Option<usize> {
    let mut blocked_keys = active_keys.clone();
    for (index, pending) in pending.iter().enumerate() {
        if pending.scheduling_keys().any(|key| blocked_keys.contains(key)) {
            blocked_keys.extend(pending.scheduling_keys().copied());
        } else {
            return Some(index);
        }
    }
    None
}

struct RpcOrderTicket {
    ordering: Arc<RpcOrdering>,
    id: u64,
    submission_keys: SchedulingKeys,
    inclusion_keys: SchedulingKeys,
    acquired: bool,
}

impl RpcOrderTicket {
    async fn acquire(mut self) -> Self {
        loop {
            let ordering = Arc::clone(&self.ordering);
            let notified = ordering.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if ordering.try_acquire(self.id) {
                self.acquired = true;
                return self;
            }
            notified.await;
        }
    }

    fn release_submission_keys(&mut self) {
        self.ordering.release(&self.submission_keys);
        self.submission_keys.clear();
    }

    fn take_inclusion_keys(&mut self) -> Option<RpcInclusionRelease> {
        if self.inclusion_keys.is_empty() {
            return None;
        }
        let keys = std::mem::take(&mut self.inclusion_keys);
        Some(RpcInclusionRelease { ordering: self.ordering.clone(), keys })
    }
}

impl Drop for RpcOrderTicket {
    fn drop(&mut self) {
        if self.acquired {
            self.ordering.release(&self.submission_keys);
            self.ordering.release(&self.inclusion_keys);
        } else {
            self.ordering.cancel_pending(self.id);
        }
    }
}

struct RpcInclusionRelease {
    ordering: Arc<RpcOrdering>,
    keys: SchedulingKeys,
}

impl Drop for RpcInclusionRelease {
    fn drop(&mut self) {
        self.ordering.release(&self.keys);
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
    /// Optional receipt collector for workload gas reporting.
    receipt_collector: Option<ReceiptCollectorHandle>,
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
            receipt_collector: None,
        }
    }

    /// Attach receipt collection for every RPC-accepted workload transaction.
    pub fn with_receipt_collector(mut self, collector: ReceiptCollectorHandle) -> Self {
        self.receipt_collector = Some(collector);
        self
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
                    if first_error.is_none()
                        && let Err(failure) = self.pump().await
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

            if let Some(limiter) = &self.rate_limiter
                && let Some(delay) = limiter.try_acquire_or_delay().await
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
        let receipt_collector = self.receipt_collector.clone();

        self.worker_tasks.spawn(async move {
            submit_tx(
                pending,
                endpoint,
                submission_headers,
                request_auth,
                receipt_collector,
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

#[allow(clippy::too_many_arguments)]
async fn submit_tx(
    pending: PendingTx,
    endpoint: RpcEndpoint,
    submission_headers: HeaderMap,
    request_auth: Option<Arc<dyn RequestAuthProvider>>,
    receipt_collector: Option<ReceiptCollectorHandle>,
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

    let expected_hash = keccak256(&pending.raw);
    let start = Instant::now();
    let tx_hash = match send_raw_transaction(&endpoint, &pending.raw, submission_headers).await {
        Ok(tx_hash) => {
            metrics.record_success(start.elapsed());
            tx_hash
        }
        Err(e) => {
            if submission_may_have_been_accepted(&e) {
                track_workload_receipt(receipt_collector.as_ref(), &pending, expected_hash);
            }
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

    track_workload_receipt(receipt_collector.as_ref(), &pending, expected_hash);

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

fn track_workload_receipt(
    collector: Option<&ReceiptCollectorHandle>,
    pending: &PendingTx,
    tx_hash: TxHash,
) {
    if pending.phase != TxPhase::Workload {
        return;
    }
    let Some(collector) = collector else { return };

    let mut labels = ReceiptMetricLabels::new();
    if let Some(input) = pending.id.clone() {
        labels.insert("input".to_string(), input);
    }
    collector.track(pending.sender, tx_hash, labels);
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

async fn submit_raw_rpc(
    endpoint: &RpcEndpoint,
    raw: &Bytes,
    headers: HeaderMap,
) -> alloy_transport::TransportResult<RpcSubmission> {
    let submitted_at = SystemTime::now();
    let start = Instant::now();
    let tx_hash = send_raw_transaction(endpoint, raw, headers).await?;

    Ok(RpcSubmission { tx_hash, acceptance_latency: start.elapsed(), submitted_at })
}

pub(crate) fn decode_receipt_details(value: serde_json::Value) -> Result<RpcReceiptDetails> {
    let effective_gas_price = value
        .get("effectiveGasPrice")
        .filter(|value| !value.is_null())
        .or_else(|| value.get("gasPrice").filter(|value| !value.is_null()))
        .map(|value| {
            serde_json::from_value::<U256>(value.clone())
                .wrap_err("invalid effective gas price in transaction receipt")
        })
        .transpose()?;
    let receipt: AnyTransactionReceipt =
        serde_json::from_value(value).wrap_err("invalid transaction receipt response")?;

    Ok(RpcReceiptDetails { gas_used: U256::from(receipt.gas_used()), effective_gas_price, receipt })
}

fn rpc_request_error(
    error: alloy_transport::TransportError,
    redact: bool,
    operation: &str,
) -> eyre::Report {
    if redact {
        eyre::eyre!("authenticated RPC {operation} failed")
    } else {
        error.into()
    }
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
    use crate::{ReceiptCollector, RunClock};
    use alloy_provider::ProviderBuilder;
    use alloy_transport::mock::Asserter;

    #[derive(Default)]
    struct RecordingAuth {
        endpoints: StdMutex<Vec<String>>,
    }

    impl RequestAuthProvider for RecordingAuth {
        fn headers_for(&self, context: &RpcRequestContext<'_>) -> Result<HeaderMap> {
            if context.sender != Some(Address::repeat_byte(0x11)) {
                eyre::bail!("missing sender mapping");
            }
            self.endpoints.lock().unwrap().push(context.endpoint.to_string());
            Ok(HeaderMap::new())
        }
    }

    fn mocked_provider(asserter: Asserter) -> DynProvider<AnyNetwork> {
        ProviderBuilder::new_with_network::<AnyNetwork>().connect_mocked_client(asserter).erased()
    }

    fn receipt_json(
        transaction_hash: TxHash,
        effective_gas_price: Option<&str>,
    ) -> serde_json::Value {
        let mut receipt = serde_json::json!({
            "transactionHash": transaction_hash,
            "transactionIndex": "0x0",
            "blockHash": TxHash::repeat_byte(0x44),
            "blockNumber": "0x1",
            "from": Address::repeat_byte(0x55),
            "to": Address::repeat_byte(0x66),
            "cumulativeGasUsed": "0x5208",
            "gasUsed": "0x5208",
            "contractAddress": null,
            "logs": [],
            "logsBloom": format!("0x{}", "00".repeat(256)),
            "status": "0x1",
            "type": "0x2"
        });
        if let Some(price) = effective_gas_price {
            receipt["effectiveGasPrice"] = serde_json::Value::String(price.to_string());
        }
        receipt
    }

    #[test]
    fn receipt_details_preserve_gas_and_effective_price() {
        let details =
            decode_receipt_details(receipt_json(TxHash::repeat_byte(0x42), Some("0x3b9aca00")))
                .unwrap();

        assert_eq!(details.gas_used, U256::from(21_000));
        assert_eq!(details.effective_gas_price, Some(U256::from(1_000_000_000u64)));
    }

    #[test]
    fn receipt_details_preserve_missing_fee_fields() {
        let details =
            decode_receipt_details(receipt_json(TxHash::repeat_byte(0x42), None)).unwrap();

        assert_eq!(details.gas_used, U256::from(21_000));
        assert_eq!(details.effective_gas_price, None);
    }

    #[test]
    fn only_known_transaction_errors_are_receipt_trackable() {
        assert!(known_transaction_error("already known"));
        assert!(known_transaction_error("Known transaction: 0x1234"));
        assert!(known_transaction_error("transaction already imported"));
        assert!(!known_transaction_error("unknown transaction type 0x7f"));
        assert!(!known_transaction_error("nonce too low"));
    }

    #[tokio::test]
    async fn sender_collects_receipts_without_inclusion_keys() {
        let asserter = Asserter::new();
        let raw = Bytes::from_static(&[0x02, 0xf8, 0x70]);
        let tx_hash = keccak256(&raw);
        asserter.push_success(&tx_hash);
        asserter.push_success(&receipt_json(tx_hash, Some("0x2")));
        let provider = mocked_provider(asserter.clone());
        let config = SenderConfig { rate_limit: 0, max_concurrent: 1 };
        let collector = ReceiptCollector::start(
            RpcSubmitter::new(vec![provider.clone()], config.clone()).unwrap(),
            1,
        );
        let metrics = MetricsCollector::new(RunClock::new());
        let mut sender =
            Sender::new(vec![provider], config, metrics).with_receipt_collector(collector.handle());

        sender
            .send(GeneratedTx {
                phase: TxPhase::Workload,
                id: Some("transfer".to_string()),
                sender: Some(Address::repeat_byte(0x55)),
                raw,
                submission_keys: vec![SchedulingKey::from([0x11; 20])],
                inclusion_keys: Vec::new(),
            })
            .await
            .unwrap();
        sender.flush().await.unwrap();
        drop(sender);

        let collection = collector.finish().await;
        let groups = &collection.metrics;
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].labels["input"], "transfer");
        assert_eq!(groups[0].gas_used.count, 1);
        assert_eq!(groups[0].gas_used.min, Some(21_000.0));
        assert_eq!(groups[0].effective_gas_price.min, Some(2.0));
        assert_eq!(groups[0].fee_paid.min, Some(42_000.0));
        assert_eq!(collection.records.len(), 1);
        assert_eq!(collection.records[0].tx_hash, tx_hash);
        assert!(asserter.read_q().is_empty());
    }

    #[test]
    fn test_sender_config_default() {
        let config = SenderConfig::default();
        assert_eq!(config.rate_limit, 0);
        assert_eq!(config.max_concurrent, 100);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_sender_config_rejects_zero_concurrency() {
        let config = SenderConfig { rate_limit: 0, max_concurrent: 0 };
        assert!(config.validate().is_err());

        let provider = mocked_provider(Asserter::new());
        assert!(RpcSubmitter::new(vec![provider], config).is_err());
    }

    #[test]
    fn test_rpc_submitter_rejects_empty_provider_list() {
        let result = RpcSubmitter::new(Vec::new(), SenderConfig::default());
        assert!(result.is_err());
    }

    #[test]
    fn rpc_submitter_preflights_auth_for_every_endpoint() {
        let auth = Arc::new(RecordingAuth::default());
        let submitter = RpcSubmitter::new_with_request_auth(
            vec![
                RpcEndpoint::new("first", mocked_provider(Asserter::new())),
                RpcEndpoint::new("second", mocked_provider(Asserter::new())),
            ],
            SenderConfig::default(),
            Some(auth.clone()),
        )
        .unwrap();

        submitter.validate_submission_auth(Some(Address::repeat_byte(0x11))).unwrap();
        assert_eq!(auth.endpoints.lock().unwrap().as_slice(), ["first", "second"]);
        assert!(submitter.validate_submission_auth(Some(Address::repeat_byte(0x22))).is_err());
    }

    #[test]
    fn rpc_submitter_uses_a_stable_endpoint_for_a_transaction_hash() {
        let submitter = RpcSubmitter::new(
            vec![mocked_provider(Asserter::new()), mocked_provider(Asserter::new())],
            SenderConfig::default(),
        )
        .unwrap();
        let transaction_hash = keccak256([0x02, 0xf8, 0x70]);

        let first = submitter.endpoint_for_hash(transaction_hash);
        let second = submitter.endpoint_for_hash(transaction_hash);
        assert_eq!(first.identity(), second.identity());
    }

    #[tokio::test]
    async fn test_rpc_submitter_returns_acceptance_result() {
        let asserter = Asserter::new();
        let tx_hash = TxHash::repeat_byte(0x42);
        asserter.push_success(&tx_hash);

        let submitter = RpcSubmitter::new(
            vec![mocked_provider(asserter.clone())],
            SenderConfig { rate_limit: 0, max_concurrent: 1 },
        )
        .unwrap();

        let submission =
            submitter.submit_raw(&Bytes::from_static(&[0x02, 0xf8, 0x70])).await.unwrap();

        assert_eq!(submission.tx_hash, tx_hash);
        assert!(submission.submitted_at.duration_since(std::time::UNIX_EPOCH).is_ok());
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn deadline_while_rate_limited_is_classified_before_send() {
        let asserter = Asserter::new();
        asserter.push_success(&TxHash::repeat_byte(0x42));
        let submitter = RpcSubmitter::new(
            vec![mocked_provider(asserter.clone())],
            SenderConfig { rate_limit: 1, max_concurrent: 1 },
        )
        .unwrap();
        let transaction = GeneratedTx {
            phase: TxPhase::Workload,
            id: None,
            sender: None,
            raw: Bytes::from_static(&[0x02, 0xf8, 0x70]),
            submission_keys: vec![SchedulingKey::from([0x11; 20])],
            inclusion_keys: Vec::new(),
        };

        submitter.submit_classified(&transaction).await.unwrap();
        let error = submitter
            .submit_classified_until(
                &transaction,
                tokio::time::Instant::now() + Duration::from_millis(10),
            )
            .await
            .unwrap_err();

        assert_eq!(error.kind(), RpcSubmitFailureKind::BeforeSend);
        assert!(error.is_timeout());
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn rpc_ordering_serializes_shared_keys_and_allows_disjoint_keys() {
        let ordering = Arc::new(RpcOrdering::default());
        let shared = SchedulingKey::from([0x11; 20]);
        let disjoint = SchedulingKey::from([0x22; 20]);

        let mut first = ordering.clone().enqueue(vec![shared], Vec::new()).unwrap().acquire().await;
        let second = ordering.clone().enqueue(vec![shared], Vec::new()).unwrap();
        let disjoint_ticket = ordering.clone().enqueue(vec![disjoint], Vec::new()).unwrap();

        let disjoint_ticket =
            tokio::time::timeout(Duration::from_millis(50), disjoint_ticket.acquire())
                .await
                .expect("disjoint key should be dispatched");
        let mut second_task = tokio::spawn(second.acquire());
        assert!(tokio::time::timeout(Duration::from_millis(10), &mut second_task).await.is_err());

        first.release_submission_keys();
        let second = tokio::time::timeout(Duration::from_millis(50), second_task)
            .await
            .expect("shared key should be released")
            .unwrap();
        drop(second);
        drop(disjoint_ticket);
    }

    #[tokio::test]
    async fn dropping_a_queued_order_unblocks_later_conflicts() {
        let ordering = Arc::new(RpcOrdering::default());
        let first_key = SchedulingKey::from([0x11; 20]);
        let second_key = SchedulingKey::from([0x22; 20]);

        let first = ordering.clone().enqueue(vec![first_key], Vec::new()).unwrap().acquire().await;
        let blocked = ordering.clone().enqueue(vec![first_key, second_key], Vec::new()).unwrap();
        let later = ordering.clone().enqueue(vec![second_key], Vec::new()).unwrap();

        drop(blocked);
        let later = tokio::time::timeout(Duration::from_millis(50), later.acquire())
            .await
            .expect("cancelled queue entry should not retain its keys");
        drop(later);
        drop(first);
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

    #[tokio::test]
    async fn test_rate_limiters_have_independent_token_buckets() {
        let first = RateLimiter::new(1);
        let second = RateLimiter::new(1);

        assert_eq!(first.try_acquire_or_delay().await, None);
        assert_eq!(second.try_acquire_or_delay().await, None);
        assert!(first.try_acquire_or_delay().await.is_some());
        assert!(second.try_acquire_or_delay().await.is_some());
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
