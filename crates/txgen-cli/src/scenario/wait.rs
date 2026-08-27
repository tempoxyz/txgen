use super::{
    error::StepError,
    log_hub::{LogInterest, LogPollHub, LogWindowSubscription},
    report::unix_ms,
    schema::{ObservationDef, ObservationMode, WaitLogStep},
    value::{coerce_event_filter, eval_expression, RuntimeContext, RuntimeValue},
};
use alloy_dyn_abi::{DynSolType, DynSolValue, EventExt, Specifier};
use alloy_eips::BlockNumberOrTag;
use alloy_json_abi::{Event, JsonAbi};
use alloy_network::{
    primitives::{BlockResponse, ReceiptResponse},
    AnyNetwork, AnyRpcBlock, AnyTransactionReceipt,
};
use alloy_primitives::{keccak256, Address, TxHash, B256, U256};
use alloy_provider::{DynProvider, Provider, ProviderBuilder, WsConnect};
use alloy_rpc_types_eth::{Filter, Log};
use bench_core::RpcSubmitter;
use eyre::{bail, Result, WrapErr};
use futures::{Stream, StreamExt};
use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

pub(crate) const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(50);
pub(crate) const DEFAULT_MAX_BLOCK_RANGE: u64 = 1_000;

pub(crate) type WakeStream = Pin<Box<dyn Stream<Item = ()> + Send>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubscriptionBehavior {
    Disabled,
    Prefer,
    Require,
}

impl From<ObservationMode> for SubscriptionBehavior {
    fn from(value: ObservationMode) -> Self {
        match value {
            ObservationMode::Auto => Self::Prefer,
            ObservationMode::Subscription => Self::Require,
            ObservationMode::Poll => Self::Disabled,
        }
    }
}

/// Chain-local observation transport.
///
/// HTTP remains authoritative for canonical receipts, logs, and block data.
/// The optional WebSocket provider only wakes those canonical reads sooner;
/// every wait also retains a polling timer so a dropped notification cannot
/// lose an inclusion.
#[derive(Clone)]
pub(crate) struct ObservationRuntime {
    query_provider: DynProvider<AnyNetwork>,
    websocket_provider: Option<DynProvider<AnyNetwork>>,
    poll_interval: Duration,
    subscription_behavior: SubscriptionBehavior,
    /// Shared per-chain log scanner. All step-level clones of this runtime
    /// reuse the same hub, so a chain never runs more than one log poller
    /// regardless of how many `wait_log` steps are active.
    log_hub: Arc<LogPollHub>,
}

impl ObservationRuntime {
    /// Construct a polling-only observer. Kept small so legacy callers and
    /// tests can use the accurate scenario default without a WebSocket.
    pub(crate) fn polling(
        query_provider: DynProvider<AnyNetwork>,
        poll_interval: Duration,
    ) -> Self {
        Self {
            log_hub: Arc::new(LogPollHub::new(
                query_provider.clone(),
                None,
                SubscriptionBehavior::Disabled,
            )),
            query_provider,
            websocket_provider: None,
            poll_interval,
            subscription_behavior: SubscriptionBehavior::Disabled,
        }
    }

    pub(crate) async fn from_config(
        query_provider: DynProvider<AnyNetwork>,
        config: &ObservationDef,
    ) -> Result<Self, StepError> {
        let behavior = SubscriptionBehavior::from(config.mode);
        Self::connect(
            query_provider,
            config.websocket_url.as_deref(),
            config.poll_interval,
            behavior,
        )
        .await
    }

    /// Connect an optional WebSocket observation endpoint.
    ///
    /// Auto mode allows a connection failure to fall back to canonical HTTP
    /// polling. Poll mode does not connect to the WebSocket endpoint at all.
    pub(crate) async fn connect(
        query_provider: DynProvider<AnyNetwork>,
        websocket_url: Option<&str>,
        poll_interval: Duration,
        behavior: SubscriptionBehavior,
    ) -> Result<Self, StepError> {
        let websocket_provider = match (behavior, websocket_url) {
            (SubscriptionBehavior::Disabled, _) => None,
            (_, Some(url)) => {
                let connect = WsConnect::new(url);
                match ProviderBuilder::new_with_network::<AnyNetwork>().connect_ws(connect).await {
                    Ok(provider) => Some(provider.erased()),
                    Err(error) if behavior == SubscriptionBehavior::Require => {
                        let _ = error;
                        return Err(StepError::new(
                            "configuration_error",
                            "failed to connect configured observation WebSocket",
                        ));
                    }
                    Err(_) => None,
                }
            }
            (SubscriptionBehavior::Require, None) => {
                return Err(StepError::new(
                    "configuration_error",
                    "subscription observation mode requires a websocket_url",
                ));
            }
            (SubscriptionBehavior::Prefer, None) => None,
        };
        Ok(Self {
            log_hub: Arc::new(LogPollHub::new(
                query_provider.clone(),
                websocket_provider.clone(),
                behavior,
            )),
            query_provider,
            websocket_provider,
            poll_interval,
            subscription_behavior: behavior,
        })
    }

    pub(crate) fn query_provider(&self) -> &DynProvider<AnyNetwork> {
        &self.query_provider
    }

    pub(crate) fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    pub(crate) fn for_step(&self, poll_interval: Option<Duration>) -> Self {
        let mut runtime = self.clone();
        if let Some(poll_interval) = poll_interval {
            runtime.poll_interval = poll_interval;
        }
        runtime
    }

    pub(crate) fn has_subscription(&self) -> bool {
        self.websocket_provider.is_some()
    }

    pub(crate) fn subscription_behavior(&self) -> SubscriptionBehavior {
        self.subscription_behavior
    }

    async fn subscribe_heads(
        &self,
        behavior: SubscriptionBehavior,
    ) -> Result<Option<WakeStream>, StepError> {
        if behavior == SubscriptionBehavior::Disabled {
            return Ok(None);
        }
        let require_subscription = behavior == SubscriptionBehavior::Require;
        let Some(provider) = &self.websocket_provider else {
            if require_subscription {
                return Err(StepError::new(
                    "configuration_error",
                    "subscription observation mode has no connected WebSocket",
                ));
            }
            return Ok(None);
        };
        match provider.subscribe_blocks().await {
            Ok(subscription) => {
                Ok(Some(Box::pin(subscription.into_stream().map(|_| ())) as WakeStream))
            }
            Err(error) if require_subscription => Err(StepError::rpc(error)),
            Err(_) => Ok(None),
        }
    }

    fn log_hub(&self) -> &Arc<LogPollHub> {
        &self.log_hub
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ObservationPoint {
    pub monotonic: Instant,
    pub wall: SystemTime,
}

impl ObservationPoint {
    pub(crate) fn now() -> Self {
        Self { monotonic: Instant::now(), wall: SystemTime::now() }
    }

    pub(crate) fn unix_ms(self) -> u64 {
        unix_ms(self.wall)
    }
}

/// Canonical inclusion and client-observation metadata returned alongside a
/// runtime save. Grouped receipt events deliberately share this one record.
#[derive(Debug, Clone)]
pub(crate) struct ObservationMetadata {
    pub first_observed: ObservationPoint,
    pub transaction_hash: TxHash,
    pub block_hash: B256,
    pub block_number: u64,
    pub transaction_index: Option<u64>,
    pub log_indices: Vec<u64>,
    pub block_timestamp_ms: Option<u64>,
    pub confirmation_depth: u64,
}

pub(crate) struct ReceiptResult {
    pub value: RuntimeValue,
    pub status: bool,
    pub observation: ObservationMetadata,
}

pub(crate) async fn wait_for_receipt(
    query_provider: &DynProvider<AnyNetwork>,
    submitter: &RpcSubmitter,
    chain: &str,
    sender: Option<Address>,
    transaction_hash: TxHash,
    poll_interval: Duration,
    confirmations: u64,
) -> Result<ReceiptResult, StepError> {
    let observation = ObservationRuntime::polling(query_provider.clone(), poll_interval);
    wait_for_receipt_observed(
        &observation,
        submitter,
        chain,
        sender,
        transaction_hash,
        confirmations,
        SubscriptionBehavior::Disabled,
    )
    .await
}

pub(crate) async fn wait_for_receipt_observed(
    observation: &ObservationRuntime,
    submitter: &RpcSubmitter,
    chain: &str,
    sender: Option<Address>,
    transaction_hash: TxHash,
    confirmations: u64,
    subscription: SubscriptionBehavior,
) -> Result<ReceiptResult, StepError> {
    let mut wake = observation.subscribe_heads(subscription).await?;
    let mut first_observed = None::<(B256, u64, ObservationPoint)>;
    loop {
        let receipt = submitter
            .get_transaction_receipt(sender, transaction_hash)
            .await
            .map_err(StepError::rpc)?;
        let Some(receipt) = receipt else {
            wait_for_wake(&mut wake, observation.poll_interval).await;
            continue;
        };

        let (Some(block_number), Some(block_hash)) = (receipt.block_number(), receipt.block_hash())
        else {
            wait_for_wake(&mut wake, observation.poll_interval).await;
            continue;
        };
        let candidate_first_observed = match first_observed {
            Some((seen_hash, seen_number, observed))
                if seen_hash == block_hash && seen_number == block_number =>
            {
                observed
            }
            _ => {
                let observed = ObservationPoint::now();
                first_observed = Some((block_hash, block_number, observed));
                observed
            }
        };
        wait_for_confirmations(observation, block_number, confirmations, &mut wake).await?;

        // Re-fetch after the confirmation wait. A receipt that vanished or moved
        // was reorged and must not be exposed through an immutable save.
        let canonical = submitter
            .get_transaction_receipt(sender, transaction_hash)
            .await
            .map_err(StepError::rpc)?;
        let Some(canonical) = canonical else {
            continue;
        };
        if canonical.block_hash() != receipt.block_hash() ||
            canonical.block_number() != receipt.block_number()
        {
            continue;
        }
        let Some(block) =
            canonical_block(observation.query_provider(), block_number, block_hash).await?
        else {
            wait_for_wake(&mut wake, observation.poll_interval).await;
            continue;
        };
        let Some(confirmation_depth) =
            current_confirmation_depth(observation, block_number, confirmations).await?
        else {
            wait_for_wake(&mut wake, observation.poll_interval).await;
            continue;
        };

        let confirmed = ObservationPoint::now();
        return Ok(receipt_runtime_value(
            chain,
            &canonical,
            candidate_first_observed,
            confirmed,
            block.timestamp_ms,
            confirmation_depth,
        ));
    }
}

#[cfg(test)]
async fn receipt_is_canonical_on_query(
    provider: &DynProvider<AnyNetwork>,
    receipt: &AnyTransactionReceipt,
) -> Result<bool, StepError> {
    let (Some(block_number), Some(receipt_block_hash)) =
        (receipt.block_number(), receipt.block_hash())
    else {
        return Ok(false);
    };
    Ok(canonical_block(provider, block_number, receipt_block_hash).await?.is_some())
}

#[derive(Debug, Clone, Copy)]
struct CanonicalBlock {
    timestamp_ms: Option<u64>,
}

async fn canonical_block(
    provider: &DynProvider<AnyNetwork>,
    block_number: u64,
    expected_hash: B256,
) -> Result<Option<CanonicalBlock>, StepError> {
    let Some(block) = block_by_number(provider, block_number).await? else {
        return Ok(None);
    };
    if block.header().hash != expected_hash {
        return Ok(None);
    }
    Ok(Some(CanonicalBlock { timestamp_ms: block_timestamp_ms(&block)? }))
}

pub(crate) async fn canonical_block_hash(
    provider: &DynProvider<AnyNetwork>,
    block_number: u64,
) -> Result<Option<B256>, StepError> {
    Ok(block_by_number(provider, block_number).await?.map(|block| block.header().hash))
}

async fn block_by_number(
    provider: &DynProvider<AnyNetwork>,
    block_number: u64,
) -> Result<Option<AnyRpcBlock>, StepError> {
    provider
        .get_block_by_number(BlockNumberOrTag::Number(block_number))
        .await
        .map_err(StepError::rpc)
}

fn block_timestamp_ms(block: &AnyRpcBlock) -> Result<Option<u64>, StepError> {
    // Tempo exposes the full millisecond value as `timestampMillis`. Its
    // consensus header also contains `timestampMillisPart`, which is only the
    // 0..999 sub-second component. Prefer the full field whenever present.
    let timestamp_millis = block
        .other_fields()
        .and_then(|fields| fields.get_deserialized::<serde_json::Value>("timestampMillis"))
        .transpose()
        .map_err(|_| StepError::rpc("query RPC block had an invalid timestampMillis"))?;
    if let Some(value) = timestamp_millis.filter(|value| !value.is_null()) {
        return parse_quantity_u64(&value)
            .map(Some)
            .map_err(|_| StepError::rpc("query RPC block had an invalid timestampMillis"));
    }

    let millis_part = block
        .other_fields()
        .and_then(|fields| fields.get_deserialized::<serde_json::Value>("timestampMillisPart"))
        .transpose()
        .map_err(|_| StepError::rpc("query RPC block had an invalid timestampMillisPart"))?
        .filter(|value| !value.is_null())
        .map(|value| parse_quantity_u64(&value))
        .transpose()
        .map_err(|_| StepError::rpc("query RPC block had an invalid timestampMillisPart"))?
        .unwrap_or(0);
    Ok(Some(block.header().timestamp.saturating_mul(1_000).saturating_add(millis_part)))
}

fn parse_quantity_u64(value: &serde_json::Value) -> Result<u64> {
    if let Some(value) = value.as_u64() {
        return Ok(value);
    }
    let value = value.as_str().ok_or_else(|| eyre::eyre!("quantity is not a string or integer"))?;
    let digits = value.strip_prefix("0x").unwrap_or(value);
    if digits.is_empty() {
        bail!("quantity is empty");
    }
    if value.starts_with("0x") {
        u64::from_str_radix(digits, 16).map_err(Into::into)
    } else {
        value.parse().map_err(Into::into)
    }
}

fn receipt_runtime_value(
    chain: &str,
    receipt: &AnyTransactionReceipt,
    first_observed: ObservationPoint,
    confirmed: ObservationPoint,
    block_timestamp_ms: Option<u64>,
    confirmation_depth: u64,
) -> ReceiptResult {
    let status = receipt.status();
    let transaction_hash = receipt.transaction_hash();
    let block_hash = receipt.block_hash().expect("canonical receipt has a block hash");
    let block_number = receipt.block_number().expect("canonical receipt has a block number");
    let transaction_index = receipt.transaction_index();
    ReceiptResult {
        status,
        observation: ObservationMetadata {
            first_observed,
            transaction_hash,
            block_hash,
            block_number,
            transaction_index,
            log_indices: Vec::new(),
            block_timestamp_ms,
            confirmation_depth,
        },
        value: object([
            ("chain", RuntimeValue::String(chain.to_string())),
            ("transaction_hash", RuntimeValue::Bytes32(transaction_hash)),
            ("tx_hash", RuntimeValue::Bytes32(transaction_hash)),
            ("block_hash", RuntimeValue::Bytes32(block_hash)),
            ("block_number", RuntimeValue::Uint(U256::from(block_number))),
            (
                "transaction_index",
                transaction_index
                    .map(|value| RuntimeValue::Uint(U256::from(value)))
                    .unwrap_or(RuntimeValue::Null),
            ),
            ("status", RuntimeValue::Bool(status)),
            ("gas_used", RuntimeValue::Uint(U256::from(receipt.gas_used()))),
            (
                "block_timestamp_ms",
                block_timestamp_ms
                    .map(|value| RuntimeValue::Uint(U256::from(value)))
                    .unwrap_or(RuntimeValue::Null),
            ),
            ("first_observed_at", RuntimeValue::Uint(U256::from(first_observed.unix_ms()))),
            ("observed_at", RuntimeValue::Uint(U256::from(first_observed.unix_ms()))),
            ("confirmed_at", RuntimeValue::Uint(U256::from(confirmed.unix_ms()))),
            ("confirmation_depth", RuntimeValue::Uint(U256::from(confirmation_depth))),
        ]),
    }
}

async fn wait_for_confirmations(
    observation: &ObservationRuntime,
    block_number: u64,
    confirmations: u64,
    wake: &mut Option<WakeStream>,
) -> Result<u64, StepError> {
    // Zero means inclusion itself. Canonical block verification below is
    // sufficient and, importantly, does not wait for another head.
    if confirmations == 0 {
        return Ok(0);
    }
    let target = block_number.saturating_add(confirmations);
    loop {
        let current =
            observation.query_provider.get_block_number().await.map_err(StepError::rpc)?;
        if current >= target {
            return Ok(current.saturating_sub(block_number));
        }
        wait_for_wake(wake, observation.poll_interval).await;
    }
}

async fn current_confirmation_depth(
    observation: &ObservationRuntime,
    block_number: u64,
    confirmations: u64,
) -> Result<Option<u64>, StepError> {
    if confirmations == 0 {
        return Ok(Some(0));
    }
    let current = observation.query_provider.get_block_number().await.map_err(StepError::rpc)?;
    let target = block_number.saturating_add(confirmations);
    Ok((current >= target).then(|| current.saturating_sub(block_number)))
}

pub(crate) async fn wait_for_wake(wake: &mut Option<WakeStream>, poll_interval: Duration) {
    let Some(stream) = wake.as_mut() else {
        tokio::time::sleep(poll_interval).await;
        return;
    };
    tokio::select! {
        _ = tokio::time::sleep(poll_interval) => {}
        notification = stream.next() => {
            if notification.is_none() {
                *wake = None;
            }
        }
    }
}

#[cfg(test)]
pub(crate) async fn wait_for_log(
    query_provider: &DynProvider<AnyNetwork>,
    submitter: &RpcSubmitter,
    chain: &str,
    abi: &JsonAbi,
    step: &WaitLogStep,
    context: &RuntimeContext,
) -> Result<RuntimeValue, StepError> {
    let observation = ObservationRuntime::polling(
        query_provider.clone(),
        step.poll_interval.unwrap_or(DEFAULT_POLL_INTERVAL),
    );
    wait_for_log_observed(
        &observation,
        submitter,
        chain,
        abi,
        step,
        context,
        SubscriptionBehavior::Disabled,
    )
    .await
    .map(|result| result.value)
}

pub(crate) struct LogResult {
    pub value: RuntimeValue,
    pub observation: ObservationMetadata,
}

pub(crate) async fn wait_for_log_observed(
    observation: &ObservationRuntime,
    submitter: &RpcSubmitter,
    chain: &str,
    abi: &JsonAbi,
    step: &WaitLogStep,
    context: &RuntimeContext,
    subscription: SubscriptionBehavior,
) -> Result<LogResult, StepError> {
    let matcher =
        EventMatcher::new(abi, &step.event, &step.where_value, context).map_err(StepError::abi)?;
    let address = step
        .address
        .as_ref()
        .map(|value| expression_address(value, context))
        .transpose()
        .map_err(StepError::expression)?;
    let transaction_hash = step
        .transaction_hash
        .as_ref()
        .map(|value| expression_hash(value, context))
        .transpose()
        .map_err(StepError::expression)?;
    let sender = step
        .sender
        .as_ref()
        .map(|value| expression_address(value, context))
        .transpose()
        .map_err(StepError::expression)?;
    let from_block = step
        .from_block
        .as_ref()
        .map(|value| expression_u64(value, context))
        .transpose()
        .map_err(StepError::expression)?;
    let poll_interval = observation.poll_interval();
    let confirmations = step.confirmations.unwrap_or(0);

    if from_block.is_none() {
        let transaction_hash = transaction_hash.ok_or_else(|| {
            StepError::missing("wait_log requires a start block or transaction hash")
        })?;
        return wait_for_transaction_log(
            observation,
            submitter,
            TransactionLogWait {
                chain,
                sender,
                transaction_hash,
                address,
                matcher: &matcher,
                confirmations,
                subscription,
            },
        )
        .await;
    }

    let start_block = from_block.expect("checked above");
    let max_range = step.max_block_range.unwrap_or(DEFAULT_MAX_BLOCK_RANGE);
    if subscription == SubscriptionBehavior::Require && !observation.has_subscription() {
        return Err(StepError::new(
            "configuration_error",
            "subscription observation mode has no connected WebSocket",
        ));
    }

    // All log waits on this chain feed one shared poller. This step only
    // issues its own RPC calls for blocks below the shared window and to
    // canonically verify a candidate the shared scan surfaced.
    let mut window_sub = observation.log_hub().subscribe(LogInterest {
        topic0: (!matcher.event.anonymous).then(|| matcher.event.selector()),
        address,
        start_block,
        poll_interval,
        max_block_range: max_range,
    });
    let mut current_epoch = None::<u64>;
    let mut next_unscanned = start_block;
    loop {
        let window = window_sub.next_window().await?;
        if current_epoch != Some(window.epoch) {
            // A reorg may have changed history below the shared window, so
            // everything from the requested start must be rescanned.
            current_epoch = Some(window.epoch);
            next_unscanned = start_block;
        }

        if window.coverage_start > next_unscanned &&
            let Some((canonical, canonical_decoded)) = find_first_canonical_log(
                &observation.query_provider,
                &matcher,
                address,
                transaction_hash,
                next_unscanned,
                window.coverage_start - 1,
                max_range,
            )
            .await? &&
            let Some(result) = finalize_canonical_log(
                observation,
                &mut window_sub,
                chain,
                &matcher,
                canonical,
                canonical_decoded,
                ObservationPoint::now(),
                confirmations,
            )
            .await?
        {
            return Ok(result);
        }

        for candidate in window.logs.iter() {
            if candidate.removed ||
                !window_sub.interest().matches(candidate) ||
                transaction_hash
                    .is_some_and(|expected| candidate.transaction_hash != Some(expected))
            {
                continue;
            }
            if matcher.decode_if_matches(candidate).map_err(StepError::abi)?.is_none() {
                continue;
            }
            let candidate_block = candidate
                .block_number
                .ok_or_else(|| StepError::missing("matching log omitted block_number"))?;
            // Re-derive the first canonical match over the complete requested
            // range so an earlier log the shared window no longer covers is
            // still preferred over this candidate.
            let Some((canonical, canonical_decoded)) = find_first_canonical_log(
                &observation.query_provider,
                &matcher,
                address,
                transaction_hash,
                start_block,
                candidate_block,
                max_range,
            )
            .await?
            else {
                continue;
            };
            let candidate_first_observed = if same_log_identity(candidate, &canonical) {
                window.observed
            } else {
                // A reorg can replace the candidate between the shared scan
                // and canonical backfill. Do not attribute the replacement to
                // an observation that preceded it.
                ObservationPoint::now()
            };
            if let Some(result) = finalize_canonical_log(
                observation,
                &mut window_sub,
                chain,
                &matcher,
                canonical,
                canonical_decoded,
                candidate_first_observed,
                confirmations,
            )
            .await?
            {
                return Ok(result);
            }
        }

        next_unscanned = next_unscanned.max(window.head.saturating_add(1));
    }
}

/// Confirm and package a canonically verified log match. Returns `Ok(None)`
/// when the chain moved underneath the match (block no longer canonical, or
/// the head regressed below the confirmation target), in which case the
/// caller resumes waiting on the shared window stream.
#[allow(clippy::too_many_arguments)]
async fn finalize_canonical_log(
    observation: &ObservationRuntime,
    window_sub: &mut LogWindowSubscription,
    chain: &str,
    matcher: &EventMatcher,
    canonical: Log,
    canonical_decoded: Vec<DynSolValue>,
    first_observed: ObservationPoint,
    confirmations: u64,
) -> Result<Option<LogResult>, StepError> {
    let canonical_number = canonical
        .block_number
        .ok_or_else(|| StepError::missing("matching log omitted block_number"))?;
    let canonical_hash = canonical
        .block_hash
        .ok_or_else(|| StepError::missing("matching log omitted block_hash"))?;
    if confirmations > 0 {
        window_sub.wait_for_head(canonical_number.saturating_add(confirmations)).await?;
    }
    let Some(block) =
        canonical_block(&observation.query_provider, canonical_number, canonical_hash).await?
    else {
        return Ok(None);
    };
    let confirmation_depth = if confirmations == 0 {
        0
    } else {
        match window_sub.latest_head() {
            Some(head) if head >= canonical_number.saturating_add(confirmations) => {
                head.saturating_sub(canonical_number)
            }
            _ => return Ok(None),
        }
    };
    let confirmed = ObservationPoint::now();
    Ok(Some(LogResult {
        observation: log_observation_metadata(
            &canonical,
            first_observed,
            block.timestamp_ms,
            confirmation_depth,
        )?,
        value: log_runtime_value(
            chain,
            &matcher.event,
            &canonical,
            canonical_decoded,
            first_observed,
            confirmed,
            block.timestamp_ms,
            confirmation_depth,
        )
        .map_err(StepError::abi)?,
    }))
}

fn same_log_identity(left: &Log, right: &Log) -> bool {
    left.block_hash == right.block_hash &&
        left.block_number == right.block_number &&
        left.transaction_hash == right.transaction_hash &&
        left.transaction_index == right.transaction_index &&
        left.log_index == right.log_index
}

pub(crate) fn bounded_range_end(start: u64, through: u64, max_range: u64) -> u64 {
    start.saturating_add(max_range.saturating_sub(1)).min(through)
}

async fn find_first_canonical_log(
    provider: &DynProvider<AnyNetwork>,
    matcher: &EventMatcher,
    address: Option<Address>,
    transaction_hash: Option<TxHash>,
    start: u64,
    through: u64,
    max_range: u64,
) -> Result<Option<(Log, Vec<DynSolValue>)>, StepError> {
    if start > through {
        return Ok(None);
    }

    loop {
        let Some(endpoint_hash_before) = canonical_block_hash(provider, through).await? else {
            return Ok(None);
        };
        let mut cursor = start;
        let mut first = None;
        loop {
            let end = bounded_range_end(cursor, through, max_range);
            let filter = matcher.rpc_filter(cursor, end, address);
            let mut logs = provider.get_logs(&filter).await.map_err(StepError::rpc)?;
            sort_logs(&mut logs);

            for log in logs {
                if log.removed ||
                    transaction_hash
                        .is_some_and(|expected| log.transaction_hash != Some(expected))
                {
                    continue;
                }
                if let Some(decoded) = matcher.decode_if_matches(&log).map_err(StepError::abi)? {
                    first = Some((log, decoded));
                    break;
                }
            }

            if first.is_some() || end == through {
                break;
            }
            cursor = end.saturating_add(1);
        }

        if canonical_block_hash(provider, through).await? == Some(endpoint_hash_before) {
            return Ok(first);
        }
    }
}

struct TransactionLogWait<'a> {
    chain: &'a str,
    sender: Option<Address>,
    transaction_hash: TxHash,
    address: Option<Address>,
    matcher: &'a EventMatcher,
    confirmations: u64,
    subscription: SubscriptionBehavior,
}

async fn wait_for_transaction_log(
    observation: &ObservationRuntime,
    submitter: &RpcSubmitter,
    request: TransactionLogWait<'_>,
) -> Result<LogResult, StepError> {
    let TransactionLogWait {
        chain,
        sender,
        transaction_hash,
        address,
        matcher,
        confirmations,
        subscription,
    } = request;
    let mut wake = observation.subscribe_heads(subscription).await?;
    let mut first_observed = None::<(B256, u64, ObservationPoint)>;
    loop {
        let receipt = submitter
            .get_transaction_receipt(sender, transaction_hash)
            .await
            .map_err(StepError::rpc)?;
        let Some(receipt) = receipt else {
            wait_for_wake(&mut wake, observation.poll_interval).await;
            continue;
        };
        let (Some(block_number), Some(block_hash)) = (receipt.block_number(), receipt.block_hash())
        else {
            wait_for_wake(&mut wake, observation.poll_interval).await;
            continue;
        };
        let candidate_first_observed = match first_observed {
            Some((seen_hash, seen_number, observed))
                if seen_hash == block_hash && seen_number == block_number =>
            {
                observed
            }
            _ => {
                let observed = ObservationPoint::now();
                first_observed = Some((block_hash, block_number, observed));
                observed
            }
        };
        wait_for_confirmations(observation, block_number, confirmations, &mut wake).await?;

        let canonical = submitter
            .get_transaction_receipt(sender, transaction_hash)
            .await
            .map_err(StepError::rpc)?;
        let Some(canonical) = canonical else { continue };
        if canonical.block_hash() != receipt.block_hash() ||
            canonical.block_number() != receipt.block_number()
        {
            continue;
        }
        let Some(block) =
            canonical_block(&observation.query_provider, block_number, block_hash).await?
        else {
            wait_for_wake(&mut wake, observation.poll_interval).await;
            continue;
        };
        let Some(confirmation_depth) =
            current_confirmation_depth(observation, block_number, confirmations).await?
        else {
            wait_for_wake(&mut wake, observation.poll_interval).await;
            continue;
        };

        let mut logs = canonical.logs().to_vec();
        sort_logs(&mut logs);
        for log in logs {
            if log.removed || address.is_some_and(|expected| log.address() != expected) {
                continue;
            }
            if let Some(decoded) = matcher.decode_if_matches(&log).map_err(StepError::abi)? {
                let confirmed = ObservationPoint::now();
                return Ok(LogResult {
                    observation: log_observation_metadata(
                        &log,
                        candidate_first_observed,
                        block.timestamp_ms,
                        confirmation_depth,
                    )?,
                    value: log_runtime_value(
                        chain,
                        &matcher.event,
                        &log,
                        decoded,
                        candidate_first_observed,
                        confirmed,
                        block.timestamp_ms,
                        confirmation_depth,
                    )
                    .map_err(StepError::abi)?,
                });
            }
        }
        return Err(StepError::missing(
            "confirmed transaction receipt contained no matching canonical event",
        ));
    }
}

/// One required event in a receipt-scoped grouped wait.
///
/// The matcher owns its resolved ABI event, so the engine can prepare these
/// from differently named artifacts before entering the asynchronous wait.
pub(crate) struct PreparedReceiptEvent {
    id: String,
    address: Option<Address>,
    matcher: EventMatcher,
}

fn complete_receipt_event_assignment(
    events: &[PreparedReceiptEvent],
    logs: &[Log],
) -> Result<Vec<usize>, StepError> {
    let mut candidates = Vec::with_capacity(events.len());
    for required in events {
        let mut matching_logs = Vec::new();
        for (position, log) in logs.iter().enumerate() {
            if log.removed || required.address.is_some_and(|expected| log.address() != expected) {
                continue;
            }
            if required.matcher.decode_if_matches(log).map_err(StepError::abi)?.is_some() {
                matching_logs.push(position);
            }
        }
        candidates.push(matching_logs);
    }

    fn augment_remaining(
        event_index: usize,
        candidates: &[Vec<usize>],
        unavailable_logs: &[bool],
        log_owners: &mut [Option<usize>],
        visited_logs: &mut [bool],
    ) -> bool {
        for &log_index in &candidates[event_index] {
            if unavailable_logs[log_index] || visited_logs[log_index] {
                continue;
            }
            visited_logs[log_index] = true;
            let previous_owner = log_owners[log_index];
            if previous_owner.is_none() ||
                augment_remaining(
                    previous_owner.expect("checked above"),
                    candidates,
                    unavailable_logs,
                    log_owners,
                    visited_logs,
                )
            {
                log_owners[log_index] = Some(event_index);
                return true;
            }
        }
        false
    }

    fn remaining_events_have_complete_assignment(
        first_event: usize,
        candidates: &[Vec<usize>],
        unavailable_logs: &[bool],
    ) -> bool {
        let mut log_owners = vec![None; unavailable_logs.len()];
        for event_index in first_event..candidates.len() {
            let mut visited_logs = vec![false; unavailable_logs.len()];
            if !augment_remaining(
                event_index,
                candidates,
                unavailable_logs,
                &mut log_owners,
                &mut visited_logs,
            ) {
                return false;
            }
        }
        true
    }

    // Select the first canonical log for each required event that still permits
    // a complete assignment for every later requirement. This is deterministic
    // and preserves the old first-match order when no backtracking is needed.
    let mut unavailable_logs = vec![false; logs.len()];
    let mut assignment = vec![usize::MAX; events.len()];
    for (event_index, required) in events.iter().enumerate() {
        for &log_index in &candidates[event_index] {
            if unavailable_logs[log_index] {
                continue;
            }
            unavailable_logs[log_index] = true;
            if remaining_events_have_complete_assignment(
                event_index + 1,
                &candidates,
                &unavailable_logs,
            ) {
                assignment[event_index] = log_index;
                break;
            }
            unavailable_logs[log_index] = false;
        }
        if assignment[event_index] == usize::MAX {
            return Err(StepError::missing(format!(
                "confirmed transaction receipt contained no complete canonical event assignment for '{}'",
                required.id
            )));
        }
    }

    Ok(assignment)
}

pub(crate) fn prepare_receipt_event(
    id: impl Into<String>,
    abi: &JsonAbi,
    event: &str,
    address: Option<&serde_yaml::Value>,
    filters: &BTreeMap<String, serde_yaml::Value>,
    context: &RuntimeContext,
) -> Result<PreparedReceiptEvent, StepError> {
    let address = address
        .map(|value| expression_address(value, context))
        .transpose()
        .map_err(StepError::expression)?;
    let matcher = EventMatcher::new(abi, event, filters, context).map_err(StepError::abi)?;
    Ok(PreparedReceiptEvent { id: id.into(), address, matcher })
}

/// Observe one canonical receipt and decode all required events from that
/// receipt. The returned events retain their own decoded arguments and log
/// indexes, while sharing one inclusion/observation milestone.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn wait_for_transaction_events(
    observation: &ObservationRuntime,
    submitter: &RpcSubmitter,
    chain: &str,
    sender: Option<Address>,
    transaction_hash: TxHash,
    events: &[PreparedReceiptEvent],
    confirmations: u64,
    subscription: SubscriptionBehavior,
) -> Result<LogResult, StepError> {
    if events.is_empty() {
        return Err(StepError::missing(
            "receipt-scoped event group must contain at least one event",
        ));
    }

    let mut wake = observation.subscribe_heads(subscription).await?;
    let mut first_observed = None::<(B256, u64, ObservationPoint)>;
    loop {
        let receipt = submitter
            .get_transaction_receipt(sender, transaction_hash)
            .await
            .map_err(StepError::rpc)?;
        let Some(receipt) = receipt else {
            wait_for_wake(&mut wake, observation.poll_interval).await;
            continue;
        };
        let (Some(block_number), Some(block_hash)) = (receipt.block_number(), receipt.block_hash())
        else {
            wait_for_wake(&mut wake, observation.poll_interval).await;
            continue;
        };
        let candidate_first_observed = match first_observed {
            Some((seen_hash, seen_number, observed))
                if seen_hash == block_hash && seen_number == block_number =>
            {
                observed
            }
            _ => {
                let observed = ObservationPoint::now();
                first_observed = Some((block_hash, block_number, observed));
                observed
            }
        };
        wait_for_confirmations(observation, block_number, confirmations, &mut wake).await?;

        let canonical = submitter
            .get_transaction_receipt(sender, transaction_hash)
            .await
            .map_err(StepError::rpc)?;
        let Some(canonical) = canonical else { continue };
        if canonical.block_hash() != Some(block_hash) ||
            canonical.block_number() != Some(block_number)
        {
            continue;
        }
        let Some(block) =
            canonical_block(&observation.query_provider, block_number, block_hash).await?
        else {
            wait_for_wake(&mut wake, observation.poll_interval).await;
            continue;
        };
        let Some(confirmation_depth) =
            current_confirmation_depth(observation, block_number, confirmations).await?
        else {
            wait_for_wake(&mut wake, observation.poll_interval).await;
            continue;
        };

        let mut logs = canonical.logs().to_vec();
        sort_logs(&mut logs);
        let mut decoded_events = BTreeMap::new();
        let mut log_indices = Vec::with_capacity(events.len());
        let assignment = complete_receipt_event_assignment(events, &logs)?;

        for (required, position) in events.iter().zip(assignment) {
            let log = &logs[position];
            let decoded = required
                .matcher
                .decode_if_matches(log)
                .map_err(StepError::abi)?
                .expect("complete assignment selects only matching logs");
            if let Some(index) = log.log_index {
                log_indices.push(index);
            }
            decoded_events.insert(
                required.id.clone(),
                grouped_event_runtime_value(&required.matcher.event, log, decoded)
                    .map_err(StepError::abi)?,
            );
        }

        let confirmed = ObservationPoint::now();
        let transaction_index = canonical.transaction_index();
        let status = canonical.status();
        let block_timestamp = block.timestamp_ms;
        let observation_metadata = ObservationMetadata {
            first_observed: candidate_first_observed,
            transaction_hash,
            block_hash,
            block_number,
            transaction_index,
            log_indices,
            block_timestamp_ms: block_timestamp,
            confirmation_depth,
        };
        let value = object([
            ("chain", RuntimeValue::String(chain.to_string())),
            ("transaction_hash", RuntimeValue::Bytes32(transaction_hash)),
            ("tx_hash", RuntimeValue::Bytes32(transaction_hash)),
            ("block_hash", RuntimeValue::Bytes32(block_hash)),
            ("block_number", RuntimeValue::Uint(U256::from(block_number))),
            (
                "transaction_index",
                transaction_index
                    .map(|value| RuntimeValue::Uint(U256::from(value)))
                    .unwrap_or(RuntimeValue::Null),
            ),
            ("status", RuntimeValue::Bool(status)),
            ("gas_used", RuntimeValue::Uint(U256::from(canonical.gas_used()))),
            (
                "block_timestamp_ms",
                block_timestamp
                    .map(|value| RuntimeValue::Uint(U256::from(value)))
                    .unwrap_or(RuntimeValue::Null),
            ),
            (
                "first_observed_at",
                RuntimeValue::Uint(U256::from(candidate_first_observed.unix_ms())),
            ),
            ("observed_at", RuntimeValue::Uint(U256::from(candidate_first_observed.unix_ms()))),
            ("confirmed_at", RuntimeValue::Uint(U256::from(confirmed.unix_ms()))),
            ("confirmation_depth", RuntimeValue::Uint(U256::from(confirmation_depth))),
            ("events", RuntimeValue::Object(decoded_events)),
        ]);
        return Ok(LogResult { value, observation: observation_metadata });
    }
}

struct EventMatcher {
    event: Event,
    parameters: Vec<EventParameter>,
    expected: BTreeMap<String, DynSolValue>,
}

struct EventParameter {
    name: String,
    indexed: bool,
    sol_type: DynSolType,
}

impl EventMatcher {
    fn new(
        abi: &JsonAbi,
        event_name: &str,
        filters: &BTreeMap<String, serde_yaml::Value>,
        context: &RuntimeContext,
    ) -> Result<Self> {
        let (event, parameters, names) = resolve_event_parameters(abi, event_name)?;
        validate_filter_names(&parameters, &names, filters.keys().map(String::as_str))?;

        let mut expected = BTreeMap::new();
        for (name, expression) in filters {
            let parameter = parameters
                .iter()
                .find(|parameter| parameter.name == *name)
                .expect("filter names were validated");
            let value = if parameter.indexed && indexed_value_is_hashed(&parameter.sol_type) {
                let runtime = eval_expression(expression, context)
                    .wrap_err_with(|| format!("failed to resolve event filter '{name}'"))?;
                match runtime {
                    RuntimeValue::Bytes32(hash) => DynSolValue::FixedBytes(hash, 32),
                    runtime => {
                        let value = runtime
                            .coerce_dyn_sol(&parameter.sol_type)
                            .wrap_err_with(|| format!("failed to resolve event filter '{name}'"))?;
                        DynSolValue::FixedBytes(keccak256(indexed_value_preimage(&value)), 32)
                    }
                }
            } else {
                coerce_event_filter(expression, &parameter.sol_type, context)
                    .wrap_err_with(|| format!("failed to resolve event filter '{name}'"))?
            };
            expected.insert(name.clone(), value);
        }

        Ok(Self { event, parameters, expected })
    }

    fn rpc_filter(&self, from_block: u64, to_block: u64, address: Option<Address>) -> Filter {
        self.subscription_filter(address).from_block(from_block).to_block(to_block)
    }

    fn subscription_filter(&self, address: Option<Address>) -> Filter {
        let mut filter = Filter::new();
        if let Some(address) = address {
            filter = filter.address(address);
        }
        if !self.event.anonymous {
            filter = filter.event_signature(self.event.selector());
        }
        filter
    }

    fn decode_if_matches(&self, log: &Log) -> Result<Option<Vec<DynSolValue>>> {
        let decoded = match self.event.decode_log(&log.inner.data) {
            Ok(decoded) => decoded,
            Err(_) => return Ok(None),
        };
        let mut indexed = decoded.indexed.into_iter();
        let mut body = decoded.body.into_iter();
        let mut values = Vec::with_capacity(self.parameters.len());
        for parameter in &self.parameters {
            let value = if parameter.indexed { indexed.next() } else { body.next() }
                .ok_or_else(|| eyre::eyre!("decoded event argument count mismatch"))?;
            if let Some(expected) = self.expected.get(&parameter.name) &&
                *expected != value
            {
                return Ok(None);
            }
            values.push(value);
        }
        Ok(Some(values))
    }
}

fn resolve_event_parameters(
    abi: &JsonAbi,
    event_name: &str,
) -> Result<(Event, Vec<EventParameter>, BTreeMap<String, usize>)> {
    let event = resolve_event(abi, event_name)?.clone();
    let mut parameters = Vec::with_capacity(event.inputs.len());
    let mut names = BTreeMap::<String, usize>::new();
    for (index, parameter) in event.inputs.iter().enumerate() {
        let sol_type = parameter.resolve().wrap_err_with(|| {
            format!("failed to parse event parameter {} type '{}'", index, parameter.ty)
        })?;
        if !parameter.name.is_empty() {
            *names.entry(parameter.name.clone()).or_default() += 1;
        }
        parameters.push(EventParameter {
            name: parameter.name.clone(),
            indexed: parameter.indexed,
            sol_type,
        });
    }
    Ok((event, parameters, names))
}

fn validate_filter_names<'a>(
    parameters: &[EventParameter],
    names: &BTreeMap<String, usize>,
    filters: impl IntoIterator<Item = &'a str>,
) -> Result<()> {
    for name in filters {
        if names.get(name).copied().unwrap_or(0) > 1 {
            bail!("event argument filter '{name}' is ambiguous");
        }
        if !parameters.iter().any(|parameter| parameter.name == name) {
            bail!("event has no argument named '{name}'");
        }
    }
    Ok(())
}

pub(crate) struct EventFilterType {
    pub sol_type: DynSolType,
    pub accepts_precomputed_hash: bool,
}

pub(crate) fn resolve_event_filter_types(
    abi: &JsonAbi,
    event_name: &str,
    filters: &BTreeMap<String, serde_yaml::Value>,
) -> Result<BTreeMap<String, EventFilterType>> {
    let (_, parameters, names) = resolve_event_parameters(abi, event_name)?;
    validate_filter_names(&parameters, &names, filters.keys().map(String::as_str))?;
    Ok(filters
        .keys()
        .map(|name| {
            let parameter = parameters
                .iter()
                .find(|parameter| parameter.name == *name)
                .expect("filter names were validated");
            (
                name.clone(),
                EventFilterType {
                    sol_type: parameter.sol_type.clone(),
                    accepts_precomputed_hash: parameter.indexed &&
                        indexed_value_is_hashed(&parameter.sol_type),
                },
            )
        })
        .collect())
}

pub(crate) fn validate_constant_event_filter(
    expression: &serde_yaml::Value,
    filter_type: &EventFilterType,
) -> Result<()> {
    let runtime = eval_expression(expression, &RuntimeContext::empty())?;
    if filter_type.accepts_precomputed_hash && matches!(runtime, RuntimeValue::Bytes32(_)) {
        return Ok(());
    }
    runtime.coerce_dyn_sol(&filter_type.sol_type).map(|_| ())
}

fn indexed_value_is_hashed(sol_type: &DynSolType) -> bool {
    !matches!(
        sol_type,
        DynSolType::Address |
            DynSolType::Function |
            DynSolType::Bool |
            DynSolType::FixedBytes(_) |
            DynSolType::Int(_) |
            DynSolType::Uint(_)
    )
}

fn indexed_value_preimage(value: &DynSolValue) -> Vec<u8> {
    fn encode(value: &DynSolValue, nested: bool, output: &mut Vec<u8>) {
        match value {
            DynSolValue::String(value) => encode_bytes(value.as_bytes(), nested, output),
            DynSolValue::Bytes(value) => encode_bytes(value, nested, output),
            DynSolValue::Array(values) |
            DynSolValue::FixedArray(values) |
            DynSolValue::Tuple(values) => {
                for value in values {
                    encode(value, true, output);
                }
            }
            DynSolValue::CustomStruct { tuple, .. } => {
                for value in tuple {
                    encode(value, true, output);
                }
            }
            _ => output.extend_from_slice(
                value.as_word().expect("non-composite ABI value is one word").as_slice(),
            ),
        }
    }

    fn encode_bytes(value: &[u8], nested: bool, output: &mut Vec<u8>) {
        output.extend_from_slice(value);
        if nested {
            let padding = if value.is_empty() { 32 } else { (32 - value.len() % 32) % 32 };
            output.resize(output.len() + padding, 0);
        }
    }

    let mut output = Vec::new();
    encode(value, false, &mut output);
    output
}

fn resolve_event<'a>(abi: &'a JsonAbi, name_or_signature: &str) -> Result<&'a Event> {
    if name_or_signature.contains('(') {
        return abi
            .events()
            .find(|event| event.signature() == name_or_signature)
            .ok_or_else(|| eyre::eyre!("event signature '{name_or_signature}' not found in ABI"));
    }

    let events = abi
        .event(name_or_signature)
        .ok_or_else(|| eyre::eyre!("event '{name_or_signature}' not found in ABI"))?;
    match events.as_slice() {
        [event] => Ok(event),
        [] => unreachable!("JsonAbi::event returned an empty overload list"),
        _ => {
            bail!("event name '{name_or_signature}' is overloaded; use an exact signature");
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn log_runtime_value(
    chain: &str,
    event: &Event,
    log: &Log,
    values: Vec<DynSolValue>,
    first_observed: ObservationPoint,
    confirmed: ObservationPoint,
    block_timestamp_ms: Option<u64>,
    confirmation_depth: u64,
) -> Result<RuntimeValue> {
    let arguments = decoded_event_arguments(event, values)?;

    Ok(object([
        ("chain", RuntimeValue::String(chain.to_string())),
        ("address", RuntimeValue::Address(log.address())),
        ("contract_address", RuntimeValue::Address(log.address())),
        (
            "transaction_hash",
            log.transaction_hash.map(RuntimeValue::Bytes32).unwrap_or(RuntimeValue::Null),
        ),
        ("tx_hash", log.transaction_hash.map(RuntimeValue::Bytes32).unwrap_or(RuntimeValue::Null)),
        ("block_hash", log.block_hash.map(RuntimeValue::Bytes32).unwrap_or(RuntimeValue::Null)),
        (
            "block_number",
            log.block_number
                .map(|value| RuntimeValue::Uint(U256::from(value)))
                .unwrap_or(RuntimeValue::Null),
        ),
        (
            "transaction_index",
            log.transaction_index
                .map(|value| RuntimeValue::Uint(U256::from(value)))
                .unwrap_or(RuntimeValue::Null),
        ),
        (
            "log_index",
            log.log_index
                .map(|value| RuntimeValue::Uint(U256::from(value)))
                .unwrap_or(RuntimeValue::Null),
        ),
        ("event", RuntimeValue::String(event.name.clone())),
        ("event_name", RuntimeValue::String(event.name.clone())),
        ("args", RuntimeValue::Object(arguments)),
        (
            "block_timestamp_ms",
            block_timestamp_ms
                .map(|value| RuntimeValue::Uint(U256::from(value)))
                .unwrap_or(RuntimeValue::Null),
        ),
        ("first_observed_at", RuntimeValue::Uint(U256::from(first_observed.unix_ms()))),
        ("observed_at", RuntimeValue::Uint(U256::from(first_observed.unix_ms()))),
        ("confirmed_at", RuntimeValue::Uint(U256::from(confirmed.unix_ms()))),
        ("confirmation_depth", RuntimeValue::Uint(U256::from(confirmation_depth))),
    ]))
}

fn grouped_event_runtime_value(
    event: &Event,
    log: &Log,
    values: Vec<DynSolValue>,
) -> Result<RuntimeValue> {
    Ok(object([
        ("address", RuntimeValue::Address(log.address())),
        ("contract_address", RuntimeValue::Address(log.address())),
        (
            "log_index",
            log.log_index
                .map(|value| RuntimeValue::Uint(U256::from(value)))
                .unwrap_or(RuntimeValue::Null),
        ),
        ("event", RuntimeValue::String(event.name.clone())),
        ("event_name", RuntimeValue::String(event.name.clone())),
        ("args", RuntimeValue::Object(decoded_event_arguments(event, values)?)),
    ]))
}

fn decoded_event_arguments(
    event: &Event,
    values: Vec<DynSolValue>,
) -> Result<BTreeMap<String, RuntimeValue>> {
    let mut arguments = BTreeMap::new();
    for (index, (parameter, value)) in event.inputs.iter().zip(values).enumerate() {
        let name =
            if parameter.name.is_empty() { index.to_string() } else { parameter.name.clone() };
        arguments.insert(name, RuntimeValue::from_dyn_sol(&value)?);
    }
    Ok(arguments)
}

fn log_observation_metadata(
    log: &Log,
    first_observed: ObservationPoint,
    block_timestamp_ms: Option<u64>,
    confirmation_depth: u64,
) -> Result<ObservationMetadata, StepError> {
    Ok(ObservationMetadata {
        first_observed,
        transaction_hash: log
            .transaction_hash
            .ok_or_else(|| StepError::missing("matching log omitted transaction_hash"))?,
        block_hash: log
            .block_hash
            .ok_or_else(|| StepError::missing("matching log omitted block_hash"))?,
        block_number: log
            .block_number
            .ok_or_else(|| StepError::missing("matching log omitted block_number"))?,
        transaction_index: log.transaction_index,
        log_indices: log.log_index.into_iter().collect(),
        block_timestamp_ms,
        confirmation_depth,
    })
}

pub(crate) fn expression_address(
    value: &serde_yaml::Value,
    context: &RuntimeContext,
) -> Result<Address> {
    match eval_expression(value, context)?.coerce_dyn_sol(&DynSolType::Address)? {
        DynSolValue::Address(value) => Ok(value),
        _ => unreachable!("address coercion returned another type"),
    }
}

fn expression_hash(value: &serde_yaml::Value, context: &RuntimeContext) -> Result<B256> {
    match eval_expression(value, context)?.coerce_dyn_sol(&DynSolType::FixedBytes(32))? {
        DynSolValue::FixedBytes(value, 32) => Ok(value),
        _ => unreachable!("bytes32 coercion returned another type"),
    }
}

fn expression_u64(value: &serde_yaml::Value, context: &RuntimeContext) -> Result<u64> {
    match eval_expression(value, context)?.coerce_dyn_sol(&DynSolType::Uint(64))? {
        DynSolValue::Uint(value, 64) => {
            value.try_into().map_err(|_| eyre::eyre!("block number expression exceeds u64"))
        }
        _ => unreachable!("uint64 coercion returned another type"),
    }
}

pub(crate) fn sort_logs(logs: &mut [Log]) {
    logs.sort_by_key(|log| {
        (log.block_number.unwrap_or(u64::MAX), log.log_index.unwrap_or(u64::MAX))
    });
}

fn object<const N: usize>(values: [(&str, RuntimeValue); N]) -> RuntimeValue {
    RuntimeValue::Object(values.into_iter().map(|(key, value)| (key.to_string(), value)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Bytes, LogData};
    use alloy_provider::ProviderBuilder;
    use alloy_transport::mock::Asserter;
    use bench_core::SenderConfig;
    use std::sync::Mutex as StdMutex;

    fn mocked_provider(asserter: Asserter) -> DynProvider<AnyNetwork> {
        ProviderBuilder::new_with_network::<AnyNetwork>().connect_mocked_client(asserter).erased()
    }

    fn mock_submitter(provider: &DynProvider<AnyNetwork>) -> RpcSubmitter {
        RpcSubmitter::new(vec![provider.clone()], SenderConfig::default()).unwrap()
    }

    /// Stateful JSON-RPC chain mock. Unlike the FIFO [`Asserter`], responses
    /// are derived from mutable chain state, which is required now that log
    /// waits are served by a background poller shared across consumers whose
    /// request interleaving is not deterministic.
    #[derive(Default)]
    struct MockNode {
        head: u64,
        hashes: BTreeMap<u64, B256>,
        logs: Vec<Log>,
        /// Logs withheld from the first `reveal_hidden_after` `eth_getLogs`
        /// responses, emulating lagging RPC log indexing.
        hidden_logs: Vec<Log>,
        reveal_hidden_after: usize,
        block_number_calls: usize,
        get_logs_calls: usize,
        get_logs_ranges: Vec<(u64, u64)>,
        get_logs_topics: Vec<serde_json::Value>,
    }

    impl MockNode {
        fn with_head(head: u64) -> Self {
            Self { head, ..Default::default() }
        }

        fn hash(&self, number: u64) -> B256 {
            self.hashes
                .get(&number)
                .copied()
                .unwrap_or_else(|| B256::left_padding_from(&(number + 1).to_be_bytes()))
        }
    }

    fn parse_hex_quantity(value: Option<&serde_json::Value>) -> Option<u64> {
        let value = value?.as_str()?;
        u64::from_str_radix(value.trim_start_matches("0x"), 16).ok()
    }

    fn mock_block_json(number: u64, hash: B256) -> serde_json::Value {
        let mut block = block_json(hash);
        block["number"] = serde_json::json!(format!("0x{number:x}"));
        block
    }

    async fn handle_mock_rpc(
        axum::extract::State(node): axum::extract::State<Arc<StdMutex<MockNode>>>,
        axum::Json(request): axum::Json<serde_json::Value>,
    ) -> axum::Json<serde_json::Value> {
        let id = request.get("id").cloned().unwrap_or(serde_json::Value::Null);
        let method = request.get("method").and_then(serde_json::Value::as_str).unwrap_or_default();
        let params = request.get("params").cloned().unwrap_or_else(|| serde_json::json!([]));
        let mut node = node.lock().expect("mock node lock");
        let result = match method {
            "eth_blockNumber" => {
                node.block_number_calls += 1;
                serde_json::json!(format!("0x{:x}", node.head))
            }
            "eth_getBlockByNumber" => {
                let number = parse_hex_quantity(params.get(0)).unwrap_or(node.head);
                mock_block_json(number, node.hash(number))
            }
            "eth_getLogs" => {
                node.get_logs_calls += 1;
                let filter = params.get(0).cloned().unwrap_or_default();
                let from = parse_hex_quantity(filter.get("fromBlock")).unwrap_or(0);
                let to = parse_hex_quantity(filter.get("toBlock")).unwrap_or(u64::MAX);
                node.get_logs_ranges.push((from, to));
                node.get_logs_topics
                    .push(filter.get("topics").cloned().unwrap_or(serde_json::Value::Null));
                let include_hidden = node.get_logs_calls > node.reveal_hidden_after;
                let logs: Vec<&Log> = node
                    .logs
                    .iter()
                    .chain(node.hidden_logs.iter().filter(|_| include_hidden))
                    .filter(|log| {
                        log.block_number.is_some_and(|number| number >= from && number <= to)
                    })
                    .collect();
                serde_json::to_value(&logs).expect("serialize mock logs")
            }
            _ => {
                return axum::Json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": { "code": -32601, "message": format!("unsupported method {method}") }
                }));
            }
        };
        axum::Json(serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": result }))
    }

    async fn spawn_mock_node(
        node: Arc<StdMutex<MockNode>>,
    ) -> (DynProvider<AnyNetwork>, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock RPC");
        let address = listener.local_addr().expect("mock RPC address");
        let app =
            axum::Router::new().route("/", axum::routing::post(handle_mock_rpc)).with_state(node);
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve mock RPC");
        });
        let url: url::Url = format!("http://{address}").parse().expect("mock RPC URL");
        let provider = ProviderBuilder::new_with_network::<AnyNetwork>().connect_http(url).erased();
        (provider, server)
    }

    fn wait_log_step(
        event: &str,
        from_block: u64,
        poll_interval: Duration,
        confirmations: u64,
        max_block_range: u64,
    ) -> WaitLogStep {
        WaitLogStep {
            chain: "chain_a".to_string(),
            from_block: Some(serde_yaml::Value::Number(serde_yaml::Number::from(from_block))),
            address: None,
            transaction_hash: None,
            sender: None,
            abi: "events".to_string(),
            event: event.to_string(),
            where_value: BTreeMap::new(),
            events: BTreeMap::new(),
            poll_interval: Some(poll_interval),
            confirmations: Some(confirmations),
            max_block_range: Some(max_block_range),
        }
    }

    fn event_abi() -> JsonAbi {
        serde_json::from_str(
            r#"[{"type":"event","name":"Moved","anonymous":false,"inputs":[{"name":"from","type":"address","indexed":true},{"name":"amount","type":"uint256","indexed":false}]}]"#,
        )
        .unwrap()
    }

    fn encoded_log() -> Log {
        encoded_log_with_amount(7, 2)
    }

    fn encoded_log_with_amount(amount: u64, log_index: u64) -> Log {
        let event = resolve_event(&event_abi(), "Moved").unwrap().clone();
        let from = Address::repeat_byte(0x11);
        Log {
            inner: alloy_primitives::Log {
                address: Address::repeat_byte(0x22),
                data: LogData::new_unchecked(
                    vec![event.selector(), B256::left_padding_from(from.as_slice())],
                    Bytes::from(DynSolValue::Uint(U256::from(amount), 256).abi_encode()),
                ),
            },
            block_hash: Some(B256::repeat_byte(0x33)),
            block_number: Some(4),
            transaction_hash: Some(B256::repeat_byte(0x44)),
            log_index: Some(log_index),
            ..Default::default()
        }
    }

    fn block_json(hash: B256) -> serde_json::Value {
        serde_json::json!({
            "hash": hash,
            "parentHash": B256::ZERO,
            "sha3Uncles": B256::ZERO,
            "miner": Address::ZERO,
            "stateRoot": B256::ZERO,
            "transactionsRoot": B256::ZERO,
            "receiptsRoot": B256::ZERO,
            "logsBloom": format!("0x{}", "00".repeat(256)),
            "difficulty": "0x0",
            "number": "0x4",
            "gasLimit": "0x0",
            "gasUsed": "0x0",
            "timestamp": "0x64",
            "extraData": "0x",
            "transactions": [],
            "timestampMillis": "0x18704"
        })
    }

    fn typed_block(
        timestamp_millis: Option<&str>,
        timestamp_millis_part: Option<&str>,
    ) -> AnyRpcBlock {
        let mut block = block_json(B256::ZERO);
        let object = block.as_object_mut().unwrap();
        match timestamp_millis {
            Some(value) => object.insert("timestampMillis".to_string(), value.into()),
            None => object.remove("timestampMillis"),
        };
        if let Some(value) = timestamp_millis_part {
            object.insert("timestampMillisPart".to_string(), value.into());
        }
        serde_json::from_value(block).unwrap()
    }

    fn receipt_json(
        transaction_hash: B256,
        block_hash: B256,
        status: bool,
        logs: Vec<Log>,
    ) -> serde_json::Value {
        serde_json::json!({
            "status": if status { "0x1" } else { "0x0" },
            "cumulativeGasUsed": "0x5208",
            "logs": logs,
            "logsBloom": format!("0x{}", "00".repeat(256)),
            "type": "0x2",
            "transactionHash": transaction_hash,
            "transactionIndex": "0x0",
            "blockHash": block_hash,
            "blockNumber": "0x4",
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x1",
            "from": Address::repeat_byte(0x11),
            "to": Address::repeat_byte(0x22),
            "contractAddress": null
        })
    }

    #[test]
    fn decodes_indexed_and_unindexed_arguments_and_filters() {
        let abi = event_abi();
        let filters = BTreeMap::from([(
            "amount".to_string(),
            serde_yaml::Value::Number(serde_yaml::Number::from(7)),
        )]);
        let matcher = EventMatcher::new(&abi, "Moved", &filters, &RuntimeContext::empty()).unwrap();
        let values = matcher.decode_if_matches(&encoded_log()).unwrap().unwrap();
        assert_eq!(values[0], DynSolValue::Address(Address::repeat_byte(0x11)));
        assert_eq!(values[1], DynSolValue::Uint(U256::from(7), 256));
    }

    #[test]
    fn rejects_nonmatching_filter_and_ambiguous_event_name() {
        let mut abi = event_abi();
        let filters = BTreeMap::from([(
            "amount".to_string(),
            serde_yaml::Value::Number(serde_yaml::Number::from(8)),
        )]);
        let matcher = EventMatcher::new(&abi, "Moved", &filters, &RuntimeContext::empty()).unwrap();
        assert!(matcher.decode_if_matches(&encoded_log()).unwrap().is_none());

        let duplicate = resolve_event(&abi, "Moved").unwrap().clone();
        abi.events.entry("Moved".into()).or_default().push(duplicate);
        assert!(resolve_event(&abi, "Moved").is_err());
        assert!(resolve_event(&abi, "Moved(address,uint256)").is_ok());
    }

    #[test]
    fn hashes_indexed_dynamic_event_filters() {
        let abi: JsonAbi = serde_json::from_str(
            r#"[{"type":"event","name":"Tagged","anonymous":false,"inputs":[{"name":"tag","type":"string","indexed":true}]}]"#,
        )
        .unwrap();
        let event = resolve_event(&abi, "Tagged").unwrap();
        let tag_hash = keccak256("hello");
        let log = Log {
            inner: alloy_primitives::Log {
                address: Address::repeat_byte(0x22),
                data: LogData::new_unchecked(vec![event.selector(), tag_hash], Bytes::new()),
            },
            ..Default::default()
        };
        let filters =
            BTreeMap::from([("tag".to_string(), serde_yaml::Value::String("hello".to_string()))]);
        let matcher =
            EventMatcher::new(&abi, "Tagged", &filters, &RuntimeContext::empty()).unwrap();

        let decoded = matcher.decode_if_matches(&log).unwrap().unwrap();
        assert_eq!(decoded, vec![DynSolValue::FixedBytes(tag_hash, 32)]);
    }

    #[test]
    fn validates_constant_event_filters_before_execution() {
        let address_filter =
            EventFilterType { sol_type: DynSolType::Address, accepts_precomputed_hash: false };
        let number = serde_yaml::Value::Number(serde_yaml::Number::from(7));
        assert!(validate_constant_event_filter(&number, &address_filter).is_err());

        let indexed_string =
            EventFilterType { sol_type: DynSolType::String, accepts_precomputed_hash: true };
        let precomputed: serde_yaml::Value = serde_yaml::from_str("{ keccak256: hello }").unwrap();
        validate_constant_event_filter(&precomputed, &indexed_string).unwrap();
    }

    #[test]
    fn resolves_tuple_event_parameter_components() {
        let abi: JsonAbi = serde_json::from_str(
            r#"[{"type":"event","name":"Structured","anonymous":false,"inputs":[{"name":"payload","type":"tuple","indexed":false,"components":[{"name":"who","type":"address"},{"name":"amount","type":"uint256"}]}]}]"#,
        )
        .unwrap();
        let event = resolve_event(&abi, "Structured").unwrap();
        let payload = DynSolValue::Tuple(vec![
            DynSolValue::Address(Address::repeat_byte(0x11)),
            DynSolValue::Uint(U256::from(7), 256),
        ]);
        let log = Log {
            inner: alloy_primitives::Log {
                address: Address::repeat_byte(0x22),
                data: LogData::new_unchecked(
                    vec![event.selector()],
                    Bytes::from(DynSolValue::Tuple(vec![payload.clone()]).abi_encode_params()),
                ),
            },
            ..Default::default()
        };
        let matcher =
            EventMatcher::new(&abi, "Structured", &BTreeMap::new(), &RuntimeContext::empty())
                .unwrap();
        assert_eq!(matcher.decode_if_matches(&log).unwrap().unwrap(), vec![payload]);
    }

    #[test]
    fn log_save_contains_typed_arguments() {
        let abi = event_abi();
        let event = resolve_event(&abi, "Moved").unwrap();
        let matcher =
            EventMatcher::new(&abi, "Moved", &BTreeMap::new(), &RuntimeContext::empty()).unwrap();
        let log = encoded_log();
        let values = matcher.decode_if_matches(&log).unwrap().unwrap();
        let observed = ObservationPoint::now();
        let saved =
            log_runtime_value("chain_a", event, &log, values, observed, observed, None, 0).unwrap();
        let RuntimeValue::Object(saved) = saved else { panic!("expected object") };
        let RuntimeValue::Object(args) = &saved["args"] else { panic!("expected args") };
        assert_eq!(args["amount"], RuntimeValue::Uint(U256::from(7)));
        assert_eq!(args["from"], RuntimeValue::Address(Address::repeat_byte(0x11)));
    }

    #[test]
    fn sorts_by_block_and_log_index() {
        let mut logs = vec![
            Log { block_number: Some(2), log_index: Some(0), ..Default::default() },
            Log { block_number: Some(1), log_index: Some(3), ..Default::default() },
            Log { block_number: Some(1), log_index: Some(1), ..Default::default() },
        ];
        sort_logs(&mut logs);
        assert_eq!(
            logs.iter().map(|log| (log.block_number, log.log_index)).collect::<Vec<_>>(),
            vec![(Some(1), Some(1)), (Some(1), Some(3)), (Some(2), Some(0))]
        );
    }

    #[test]
    fn canonical_recheck_covers_the_complete_requested_range() {
        let candidate_block = 110;
        let max_range = 10;
        let mut cursor = 0;
        let mut ranges = Vec::new();
        loop {
            let end = bounded_range_end(cursor, candidate_block, max_range);
            ranges.push((cursor, end));
            if end == candidate_block {
                break;
            }
            cursor = end + 1;
        }

        assert_eq!(ranges.first(), Some(&(0, 9)));
        assert_eq!(ranges.last(), Some(&(110, 110)));
    }

    #[test]
    fn canonical_timestamp_prefers_full_millis_and_supports_part_fallback() {
        assert_eq!(
            block_timestamp_ms(&typed_block(Some("0x1876b"), Some("0x1"))).unwrap(),
            Some(100_203)
        );
        assert_eq!(block_timestamp_ms(&typed_block(None, Some("0xcb"))).unwrap(), Some(100_203));
        assert_eq!(block_timestamp_ms(&typed_block(None, None)).unwrap(), Some(100_000));
    }

    #[tokio::test(start_paused = true)]
    async fn fifty_millisecond_receipt_polling_does_not_quantize_507ms_to_one_second() {
        let transaction_hash = B256::repeat_byte(0x44);
        let block_hash = B256::repeat_byte(0x33);
        let receipt = receipt_json(transaction_hash, block_hash, true, Vec::new());
        let asserter = Asserter::new();
        // Polls occur at 0, 50, ..., 500ms. Model an inclusion just after the
        // 500ms poll, then expose it to the next observation at 550ms.
        for _ in 0..=10 {
            asserter.push_success(&Option::<serde_json::Value>::None);
        }
        asserter.push_success(&receipt);
        asserter.push_success(&receipt);
        asserter.push_success(&block_json(block_hash));
        let provider = mocked_provider(asserter.clone());
        let submitter = mock_submitter(&provider);
        let started = tokio::time::Instant::now();
        wait_for_receipt(
            &provider,
            &submitter,
            "chain_a",
            None,
            transaction_hash,
            DEFAULT_POLL_INTERVAL,
            0,
        )
        .await
        .unwrap();
        let observed = tokio::time::Instant::now().duration_since(started);
        assert_eq!(observed, Duration::from_millis(550));
        assert!(observed < Duration::from_secs(1));
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn receipt_scoped_events_share_one_inclusion_observation() {
        let transaction_hash = B256::repeat_byte(0x44);
        let block_hash = B256::repeat_byte(0x33);
        let first = encoded_log();
        let mut second = encoded_log();
        second.log_index = Some(3);
        let receipt = receipt_json(transaction_hash, block_hash, true, vec![first, second]);
        let asserter = Asserter::new();
        asserter.push_success(&receipt);
        asserter.push_success(&receipt);
        asserter.push_success(&block_json(block_hash));
        let provider = mocked_provider(asserter.clone());
        let submitter = mock_submitter(&provider);
        let observer = ObservationRuntime::polling(provider, Duration::from_millis(1));
        let abi = event_abi();
        let events = vec![
            prepare_receipt_event(
                "processed",
                &abi,
                "Moved",
                None,
                &BTreeMap::new(),
                &RuntimeContext::empty(),
            )
            .unwrap(),
            prepare_receipt_event(
                "callback",
                &abi,
                "Moved",
                None,
                &BTreeMap::new(),
                &RuntimeContext::empty(),
            )
            .unwrap(),
        ];

        let result = wait_for_transaction_events(
            &observer,
            &submitter,
            "chain_a",
            None,
            transaction_hash,
            &events,
            0,
            SubscriptionBehavior::Disabled,
        )
        .await
        .unwrap();

        assert_eq!(result.observation.log_indices, vec![2, 3]);
        assert_eq!(result.observation.block_timestamp_ms, Some(100_100));
        let RuntimeValue::Object(value) = result.value else { panic!("expected object") };
        let RuntimeValue::Object(events) = &value["events"] else {
            panic!("expected grouped events")
        };
        assert!(events.contains_key("processed"));
        assert!(events.contains_key("callback"));
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn receipt_scoped_events_find_complete_assignment_for_overlapping_filters() {
        let transaction_hash = B256::repeat_byte(0x44);
        let block_hash = B256::repeat_byte(0x33);
        let receipt = receipt_json(
            transaction_hash,
            block_hash,
            true,
            vec![encoded_log_with_amount(7, 2), encoded_log_with_amount(8, 3)],
        );
        let asserter = Asserter::new();
        asserter.push_success(&receipt);
        asserter.push_success(&receipt);
        asserter.push_success(&block_json(block_hash));
        let provider = mocked_provider(asserter.clone());
        let submitter = mock_submitter(&provider);
        let observer = ObservationRuntime::polling(provider, Duration::from_millis(1));
        let abi = event_abi();
        let events = vec![
            prepare_receipt_event(
                "any",
                &abi,
                "Moved",
                None,
                &BTreeMap::new(),
                &RuntimeContext::empty(),
            )
            .unwrap(),
            prepare_receipt_event(
                "specific",
                &abi,
                "Moved",
                None,
                &BTreeMap::from([(
                    "amount".to_string(),
                    serde_yaml::Value::Number(serde_yaml::Number::from(7)),
                )]),
                &RuntimeContext::empty(),
            )
            .unwrap(),
        ];

        let result = wait_for_transaction_events(
            &observer,
            &submitter,
            "chain_a",
            None,
            transaction_hash,
            &events,
            0,
            SubscriptionBehavior::Disabled,
        )
        .await
        .unwrap();

        assert_eq!(result.observation.log_indices, vec![3, 2]);
        let RuntimeValue::Object(value) = result.value else { panic!("expected object") };
        let RuntimeValue::Object(events) = &value["events"] else {
            panic!("expected grouped events")
        };
        let RuntimeValue::Object(any) = &events["any"] else { panic!("expected any event") };
        let RuntimeValue::Object(any_args) = &any["args"] else { panic!("expected any args") };
        assert_eq!(any_args["amount"], RuntimeValue::Uint(U256::from(8)));
        let RuntimeValue::Object(specific) = &events["specific"] else {
            panic!("expected specific event")
        };
        let RuntimeValue::Object(specific_args) = &specific["args"] else {
            panic!("expected specific args")
        };
        assert_eq!(specific_args["amount"], RuntimeValue::Uint(U256::from(7)));
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn wait_log_backfills_before_polling() {
        let log = encoded_log();
        let node = Arc::new(StdMutex::new(MockNode::with_head(10)));
        {
            let mut node = node.lock().unwrap();
            node.hashes.insert(4, log.block_hash.unwrap());
            node.logs.push(log);
        }
        let (provider, server) = spawn_mock_node(node.clone()).await;
        let submitter = mock_submitter(&provider);
        let step = wait_log_step("Moved", 4, Duration::from_millis(5), 0, 100);

        let saved = tokio::time::timeout(
            Duration::from_secs(10),
            wait_for_log(
                &provider,
                &submitter,
                "chain_a",
                &event_abi(),
                &step,
                &RuntimeContext::empty(),
            ),
        )
        .await
        .expect("wait_log timed out")
        .unwrap();
        let RuntimeValue::Object(saved) = saved else { panic!("expected object") };
        assert_eq!(saved["block_number"], RuntimeValue::Uint(U256::from(4)));
        // The shared window scan is clamped to the requested start block.
        assert_eq!(node.lock().unwrap().get_logs_ranges.first(), Some(&(4, 10)));
        server.abort();
    }

    #[tokio::test]
    async fn wait_log_recheck_preserves_first_match_across_a_reorg() {
        let mut later = encoded_log();
        later.block_number = Some(10);
        later.log_index = Some(0);
        let mut earlier = encoded_log();
        earlier.block_number = Some(5);
        earlier.log_index = Some(3);
        earlier.block_hash = Some(B256::repeat_byte(0x55));

        let node = Arc::new(StdMutex::new(MockNode::with_head(10)));
        {
            let mut node = node.lock().unwrap();
            node.hashes.insert(10, later.block_hash.unwrap());
            node.hashes.insert(5, earlier.block_hash.unwrap());
            node.logs.push(later);
            // Withhold the earlier log from the first query so the canonical
            // recheck of the later candidate is what discovers it.
            node.hidden_logs.push(earlier);
            node.reveal_hidden_after = 1;
        }
        let (provider, server) = spawn_mock_node(node).await;
        let submitter = mock_submitter(&provider);
        let step = wait_log_step("Moved", 0, Duration::from_millis(5), 0, 100);

        let saved = tokio::time::timeout(
            Duration::from_secs(10),
            wait_for_log(
                &provider,
                &submitter,
                "chain_a",
                &event_abi(),
                &step,
                &RuntimeContext::empty(),
            ),
        )
        .await
        .expect("wait_log timed out")
        .unwrap();
        let RuntimeValue::Object(saved) = saved else { panic!("expected object") };
        assert_eq!(saved["block_number"], RuntimeValue::Uint(U256::from(5)));
        server.abort();
    }

    #[tokio::test]
    async fn wait_log_records_first_observation_before_confirmation_wait() {
        let log = encoded_log();
        let node = Arc::new(StdMutex::new(MockNode::with_head(4)));
        {
            let mut node = node.lock().unwrap();
            node.hashes.insert(4, log.block_hash.unwrap());
            node.logs.push(log);
        }
        let (provider, server) = spawn_mock_node(node.clone()).await;
        let submitter = mock_submitter(&provider);
        let observation = ObservationRuntime::polling(provider, Duration::from_millis(5));
        let step = wait_log_step("Moved", 4, Duration::from_millis(5), 1, 100);

        // Hold the head at the inclusion block long enough for the log to be
        // observed, then release the confirmation block.
        let advanced_at = Arc::new(StdMutex::new(None::<Instant>));
        let advance = tokio::spawn({
            let node = node.clone();
            let advanced_at = advanced_at.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                *advanced_at.lock().unwrap() = Some(Instant::now());
                node.lock().unwrap().head = 5;
            }
        });

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            wait_for_log_observed(
                &observation,
                &submitter,
                "chain_a",
                &event_abi(),
                &step,
                &RuntimeContext::empty(),
                SubscriptionBehavior::Disabled,
            ),
        )
        .await
        .expect("wait_log timed out")
        .unwrap();
        advance.await.unwrap();

        assert_eq!(result.observation.confirmation_depth, 1);
        let advanced_at = advanced_at.lock().unwrap().expect("head was advanced");
        assert!(
            result.observation.first_observed.monotonic < advanced_at,
            "first observation must precede the confirmation wait"
        );
        server.abort();
    }

    #[tokio::test]
    async fn wait_log_rescans_recent_blocks_after_reorg() {
        let log = encoded_log();
        let node = Arc::new(StdMutex::new(MockNode::with_head(4)));
        node.lock().unwrap().hashes.insert(4, B256::repeat_byte(0x66));
        let (provider, server) = spawn_mock_node(node.clone()).await;
        let submitter = mock_submitter(&provider);
        let step = wait_log_step("Moved", 4, Duration::from_millis(5), 0, 100);

        // Let the empty pre-reorg chain be scanned first, then replace the
        // head block with one that contains the event.
        let reorg = tokio::spawn({
            let node = node.clone();
            let log = log.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let mut node = node.lock().unwrap();
                node.hashes.insert(4, log.block_hash.unwrap());
                node.logs.push(log);
            }
        });

        let saved = tokio::time::timeout(
            Duration::from_secs(10),
            wait_for_log(
                &provider,
                &submitter,
                "chain_a",
                &event_abi(),
                &step,
                &RuntimeContext::empty(),
            ),
        )
        .await
        .expect("wait_log timed out")
        .unwrap();
        reorg.await.unwrap();
        let RuntimeValue::Object(saved) = saved else { panic!("expected object") };
        assert_eq!(saved["block_number"], RuntimeValue::Uint(U256::from(4)));
        assert!(node.lock().unwrap().get_logs_calls >= 2, "reorg must trigger a rescan");
        server.abort();
    }

    #[tokio::test]
    async fn wait_log_deep_reorg_resets_before_the_recent_window() {
        let mut log = encoded_log();
        log.block_number = Some(10);
        log.block_hash = Some(B256::repeat_byte(0x77));
        let node = Arc::new(StdMutex::new(MockNode::with_head(100)));
        node.lock().unwrap().hashes.insert(100, B256::repeat_byte(0x66));
        let (provider, server) = spawn_mock_node(node.clone()).await;
        let submitter = mock_submitter(&provider);
        let step = wait_log_step("Moved", 0, Duration::from_millis(5), 0, 200);

        // After the empty chain is scanned end to end, reorg deep history: a
        // new head hash plus an event well below the recent rescan window.
        let reorg = tokio::spawn({
            let node = node.clone();
            let log = log.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(60)).await;
                let mut node = node.lock().unwrap();
                node.hashes.insert(100, B256::repeat_byte(0x88));
                node.hashes.insert(10, log.block_hash.unwrap());
                node.logs.push(log);
            }
        });

        let saved = tokio::time::timeout(
            Duration::from_secs(10),
            wait_for_log(
                &provider,
                &submitter,
                "chain_a",
                &event_abi(),
                &step,
                &RuntimeContext::empty(),
            ),
        )
        .await
        .expect("wait_log timed out")
        .unwrap();
        reorg.await.unwrap();
        let RuntimeValue::Object(saved) = saved else { panic!("expected object") };
        assert_eq!(saved["block_number"], RuntimeValue::Uint(U256::from(10)));
        // The pre-window history was rescanned from the requested start.
        let ranges = node.lock().unwrap().get_logs_ranges.clone();
        assert!(
            ranges.iter().filter(|(from, to)| *from == 0 && *to == 36).count() >= 2,
            "history below the shared window must be rescanned after the reorg: {ranges:?}"
        );
        server.abort();
    }

    #[tokio::test]
    async fn wait_log_retries_recent_blocks_when_rpc_indexing_lags() {
        let log = encoded_log();
        let node = Arc::new(StdMutex::new(MockNode::with_head(4)));
        {
            let mut node = node.lock().unwrap();
            node.hashes.insert(4, log.block_hash.unwrap());
            // The chain never reorgs, but the RPC only starts returning the
            // log from the third query onward.
            node.hidden_logs.push(log);
            node.reveal_hidden_after = 2;
        }
        let (provider, server) = spawn_mock_node(node.clone()).await;
        let submitter = mock_submitter(&provider);
        let step = wait_log_step("Moved", 4, Duration::from_millis(5), 0, 100);

        let saved = tokio::time::timeout(
            Duration::from_secs(10),
            wait_for_log(
                &provider,
                &submitter,
                "chain_a",
                &event_abi(),
                &step,
                &RuntimeContext::empty(),
            ),
        )
        .await
        .expect("wait_log timed out")
        .unwrap();
        let RuntimeValue::Object(saved) = saved else { panic!("expected object") };
        assert_eq!(saved["block_number"], RuntimeValue::Uint(U256::from(4)));
        assert!(node.lock().unwrap().get_logs_calls >= 3, "recent blocks must be retried");
        server.abort();
    }

    #[tokio::test]
    async fn concurrent_wait_logs_share_a_single_poller() {
        let poll_interval = Duration::from_millis(10);
        let node = Arc::new(StdMutex::new(MockNode::with_head(100)));
        node.lock().unwrap().hashes.insert(95, B256::repeat_byte(0x33));
        let (provider, server) = spawn_mock_node(node.clone()).await;
        let submitter = mock_submitter(&provider);
        let observation = ObservationRuntime::polling(provider, poll_interval);
        let started = Instant::now();

        let waiters: Vec<_> = (0..8)
            .map(|_| {
                let observation = observation.clone();
                let submitter = submitter.clone();
                tokio::spawn(async move {
                    let step = wait_log_step("Moved", 90, poll_interval, 0, 1_000);
                    wait_for_log_observed(
                        &observation,
                        &submitter,
                        "chain_a",
                        &event_abi(),
                        &step,
                        &RuntimeContext::empty(),
                        SubscriptionBehavior::Disabled,
                    )
                    .await
                })
            })
            .collect();

        tokio::time::sleep(Duration::from_millis(100)).await;
        {
            let mut node = node.lock().unwrap();
            let mut log = encoded_log();
            log.block_number = Some(95);
            node.logs.push(log);
        }
        for waiter in waiters {
            let result = tokio::time::timeout(Duration::from_secs(10), waiter)
                .await
                .expect("wait_log timed out")
                .unwrap()
                .unwrap();
            assert_eq!(result.observation.block_number, 95);
        }

        // All eight waiters must be served by one poller: the total request
        // volume tracks elapsed polling ticks, not the number of consumers.
        let elapsed_ticks = started.elapsed().as_millis() / poll_interval.as_millis();
        let node = node.lock().unwrap();
        let allowed_polls = elapsed_ticks + 20;
        assert!(
            (node.block_number_calls as u128) <= allowed_polls,
            "{} eth_blockNumber calls exceed one poller's budget of {allowed_polls}",
            node.block_number_calls
        );
        // One window fetch per tick plus one canonical recheck per consumer.
        let allowed_log_queries = elapsed_ticks + 8 + 20;
        assert!(
            (node.get_logs_calls as u128) <= allowed_log_queries,
            "{} eth_getLogs calls exceed one poller's budget of {allowed_log_queries}",
            node.get_logs_calls
        );
        server.abort();
    }

    #[tokio::test]
    async fn faster_waiter_wakes_poller_sleeping_on_a_long_interval() {
        let long_interval = Duration::from_secs(30);
        let node = Arc::new(StdMutex::new(MockNode::with_head(10)));
        let (provider, server) = spawn_mock_node(node.clone()).await;
        let submitter = mock_submitter(&provider);
        let observation = ObservationRuntime::polling(provider, long_interval);

        let long_wait = tokio::spawn({
            let observation = observation.clone();
            let submitter = submitter.clone();
            async move {
                let abi = event_abi();
                let step = wait_log_step("Moved", 0, long_interval, 0, 100);
                wait_for_log_observed(
                    &observation,
                    &submitter,
                    "chain_a",
                    &abi,
                    &step,
                    &RuntimeContext::empty(),
                    SubscriptionBehavior::Disabled,
                )
                .await
            }
        });

        // Wait until the first consumer's initial scan has started. Its hub
        // will otherwise sleep for 30 seconds before rebuilding the plan.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if node.lock().unwrap().get_logs_calls > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("initial shared scan did not start");

        let jumped = jumped_log(6);
        {
            let mut node = node.lock().unwrap();
            node.hashes.insert(6, jumped.block_hash.unwrap());
            node.logs.push(jumped);
        }

        let short_interval = Duration::from_millis(5);
        let short_observation = observation.for_step(Some(short_interval));
        let short_step = wait_log_step("Jumped", 0, short_interval, 0, 100);
        let jumped_abi = jumped_abi();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            wait_for_log_observed(
                &short_observation,
                &submitter,
                "chain_a",
                &jumped_abi,
                &short_step,
                &RuntimeContext::empty(),
                SubscriptionBehavior::Disabled,
            ),
        )
        .await
        .expect("new interest did not wake the shared poller")
        .unwrap();

        assert_eq!(result.observation.block_number, 6);
        long_wait.abort();
        server.abort();
    }

    fn jumped_abi() -> JsonAbi {
        serde_json::from_str(
            r#"[{"type":"event","name":"Jumped","anonymous":false,"inputs":[{"name":"height","type":"uint256","indexed":false}]}]"#,
        )
        .unwrap()
    }

    fn jumped_log(block_number: u64) -> Log {
        let event = resolve_event(&jumped_abi(), "Jumped").unwrap().clone();
        Log {
            inner: alloy_primitives::Log {
                address: Address::repeat_byte(0x23),
                data: LogData::new_unchecked(
                    vec![event.selector()],
                    Bytes::from(DynSolValue::Uint(U256::from(9), 256).abi_encode()),
                ),
            },
            block_hash: Some(B256::repeat_byte(0x34)),
            block_number: Some(block_number),
            transaction_hash: Some(B256::repeat_byte(0x45)),
            log_index: Some(0),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn heterogeneous_waits_share_the_union_filter() {
        let poll_interval = Duration::from_millis(10);
        let moved = encoded_log();
        let jumped = jumped_log(6);
        let node = Arc::new(StdMutex::new(MockNode::with_head(10)));
        {
            let mut node = node.lock().unwrap();
            node.hashes.insert(4, moved.block_hash.unwrap());
            node.hashes.insert(6, jumped.block_hash.unwrap());
        }
        let (provider, server) = spawn_mock_node(node.clone()).await;
        let submitter = mock_submitter(&provider);
        let observation = ObservationRuntime::polling(provider, poll_interval);

        let mut waiters = Vec::new();
        for (abi, event) in [(event_abi(), "Moved"), (jumped_abi(), "Jumped")] {
            let observation = observation.clone();
            let submitter = submitter.clone();
            waiters.push(tokio::spawn(async move {
                let step = wait_log_step(event, 0, poll_interval, 0, 1_000);
                wait_for_log_observed(
                    &observation,
                    &submitter,
                    "chain_a",
                    &abi,
                    &step,
                    &RuntimeContext::empty(),
                    SubscriptionBehavior::Disabled,
                )
                .await
            }));
        }

        // Publish both events only after both waiters are subscribed so the
        // discovering scan must have carried the union of both signatures.
        tokio::time::sleep(Duration::from_millis(50)).await;
        {
            let mut node = node.lock().unwrap();
            node.logs.push(moved);
            node.logs.push(jumped);
        }
        let mut blocks = Vec::new();
        for waiter in waiters {
            let result = tokio::time::timeout(Duration::from_secs(10), waiter)
                .await
                .expect("wait_log timed out")
                .unwrap()
                .unwrap();
            blocks.push(result.observation.block_number);
        }
        assert_eq!(blocks, vec![4, 6]);

        let moved_selector =
            format!("{:?}", resolve_event(&event_abi(), "Moved").unwrap().selector());
        let jumped_selector =
            format!("{:?}", resolve_event(&jumped_abi(), "Jumped").unwrap().selector());
        let node = node.lock().unwrap();
        let unioned = node.get_logs_topics.iter().any(|topics| {
            let Some(topic0) = topics.get(0) else { return false };
            let rendered = topic0.to_string();
            rendered.contains(&moved_selector) && rendered.contains(&jumped_selector)
        });
        assert!(
            unioned,
            "some shared scan must carry both event signatures: {:?}",
            node.get_logs_topics
        );
        server.abort();
    }

    #[tokio::test]
    async fn receipt_wait_returns_revert_status_and_can_be_timed_out() {
        let transaction_hash = B256::repeat_byte(0x44);
        let block_hash = B256::repeat_byte(0x33);
        let receipt = receipt_json(transaction_hash, block_hash, false, Vec::new());
        let asserter = Asserter::new();
        asserter.push_success(&receipt);
        asserter.push_success(&receipt);
        asserter.push_success(&block_json(block_hash));
        let provider = mocked_provider(asserter.clone());
        let submitter = mock_submitter(&provider);
        let result = wait_for_receipt(
            &provider,
            &submitter,
            "chain_a",
            None,
            transaction_hash,
            Duration::from_millis(1),
            0,
        )
        .await
        .unwrap();
        assert!(!result.status);
        assert_eq!(result.observation.confirmation_depth, 0);
        assert_eq!(result.observation.block_timestamp_ms, Some(100_100));
        assert!(asserter.read_q().is_empty());

        let asserter = Asserter::new();
        for _ in 0..100 {
            asserter.push_success(&Option::<serde_json::Value>::None);
        }
        let provider = mocked_provider(asserter);
        let submitter = mock_submitter(&provider);
        assert!(tokio::time::timeout(
            Duration::from_millis(10),
            wait_for_receipt(
                &provider,
                &submitter,
                "chain_a",
                None,
                transaction_hash,
                Duration::from_millis(1),
                0,
            ),
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn receipt_canonicality_uses_the_query_block_hash() {
        let transaction_hash = B256::repeat_byte(0x44);
        let receipt: AnyTransactionReceipt = serde_json::from_value(serde_json::json!({
            "status": "0x1",
            "cumulativeGasUsed": "0x5208",
            "logs": [],
            "logsBloom": format!("0x{}", "00".repeat(256)),
            "type": "0x2",
            "transactionHash": transaction_hash,
            "transactionIndex": "0x0",
            "blockHash": B256::repeat_byte(0x33),
            "blockNumber": "0x4",
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x1",
            "from": Address::repeat_byte(0x11),
            "to": Address::repeat_byte(0x22),
            "contractAddress": null
        }))
        .unwrap();
        let asserter = Asserter::new();
        asserter.push_success(&block_json(B256::repeat_byte(0x99)));
        let provider = mocked_provider(asserter);

        assert!(!receipt_is_canonical_on_query(&provider, &receipt).await.unwrap());
    }
}
