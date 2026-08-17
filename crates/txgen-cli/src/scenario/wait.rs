use super::{
    error::StepError,
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
    time::{Duration, Instant, SystemTime},
};

pub(crate) const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(50);
pub(crate) const DEFAULT_MAX_BLOCK_RANGE: u64 = 1_000;
const RECENT_LOG_RESCAN_BLOCKS: u64 = 64;

type WakeStream = Pin<Box<dyn Stream<Item = ()> + Send>>;

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
    default_subscription: SubscriptionBehavior,
}

impl ObservationRuntime {
    /// Construct a polling-only observer. Kept small so legacy callers and
    /// tests can use the accurate scenario default without a WebSocket.
    pub(crate) fn polling(
        query_provider: DynProvider<AnyNetwork>,
        poll_interval: Duration,
    ) -> Self {
        Self {
            query_provider,
            websocket_provider: None,
            poll_interval,
            default_subscription: SubscriptionBehavior::Disabled,
        }
    }

    pub(crate) async fn from_config(
        query_provider: DynProvider<AnyNetwork>,
        config: &ObservationDef,
        require_subscription: bool,
    ) -> Result<Self, StepError> {
        let behavior = SubscriptionBehavior::from(config.mode);
        let require_subscription =
            require_subscription || behavior == SubscriptionBehavior::Require;
        let mut runtime = Self::connect(
            query_provider,
            config.websocket_url.as_deref(),
            config.poll_interval,
            require_subscription,
        )
        .await?;
        runtime.default_subscription = behavior;
        Ok(runtime)
    }

    /// Connect an optional WebSocket observation endpoint.
    ///
    /// `require_subscription` is used for explicit subscription mode. Auto
    /// mode passes `false`, allowing a connection failure to fall back to
    /// canonical HTTP polling.
    pub(crate) async fn connect(
        query_provider: DynProvider<AnyNetwork>,
        websocket_url: Option<&str>,
        poll_interval: Duration,
        require_subscription: bool,
    ) -> Result<Self, StepError> {
        let websocket_provider = match websocket_url {
            Some(url) => {
                let connect = WsConnect::new(url);
                match ProviderBuilder::new_with_network::<AnyNetwork>().connect_ws(connect).await {
                    Ok(provider) => Some(provider.erased()),
                    Err(error) if require_subscription => {
                        let _ = error;
                        return Err(StepError::new(
                            "configuration_error",
                            "failed to connect configured observation WebSocket",
                        ));
                    }
                    Err(_) => None,
                }
            }
            None if require_subscription => {
                return Err(StepError::new(
                    "configuration_error",
                    "subscription observation mode requires a websocket_url",
                ));
            }
            None => None,
        };
        Ok(Self {
            query_provider,
            websocket_provider,
            poll_interval,
            default_subscription: if require_subscription {
                SubscriptionBehavior::Require
            } else {
                SubscriptionBehavior::Prefer
            },
        })
    }

    pub(crate) fn query_provider(&self) -> &DynProvider<AnyNetwork> {
        &self.query_provider
    }

    pub(crate) fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    pub(crate) fn subscription_behavior(
        &self,
        step_override: Option<ObservationMode>,
    ) -> SubscriptionBehavior {
        step_override.map(SubscriptionBehavior::from).unwrap_or(self.default_subscription)
    }

    pub(crate) fn for_step(
        &self,
        mode: Option<ObservationMode>,
        poll_interval: Option<Duration>,
    ) -> Self {
        let mut runtime = self.clone();
        runtime.default_subscription = self.subscription_behavior(mode);
        if let Some(poll_interval) = poll_interval {
            runtime.poll_interval = poll_interval;
        }
        runtime
    }

    pub(crate) fn has_subscription(&self) -> bool {
        self.websocket_provider.is_some()
    }

    pub(crate) fn default_subscription(&self) -> SubscriptionBehavior {
        self.default_subscription
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

    async fn subscribe_logs(
        &self,
        filter: &Filter,
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
        match provider.subscribe_logs(filter).await {
            Ok(subscription) => {
                Ok(Some(Box::pin(subscription.into_stream().map(|_| ())) as WakeStream))
            }
            Err(error) if require_subscription => Err(StepError::rpc(error)),
            Err(_) => Ok(None),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ObservationPoint {
    pub monotonic: Instant,
    pub wall: SystemTime,
}

impl ObservationPoint {
    fn now() -> Self {
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

async fn canonical_block_hash(
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

async fn wait_for_wake(wake: &mut Option<WakeStream>, poll_interval: Duration) {
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
    let mut cursor = start_block;
    let max_range = step.max_block_range.unwrap_or(DEFAULT_MAX_BLOCK_RANGE);
    let subscription_filter = matcher.subscription_filter(address);
    let mut wake = observation.subscribe_logs(&subscription_filter, subscription).await?;
    let mut scanned_checkpoint = None::<(u64, B256)>;
    'observe: loop {
        let head = observation.query_provider.get_block_number().await.map_err(StepError::rpc)?;
        let Some(head_hash_before) =
            canonical_block_hash(&observation.query_provider, head).await?
        else {
            wait_for_wake(&mut wake, poll_interval).await;
            continue;
        };
        if let Some((number, hash)) = scanned_checkpoint {
            let checkpoint_hash = if number == head {
                Some(head_hash_before)
            } else if number < head {
                canonical_block_hash(&observation.query_provider, number).await?
            } else {
                None
            };
            if checkpoint_hash != Some(hash) {
                cursor = start_block;
                scanned_checkpoint = None;
                continue;
            }
        }
        let scan_start = cursor;

        while cursor <= head {
            let end = bounded_range_end(cursor, head, max_range);
            let filter = matcher.rpc_filter(cursor, end, address);
            let mut logs =
                observation.query_provider.get_logs(&filter).await.map_err(StepError::rpc)?;
            sort_logs(&mut logs);
            let first_observed = ObservationPoint::now();

            for candidate in logs {
                if candidate.removed ||
                    transaction_hash
                        .is_some_and(|expected| candidate.transaction_hash != Some(expected))
                {
                    continue;
                }
                let Some(_) = matcher.decode_if_matches(&candidate).map_err(StepError::abi)? else {
                    continue;
                };

                let block_number = candidate
                    .block_number
                    .ok_or_else(|| StepError::missing("matching log omitted block_number"))?;
                // A stable checkpoint proves its ancestors unchanged, so only
                // re-query the increment since that checkpoint. If it moved,
                // discard this candidate and backfill from the requested start.
                let recheck_start = match scanned_checkpoint {
                    Some((number, hash)) => {
                        if canonical_block_hash(&observation.query_provider, number).await? !=
                            Some(hash)
                        {
                            cursor = start_block;
                            scanned_checkpoint = None;
                            continue 'observe;
                        }
                        scan_start
                    }
                    None => start_block,
                };
                if let Some((canonical, canonical_decoded)) = find_first_canonical_log(
                    &observation.query_provider,
                    &matcher,
                    address,
                    transaction_hash,
                    recheck_start,
                    block_number,
                    max_range,
                )
                .await?
                {
                    if let Some((number, hash)) = scanned_checkpoint &&
                        canonical_block_hash(&observation.query_provider, number).await? !=
                            Some(hash)
                    {
                        cursor = start_block;
                        scanned_checkpoint = None;
                        continue 'observe;
                    }
                    let candidate_first_observed = if same_log_identity(&candidate, &canonical) {
                        first_observed
                    } else {
                        // A reorg can replace the candidate between the initial
                        // scan and canonical backfill. Do not attribute the
                        // replacement to an observation that preceded it.
                        ObservationPoint::now()
                    };
                    let canonical_number = canonical
                        .block_number
                        .ok_or_else(|| StepError::missing("matching log omitted block_number"))?;
                    let canonical_hash = canonical
                        .block_hash
                        .ok_or_else(|| StepError::missing("matching log omitted block_hash"))?;
                    wait_for_confirmations(observation, canonical_number, confirmations, &mut wake)
                        .await?;
                    let Some(block) = canonical_block(
                        &observation.query_provider,
                        canonical_number,
                        canonical_hash,
                    )
                    .await?
                    else {
                        continue;
                    };
                    let Some(confirmation_depth) =
                        current_confirmation_depth(observation, canonical_number, confirmations)
                            .await?
                    else {
                        wait_for_wake(&mut wake, poll_interval).await;
                        cursor = start_block;
                        continue 'observe;
                    };
                    let confirmed = ObservationPoint::now();
                    return Ok(LogResult {
                        observation: log_observation_metadata(
                            &canonical,
                            candidate_first_observed,
                            block.timestamp_ms,
                            confirmation_depth,
                        )?,
                        value: log_runtime_value(
                            chain,
                            &matcher.event,
                            &canonical,
                            canonical_decoded,
                            candidate_first_observed,
                            confirmed,
                            block.timestamp_ms,
                            confirmation_depth,
                        )
                        .map_err(StepError::abi)?,
                    });
                }
            }

            cursor = end.saturating_add(1);
        }

        // Commit the incremental scan only if both its endpoint and the
        // previously scanned checkpoint stayed canonical for its duration.
        // A changed descendant hash proves that some ancestor changed, so the
        // next pass backfills the complete requested range. This catches deep
        // reorgs without issuing a historical eth_getLogs query every 50ms.
        let head_hash_after = canonical_block_hash(&observation.query_provider, head).await?;
        let checkpoint_still_canonical = match scanned_checkpoint {
            Some((number, hash)) if number == head => head_hash_after == Some(hash),
            Some((number, hash)) if number < head => {
                canonical_block_hash(&observation.query_provider, number).await? == Some(hash)
            }
            Some(_) => false,
            None => true,
        };
        if head_hash_after != Some(head_hash_before) || !checkpoint_still_canonical {
            cursor = start_block;
            scanned_checkpoint = None;
            continue;
        }
        scanned_checkpoint = Some((head, head_hash_before));
        cursor = head.saturating_sub(RECENT_LOG_RESCAN_BLOCKS.saturating_sub(1)).max(start_block);
        wait_for_wake(&mut wake, poll_interval).await;
    }
}

fn same_log_identity(left: &Log, right: &Log) -> bool {
    left.block_hash == right.block_hash &&
        left.block_number == right.block_number &&
        left.transaction_hash == right.transaction_hash &&
        left.transaction_index == right.transaction_index &&
        left.log_index == right.log_index
}

fn bounded_range_end(start: u64, through: u64, max_range: u64) -> u64 {
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
        if !canonical.status() {
            return Err(StepError::new(
                "reverted_receipt",
                format!(
                    "transaction {transaction_hash} reverted before emitting the expected event"
                ),
            ));
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
        if !canonical.status() {
            return Err(StepError::new(
                "reverted_receipt",
                format!(
                    "transaction {transaction_hash} reverted before emitting the expected events"
                ),
            ));
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

fn sort_logs(logs: &mut [Log]) {
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

    fn mocked_provider(asserter: Asserter) -> DynProvider<AnyNetwork> {
        ProviderBuilder::new_with_network::<AnyNetwork>().connect_mocked_client(asserter).erased()
    }

    fn mock_submitter(provider: &DynProvider<AnyNetwork>) -> RpcSubmitter {
        RpcSubmitter::new(vec![provider.clone()], SenderConfig::default()).unwrap()
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
    async fn receipt_scoped_log_reports_reverted_transaction_hash() {
        let transaction_hash = B256::repeat_byte(0x44);
        let block_hash = B256::repeat_byte(0x33);
        let receipt = receipt_json(transaction_hash, block_hash, false, Vec::new());
        let asserter = Asserter::new();
        asserter.push_success(&receipt);
        asserter.push_success(&receipt);
        let provider = mocked_provider(asserter.clone());
        let submitter = mock_submitter(&provider);
        let observer = ObservationRuntime::polling(provider, Duration::from_millis(1));
        let matcher =
            EventMatcher::new(&event_abi(), "Moved", &BTreeMap::new(), &RuntimeContext::empty())
                .unwrap();

        let result = wait_for_transaction_log(
            &observer,
            &submitter,
            TransactionLogWait {
                chain: "chain_a",
                sender: None,
                transaction_hash,
                address: None,
                matcher: &matcher,
                confirmations: 0,
                subscription: SubscriptionBehavior::Disabled,
            },
        )
        .await;
        let Err(error) = result else { panic!("expected reverted receipt error") };

        assert_eq!(error.classification, "reverted_receipt");
        assert!(error.sanitized_detail().unwrap().contains(&transaction_hash.to_string()));
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn grouped_receipt_events_report_reverted_transaction_hash() {
        let transaction_hash = B256::repeat_byte(0x44);
        let block_hash = B256::repeat_byte(0x33);
        let receipt = receipt_json(transaction_hash, block_hash, false, Vec::new());
        let asserter = Asserter::new();
        asserter.push_success(&receipt);
        asserter.push_success(&receipt);
        let provider = mocked_provider(asserter.clone());
        let submitter = mock_submitter(&provider);
        let observer = ObservationRuntime::polling(provider, Duration::from_millis(1));
        let event = prepare_receipt_event(
            "processed",
            &event_abi(),
            "Moved",
            None,
            &BTreeMap::new(),
            &RuntimeContext::empty(),
        )
        .unwrap();

        let result = wait_for_transaction_events(
            &observer,
            &submitter,
            "chain_a",
            None,
            transaction_hash,
            &[event],
            0,
            SubscriptionBehavior::Disabled,
        )
        .await;
        let Err(error) = result else { panic!("expected reverted receipt error") };

        assert_eq!(error.classification, "reverted_receipt");
        assert!(error.sanitized_detail().unwrap().contains(&transaction_hash.to_string()));
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
        let asserter = Asserter::new();
        let log = encoded_log();
        asserter.push_success(&10u64);
        asserter.push_success(&block_json(B256::repeat_byte(0x10)));
        asserter.push_success(&vec![log.clone()]);
        asserter.push_success(&block_json(log.block_hash.unwrap()));
        asserter.push_success(&vec![log.clone()]);
        asserter.push_success(&block_json(log.block_hash.unwrap()));
        asserter.push_success(&block_json(log.block_hash.unwrap()));
        let provider = mocked_provider(asserter.clone());
        let submitter = mock_submitter(&provider);
        let step = WaitLogStep {
            chain: "chain_a".to_string(),
            from_block: Some(serde_yaml::Value::Number(serde_yaml::Number::from(4))),
            address: None,
            transaction_hash: None,
            sender: None,
            abi: "events".to_string(),
            event: "Moved".to_string(),
            where_value: BTreeMap::new(),
            events: BTreeMap::new(),
            mode: None,
            poll_interval: Some(Duration::from_millis(1)),
            confirmations: Some(0),
            max_block_range: Some(100),
        };

        let saved = wait_for_log(
            &provider,
            &submitter,
            "chain_a",
            &event_abi(),
            &step,
            &RuntimeContext::empty(),
        )
        .await
        .unwrap();
        let RuntimeValue::Object(saved) = saved else { panic!("expected object") };
        assert_eq!(saved["block_number"], RuntimeValue::Uint(U256::from(4)));
        assert!(asserter.read_q().is_empty());
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

        let asserter = Asserter::new();
        asserter.push_success(&10u64);
        asserter.push_success(&block_json(later.block_hash.unwrap()));
        asserter.push_success(&vec![later.clone()]);
        asserter.push_success(&block_json(later.block_hash.unwrap()));
        asserter.push_success(&vec![later, earlier.clone()]);
        asserter.push_success(&block_json(B256::repeat_byte(0x33)));
        asserter.push_success(&block_json(earlier.block_hash.unwrap()));
        let provider = mocked_provider(asserter.clone());
        let submitter = mock_submitter(&provider);
        let step = WaitLogStep {
            chain: "chain_a".to_string(),
            from_block: Some(serde_yaml::Value::Number(serde_yaml::Number::from(0))),
            address: None,
            transaction_hash: None,
            sender: None,
            abi: "events".to_string(),
            event: "Moved".to_string(),
            where_value: BTreeMap::new(),
            events: BTreeMap::new(),
            mode: None,
            poll_interval: Some(Duration::from_millis(1)),
            confirmations: Some(0),
            max_block_range: Some(100),
        };

        let saved = wait_for_log(
            &provider,
            &submitter,
            "chain_a",
            &event_abi(),
            &step,
            &RuntimeContext::empty(),
        )
        .await
        .unwrap();
        let RuntimeValue::Object(saved) = saved else { panic!("expected object") };
        assert_eq!(saved["block_number"], RuntimeValue::Uint(U256::from(5)));
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn wait_log_records_first_observation_before_confirmation_wait() {
        let asserter = Asserter::new();
        let log = encoded_log();
        asserter.push_success(&4u64);
        asserter.push_success(&block_json(log.block_hash.unwrap()));
        asserter.push_success(&vec![log.clone()]);
        asserter.push_success(&block_json(log.block_hash.unwrap()));
        asserter.push_success(&vec![log.clone()]);
        asserter.push_success(&block_json(log.block_hash.unwrap()));
        asserter.push_success(&4u64);
        asserter.push_success(&5u64);
        asserter.push_success(&block_json(log.block_hash.unwrap()));
        asserter.push_success(&5u64);
        let provider = mocked_provider(asserter.clone());
        let submitter = mock_submitter(&provider);
        let observation = ObservationRuntime::polling(provider, Duration::from_millis(20));
        let step = WaitLogStep {
            chain: "chain_a".to_string(),
            from_block: Some(serde_yaml::Value::Number(serde_yaml::Number::from(4))),
            address: None,
            transaction_hash: None,
            sender: None,
            abi: "events".to_string(),
            event: "Moved".to_string(),
            where_value: BTreeMap::new(),
            events: BTreeMap::new(),
            mode: None,
            poll_interval: Some(Duration::from_millis(20)),
            confirmations: Some(1),
            max_block_range: Some(100),
        };

        let result = wait_for_log_observed(
            &observation,
            &submitter,
            "chain_a",
            &event_abi(),
            &step,
            &RuntimeContext::empty(),
            SubscriptionBehavior::Disabled,
        )
        .await
        .unwrap();

        assert_eq!(result.observation.confirmation_depth, 1);
        assert!(
            result.observation.first_observed.monotonic.elapsed() >= Duration::from_millis(20),
            "first observation must precede the confirmation wait"
        );
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn wait_log_rescans_recent_blocks_after_reorg() {
        let asserter = Asserter::new();
        let log = encoded_log();
        let old_head_hash = B256::repeat_byte(0x66);
        let new_head_hash = log.block_hash.unwrap();
        asserter.push_success(&4u64);
        asserter.push_success(&block_json(old_head_hash));
        asserter.push_success(&Vec::<Log>::new());
        asserter.push_success(&block_json(old_head_hash));
        asserter.push_success(&4u64);
        asserter.push_success(&block_json(new_head_hash));
        asserter.push_success(&4u64);
        asserter.push_success(&block_json(new_head_hash));
        asserter.push_success(&vec![log.clone()]);
        asserter.push_success(&block_json(new_head_hash));
        asserter.push_success(&vec![log.clone()]);
        asserter.push_success(&block_json(new_head_hash));
        asserter.push_success(&block_json(new_head_hash));
        let provider = mocked_provider(asserter.clone());
        let submitter = mock_submitter(&provider);
        let step = WaitLogStep {
            chain: "chain_a".to_string(),
            from_block: Some(serde_yaml::Value::Number(serde_yaml::Number::from(4))),
            address: None,
            transaction_hash: None,
            sender: None,
            abi: "events".to_string(),
            event: "Moved".to_string(),
            where_value: BTreeMap::new(),
            events: BTreeMap::new(),
            mode: None,
            poll_interval: Some(Duration::from_millis(1)),
            confirmations: Some(0),
            max_block_range: Some(100),
        };

        let saved = wait_for_log(
            &provider,
            &submitter,
            "chain_a",
            &event_abi(),
            &step,
            &RuntimeContext::empty(),
        )
        .await
        .unwrap();
        let RuntimeValue::Object(saved) = saved else { panic!("expected object") };
        assert_eq!(saved["block_number"], RuntimeValue::Uint(U256::from(4)));
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn wait_log_deep_reorg_resets_before_the_recent_window() {
        let asserter = Asserter::new();
        let mut log = encoded_log();
        log.block_number = Some(10);
        log.block_hash = Some(B256::repeat_byte(0x77));
        let old_head_hash = B256::repeat_byte(0x66);
        let new_head_hash = B256::repeat_byte(0x88);
        asserter.push_success(&100u64);
        asserter.push_success(&block_json(old_head_hash));
        asserter.push_success(&Vec::<Log>::new());
        asserter.push_success(&block_json(old_head_hash));
        asserter.push_success(&100u64);
        asserter.push_success(&block_json(new_head_hash));
        asserter.push_success(&100u64);
        asserter.push_success(&block_json(new_head_hash));
        asserter.push_success(&vec![log.clone()]);
        asserter.push_success(&block_json(log.block_hash.unwrap()));
        asserter.push_success(&vec![log.clone()]);
        asserter.push_success(&block_json(log.block_hash.unwrap()));
        asserter.push_success(&block_json(log.block_hash.unwrap()));
        let provider = mocked_provider(asserter.clone());
        let submitter = mock_submitter(&provider);
        let step = WaitLogStep {
            chain: "chain_a".to_string(),
            from_block: Some(serde_yaml::Value::Number(serde_yaml::Number::from(0))),
            address: None,
            transaction_hash: None,
            sender: None,
            abi: "events".to_string(),
            event: "Moved".to_string(),
            where_value: BTreeMap::new(),
            events: BTreeMap::new(),
            mode: None,
            poll_interval: Some(Duration::from_millis(1)),
            confirmations: Some(0),
            max_block_range: Some(200),
        };

        let saved = wait_for_log(
            &provider,
            &submitter,
            "chain_a",
            &event_abi(),
            &step,
            &RuntimeContext::empty(),
        )
        .await
        .unwrap();
        let RuntimeValue::Object(saved) = saved else { panic!("expected object") };
        assert_eq!(saved["block_number"], RuntimeValue::Uint(U256::from(10)));
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn wait_log_retries_recent_blocks_when_rpc_indexing_lags() {
        let asserter = Asserter::new();
        let log = encoded_log();
        let head_hash = log.block_hash.unwrap();
        asserter.push_success(&4u64);
        asserter.push_success(&block_json(head_hash));
        asserter.push_success(&Vec::<Log>::new());
        asserter.push_success(&block_json(head_hash));
        asserter.push_success(&4u64);
        asserter.push_success(&block_json(head_hash));
        asserter.push_success(&vec![log.clone()]);
        asserter.push_success(&block_json(head_hash));
        asserter.push_success(&block_json(head_hash));
        asserter.push_success(&vec![log.clone()]);
        asserter.push_success(&block_json(head_hash));
        asserter.push_success(&block_json(head_hash));
        asserter.push_success(&block_json(head_hash));
        let provider = mocked_provider(asserter.clone());
        let submitter = mock_submitter(&provider);
        let step = WaitLogStep {
            chain: "chain_a".to_string(),
            from_block: Some(serde_yaml::Value::Number(serde_yaml::Number::from(4))),
            address: None,
            transaction_hash: None,
            sender: None,
            abi: "events".to_string(),
            event: "Moved".to_string(),
            where_value: BTreeMap::new(),
            events: BTreeMap::new(),
            mode: None,
            poll_interval: Some(Duration::from_millis(1)),
            confirmations: Some(0),
            max_block_range: Some(100),
        };

        let saved = wait_for_log(
            &provider,
            &submitter,
            "chain_a",
            &event_abi(),
            &step,
            &RuntimeContext::empty(),
        )
        .await
        .unwrap();
        let RuntimeValue::Object(saved) = saved else { panic!("expected object") };
        assert_eq!(saved["block_number"], RuntimeValue::Uint(U256::from(4)));
        assert!(asserter.read_q().is_empty());
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
