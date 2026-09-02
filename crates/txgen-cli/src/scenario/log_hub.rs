//! Shared per-chain log polling.
//!
//! Every active `wait_log` used to run its own polling loop against the query
//! RPC, so thousands of concurrent scenario instances multiplied identical
//! `eth_blockNumber`/`eth_getLogs` traffic until the endpoint rejected
//! requests. This module maintains at most one poller per chain: a single
//! scanner task fetches the recent-block window with the union of all
//! subscriber filters and publishes each committed scan; waiting steps consume
//! the shared snapshots and only issue their own RPC calls for pre-window
//! history and for canonical verification of a discovered candidate.

use super::{
    error::StepError,
    wait::{
        bounded_range_end, canonical_block_hash, sort_logs, ObservationPoint, SubscriptionBehavior,
        WakeStream,
    },
};
use alloy_network::AnyNetwork;
use alloy_primitives::{Address, B256};
use alloy_provider::{DynProvider, Provider};
use alloy_rpc_types_eth::{Filter, Log};
use futures::StreamExt;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::watch;

/// Number of most recent blocks re-fetched on every poll so reorged or
/// late-indexed logs near the head are observed without a historical rescan.
pub(crate) const RECENT_LOG_RESCAN_BLOCKS: u64 = 64;

/// A shared scan error affects every active waiter, so tolerate a short burst
/// of RPC failures before treating the scanner as permanently unavailable.
const SCAN_RETRY_ATTEMPTS: u32 = 5;
const SCAN_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const SCAN_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(2);

/// One `wait_log` consumer's standing interest in the shared scan.
#[derive(Debug, Clone)]
pub(crate) struct LogInterest {
    /// Event signature topic; `None` for anonymous events.
    pub topic0: Option<B256>,
    pub address: Option<Address>,
    pub start_block: u64,
    pub poll_interval: Duration,
    pub max_block_range: u64,
}

impl LogInterest {
    /// Whether a scanned log is a candidate for this interest. Argument
    /// filters and ABI decoding remain the consumer's responsibility.
    pub(crate) fn matches(&self, log: &Log) -> bool {
        if log.block_number.is_none_or(|number| number < self.start_block) {
            return false;
        }
        if self.address.is_some_and(|expected| log.address() != expected) {
            return false;
        }
        if let Some(expected) = &self.topic0 {
            return log.inner.data.topics().first() == Some(expected);
        }
        true
    }
}

/// One committed scan of the recent-block window.
#[derive(Debug, Clone)]
pub(crate) struct LogWindow {
    /// Bumped whenever previously scanned history may have changed (reorg).
    /// Consumers must rescan everything below `coverage_start` on a change.
    pub epoch: u64,
    pub head: u64,
    /// First block covered by `logs`. Exceeds `head` when every subscriber
    /// starts beyond the current head and nothing was fetched.
    pub coverage_start: u64,
    pub observed: ObservationPoint,
    pub logs: Arc<Vec<Log>>,
}

#[derive(Debug, Clone)]
struct WindowUpdate {
    /// Registry generation whose union filter produced this scan. Consumers
    /// ignore successful windows older than their own registration because
    /// the filter may not have included their topics yet.
    generation: u64,
    result: Result<LogWindow, String>,
}

struct HubState {
    next_id: u64,
    generation: u64,
    epoch: u64,
    running: bool,
    interests: BTreeMap<u64, LogInterest>,
}

struct TickPlan {
    generation: u64,
    interests: Vec<LogInterest>,
}

/// Shared scanner owning the only standing log poller for one chain RPC.
///
/// The scanner task is spawned lazily on first subscription and exits when
/// the last subscription is dropped, so idle chains issue no requests.
pub(crate) struct LogPollHub {
    provider: DynProvider<AnyNetwork>,
    websocket_provider: Option<DynProvider<AnyNetwork>>,
    subscription_behavior: SubscriptionBehavior,
    state: Mutex<HubState>,
    registry_updates: watch::Sender<u64>,
    updates: watch::Sender<Option<WindowUpdate>>,
}

impl LogPollHub {
    pub(crate) fn new(
        provider: DynProvider<AnyNetwork>,
        websocket_provider: Option<DynProvider<AnyNetwork>>,
        subscription_behavior: SubscriptionBehavior,
    ) -> Self {
        Self {
            provider,
            websocket_provider,
            subscription_behavior,
            state: Mutex::new(HubState {
                next_id: 0,
                generation: 0,
                epoch: 0,
                running: false,
                interests: BTreeMap::new(),
            }),
            registry_updates: watch::channel(0).0,
            updates: watch::channel(None).0,
        }
    }

    pub(crate) fn subscribe(self: &Arc<Self>, interest: LogInterest) -> LogWindowSubscription {
        let receiver = self.updates.subscribe();
        let mut state = self.state.lock().expect("log hub state lock");
        state.generation += 1;
        state.next_id += 1;
        let id = state.next_id;
        let generation = state.generation;
        state.interests.insert(id, interest.clone());
        let spawn = !state.running;
        state.running = true;
        drop(state);
        self.registry_updates.send_replace(generation);
        if spawn {
            tokio::spawn(Arc::clone(self).run());
        }
        LogWindowSubscription { hub: Arc::clone(self), id, generation, interest, receiver }
    }

    /// Snapshot the registry, or mark the scanner stopped and return `None`
    /// under the same lock so a concurrent subscribe observes the stop and
    /// respawns the task.
    fn tick_plan(&self) -> Option<TickPlan> {
        let mut state = self.state.lock().expect("log hub state lock");
        if state.interests.is_empty() {
            state.running = false;
            return None;
        }
        Some(TickPlan {
            generation: state.generation,
            interests: state.interests.values().cloned().collect(),
        })
    }

    fn bump_epoch(&self) {
        self.state.lock().expect("log hub state lock").epoch += 1;
    }

    fn epoch(&self) -> u64 {
        self.state.lock().expect("log hub state lock").epoch
    }

    async fn run(self: Arc<Self>) {
        let mut checkpoint = None::<(u64, B256)>;
        let mut consecutive_scan_failures = 0;
        let mut wake = None;
        let mut registry_updates = self.registry_updates.subscribe();
        let mut subscription_initialized =
            self.subscription_behavior == SubscriptionBehavior::Disabled;
        loop {
            let Some(plan) = self.tick_plan() else { return };
            let interval = plan
                .interests
                .iter()
                .map(|interest| interest.poll_interval)
                .min()
                .expect("tick plan is non-empty");
            if !subscription_initialized {
                match self.subscribe_wake().await {
                    Ok(stream) => {
                        wake = stream;
                        subscription_initialized = true;
                    }
                    Err(error) => {
                        let _ = self.updates.send(Some(WindowUpdate {
                            generation: plan.generation,
                            result: Err(error),
                        }));
                        self.wait_for_next_tick(&mut wake, &mut registry_updates, interval).await;
                        continue;
                    }
                }
            }
            let (update, next_tick) = match self.scan(&plan, &mut checkpoint).await {
                Ok(Some(window)) => {
                    consecutive_scan_failures = 0;
                    (Some(Ok(window)), interval)
                }
                Ok(None) => {
                    consecutive_scan_failures = 0;
                    (None, interval)
                }
                Err(error) => {
                    consecutive_scan_failures += 1;
                    if consecutive_scan_failures >= SCAN_RETRY_ATTEMPTS {
                        consecutive_scan_failures = 0;
                        (Some(Err(error)), interval)
                    } else {
                        let exponent = consecutive_scan_failures.saturating_sub(1);
                        let backoff = SCAN_RETRY_INITIAL_BACKOFF
                            .saturating_mul(2u32.saturating_pow(exponent))
                            .min(SCAN_RETRY_MAX_BACKOFF);
                        (None, backoff)
                    }
                }
            };
            if let Some(result) = update {
                let _ =
                    self.updates.send(Some(WindowUpdate { generation: plan.generation, result }));
            }
            self.wait_for_next_tick(&mut wake, &mut registry_updates, next_tick).await;
        }
    }

    async fn wait_for_next_tick(
        &self,
        wake: &mut Option<WakeStream>,
        registry_updates: &mut watch::Receiver<u64>,
        interval: Duration,
    ) {
        let Some(stream) = wake.as_mut() else {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = registry_updates.changed() => {}
            }
            return;
        };
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = registry_updates.changed() => {}
            notification = stream.next() => {
                if notification.is_none() {
                    *wake = None;
                }
            }
        }
    }

    async fn subscribe_wake(&self) -> Result<Option<WakeStream>, String> {
        if self.subscription_behavior == SubscriptionBehavior::Disabled {
            return Ok(None);
        }
        let Some(provider) = &self.websocket_provider else {
            return if self.subscription_behavior == SubscriptionBehavior::Require {
                Err("subscription observation mode has no connected WebSocket".to_string())
            } else {
                Ok(None)
            };
        };
        match provider.subscribe_blocks().await {
            Ok(subscription) => {
                Ok(Some(Box::pin(subscription.into_stream().map(|_| ())) as WakeStream))
            }
            Err(error) if self.subscription_behavior == SubscriptionBehavior::Require => {
                Err(error.to_string())
            }
            Err(_) => Ok(None),
        }
    }

    /// Fetch and publish one window scan. `Ok(None)` means the chain was
    /// unstable during the scan, so nothing trustworthy can be published; any
    /// invalidated history was already accounted for with an epoch bump.
    async fn scan(
        &self,
        plan: &TickPlan,
        checkpoint: &mut Option<(u64, B256)>,
    ) -> Result<Option<LogWindow>, String> {
        let provider = &self.provider;
        let head = provider.get_block_number().await.map_err(|error| error.to_string())?;
        let Some(head_hash) = block_hash(provider, head).await? else {
            return Ok(None);
        };
        if let Some((number, hash)) = *checkpoint {
            let intact = if number == head {
                head_hash == hash
            } else if number < head {
                block_hash(provider, number).await? == Some(hash)
            } else {
                false
            };
            if !intact {
                *checkpoint = None;
                self.bump_epoch();
            }
        }

        let min_start = plan
            .interests
            .iter()
            .map(|interest| interest.start_block)
            .min()
            .expect("tick plan is non-empty");
        let chunk = plan
            .interests
            .iter()
            .map(|interest| interest.max_block_range)
            .min()
            .expect("tick plan is non-empty")
            .max(1);
        let coverage_start = head.saturating_sub(RECENT_LOG_RESCAN_BLOCKS - 1).max(min_start);
        let mut logs = Vec::new();
        let mut cursor = coverage_start;
        while cursor <= head {
            let end = bounded_range_end(cursor, head, chunk);
            let filter = union_filter(&plan.interests).from_block(cursor).to_block(end);
            logs.extend(provider.get_logs(&filter).await.map_err(|error| error.to_string())?);
            cursor = end.saturating_add(1);
        }
        sort_logs(&mut logs);
        let observed = ObservationPoint::now();

        // Commit only if the head block is unchanged across the scan; the
        // hash chain then proves every scanned ancestor unchanged as well.
        if block_hash(provider, head).await? != Some(head_hash) {
            *checkpoint = None;
            self.bump_epoch();
            return Ok(None);
        }
        *checkpoint = Some((head, head_hash));
        Ok(Some(LogWindow {
            epoch: self.epoch(),
            head,
            coverage_start,
            observed,
            logs: Arc::new(logs),
        }))
    }
}

async fn block_hash(
    provider: &DynProvider<AnyNetwork>,
    number: u64,
) -> Result<Option<B256>, String> {
    canonical_block_hash(provider, number).await.map_err(|error| error.to_string())
}

/// The broadest filter still restrictive enough for every subscriber: any
/// heterogeneous dimension (an anonymous event, a missing address filter)
/// drops that constraint from the shared query.
fn union_filter(interests: &[LogInterest]) -> Filter {
    let mut filter = Filter::new();
    if interests.iter().all(|interest| interest.topic0.is_some()) {
        let topics: Vec<B256> = interests
            .iter()
            .filter_map(|interest| interest.topic0)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        filter = filter.event_signature(topics);
    }
    if interests.iter().all(|interest| interest.address.is_some()) {
        let addresses: Vec<Address> = interests
            .iter()
            .filter_map(|interest| interest.address)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        filter = filter.address(addresses);
    }
    filter
}

/// A registered consumer's live view of the shared scanner.
pub(crate) struct LogWindowSubscription {
    hub: Arc<LogPollHub>,
    id: u64,
    generation: u64,
    interest: LogInterest,
    receiver: watch::Receiver<Option<WindowUpdate>>,
}

impl LogWindowSubscription {
    pub(crate) fn interest(&self) -> &LogInterest {
        &self.interest
    }

    /// Next committed window whose union filter includes this subscriber.
    /// Scanner RPC failures propagate only after the shared retry budget is
    /// exhausted, preventing one transient error from failing every waiter.
    pub(crate) async fn next_window(&mut self) -> Result<LogWindow, StepError> {
        loop {
            self.receiver
                .changed()
                .await
                .map_err(|_| StepError::rpc("shared log poller stopped"))?;
            let update = self.receiver.borrow_and_update().clone();
            let Some(update) = update else { continue };
            match update.result {
                Err(message) => return Err(StepError::rpc(message)),
                Ok(window) if update.generation >= self.generation => return Ok(window),
                Ok(_) => continue,
            }
        }
    }

    /// Wait until the shared scanner has observed `target` or beyond.
    pub(crate) async fn wait_for_head(&mut self, target: u64) -> Result<u64, StepError> {
        loop {
            if let Some(update) = self.receiver.borrow_and_update().clone() {
                match update.result {
                    Err(message) => return Err(StepError::rpc(message)),
                    Ok(window) if window.head >= target => return Ok(window.head),
                    Ok(_) => {}
                }
            }
            self.receiver
                .changed()
                .await
                .map_err(|_| StepError::rpc("shared log poller stopped"))?;
        }
    }

    /// Most recently scanned head, if any successful scan happened yet.
    pub(crate) fn latest_head(&self) -> Option<u64> {
        self.receiver
            .borrow()
            .as_ref()
            .and_then(|update| update.result.as_ref().ok().map(|window| window.head))
    }
}

impl Drop for LogWindowSubscription {
    fn drop(&mut self) {
        let mut state = self.hub.state.lock().expect("log hub state lock");
        if state.interests.remove(&self.id).is_none() {
            return;
        }
        state.generation += 1;
        let generation = state.generation;
        drop(state);
        self.hub.registry_updates.send_replace(generation);
    }
}
