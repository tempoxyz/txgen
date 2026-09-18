//! Warm-up orchestration for `bench send`.
//!
//! Polls the chain (and optionally the txpool) while the sender is running,
//! applies the send-rate ramp, and reports when the measurement origin should
//! move. The decision logic, defaults, and their derivations live in
//! [`bench_core::warmup`].

use alloy_consensus::BlockHeader;
use alloy_eips::BlockNumberOrTag;
use alloy_network::{primitives::BlockResponse, AnyNetwork, AnyRpcBlock};
use alloy_provider::{ext::TxPoolApi, DynProvider, Provider};
use bench_core::{
    block_timestamp_ms, ObservedBlock, RunClock, Sender, WarmupConfig, WarmupDecision,
    WarmupOutcome, WarmupSummary, WarmupTracker,
};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{sync::watch, task::JoinHandle};

/// How often the head is polled. Blocks arrive every ~0.7 s, so 200 ms keeps
/// observation lag small at a handful of cheap RPC calls per second.
const BLOCK_POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Upper bound on blocks fetched per poll so a stalled poller catches up
/// without starving the sender.
const MAX_BLOCKS_PER_POLL: u64 = 8;
/// `txpool_status` is polled once per second: the pool check compares
/// readings ten seconds apart, so finer sampling adds nothing.
const POOL_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Readiness is re-evaluated at most this often from the send loop.
const EVALUATION_INTERVAL: Duration = Duration::from_millis(250);
/// Progress is logged this often while warming up.
const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(5);

/// A running warm-up phase.
pub struct WarmupPhase {
    tracker: Arc<Mutex<WarmupTracker>>,
    stop_tx: watch::Sender<bool>,
    poller: JoinHandle<()>,
    started: Instant,
    started_unix_ms: u64,
    target_tps: u64,
    applied_rate: Option<u64>,
    next_evaluation: Instant,
    next_progress_log: Instant,
}

impl WarmupPhase {
    /// Start warming up. Blocks after `after_block` are attributed to the
    /// warm-up; `target_tps` is the configured `--tps` (0 = unlimited).
    pub fn start(
        config: WarmupConfig,
        provider: DynProvider<AnyNetwork>,
        after_block: u64,
        target_tps: u64,
        clock: &RunClock,
    ) -> Self {
        let pool_check = config.pool_check;
        let tracker = Arc::new(Mutex::new(WarmupTracker::new(config)));
        let (stop_tx, stop_rx) = watch::channel(false);
        let started = Instant::now();
        let poller = tokio::spawn(poll_chain(
            provider,
            after_block.saturating_add(1),
            tracker.clone(),
            started,
            pool_check,
            stop_rx,
        ));
        let now = Instant::now();
        Self {
            tracker,
            stop_tx,
            poller,
            started,
            started_unix_ms: clock.unix_ms(),
            target_tps,
            applied_rate: None,
            next_evaluation: now,
            next_progress_log: now + PROGRESS_LOG_INTERVAL,
        }
    }

    /// Apply the ramp's starting rate before the first transaction is sent.
    pub async fn apply_initial_rate(&mut self, sender: &Sender) {
        self.apply_rate(sender, Duration::ZERO).await;
    }

    async fn apply_rate(&mut self, sender: &Sender, elapsed: Duration) {
        if self.target_tps == 0 {
            return;
        }
        let rate = self
            .tracker
            .lock()
            .expect("warm-up tracker poisoned")
            .ramp_rate(elapsed, self.target_tps);
        if self.applied_rate != Some(rate) {
            sender.set_rate_limit(rate).await;
            if self.applied_rate.is_some_and(|previous| previous < self.target_tps) &&
                rate == self.target_tps
            {
                tracing::info!(tps = rate, "Warm-up ramp reached the target rate");
            }
            self.applied_rate = Some(rate);
        }
    }

    /// Advance the ramp and evaluate readiness. Call between transactions.
    ///
    /// Returns the outcome once the warm-up should end; the caller then moves
    /// the measurement origin and calls [`WarmupPhase::finish`].
    pub async fn tick(&mut self, sender: &Sender) -> Option<WarmupOutcome> {
        let elapsed = self.started.elapsed();
        self.apply_rate(sender, elapsed).await;

        let now = Instant::now();
        if now < self.next_evaluation {
            return None;
        }
        self.next_evaluation = now + EVALUATION_INTERVAL;

        let status = {
            let tracker = self.tracker.lock().expect("warm-up tracker poisoned");
            tracker.evaluate(elapsed)
        };
        let required = self.tracker.lock().expect("warm-up tracker poisoned").config().clone();

        if now >= self.next_progress_log {
            self.next_progress_log = now + PROGRESS_LOG_INTERVAL;
            tracing::info!(
                elapsed_secs = elapsed.as_secs(),
                blocks = self.tracker.lock().expect("warm-up tracker poisoned").blocks().len(),
                proposers_ready = status.ready_proposers(required.proposals_per_proposer),
                proposers_expected = required.expected_proposers,
                proposers_seen = status.proposals.len(),
                full_block_threshold = status.full_block_threshold,
                plateau = status.conditions.plateau,
                pool_stable = ?status.conditions.pool,
                min_elapsed = status.conditions.min_elapsed,
                rate = self.applied_rate.unwrap_or(self.target_tps),
                "Warm-up in progress"
            );
        }

        match status.decision {
            WarmupDecision::Continue => None,
            WarmupDecision::Ready => Some(WarmupOutcome::Ready),
            WarmupDecision::Timeout => Some(WarmupOutcome::Timeout),
            WarmupDecision::FixedElapsed => Some(WarmupOutcome::Fixed),
        }
    }

    /// Stop polling and summarize a warm-up that ended with `outcome` at
    /// `ended_unix_ms`.
    pub async fn finish(self, outcome: WarmupOutcome, ended_unix_ms: u64) -> WarmupSummary {
        self.stop_and_summarize(outcome, Some(ended_unix_ms)).await
    }

    /// Stop polling and summarize a warm-up that never finished because the
    /// transaction source ran dry.
    pub async fn abandon(self) -> WarmupSummary {
        self.stop_and_summarize(WarmupOutcome::SourceExhausted, None).await
    }

    async fn stop_and_summarize(
        self,
        outcome: WarmupOutcome,
        ended_unix_ms: Option<u64>,
    ) -> WarmupSummary {
        let elapsed = self.started.elapsed();
        let _ = self.stop_tx.send(true);
        self.poller.abort();
        let _ = self.poller.await;
        let tracker = self.tracker.lock().expect("warm-up tracker poisoned");
        tracker.summary(outcome, self.started_unix_ms, ended_unix_ms, elapsed)
    }
}

async fn poll_chain(
    provider: DynProvider<AnyNetwork>,
    mut next_block: u64,
    tracker: Arc<Mutex<WarmupTracker>>,
    started: Instant,
    pool_check: bool,
    mut stop_rx: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(BLOCK_POLL_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut pool_available = pool_check;
    let mut last_pool_poll: Option<Instant> = None;

    loop {
        tokio::select! {
            changed = stop_rx.changed() => {
                if changed.is_err() || *stop_rx.borrow() {
                    break;
                }
            }
            _ = ticker.tick() => {}
        }

        match provider.get_block_number().await {
            Ok(head) => {
                let mut fetched = 0u64;
                while next_block <= head && fetched < MAX_BLOCKS_PER_POLL {
                    match provider.get_block(BlockNumberOrTag::Number(next_block).into()).await {
                        Ok(Some(block)) => {
                            let observed = observed_block(&block);
                            tracker
                                .lock()
                                .expect("warm-up tracker poisoned")
                                .observe_block(observed);
                            next_block += 1;
                            fetched += 1;
                        }
                        Ok(None) => break,
                        Err(err) => {
                            tracing::debug!(%err, block = next_block, "Warm-up block fetch failed");
                            break;
                        }
                    }
                }
            }
            Err(err) => tracing::debug!(%err, "Warm-up head poll failed"),
        }

        if pool_available && last_pool_poll.is_none_or(|at| at.elapsed() >= POOL_POLL_INTERVAL) {
            last_pool_poll = Some(Instant::now());
            match provider.txpool_status().await {
                Ok(status) => tracker
                    .lock()
                    .expect("warm-up tracker poisoned")
                    .observe_pool_pending(started.elapsed(), status.pending),
                Err(err) => {
                    tracing::warn!(%err, "txpool_status unavailable; skipping warm-up pool check");
                    tracker.lock().expect("warm-up tracker poisoned").mark_pool_unavailable();
                    pool_available = false;
                }
            }
        }
    }
}

fn observed_block(block: &AnyRpcBlock) -> ObservedBlock {
    let header = block.header();
    ObservedBlock {
        number: header.number(),
        timestamp_ms: block_timestamp_ms(block),
        tx_count: block.transactions().len() as u64,
        gas_used: header.gas_used(),
        proposer: header.beneficiary(),
    }
}
