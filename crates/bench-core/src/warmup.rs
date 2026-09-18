//! Warm-up phase for `bench send`.
//!
//! A benchmark that starts measuring with its first transaction measures a cold
//! network. Three cold-start effects were visible in multi-region Tempo runs
//! (10 validators across four regions, ~50k target TPS, 90 s windows):
//!
//! - **Cold connections.** The first multi-megabyte block body a proposer sends to a far peer took
//!   0.7–2.3 s to arrive, the second one 0.13–0.25 s. Until every proposer has pushed a full body
//!   over every connection, block times are 1–2 s instead of ~0.7 s. Empty blocks do not warm this
//!   path.
//! - **Cold caches and estimators.** The payload builder's fill rate reached its plateau only after
//!   ~15 loaded blocks, and the node-side build-time and persistence estimators need full-size
//!   blocks to converge.
//! - **Pool shock.** Jumping to the target rate instantly filled every pool to its cap within two
//!   seconds and phase-locked the transaction expiry cycle to that burst.
//!
//! The warm-up therefore runs the *same* workload the measurement will use,
//! ramps the send rate instead of bursting, holds at the target rate until
//! chain-observable readiness signals hold, and then moves the measurement
//! origin (`start_block`, `started_at`, metric and sample offsets) to that
//! boundary. Load never pauses between warm-up and measurement: a pause would
//! let pools drain and connections cool down again.
//!
//! This module holds the pure decision logic (configuration, readiness
//! evaluation, ramp schedule, summary). The asynchronous chain polling lives in
//! the CLI, which feeds [`WarmupTracker`] with [`ObservedBlock`]s.

use alloy_primitives::Address;
use eyre::{bail, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    time::Duration,
};

/// Default ramp duration from a fraction of the target rate to the full rate.
///
/// Derivation: at 50k TPS the previous instant start filled 50k-transaction
/// pools in under two seconds. Spreading first admissions over 10 s covers
/// about 40% of the 25 s transaction validity window used by the public
/// workload, so expiry no longer happens in one mass eviction, and ~14 blocks
/// at 0.7 s pass before full pressure while connections and caches grow.
pub const DEFAULT_WARMUP_RAMP: Duration = Duration::from_secs(10);

/// Fraction of the warm-up rate the ramp starts from.
///
/// Derivation: a tenth of the warm-up rate keeps the first blocks non-empty
/// without a burst, and reaches the warm-up rate within the ramp.
pub const WARMUP_RAMP_START_FRACTION: f64 = 0.10;

/// Default cap on the send rate during the warm-up, in transactions per second
/// (0 = no cap, warm up at the target rate).
///
/// Derivation: a cold network should not see the full target load before its
/// connections, caches and estimators are warm. 2k TPS produces non-empty
/// blocks from every proposer within the minimum warm-up (about 1.4k
/// transactions per 0.7 s block) while staying more than an order of magnitude
/// below the 50k to 100k targets that overwhelmed a cold network. When the cap
/// is below the target, the rate ramps from the cap to the target over
/// [`WarmupConfig::ramp`] right before the measured window starts, so the
/// window itself runs at full rate from its first block.
pub const DEFAULT_WARMUP_TPS_CAP: u64 = 2_000;

/// Default minimum warm-up duration.
///
/// Derivation: with random leader election among ten validators, every
/// validator has proposed once after ~29 blocks on average (coupon collector,
/// 10 × H(10)), about 20 s at 0.7 s blocks. A 30 s floor also spans one full
/// 25 s validity window, so the pool's steady-state turnover is running.
pub const DEFAULT_WARMUP_MIN: Duration = Duration::from_secs(30);

/// Default maximum warm-up duration before measuring anyway.
///
/// Derivation: two full blocks from each of ten proposers takes 45–50 blocks
/// (~35 s). 90 s is roughly 2.5× that. Past it, something is wrong with the
/// network, and a flagged measurement is more useful than none.
pub const DEFAULT_WARMUP_MAX: Duration = Duration::from_secs(90);

/// Default number of full blocks each proposer must have produced.
///
/// Derivation: the first body over each proposer→peer connection was slow and
/// the second was already fast; requiring two confirms the warm connection
/// and tolerates a first block that was still small during the ramp.
pub const DEFAULT_WARMUP_PROPOSALS: u32 = 2;

/// Default window, in blocks, for the throughput plateau check.
///
/// Derivation: the builder fill rate plateaued after ~15 blocks. Comparing two
/// consecutive 10-block windows detects the plateau within a few blocks of it
/// forming while a median is robust to single-block dips.
pub const DEFAULT_WARMUP_STABLE_BLOCKS: usize = 10;

/// Default relative tolerance for the plateau and pool checks.
///
/// Derivation: steady-state block-to-block transaction counts varied by
/// roughly ±5%; 10% accepts that noise without accepting a ramp still in
/// progress (consecutive windows during the ramp differed by 30–50%).
pub const DEFAULT_WARMUP_STABLE_TOLERANCE: f64 = 0.10;

/// Fraction of the running median transaction count that makes a block count
/// as "full" for the proposer check.
///
/// Derivation: blocks cut short by node-side budget estimators held 45–60% of
/// the median; anything at or above half the median exercised the body path
/// with a multi-megabyte payload, which is what the connection warm-up needs.
pub const WARMUP_FULL_BLOCK_FRACTION: f64 = 0.50;

/// Window over which the pool occupancy must be stable.
///
/// Derivation: matches the plateau window (10 blocks at ~1 s worst case).
pub const WARMUP_POOL_STABLE_WINDOW: Duration = Duration::from_secs(10);

/// How the warm-up phase ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarmupMode {
    /// No warm-up: measurement starts with the first workload transaction.
    Off,
    /// Warm up until the readiness conditions hold, bounded by the configured
    /// minimum and maximum durations.
    Auto,
    /// Warm up for exactly this long regardless of readiness.
    Fixed(Duration),
}

impl fmt::Display for WarmupMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Off => f.write_str("off"),
            Self::Auto => f.write_str("auto"),
            Self::Fixed(duration) => write!(f, "{:.1}s", duration.as_secs_f64()),
        }
    }
}

/// Warm-up tunables.
#[derive(Debug, Clone)]
pub struct WarmupConfig {
    /// How the warm-up ends.
    pub mode: WarmupMode,
    /// The measured window's send rate (`--tps`, 0 = unlimited).
    pub target_tps: u64,
    /// Cap on the warm-up send rate (0 = warm up at the target rate). See
    /// [`DEFAULT_WARMUP_TPS_CAP`].
    pub rate_cap: u64,
    /// Ramp from [`WARMUP_RAMP_START_FRACTION`] of the warm-up rate to the
    /// warm-up rate over this duration, and from the warm-up rate to the target
    /// rate over the same duration once readiness holds. Zero disables both.
    pub ramp: Duration,
    /// Earliest time the warm-up may end in [`WarmupMode::Auto`].
    pub min_duration: Duration,
    /// Time after which [`WarmupMode::Auto`] ends the warm-up unconditionally.
    pub max_duration: Duration,
    /// Full blocks each proposer must have produced.
    pub proposals_per_proposer: u32,
    /// Number of distinct proposers expected to produce blocks.
    pub expected_proposers: usize,
    /// Window, in blocks, for the plateau check.
    pub stable_blocks: usize,
    /// Relative tolerance for the plateau and pool checks.
    pub stable_tolerance: f64,
    /// Whether to require stable `txpool_status` pending counts.
    pub pool_check: bool,
}

impl Default for WarmupConfig {
    fn default() -> Self {
        Self {
            mode: WarmupMode::Auto,
            target_tps: 0,
            rate_cap: DEFAULT_WARMUP_TPS_CAP,
            ramp: DEFAULT_WARMUP_RAMP,
            min_duration: DEFAULT_WARMUP_MIN,
            max_duration: DEFAULT_WARMUP_MAX,
            proposals_per_proposer: DEFAULT_WARMUP_PROPOSALS,
            expected_proposers: 1,
            stable_blocks: DEFAULT_WARMUP_STABLE_BLOCKS,
            stable_tolerance: DEFAULT_WARMUP_STABLE_TOLERANCE,
            pool_check: true,
        }
    }
}

impl WarmupConfig {
    /// Whether a warm-up phase runs at all.
    pub fn is_enabled(&self) -> bool {
        self.mode != WarmupMode::Off
    }

    /// Send rate held during the warm-up (0 = unlimited).
    ///
    /// The target rate, lowered to the cap when one is set. An unlimited
    /// target with a cap warms up at the cap.
    pub fn warmup_rate(&self) -> u64 {
        match (self.target_tps, self.rate_cap) {
            (_, 0) => self.target_tps,
            (0, cap) => cap,
            (target, cap) => target.min(cap),
        }
    }

    /// Whether the warm-up runs below the target rate, so a hand-off ramp to the
    /// target is needed before the measured window.
    pub fn is_rate_capped(&self) -> bool {
        self.warmup_rate() != self.target_tps
    }

    /// Duration of the ramp from the warm-up rate to the target rate that
    /// follows readiness. Zero when the rates are equal, the target is
    /// unlimited (nothing to ramp towards), or ramping is disabled.
    pub fn handoff_ramp(&self) -> Duration {
        if self.is_rate_capped() && self.target_tps > 0 {
            self.ramp
        } else {
            Duration::ZERO
        }
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.mode == WarmupMode::Off {
            return Ok(());
        }
        if self.proposals_per_proposer == 0 {
            bail!("warmup proposals per proposer must be at least 1");
        }
        if self.expected_proposers == 0 {
            bail!("warmup expected proposers must be at least 1");
        }
        if self.stable_blocks < 2 {
            bail!("warmup stable blocks must be at least 2");
        }
        if !(self.stable_tolerance > 0.0 && self.stable_tolerance < 1.0) {
            bail!("warmup stable tolerance must be between 0 and 1 (exclusive)");
        }
        if let WarmupMode::Fixed(duration) = self.mode &&
            duration.is_zero()
        {
            bail!("fixed warmup duration must be positive; use `off` to disable");
        }
        if self.mode == WarmupMode::Auto && self.min_duration > self.max_duration {
            bail!("warmup min duration must not exceed warmup max duration");
        }
        Ok(())
    }
}

/// A block observed on chain while warming up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservedBlock {
    /// Block number.
    pub number: u64,
    /// Block timestamp in milliseconds.
    pub timestamp_ms: u64,
    /// Transactions in the block, including system transactions.
    pub tx_count: u64,
    /// Gas used by the block.
    pub gas_used: u64,
    /// Block beneficiary. Tempo resolves a distinct fee recipient per
    /// validator, so this identifies the proposer without node metrics.
    pub proposer: Address,
}

/// Outcome of one readiness evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarmupDecision {
    /// Keep warming up.
    Continue,
    /// Every readiness condition holds; start measuring.
    Ready,
    /// The maximum duration elapsed without readiness; start measuring anyway.
    Timeout,
    /// The fixed duration elapsed.
    FixedElapsed,
}

/// Individual readiness conditions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WarmupConditions {
    /// The minimum duration elapsed.
    pub min_elapsed: bool,
    /// Every expected proposer produced enough full blocks.
    pub proposers: bool,
    /// Consecutive block windows agree on the transaction count.
    pub plateau: bool,
    /// Pool occupancy is stable. `None` when the check is disabled or
    /// `txpool_status` is unavailable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<bool>,
}

impl WarmupConditions {
    fn all_met(&self) -> bool {
        self.min_elapsed && self.proposers && self.plateau && self.pool != Some(false)
    }
}

/// Full readiness evaluation.
#[derive(Debug, Clone)]
pub struct WarmupStatus {
    /// What to do next.
    pub decision: WarmupDecision,
    /// Individual conditions.
    pub conditions: WarmupConditions,
    /// Full blocks produced per proposer.
    pub proposals: BTreeMap<Address, u32>,
    /// Transaction count at or above which a block counts as full.
    pub full_block_threshold: u64,
    /// Median transaction count of the most recent window.
    pub last_window_median_txs: Option<f64>,
    /// Median transaction count of the window before it.
    pub previous_window_median_txs: Option<f64>,
}

impl WarmupStatus {
    /// Number of proposers that produced enough full blocks.
    pub fn ready_proposers(&self, required: u32) -> usize {
        self.proposals.values().filter(|count| **count >= required).count()
    }
}

/// Summary of the warm-up phase, attached to reports.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WarmupSummary {
    /// Configured mode (`off`, `auto`, or the fixed duration).
    pub mode: String,
    /// `ready`, `timeout`, `fixed`, `source-exhausted`, or `off`.
    pub outcome: String,
    /// Whether the measurement origin was moved to a warm-up boundary.
    pub completed: bool,
    /// Whether every readiness condition held when the warm-up ended.
    pub ready: bool,
    /// Wall-clock start of the warm-up in Unix milliseconds.
    pub started_unix_ms: u64,
    /// Wall-clock end of the warm-up in Unix milliseconds, when it ended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_unix_ms: Option<u64>,
    /// Warm-up duration in milliseconds.
    pub duration_ms: u64,
    /// Blocks observed during the warm-up.
    pub blocks: u64,
    /// First warm-up block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_block: Option<u64>,
    /// Last warm-up block. Measurement starts at the next block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_block: Option<u64>,
    /// Proposers expected to produce blocks.
    pub expected_proposers: usize,
    /// Distinct proposers that produced at least one block.
    pub proposers_seen: usize,
    /// Proposers that produced enough full blocks.
    pub proposers_ready: usize,
    /// Transaction count at or above which a block counted as full.
    pub full_block_threshold: u64,
    /// Full blocks per proposer, keyed by beneficiary address.
    pub proposals_by_proposer: BTreeMap<String, u32>,
    /// Median transaction count of the last block window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_window_median_txs: Option<f64>,
    /// Send rate held during the warm-up in transactions per second (0 =
    /// unlimited).
    pub warmup_tps: u64,
    /// Duration of the ramp from the warm-up rate to the target rate that
    /// preceded the boundary, in milliseconds (0 when the rates were equal).
    pub handoff_ramp_ms: u64,
    /// Transactions sent before the boundary that were still awaiting a
    /// response when it happened. Their completions land in the measured
    /// window, so `success + failed` may exceed `sent` by up to this number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inflight_at_boundary: Option<u64>,
    /// Readiness conditions at the end of the warm-up.
    pub conditions: WarmupConditions,
}

/// Why the warm-up ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarmupOutcome {
    /// Readiness conditions held.
    Ready,
    /// The maximum duration elapsed first.
    Timeout,
    /// The fixed duration elapsed.
    Fixed,
    /// The transaction source ran dry before the warm-up ended.
    SourceExhausted,
}

impl WarmupOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Timeout => "timeout",
            Self::Fixed => "fixed",
            Self::SourceExhausted => "source-exhausted",
        }
    }
}

/// Accumulates chain observations and decides when the warm-up is done.
#[derive(Debug)]
pub struct WarmupTracker {
    config: WarmupConfig,
    blocks: Vec<ObservedBlock>,
    /// `(elapsed, pending)` pool readings, oldest first.
    pool: VecDeque<(Duration, u64)>,
    pool_unavailable: bool,
}

impl WarmupTracker {
    /// Create a tracker.
    pub fn new(config: WarmupConfig) -> Self {
        Self { config, blocks: Vec::new(), pool: VecDeque::new(), pool_unavailable: false }
    }

    /// The configuration.
    pub fn config(&self) -> &WarmupConfig {
        &self.config
    }

    /// Blocks observed so far.
    pub fn blocks(&self) -> &[ObservedBlock] {
        &self.blocks
    }

    /// Record a block. Returns `false` if the block number was already seen.
    pub fn observe_block(&mut self, block: ObservedBlock) -> bool {
        if self.blocks.iter().any(|seen| seen.number == block.number) {
            return false;
        }
        self.blocks.push(block);
        self.blocks.sort_by_key(|block| block.number);
        true
    }

    /// Record a pool occupancy reading.
    pub fn observe_pool_pending(&mut self, elapsed: Duration, pending: u64) {
        self.pool.push_back((elapsed, pending));
        // Keep a little more than the stability window.
        let horizon = WARMUP_POOL_STABLE_WINDOW.saturating_mul(3);
        while let Some((oldest, _)) = self.pool.front() &&
            elapsed.saturating_sub(*oldest) > horizon &&
            self.pool.len() > 2
        {
            self.pool.pop_front();
        }
    }

    /// Mark `txpool_status` as unsupported so the pool check is skipped.
    pub fn mark_pool_unavailable(&mut self) {
        self.pool_unavailable = true;
    }

    /// Send rate to use `elapsed` into the warm-up.
    ///
    /// Linear from [`WARMUP_RAMP_START_FRACTION`] × the warm-up rate to the
    /// warm-up rate over the configured ramp. Returns the warm-up rate when
    /// ramping is disabled or over, and 0 (unlimited) when the warm-up rate is
    /// unlimited.
    pub fn ramp_rate(&self, elapsed: Duration) -> u64 {
        let warmup_rate = self.config.warmup_rate();
        if warmup_rate == 0 || self.config.ramp.is_zero() || elapsed >= self.config.ramp {
            return warmup_rate;
        }
        let start = (warmup_rate as f64 * WARMUP_RAMP_START_FRACTION).round().max(1.0);
        linear_rate(start, warmup_rate as f64, elapsed, self.config.ramp).clamp(1, warmup_rate)
    }

    /// Send rate to use `elapsed` into the hand-off ramp that follows
    /// readiness when the warm-up rate is capped below the target.
    ///
    /// Linear from the warm-up rate to the target over
    /// [`WarmupConfig::handoff_ramp`]; returns the target once that elapsed.
    pub fn handoff_rate(&self, elapsed: Duration) -> u64 {
        let ramp = self.config.handoff_ramp();
        let target = self.config.target_tps;
        if ramp.is_zero() || elapsed >= ramp {
            return target;
        }
        let warmup_rate = self.config.warmup_rate() as f64;
        linear_rate(warmup_rate, target as f64, elapsed, ramp).clamp(1, target)
    }

    /// Transaction count at or above which a block counts as full.
    ///
    /// Half of the median transaction count over all non-empty blocks seen so
    /// far. Recomputed on every evaluation because the median rises during the
    /// ramp.
    pub fn full_block_threshold(&self) -> u64 {
        let mut counts: Vec<u64> =
            self.blocks.iter().map(|block| block.tx_count).filter(|count| *count > 0).collect();
        if counts.is_empty() {
            return 0;
        }
        counts.sort_unstable();
        let median = median_u64(&counts);
        ((median * WARMUP_FULL_BLOCK_FRACTION).ceil() as u64).max(1)
    }

    /// Full blocks produced per proposer.
    pub fn proposals(&self) -> BTreeMap<Address, u32> {
        let threshold = self.full_block_threshold();
        let mut proposals: BTreeMap<Address, u32> = BTreeMap::new();
        for block in &self.blocks {
            let entry = proposals.entry(block.proposer).or_default();
            if block.tx_count > 0 && block.tx_count >= threshold {
                *entry += 1;
            }
        }
        proposals
    }

    fn window_medians(&self) -> (Option<f64>, Option<f64>) {
        let window = self.config.stable_blocks;
        let len = self.blocks.len();
        if len < 2 * window {
            let last = if len >= window {
                Some(median_u64(&self.sorted_tx_counts(len - window..len)))
            } else {
                None
            };
            return (None, last);
        }
        let previous = median_u64(&self.sorted_tx_counts(len - 2 * window..len - window));
        let last = median_u64(&self.sorted_tx_counts(len - window..len));
        (Some(previous), Some(last))
    }

    fn sorted_tx_counts(&self, range: std::ops::Range<usize>) -> Vec<u64> {
        let mut counts: Vec<u64> = self.blocks[range].iter().map(|block| block.tx_count).collect();
        counts.sort_unstable();
        counts
    }

    /// `None` when the check is off, `txpool_status` is unavailable, or the
    /// warm-up rate is capped below the target (pool occupancy at the
    /// warm-up rate says nothing about the measured window); `Some(false)`
    /// until a full window of readings exists.
    fn pool_stable(&self, elapsed: Duration) -> Option<bool> {
        if !self.config.pool_check || self.pool_unavailable || self.config.is_rate_capped() {
            return None;
        }
        if elapsed < WARMUP_POOL_STABLE_WINDOW {
            return Some(false);
        }
        let Some((latest_elapsed, latest)) = self.pool.back().copied() else {
            return Some(false);
        };
        let cutoff = latest_elapsed.saturating_sub(WARMUP_POOL_STABLE_WINDOW);
        // The most recent reading that is at least one window old.
        let Some((_, old)) = self.pool.iter().rev().find(|(at, _)| *at <= cutoff).copied() else {
            return Some(false);
        };
        Some(within_tolerance(old as f64, latest as f64, self.config.stable_tolerance))
    }

    /// Evaluate readiness `elapsed` into the warm-up.
    pub fn evaluate(&self, elapsed: Duration) -> WarmupStatus {
        let proposals = self.proposals();
        let ready_proposers = proposals
            .values()
            .filter(|count| **count >= self.config.proposals_per_proposer)
            .count();
        let (previous_median, last_median) = self.window_medians();
        let plateau = match (previous_median, last_median) {
            (Some(previous), Some(last)) if previous > 0.0 => {
                within_tolerance(previous, last, self.config.stable_tolerance)
            }
            _ => false,
        };
        let conditions = WarmupConditions {
            min_elapsed: elapsed >= self.config.min_duration,
            proposers: ready_proposers >= self.config.expected_proposers,
            plateau,
            pool: self.pool_stable(elapsed),
        };
        let decision = match self.config.mode {
            WarmupMode::Off => WarmupDecision::Ready,
            WarmupMode::Fixed(duration) => {
                if elapsed >= duration {
                    WarmupDecision::FixedElapsed
                } else {
                    WarmupDecision::Continue
                }
            }
            WarmupMode::Auto => {
                if conditions.all_met() {
                    WarmupDecision::Ready
                } else if elapsed >= self.config.max_duration {
                    WarmupDecision::Timeout
                } else {
                    WarmupDecision::Continue
                }
            }
        };
        WarmupStatus {
            decision,
            conditions,
            proposals,
            full_block_threshold: self.full_block_threshold(),
            last_window_median_txs: last_median,
            previous_window_median_txs: previous_median,
        }
    }

    /// Build the summary for a warm-up that ended with `outcome`.
    ///
    /// `decided` is the evaluation that ended the warm-up. It is reported
    /// instead of a fresh evaluation because blocks produced during the
    /// hand-off ramp would otherwise change the conditions after the fact.
    /// `None` evaluates now (fixed mode, source exhausted).
    pub fn summary(
        &self,
        outcome: WarmupOutcome,
        started_unix_ms: u64,
        ended_unix_ms: Option<u64>,
        elapsed: Duration,
        decided: Option<&WarmupStatus>,
    ) -> WarmupSummary {
        let status = decided.cloned().unwrap_or_else(|| self.evaluate(elapsed));
        let proposals_by_proposer = status
            .proposals
            .iter()
            .map(|(proposer, count)| (format!("{proposer:#x}"), *count))
            .collect();
        WarmupSummary {
            mode: self.config.mode.to_string(),
            outcome: outcome.as_str().to_string(),
            completed: outcome != WarmupOutcome::SourceExhausted,
            ready: status.conditions.all_met(),
            started_unix_ms,
            ended_unix_ms,
            duration_ms: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
            blocks: self.blocks.len() as u64,
            first_block: self.blocks.first().map(|block| block.number),
            last_block: self.blocks.last().map(|block| block.number),
            expected_proposers: self.config.expected_proposers,
            proposers_seen: status.proposals.len(),
            proposers_ready: status.ready_proposers(self.config.proposals_per_proposer),
            full_block_threshold: status.full_block_threshold,
            proposals_by_proposer,
            last_window_median_txs: status.last_window_median_txs,
            warmup_tps: self.config.warmup_rate(),
            handoff_ramp_ms: u64::try_from(self.config.handoff_ramp().as_millis())
                .unwrap_or(u64::MAX),
            inflight_at_boundary: None,
            conditions: status.conditions,
        }
    }
}

fn linear_rate(from: f64, to: f64, elapsed: Duration, ramp: Duration) -> u64 {
    let progress = (elapsed.as_secs_f64() / ramp.as_secs_f64()).clamp(0.0, 1.0);
    (from + (to - from) * progress).round() as u64
}

fn median_u64(sorted: &[u64]) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let mid = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        (sorted[mid - 1] as f64 + sorted[mid] as f64) / 2.0
    } else {
        sorted[mid] as f64
    }
}

fn within_tolerance(reference: f64, value: f64, tolerance: f64) -> bool {
    let scale = reference.abs().max(1.0);
    (value - reference).abs() <= tolerance * scale
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proposer(index: u8) -> Address {
        Address::with_last_byte(index)
    }

    fn block(number: u64, tx_count: u64, proposer_index: u8) -> ObservedBlock {
        ObservedBlock {
            number,
            timestamp_ms: number * 700,
            tx_count,
            gas_used: tx_count * 90_000,
            proposer: proposer(proposer_index),
        }
    }

    fn config(expected_proposers: usize) -> WarmupConfig {
        WarmupConfig {
            expected_proposers,
            pool_check: false,
            target_tps: 50_000,
            rate_cap: 0,
            ..WarmupConfig::default()
        }
    }

    #[test]
    fn mode_display() {
        assert_eq!(WarmupMode::Off.to_string(), "off");
        assert_eq!(WarmupMode::Auto.to_string(), "auto");
        assert_eq!(WarmupMode::Fixed(Duration::from_millis(45_500)).to_string(), "45.5s");
    }

    #[test]
    fn validate_rejects_bad_values() {
        assert!(WarmupConfig::default().validate().is_ok());
        assert!(WarmupConfig { mode: WarmupMode::Off, stable_blocks: 0, ..config(1) }
            .validate()
            .is_ok());
        assert!(WarmupConfig { proposals_per_proposer: 0, ..config(1) }.validate().is_err());
        assert!(config(0).validate().is_err());
        assert!(WarmupConfig { stable_blocks: 1, ..config(1) }.validate().is_err());
        assert!(WarmupConfig { stable_tolerance: 1.0, ..config(1) }.validate().is_err());
        assert!(WarmupConfig { mode: WarmupMode::Fixed(Duration::ZERO), ..config(1) }
            .validate()
            .is_err());
        assert!(WarmupConfig {
            min_duration: Duration::from_secs(10),
            max_duration: Duration::from_secs(5),
            ..config(1)
        }
        .validate()
        .is_err());
    }

    #[test]
    fn ramp_is_linear_from_start_fraction_to_warmup_rate() {
        let tracker = WarmupTracker::new(config(1));
        assert_eq!(tracker.ramp_rate(Duration::ZERO), 5_000);
        assert_eq!(tracker.ramp_rate(Duration::from_secs(5)), 27_500);
        assert_eq!(tracker.ramp_rate(Duration::from_secs(10)), 50_000);
        assert_eq!(tracker.ramp_rate(Duration::from_secs(60)), 50_000);
        // Tiny targets never ramp below one transaction per second.
        let tiny = WarmupTracker::new(WarmupConfig { target_tps: 3, ..config(1) });
        assert_eq!(tiny.ramp_rate(Duration::ZERO), 1);
        // Unlimited stays unlimited without a cap.
        let unlimited = WarmupTracker::new(WarmupConfig { target_tps: 0, ..config(1) });
        assert_eq!(unlimited.ramp_rate(Duration::ZERO), 0);
        assert!(!unlimited.config().is_rate_capped());
    }

    #[test]
    fn ramp_disabled_returns_warmup_rate() {
        let tracker = WarmupTracker::new(WarmupConfig { ramp: Duration::ZERO, ..config(1) });
        assert_eq!(tracker.ramp_rate(Duration::ZERO), 50_000);
    }

    #[test]
    fn rate_cap_bounds_warmup_and_hands_off_to_target() {
        let capped = WarmupTracker::new(WarmupConfig { rate_cap: 2_000, ..config(1) });
        assert_eq!(capped.config().warmup_rate(), 2_000);
        assert!(capped.config().is_rate_capped());
        assert_eq!(capped.config().handoff_ramp(), DEFAULT_WARMUP_RAMP);
        // Initial ramp goes to the cap, not the target.
        assert_eq!(capped.ramp_rate(Duration::ZERO), 200);
        assert_eq!(capped.ramp_rate(Duration::from_secs(10)), 2_000);
        // Hand-off ramps from the cap to the target.
        assert_eq!(capped.handoff_rate(Duration::ZERO), 2_000);
        assert_eq!(capped.handoff_rate(Duration::from_secs(5)), 26_000);
        assert_eq!(capped.handoff_rate(Duration::from_secs(10)), 50_000);
        assert_eq!(capped.handoff_rate(Duration::from_secs(11)), 50_000);

        // A cap above the target changes nothing.
        let loose = WarmupTracker::new(WarmupConfig { rate_cap: 80_000, ..config(1) });
        assert_eq!(loose.config().warmup_rate(), 50_000);
        assert!(!loose.config().is_rate_capped());
        assert!(loose.config().handoff_ramp().is_zero());

        // An unlimited target warms up at the cap and hands off with a step.
        let unlimited =
            WarmupTracker::new(WarmupConfig { target_tps: 0, rate_cap: 2_000, ..config(1) });
        assert_eq!(unlimited.config().warmup_rate(), 2_000);
        assert!(unlimited.config().is_rate_capped());
        assert!(unlimited.config().handoff_ramp().is_zero());
        assert_eq!(unlimited.handoff_rate(Duration::ZERO), 0);

        let summary =
            capped.summary(WarmupOutcome::Ready, 0, Some(1), Duration::from_secs(40), None);
        assert_eq!(summary.warmup_tps, 2_000);
        assert_eq!(summary.handoff_ramp_ms, 10_000);
    }

    #[test]
    fn pool_check_is_not_applicable_when_rate_is_capped() {
        let mut tracker =
            WarmupTracker::new(WarmupConfig { pool_check: true, rate_cap: 2_000, ..config(1) });
        for second in 0..=25u64 {
            tracker.observe_pool_pending(Duration::from_secs(second), 1_000);
        }
        assert_eq!(tracker.evaluate(Duration::from_secs(25)).conditions.pool, None);
    }

    #[test]
    fn observe_block_deduplicates_and_sorts() {
        let mut tracker = WarmupTracker::new(config(1));
        assert!(tracker.observe_block(block(12, 10, 1)));
        assert!(tracker.observe_block(block(11, 10, 1)));
        assert!(!tracker.observe_block(block(12, 99, 1)));
        assert_eq!(tracker.blocks().iter().map(|b| b.number).collect::<Vec<_>>(), vec![11, 12]);
    }

    #[test]
    fn small_blocks_do_not_count_as_proposals() {
        let mut tracker = WarmupTracker::new(config(2));
        for number in 1..=8 {
            tracker.observe_block(block(number, 10_000, 1));
        }
        // Proposer 2 only produced blocks well below half the median.
        tracker.observe_block(block(9, 1_000, 2));
        tracker.observe_block(block(10, 2_000, 2));
        tracker.observe_block(block(11, 0, 2));
        let proposals = tracker.proposals();
        assert_eq!(proposals[&proposer(1)], 8);
        assert_eq!(proposals[&proposer(2)], 0);
        assert_eq!(tracker.full_block_threshold(), 5_000);
    }

    #[test]
    fn auto_waits_for_minimum_duration_and_conditions() {
        let mut tracker = WarmupTracker::new(WarmupConfig {
            min_duration: Duration::from_secs(30),
            max_duration: Duration::from_secs(90),
            stable_blocks: 4,
            ..config(2)
        });
        // Ramp: growing block sizes, both proposers active.
        let sizes = [1_000, 2_000, 4_000, 6_000, 8_000, 9_000, 9_500, 10_000];
        for (index, size) in sizes.iter().enumerate() {
            tracker.observe_block(block(index as u64 + 1, *size, (index % 2) as u8 + 1));
        }
        let status = tracker.evaluate(Duration::from_secs(10));
        assert_eq!(status.decision, WarmupDecision::Continue);
        assert!(!status.conditions.min_elapsed);
        assert!(!status.conditions.plateau, "ramp windows differ by more than 10%");
        assert!(status.conditions.proposers);

        // Plateau: eight more blocks around 10k.
        let plateau = [10_200, 9_900, 10_100, 9_800, 10_300, 9_950, 10_050, 10_000];
        for (index, size) in plateau.iter().enumerate() {
            tracker.observe_block(block(index as u64 + 9, *size, (index % 2) as u8 + 1));
        }
        let status = tracker.evaluate(Duration::from_secs(20));
        assert_eq!(status.decision, WarmupDecision::Continue, "minimum duration not reached");
        assert!(status.conditions.plateau);

        let status = tracker.evaluate(Duration::from_secs(31));
        assert_eq!(status.decision, WarmupDecision::Ready);
        assert!(status.conditions.min_elapsed);
        assert_eq!(status.ready_proposers(2), 2);
    }

    #[test]
    fn auto_times_out_when_a_proposer_never_shows_up() {
        let mut tracker = WarmupTracker::new(WarmupConfig {
            min_duration: Duration::from_secs(1),
            max_duration: Duration::from_secs(90),
            stable_blocks: 2,
            ..config(3)
        });
        for number in 1..=10 {
            tracker.observe_block(block(number, 10_000, (number % 2) as u8 + 1));
        }
        let status = tracker.evaluate(Duration::from_secs(60));
        assert_eq!(status.decision, WarmupDecision::Continue);
        assert!(!status.conditions.proposers);
        let status = tracker.evaluate(Duration::from_secs(90));
        assert_eq!(status.decision, WarmupDecision::Timeout);
        let summary = tracker.summary(
            WarmupOutcome::Timeout,
            1_000,
            Some(91_000),
            Duration::from_secs(90),
            None,
        );
        assert_eq!(summary.outcome, "timeout");
        assert!(summary.completed);
        assert!(!summary.ready);
        assert_eq!(summary.expected_proposers, 3);
        assert_eq!(summary.proposers_seen, 2);
        assert_eq!(summary.proposers_ready, 2);
        assert_eq!(summary.blocks, 10);
        assert_eq!(summary.first_block, Some(1));
        assert_eq!(summary.last_block, Some(10));
    }

    #[test]
    fn fixed_mode_ignores_readiness() {
        let tracker = WarmupTracker::new(WarmupConfig {
            mode: WarmupMode::Fixed(Duration::from_secs(45)),
            ..config(10)
        });
        assert_eq!(tracker.evaluate(Duration::from_secs(44)).decision, WarmupDecision::Continue);
        assert_eq!(
            tracker.evaluate(Duration::from_secs(45)).decision,
            WarmupDecision::FixedElapsed
        );
        let summary =
            tracker.summary(WarmupOutcome::Fixed, 0, Some(45_000), Duration::from_secs(45), None);
        assert_eq!(summary.mode, "45.0s");
        assert_eq!(summary.outcome, "fixed");
        assert!(summary.completed);
    }

    #[test]
    fn pool_check_requires_a_window_of_stable_readings() {
        let mut tracker = WarmupTracker::new(WarmupConfig {
            pool_check: true,
            target_tps: 50_000,
            rate_cap: 0,
            ..WarmupConfig::default()
        });
        // No readings yet: the check is pending, not satisfied.
        assert_eq!(tracker.evaluate(Duration::from_secs(5)).conditions.pool, Some(false));
        assert_eq!(tracker.evaluate(Duration::from_secs(15)).conditions.pool, Some(false));
        for second in 0..=12u64 {
            tracker.observe_pool_pending(Duration::from_secs(second), 10_000 + second * 5_000);
        }
        // Occupancy grew 60% over the window.
        assert_eq!(tracker.evaluate(Duration::from_secs(12)).conditions.pool, Some(false));
        for second in 13..=25u64 {
            tracker.observe_pool_pending(Duration::from_secs(second), 50_000);
        }
        assert_eq!(tracker.evaluate(Duration::from_secs(25)).conditions.pool, Some(true));
        tracker.mark_pool_unavailable();
        assert_eq!(tracker.evaluate(Duration::from_secs(25)).conditions.pool, None);
    }

    #[test]
    fn summary_reports_the_deciding_evaluation() {
        let mut tracker = WarmupTracker::new(WarmupConfig {
            min_duration: Duration::from_secs(1),
            stable_blocks: 2,
            ..config(1)
        });
        for number in 1..=4 {
            tracker.observe_block(block(number, 10_000, 1));
        }
        let decided = tracker.evaluate(Duration::from_secs(5));
        assert_eq!(decided.decision, WarmupDecision::Ready);
        // Hand-off ramp blocks grow and break the plateau after the decision.
        tracker.observe_block(block(5, 20_000, 1));
        tracker.observe_block(block(6, 40_000, 1));
        let fresh = tracker.summary(WarmupOutcome::Ready, 0, Some(1), Duration::from_secs(7), None);
        assert!(!fresh.ready);
        let summary = tracker.summary(
            WarmupOutcome::Ready,
            0,
            Some(1),
            Duration::from_secs(7),
            Some(&decided),
        );
        assert!(summary.ready);
        assert!(summary.conditions.plateau);
        // Block accounting still covers the whole warm-up.
        assert_eq!(summary.blocks, 6);
        assert_eq!(summary.last_block, Some(6));
    }

    #[test]
    fn source_exhausted_summary_is_not_completed() {
        let tracker = WarmupTracker::new(config(1));
        let summary =
            tracker.summary(WarmupOutcome::SourceExhausted, 5, None, Duration::from_secs(3), None);
        assert_eq!(summary.outcome, "source-exhausted");
        assert!(!summary.completed);
        assert_eq!(summary.blocks, 0);
        assert_eq!(summary.first_block, None);
    }

    #[test]
    fn summary_round_trips_through_json() {
        let mut tracker = WarmupTracker::new(config(1));
        tracker.observe_block(block(7, 10, 1));
        let summary =
            tracker.summary(WarmupOutcome::Ready, 1, Some(2), Duration::from_secs(1), None);
        let json = serde_json::to_string(&summary).unwrap();
        let parsed: WarmupSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, summary);
    }
}
