//! Metrics collection for bench.
//!
//! Collects statistics about transaction sending:
//! - Sent/success/failed counts
//! - Timing (latencies, throughput)
//! - Block-level statistics (post-run)

use crate::{clock::RunClock, sample::Sample};
use alloy_consensus::BlockHeader;
use alloy_eips::BlockNumberOrTag;
use alloy_network::{primitives::BlockResponse, Network};
use alloy_provider::Provider;

use eyre::{Context, Result};
use futures::{stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, Mutex},
    task::JoinHandle,
};

const BLOCK_STATS_FETCH_CONCURRENCY: usize = 32;

/// Metrics collected during a benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchMetrics {
    /// Total transactions sent.
    pub sent: u64,
    /// Successful transactions (accepted by RPC).
    pub success: u64,
    /// Failed transactions (rejected by RPC or network error).
    pub failed: u64,
    /// Transactions with observed receipts.
    #[serde(default)]
    pub included: u64,
    /// Transactions with observed reverted receipts.
    #[serde(default)]
    pub reverted: u64,
    /// Transactions still missing receipts when final metrics were collected.
    #[serde(default)]
    pub receipt_pending: u64,
    /// Receipt polling errors.
    #[serde(default)]
    pub receipt_errors: u64,
    /// Total elapsed time.
    #[serde(with = "duration_serde")]
    pub elapsed: Duration,
    /// Latency statistics, when latency collection is enabled.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub latency: Option<LatencyStats>,
}

impl BenchMetrics {
    /// Calculate transactions per second.
    pub fn tps(&self) -> f64 {
        if self.elapsed.as_secs_f64() > 0.0 {
            self.sent as f64 / self.elapsed.as_secs_f64()
        } else {
            0.0
        }
    }

    /// Calculate success rate as a percentage.
    pub fn success_rate(&self) -> f64 {
        if self.sent > 0 {
            (self.success as f64 / self.sent as f64) * 100.0
        } else {
            0.0
        }
    }
}

/// Latency statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LatencyStats {
    /// Minimum latency observed.
    #[serde(with = "duration_serde")]
    pub min: Duration,
    /// Maximum latency observed.
    #[serde(with = "duration_serde")]
    pub max: Duration,
    /// Mean latency.
    #[serde(with = "duration_serde")]
    pub mean: Duration,
    /// P50 latency.
    #[serde(with = "duration_serde")]
    pub p50: Duration,
    /// P95 latency.
    #[serde(with = "duration_serde")]
    pub p95: Duration,
    /// P99 latency.
    #[serde(with = "duration_serde")]
    pub p99: Duration,
}

impl LatencyStats {
    /// Compute latency stats from a sorted slice of durations.
    pub fn from_sorted(samples: &[Duration]) -> Self {
        if samples.is_empty() {
            return Self::default();
        }

        let n = samples.len();
        let sum: Duration = samples.iter().sum();
        let mean = sum / n as u32;

        Self {
            min: samples[0],
            max: samples[n - 1],
            mean,
            p50: percentile(samples, 50),
            p95: percentile(samples, 95),
            p99: percentile(samples, 99),
        }
    }
}

/// Calculate percentile from a sorted slice.
fn percentile(sorted: &[Duration], p: usize) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = (sorted.len() * p / 100).min(sorted.len() - 1);
    sorted[idx]
}

/// Compute latency statistics from an unsorted slice of durations.
///
/// Returns `LatencyStats` with min, max, mean, p50, p95, p99.
pub fn compute_latency_stats(samples: &[Duration]) -> LatencyStats {
    if samples.is_empty() {
        return LatencyStats::default();
    }

    let mut sorted = samples.to_vec();
    sorted.sort();
    LatencyStats::from_sorted(&sorted)
}

/// A timestamped latency sample.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencySample {
    /// Offset from benchmark start in milliseconds.
    pub offset_ms: u64,
    /// Latency of this request.
    #[serde(with = "duration_serde")]
    pub latency: Duration,
}

/// Per-second throughput snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputSample {
    /// Second offset from benchmark start (0 = first second).
    pub second: u64,
    /// Transactions sent during this second.
    pub sent: u64,
    /// Successful transactions during this second.
    pub success: u64,
    /// Failed transactions during this second.
    pub failed: u64,
}

/// Time-series metrics for graphing.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TimeSeriesMetrics {
    /// Per-second throughput samples.
    pub throughput: Vec<ThroughputSample>,
    /// Individual latency samples with timestamps, when collection is enabled.
    pub latencies: Vec<LatencySample>,
}

/// Block-level statistics collected post-run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockStats {
    /// Block number.
    pub number: u64,
    /// Block timestamp in milliseconds. Uses `timestampMillisPart` when
    /// available, otherwise `header.timestamp * 1000`.
    pub timestamp_ms: u64,
    /// Total transactions in the block.
    pub tx_count: usize,
    /// Gas used by the block.
    pub gas_used: u64,
    /// Gas limit of the block.
    pub gas_limit: u64,
    /// Time since previous block in milliseconds.
    pub block_time_ms: Option<u64>,

    // -- Engine API timing (send-blocks mode only) --
    /// Client-side `reth_newPayload` latency in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_payload_ms: Option<u64>,
    /// Client-side `reth_forkchoiceUpdated` latency in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forkchoice_updated_ms: Option<u64>,
    /// Server-side execution latency in microseconds (from `reth_newPayload` response).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_payload_server_latency_us: Option<u64>,
    /// Server-side persistence wait in microseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistence_wait_us: Option<u64>,
    /// Server-side execution cache wait in microseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_cache_wait_us: Option<u64>,
    /// Server-side sparse trie wait in microseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sparse_trie_wait_us: Option<u64>,
}

/// Run summary statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunStats {
    /// Starting block number.
    pub start_block: u64,
    /// Ending block number.
    pub end_block: u64,
    /// Total blocks in the run.
    pub total_blocks: u64,
    /// Total transactions across all blocks.
    pub total_txs: u64,
    /// Total gas used across all blocks.
    pub total_gas: u64,
    /// Total duration in milliseconds.
    pub duration_ms: u64,
    /// Average blocks per second.
    pub avg_blocks_per_second: f64,
    /// Average transactions per second.
    pub avg_tps: f64,
    /// Average gas per second.
    pub avg_gas_per_second: f64,
    /// P50 block time in milliseconds.
    pub block_time_p50_ms: u64,
    /// P95 block time in milliseconds.
    pub block_time_p95_ms: u64,
    /// P99 block time in milliseconds.
    pub block_time_p99_ms: u64,
}

impl RunStats {
    /// Compute run stats from block chain timestamps.
    pub fn from_blocks_chain_time(blocks: &[BlockStats]) -> Self {
        let start_ms = blocks.first().map(|b| b.timestamp_ms).unwrap_or(0);
        let end_ms = blocks.last().map(|b| b.timestamp_ms).unwrap_or(0);
        Self::from_blocks_with_duration_ms(blocks, end_ms.saturating_sub(start_ms))
    }

    /// Compute run stats from a wall-clock benchmark duration.
    pub fn from_blocks_wall_time(blocks: &[BlockStats], elapsed: Duration) -> Self {
        let duration_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        Self::from_blocks_with_duration_ms(blocks, duration_ms)
    }

    fn from_blocks_with_duration_ms(blocks: &[BlockStats], duration_ms: u64) -> Self {
        if blocks.is_empty() {
            return Self {
                start_block: 0,
                end_block: 0,
                total_blocks: 0,
                total_txs: 0,
                total_gas: 0,
                duration_ms: 0,
                avg_blocks_per_second: 0.0,
                avg_tps: 0.0,
                avg_gas_per_second: 0.0,
                block_time_p50_ms: 0,
                block_time_p95_ms: 0,
                block_time_p99_ms: 0,
            };
        }

        let start_block = blocks.first().map(|b| b.number).unwrap_or(0);
        let end_block = blocks.last().map(|b| b.number).unwrap_or(0);
        let total_blocks = blocks.len() as u64;

        let total_txs: u64 = blocks.iter().map(|b| b.tx_count as u64).sum();
        let total_gas: u64 = blocks.iter().map(|b| b.gas_used).sum();
        let duration_secs = duration_ms as f64 / 1000.0;

        let avg_blocks_per_second =
            if duration_secs > 0.0 { total_blocks as f64 / duration_secs } else { 0.0 };

        let avg_tps = if duration_secs > 0.0 { total_txs as f64 / duration_secs } else { 0.0 };

        let avg_gas_per_second =
            if duration_secs > 0.0 { total_gas as f64 / duration_secs } else { 0.0 };

        let mut block_times: Vec<u64> = blocks.iter().filter_map(|b| b.block_time_ms).collect();
        block_times.sort();

        let block_time_p50_ms = percentile_u64(&block_times, 50);
        let block_time_p95_ms = percentile_u64(&block_times, 95);
        let block_time_p99_ms = percentile_u64(&block_times, 99);

        Self {
            start_block,
            end_block,
            total_blocks,
            total_txs,
            total_gas,
            duration_ms,
            avg_blocks_per_second,
            avg_tps,
            avg_gas_per_second,
            block_time_p50_ms,
            block_time_p95_ms,
            block_time_p99_ms,
        }
    }
}

fn percentile_u64(sorted: &[u64], p: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = (sorted.len() * p / 100).min(sorted.len() - 1);
    sorted[idx]
}

/// Collect block statistics from the chain.
///
/// Supports millisecond-precision block times when the node returns
/// Tempo-style `timestampMillis` in the block response's extra fields.
/// Falls back to second-precision (`timestamp * 1000`) for standard
/// Ethereum nodes.
pub async fn collect_block_stats<N: Network, P: Provider<N>>(
    provider: &P,
    start_block: u64,
    end_block: u64,
) -> Result<Vec<BlockStats>> {
    let blocks = stream::iter(start_block..=end_block)
        .map(|number| async move {
            let block = provider
                .get_block(BlockNumberOrTag::Number(number).into())
                .await
                .wrap_err_with(|| format!("failed to fetch block {number}"))?
                .ok_or_else(|| eyre::eyre!("block {number} not found"))?;

            let timestamp_secs = block.header().timestamp();
            let timestamp_ms = extract_timestamp_ms(&block, timestamp_secs);

            Ok::<_, eyre::Report>((
                number,
                timestamp_ms,
                block.transactions().len(),
                block.header().gas_used(),
                block.header().gas_limit(),
            ))
        })
        .buffered(BLOCK_STATS_FETCH_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;

    let mut stats = Vec::with_capacity(blocks.len());
    let mut prev_timestamp_ms: Option<u64> = None;

    for (number, timestamp_ms, tx_count, gas_used, gas_limit) in blocks {
        let block_time_ms = prev_timestamp_ms.map(|prev| timestamp_ms.saturating_sub(prev));
        prev_timestamp_ms = Some(timestamp_ms);

        stats.push(BlockStats {
            number,
            timestamp_ms,
            tx_count,
            gas_used,
            gas_limit,
            block_time_ms,
            new_payload_ms: None,
            forkchoice_updated_ms: None,
            new_payload_server_latency_us: None,
            persistence_wait_us: None,
            execution_cache_wait_us: None,
            sparse_trie_wait_us: None,
        });
    }

    Ok(stats)
}

/// Trim trailing empty blocks from collected block stats.
///
/// Removes suffix blocks where `gas_used == 0` (system-only blocks that
/// contain no user transactions). These typically accumulate while waiting
/// for the txpool to drain.
///
/// Middle gaps (empty blocks between blocks with user transactions) are
/// preserved since they may indicate real chain stalls.
///
/// Returns the millisecond timestamp of the last retained block, or `None`
/// if no blocks with user transactions were found (in which case the
/// blocks are left unmodified).
pub fn trim_trailing_empty_blocks(blocks: &mut Vec<BlockStats>) -> Option<u64> {
    let last_real_idx = blocks.iter().rposition(|b| b.gas_used > 0)?;

    let trimmed = blocks.len() - (last_real_idx + 1);
    if trimmed > 0 {
        tracing::info!(trimmed, "Trimmed trailing empty blocks");
        blocks.truncate(last_real_idx + 1);
    }

    blocks.last().map(|b| b.timestamp_ms)
}

/// Extract a millisecond-precision timestamp from a block response.
///
/// Checks `other_fields` for Tempo's `timestampMillisPart` field and
/// combines it with the second-precision timestamp. Falls back to
/// `timestamp_secs * 1000` for standard Ethereum blocks.
fn extract_timestamp_ms<B: BlockResponse>(block: &B, timestamp_secs: u64) -> u64 {
    if let Some(other) = block.other_fields()
        && let Some(Ok(ms_part)) =
            other.get_deserialized::<alloy_primitives::U64>("timestampMillisPart")
    {
        return timestamp_secs.saturating_mul(1000).saturating_add(ms_part.to());
    }
    timestamp_secs.saturating_mul(1000)
}

/// Internal record for a latency with its timestamp.
#[derive(Debug, Clone)]
struct TimestampedLatency {
    offset: Duration,
    latency: Duration,
}

/// Internal record for a sent/success/fail event.
#[derive(Debug, Clone, Copy)]
enum TxEvent {
    Sent,
    Success,
    Failed,
}

/// Internal record with timestamp.
#[derive(Debug, Clone)]
struct TimestampedEvent {
    offset: Duration,
    event: TxEvent,
}

#[derive(Debug, Clone, Copy, Default)]
struct ThroughputCounts {
    sent: u64,
    success: u64,
    failed: u64,
}

/// Options controlling metrics collection behavior.
#[derive(Debug, Clone, Copy)]
pub struct MetricsCollectorOptions {
    /// Whether to retain per-transaction latency samples for aggregate latency
    /// stats and `time_series.latencies` reporting.
    pub collect_latencies: bool,
}

impl Default for MetricsCollectorOptions {
    fn default() -> Self {
        Self { collect_latencies: true }
    }
}

#[derive(Debug)]
struct MetricsAggregation {
    throughput: BTreeMap<u64, ThroughputCounts>,
    latencies: Vec<TimestampedLatency>,
}

impl MetricsAggregation {
    fn new() -> Self {
        Self { throughput: BTreeMap::new(), latencies: Vec::new() }
    }

    fn record_event(&mut self, event: TimestampedEvent) {
        let bucket = self.throughput.entry(event.offset.as_secs()).or_default();
        match event.event {
            TxEvent::Sent => bucket.sent += 1,
            TxEvent::Success => bucket.success += 1,
            TxEvent::Failed => bucket.failed += 1,
        }
    }

    fn record_latency(&mut self, latency: TimestampedLatency) {
        self.latencies.push(latency);
    }
}

async fn metrics_aggregation_loop(
    aggregation: Arc<Mutex<MetricsAggregation>>,
    mut latency_rx: mpsc::UnboundedReceiver<TimestampedLatency>,
    mut event_rx: mpsc::UnboundedReceiver<TimestampedEvent>,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => break,
            Some(event) = event_rx.recv() => {
                let mut aggregation = aggregation.lock().await;
                aggregation.record_event(event);
                drain_available_metrics(&mut aggregation, &mut latency_rx, &mut event_rx);
            }
            Some(latency) = latency_rx.recv() => {
                let mut aggregation = aggregation.lock().await;
                aggregation.record_latency(latency);
                drain_available_metrics(&mut aggregation, &mut latency_rx, &mut event_rx);
            }
            else => break,
        }
    }

    let mut aggregation = aggregation.lock().await;
    drain_available_metrics(&mut aggregation, &mut latency_rx, &mut event_rx);
}

fn drain_available_metrics(
    aggregation: &mut MetricsAggregation,
    latency_rx: &mut mpsc::UnboundedReceiver<TimestampedLatency>,
    event_rx: &mut mpsc::UnboundedReceiver<TimestampedEvent>,
) {
    while let Ok(event) = event_rx.try_recv() {
        aggregation.record_event(event);
    }
    while let Ok(latency) = latency_rx.try_recv() {
        aggregation.record_latency(latency);
    }
}

/// Atomic counters for concurrent metrics collection.
///
/// Events and latencies are sent through lock-free mpsc channels to avoid
/// write-lock contention on the hot path at high TPS. A background aggregation
/// task drains those channels continuously so they do not retain one queued
/// allocation per transaction until final reporting.
pub struct MetricsCollector {
    sent: AtomicU64,
    success: AtomicU64,
    failed: AtomicU64,
    included: AtomicU64,
    reverted: AtomicU64,
    receipt_pending: AtomicU64,
    receipt_errors: AtomicU64,
    clock: RunClock,
    collect_latencies: bool,
    latency_tx: mpsc::UnboundedSender<TimestampedLatency>,
    event_tx: mpsc::UnboundedSender<TimestampedEvent>,
    aggregation: Arc<Mutex<MetricsAggregation>>,
    aggregation_shutdown: Mutex<Option<oneshot::Sender<()>>>,
    aggregation_handle: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for MetricsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsCollector")
            .field("sent", &self.sent)
            .field("success", &self.success)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl MetricsCollector {
    /// Create a new metrics collector with a shared [`RunClock`].
    pub fn new(clock: RunClock) -> Arc<Self> {
        Self::new_with_options(clock, MetricsCollectorOptions::default())
    }

    /// Create a new metrics collector with explicit latency collection behavior.
    pub fn new_with_latencies(clock: RunClock, collect_latencies: bool) -> Arc<Self> {
        Self::new_with_options(clock, MetricsCollectorOptions { collect_latencies })
    }

    /// Create a new metrics collector with explicit options.
    pub fn new_with_options(clock: RunClock, options: MetricsCollectorOptions) -> Arc<Self> {
        let (latency_tx, latency_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (aggregation_shutdown_tx, aggregation_shutdown_rx) = oneshot::channel();
        let aggregation = Arc::new(Mutex::new(MetricsAggregation::new()));
        let aggregation_handle = tokio::spawn(metrics_aggregation_loop(
            aggregation.clone(),
            latency_rx,
            event_rx,
            aggregation_shutdown_rx,
        ));

        Arc::new(Self {
            sent: AtomicU64::new(0),
            success: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            included: AtomicU64::new(0),
            reverted: AtomicU64::new(0),
            receipt_pending: AtomicU64::new(0),
            receipt_errors: AtomicU64::new(0),
            clock,
            collect_latencies: options.collect_latencies,
            latency_tx,
            event_tx,
            aggregation,
            aggregation_shutdown: Mutex::new(Some(aggregation_shutdown_tx)),
            aggregation_handle: Mutex::new(Some(aggregation_handle)),
        })
    }

    /// Get a reference to the shared [`RunClock`].
    pub fn clock(&self) -> &RunClock {
        &self.clock
    }

    /// Get the elapsed time since start (lock-free).
    pub fn elapsed_since_start(&self) -> Duration {
        self.clock.elapsed()
    }

    /// Get the elapsed time since start.
    fn elapsed(&self) -> Duration {
        self.clock.elapsed()
    }

    /// Record a sent transaction.
    pub fn record_sent(&self) {
        self.sent.fetch_add(1, Ordering::Relaxed);
        let offset = self.elapsed();
        let _ = self.event_tx.send(TimestampedEvent { offset, event: TxEvent::Sent });
    }

    /// Record a successful transaction with its latency.
    pub fn record_success(&self, latency: Duration) {
        self.success.fetch_add(1, Ordering::Relaxed);
        let offset = self.elapsed();
        if self.collect_latencies {
            let _ = self.latency_tx.send(TimestampedLatency { offset, latency });
        }
        let _ = self.event_tx.send(TimestampedEvent { offset, event: TxEvent::Success });
    }

    /// Record a failed transaction.
    pub fn record_failure(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
        let offset = self.elapsed();
        let _ = self.event_tx.send(TimestampedEvent { offset, event: TxEvent::Failed });
    }

    /// Record that a transaction receipt was observed.
    pub fn record_receipt(&self, success: bool) {
        self.included.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.reverted.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record transactions still missing receipts after receipt collection drained.
    pub fn set_receipt_pending(&self, pending: u64) {
        self.receipt_pending.store(pending, Ordering::Relaxed);
    }

    /// Record a receipt polling error.
    pub fn record_receipt_error(&self) {
        self.receipt_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the current counts (sent, success, failed).
    pub fn counts(&self) -> (u64, u64, u64) {
        (
            self.sent.load(Ordering::Relaxed),
            self.success.load(Ordering::Relaxed),
            self.failed.load(Ordering::Relaxed),
        )
    }

    /// Snapshot internal counters as unified [`Sample`]s.
    ///
    /// Returns samples for all txgen internal metrics at the current point
    /// in time. Intended to be called periodically on the scrape ticker.
    pub fn snapshot_samples(&self) -> Vec<Sample> {
        let sent = self.sent.load(Ordering::Relaxed);
        let success = self.success.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        let included = self.included.load(Ordering::Relaxed);
        let reverted = self.reverted.load(Ordering::Relaxed);
        let receipt_pending = self.receipt_pending.load(Ordering::Relaxed);
        let receipt_errors = self.receipt_errors.load(Ordering::Relaxed);
        let inflight = sent.saturating_sub(success + failed);
        let offset_ms = self.clock.offset_ms();
        let unix_ms = self.clock.unix_ms();
        let labels = BTreeMap::new();

        vec![
            Sample {
                name: "txgen_transactions_sent_total".to_string(),
                labels: labels.clone(),
                value: sent as f64,
                offset_ms,
                unix_ms,
            },
            Sample {
                name: "txgen_transactions_success_total".to_string(),
                labels: labels.clone(),
                value: success as f64,
                offset_ms,
                unix_ms,
            },
            Sample {
                name: "txgen_transactions_failed_total".to_string(),
                labels: labels.clone(),
                value: failed as f64,
                offset_ms,
                unix_ms,
            },
            Sample {
                name: "txgen_transactions_included_total".to_string(),
                labels: labels.clone(),
                value: included as f64,
                offset_ms,
                unix_ms,
            },
            Sample {
                name: "txgen_transactions_reverted_total".to_string(),
                labels: labels.clone(),
                value: reverted as f64,
                offset_ms,
                unix_ms,
            },
            Sample {
                name: "txgen_receipts_pending".to_string(),
                labels: labels.clone(),
                value: receipt_pending as f64,
                offset_ms,
                unix_ms,
            },
            Sample {
                name: "txgen_receipt_errors_total".to_string(),
                labels: labels.clone(),
                value: receipt_errors as f64,
                offset_ms,
                unix_ms,
            },
            Sample {
                name: "txgen_transactions_inflight".to_string(),
                labels,
                value: inflight as f64,
                offset_ms,
                unix_ms,
            },
        ]
    }

    async fn finish_aggregation(&self) {
        if let Some(shutdown) = self.aggregation_shutdown.lock().await.take() {
            let _ = shutdown.send(());
        }

        if let Some(handle) = self.aggregation_handle.lock().await.take()
            && let Err(err) = handle.await
        {
            tracing::warn!(%err, "metrics aggregation task failed");
        }
    }

    /// Compute final metrics.
    pub async fn finalize(&self) -> BenchMetrics {
        let elapsed = self.clock.elapsed();
        self.finish_aggregation().await;

        let latency = if self.collect_latencies {
            let aggregation = self.aggregation.lock().await;
            let mut durations: Vec<Duration> =
                aggregation.latencies.iter().map(|l| l.latency).collect();
            durations.sort();
            Some(LatencyStats::from_sorted(&durations))
        } else {
            None
        };

        BenchMetrics {
            sent: self.sent.load(Ordering::Relaxed),
            success: self.success.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            included: self.included.load(Ordering::Relaxed),
            reverted: self.reverted.load(Ordering::Relaxed),
            receipt_pending: self.receipt_pending.load(Ordering::Relaxed),
            receipt_errors: self.receipt_errors.load(Ordering::Relaxed),
            elapsed,
            latency,
        }
    }

    /// Extract time-series metrics for graphing.
    pub async fn time_series(&self) -> TimeSeriesMetrics {
        let total_elapsed = self.clock.elapsed();
        let total_seconds = total_elapsed.as_secs() + 1;
        self.finish_aggregation().await;

        let aggregation = self.aggregation.lock().await;
        let mut throughput = Vec::with_capacity(total_seconds as usize);
        for second in 0..total_seconds {
            let counts = aggregation.throughput.get(&second).copied().unwrap_or_default();
            throughput.push(ThroughputSample {
                second,
                sent: counts.sent,
                success: counts.success,
                failed: counts.failed,
            });
        }

        let latency_samples: Vec<LatencySample> = aggregation
            .latencies
            .iter()
            .map(|l| LatencySample { offset_ms: l.offset.as_millis() as u64, latency: l.latency })
            .collect();

        TimeSeriesMetrics { throughput, latencies: latency_samples }
    }
}

mod duration_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    #[derive(Serialize, Deserialize)]
    struct DurationRepr {
        secs: u64,
        nanos: u32,
    }

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        DurationRepr { secs: duration.as_secs(), nanos: duration.subsec_nanos() }
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let repr = DurationRepr::deserialize(deserializer)?;
        Ok(Duration::new(repr.secs, repr.nanos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_collection() {
        let collector = MetricsCollector::new(RunClock::new());

        collector.record_sent();
        collector.record_sent();
        collector.record_success(Duration::from_millis(10));
        collector.record_failure();

        let (sent, success, failed) = collector.counts();
        assert_eq!(sent, 2);
        assert_eq!(success, 1);
        assert_eq!(failed, 1);
    }

    #[tokio::test]
    async fn test_snapshot_samples() {
        let collector = MetricsCollector::new(RunClock::new());

        collector.record_sent();
        collector.record_sent();
        collector.record_sent();
        collector.record_success(Duration::from_millis(5));
        collector.record_failure();
        collector.record_receipt(false);
        collector.set_receipt_pending(1);
        collector.record_receipt_error();

        let samples = collector.snapshot_samples();
        assert_eq!(samples.len(), 8);

        let by_name: std::collections::HashMap<&str, f64> =
            samples.iter().map(|s| (s.name.as_str(), s.value)).collect();

        assert_eq!(by_name["txgen_transactions_sent_total"], 3.0);
        assert_eq!(by_name["txgen_transactions_success_total"], 1.0);
        assert_eq!(by_name["txgen_transactions_failed_total"], 1.0);
        assert_eq!(by_name["txgen_transactions_included_total"], 1.0);
        assert_eq!(by_name["txgen_transactions_reverted_total"], 1.0);
        assert_eq!(by_name["txgen_receipts_pending"], 1.0);
        assert_eq!(by_name["txgen_receipt_errors_total"], 1.0);
        assert_eq!(by_name["txgen_transactions_inflight"], 1.0);

        // All samples share the same timestamp.
        let offset = samples[0].offset_ms;
        assert!(samples.iter().all(|s| s.offset_ms == offset));
    }

    #[test]
    fn test_latency_stats() {
        let samples = vec![
            Duration::from_millis(1),
            Duration::from_millis(2),
            Duration::from_millis(3),
            Duration::from_millis(4),
            Duration::from_millis(100),
        ];
        let stats = LatencyStats::from_sorted(&samples);

        assert_eq!(stats.min, Duration::from_millis(1));
        assert_eq!(stats.max, Duration::from_millis(100));
    }

    #[test]
    fn test_bench_metrics() {
        let metrics = BenchMetrics {
            sent: 100,
            success: 90,
            failed: 10,
            included: 0,
            reverted: 0,
            receipt_pending: 0,
            receipt_errors: 0,
            elapsed: Duration::from_secs(10),
            latency: None,
        };

        assert!((metrics.tps() - 10.0).abs() < 0.001);
        assert!((metrics.success_rate() - 90.0).abs() < 0.001);
    }

    #[test]
    fn test_run_stats_from_blocks() {
        let blocks = vec![
            BlockStats {
                number: 100,
                timestamp_ms: 1_000_000,
                tx_count: 10,
                gas_used: 1_000_000,
                gas_limit: 30_000_000,
                block_time_ms: None,
                new_payload_ms: None,
                forkchoice_updated_ms: None,
                new_payload_server_latency_us: None,
                persistence_wait_us: None,
                execution_cache_wait_us: None,
                sparse_trie_wait_us: None,
            },
            BlockStats {
                number: 101,
                timestamp_ms: 1_012_000,
                tx_count: 15,
                gas_used: 1_500_000,
                gas_limit: 30_000_000,
                block_time_ms: Some(12000),
                new_payload_ms: None,
                forkchoice_updated_ms: None,
                new_payload_server_latency_us: None,
                persistence_wait_us: None,
                execution_cache_wait_us: None,
                sparse_trie_wait_us: None,
            },
            BlockStats {
                number: 102,
                timestamp_ms: 1_024_000,
                tx_count: 20,
                gas_used: 2_000_000,
                gas_limit: 30_000_000,
                block_time_ms: Some(12000),
                new_payload_ms: None,
                forkchoice_updated_ms: None,
                new_payload_server_latency_us: None,
                persistence_wait_us: None,
                execution_cache_wait_us: None,
                sparse_trie_wait_us: None,
            },
        ];

        let run_stats = RunStats::from_blocks_chain_time(&blocks);

        assert_eq!(run_stats.start_block, 100);
        assert_eq!(run_stats.end_block, 102);
        assert_eq!(run_stats.total_txs, 45);
        assert_eq!(run_stats.total_gas, 4_500_000);
        assert_eq!(run_stats.duration_ms, 24000);
    }

    #[test]
    fn test_run_stats_from_blocks_wall_time() {
        let blocks = vec![
            BlockStats {
                number: 100,
                timestamp_ms: 1_000_000,
                tx_count: 10,
                gas_used: 1_000_000,
                gas_limit: 30_000_000,
                block_time_ms: None,
                new_payload_ms: None,
                forkchoice_updated_ms: None,
                new_payload_server_latency_us: None,
                persistence_wait_us: None,
                execution_cache_wait_us: None,
                sparse_trie_wait_us: None,
            },
            BlockStats {
                number: 101,
                timestamp_ms: 1_012_000,
                tx_count: 15,
                gas_used: 1_500_000,
                gas_limit: 30_000_000,
                block_time_ms: Some(12000),
                new_payload_ms: None,
                forkchoice_updated_ms: None,
                new_payload_server_latency_us: None,
                persistence_wait_us: None,
                execution_cache_wait_us: None,
                sparse_trie_wait_us: None,
            },
        ];

        let run_stats = RunStats::from_blocks_wall_time(&blocks, Duration::from_secs(2));

        assert_eq!(run_stats.start_block, 100);
        assert_eq!(run_stats.end_block, 101);
        assert_eq!(run_stats.total_blocks, 2);
        assert_eq!(run_stats.total_txs, 25);
        assert_eq!(run_stats.total_gas, 2_500_000);
        assert_eq!(run_stats.duration_ms, 2000);
        assert_eq!(run_stats.avg_blocks_per_second, 1.0);
        assert_eq!(run_stats.avg_tps, 12.5);
        assert_eq!(run_stats.block_time_p50_ms, 12000);
    }

    #[test]
    fn test_run_stats_empty() {
        let run_stats = RunStats::from_blocks_chain_time(&[]);
        assert_eq!(run_stats.start_block, 0);
        assert_eq!(run_stats.total_txs, 0);
        assert_eq!(run_stats.avg_tps, 0.0);

        let run_stats = RunStats::from_blocks_wall_time(&[], Duration::from_secs(1));
        assert_eq!(run_stats.duration_ms, 0);
    }

    #[test]
    fn test_bench_metrics_serde() {
        let metrics = BenchMetrics {
            sent: 100,
            success: 90,
            failed: 10,
            included: 90,
            reverted: 1,
            receipt_pending: 9,
            receipt_errors: 2,
            elapsed: Duration::from_millis(1500),
            latency: Some(LatencyStats {
                min: Duration::from_millis(5),
                max: Duration::from_millis(200),
                mean: Duration::from_millis(50),
                p50: Duration::from_millis(40),
                p95: Duration::from_millis(150),
                p99: Duration::from_millis(180),
            }),
        };

        let json = serde_json::to_string(&metrics).unwrap();
        let parsed: BenchMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.sent, 100);
        assert_eq!(parsed.elapsed, Duration::from_millis(1500));
        assert_eq!(parsed.latency.unwrap().p50, Duration::from_millis(40));
    }

    #[tokio::test]
    async fn test_time_series_metrics() {
        let collector = MetricsCollector::new(RunClock::new());

        collector.record_sent();
        collector.record_success(Duration::from_millis(50));
        collector.record_sent();
        collector.record_failure();

        let ts = collector.time_series().await;

        assert!(!ts.throughput.is_empty());
        assert_eq!(ts.latencies.len(), 1);
        assert_eq!(ts.latencies[0].latency, Duration::from_millis(50));

        let first_second = &ts.throughput[0];
        assert_eq!(first_second.second, 0);
        assert_eq!(first_second.sent, 2);
        assert_eq!(first_second.success, 1);
        assert_eq!(first_second.failed, 1);
    }

    #[tokio::test]
    async fn finalize_then_time_series_retains_aggregated_samples() {
        let collector = MetricsCollector::new(RunClock::new());

        collector.record_sent();
        collector.record_success(Duration::from_millis(20));
        collector.record_sent();
        collector.record_success(Duration::from_millis(10));

        let metrics = collector.finalize().await;
        let ts = collector.time_series().await;

        assert_eq!(metrics.sent, 2);
        assert_eq!(metrics.success, 2);
        assert_eq!(metrics.failed, 0);
        let latency = metrics.latency.unwrap();
        assert_eq!(latency.min, Duration::from_millis(10));
        assert_eq!(latency.max, Duration::from_millis(20));
        assert_eq!(ts.latencies.len(), 2);
        assert_eq!(ts.latencies[0].latency, Duration::from_millis(20));
        assert_eq!(ts.latencies[1].latency, Duration::from_millis(10));
        assert_eq!(ts.throughput.iter().map(|sample| sample.sent).sum::<u64>(), 2);
        assert_eq!(ts.throughput.iter().map(|sample| sample.success).sum::<u64>(), 2);
        assert_eq!(ts.throughput.iter().map(|sample| sample.failed).sum::<u64>(), 0);
    }

    #[tokio::test]
    async fn latencies_can_be_disabled() {
        let collector = MetricsCollector::new_with_latencies(RunClock::new(), false);

        collector.record_sent();
        collector.record_success(Duration::from_millis(20));
        collector.record_sent();
        collector.record_success(Duration::from_millis(10));

        let metrics = collector.finalize().await;
        let ts = collector.time_series().await;

        assert!(metrics.latency.is_none());
        assert!(ts.latencies.is_empty());
        assert_eq!(ts.throughput.iter().map(|sample| sample.sent).sum::<u64>(), 2);
        assert_eq!(ts.throughput.iter().map(|sample| sample.success).sum::<u64>(), 2);
    }

    #[test]
    fn test_time_series_serde() {
        let ts = TimeSeriesMetrics {
            throughput: vec![ThroughputSample { second: 0, sent: 100, success: 90, failed: 10 }],
            latencies: vec![LatencySample { offset_ms: 500, latency: Duration::from_millis(25) }],
        };

        let json = serde_json::to_string(&ts).unwrap();
        let parsed: TimeSeriesMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.throughput.len(), 1);
        assert_eq!(parsed.throughput[0].sent, 100);
        assert_eq!(parsed.latencies.len(), 1);
        assert_eq!(parsed.latencies[0].latency, Duration::from_millis(25));
    }

    fn make_block(number: u64, gas_used: u64, timestamp_ms: u64) -> BlockStats {
        BlockStats {
            number,
            timestamp_ms,
            tx_count: if gas_used > 0 { 100 } else { 1 },
            gas_used,
            gas_limit: 30_000_000,
            block_time_ms: Some(500),
            new_payload_ms: None,
            forkchoice_updated_ms: None,
            new_payload_server_latency_us: None,
            persistence_wait_us: None,
            execution_cache_wait_us: None,
            sparse_trie_wait_us: None,
        }
    }

    #[test]
    fn trim_trailing_empty_removes_suffix() {
        let mut blocks = vec![
            make_block(100, 1_000_000, 1_000_000),
            make_block(101, 2_000_000, 1_000_500),
            make_block(102, 0, 1_000_502),
            make_block(103, 0, 1_000_504),
        ];

        let cutoff = trim_trailing_empty_blocks(&mut blocks);
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks.last().unwrap().number, 101);
        assert_eq!(cutoff, Some(1_000_500));
    }

    #[test]
    fn trim_trailing_empty_preserves_middle_gaps() {
        let mut blocks = vec![
            make_block(100, 1_000_000, 1_000_000),
            make_block(101, 0, 1_000_500), // middle gap
            make_block(102, 2_000_000, 1_001_000),
            make_block(103, 0, 1_001_002), // trailing
        ];

        let cutoff = trim_trailing_empty_blocks(&mut blocks);
        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[1].gas_used, 0); // middle gap preserved
        assert_eq!(blocks.last().unwrap().number, 102);
        assert_eq!(cutoff, Some(1_001_000));
    }

    #[test]
    fn trim_trailing_empty_noop_when_no_trailing() {
        let mut blocks =
            vec![make_block(100, 1_000_000, 1_000_000), make_block(101, 2_000_000, 1_000_500)];

        let cutoff = trim_trailing_empty_blocks(&mut blocks);
        assert_eq!(blocks.len(), 2);
        assert_eq!(cutoff, Some(1_000_500));
    }

    #[test]
    fn trim_trailing_empty_all_empty() {
        let mut blocks = vec![make_block(100, 0, 1_000_000), make_block(101, 0, 1_000_500)];

        let cutoff = trim_trailing_empty_blocks(&mut blocks);
        // No real blocks found — leave unmodified
        assert_eq!(blocks.len(), 2);
        assert_eq!(cutoff, None);
    }

    #[test]
    fn trim_trailing_empty_vec() {
        let mut blocks: Vec<BlockStats> = vec![];
        let cutoff = trim_trailing_empty_blocks(&mut blocks);
        assert!(blocks.is_empty());
        assert_eq!(cutoff, None);
    }
}
