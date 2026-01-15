//! Metrics collection for bench.
//!
//! Collects statistics about transaction sending:
//! - Sent/success/failed counts
//! - Timing (latencies, throughput)
//! - Block-level statistics (post-run)

use alloy_eips::BlockNumberOrTag;
use alloy_provider::Provider;

use eyre::{Context, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Metrics collected during a benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchMetrics {
    /// Total transactions sent.
    pub sent: u64,
    /// Successful transactions (accepted by RPC).
    pub success: u64,
    /// Failed transactions (rejected by RPC or network error).
    pub failed: u64,
    /// Total elapsed time.
    #[serde(with = "duration_serde")]
    pub elapsed: Duration,
    /// Latency statistics.
    pub latency: LatencyStats,
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
    /// Individual latency samples with timestamps.
    pub latencies: Vec<LatencySample>,
}

/// Block-level statistics collected post-run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockStats {
    /// Block number.
    pub number: u64,
    /// Block timestamp (unix seconds).
    pub timestamp: u64,
    /// Total transactions in the block.
    pub tx_count: usize,
    /// Successful transactions in the block.
    pub success_count: usize,
    /// Gas used by the block.
    pub gas_used: u64,
    /// Gas limit of the block.
    pub gas_limit: u64,
    /// Time since previous block in milliseconds.
    pub block_time_ms: Option<u64>,
}

/// Statistics for a replayed block via Engine API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBlockStats {
    /// Block number.
    pub number: u64,
    /// Block timestamp (unix seconds).
    pub timestamp: u64,
    /// Total transactions in the block.
    pub tx_count: usize,
    /// Gas used by the block.
    pub gas_used: u64,
    /// Gas limit of the block.
    pub gas_limit: u64,
    /// newPayload latency in milliseconds.
    pub new_payload_ms: u64,
    /// forkchoiceUpdated latency in milliseconds.
    pub fcu_ms: u64,
    /// Total execution latency in milliseconds (newPayload + FCU).
    pub total_latency_ms: u64,
    /// Payload status from newPayload response.
    pub payload_status: String,
}

/// Run summary statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunStats {
    /// Starting block number.
    pub start_block: u64,
    /// Ending block number.
    pub end_block: u64,
    /// Total transactions across all blocks.
    pub total_txs: u64,
    /// Total gas used across all blocks.
    pub total_gas: u64,
    /// Total duration in milliseconds.
    pub duration_ms: u64,
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
    /// Compute run stats from a slice of block stats.
    pub fn from_blocks(blocks: &[BlockStats]) -> Self {
        if blocks.is_empty() {
            return Self {
                start_block: 0,
                end_block: 0,
                total_txs: 0,
                total_gas: 0,
                duration_ms: 0,
                avg_tps: 0.0,
                avg_gas_per_second: 0.0,
                block_time_p50_ms: 0,
                block_time_p95_ms: 0,
                block_time_p99_ms: 0,
            };
        }

        let start_block = blocks.first().map(|b| b.number).unwrap_or(0);
        let end_block = blocks.last().map(|b| b.number).unwrap_or(0);

        let total_txs: u64 = blocks.iter().map(|b| b.tx_count as u64).sum();
        let total_gas: u64 = blocks.iter().map(|b| b.gas_used).sum();

        let start_ts = blocks.first().map(|b| b.timestamp).unwrap_or(0);
        let end_ts = blocks.last().map(|b| b.timestamp).unwrap_or(0);
        let duration_secs = end_ts.saturating_sub(start_ts);
        let duration_ms = duration_secs * 1000;

        let avg_tps = if duration_secs > 0 {
            total_txs as f64 / duration_secs as f64
        } else {
            0.0
        };

        let avg_gas_per_second = if duration_secs > 0 {
            total_gas as f64 / duration_secs as f64
        } else {
            0.0
        };

        let mut block_times: Vec<u64> = blocks.iter().filter_map(|b| b.block_time_ms).collect();
        block_times.sort();

        let block_time_p50_ms = percentile_u64(&block_times, 50);
        let block_time_p95_ms = percentile_u64(&block_times, 95);
        let block_time_p99_ms = percentile_u64(&block_times, 99);

        Self {
            start_block,
            end_block,
            total_txs,
            total_gas,
            duration_ms,
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
pub async fn collect_block_stats<P: Provider>(
    provider: &P,
    start_block: u64,
    end_block: u64,
) -> Result<Vec<BlockStats>> {
    let mut stats = Vec::with_capacity((end_block - start_block + 1) as usize);
    let mut prev_timestamp: Option<u64> = None;

    for number in start_block..=end_block {
        let block = provider
            .get_block(BlockNumberOrTag::Number(number).into())
            .await
            .wrap_err_with(|| format!("failed to fetch block {number}"))?
            .ok_or_else(|| eyre::eyre!("block {number} not found"))?;

        let receipts = provider
            .get_block_receipts(BlockNumberOrTag::Number(number).into())
            .await
            .wrap_err_with(|| format!("failed to fetch receipts for block {number}"))?
            .unwrap_or_default();

        let block_time_ms =
            prev_timestamp.map(|prev| block.header.timestamp.saturating_sub(prev) * 1000);
        prev_timestamp = Some(block.header.timestamp);

        let success_count = receipts.iter().filter(|r| r.status()).count();

        stats.push(BlockStats {
            number,
            timestamp: block.header.timestamp,
            tx_count: receipts.len(),
            success_count,
            gas_used: block.header.gas_used,
            gas_limit: block.header.gas_limit,
            block_time_ms,
        });
    }

    Ok(stats)
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

/// Atomic counters for concurrent metrics collection.
#[derive(Debug, Default)]
pub struct MetricsCollector {
    sent: AtomicU64,
    success: AtomicU64,
    failed: AtomicU64,
    start: RwLock<Option<Instant>>,
    latencies: RwLock<Vec<TimestampedLatency>>,
    events: RwLock<Vec<TimestampedEvent>>,
}

impl MetricsCollector {
    /// Create a new metrics collector.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Mark the start of the benchmark.
    pub async fn start(&self) {
        let mut start = self.start.write().await;
        *start = Some(Instant::now());
    }

    /// Get the elapsed time since start.
    async fn elapsed(&self) -> Duration {
        let start = self.start.read().await;
        start.map_or(Duration::ZERO, |s| s.elapsed())
    }

    /// Record a sent transaction.
    pub async fn record_sent(&self) {
        self.sent.fetch_add(1, Ordering::Relaxed);
        let offset = self.elapsed().await;
        self.events.write().await.push(TimestampedEvent {
            offset,
            event: TxEvent::Sent,
        });
    }

    /// Record a successful transaction with its latency.
    pub async fn record_success(&self, latency: Duration) {
        self.success.fetch_add(1, Ordering::Relaxed);
        let offset = self.elapsed().await;
        self.latencies
            .write()
            .await
            .push(TimestampedLatency { offset, latency });
        self.events.write().await.push(TimestampedEvent {
            offset,
            event: TxEvent::Success,
        });
    }

    /// Record a failed transaction.
    pub async fn record_failure(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
        let offset = self.elapsed().await;
        self.events.write().await.push(TimestampedEvent {
            offset,
            event: TxEvent::Failed,
        });
    }

    /// Get the current counts (sent, success, failed).
    pub fn counts(&self) -> (u64, u64, u64) {
        (
            self.sent.load(Ordering::Relaxed),
            self.success.load(Ordering::Relaxed),
            self.failed.load(Ordering::Relaxed),
        )
    }

    /// Compute final metrics.
    pub async fn finalize(&self) -> BenchMetrics {
        let start = self.start.read().await;
        let elapsed = start.map_or(Duration::ZERO, |s| s.elapsed());

        let mut latencies = self.latencies.write().await;
        latencies.sort_by_key(|l| l.latency);
        let durations: Vec<Duration> = latencies.iter().map(|l| l.latency).collect();

        BenchMetrics {
            sent: self.sent.load(Ordering::Relaxed),
            success: self.success.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            elapsed,
            latency: LatencyStats::from_sorted(&durations),
        }
    }

    /// Extract time-series metrics for graphing.
    pub async fn time_series(&self) -> TimeSeriesMetrics {
        let start = self.start.read().await;
        let total_elapsed = start.map_or(Duration::ZERO, |s| s.elapsed());
        let total_seconds = total_elapsed.as_secs() + 1;

        let events = self.events.read().await;
        let latencies = self.latencies.read().await;

        let mut throughput = Vec::with_capacity(total_seconds as usize);
        for second in 0..total_seconds {
            let start_offset = Duration::from_secs(second);
            let end_offset = Duration::from_secs(second + 1);

            let mut sent = 0u64;
            let mut success = 0u64;
            let mut failed = 0u64;

            for event in events.iter() {
                if event.offset >= start_offset && event.offset < end_offset {
                    match event.event {
                        TxEvent::Sent => sent += 1,
                        TxEvent::Success => success += 1,
                        TxEvent::Failed => failed += 1,
                    }
                }
            }

            throughput.push(ThroughputSample {
                second,
                sent,
                success,
                failed,
            });
        }

        let latency_samples: Vec<LatencySample> = latencies
            .iter()
            .map(|l| LatencySample {
                offset_ms: l.offset.as_millis() as u64,
                latency: l.latency,
            })
            .collect();

        TimeSeriesMetrics {
            throughput,
            latencies: latency_samples,
        }
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
        DurationRepr {
            secs: duration.as_secs(),
            nanos: duration.subsec_nanos(),
        }
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
        let collector = MetricsCollector::new();
        collector.start().await;

        collector.record_sent().await;
        collector.record_sent().await;
        collector.record_success(Duration::from_millis(10)).await;
        collector.record_failure().await;

        let (sent, success, failed) = collector.counts();
        assert_eq!(sent, 2);
        assert_eq!(success, 1);
        assert_eq!(failed, 1);
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
            elapsed: Duration::from_secs(10),
            latency: LatencyStats::default(),
        };

        assert!((metrics.tps() - 10.0).abs() < 0.001);
        assert!((metrics.success_rate() - 90.0).abs() < 0.001);
    }

    #[test]
    fn test_run_stats_from_blocks() {
        let blocks = vec![
            BlockStats {
                number: 100,
                timestamp: 1000,
                tx_count: 10,
                success_count: 9,
                gas_used: 1_000_000,
                gas_limit: 30_000_000,
                block_time_ms: None,
            },
            BlockStats {
                number: 101,
                timestamp: 1012,
                tx_count: 15,
                success_count: 15,
                gas_used: 1_500_000,
                gas_limit: 30_000_000,
                block_time_ms: Some(12000),
            },
            BlockStats {
                number: 102,
                timestamp: 1024,
                tx_count: 20,
                success_count: 18,
                gas_used: 2_000_000,
                gas_limit: 30_000_000,
                block_time_ms: Some(12000),
            },
        ];

        let run_stats = RunStats::from_blocks(&blocks);

        assert_eq!(run_stats.start_block, 100);
        assert_eq!(run_stats.end_block, 102);
        assert_eq!(run_stats.total_txs, 45);
        assert_eq!(run_stats.total_gas, 4_500_000);
        assert_eq!(run_stats.duration_ms, 24000);
    }

    #[test]
    fn test_run_stats_empty() {
        let run_stats = RunStats::from_blocks(&[]);
        assert_eq!(run_stats.start_block, 0);
        assert_eq!(run_stats.total_txs, 0);
        assert_eq!(run_stats.avg_tps, 0.0);
    }

    #[test]
    fn test_bench_metrics_serde() {
        let metrics = BenchMetrics {
            sent: 100,
            success: 90,
            failed: 10,
            elapsed: Duration::from_millis(1500),
            latency: LatencyStats {
                min: Duration::from_millis(5),
                max: Duration::from_millis(200),
                mean: Duration::from_millis(50),
                p50: Duration::from_millis(40),
                p95: Duration::from_millis(150),
                p99: Duration::from_millis(180),
            },
        };

        let json = serde_json::to_string(&metrics).unwrap();
        let parsed: BenchMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.sent, 100);
        assert_eq!(parsed.elapsed, Duration::from_millis(1500));
        assert_eq!(parsed.latency.p50, Duration::from_millis(40));
    }

    #[tokio::test]
    async fn test_time_series_metrics() {
        let collector = MetricsCollector::new();
        collector.start().await;

        collector.record_sent().await;
        collector.record_success(Duration::from_millis(50)).await;
        collector.record_sent().await;
        collector.record_failure().await;

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

    #[test]
    fn test_time_series_serde() {
        let ts = TimeSeriesMetrics {
            throughput: vec![ThroughputSample {
                second: 0,
                sent: 100,
                success: 90,
                failed: 10,
            }],
            latencies: vec![LatencySample {
                offset_ms: 500,
                latency: Duration::from_millis(25),
            }],
        };

        let json = serde_json::to_string(&ts).unwrap();
        let parsed: TimeSeriesMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.throughput.len(), 1);
        assert_eq!(parsed.throughput[0].sent, 100);
        assert_eq!(parsed.latencies.len(), 1);
        assert_eq!(parsed.latencies[0].latency, Duration::from_millis(25));
    }
}
