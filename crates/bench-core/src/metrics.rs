//! Metrics collection for bench.
//!
//! Collects statistics about transaction sending:
//! - Sent/success/failed counts
//! - Timing (latencies, throughput)

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Metrics collected during a benchmark run.
#[derive(Debug, Clone)]
pub struct BenchMetrics {
    /// Total transactions sent.
    pub sent: u64,
    /// Successful transactions (accepted by RPC).
    pub success: u64,
    /// Failed transactions (rejected by RPC or network error).
    pub failed: u64,
    /// Total elapsed time.
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
#[derive(Debug, Clone, Default)]
pub struct LatencyStats {
    /// Minimum latency observed.
    pub min: Duration,
    /// Maximum latency observed.
    pub max: Duration,
    /// Mean latency.
    pub mean: Duration,
    /// P50 latency.
    pub p50: Duration,
    /// P95 latency.
    pub p95: Duration,
    /// P99 latency.
    pub p99: Duration,
}

impl LatencyStats {
    /// Compute latency stats from a sorted slice of durations.
    fn from_sorted(samples: &[Duration]) -> Self {
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
            p50: samples[n * 50 / 100],
            p95: samples[n * 95 / 100],
            p99: samples[n * 99 / 100],
        }
    }
}

/// Atomic counters for concurrent metrics collection.
#[derive(Debug, Default)]
pub struct MetricsCollector {
    sent: AtomicU64,
    success: AtomicU64,
    failed: AtomicU64,
    start: RwLock<Option<Instant>>,
    latencies: RwLock<Vec<Duration>>,
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

    /// Record a sent transaction.
    pub fn record_sent(&self) {
        self.sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful transaction with its latency.
    pub async fn record_success(&self, latency: Duration) {
        self.success.fetch_add(1, Ordering::Relaxed);
        self.latencies.write().await.push(latency);
    }

    /// Record a failed transaction.
    pub fn record_failure(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
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
        latencies.sort();

        BenchMetrics {
            sent: self.sent.load(Ordering::Relaxed),
            success: self.success.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            elapsed,
            latency: LatencyStats::from_sorted(&latencies),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_collection() {
        let collector = MetricsCollector::new();
        collector.start().await;

        collector.record_sent();
        collector.record_sent();
        collector.record_success(Duration::from_millis(10)).await;
        collector.record_failure();

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
}
