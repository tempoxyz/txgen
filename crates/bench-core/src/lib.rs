//! Core library for the bench tool.
//!
//! Provides shared foundation components:
//! - [`source`] - Transaction sources (txgen subprocess, file, stdin)
//! - [`sender`] - Sending with scheduling key ordering + rate limiting
//! - [`metrics`] - Collection (sent/success/failed counts, timing)
//! - [`reporter`] - Output (console, JSON, ClickHouse)

pub mod metrics;
pub mod reporter;
pub mod sender;
pub mod source;

pub use metrics::{
    BenchMetrics, BlockStats, LatencySample, LatencyStats, MetricsCollector, RunStats,
    ThroughputSample, TimeSeriesMetrics, collect_block_stats,
};
pub use reporter::{
    ClickHouseConfig, ClickHouseReporter, ConsoleReporter, JsonBlockStats, JsonLatency,
    JsonLatencySample, JsonReport, JsonReporter, JsonRunStats, JsonThroughputSample,
    JsonTimeSeries, Reporter,
};
pub use sender::{Sender, SenderConfig};
pub use source::{FileSource, SourceTx, StdinSource, TxSource, TxgenSource};
