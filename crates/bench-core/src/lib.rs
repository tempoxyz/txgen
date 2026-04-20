//! Core library for the bench tool.
//!
//! Provides shared foundation components:
//! - [`source`] - Transaction sources (txgen subprocess, file, stdin)
//! - [`sender`] - Sending with scheduling key ordering + rate limiting
//! - [`metrics`] - Collection (sent/success/failed counts, timing)
//! - [`reporter`] - Output (console, JSON, ClickHouse)
//! - [`reth_api`] - reth custom Engine API types (`reth_newPayload`, `reth_forkchoiceUpdated`)

pub mod clock;
pub mod metrics;
pub mod prometheus;
pub mod reporter;
pub mod reth_api;
pub mod sample;
pub mod scraper;
pub mod sender;
pub mod source;

pub use clock::RunClock;
pub use metrics::{
    BenchMetrics, BlockStats, LatencySample, LatencyStats, MetricsCollector, RunStats,
    ThroughputSample, TimeSeriesMetrics, collect_block_stats, compute_latency_stats,
    trim_trailing_empty_blocks,
};
pub use prometheus::parse_prometheus_text;
pub use reporter::{
    ClickHouseConfig, ClickHouseReporter, ConsoleReporter, FinalReport, JsonLatency,
    JsonLatencySample, JsonReport, JsonReporter, JsonThroughputSample, JsonTimeSeries,
    ProgressState, Reporter, parse_reporters,
};
pub use reth_api::{
    DEFAULT_PERSISTENCE_THRESHOLD, RethApi, RethForkchoiceUpdated, RethNewPayloadInput,
    RethPayloadStatus, WaitForPersistence,
};
pub use sample::{Sample, SampleStore};
pub use scraper::{SampleCallback, ScraperConfig, ScraperHandle, start_scraper};
pub use sender::{Sender, SenderConfig};
pub use source::{FileSource, SourceTx, StdinSource, TxSource, TxgenSource};
