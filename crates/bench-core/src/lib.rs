//! Core library for the bench tool.
//!
//! Provides shared foundation components:
//! - [`source`] - Transaction sources (file, stdin)
//! - [`sender`] - Sending with scheduling key ordering + rate limiting
//! - [`metrics`] - Collection (sent/success/failed counts, timing)
//! - [`reporter`] - Output (console, JSON + NDJSON samples, ClickHouse, Prometheus remote write)
//! - [`reth_api`] - reth custom Engine API types (`reth_newPayload`, `reth_forkchoiceUpdated`)

pub mod clock;
pub mod metrics;
pub mod prometheus;
pub mod prometheus_reporter;
pub mod reporter;
pub mod reth_api;
pub mod sample;
pub mod scraper;
pub mod sender;
pub mod source;

pub use clock::RunClock;
pub use metrics::{
    collect_block_stats, compute_latency_stats, trim_trailing_empty_blocks, BenchMetrics,
    BlockStats, LatencySample, LatencyStats, MetricsCollector, RunStats, ThroughputSample,
    TimeSeriesMetrics,
};
pub use prometheus::parse_prometheus_text;
pub use prometheus_reporter::{PrometheusConfig, PrometheusReporter};
pub use reporter::{
    parse_reporters, ClickHouseConfig, ClickHouseReporter, ConsoleReporter, FinalReport,
    JsonLatency, JsonLatencySample, JsonReport, JsonReporter, JsonTimeSeries, ProgressState,
    Reporter,
};
pub use reth_api::{
    BigBlockData, RethApi, RethForkchoiceUpdated, RethNewPayloadInput, RethPayloadStatus,
    WaitForPersistence, DEFAULT_PERSISTENCE_THRESHOLD,
};
pub use sample::{Sample, SampleStore};
pub use scraper::{start_scrapers, SampleCallback, ScraperConfig, ScraperHandle};
pub use sender::{Sender, SenderConfig};
pub use source::{FileSource, SourceTx, StdinSource, TxSource};
pub use txgen_core::{GeneratedTx, TxPhase};
