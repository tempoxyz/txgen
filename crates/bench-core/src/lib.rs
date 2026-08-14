//! Core library for the bench tool.
//!
//! Provides shared foundation components:
//! - [`source`] - Transaction sources (file, stdin)
//! - [`sender`] - Sending with scheduling key ordering + rate limiting
//! - [`metrics`] - Collection (sent/success/failed counts, timing)
//! - [`reporter`] - Output (console, JSON + NDJSON samples, ClickHouse, Prometheus remote write)

pub mod auth;
pub mod clickhouse;
pub mod clock;
pub mod metrics;
pub mod prometheus;
pub mod prometheus_reporter;
pub mod receipt_clickhouse;
pub mod receipt_metrics;
pub mod reporter;
pub mod sample;
pub mod scraper;
pub mod sender;
pub mod source;

pub use auth::{RequestAuthProvider, RpcRequestContext, SenderHeaderAuthProvider};
pub use clickhouse::ClickHouseClient;
pub use clock::RunClock;
pub use metrics::{
    collect_block_stats, compute_latency_stats, trim_trailing_empty_blocks, BenchMetrics,
    BlockStats, LatencySample, LatencyStats, MetricsCollector, MetricsCollectorOptions, RunStats,
    ThroughputSample, TimeSeriesMetrics,
};
pub use prometheus::parse_prometheus_text;
pub use prometheus_reporter::{
    PrometheusConfig, PrometheusForwarder, PrometheusForwarderHandle, PrometheusForwarderSummary,
    PrometheusReporter,
};
pub use receipt_clickhouse::{
    insert_receipt_gas_records, insert_receipt_gas_records_with_default_batch_size,
    DEFAULT_CLICKHOUSE_RECEIPT_BATCH_SIZE,
};
pub use receipt_metrics::{
    total_fees_paid, BlockReceiptCollector, ReceiptCollection, ReceiptCollector,
    ReceiptCollectorHandle, ReceiptGasRecord, ReceiptGasSample, ReceiptMetricDistribution,
    ReceiptMetricGroup, ReceiptMetricLabels, ReceiptMetrics, ReceiptMetricsAccumulator,
};
pub use reporter::{
    parse_reporters, ClickHouseConfig, ClickHouseReporter, ConsoleReporter, FinalReport,
    JsonLatency, JsonLatencySample, JsonReport, JsonReporter, JsonTimeSeries, ProgressState,
    Reporter,
};
pub use sample::{Sample, SampleArchive, SampleStore};
pub use scraper::{start_scrapers, SampleCallback, ScraperConfig, ScraperHandle};
pub use sender::{
    RpcEndpoint, RpcReceiptDetails, RpcSubmission, RpcSubmitError, RpcSubmitFailureKind,
    RpcSubmitter, Sender, SenderConfig,
};
pub use source::{FileSource, SourceTx, StdinSource, TxSource};
pub use txgen_core::{GeneratedTx, TxPhase};
