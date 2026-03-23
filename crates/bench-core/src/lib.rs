//! Core library for the bench tool.
//!
//! Provides shared foundation components:
//! - [`source`] - Transaction sources (txgen subprocess, file, stdin)
//! - [`sender`] - Sending with scheduling key ordering + rate limiting
//! - [`metrics`] - Collection (sent/success/failed counts, timing)
//! - [`reporter`] - Output (console, JSON, ClickHouse)
//! - [`reth_api`] - reth custom Engine API types (`reth_newPayload`, `reth_forkchoiceUpdated`)
//! - [`reth_testing`] - reth testing RPC types (`testing_buildBlockV1`)
//! - [`historical`] - Historical transaction fetching from archive RPC

pub mod historical;
pub mod metrics;
pub mod reporter;
pub mod reth_api;
pub mod reth_testing;
pub mod sender;
pub mod source;

pub use historical::{HistoricalFetcher, HistoricalFetcherConfig, HistoricalTx};
pub use metrics::{
    BenchMetrics, BlockStats, LatencySample, LatencyStats, MetricsCollector, ReplayBlockStats,
    ReplayRunStats, RunStats, ThroughputSample, TimeSeriesMetrics, collect_block_stats,
    compute_latency_stats,
};
pub use reporter::{
    ClickHouseConfig, ClickHouseReporter, ConsoleReporter, JsonBlockStats, JsonLatency,
    JsonLatencySample, JsonReplayBlockStats, JsonReport, JsonReporter, JsonRunStats,
    JsonThroughputSample, JsonTimeSeries, Reporter, parse_reporters,
};
pub use reth_api::{RethApi, RethForkchoiceUpdated, RethNewPayloadInput, RethPayloadStatus};
pub use reth_testing::{TestingBuildBlockRequestV1, TestingBuildBlockResponse, build_block_v1};
pub use sender::{Sender, SenderConfig};
pub use source::{FileSource, SourceTx, StdinSource, TxSource, TxgenSource};
