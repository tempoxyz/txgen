//! Core library for the bench tool.
//!
//! Provides shared foundation components:
//! - [`call`] - RPC corpus replay: method allowlist, response digests, accounting
//! - [`source`] - Transaction sources (file, stdin)
//! - [`sender`] - Sending with scheduling key ordering + rate limiting
//! - [`metrics`] - Collection (sent/success/failed counts, timing)
//! - [`reporter`] - Output (console, JSON + NDJSON samples, ClickHouse, Prometheus remote write)

pub mod auth;
pub mod call;
pub mod clickhouse;
pub mod clock;
pub mod composition;
pub mod metrics;
pub mod prometheus;
pub mod prometheus_reporter;
pub mod receipt_clickhouse;
pub mod receipt_metrics;
pub mod receipt_tracker;
pub mod reporter;
pub mod sample;
pub mod scraper;
pub mod sender;
pub mod source;

pub use auth::{RequestAuthProvider, RpcRequestContext, SenderHeaderAuthProvider};
pub use call::{
    digest_response, CallMethod, CallReport, CallRunConfig, ClosedLoopRow, Corpus, CorpusOptions,
    CorpusRecord, CorpusSummary, MethodStats, NodeIdentity, Nondeterministic, OpenLoopRow,
    PhaseStats, ReplayRecorder, ReplayResults, RequestOutcome, RequestStatus, ResponseBytes,
    ResponseKind, ResponseRow, ResponseScanner, ResponseSummary, StatusCounts,
};
pub use clickhouse::ClickHouseClient;
pub use clock::RunClock;
pub use composition::{
    block_composition, BlockComposition, KindComposition, RunComposition, TransactionComposition,
};
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
pub use receipt_clickhouse::{insert_receipt_gas_records, DEFAULT_CLICKHOUSE_RECEIPT_BATCH_SIZE};
pub use receipt_metrics::{
    total_fees_paid, BlockReceiptCollector, ReceiptCollection, ReceiptCollector,
    ReceiptCollectorHandle, ReceiptGasRecord, ReceiptGasSample, ReceiptMetricDistribution,
    ReceiptMetricGroup, ReceiptMetricLabels, ReceiptMetrics, ReceiptMetricsAccumulator,
};
pub use receipt_tracker::ReceiptTracker;
pub use reporter::{
    parse_reporters, ClickHouseConfig, ClickHouseReporter, ConsoleReporter, FinalReport,
    JsonLatency, JsonLatencySample, JsonReport, JsonReporter, JsonTimeSeries, ProgressState,
    Reporter,
};
pub use sample::{Sample, SampleArchive, SampleStore};
pub use scraper::{start_scrapers, SampleCallback, ScraperConfig, ScraperHandle};
pub use sender::{
    LateSigner, RpcEndpoint, RpcReceiptDetails, RpcSubmission, RpcSubmitError,
    RpcSubmitFailureKind, RpcSubmitter, Sender, SenderConfig,
};
pub use source::{FileSource, SourceTx, StdinSource, TxSource};
pub use txgen_core::{GeneratedTx, TxPhase};

/// Drive `future` to completion from synchronous code running on a
/// multi-threaded Tokio runtime, without blocking a runtime worker.
///
/// Errors instead of panicking when called outside such a runtime.
pub(crate) fn block_on<F: std::future::Future>(future: F) -> eyre::Result<F::Output> {
    let rt = tokio::runtime::Handle::try_current()
        .map_err(|_| eyre::eyre!("blocking call requires a Tokio runtime"))?;
    if !matches!(rt.runtime_flavor(), tokio::runtime::RuntimeFlavor::MultiThread) {
        eyre::bail!("blocking call requires a multi-threaded Tokio runtime");
    }
    Ok(tokio::task::block_in_place(|| rt.block_on(future)))
}

#[cfg(test)]
mod tests {
    #[test]
    fn block_on_requires_runtime() {
        assert!(super::block_on(async {}).is_err());
    }

    #[tokio::test]
    async fn block_on_rejects_current_thread_runtime() {
        assert!(super::block_on(async {}).is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn block_on_runs_on_multi_thread_runtime() {
        assert_eq!(super::block_on(async { 7 }).unwrap(), 7);
    }
}
