//! Reporters for benchmark results.
//!
//! Output formats:
//! - Console (human-readable)
//! - JSON (machine-readable)
//! - ClickHouse (for time-series storage)

use crate::{
    metrics::{BenchMetrics, BlockStats, RunStats, ThroughputSample, TimeSeriesMetrics},
    sample::Sample,
};
use eyre::{bail, Context, Result};
use std::{collections::HashMap, io::Write, path::Path};

/// Unified final report passed to reporters at finalization.
///
/// Contains both typed aggregates (for console/JSON reporters) and
/// the raw unified sample stream (for TSDB reporters).
#[derive(Debug, Default)]
pub struct FinalReport {
    /// User-provided metadata key/value pairs.
    pub metadata: HashMap<String, String>,
    /// Typed transaction metrics (send mode only).
    pub bench_metrics: Option<BenchMetrics>,
    /// Per-second throughput + latency time-series (send mode only).
    pub time_series: Option<TimeSeriesMetrics>,
    /// Block-level run summary.
    pub run_stats: Option<RunStats>,
    /// Unified time-series samples (internal + node).
    pub samples: Vec<Sample>,
    /// Per-block statistics.
    pub blocks: Vec<BlockStats>,
}

impl FinalReport {
    /// Merge run-level labels into all samples.
    ///
    /// Labels from `extra` are added to each sample. If a sample already
    /// has a label with the same key, the existing (node) value wins.
    pub fn apply_labels(&mut self, extra: &HashMap<String, String>) {
        if extra.is_empty() {
            return;
        }
        for sample in &mut self.samples {
            for (k, v) in extra {
                sample.labels.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
    }
}

/// Snapshot of progress state passed to reporters.
pub struct ProgressState {
    /// Total items submitted.
    pub sent: u64,
    /// Items that received a successful response.
    pub success: u64,
    /// Items that failed.
    pub failed: u64,
    /// Elapsed time since benchmark start.
    pub elapsed: std::time::Duration,
    /// Configured `--max-concurrent` limit (0 = not applicable).
    pub max_concurrent: usize,
    /// Configured target rate (`None` = unlimited).
    pub target_tps: Option<u64>,
    /// Display unit for items (e.g. `"tx"`, `"block"`).
    pub unit: &'static str,
}

impl ProgressState {
    /// Number of transactions currently in flight (sent but not yet resolved).
    pub fn inflight(&self) -> u64 {
        self.sent.saturating_sub(self.success + self.failed)
    }

    /// Actual send rate in items per second.
    pub fn actual_rate(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs > 0.0 {
            self.sent as f64 / secs
        } else {
            0.0
        }
    }
}

/// Reporter trait for outputting benchmark results.
pub trait Reporter: Send {
    /// Called periodically during send with current progress.
    fn on_progress(&mut self, _state: &ProgressState) -> Result<()> {
        Ok(())
    }

    /// Called for each block during block stats collection.
    fn on_block(&mut self, _block: &BlockStats) -> Result<()> {
        Ok(())
    }

    /// Finalize and output results from the unified [`FinalReport`].
    fn finalize(&mut self, report: &FinalReport) -> Result<()>;
}

/// Console reporter with human-readable output.
pub struct ConsoleReporter<W: Write + Send = Box<dyn Write + Send>> {
    writer: W,
    show_progress: bool,
}

impl ConsoleReporter {
    /// Create a new console reporter writing to stdout.
    pub fn stdout(show_progress: bool) -> Self {
        Self { writer: Box::new(std::io::stdout()), show_progress }
    }

    /// Create a new console reporter writing to stderr.
    pub fn stderr(show_progress: bool) -> Self {
        Self { writer: Box::new(std::io::stderr()), show_progress }
    }
}

impl<W: Write + Send> ConsoleReporter<W> {
    /// Create a new console reporter with a custom writer.
    pub fn new(writer: W, show_progress: bool) -> Self {
        Self { writer, show_progress }
    }
}

impl<W: Write + Send> Reporter for ConsoleReporter<W> {
    fn on_progress(&mut self, state: &ProgressState) -> Result<()> {
        if self.show_progress {
            let rate = state.actual_rate();

            write!(
                self.writer,
                "\r  Sent: {} | OK: {} | Fail: {}",
                state.sent, state.success, state.failed,
            )?;

            if state.max_concurrent > 0 {
                let inflight = state.inflight();
                write!(self.writer, " | Inflight: {}/{}", inflight, state.max_concurrent)?;
            }

            write!(self.writer, " | Rate: {:.0}", rate)?;

            if let Some(target) = state.target_tps {
                write!(self.writer, "/{}", target)?;
            }

            write!(self.writer, " {}/s", state.unit)?;
            self.writer.flush()?;
        }
        Ok(())
    }

    fn on_block(&mut self, block: &BlockStats) -> Result<()> {
        if self.show_progress {
            writeln!(
                self.writer,
                "\r  Block {}: {} txs, {} gas, {}ms",
                block.number,
                block.tx_count,
                block.gas_used,
                block.block_time_ms.unwrap_or(0)
            )?;
        }
        Ok(())
    }

    fn finalize(&mut self, report: &FinalReport) -> Result<()> {
        let has_send_metrics = report.bench_metrics.is_some();
        let has_block_data = report.run_stats.is_some();

        if !has_send_metrics && !has_block_data {
            return Ok(());
        }

        writeln!(self.writer)?;
        writeln!(self.writer, "═══════════════════════════════════════")?;
        writeln!(self.writer, "              Benchmark Results")?;
        writeln!(self.writer, "═══════════════════════════════════════")?;

        if let Some(metrics) = &report.bench_metrics {
            writeln!(self.writer)?;
            writeln!(self.writer, "  Total Sent:      {:>10}", metrics.sent)?;
            writeln!(self.writer, "  Successful:      {:>10}", metrics.success)?;
            writeln!(self.writer, "  Failed:          {:>10}", metrics.failed)?;
            writeln!(self.writer)?;
            writeln!(self.writer, "  Duration:        {:>10.2}s", metrics.elapsed.as_secs_f64())?;
            writeln!(self.writer, "  Throughput:      {:>10.2} tx/s", metrics.tps())?;
            writeln!(self.writer, "  Success Rate:    {:>10.1}%", metrics.success_rate())?;
            writeln!(self.writer)?;
            writeln!(self.writer, "  Latency:")?;
            writeln!(
                self.writer,
                "    Min:           {:>10.2}ms",
                metrics.latency.min.as_secs_f64() * 1000.0
            )?;
            writeln!(
                self.writer,
                "    Max:           {:>10.2}ms",
                metrics.latency.max.as_secs_f64() * 1000.0
            )?;
            writeln!(
                self.writer,
                "    Mean:          {:>10.2}ms",
                metrics.latency.mean.as_secs_f64() * 1000.0
            )?;
            writeln!(
                self.writer,
                "    P50:           {:>10.2}ms",
                metrics.latency.p50.as_secs_f64() * 1000.0
            )?;
            writeln!(
                self.writer,
                "    P95:           {:>10.2}ms",
                metrics.latency.p95.as_secs_f64() * 1000.0
            )?;
            writeln!(
                self.writer,
                "    P99:           {:>10.2}ms",
                metrics.latency.p99.as_secs_f64() * 1000.0
            )?;
        }

        if let Some(run) = &report.run_stats {
            writeln!(self.writer)?;
            writeln!(self.writer, "  Blocks:")?;
            writeln!(
                self.writer,
                "    Range:         {:>10} - {}",
                run.start_block, run.end_block
            )?;
            writeln!(self.writer, "    Count:         {:>10}", run.total_blocks)?;
            writeln!(self.writer, "    Total Txs:     {:>10}", run.total_txs)?;
            writeln!(self.writer, "    Avg TPS:       {:>10.2}", run.avg_tps)?;
            writeln!(self.writer, "    Blocks/s:      {:>10.2}", run.avg_blocks_per_second)?;
            writeln!(self.writer, "    Avg Gas/s:     {:>10.2}", run.avg_gas_per_second)?;
            writeln!(self.writer, "    Block Time P50:{:>10}ms", run.block_time_p50_ms)?;
            writeln!(self.writer, "    Block Time P95:{:>10}ms", run.block_time_p95_ms)?;
            writeln!(self.writer, "    Block Time P99:{:>10}ms", run.block_time_p99_ms)?;
        }

        writeln!(self.writer)?;
        writeln!(self.writer, "═══════════════════════════════════════")?;

        Ok(())
    }
}

/// JSON output format.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct JsonReport {
    /// Total transactions sent (send mode).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sent: Option<u64>,
    /// Successful transactions (send mode).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub success: Option<u64>,
    /// Failed transactions (send mode).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed: Option<u64>,
    /// Elapsed time in seconds (send mode).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed_secs: Option<f64>,
    /// Transactions per second (send mode).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tps: Option<f64>,
    /// Success rate percentage (send mode).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub success_rate: Option<f64>,
    /// Latency statistics (send mode).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency: Option<JsonLatency>,
    /// Time-series data for graphing (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_series: Option<JsonTimeSeries>,
    /// Block-level statistics (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocks: Option<Vec<BlockStats>>,
    /// Run summary statistics (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_stats: Option<RunStats>,
    /// User-provided metadata key/value pairs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,
    /// Unified time-series samples (internal + node metrics).
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub samples: Vec<Sample>,
}

/// Latency statistics in JSON format.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct JsonLatency {
    /// Minimum latency in milliseconds.
    pub min_ms: f64,
    /// Maximum latency in milliseconds.
    pub max_ms: f64,
    /// Mean latency in milliseconds.
    pub mean_ms: f64,
    /// P50 latency in milliseconds.
    pub p50_ms: f64,
    /// P95 latency in milliseconds.
    pub p95_ms: f64,
    /// P99 latency in milliseconds.
    pub p99_ms: f64,
}

/// Time-series data in JSON format.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct JsonTimeSeries {
    /// Per-second throughput samples.
    pub throughput: Vec<ThroughputSample>,
    /// Individual latency samples.
    pub latencies: Vec<JsonLatencySample>,
}

/// Individual latency sample.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct JsonLatencySample {
    /// Offset from start in milliseconds.
    pub offset_ms: u64,
    /// Latency in milliseconds.
    pub latency_ms: f64,
}

/// JSON reporter for machine-readable output.
pub struct JsonReporter<W: Write + Send = Box<dyn Write + Send>> {
    writer: W,
}

impl JsonReporter {
    /// Create a JSON reporter writing to stdout.
    pub fn stdout() -> Self {
        Self { writer: Box::new(std::io::stdout()) }
    }

    /// Create a JSON reporter writing to a file.
    pub fn file(path: &Path) -> Result<JsonReporter<std::io::BufWriter<std::fs::File>>> {
        let file = std::fs::File::create(path).context("failed to create output file")?;
        Ok(JsonReporter { writer: std::io::BufWriter::new(file) })
    }
}

impl<W: Write + Send> JsonReporter<W> {
    /// Create a new JSON reporter with a custom writer.
    pub fn new(writer: W) -> Self {
        Self { writer }
    }
}

impl<W: Write + Send> Reporter for JsonReporter<W> {
    fn finalize(&mut self, report: &FinalReport) -> Result<()> {
        let has_data = report.bench_metrics.is_some() ||
            !report.blocks.is_empty() ||
            !report.samples.is_empty();
        if !has_data {
            return Ok(());
        }

        let (sent, success, failed, elapsed_secs, tps, success_rate, latency, time_series) =
            if let Some(metrics) = &report.bench_metrics {
                let ts = report.time_series.as_ref().map(|ts| JsonTimeSeries {
                    throughput: ts.throughput.to_vec(),
                    latencies: ts
                        .latencies
                        .iter()
                        .map(|l| JsonLatencySample {
                            offset_ms: l.offset_ms,
                            latency_ms: l.latency.as_secs_f64() * 1000.0,
                        })
                        .collect(),
                });

                (
                    Some(metrics.sent),
                    Some(metrics.success),
                    Some(metrics.failed),
                    Some(metrics.elapsed.as_secs_f64()),
                    Some(metrics.tps()),
                    Some(metrics.success_rate()),
                    Some(JsonLatency {
                        min_ms: metrics.latency.min.as_secs_f64() * 1000.0,
                        max_ms: metrics.latency.max.as_secs_f64() * 1000.0,
                        mean_ms: metrics.latency.mean.as_secs_f64() * 1000.0,
                        p50_ms: metrics.latency.p50.as_secs_f64() * 1000.0,
                        p95_ms: metrics.latency.p95.as_secs_f64() * 1000.0,
                        p99_ms: metrics.latency.p99.as_secs_f64() * 1000.0,
                    }),
                    ts,
                )
            } else {
                (None, None, None, None, None, None, None, None)
            };

        let blocks = if report.blocks.is_empty() { None } else { Some(report.blocks.clone()) };

        let metadata =
            if report.metadata.is_empty() { None } else { Some(report.metadata.clone()) };

        let json_report = JsonReport {
            sent,
            success,
            failed,
            elapsed_secs,
            tps,
            success_rate,
            latency,
            time_series,
            blocks,
            run_stats: report.run_stats.clone(),
            metadata,
            samples: report.samples.clone(),
        };

        serde_json::to_writer_pretty(&mut self.writer, &json_report)?;
        writeln!(self.writer)?;

        Ok(())
    }
}

/// ClickHouse reporter configuration.
#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    /// ClickHouse HTTP endpoint (e.g. `https://host:8443`).
    pub url: String,
    /// Database name (from `CLICKHOUSE_DATABASE`, default: `default`).
    pub database: String,
    /// ClickHouse user (from `CLICKHOUSE_USER`).
    pub user: Option<String>,
    /// ClickHouse password (from `CLICKHOUSE_PASSWORD`).
    pub password: Option<String>,
    /// Run identifier.
    pub run_id: uuid::Uuid,
    /// Benchmark start time.
    pub started_at: std::time::SystemTime,
    /// Scenario name (from metadata `scenario`).
    pub scenario_name: String,
    /// Platform: `ethereum` or `tempo` (from metadata `platform`).
    pub platform: String,
    /// Benchmark mode: `send` or `send-blocks`.
    pub mode: String,
    /// Node git SHA (from metadata `git-sha`).
    pub git_sha: String,
    /// Node git ref (from metadata `git-ref`).
    pub git_ref: String,
    /// Config key-value pairs.
    pub config: HashMap<String, String>,
    /// Additional metadata key-value pairs.
    pub metadata: HashMap<String, String>,
}

/// Required metadata keys for the ClickHouse reporter.
const REQUIRED_METADATA: &[&str] = &["scenario", "platform", "git-sha", "git-ref"];

impl ClickHouseConfig {
    /// Create a ClickHouse config from the reporter URL and user metadata.
    ///
    /// Extracts required fields (`scenario`, `platform`, `git-sha`, `git-ref`)
    /// from `metadata` and returns an error if any are missing.
    ///
    /// `mode` is the bench subcommand (`send`, `send-blocks`).
    pub fn from_metadata(
        url: &str,
        mode: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<Self> {
        let missing: Vec<&str> =
            REQUIRED_METADATA.iter().filter(|k| !metadata.contains_key(**k)).copied().collect();
        if !missing.is_empty() {
            bail!(
                "ClickHouse reporter requires metadata: {}. Use -m key=value for each.",
                missing.join(", ")
            );
        }

        let database =
            std::env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".to_string());
        let user = std::env::var("CLICKHOUSE_USER").ok();
        let password = std::env::var("CLICKHOUSE_PASSWORD").ok();

        // Separate config-like keys from remaining metadata.
        let config_keys = ["tps", "max_concurrent", "chain_id", "scrape_interval_ms"];
        let mut config = HashMap::new();
        let mut remaining_metadata = HashMap::new();

        for (k, v) in metadata {
            if config_keys.contains(&k.as_str()) {
                config.insert(k.clone(), v.clone());
            } else if !REQUIRED_METADATA.contains(&k.as_str()) {
                remaining_metadata.insert(k.clone(), v.clone());
            }
        }

        Ok(Self {
            url: url.to_string(),
            database,
            user,
            password,
            run_id: uuid::Uuid::new_v4(),
            started_at: std::time::SystemTime::now(),
            scenario_name: metadata["scenario"].clone(),
            platform: metadata["platform"].clone(),
            mode: mode.to_string(),
            git_sha: metadata["git-sha"].clone(),
            git_ref: metadata["git-ref"].clone(),
            config,
            metadata: remaining_metadata,
        })
    }
}

/// ClickHouse reporter for benchmark result storage.
///
/// Inserts into three tables:
/// - `txgen_runs` — run metadata
/// - `txgen_blocks` — per-block chain facts
/// - `txgen_metric_samples` — point-in-time metric snapshots
pub struct ClickHouseReporter {
    config: ClickHouseConfig,
    client: reqwest::Client,
}

impl ClickHouseReporter {
    /// Create a new ClickHouse reporter.
    pub fn new(config: ClickHouseConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .context("failed to create HTTP client")?;

        tracing::info!(
            run_id = %config.run_id,
            scenario = %config.scenario_name,
            platform = %config.platform,
            mode = %config.mode,
            url = %config.url,
            database = %config.database,
            "ClickHouse reporter initialized"
        );

        Ok(Self { config, client })
    }

    /// Insert rows into a table using `FORMAT JSONEachRow`.
    fn insert_rows<T: serde::Serialize>(&self, table: &str, rows: &[T]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut body = String::new();
        for row in rows {
            // SAFETY: serialization of known structs should not fail
            body.push_str(&serde_json::to_string(row).unwrap());
            body.push('\n');
        }

        let query = format!("INSERT INTO {}.{} FORMAT JSONEachRow", self.config.database, table);
        let url = format!("{}/?query={}", self.config.url, urlencoding::encode(&query));

        let rt = tokio::runtime::Handle::current();
        let mut req = self.client.post(&url).header("Content-Type", "application/json");
        if let Some(ref user) = self.config.user {
            req = req.header("X-ClickHouse-User", user);
        }
        if let Some(ref password) = self.config.password {
            req = req.header("X-ClickHouse-Key", password);
        }
        let resp = tokio::task::block_in_place(|| rt.block_on(req.body(body).send()))
            .wrap_err_with(|| format!("failed to insert into {table}"))?;

        let status = resp.status();
        if !status.is_success() {
            let body = tokio::task::block_in_place(|| rt.block_on(resp.text()))
                .unwrap_or_else(|_| "<no body>".to_string());
            bail!("ClickHouse insert into {table} failed (HTTP {status}): {body}");
        }

        tracing::info!(table, rows = rows.len(), "Inserted rows into ClickHouse");
        Ok(())
    }

    /// Build the run row for insertion.
    fn build_run_row(&self, finished_at: std::time::SystemTime) -> ClickHouseRunRow<'_> {
        ClickHouseRunRow {
            run_id: self.config.run_id,
            started_at: system_time_to_millis(self.config.started_at),
            finished_at: system_time_to_millis(finished_at),
            scenario_name: &self.config.scenario_name,
            platform: &self.config.platform,
            mode: &self.config.mode,
            git_sha: &self.config.git_sha,
            git_ref: &self.config.git_ref,
            config: &self.config.config,
            metadata: &self.config.metadata,
        }
    }

    /// Build block rows for insertion.
    fn build_block_rows(&self, report: &FinalReport) -> Vec<ClickHouseBlockRow> {
        report
            .blocks
            .iter()
            .enumerate()
            .map(|(idx, block)| ClickHouseBlockRow {
                run_id: self.config.run_id,
                block_index: idx as u32,
                block_number: block.number,
                chain_timestamp_ms: Some(block.timestamp_ms),
                tx_count: block.tx_count as u32,
                gas_used: block.gas_used,
                gas_limit: block.gas_limit,
                block_time_ms: block.block_time_ms,
                new_payload_ms: block.new_payload_ms,
                forkchoice_updated_ms: block.forkchoice_updated_ms,
                new_payload_server_latency_us: block.new_payload_server_latency_us,
                persistence_wait_us: block.persistence_wait_us,
                execution_cache_wait_us: block.execution_cache_wait_us,
                sparse_trie_wait_us: block.sparse_trie_wait_us,
            })
            .collect()
    }

    /// Build metric sample rows for insertion.
    fn build_sample_rows<'a>(&self, samples: &'a [Sample]) -> Vec<ClickHouseMetricSampleRow<'a>> {
        samples
            .iter()
            .map(|s| {
                let source = if s.name.starts_with("txgen_") { "txgen" } else { "prometheus" };
                ClickHouseMetricSampleRow {
                    run_id: self.config.run_id,
                    offset_ms: s.offset_ms,
                    unix_ms: s.unix_ms,
                    metric_name: &s.name,
                    labels_json: serde_json::to_string(&s.labels).unwrap_or_default(),
                    source,
                    value: s.value,
                }
            })
            .collect()
    }
}

impl Reporter for ClickHouseReporter {
    fn finalize(&mut self, report: &FinalReport) -> Result<()> {
        let finished_at = std::time::SystemTime::now();

        tracing::info!(
            run_id = %self.config.run_id,
            blocks = report.blocks.len(),
            samples = report.samples.len(),
            "Inserting benchmark results into ClickHouse"
        );

        // Insert run.
        let run_row = self.build_run_row(finished_at);
        self.insert_rows("txgen_runs", &[run_row])?;

        // Insert blocks.
        let block_rows = self.build_block_rows(report);
        self.insert_rows("txgen_blocks", &block_rows)?;

        // Insert metric samples.
        let sample_rows = self.build_sample_rows(&report.samples);
        if !sample_rows.is_empty() {
            const BATCH_SIZE: usize = 100_000;
            for chunk in sample_rows.chunks(BATCH_SIZE) {
                self.insert_rows("txgen_metric_samples", chunk)?;
            }
        }

        tracing::info!(
            run_id = %self.config.run_id,
            scenario = %self.config.scenario_name,
            platform = %self.config.platform,
            blocks = block_rows.len(),
            samples = sample_rows.len(),
            "ClickHouse insert complete"
        );

        Ok(())
    }
}

/// Row for `txgen_runs` table.
#[derive(serde::Serialize)]
struct ClickHouseRunRow<'a> {
    run_id: uuid::Uuid,
    started_at: u64,
    finished_at: u64,
    scenario_name: &'a str,
    platform: &'a str,
    mode: &'a str,
    git_sha: &'a str,
    git_ref: &'a str,
    config: &'a HashMap<String, String>,
    metadata: &'a HashMap<String, String>,
}

/// Row for `txgen_blocks` table.
#[derive(serde::Serialize)]
struct ClickHouseBlockRow {
    run_id: uuid::Uuid,
    block_index: u32,
    block_number: u64,
    chain_timestamp_ms: Option<u64>,
    tx_count: u32,
    gas_used: u64,
    gas_limit: u64,
    block_time_ms: Option<u64>,
    new_payload_ms: Option<u64>,
    forkchoice_updated_ms: Option<u64>,
    new_payload_server_latency_us: Option<u64>,
    persistence_wait_us: Option<u64>,
    execution_cache_wait_us: Option<u64>,
    sparse_trie_wait_us: Option<u64>,
}

/// Row for `txgen_metric_samples` table.
#[derive(serde::Serialize)]
struct ClickHouseMetricSampleRow<'a> {
    run_id: uuid::Uuid,
    offset_ms: u64,
    unix_ms: u64,
    metric_name: &'a str,
    labels_json: String,
    source: &'static str,
    value: f64,
}

/// Convert a [`SystemTime`](std::time::SystemTime) to Unix milliseconds.
fn system_time_to_millis(t: std::time::SystemTime) -> u64 {
    // SAFETY: SystemTime::now() is always after UNIX_EPOCH
    t.duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
}

/// Parse reporter specifications into boxed reporters.
///
/// Supported formats:
/// - `console` - Human-readable output to stderr
/// - `json:<path>` - JSON output to file
/// - `clickhouse:<url>` - Push benchmark data to ClickHouse
/// - `prometheus:<url>` - Push samples via Prometheus remote write protocol
///   (protobuf + snappy on `/api/v1/write`). Works with VictoriaMetrics,
///   Prometheus, Cortex, Thanos, etc.
///   Auth and other knobs are read from environment variables (`VM_BEARER_TOKEN`, `VM_USER`,
///   `VM_PASSWORD`, `VM_TENANT_ID`, `VM_BATCH_SIZE`, `VM_TIMEOUT_SECS`).
///
/// The ClickHouse reporter requires metadata keys: `scenario`, `platform`,
/// `git-sha`, `git-ref`. Pass them via `-m key=value`.
pub fn parse_reporters(
    specs: &[String],
    mode: &str,
    metadata: &HashMap<String, String>,
) -> Result<Vec<Box<dyn Reporter>>> {
    let mut reporters: Vec<Box<dyn Reporter>> = Vec::new();

    if specs.is_empty() {
        return Ok(reporters);
    }

    for spec in specs {
        if spec == "console" {
            reporters.push(Box::new(ConsoleReporter::stderr(true)));
        } else if let Some(path) = spec.strip_prefix("json:") {
            let path = Path::new(path);
            reporters.push(Box::new(
                JsonReporter::file(path).wrap_err("failed to create JSON reporter")?,
            ));
        } else if let Some(url) = spec.strip_prefix("clickhouse:") {
            let config = ClickHouseConfig::from_metadata(url, mode, metadata)?;
            reporters.push(Box::new(
                ClickHouseReporter::new(config).wrap_err("failed to create ClickHouse reporter")?,
            ));
        } else if let Some(url) = spec
            .strip_prefix("prometheus:")
            .or_else(|| spec.strip_prefix("victoriametrics:"))
        {
            let config =
                crate::prometheus_reporter::PrometheusConfig::from_metadata(url, metadata)?;
            reporters.push(Box::new(
                crate::prometheus_reporter::PrometheusReporter::new(config)
                    .wrap_err("failed to create Prometheus reporter")?,
            ));
        } else {
            bail!("unknown report format: {}", spec);
        }
    }

    Ok(reporters)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::LatencyStats;
    use std::time::Duration;

    fn sample_metrics() -> BenchMetrics {
        BenchMetrics {
            sent: 1000,
            success: 950,
            failed: 50,
            elapsed: Duration::from_secs(10),
            latency: LatencyStats {
                min: Duration::from_millis(1),
                max: Duration::from_millis(100),
                mean: Duration::from_millis(15),
                p50: Duration::from_millis(10),
                p95: Duration::from_millis(50),
                p99: Duration::from_millis(80),
            },
        }
    }

    fn sample_report() -> FinalReport {
        FinalReport { bench_metrics: Some(sample_metrics()), ..Default::default() }
    }

    #[test]
    fn test_console_reporter() {
        let mut output = Vec::new();
        {
            let mut reporter = ConsoleReporter::new(&mut output, false);
            reporter.finalize(&sample_report()).unwrap();
        }

        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("1000"));
        assert!(output_str.contains("950"));
        assert!(output_str.contains("100.00 tx/s"));
    }

    #[test]
    fn test_json_reporter() {
        let mut output = Vec::new();
        {
            let mut reporter = JsonReporter::new(&mut output);
            reporter.finalize(&sample_report()).unwrap();
        }

        let output_str = String::from_utf8(output).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output_str).unwrap();
        assert_eq!(parsed["sent"], 1000);
        assert_eq!(parsed["success"], 950);
    }

    #[test]
    fn test_json_reporter_with_blocks() {
        let mut output = Vec::new();
        {
            let mut reporter = JsonReporter::new(&mut output);
            let mut report = sample_report();
            report.blocks = vec![BlockStats {
                number: 100,
                timestamp_ms: 1_000_000,
                tx_count: 10,
                gas_used: 1_000_000,
                gas_limit: 30_000_000,
                block_time_ms: Some(12000),
                new_payload_ms: None,
                forkchoice_updated_ms: None,
                new_payload_server_latency_us: None,
                persistence_wait_us: None,
                execution_cache_wait_us: None,
                sparse_trie_wait_us: None,
            }];
            reporter.finalize(&report).unwrap();
        }

        let output_str = String::from_utf8(output).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output_str).unwrap();
        assert_eq!(parsed["blocks"][0]["number"], 100);
        assert_eq!(parsed["blocks"][0]["tx_count"], 10);
    }

    #[test]
    fn test_console_reporter_on_progress() {
        let mut output = Vec::new();
        {
            let mut reporter = ConsoleReporter::new(&mut output, true);
            let state = ProgressState {
                sent: 100,
                success: 90,
                failed: 10,
                elapsed: Duration::from_secs(10),
                max_concurrent: 200,
                target_tps: Some(1000),
                unit: "tx",
            };
            reporter.on_progress(&state).unwrap();
        }

        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("Sent: 100"));
        assert!(output_str.contains("OK: 90"));
        assert!(output_str.contains("Fail: 10"));
        assert!(output_str.contains("Inflight: 0/200"));
        assert!(output_str.contains("10/1000 tx/s"));
    }

    #[test]
    fn test_console_reporter_on_progress_unlimited() {
        let mut output = Vec::new();
        {
            let mut reporter = ConsoleReporter::new(&mut output, true);
            let state = ProgressState {
                sent: 5000,
                success: 4800,
                failed: 50,
                elapsed: Duration::from_secs(5),
                max_concurrent: 100,
                target_tps: None,
                unit: "tx",
            };
            reporter.on_progress(&state).unwrap();
        }

        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("Inflight: 150/100"));
        assert!(output_str.contains("1000 tx/s"));
        assert!(!output_str.contains("1000/"));
    }

    #[test]
    fn test_apply_labels() {
        use crate::sample::Sample;
        use std::collections::BTreeMap;

        let mut report = FinalReport {
            samples: vec![
                Sample {
                    name: "txgen_sent_total".to_string(),
                    labels: BTreeMap::new(),
                    value: 100.0,
                    offset_ms: 0,
                    unix_ms: 0,
                },
                Sample {
                    name: "reth_metric".to_string(),
                    labels: BTreeMap::from([("host".to_string(), "node-1".to_string())]),
                    value: 42.0,
                    offset_ms: 0,
                    unix_ms: 0,
                },
            ],
            ..Default::default()
        };

        let labels = HashMap::from([
            ("run_id".to_string(), "abc123".to_string()),
            ("host".to_string(), "override-me".to_string()),
        ]);
        report.apply_labels(&labels);

        // run_id added to both.
        assert_eq!(report.samples[0].labels["run_id"], "abc123");
        assert_eq!(report.samples[1].labels["run_id"], "abc123");

        // Node label "host" preserved (not overwritten).
        assert_eq!(report.samples[1].labels["host"], "node-1");

        // No prior "host" label → gets the new value.
        assert_eq!(report.samples[0].labels["host"], "override-me");
    }
}
