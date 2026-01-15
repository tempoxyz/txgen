//! Reporters for benchmark results.
//!
//! Output formats:
//! - Console (human-readable)
//! - JSON (machine-readable)
//! - ClickHouse (for time-series storage)

use crate::metrics::{BenchMetrics, BlockStats, ReplayBlockStats, RunStats, TimeSeriesMetrics};
use eyre::{Context, Result, bail};
use std::io::Write;
use std::path::Path;

/// Reporter trait for outputting benchmark results.
pub trait Reporter: Send {
    /// Called periodically during send with current counts.
    fn on_progress(&mut self, _sent: u64, _success: u64, _failed: u64) -> Result<()> {
        Ok(())
    }

    /// Called for each block during block stats collection.
    fn on_block(&mut self, _block: &BlockStats) -> Result<()> {
        Ok(())
    }

    /// Called for each replayed block during Engine API replay.
    fn on_replay_block(&mut self, _block: &ReplayBlockStats) -> Result<()> {
        Ok(())
    }

    /// Finalize and output the benchmark results.
    fn finalize(
        &mut self,
        metrics: &BenchMetrics,
        time_series: Option<&TimeSeriesMetrics>,
        run_stats: Option<&RunStats>,
    ) -> Result<()>;
}

/// Console reporter with human-readable output.
pub struct ConsoleReporter<W: Write + Send = Box<dyn Write + Send>> {
    writer: W,
    show_progress: bool,
}

impl ConsoleReporter {
    /// Create a new console reporter writing to stdout.
    pub fn stdout(show_progress: bool) -> Self {
        Self {
            writer: Box::new(std::io::stdout()),
            show_progress,
        }
    }

    /// Create a new console reporter writing to stderr.
    pub fn stderr(show_progress: bool) -> Self {
        Self {
            writer: Box::new(std::io::stderr()),
            show_progress,
        }
    }
}

impl<W: Write + Send> ConsoleReporter<W> {
    /// Create a new console reporter with a custom writer.
    pub fn new(writer: W, show_progress: bool) -> Self {
        Self {
            writer,
            show_progress,
        }
    }
}

impl<W: Write + Send> Reporter for ConsoleReporter<W> {
    fn on_progress(&mut self, sent: u64, success: u64, failed: u64) -> Result<()> {
        if self.show_progress {
            write!(
                self.writer,
                "\r  Sent: {} | Success: {} | Failed: {}",
                sent, success, failed
            )?;
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

    fn finalize(
        &mut self,
        metrics: &BenchMetrics,
        _time_series: Option<&TimeSeriesMetrics>,
        run_stats: Option<&RunStats>,
    ) -> Result<()> {
        writeln!(self.writer)?;
        writeln!(self.writer, "═══════════════════════════════════════")?;
        writeln!(self.writer, "              Benchmark Results")?;
        writeln!(self.writer, "═══════════════════════════════════════")?;
        writeln!(self.writer)?;
        writeln!(self.writer, "  Total Sent:      {:>10}", metrics.sent)?;
        writeln!(self.writer, "  Successful:      {:>10}", metrics.success)?;
        writeln!(self.writer, "  Failed:          {:>10}", metrics.failed)?;
        writeln!(self.writer)?;
        writeln!(
            self.writer,
            "  Duration:        {:>10.2}s",
            metrics.elapsed.as_secs_f64()
        )?;
        writeln!(
            self.writer,
            "  Throughput:      {:>10.2} tx/s",
            metrics.tps()
        )?;
        writeln!(
            self.writer,
            "  Success Rate:    {:>10.1}%",
            metrics.success_rate()
        )?;
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

        if let Some(run) = run_stats {
            writeln!(self.writer)?;
            writeln!(self.writer, "  Blocks:")?;
            writeln!(
                self.writer,
                "    Range:         {:>10} - {}",
                run.start_block, run.end_block
            )?;
            writeln!(self.writer, "    Avg TPS:       {:>10.2}", run.avg_tps)?;
            writeln!(
                self.writer,
                "    Avg Gas/s:     {:>10.2}",
                run.avg_gas_per_second
            )?;
            writeln!(
                self.writer,
                "    Block Time P50:{:>10}ms",
                run.block_time_p50_ms
            )?;
            writeln!(
                self.writer,
                "    Block Time P95:{:>10}ms",
                run.block_time_p95_ms
            )?;
            writeln!(
                self.writer,
                "    Block Time P99:{:>10}ms",
                run.block_time_p99_ms
            )?;
        }

        writeln!(self.writer)?;
        writeln!(self.writer, "═══════════════════════════════════════")?;

        Ok(())
    }
}

/// JSON output format.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct JsonReport {
    /// Total transactions sent.
    pub sent: u64,
    /// Successful transactions.
    pub success: u64,
    /// Failed transactions.
    pub failed: u64,
    /// Elapsed time in seconds.
    pub elapsed_secs: f64,
    /// Transactions per second.
    pub tps: f64,
    /// Success rate percentage.
    pub success_rate: f64,
    /// Latency statistics.
    pub latency: JsonLatency,
    /// Time-series data for graphing (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_series: Option<JsonTimeSeries>,
    /// Block-level statistics (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocks: Option<Vec<JsonBlockStats>>,
    /// Run summary statistics (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_stats: Option<JsonRunStats>,
}

/// Block statistics in JSON format.
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct JsonBlockStats {
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_time_ms: Option<u64>,
}

impl From<&BlockStats> for JsonBlockStats {
    fn from(b: &BlockStats) -> Self {
        Self {
            number: b.number,
            timestamp: b.timestamp,
            tx_count: b.tx_count,
            success_count: b.success_count,
            gas_used: b.gas_used,
            gas_limit: b.gas_limit,
            block_time_ms: b.block_time_ms,
        }
    }
}

/// Replay block statistics in JSON format.
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct JsonReplayBlockStats {
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
    /// Total execution latency in milliseconds.
    pub total_latency_ms: u64,
    /// Payload status from newPayload response.
    pub payload_status: String,
}

impl From<&ReplayBlockStats> for JsonReplayBlockStats {
    fn from(b: &ReplayBlockStats) -> Self {
        Self {
            number: b.number,
            timestamp: b.timestamp,
            tx_count: b.tx_count,
            gas_used: b.gas_used,
            gas_limit: b.gas_limit,
            new_payload_ms: b.new_payload_ms,
            fcu_ms: b.fcu_ms,
            total_latency_ms: b.total_latency_ms,
            payload_status: b.payload_status.clone(),
        }
    }
}

/// Run summary statistics in JSON format.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct JsonRunStats {
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

impl From<&RunStats> for JsonRunStats {
    fn from(r: &RunStats) -> Self {
        Self {
            start_block: r.start_block,
            end_block: r.end_block,
            total_txs: r.total_txs,
            total_gas: r.total_gas,
            duration_ms: r.duration_ms,
            avg_tps: r.avg_tps,
            avg_gas_per_second: r.avg_gas_per_second,
            block_time_p50_ms: r.block_time_p50_ms,
            block_time_p95_ms: r.block_time_p95_ms,
            block_time_p99_ms: r.block_time_p99_ms,
        }
    }
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
    pub throughput: Vec<JsonThroughputSample>,
    /// Individual latency samples.
    pub latencies: Vec<JsonLatencySample>,
}

/// Per-second throughput sample.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct JsonThroughputSample {
    /// Second offset from start.
    pub second: u64,
    /// Transactions sent.
    pub sent: u64,
    /// Successful transactions.
    pub success: u64,
    /// Failed transactions.
    pub failed: u64,
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
    blocks: Vec<JsonBlockStats>,
    replay_blocks: Vec<JsonReplayBlockStats>,
}

impl JsonReporter {
    /// Create a JSON reporter writing to stdout.
    pub fn stdout() -> Self {
        Self {
            writer: Box::new(std::io::stdout()),
            blocks: Vec::new(),
            replay_blocks: Vec::new(),
        }
    }

    /// Create a JSON reporter writing to a file.
    pub fn file(path: &Path) -> Result<JsonReporter<std::io::BufWriter<std::fs::File>>> {
        let file = std::fs::File::create(path).context("failed to create output file")?;
        Ok(JsonReporter {
            writer: std::io::BufWriter::new(file),
            blocks: Vec::new(),
            replay_blocks: Vec::new(),
        })
    }
}

impl<W: Write + Send> JsonReporter<W> {
    /// Create a new JSON reporter with a custom writer.
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            blocks: Vec::new(),
            replay_blocks: Vec::new(),
        }
    }
}

impl<W: Write + Send> Reporter for JsonReporter<W> {
    fn on_block(&mut self, block: &BlockStats) -> Result<()> {
        self.blocks.push(JsonBlockStats::from(block));
        Ok(())
    }

    fn on_replay_block(&mut self, block: &ReplayBlockStats) -> Result<()> {
        self.replay_blocks.push(JsonReplayBlockStats::from(block));
        Ok(())
    }

    fn finalize(
        &mut self,
        metrics: &BenchMetrics,
        time_series: Option<&TimeSeriesMetrics>,
        run_stats: Option<&RunStats>,
    ) -> Result<()> {
        let ts = time_series.map(|ts| JsonTimeSeries {
            throughput: ts
                .throughput
                .iter()
                .map(|s| JsonThroughputSample {
                    second: s.second,
                    sent: s.sent,
                    success: s.success,
                    failed: s.failed,
                })
                .collect(),
            latencies: ts
                .latencies
                .iter()
                .map(|l| JsonLatencySample {
                    offset_ms: l.offset_ms,
                    latency_ms: l.latency.as_secs_f64() * 1000.0,
                })
                .collect(),
        });

        let blocks = if self.blocks.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut self.blocks))
        };

        let report = JsonReport {
            sent: metrics.sent,
            success: metrics.success,
            failed: metrics.failed,
            elapsed_secs: metrics.elapsed.as_secs_f64(),
            tps: metrics.tps(),
            success_rate: metrics.success_rate(),
            latency: JsonLatency {
                min_ms: metrics.latency.min.as_secs_f64() * 1000.0,
                max_ms: metrics.latency.max.as_secs_f64() * 1000.0,
                mean_ms: metrics.latency.mean.as_secs_f64() * 1000.0,
                p50_ms: metrics.latency.p50.as_secs_f64() * 1000.0,
                p95_ms: metrics.latency.p95.as_secs_f64() * 1000.0,
                p99_ms: metrics.latency.p99.as_secs_f64() * 1000.0,
            },
            time_series: ts,
            blocks,
            run_stats: run_stats.map(JsonRunStats::from),
        };

        serde_json::to_writer_pretty(&mut self.writer, &report)?;
        writeln!(self.writer)?;

        Ok(())
    }
}

/// ClickHouse reporter configuration.
#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    /// ClickHouse HTTP endpoint.
    pub url: String,
    /// Database name.
    pub database: String,
    /// Table name.
    pub table: String,
    /// Run identifier for grouping results.
    pub run_id: String,
    /// Additional tags/labels.
    pub tags: std::collections::HashMap<String, String>,
}

/// ClickHouse reporter for time-series storage.
pub struct ClickHouseReporter {
    config: ClickHouseConfig,
    #[allow(dead_code)]
    client: reqwest::Client,
}

impl ClickHouseReporter {
    /// Create a new ClickHouse reporter.
    pub fn new(config: ClickHouseConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .context("failed to create HTTP client")?;

        Ok(Self { config, client })
    }
}

impl Reporter for ClickHouseReporter {
    fn on_block(&mut self, block: &BlockStats) -> Result<()> {
        tracing::debug!(
            run_id = %self.config.run_id,
            block = block.number,
            tx_count = block.tx_count,
            gas_used = block.gas_used,
            "Would insert block to ClickHouse"
        );
        Ok(())
    }

    fn finalize(
        &mut self,
        metrics: &BenchMetrics,
        _time_series: Option<&TimeSeriesMetrics>,
        run_stats: Option<&RunStats>,
    ) -> Result<()> {
        // SAFETY: SystemTime::now() is always after UNIX_EPOCH
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let tags_json = serde_json::to_string(&self.config.tags)?;

        let query = format!(
            "INSERT INTO {}.{} (timestamp, run_id, sent, success, failed, elapsed_secs, tps, success_rate, latency_min_ms, latency_max_ms, latency_mean_ms, latency_p50_ms, latency_p95_ms, latency_p99_ms, tags) VALUES ({}, '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, '{}')",
            self.config.database,
            self.config.table,
            timestamp,
            self.config.run_id,
            metrics.sent,
            metrics.success,
            metrics.failed,
            metrics.elapsed.as_secs_f64(),
            metrics.tps(),
            metrics.success_rate(),
            metrics.latency.min.as_secs_f64() * 1000.0,
            metrics.latency.max.as_secs_f64() * 1000.0,
            metrics.latency.mean.as_secs_f64() * 1000.0,
            metrics.latency.p50.as_secs_f64() * 1000.0,
            metrics.latency.p95.as_secs_f64() * 1000.0,
            metrics.latency.p99.as_secs_f64() * 1000.0,
            tags_json.replace('\'', "\\'"),
        );

        tracing::info!(
            run_id = %self.config.run_id,
            sent = metrics.sent,
            success = metrics.success,
            tps = metrics.tps(),
            "Would insert to ClickHouse: {}",
            query
        );

        if let Some(run) = run_stats {
            tracing::info!(
                run_id = %self.config.run_id,
                start_block = run.start_block,
                end_block = run.end_block,
                avg_tps = run.avg_tps,
                avg_gas_per_second = run.avg_gas_per_second,
                "Would insert run stats to ClickHouse"
            );
        }

        Ok(())
    }
}

/// Parse reporter specifications into boxed reporters.
///
/// Supported formats:
/// - `console` - Human-readable output to stderr
/// - `json:<path>` - JSON output to file
/// - `clickhouse:<url>` - ClickHouse (not yet implemented)
pub fn parse_reporters(specs: &[String]) -> Result<Vec<Box<dyn Reporter>>> {
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
        } else if let Some(_url) = spec.strip_prefix("clickhouse:") {
            tracing::warn!("ClickHouse reporter not yet fully implemented");
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

    #[test]
    fn test_console_reporter() {
        let mut output = Vec::new();
        {
            let mut reporter = ConsoleReporter::new(&mut output, false);
            reporter.finalize(&sample_metrics(), None, None).unwrap();
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
            reporter.finalize(&sample_metrics(), None, None).unwrap();
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
            reporter
                .on_block(&BlockStats {
                    number: 100,
                    timestamp: 1000,
                    tx_count: 10,
                    success_count: 9,
                    gas_used: 1_000_000,
                    gas_limit: 30_000_000,
                    block_time_ms: Some(12000),
                })
                .unwrap();
            reporter.finalize(&sample_metrics(), None, None).unwrap();
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
            reporter.on_progress(100, 90, 10).unwrap();
        }

        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("100"));
        assert!(output_str.contains("90"));
        assert!(output_str.contains("10"));
    }
}
