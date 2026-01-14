//! Reporters for benchmark results.
//!
//! Output formats:
//! - Console (human-readable)
//! - JSON (machine-readable)
//! - ClickHouse (for time-series storage)

use crate::metrics::BenchMetrics;
use eyre::{Context, Result};
use std::io::Write;
use std::path::Path;

/// Reporter trait for outputting benchmark results.
pub trait Reporter: Send {
    /// Report the final metrics.
    fn report(&mut self, metrics: &BenchMetrics) -> Result<()>;

    /// Report periodic progress (optional).
    fn progress(&mut self, _sent: u64, _success: u64, _failed: u64) -> Result<()> {
        Ok(())
    }
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
    fn report(&mut self, metrics: &BenchMetrics) -> Result<()> {
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
        writeln!(self.writer)?;
        writeln!(self.writer, "═══════════════════════════════════════")?;

        Ok(())
    }

    fn progress(&mut self, sent: u64, success: u64, failed: u64) -> Result<()> {
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
}

/// JSON output format.
#[derive(serde::Serialize)]
struct JsonReport {
    sent: u64,
    success: u64,
    failed: u64,
    elapsed_secs: f64,
    tps: f64,
    success_rate: f64,
    latency: JsonLatency,
}

#[derive(serde::Serialize)]
struct JsonLatency {
    min_ms: f64,
    max_ms: f64,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

/// JSON reporter for machine-readable output.
pub struct JsonReporter<W: Write + Send = Box<dyn Write + Send>> {
    writer: W,
}

impl JsonReporter {
    /// Create a JSON reporter writing to stdout.
    pub fn stdout() -> Self {
        Self {
            writer: Box::new(std::io::stdout()),
        }
    }

    /// Create a JSON reporter writing to a file.
    pub fn file(path: &Path) -> Result<JsonReporter<std::io::BufWriter<std::fs::File>>> {
        let file = std::fs::File::create(path).context("failed to create output file")?;
        Ok(JsonReporter {
            writer: std::io::BufWriter::new(file),
        })
    }
}

impl<W: Write + Send> JsonReporter<W> {
    /// Create a new JSON reporter with a custom writer.
    pub fn new(writer: W) -> Self {
        Self { writer }
    }
}

impl<W: Write + Send> Reporter for JsonReporter<W> {
    fn report(&mut self, metrics: &BenchMetrics) -> Result<()> {
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
    fn report(&mut self, metrics: &BenchMetrics) -> Result<()> {
        // Build ClickHouse insert query.
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

        // Note: This is synchronous but wrapped in a blocking task in practice.
        // For now, we just log that we would insert.
        tracing::info!(
            run_id = %self.config.run_id,
            sent = metrics.sent,
            success = metrics.success,
            tps = metrics.tps(),
            "Would insert to ClickHouse: {}",
            query
        );

        Ok(())
    }
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
            reporter.report(&sample_metrics()).unwrap();
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
            reporter.report(&sample_metrics()).unwrap();
        }

        let output_str = String::from_utf8(output).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output_str).unwrap();
        assert_eq!(parsed["sent"], 1000);
        assert_eq!(parsed["success"], 950);
    }
}
