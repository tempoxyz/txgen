//! `bench view` - Print an existing JSON report with the console reporter.

use bench_core::{BenchMetrics, ConsoleReporter, FinalReport, JsonReport, LatencyStats, Reporter};
use eyre::{Context, Result};
use std::{fs, time::Duration};

use crate::ViewArgs;

pub fn execute(args: ViewArgs) -> Result<()> {
    let content = fs::read_to_string(&args.input)
        .wrap_err_with(|| format!("failed to read {}", args.input.display()))?;

    let report: JsonReport =
        serde_json::from_str(&content).wrap_err("failed to parse JSON report")?;

    let bench_metrics = match (report.sent, report.latency) {
        (Some(sent), Some(lat)) => Some(BenchMetrics {
            sent,
            success: report.success.unwrap_or(0),
            failed: report.failed.unwrap_or(0),
            elapsed: Duration::from_secs_f64(report.elapsed_secs.unwrap_or(0.0)),
            latency: LatencyStats {
                min: Duration::from_secs_f64(lat.min_ms / 1000.0),
                max: Duration::from_secs_f64(lat.max_ms / 1000.0),
                mean: Duration::from_secs_f64(lat.mean_ms / 1000.0),
                p50: Duration::from_secs_f64(lat.p50_ms / 1000.0),
                p95: Duration::from_secs_f64(lat.p95_ms / 1000.0),
                p99: Duration::from_secs_f64(lat.p99_ms / 1000.0),
            },
        }),
        _ => None,
    };

    let final_report = FinalReport {
        bench_metrics,
        run_stats: report.run_stats,
        blocks: report.blocks.unwrap_or_default(),
        ..Default::default()
    };

    let mut reporter = ConsoleReporter::stderr(false);
    reporter.finalize(&final_report)?;

    Ok(())
}
