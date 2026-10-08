//! `bench view` - Print an existing JSON report with the console reporter.

use alloy_primitives::U256;
use bench_core::{BenchMetrics, ConsoleReporter, FinalReport, JsonReport, LatencyStats, Reporter};
use eyre::{Context, Result};
use std::{fs, time::Duration};

use crate::ViewArgs;

pub fn execute(args: ViewArgs) -> Result<()> {
    let content = fs::read_to_string(&args.input)
        .wrap_err_with(|| format!("failed to read {}", args.input.display()))?;

    let report: JsonReport =
        serde_json::from_str(&content).wrap_err("failed to parse JSON report")?;

    let bench_metrics = match report.sent {
        Some(sent) => Some(BenchMetrics {
            sent,
            success: report.success.unwrap_or(0),
            failed: report.failed.unwrap_or(0),
            elapsed: Duration::from_secs_f64(report.elapsed_secs.unwrap_or(0.0)),
            latency: report.latency.as_ref().map(LatencyStats::from),
        }),
        _ => None,
    };

    let final_report = FinalReport {
        bench_metrics,
        call: report.call,
        run_stats: report.run_stats,
        blocks: report.blocks.unwrap_or_default(),
        total_fees_paid: report
            .total_fees_paid
            .map(|fees| fees.parse::<U256>())
            .transpose()
            .wrap_err("invalid total_fees_paid in JSON report")?,
        ..Default::default()
    };

    let mut reporter = ConsoleReporter::stderr(false);
    reporter.finalize(&final_report)?;

    Ok(())
}
