//! `bench plot` - Generate PNG plots from JSON report

use bench_core::{JsonReport, JsonTimeSeries};
use clap::ValueEnum;
use eyre::{Context, Result, bail};
use plotters::prelude::*;
use std::fs;
use std::path::PathBuf;

use crate::PlotArgs;

/// Available plot types.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum PlotType {
    /// Throughput over time (TPS per second).
    Throughput,
    /// Latency over time (individual samples).
    Latency,
    /// Cumulative sent/success/failed.
    Cumulative,
    /// All plots in one.
    All,
}

pub fn execute(args: PlotArgs) -> Result<()> {
    let content = fs::read_to_string(&args.input)
        .wrap_err_with(|| format!("failed to read {}", args.input.display()))?;

    let report: JsonReport =
        serde_json::from_str(&content).wrap_err("failed to parse JSON report")?;

    let time_series = report
        .time_series
        .as_ref()
        .ok_or_else(|| eyre::eyre!("JSON report does not contain time_series data"))?;

    let output_dir = args.output.unwrap_or_else(|| PathBuf::from("."));
    fs::create_dir_all(&output_dir).wrap_err("failed to create output directory")?;

    match args.plot_type {
        PlotType::Throughput => {
            let path = output_dir.join("throughput.png");
            plot_throughput(time_series, &path, args.width, args.height)?;
            println!("Generated: {}", path.display());
        }
        PlotType::Latency => {
            let path = output_dir.join("latency.png");
            plot_latency(time_series, &path, args.width, args.height)?;
            println!("Generated: {}", path.display());
        }
        PlotType::Cumulative => {
            let path = output_dir.join("cumulative.png");
            plot_cumulative(time_series, &path, args.width, args.height)?;
            println!("Generated: {}", path.display());
        }
        PlotType::All => {
            let path = output_dir.join("throughput.png");
            plot_throughput(time_series, &path, args.width, args.height)?;
            println!("Generated: {}", path.display());

            let path = output_dir.join("latency.png");
            plot_latency(time_series, &path, args.width, args.height)?;
            println!("Generated: {}", path.display());

            let path = output_dir.join("cumulative.png");
            plot_cumulative(time_series, &path, args.width, args.height)?;
            println!("Generated: {}", path.display());
        }
    }

    Ok(())
}

fn plot_throughput(
    ts: &JsonTimeSeries,
    output: &std::path::Path,
    width: u32,
    height: u32,
) -> Result<()> {
    if ts.throughput.is_empty() {
        bail!("no throughput data available");
    }

    let max_x = ts.throughput.last().map(|s| s.second).unwrap_or(1) as f64;
    let max_y = ts
        .throughput
        .iter()
        .map(|s| s.sent.max(s.success).max(s.failed))
        .max()
        .unwrap_or(1) as f64
        * 1.1;

    let root = BitMapBackend::new(output, (width, height)).into_drawing_area();
    root.fill(&WHITE).wrap_err("failed to fill background")?;

    let mut chart = ChartBuilder::on(&root)
        .caption("Throughput Over Time", ("sans-serif", 24).into_font())
        .margin(10)
        .x_label_area_size(40)
        .y_label_area_size(60)
        .build_cartesian_2d(0.0..max_x, 0.0..max_y)
        .wrap_err("failed to build chart")?;

    chart
        .configure_mesh()
        .x_desc("Time (seconds)")
        .y_desc("Transactions per second")
        .draw()
        .wrap_err("failed to draw mesh")?;

    chart
        .draw_series(LineSeries::new(
            ts.throughput
                .iter()
                .map(|s| (s.second as f64, s.sent as f64)),
            &BLUE,
        ))
        .wrap_err("failed to draw sent series")?
        .label("Sent")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], BLUE));

    chart
        .draw_series(LineSeries::new(
            ts.throughput
                .iter()
                .map(|s| (s.second as f64, s.success as f64)),
            &GREEN,
        ))
        .wrap_err("failed to draw success series")?
        .label("Success")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], GREEN));

    chart
        .draw_series(LineSeries::new(
            ts.throughput
                .iter()
                .map(|s| (s.second as f64, s.failed as f64)),
            &RED,
        ))
        .wrap_err("failed to draw failed series")?
        .label("Failed")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], RED));

    chart
        .configure_series_labels()
        .background_style(WHITE.mix(0.8))
        .border_style(BLACK)
        .draw()
        .wrap_err("failed to draw legend")?;

    root.present().wrap_err("failed to write PNG")?;

    Ok(())
}

fn plot_latency(
    ts: &JsonTimeSeries,
    output: &std::path::Path,
    width: u32,
    height: u32,
) -> Result<()> {
    if ts.latencies.is_empty() {
        bail!("no latency data available");
    }

    let max_x = ts
        .latencies
        .last()
        .map(|l| l.offset_ms as f64 / 1000.0)
        .unwrap_or(1.0);
    let max_y = ts
        .latencies
        .iter()
        .map(|l| l.latency_ms)
        .fold(0.0f64, |a, b| a.max(b))
        * 1.1;

    let root = BitMapBackend::new(output, (width, height)).into_drawing_area();
    root.fill(&WHITE).wrap_err("failed to fill background")?;

    let mut chart = ChartBuilder::on(&root)
        .caption("Latency Over Time", ("sans-serif", 24).into_font())
        .margin(10)
        .x_label_area_size(40)
        .y_label_area_size(60)
        .build_cartesian_2d(0.0..max_x, 0.0..max_y)
        .wrap_err("failed to build chart")?;

    chart
        .configure_mesh()
        .x_desc("Time (seconds)")
        .y_desc("Latency (ms)")
        .draw()
        .wrap_err("failed to draw mesh")?;

    chart
        .draw_series(ts.latencies.iter().map(|l| {
            Circle::new(
                (l.offset_ms as f64 / 1000.0, l.latency_ms),
                2,
                BLUE.filled(),
            )
        }))
        .wrap_err("failed to draw latency points")?;

    root.present().wrap_err("failed to write PNG")?;

    Ok(())
}

fn plot_cumulative(
    ts: &JsonTimeSeries,
    output: &std::path::Path,
    width: u32,
    height: u32,
) -> Result<()> {
    if ts.throughput.is_empty() {
        bail!("no throughput data available");
    }

    let mut cumulative_sent = 0u64;
    let mut cumulative_success = 0u64;
    let mut cumulative_failed = 0u64;

    let sent_points: Vec<(f64, f64)> = ts
        .throughput
        .iter()
        .map(|s| {
            cumulative_sent += s.sent;
            (s.second as f64, cumulative_sent as f64)
        })
        .collect();

    // Reset for success
    let success_points: Vec<(f64, f64)> = ts
        .throughput
        .iter()
        .map(|s| {
            cumulative_success += s.success;
            (s.second as f64, cumulative_success as f64)
        })
        .collect();

    // Reset for failed
    let failed_points: Vec<(f64, f64)> = ts
        .throughput
        .iter()
        .map(|s| {
            cumulative_failed += s.failed;
            (s.second as f64, cumulative_failed as f64)
        })
        .collect();

    let max_x = ts.throughput.last().map(|s| s.second).unwrap_or(1) as f64;
    let max_y = cumulative_sent as f64 * 1.1;

    let root = BitMapBackend::new(output, (width, height)).into_drawing_area();
    root.fill(&WHITE).wrap_err("failed to fill background")?;

    let mut chart = ChartBuilder::on(&root)
        .caption("Cumulative Transactions", ("sans-serif", 24).into_font())
        .margin(10)
        .x_label_area_size(40)
        .y_label_area_size(60)
        .build_cartesian_2d(0.0..max_x, 0.0..max_y)
        .wrap_err("failed to build chart")?;

    chart
        .configure_mesh()
        .x_desc("Time (seconds)")
        .y_desc("Total transactions")
        .draw()
        .wrap_err("failed to draw mesh")?;

    chart
        .draw_series(LineSeries::new(sent_points, &BLUE))
        .wrap_err("failed to draw sent series")?
        .label("Sent")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], BLUE));

    chart
        .draw_series(LineSeries::new(success_points, &GREEN))
        .wrap_err("failed to draw success series")?
        .label("Success")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], GREEN));

    chart
        .draw_series(LineSeries::new(failed_points, &RED))
        .wrap_err("failed to draw failed series")?
        .label("Failed")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], RED));

    chart
        .configure_series_labels()
        .background_style(WHITE.mix(0.8))
        .border_style(BLACK)
        .draw()
        .wrap_err("failed to draw legend")?;

    root.present().wrap_err("failed to write PNG")?;

    Ok(())
}
