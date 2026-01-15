//! `bench send` - Send transactions from file or stdin

use crate::SendArgs;
use bench_core::{
    ConsoleReporter, FileSource, JsonReporter, MetricsCollector, Reporter, Sender, SenderConfig,
    StdinSource, TxSource,
};
use eyre::{Context, Result, bail};

pub async fn execute(args: SendArgs) -> Result<()> {
    tracing::info!(
        input = args.input.as_ref().map(|p| p.display().to_string()).as_deref().unwrap_or("stdin"),
        rpc = %args.rpc,
        tps = args.tps,
        "Starting send"
    );

    let metrics = MetricsCollector::new();
    let config = SenderConfig {
        rpc_url: args.rpc.clone(),
        rate_limit: args.tps,
        max_concurrent: args.max_concurrent,
        timeout: args.timeout,
    };
    let mut sender = Sender::new(config, metrics.clone())?;

    let mut reporters = parse_reporters(&args.reports)?;

    metrics.start().await;

    match &args.input {
        Some(path) => {
            let mut source = FileSource::new(path).wrap_err("failed to open input file")?;
            send_from_source(&mut source, &mut sender, &metrics, &mut reporters).await?;
        }
        None => {
            let mut source = StdinSource::new();
            send_from_source(&mut source, &mut sender, &metrics, &mut reporters).await?;
        }
    }

    sender.flush().await;

    let final_metrics = metrics.finalize().await;
    let time_series = metrics.time_series().await;

    for reporter in &mut reporters {
        reporter.report(&final_metrics, Some(&time_series))?;
    }

    Ok(())
}

async fn send_from_source<S: TxSource>(
    source: &mut S,
    sender: &mut Sender,
    metrics: &MetricsCollector,
    reporters: &mut [Box<dyn Reporter>],
) -> Result<()> {
    while let Some(tx) = source.next_tx().await? {
        sender.send(tx).await?;

        let (sent, success, failed) = metrics.counts();
        for reporter in reporters.iter_mut() {
            reporter.progress(sent, success, failed)?;
        }
    }
    Ok(())
}

fn parse_reporters(specs: &[String]) -> Result<Vec<Box<dyn Reporter>>> {
    let mut reporters: Vec<Box<dyn Reporter>> = Vec::new();

    if specs.is_empty() {
        reporters.push(Box::new(ConsoleReporter::stderr(true)));
        return Ok(reporters);
    }

    for spec in specs {
        if spec == "console" {
            reporters.push(Box::new(ConsoleReporter::stderr(true)));
        } else if let Some(path) = spec.strip_prefix("json:") {
            let path = std::path::Path::new(path);
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
