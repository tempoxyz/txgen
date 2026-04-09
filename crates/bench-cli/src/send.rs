//! `bench send` - Send transactions from file or stdin

use crate::SendArgs;
use bench_core::{
    ConsoleReporter, FileSource, MetricsCollector, Reporter, Sender, SenderConfig, StdinSource,
    TxSource, parse_reporters,
};
use eyre::{Context, Result};

pub async fn execute(args: SendArgs) -> Result<()> {
    tracing::info!(
        input = args.input.as_ref().map(|p| p.display().to_string()).as_deref().unwrap_or("stdin"),
        rpc_urls = ?args.rpc_urls,
        tps = args.tps,
        "Starting send"
    );

    let metrics = MetricsCollector::new();
    let config = SenderConfig {
        rpc_urls: args.rpc_urls.clone(),
        rate_limit: args.tps,
        max_concurrent: args.max_concurrent,
        timeout: args.timeout,
    };
    let mut sender = Sender::new(config, metrics.clone())?;

    let mut reporters = parse_reporters(&args.reports)?;
    if reporters.is_empty() {
        reporters.push(Box::new(ConsoleReporter::stderr(true)));
    }

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
        reporter.finalize(&final_metrics, Some(&time_series), None)?;
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
        if sent % 1000 == 0 {
            for reporter in reporters.iter_mut() {
                reporter.on_progress(sent, success, failed)?;
            }
        }
    }
    Ok(())
}
