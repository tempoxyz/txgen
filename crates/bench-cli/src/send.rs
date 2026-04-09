//! `bench send` - Send transactions from file or stdin

use crate::SendArgs;
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_client::RpcClient;
use alloy_transport::layers::RetryBackoffLayer;
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

    // CU/s set to u64::MAX to disable the layer's built-in rate limiting
    // while keeping retry-on-429 behavior. The benchmarking tool has its own
    // rate limiter and typically targets local nodes that don't rate-limit.
    let retry_layer = RetryBackoffLayer::new(10, 100, u64::MAX);
    let providers = args
        .rpc_urls
        .iter()
        .map(|url| {
            let url = url.parse().context("failed to parse RPC URL")?;
            let client = RpcClient::builder().layer(retry_layer.clone()).http(url);
            Ok(ProviderBuilder::new().connect_client(client).erased())
        })
        .collect::<Result<Vec<_>>>()?;

    let metrics = MetricsCollector::new();
    let config = SenderConfig {
        rate_limit: args.tps,
        max_concurrent: args.max_concurrent,
    };
    let mut sender = Sender::new(providers, config, metrics.clone());

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
