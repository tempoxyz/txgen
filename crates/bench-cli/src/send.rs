//! `bench send` - Send transactions from file or stdin

use crate::SendArgs;
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_client::RpcClient;
use alloy_transport::layers::RetryBackoffLayer;
use bench_core::{
    ConsoleReporter, FileSource, FinalReport, MetricsCollector, ProgressState, Reporter, RunClock,
    RunStats, SampleStore, ScraperConfig, Sender, SenderConfig, StdinSource, TxSource,
    collect_block_stats, parse_reporters, start_block_poller, start_scraper,
};
use eyre::{Context, Result, bail};
use std::collections::HashMap;
use std::time::Duration;

pub async fn execute(args: SendArgs) -> Result<()> {
    tracing::info!(
        input = args.input.as_ref().map(|p| p.display().to_string()).as_deref().unwrap_or("stdin"),
        rpc_urls = ?args.rpc_urls,
        tps = args.tps,
        "Starting send"
    );

    let metadata = parse_metadata(&args.metadata)?;

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

    let clock = RunClock::new();
    let store = SampleStore::new();
    let metrics = MetricsCollector::new(clock.clone());

    // Start background scraper + internal snapshotter if metrics URL is configured.
    let scraper_handle = if let Some(ref url) = args.metrics_url {
        let scraper_config =
            ScraperConfig::new(url).with_interval(Duration::from_millis(args.scrape_interval_ms));

        // Internal metrics are snapshotted on the same tick as the Prometheus
        // scrape so all samples share identical timestamps.
        let snap_metrics = metrics.clone();
        let callback: bench_core::SampleCallback =
            std::sync::Arc::new(move || snap_metrics.snapshot_samples());

        let handle = start_scraper(scraper_config, clock.clone(), store.clone(), Some(callback));

        tracing::info!(url, "Started metrics scraper");
        Some(handle)
    } else {
        None
    };

    let config = SenderConfig {
        rate_limit: args.tps,
        max_concurrent: args.max_concurrent,
    };
    let mut sender = Sender::new(providers.clone(), config.clone(), metrics.clone());

    let mut reporters = parse_reporters(&args.reports, "send", &metadata)?;
    if reporters.is_empty() {
        reporters.push(Box::new(ConsoleReporter::stderr(true)));
    }

    // Record the block number before sending so we can collect per-block stats
    // afterwards. Use the first provider for block queries.
    let query_provider = &providers[0];
    let start_block = query_provider
        .get_block_number()
        .await
        .wrap_err("failed to get starting block number")?;

    // Start background block head-poller to record BlockMarkers.
    let block_poller = start_block_poller(
        query_provider.clone(),
        clock.clone(),
        Duration::from_millis(args.scrape_interval_ms),
        start_block,
    );

    match &args.input {
        Some(path) => {
            let mut source = FileSource::new(path).wrap_err("failed to open input file")?;
            send_from_source(&mut source, &mut sender, &metrics, &config, &mut reporters).await?;
        }
        None => {
            let mut source = StdinSource::new();
            send_from_source(&mut source, &mut sender, &metrics, &config, &mut reporters).await?;
        }
    }

    sender.flush().await;

    // Stop the block poller and collect markers.
    let block_markers = block_poller.drain().await;

    // Stop the scraper before finalizing.
    if let Some(handle) = scraper_handle {
        tracing::info!(
            scrapes = handle.scrape_count(),
            errors = handle.error_count(),
            "Stopping metrics scraper"
        );
        handle.stop().await;
    }

    let final_metrics = metrics.finalize().await;
    let time_series = metrics.time_series().await;

    // Drain all collected samples and apply metadata as labels.
    let samples = store.drain().await;

    // Collect per-block stats from the chain. The range starts one block after
    // the block that was current before sending (start_block is the last
    // existing block at that point, so start_block+1 is the first block that
    // could contain our transactions) and ends at the current latest block.
    let end_block = query_provider
        .get_block_number()
        .await
        .wrap_err("failed to get ending block number")?;

    let mut report = FinalReport {
        metadata: metadata.clone(),
        bench_metrics: Some(final_metrics),
        time_series: Some(time_series),
        samples,
        block_markers,
        ..Default::default()
    };

    report.apply_labels(&metadata);

    if end_block > start_block {
        let block_range_start = start_block + 1;
        tracing::info!(
            start = block_range_start,
            end = end_block,
            "Collecting per-block stats"
        );

        let block_stats = collect_block_stats(query_provider, block_range_start, end_block).await?;

        for block in &block_stats {
            for reporter in reporters.iter_mut() {
                reporter.on_block(block)?;
            }
        }

        report.run_stats = Some(RunStats::from_blocks(&block_stats));
        report.blocks = block_stats;
    }

    for reporter in &mut reporters {
        reporter.finalize(&report)?;
    }

    Ok(())
}

/// Parse `key=value` metadata strings into a HashMap.
pub(crate) fn parse_metadata(args: &[String]) -> Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    for arg in args {
        let (key, value) = arg
            .split_once('=')
            .ok_or_else(|| eyre::eyre!("invalid metadata format: {arg}"))?;
        if key.is_empty() {
            bail!("metadata key cannot be empty: {arg}");
        }
        map.insert(key.to_string(), value.to_string());
    }
    Ok(map)
}

async fn send_from_source<S: TxSource>(
    source: &mut S,
    sender: &mut Sender,
    metrics: &MetricsCollector,
    config: &SenderConfig,
    reporters: &mut [Box<dyn Reporter>],
) -> Result<()> {
    while let Some(tx) = source.next_tx().await? {
        sender.send(tx).await?;

        let (sent, success, failed) = metrics.counts();
        if sent % 1000 == 0 {
            let state = ProgressState {
                sent,
                success,
                failed,
                elapsed: metrics.elapsed_since_start(),
                max_concurrent: config.max_concurrent,
                target_tps: (config.rate_limit > 0).then_some(config.rate_limit),
            };
            for reporter in reporters.iter_mut() {
                reporter.on_progress(&state)?;
            }
        }
    }
    Ok(())
}
