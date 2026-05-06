//! `bench send` - Send transactions from file or stdin

use crate::SendArgs;
use alloy_network::AnyNetwork;
use alloy_provider::{ext::TxPoolApi, DynProvider, Provider, ProviderBuilder};
use alloy_rpc_client::RpcClient;
use alloy_transport::layers::RetryBackoffLayer;
use bench_core::{
    collect_block_stats, parse_reporters, start_scraper, trim_trailing_empty_blocks,
    ConsoleReporter, FileSource, FinalReport, GeneratedTx, MetricsCollector, ProgressState,
    Reporter, RunClock, RunStats, SampleStore, ScraperConfig, Sender, SenderConfig, StdinSource,
    TxPhase, TxSource,
};
use eyre::{bail, Context, Result};
use std::{collections::HashMap, time::Duration};

pub async fn execute(args: SendArgs) -> Result<()> {
    tracing::info!(
        input = args.input.as_ref().map(|p| p.display().to_string()).as_deref().unwrap_or("stdin"),
        rpc_urls = ?args.rpc_urls,
        tps = args.tps,
        skip_setup = args.skip_setup,
        "Starting send"
    );

    let metadata = parse_metadata(&args.metadata)?;

    // CU/s set to u64::MAX to disable the layer's built-in rate limiting
    // while keeping retry-on-429 behavior. The benchmarking tool has its own
    // rate limiter and typically targets local nodes that don't rate-limit.
    let retry_layer = RetryBackoffLayer::new(10, 100, u64::MAX);
    let http_client = reqwest::Client::builder()
        .timeout(args.timeout)
        .build()
        .wrap_err("failed to build RPC HTTP client")?;
    let providers = args
        .rpc_urls
        .iter()
        .map(|url| {
            let url = url.parse().context("failed to parse RPC URL")?;
            let client = RpcClient::builder()
                .layer(retry_layer.clone())
                .http_with_client(http_client.clone(), url);
            Ok(ProviderBuilder::new_with_network::<AnyNetwork>().connect_client(client).erased())
        })
        .collect::<Result<Vec<_>>>()?;

    match &args.input {
        Some(path) => {
            let mut source = FileSource::new(path).wrap_err("failed to open input file")?;
            execute_source(&args, &metadata, providers, &mut source).await
        }
        None => {
            let mut source = StdinSource::new();
            execute_source(&args, &metadata, providers, &mut source).await
        }
    }
}

async fn execute_source<S: TxSource>(
    args: &SendArgs,
    metadata: &HashMap<String, String>,
    providers: Vec<DynProvider<AnyNetwork>>,
    source: &mut S,
) -> Result<()> {
    let config = SenderConfig { rate_limit: args.tps, max_concurrent: args.max_concurrent };
    let query_provider = &providers[0];

    let first_workload = run_setup_phase(args, source, &providers, &config, query_provider).await?;

    let clock = RunClock::new();
    let store = SampleStore::new();
    let metrics = MetricsCollector::new(clock.clone());

    // Start background scraper + internal snapshotter after setup so setup is
    // excluded from benchmark metrics.
    let scraper_handle = if let Some(ref url) = args.metrics_url {
        let scraper_config =
            ScraperConfig::new(url).with_interval(Duration::from_millis(args.scrape_interval_ms));

        let snap_metrics = metrics.clone();
        let callback: bench_core::SampleCallback =
            std::sync::Arc::new(move || snap_metrics.snapshot_samples());

        let handle = start_scraper(scraper_config, clock.clone(), store.clone(), Some(callback));

        tracing::info!(url, "Started metrics scraper");
        Some(handle)
    } else {
        None
    };

    let mut sender = Sender::new(providers.clone(), config.clone(), metrics.clone());

    let mut reporters = parse_reporters(&args.reports, "send", metadata)?;
    if reporters.is_empty() {
        reporters.push(Box::new(ConsoleReporter::stderr(true)));
    }

    // Record the block number after setup and before workload sending so per-block
    // stats exclude setup blocks.
    let start_block =
        query_provider.get_block_number().await.wrap_err("failed to get starting block number")?;

    if let Some(tx) = first_workload {
        send_workload_tx(tx, &mut sender, &metrics, &config, &mut reporters).await?;
    }
    send_workload_from_source(source, &mut sender, &metrics, &config, &mut reporters).await?;

    sender.flush().await;

    // Wait for the txpool to drain so all transactions are included in blocks
    // before we collect block stats. The scraper and block poller keep running.
    if args.drain_timeout > 0 {
        wait_for_pool_drain(query_provider, args.drain_timeout).await?;
    }

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
    let end_block =
        query_provider.get_block_number().await.wrap_err("failed to get ending block number")?;

    let mut report = FinalReport {
        metadata: metadata.clone(),
        bench_metrics: Some(final_metrics),
        time_series: Some(time_series),
        samples,
        ..Default::default()
    };

    report.apply_labels(metadata);

    if end_block > start_block {
        let block_range_start = start_block + 1;
        tracing::info!(start = block_range_start, end = end_block, "Collecting per-block stats");

        let mut block_stats =
            collect_block_stats(query_provider, block_range_start, end_block).await?;

        // Trim trailing empty blocks (system-only, gas_used == 0) that
        // accumulated during the txpool drain wait. Also trim metric
        // samples captured after the last real block.
        if let Some(cutoff_ms) = trim_trailing_empty_blocks(&mut block_stats) {
            report.samples.retain(|s| s.unix_ms <= cutoff_ms);
            if let Some(ts) = report.time_series.as_mut() {
                ts.latencies
                    .retain(|l| l.offset_ms <= cutoff_ms.saturating_sub(clock.start_unix_ms()));
                ts.throughput
                    .retain(|t| t.second * 1000 <= cutoff_ms.saturating_sub(clock.start_unix_ms()));
            }
        }

        for block in &block_stats {
            for reporter in reporters.iter_mut() {
                reporter.on_block(block)?;
            }
        }

        report.run_stats = Some(RunStats::from_blocks_chain_time(&block_stats));
        report.blocks = block_stats;
    }

    for reporter in &mut reporters {
        reporter.finalize(&report)?;
    }

    Ok(())
}

async fn run_setup_phase<S: TxSource, P: TxPoolApi<AnyNetwork>>(
    args: &SendArgs,
    source: &mut S,
    providers: &[DynProvider<AnyNetwork>],
    config: &SenderConfig,
    query_provider: &P,
) -> Result<Option<GeneratedTx>> {
    let setup_clock = RunClock::new();
    let setup_metrics = MetricsCollector::new(setup_clock);
    let mut setup_sender = Sender::new(providers.to_vec(), config.clone(), setup_metrics.clone());
    let mut setup_seen = 0u64;

    while let Some(tx) = source.next_tx().await? {
        match tx.phase {
            TxPhase::Setup if args.skip_setup => {
                setup_seen += 1;
                tracing::debug!(id = tx.id.as_deref(), "Skipping setup transaction");
            }
            TxPhase::Setup => {
                setup_seen += 1;
                setup_sender.send(tx).await?;
            }
            TxPhase::Workload => {
                finish_setup_phase(
                    args,
                    setup_seen,
                    &mut setup_sender,
                    &setup_metrics,
                    query_provider,
                )
                .await?;
                return Ok(Some(tx));
            }
        }
    }

    finish_setup_phase(args, setup_seen, &mut setup_sender, &setup_metrics, query_provider).await?;
    Ok(None)
}

async fn finish_setup_phase<P: TxPoolApi<AnyNetwork>>(
    args: &SendArgs,
    setup_seen: u64,
    setup_sender: &mut Sender,
    setup_metrics: &MetricsCollector,
    query_provider: &P,
) -> Result<()> {
    if setup_seen == 0 {
        return Ok(());
    }

    if args.skip_setup {
        tracing::info!(setup_txs = setup_seen, "Skipped setup transactions");
        return Ok(());
    }

    tracing::info!(setup_txs = setup_seen, "Waiting for setup transactions");
    setup_sender.flush().await;

    let (_, _, failed) = setup_metrics.counts();
    if failed > 0 {
        bail!("setup phase failed: {failed} setup transaction(s) failed or reverted");
    }

    if args.drain_timeout > 0 {
        wait_for_pool_drain(query_provider, args.drain_timeout).await?;
    } else {
        tracing::warn!("Skipping setup txpool drain because --drain-timeout=0");
    }

    Ok(())
}

/// Parse `key=value` metadata strings into a HashMap.
pub(crate) fn parse_metadata(args: &[String]) -> Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    for arg in args {
        let (key, value) =
            arg.split_once('=').ok_or_else(|| eyre::eyre!("invalid metadata format: {arg}"))?;
        if key.is_empty() {
            bail!("metadata key cannot be empty: {arg}");
        }
        map.insert(key.to_string(), value.to_string());
    }
    Ok(map)
}

async fn send_workload_from_source<S: TxSource>(
    source: &mut S,
    sender: &mut Sender,
    metrics: &MetricsCollector,
    config: &SenderConfig,
    reporters: &mut [Box<dyn Reporter>],
) -> Result<()> {
    while let Some(tx) = source.next_tx().await? {
        if tx.phase == TxPhase::Setup {
            bail!("setup transaction appeared after workload started");
        }
        send_workload_tx(tx, sender, metrics, config, reporters).await?;
    }
    Ok(())
}

async fn send_workload_tx(
    tx: GeneratedTx,
    sender: &mut Sender,
    metrics: &MetricsCollector,
    config: &SenderConfig,
    reporters: &mut [Box<dyn Reporter>],
) -> Result<()> {
    sender.send(tx).await?;

    let (sent, success, failed) = metrics.counts();
    if sent.is_multiple_of(1000) {
        let state = ProgressState {
            sent,
            success,
            failed,
            elapsed: metrics.elapsed_since_start(),
            max_concurrent: config.max_concurrent,
            target_tps: (config.rate_limit > 0).then_some(config.rate_limit),
            unit: "tx",
        };
        for reporter in reporters.iter_mut() {
            reporter.on_progress(&state)?;
        }
    }

    Ok(())
}

/// Wait for the transaction pool to drain (pending count reaches zero).
///
/// Polls `txpool_status` every second. Returns after 3 consecutive zero
/// readings, or fails if polling fails or the timeout is reached.
async fn wait_for_pool_drain<P: TxPoolApi<AnyNetwork>>(
    provider: &P,
    timeout_secs: u64,
) -> Result<()> {
    tracing::info!(timeout_secs, "Waiting for txpool to drain...");

    let mut zero_count: u32 = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);

    loop {
        if tokio::time::Instant::now() >= deadline {
            bail!("txpool drain timeout reached after {timeout_secs}s");
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        let status = provider
            .txpool_status()
            .await
            .wrap_err("failed to query txpool_status while waiting for txpool drain")?;
        let pending = status.pending;

        if pending == 0 {
            zero_count += 1;
            if zero_count >= 3 {
                tracing::info!("Txpool drained (3 consecutive zero readings)");
                return Ok(());
            }
        } else {
            zero_count = 0;
            tracing::debug!(pending, "Txpool still draining...");
        }
    }
}
