//! `bench send` - Send transactions from file or stdin

use crate::{
    metrics_forwarder::{build_metrics_forwarder, finish_metrics_forwarder},
    metrics_url::metrics_scraper_configs,
    SendArgs,
};
use alloy_network::AnyNetwork;
use alloy_provider::{ext::TxPoolApi, DynProvider, Provider, ProviderBuilder};
use alloy_rpc_client::RpcClient;
use alloy_transport::layers::RetryBackoffLayer;
use bench_core::{
    collect_block_stats, parse_reporters, start_scrapers, trim_trailing_empty_blocks,
    ConsoleReporter, FileSource, FinalReport, GeneratedTx, MetricsCollector, ProgressState,
    Reporter, RequestAuthProvider, RpcEndpoint, RunClock, RunStats, SampleStore, ScraperConfig,
    Sender, SenderConfig, SenderHeaderAuthProvider, StdinSource, TxPhase, TxSource,
};
use eyre::{bail, Context, Result};
use std::{collections::HashMap, sync::Arc, time::Duration};

pub async fn execute(args: SendArgs) -> Result<()> {
    tracing::info!(
        input = args.input.as_ref().map(|p| p.display().to_string()).as_deref().unwrap_or("stdin"),
        rpc_urls = ?args.rpc_urls,
        tps = args.tps,
        skip_setup = args.skip_setup,
        collect_latencies = args.collect_latencies,
        retries = args.retries.map_or("forever".to_string(), |retries| retries.to_string()),
        "Starting send"
    );

    let metadata = parse_metadata(&args.metadata)?;
    let scraper_configs =
        metrics_scraper_configs(&args.metrics_url, Duration::from_millis(args.scrape_interval_ms))?;

    // CU/s set to u64::MAX to disable the layer's built-in rate limiting
    // while keeping retry-on-429 behavior. The benchmarking tool has its own
    // rate limiter and typically targets local nodes that don't rate-limit.
    let retry_layer = RetryBackoffLayer::new(args.retries.unwrap_or(u32::MAX), 100, u64::MAX);
    let http_client = reqwest::Client::builder()
        .timeout(args.timeout)
        .build()
        .wrap_err("failed to build RPC HTTP client")?;
    let providers = args
        .rpc_urls
        .iter()
        .map(|url| build_provider(url, &http_client, &retry_layer))
        .collect::<Result<Vec<_>>>()?;
    let endpoints = args
        .rpc_urls
        .iter()
        .zip(providers.iter().cloned())
        .map(|(url, provider)| RpcEndpoint::new(url.clone(), provider))
        .collect::<Vec<_>>();
    let query_provider = match args.query_rpc_url.as_deref() {
        Some(url) => build_provider(url, &http_client, &retry_layer)
            .wrap_err("failed to build query RPC provider")?,
        None => providers[0].clone(),
    };
    let request_auth = build_request_auth(&args)?;

    match &args.input {
        Some(path) => {
            let mut source = FileSource::new(path).wrap_err("failed to open input file")?;
            execute_source(
                &args,
                &metadata,
                endpoints,
                query_provider,
                request_auth,
                &mut source,
                &scraper_configs,
            )
            .await
        }
        None => {
            let mut source = StdinSource::new();
            execute_source(
                &args,
                &metadata,
                endpoints,
                query_provider,
                request_auth,
                &mut source,
                &scraper_configs,
            )
            .await
        }
    }
}

fn build_provider(
    url: &str,
    http_client: &reqwest::Client,
    retry_layer: &RetryBackoffLayer,
) -> Result<DynProvider<AnyNetwork>> {
    let parsed = url.parse().context("failed to parse RPC URL")?;
    let client = RpcClient::builder()
        .layer(retry_layer.clone())
        .http_with_client(http_client.clone(), parsed);
    Ok(ProviderBuilder::new_with_network::<AnyNetwork>().connect_client(client).erased())
}

fn build_request_auth(args: &SendArgs) -> Result<Option<Arc<dyn RequestAuthProvider>>> {
    match (&args.sender_header_name, &args.sender_header_map) {
        (None, None) => Ok(None),
        (Some(header_name), Some(path)) => Ok(Some(Arc::new(SenderHeaderAuthProvider::from_file(
            header_name,
            path,
            args.sender_header_reload_interval,
        )?))),
        (Some(_), None) => Err(eyre::eyre!("--sender-header-name requires --sender-header-map")),
        (None, Some(_)) => Err(eyre::eyre!("--sender-header-map requires --sender-header-name")),
    }
}

async fn execute_source<S: TxSource>(
    args: &SendArgs,
    metadata: &HashMap<String, String>,
    endpoints: Vec<RpcEndpoint>,
    query_provider: DynProvider<AnyNetwork>,
    request_auth: Option<Arc<dyn RequestAuthProvider>>,
    source: &mut S,
    scraper_configs: &[ScraperConfig],
) -> Result<()> {
    let config = SenderConfig { rate_limit: args.tps, max_concurrent: args.max_concurrent };

    let first_workload =
        run_setup_phase(args, source, &endpoints, request_auth.clone(), &config).await?;

    let clock = if let Some(start) = args.metrics_align {
        RunClock::new_with_start_unix_ms(start)
    } else {
        RunClock::new()
    };
    let store = SampleStore::with_labels(metadata.clone())?;
    let metrics = MetricsCollector::new_with_latencies(clock.clone(), args.collect_latencies);
    let metrics_forwarder =
        build_metrics_forwarder(args.metrics_forward.as_deref(), metadata, scraper_configs)?;

    // Start background scraper + internal snapshotter after setup so setup is
    // excluded from benchmark metrics.
    let scraper_handles = if !scraper_configs.is_empty() {
        let snap_metrics = metrics.clone();
        let callback: bench_core::SampleCallback =
            std::sync::Arc::new(move || snap_metrics.snapshot_samples());
        let forwarder_handle = metrics_forwarder.as_ref().map(|f| f.handle());

        start_scrapers(scraper_configs, clock.clone(), store.clone(), callback, forwarder_handle)
    } else {
        Vec::new()
    };

    let mut sender =
        Sender::new_with_request_auth(endpoints, config.clone(), metrics.clone(), request_auth);

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

    sender.flush().await?;

    let (sent, success, failed) = metrics.counts();
    tracing::info!(sent, success, failed, "Bench send completed; starting post-processing");

    // Wait for the txpool to drain so all transactions are included in blocks
    // before we collect block stats. The scraper and block poller keep running.
    if args.drain_timeout > 0 {
        wait_for_pool_drain(&query_provider, args.drain_timeout).await?;
        tracing::info!("Txpool drain completed");
    } else {
        tracing::info!(reason = "--drain-timeout=0", "Skipped txpool drain");
    }

    // Stop the scraper before finalizing.
    if !scraper_handles.is_empty() {
        let scrapers = scraper_handles.len();
        let scrapes = scraper_handles.iter().map(|h| h.scrape_count()).sum::<u64>();
        let errors = scraper_handles.iter().map(|h| h.error_count()).sum::<u64>();
        for handle in scraper_handles {
            handle.stop().await;
        }
        tracing::info!(scrapers, scrapes, errors, "Metrics scrapers stopped");
    } else {
        tracing::info!(reason = "no metrics scrapers", "Skipped metrics scraper stop");
    }

    let final_metrics = metrics.finalize().await;
    tracing::info!("Metrics finalized");

    let time_series = metrics.time_series().await;
    tracing::info!("Time series built");

    // Finalize the sample archive before reporters read it.
    let sample_archive = store.finish().await?;
    tracing::info!("Sample archive finalized");

    // Collect per-block stats from the chain. The range starts one block after
    // the block that was current before sending (start_block is the last
    // existing block at that point, so start_block+1 is the first block that
    // could contain our transactions) and ends at the current latest block.
    let end_block =
        query_provider.get_block_number().await.wrap_err("failed to get ending block number")?;
    tracing::info!(end_block, "Ending block fetched");

    let mut report = FinalReport {
        metadata: metadata.clone(),
        bench_metrics: Some(final_metrics),
        time_series: Some(time_series),
        sample_archive: Some(sample_archive),
        ..Default::default()
    };

    if end_block > start_block {
        let block_range_start = start_block + 1;
        let mut block_stats =
            collect_block_stats(&query_provider, block_range_start, end_block).await?;
        tracing::info!(
            start = block_range_start,
            end = end_block,
            blocks = block_stats.len(),
            "Block stats collected"
        );

        // Trim trailing empty blocks (system-only, gas_used == 0) that
        // accumulated during the txpool drain wait. Also trim metric
        // samples captured after the last real block.
        let cutoff_ms = trim_trailing_empty_blocks(&mut block_stats);
        if let Some(cutoff_ms) = cutoff_ms {
            report.retain_samples_until(cutoff_ms)?;
            if let Some(ts) = report.time_series.as_mut() {
                ts.latencies
                    .retain(|l| l.offset_ms <= cutoff_ms.saturating_sub(clock.start_unix_ms()));
                ts.throughput
                    .retain(|t| t.second * 1000 <= cutoff_ms.saturating_sub(clock.start_unix_ms()));
            }
        }
        tracing::info!(cutoff_ms = ?cutoff_ms, "Report trimmed");

        for block in &block_stats {
            for reporter in reporters.iter_mut() {
                reporter.on_block(block)?;
            }
        }
        tracing::info!(blocks = block_stats.len(), "Block reporter events emitted");

        report.run_stats = Some(RunStats::from_blocks_chain_time(&block_stats));
        tracing::info!("Run stats built");
        report.blocks = block_stats;
    } else {
        tracing::info!(reason = "no new blocks", "Skipped block stats collection");
        tracing::info!(reason = "no block stats", "Skipped report trim");
        tracing::info!(reason = "no block stats", "Skipped block reporter events");
        tracing::info!(reason = "no block stats", "Skipped run stats build");
    }

    let mut finalize_result = Ok(());
    for reporter in &mut reporters {
        if let Err(err) = reporter.finalize(&report) {
            finalize_result = Err(err);
            break;
        }
    }
    tracing::info!("Reporters finalized");

    tracing::info!("Post-processing completed");

    let forwarder_result = finish_metrics_forwarder(metrics_forwarder).await;

    finalize_result?;
    forwarder_result?;
    Ok(())
}

async fn run_setup_phase<S: TxSource>(
    args: &SendArgs,
    source: &mut S,
    endpoints: &[RpcEndpoint],
    request_auth: Option<Arc<dyn RequestAuthProvider>>,
    config: &SenderConfig,
) -> Result<Option<GeneratedTx>> {
    let setup_clock = RunClock::new();
    let setup_metrics = MetricsCollector::new_with_latencies(setup_clock, false);
    let mut setup_sender = Sender::new_with_request_auth(
        endpoints.to_vec(),
        config.clone(),
        setup_metrics.clone(),
        request_auth,
    );
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
                finish_setup_phase(args, setup_seen, &mut setup_sender, &setup_metrics).await?;
                return Ok(Some(tx));
            }
        }
    }

    finish_setup_phase(args, setup_seen, &mut setup_sender, &setup_metrics).await?;
    Ok(None)
}

async fn finish_setup_phase(
    args: &SendArgs,
    setup_seen: u64,
    setup_sender: &mut Sender,
    setup_metrics: &MetricsCollector,
) -> Result<()> {
    if setup_seen == 0 {
        return Ok(());
    }

    if args.skip_setup {
        tracing::info!(setup_txs = setup_seen, "Skipped setup transactions");
        return Ok(());
    }

    tracing::info!(setup_txs = setup_seen, "Waiting for setup transactions");
    setup_sender.flush().await?;

    let (_, _, failed) = setup_metrics.counts();
    if failed > 0 {
        bail!("setup phase failed: {failed} setup transaction(s) failed or reverted");
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
