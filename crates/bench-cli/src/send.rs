//! `bench send` - Send transactions from file or stdin

use crate::{
    load_metric_names,
    metrics_forwarder::{build_metrics_forwarder, finish_metrics_forwarder},
    metrics_url::metrics_scraper_configs,
    warmup::WarmupPhase,
    SendArgs,
};
use alloy_network::AnyNetwork;
use alloy_provider::{ext::TxPoolApi, DynProvider, Provider, ProviderBuilder};
use alloy_rpc_client::RpcClient;
use alloy_transport::layers::RetryBackoffLayer;
use bench_core::{
    collect_block_stats, parse_reporters, start_scrapers, total_fees_paid,
    trim_trailing_empty_blocks, BlockReceiptCollector, ConsoleReporter, FileSource, FinalReport,
    GeneratedTx, LateSigner, MeasurementStart, MetricsCheckpoint, MetricsCollector, ProgressState,
    ReceiptTracker, Reporter, RequestAuthProvider, RpcEndpoint, RunClock, RunStats, SampleStore,
    ScraperConfig, Sender, SenderConfig, SenderHeaderAuthProvider, StdinSource, TxPhase, TxSource,
    WarmupOutcome, WarmupSummary,
};
use eyre::{bail, Context, Result};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use txgen_tempo::TempoLateSigner;

const SETUP_PROGRESS_INTERVAL: Duration = Duration::from_secs(5);

pub async fn execute(args: SendArgs) -> Result<()> {
    let max_pending = args.pending_limit()?;
    tracing::info!(
        input = args.input.as_ref().map(|p| p.display().to_string()).as_deref().unwrap_or("stdin"),
        rpc_urls = ?args.rpc_urls,
        tps = args.tps,
        max_pending = max_pending.map_or(0, |limit| limit.get()),
        skip_setup = args.skip_setup,
        collect_latencies = args.collect_latencies,
        collect_receipt_metrics = args.collect_receipt_metrics,
        retries = args.retries.map_or("forever".to_string(), |retries| retries.to_string()),
        "Starting send"
    );

    let mut metadata = parse_metadata(&args.metadata)?;
    metadata
        .insert("max_pending".to_string(), max_pending.map_or(0, |limit| limit.get()).to_string());
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
    let late_signer = args
        .late_signing_spec
        .as_deref()
        .map(TempoLateSigner::from_workload_file)
        .transpose()
        .wrap_err("failed to configure deferred signing")?
        .map(|signer| Arc::new(signer) as Arc<dyn LateSigner>);

    match &args.input {
        Some(path) => {
            let mut source = FileSource::new(path).wrap_err("failed to open input file")?;
            execute_source(
                &args,
                &metadata,
                endpoints,
                query_provider,
                request_auth,
                late_signer.clone(),
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
                late_signer,
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

#[allow(clippy::too_many_arguments)]
async fn execute_source<S: TxSource>(
    args: &SendArgs,
    metadata: &HashMap<String, String>,
    endpoints: Vec<RpcEndpoint>,
    query_provider: DynProvider<AnyNetwork>,
    request_auth: Option<Arc<dyn RequestAuthProvider>>,
    late_signer: Option<Arc<dyn LateSigner>>,
    source: &mut S,
    scraper_configs: &[ScraperConfig],
) -> Result<()> {
    let config = SenderConfig { rate_limit: args.tps, max_concurrent: args.max_concurrent };

    let receipt_tracker = ReceiptTracker::new(query_provider.clone());
    let first_workload = run_setup_phase(
        args,
        source,
        &endpoints,
        request_auth.clone(),
        late_signer.clone(),
        &config,
        receipt_tracker.clone(),
    )
    .await?;

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

    let receipt_collector = args.collect_receipt_metrics.then(BlockReceiptCollector::start);
    let mut sender =
        Sender::new_with_request_auth(endpoints, config.clone(), metrics.clone(), request_auth)
            .with_receipt_tracker(receipt_tracker)
            .with_transaction_expiry(Arc::new(txgen_tempo::transaction_expiry));
    if let Some(limit) = args.pending_limit()? {
        sender = sender.with_max_pending(limit);
    }
    if let Some(late_signer) = late_signer {
        sender = sender.with_late_signer(late_signer);
    }
    if let Some(collector) = &receipt_collector {
        sender = sender.with_receipt_collector(collector.handle());
    }

    let clickhouse_metric_names = load_metric_names(args.clickhouse_metrics_file.as_ref())?;
    let mut reporters = parse_reporters(&args.reports, "send", metadata, clickhouse_metric_names)?;
    if reporters.is_empty() {
        reporters.push(Box::new(ConsoleReporter::stderr(true)));
    }

    // Record the block number after setup and before workload sending so per-block
    // stats exclude setup blocks. A warm-up moves this to its boundary later.
    let start_block =
        query_provider.get_block_number().await.wrap_err("failed to get starting block number")?;

    let warmup_config = args.warmup_config()?;
    let warmup = if warmup_config.is_enabled() {
        tracing::info!(
            mode = %warmup_config.mode,
            ramp = ?warmup_config.ramp,
            min = ?warmup_config.min_duration,
            max = ?warmup_config.max_duration,
            proposals_per_proposer = warmup_config.proposals_per_proposer,
            expected_proposers = warmup_config.expected_proposers,
            stable_blocks = warmup_config.stable_blocks,
            stable_tolerance = warmup_config.stable_tolerance,
            pool_check = warmup_config.pool_check,
            warmup_tps = warmup_config.warmup_rate(),
            target_tps = warmup_config.target_tps,
            handoff_ramp = ?warmup_config.handoff_ramp(),
            "Warm-up started"
        );
        if warmup_config.warmup_rate() == 0 {
            tracing::info!(reason = "unlimited warm-up rate", "Skipped warm-up ramp");
        }
        if warmup_config.pool_check && warmup_config.is_rate_capped() {
            tracing::info!(
                reason = "warm-up rate is capped below the target",
                "Skipped warm-up pool check"
            );
        }
        let mut phase =
            WarmupPhase::start(warmup_config, query_provider.clone(), start_block, &clock);
        phase.apply_initial_rate(&mut sender).await;
        Some(phase)
    } else {
        tracing::info!(reason = "--warmup=off", "Skipped warm-up");
        None
    };

    let mut run = WorkloadRun {
        start_block,
        checkpoint: None,
        measurement_start_unix_ms: clock.start_unix_ms(),
        // Without a warm-up the measured window starts now; a warm-up moves it.
        measurement_started: Instant::now(),
        warmup: None,
    };
    {
        let mut ctx = WorkloadContext {
            sender: &mut sender,
            metrics: &metrics,
            config: &config,
            reporters: &mut reporters,
            warmup,
            run: &mut run,
            query_provider: &query_provider,
            clock: &clock,
            measure_duration: args.duration,
        };

        if let Some(tx) = first_workload {
            send_workload_tx(tx, &mut ctx).await?;
        }

        send_workload_from_source(source, &mut ctx).await?;

        // The source is drained, but the sender still holds a buffered backlog
        // (up to `max_concurrent x 4` transactions). Keep the warm-up ticking
        // while that backlog goes out so the boundary can still be reached;
        // give up once nothing is left to send.
        while ctx.warmup.is_some() {
            let remaining = ctx.sender.flush_step(Duration::from_millis(100)).await?;
            let outcome = match ctx.warmup.as_mut() {
                Some(phase) => phase.tick(ctx.sender).await,
                None => None,
            };
            if let Some(outcome) = outcome {
                finish_warmup(&mut ctx, outcome).await?;
                break;
            }
            if !remaining {
                break;
            }
        }

        if let Some(phase) = ctx.warmup.take() {
            let summary = phase.abandon().await;
            tracing::warn!(
                duration_secs = summary.duration_ms as f64 / 1000.0,
                blocks = summary.blocks,
                proposers_ready = summary.proposers_ready,
                proposers_expected = summary.expected_proposers,
                "Transaction source ended before the warm-up finished; reporting the whole run"
            );
            ctx.run.warmup = Some(summary);
        }
    }

    sender.flush().await?;
    drop(sender);

    // From here on, `start_block` is the last block before the measured window.
    let start_block = run.start_block;

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

    // Snapshot the range before post-processing starts. Receipt collection uses
    // one block-level request per block rather than polling each transaction.
    let end_block =
        query_provider.get_block_number().await.wrap_err("failed to get ending block number")?;
    tracing::info!(end_block, "Ending block fetched");

    let receipt_collection = match receipt_collector {
        Some(collector) => {
            let collection = collector
                .finish(&query_provider, start_block.saturating_add(1), end_block)
                .await
                .wrap_err("failed to collect block receipts")?;
            tracing::info!(
                groups = collection.metrics.len(),
                records = collection.records.len(),
                "Block receipt gas metrics finalized"
            );
            collection
        }
        None => {
            tracing::info!(
                reason = "--collect-receipt-metrics not set",
                "Skipped receipt gas metrics"
            );
            Default::default()
        }
    };
    let receipt_metrics = receipt_collection.metrics;
    let total_fees_paid = total_fees_paid(&receipt_collection.records);
    let receipt_records = receipt_collection.records;

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

    let final_metrics = metrics.finalize_since(run.checkpoint).await;
    tracing::info!("Metrics finalized");

    let time_series = metrics.time_series_since(run.checkpoint).await;
    tracing::info!("Time series built");

    // Finalize the sample archive before reporters read it.
    let sample_archive = store.finish().await?;
    tracing::info!("Sample archive finalized");

    // Collect per-block stats from the chain. The range starts one block after
    // the block that was current before sending (start_block is the last
    // existing block at that point, so start_block+1 is the first block that
    // could contain our transactions) and ends at the current latest block.
    let mut report = FinalReport {
        metadata: metadata.clone(),
        bench_metrics: Some(final_metrics),
        time_series: Some(time_series),
        sample_archive: Some(sample_archive),
        receipt_metrics,
        total_fees_paid,
        receipt_records,
        warmup: run.warmup.clone(),
        started_unix_ms: Some(run.measurement_start_unix_ms),
        ..Default::default()
    };

    // After a warm-up, drop samples from before the measured window and report
    // offsets relative to its start so they line up with the block range.
    if run.checkpoint.is_some() {
        report.rebase_samples_to(run.measurement_start_unix_ms)?;
    }

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
                let origin_ms = run.measurement_start_unix_ms;
                ts.latencies.retain(|l| l.offset_ms <= cutoff_ms.saturating_sub(origin_ms));
                ts.throughput.retain(|t| t.second * 1000 <= cutoff_ms.saturating_sub(origin_ms));
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
    late_signer: Option<Arc<dyn LateSigner>>,
    config: &SenderConfig,
    receipt_tracker: ReceiptTracker,
) -> Result<Option<GeneratedTx>> {
    let setup_clock = RunClock::new();
    let setup_metrics = MetricsCollector::new_with_latencies(setup_clock, false);
    let mut setup_sender = Sender::new_with_request_auth(
        endpoints.to_vec(),
        config.clone(),
        setup_metrics.clone(),
        request_auth,
    )
    .with_receipt_tracker(receipt_tracker)
    .with_transaction_expiry(Arc::new(txgen_tempo::transaction_expiry));
    if let Some(late_signer) = late_signer {
        setup_sender = setup_sender.with_late_signer(late_signer);
    }
    if let Some(limit) = args.pending_limit()? {
        setup_sender = setup_sender.with_max_pending(limit);
    }
    let mut setup_seen = 0u64;
    let mut setup = Vec::new();

    while let Some(tx) = source.next_tx().await? {
        match tx.phase {
            TxPhase::Setup if args.skip_setup => {
                setup_seen += 1;
                tracing::debug!(id = tx.id.as_deref(), "Skipping setup transaction");
            }
            TxPhase::Setup => {
                setup_seen += 1;
                setup.push(tx);
            }
            TxPhase::Workload => {
                setup_sender.send_setup(setup).await?;
                finish_setup_phase(args, setup_seen, &mut setup_sender, &setup_metrics).await?;
                return Ok(Some(tx));
            }
        }
    }

    setup_sender.send_setup(setup).await?;
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
    let mut progress = tokio::time::interval(SETUP_PROGRESS_INTERVAL);
    progress.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Consume the immediate first tick so progress is only reported after the
    // setup phase has actually been waiting for an interval.
    progress.tick().await;

    let flush = setup_sender.flush();
    tokio::pin!(flush);
    let flush_result = loop {
        tokio::select! {
            result = &mut flush => break result,
            _ = progress.tick() => {
                let (sent, success, failed) = setup_metrics.counts();
                tracing::info!(
                    setup_txs = setup_seen,
                    sent,
                    success,
                    failed,
                    in_flight = sent.saturating_sub(success + failed),
                    elapsed = ?setup_metrics.elapsed_since_start(),
                    "Setup transaction progress"
                );
            }
        }
    };
    flush_result?;

    let (sent, success, failed) = setup_metrics.counts();
    tracing::info!(
        setup_txs = setup_seen,
        sent,
        success,
        failed,
        elapsed = ?setup_metrics.elapsed_since_start(),
        "Setup transactions completed"
    );
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

/// Measurement bookkeeping. A warm-up moves all of it to its boundary.
struct WorkloadRun {
    /// Last block before the measured window; measured blocks start at `+1`.
    start_block: u64,
    /// Metrics origin when a warm-up ended, `None` for the whole run.
    checkpoint: Option<MetricsCheckpoint>,
    /// Wall-clock origin of the measured window in Unix milliseconds.
    measurement_start_unix_ms: u64,
    /// Monotonic origin of the measured window, for `--duration`.
    measurement_started: Instant,
    /// Warm-up summary, when a warm-up ran.
    warmup: Option<WarmupSummary>,
}

/// Everything the workload send loop touches.
struct WorkloadContext<'a> {
    sender: &'a mut Sender,
    metrics: &'a MetricsCollector,
    config: &'a SenderConfig,
    reporters: &'a mut [Box<dyn Reporter>],
    /// Running warm-up, `None` once it finished or when disabled.
    warmup: Option<WarmupPhase>,
    run: &'a mut WorkloadRun,
    query_provider: &'a DynProvider<AnyNetwork>,
    clock: &'a RunClock,
    /// `--duration`: stop taking transactions this long after the measured
    /// window started.
    measure_duration: Option<Duration>,
}

impl WorkloadContext<'_> {
    /// Whether `--duration` has elapsed since the measured window started.
    ///
    /// Never true while a warm-up is still running: the window has not started.
    fn measurement_deadline_reached(&self) -> bool {
        self.warmup.is_none() &&
            self.measure_duration
                .is_some_and(|duration| self.run.measurement_started.elapsed() >= duration)
    }
}

async fn send_workload_from_source<S: TxSource>(
    source: &mut S,
    ctx: &mut WorkloadContext<'_>,
) -> Result<()> {
    while let Some(tx) = source.next_tx().await? {
        if tx.phase == TxPhase::Setup {
            bail!("setup transaction appeared after workload started");
        }
        send_workload_tx(tx, ctx).await?;
        if ctx.measurement_deadline_reached() {
            // Queued transactions would keep the window open for as long as
            // they take to send at the configured rate; drop them and let the
            // final flush only wait for requests already on the wire.
            let dropped = ctx.sender.discard_queued();
            tracing::info!(
                duration = ?ctx.measure_duration.unwrap_or_default(),
                dropped_queued = dropped,
                "Measured window duration reached; stopping the transaction source"
            );
            break;
        }
    }
    Ok(())
}

async fn send_workload_tx(tx: GeneratedTx, ctx: &mut WorkloadContext<'_>) -> Result<()> {
    ctx.sender.send(tx).await?;

    let (sent, success, failed) = ctx.metrics.counts();
    if sent.is_multiple_of(1000) {
        // Report the live limit so the warm-up ramp is visible in progress output.
        let rate_limit = ctx.sender.rate_limit();
        let state = ProgressState {
            sent,
            success,
            failed,
            elapsed: ctx.metrics.elapsed_since_start(),
            max_concurrent: ctx.config.max_concurrent,
            target_tps: (rate_limit > 0).then_some(rate_limit),
            unit: "tx",
        };
        for reporter in ctx.reporters.iter_mut() {
            reporter.on_progress(&state)?;
        }
    }

    let outcome = match ctx.warmup.as_mut() {
        Some(phase) => phase.tick(ctx.sender).await,
        None => None,
    };
    if let Some(outcome) = outcome {
        finish_warmup(ctx, outcome).await?;
    }

    Ok(())
}

/// Move the measurement origin to now: the warm-up is over.
///
/// Sending continues uninterrupted; only the bookkeeping changes. Blocks up to
/// the current head belong to the warm-up, metrics recorded so far are
/// excluded through a checkpoint, and reporters learn the new `started_at`.
async fn finish_warmup(ctx: &mut WorkloadContext<'_>, outcome: WarmupOutcome) -> Result<()> {
    let Some(phase) = ctx.warmup.take() else {
        return Ok(());
    };

    // Whatever the ramps reached, the measured window runs at the configured
    // rate (0 removes the limiter again for an unlimited target).
    ctx.sender.set_rate_limit(ctx.config.rate_limit).await;

    let start_block = ctx
        .query_provider
        .get_block_number()
        .await
        .wrap_err("failed to get block number at the warm-up boundary")?;
    let ended_unix_ms = ctx.clock.unix_ms();
    let checkpoint = ctx.metrics.checkpoint();
    let mut summary = phase.finish(outcome, ended_unix_ms).await;
    summary.inflight_at_boundary = Some(checkpoint.inflight());

    tracing::info!(
        outcome = %summary.outcome,
        ready = summary.ready,
        duration_secs = summary.duration_ms as f64 / 1000.0,
        blocks = summary.blocks,
        proposers_ready = summary.proposers_ready,
        proposers_expected = summary.expected_proposers,
        full_block_threshold = summary.full_block_threshold,
        inflight_at_boundary = checkpoint.inflight(),
        first_measured_block = start_block + 1,
        "Warm-up finished; measurement window starts"
    );

    ctx.run.start_block = start_block;
    ctx.run.checkpoint = Some(checkpoint);
    ctx.run.measurement_start_unix_ms = ended_unix_ms;
    ctx.run.measurement_started = Instant::now();
    ctx.run.warmup = Some(summary.clone());

    let start = MeasurementStart {
        started_at: std::time::UNIX_EPOCH + Duration::from_millis(ended_unix_ms),
        first_block: start_block + 1,
        warmup: summary,
    };
    for reporter in ctx.reporters.iter_mut() {
        reporter.on_measurement_start(&start)?;
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
