//! `bench send-blocks` - Submit blocks via reth Engine API
//!
//! Reads NDJSON `{raw, key, number, timestamp, gas_used, gas_limit, tx_count}`
//! lines from stdin or file (produced by `txgen extract`). Lines may include
//! optional RLP-encoded `bal` bytes. Submits each block via `reth_newPayload`
//! (as `BlockRlp`) and `reth_forkchoiceUpdated`,
//! collecting per-block timing and engine status from [`RethPayloadStatus`].

mod reorg;

use crate::{
    metrics_forwarder::{build_metrics_forwarder, finish_metrics_forwarder, push_samples},
    metrics_url::metrics_scraper_configs,
    send::parse_metadata,
    SendBlocksArgs,
};
use alloy_network::Ethereum;
use alloy_primitives::{Bytes, B256};
use alloy_provider::{ext::TestingApi, Provider, RootProvider};
use alloy_rpc_types_engine::{ExecutionData, ForkchoiceState, JwtSecret};
use alloy_transport_http::{AuthLayer, Http, HyperClient};
use bench_core::{
    parse_reporters, start_scrapers, BigBlockData, BlockStats, ConsoleReporter, FinalReport,
    ProgressState, Reporter, RethApi, RethNewPayloadInput, RunClock, RunStats, Sample, SampleStore,
    WaitForPersistence,
};
use eyre::{Context, Result};
use reorg::{drive_reorg_state_machine, ReorgStateMachine};
use std::{
    collections::BTreeMap,
    io::BufRead,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::io::{AsyncBufReadExt, BufReader};

/// NDJSON line from the block source (`txgen extract` output).
#[derive(serde::Deserialize)]
pub(crate) struct BlockLine {
    /// RLP-encoded block bytes (hex with 0x prefix).
    raw: Bytes,
    /// Optional RLP-encoded block access list bytes (hex with 0x prefix).
    #[serde(default)]
    bal: Option<Bytes>,
    /// Block hash.
    key: B256,
    /// Block number.
    number: u64,
    /// Block timestamp in seconds.
    timestamp: u64,
    /// Gas used by the block.
    gas_used: u64,
    /// Gas limit of the block.
    gas_limit: u64,
    /// Number of transactions in the block.
    tx_count: usize,
}

/// NDJSON line accepted by `bench send-blocks`.
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum InputLine {
    /// Raw RLP block line produced by `txgen extract`.
    RawBlock(BlockLine),
    /// Big-block payload line produced by `txgen extract-big-blocks`.
    BigBlock(Box<BigBlockData<ExecutionData>>),
}

struct ProcessingState<'a> {
    collector: &'a mut MetricsCollector,
    reorg_state: Option<&'a mut ReorgStateMachine>,
    persistence_policy: &'a WaitForPersistence,
}

pub async fn execute(args: SendBlocksArgs) -> Result<()> {
    let jwt_secret_hex =
        tokio::fs::read_to_string(&args.jwt_secret).await.wrap_err("failed to read JWT secret")?;
    let jwt_secret =
        JwtSecret::from_hex(jwt_secret_hex.trim()).wrap_err("invalid JWT secret hex")?;

    let metadata = parse_metadata(&args.metadata)?;
    let scraper_configs =
        metrics_scraper_configs(&args.metrics_url, Duration::from_millis(args.scrape_interval_ms))?;
    let persistence_policy = args.wait_for_persistence;
    let reorg_every = args.reorg.map(|depth| args.reorg_every.unwrap_or(depth));

    tracing::info!(
        engine = %args.engine,
        input = args.input.as_ref().map_or("<stdin>", |p| p.to_str().unwrap_or("?")),
        wait_for_persistence = ?persistence_policy,
        wait_time_ms = args.wait_time.map(|d| d.as_millis()),
        reorg_depth = args.reorg,
        reorg_every,
        rpc = %args.rpc,
        "Starting block submission"
    );

    let hyper_client = HyperClient::new().layer(AuthLayer::new(jwt_secret));
    let transport = Http::with_client(hyper_client, args.engine.parse()?);
    let provider = RootProvider::<Ethereum>::new(alloy_rpc_client::RpcClient::new(transport, true));
    let testing_provider =
        RootProvider::<Ethereum>::new_http(args.rpc.parse().wrap_err("invalid RPC URL")?);

    let mut reporters = parse_reporters(&args.reports, "send-blocks", &metadata)?;
    if reporters.is_empty() {
        reporters.push(Box::new(ConsoleReporter::stderr(false)));
    }

    let clock = RunClock::new();
    let store = SampleStore::with_labels(metadata.clone())?;
    let counters = Arc::new(BlockCounters::default());
    let metrics_forwarder =
        build_metrics_forwarder(args.metrics_forward.as_deref(), &metadata, &scraper_configs)?;

    // Start background scraper if metrics URL is configured.
    let scraper_handles = if !scraper_configs.is_empty() {
        let snap_counters = counters.clone();
        let snap_clock = clock.clone();
        let callback: bench_core::SampleCallback =
            Arc::new(move || snap_counters.snapshot_samples(&snap_clock));
        let forwarder_handle = metrics_forwarder.as_ref().map(|f| f.handle());

        start_scrapers(&scraper_configs, clock.clone(), store.clone(), callback, forwarder_handle)
    } else {
        Vec::new()
    };

    let mut collector = MetricsCollector::new(counters);
    let mut reorg_state =
        args.reorg.map(|depth| ReorgStateMachine::new(depth, reorg_every.unwrap_or(depth)));
    let start = Instant::now();

    if let Some(ref path) = args.input {
        let file = std::fs::File::open(path).wrap_err("failed to open input file")?;
        let reader = std::io::BufReader::new(file);

        for line in reader.lines() {
            let line = line.wrap_err("failed to read line")?;
            let input = parse_input_line(&line)?;

            process_input_and_wait(
                &provider,
                &testing_provider,
                input,
                ProcessingState {
                    collector: &mut collector,
                    reorg_state: reorg_state.as_mut(),
                    persistence_policy: &persistence_policy,
                },
                args.wait_time,
                start,
                &mut reporters,
            )
            .await?;
        }
    } else {
        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin);
        let mut line_buf = String::new();

        loop {
            line_buf.clear();
            let bytes_read =
                reader.read_line(&mut line_buf).await.wrap_err("failed to read from stdin")?;
            if bytes_read == 0 {
                break;
            }

            let input = parse_input_line(&line_buf)?;

            process_input_and_wait(
                &provider,
                &testing_provider,
                input,
                ProcessingState {
                    collector: &mut collector,
                    reorg_state: reorg_state.as_mut(),
                    persistence_policy: &persistence_policy,
                },
                args.wait_time,
                start,
                &mut reporters,
            )
            .await?;
        }
    }

    if let Some(reorg_state) = reorg_state.as_mut() {
        drive_reorg_state_machine(
            &provider,
            &testing_provider,
            reorg_state,
            &mut collector,
            &persistence_policy,
            args.wait_time,
            start,
            &mut reporters,
            true,
        )
        .await?;
    }

    // Stop the scraper before finalizing.
    if !scraper_handles.is_empty() {
        tracing::info!(
            scrapers = scraper_handles.len(),
            scrapes = scraper_handles.iter().map(|h| h.scrape_count()).sum::<u64>(),
            errors = scraper_handles.iter().map(|h| h.error_count()).sum::<u64>(),
            "Stopping metrics scrapers"
        );
        for handle in scraper_handles {
            handle.stop().await;
        }
    }

    // Push a final snapshot so counter totals are captured even if the
    // last scraper tick fired before the final block was submitted.
    let final_samples = collector.final_snapshot(&clock);
    let forwarder_handle = metrics_forwarder.as_ref().map(|f| f.handle());
    push_samples(&store, forwarder_handle.as_ref(), final_samples).await?;

    let sample_archive = store.finish().await?;

    let blocks = std::mem::take(&mut collector.blocks);
    let run_stats = RunStats::from_blocks_wall_time(&blocks, start.elapsed());

    for block in &blocks {
        for reporter in reporters.iter_mut() {
            reporter.on_block(block)?;
        }
    }

    let report = FinalReport {
        metadata: metadata.clone(),
        sample_archive: Some(sample_archive),
        blocks,
        run_stats: Some(run_stats),
        ..Default::default()
    };

    let mut finalize_result = Ok(());
    for reporter in reporters.iter_mut() {
        if let Err(err) = reporter.finalize(&report) {
            finalize_result = Err(err);
            break;
        }
    }

    let forwarder_result = finish_metrics_forwarder(metrics_forwarder).await;

    finalize_result?;
    forwarder_result?;
    Ok(())
}

fn parse_input_line(line: &str) -> Result<InputLine> {
    serde_json::from_str(line.trim()).wrap_err("failed to parse NDJSON line")
}

fn report_progress(
    collector: &MetricsCollector,
    start: Instant,
    reporters: &mut [Box<dyn Reporter>],
) -> Result<()> {
    let state = ProgressState {
        sent: collector.blocks_submitted(),
        success: collector.blocks_success(),
        failed: collector.blocks_failed(),
        elapsed: start.elapsed(),
        max_concurrent: 0,
        target_tps: None,
        unit: "block",
    };
    for reporter in reporters.iter_mut() {
        reporter.on_progress(&state)?;
    }
    Ok(())
}

async fn process_input_and_wait(
    provider: &(impl Provider + RethApi<Ethereum>),
    testing_provider: &(impl Provider + TestingApi<Ethereum>),
    input: InputLine,
    state: ProcessingState<'_>,
    wait_time: Option<Duration>,
    start: Instant,
    reporters: &mut [Box<dyn Reporter>],
) -> Result<()> {
    let ProcessingState { collector, reorg_state, persistence_policy } = state;
    if let Some(reorg_state) = reorg_state {
        let InputLine::RawBlock(block) = input else {
            eyre::bail!("--reorg is only supported for raw RLP block input");
        };
        reorg_state.push(block);
        return drive_reorg_state_machine(
            provider,
            testing_provider,
            reorg_state,
            collector,
            persistence_policy,
            wait_time,
            start,
            reporters,
            false,
        )
        .await;
    }

    let block_start = Instant::now();

    match &input {
        InputLine::RawBlock(block) => {
            process_block(provider, block, collector, None, persistence_policy).await?
        }
        InputLine::BigBlock(big_block) => {
            process_big_block(provider, big_block, collector, persistence_policy).await?
        }
    }
    report_progress(collector, start, reporters)?;
    wait_for_next_block(block_start, wait_time).await;
    Ok(())
}

async fn wait_for_next_block(block_start: Instant, wait_time: Option<Duration>) {
    if let Some(wait_time) = wait_time {
        let remaining = wait_time.saturating_sub(block_start.elapsed());
        if !remaining.is_zero() {
            tokio::time::sleep(remaining).await;
        }
    }
}

async fn process_block(
    provider: &(impl Provider + RethApi<Ethereum>),
    block: &BlockLine,
    collector: &mut MetricsCollector,
    forkchoice_anchor: Option<B256>,
    persistence_policy: &WaitForPersistence,
) -> Result<()> {
    let input = RethNewPayloadInput::BlockRlp { block: block.raw.clone(), bal: block.bal.clone() };
    let wait = persistence_policy.should_wait(collector.blocks_submitted());

    let new_payload_start = Instant::now();
    let payload_status =
        provider.reth_new_payload(input, wait).await.wrap_err("reth_newPayload failed")?;
    let new_payload_latency = new_payload_start.elapsed();

    if !payload_status.status.is_valid() {
        collector.record_failure();
        eyre::bail!(
            "reth_newPayload returned non-VALID status for block {}: {:?}",
            block.number,
            payload_status.status,
        );
    }

    let (safe_block_hash, finalized_block_hash) = if let Some(anchor) = forkchoice_anchor {
        (anchor, anchor)
    } else {
        let safe_hash = collector.prev_block_hash.unwrap_or(block.key);
        if collector.finalized_hash.is_none() {
            collector.finalized_hash = Some(block.key);
        }
        // SAFETY: finalized_hash is always Some after the check above.
        (safe_hash, collector.finalized_hash.unwrap())
    };

    let forkchoice_state =
        ForkchoiceState { head_block_hash: block.key, safe_block_hash, finalized_block_hash };
    let fcu_start = Instant::now();
    let fcu_result = provider
        .reth_forkchoice_updated(forkchoice_state)
        .await
        .wrap_err("reth_forkchoiceUpdated failed")?;
    let fcu_latency = fcu_start.elapsed();

    if !fcu_result.is_valid() {
        collector.record_failure();
        eyre::bail!(
            "reth_forkchoiceUpdated returned non-VALID status for block {}: {:?}",
            block.number,
            fcu_result.payload_status,
        );
    }

    let total_latency = new_payload_latency + fcu_latency;
    let payload_status_str = payload_status.status.status.to_string();

    let timestamp_ms = block.timestamp * 1000;
    let block_time_ms = collector.prev_timestamp_ms.map(|prev| timestamp_ms.saturating_sub(prev));
    collector.prev_timestamp_ms = Some(timestamp_ms);

    let block_stats = BlockStats {
        number: block.number,
        timestamp_ms,
        tx_count: block.tx_count,
        gas_used: block.gas_used,
        gas_limit: block.gas_limit,
        block_time_ms,
        new_payload_ms: Some(new_payload_latency.as_millis() as u64),
        forkchoice_updated_ms: Some(fcu_latency.as_millis() as u64),
        new_payload_server_latency_us: Some(payload_status.latency_us),
        persistence_wait_us: payload_status.persistence_wait_us,
        execution_cache_wait_us: payload_status.execution_cache_wait_us,
        sparse_trie_wait_us: payload_status.sparse_trie_wait_us,
    };

    collector.record_success(block_stats);

    tracing::info!(
        block = block.number,
        txs = block.tx_count,
        gas = block.gas_used,
        new_payload_ms = new_payload_latency.as_millis(),
        forkchoice_updated_ms = fcu_latency.as_millis(),
        total_ms = total_latency.as_millis(),
        new_payload_server_latency_us = payload_status.latency_us,
        status = %payload_status_str,
        "Submitted block"
    );

    collector.prev_block_hash = Some(block.key);

    if block.number.is_multiple_of(32) {
        collector.finalized_hash = Some(block.key);
    }

    Ok(())
}

async fn process_big_block(
    provider: &(impl Provider + RethApi<Ethereum>),
    big_block: &BigBlockData<ExecutionData>,
    collector: &mut MetricsCollector,
    persistence_policy: &WaitForPersistence,
) -> Result<()> {
    let first_payload = big_block
        .env_switches
        .first()
        .ok_or_else(|| eyre::eyre!("big-block payload contains no execution payloads"))?;
    let last_payload = big_block
        .env_switches
        .last()
        .ok_or_else(|| eyre::eyre!("big-block payload contains no execution payloads"))?;

    let block_hash = last_payload.block_hash();
    let block_number = big_block.block_number;
    let tx_count = big_block.env_switches.iter().map(|data| data.transaction_count()).sum();
    let gas_used = big_block.env_switches.iter().map(|data| data.payload.as_v1().gas_used).sum();
    let gas_limit = big_block.env_switches.iter().map(|data| data.payload.gas_limit()).sum();
    let wait = persistence_policy.should_wait(collector.blocks_submitted());

    let input = RethNewPayloadInput::BigBlockData(Box::new(big_block.clone()));

    let new_payload_start = Instant::now();
    let payload_status =
        provider.reth_new_payload(input, wait).await.wrap_err("reth_newPayload failed")?;
    let new_payload_latency = new_payload_start.elapsed();

    if !payload_status.status.is_valid() {
        collector.record_failure();
        eyre::bail!(
            "reth_newPayload returned non-VALID status for big block {}: {:?}",
            block_number,
            payload_status.status,
        );
    }

    let forkchoice_state = ForkchoiceState {
        head_block_hash: block_hash,
        safe_block_hash: block_hash,
        finalized_block_hash: block_hash,
    };

    let fcu_start = Instant::now();
    let fcu_result = provider
        .reth_forkchoice_updated(forkchoice_state)
        .await
        .wrap_err("reth_forkchoiceUpdated failed")?;
    let fcu_latency = fcu_start.elapsed();

    if !fcu_result.is_valid() {
        collector.record_failure();
        eyre::bail!(
            "reth_forkchoiceUpdated returned non-VALID status for big block {}: {:?}",
            block_number,
            fcu_result.payload_status,
        );
    }

    let timestamp_ms = first_payload.payload.timestamp() * 1000;
    let block_time_ms = collector.prev_timestamp_ms.map(|prev| timestamp_ms.saturating_sub(prev));
    collector.prev_timestamp_ms = Some(timestamp_ms);

    let block_stats = BlockStats {
        number: block_number,
        timestamp_ms,
        tx_count,
        gas_used,
        gas_limit,
        block_time_ms,
        new_payload_ms: Some(new_payload_latency.as_millis() as u64),
        forkchoice_updated_ms: Some(fcu_latency.as_millis() as u64),
        new_payload_server_latency_us: Some(payload_status.latency_us),
        persistence_wait_us: payload_status.persistence_wait_us,
        execution_cache_wait_us: payload_status.execution_cache_wait_us,
        sparse_trie_wait_us: payload_status.sparse_trie_wait_us,
    };

    collector.record_success(block_stats);

    tracing::info!(
        block = block_number,
        txs = tx_count,
        gas = gas_used,
        new_payload_ms = new_payload_latency.as_millis(),
        forkchoice_updated_ms = fcu_latency.as_millis(),
        total_ms = (new_payload_latency + fcu_latency).as_millis(),
        new_payload_server_latency_us = payload_status.latency_us,
        status = %payload_status.status.status,
        "Submitted big block"
    );

    collector.prev_block_hash = Some(block_hash);
    if block_number.is_multiple_of(32) {
        collector.finalized_hash = Some(block_hash);
    }

    Ok(())
}

/// Shared atomic counters for the scraper snapshot callback.
#[derive(Debug, Default)]
struct BlockCounters {
    submitted: AtomicU64,
    success: AtomicU64,
    failed: AtomicU64,
}

impl BlockCounters {
    fn snapshot_samples(&self, clock: &RunClock) -> Vec<Sample> {
        let submitted = self.submitted.load(Ordering::Relaxed);
        let success = self.success.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        let offset_ms = clock.offset_ms();
        let unix_ms = clock.unix_ms();
        let labels = BTreeMap::new();

        vec![
            Sample {
                name: "txgen_blocks_sent_total".to_string(),
                labels: labels.clone(),
                value: submitted as f64,
                offset_ms,
                unix_ms,
            },
            Sample {
                name: "txgen_blocks_success_total".to_string(),
                labels: labels.clone(),
                value: success as f64,
                offset_ms,
                unix_ms,
            },
            Sample {
                name: "txgen_blocks_failed_total".to_string(),
                labels,
                value: failed as f64,
                offset_ms,
                unix_ms,
            },
        ]
    }
}

/// Block submission state and metrics collector.
struct MetricsCollector {
    counters: Arc<BlockCounters>,
    prev_block_hash: Option<B256>,
    finalized_hash: Option<B256>,
    prev_timestamp_ms: Option<u64>,
    blocks: Vec<BlockStats>,
}

impl MetricsCollector {
    fn new(counters: Arc<BlockCounters>) -> Self {
        Self {
            counters,
            prev_block_hash: None,
            finalized_hash: None,
            prev_timestamp_ms: None,
            blocks: Vec::new(),
        }
    }

    fn record_success(&mut self, stats: BlockStats) {
        self.counters.submitted.fetch_add(1, Ordering::Relaxed);
        self.counters.success.fetch_add(1, Ordering::Relaxed);
        self.blocks.push(stats);
    }

    fn record_failure(&self) {
        self.counters.submitted.fetch_add(1, Ordering::Relaxed);
        self.counters.failed.fetch_add(1, Ordering::Relaxed);
    }

    fn blocks_submitted(&self) -> u64 {
        self.counters.submitted.load(Ordering::Relaxed)
    }

    fn blocks_success(&self) -> u64 {
        self.counters.success.load(Ordering::Relaxed)
    }

    fn blocks_failed(&self) -> u64 {
        self.counters.failed.load(Ordering::Relaxed)
    }

    fn final_snapshot(&self, clock: &RunClock) -> Vec<Sample> {
        self.counters.snapshot_samples(clock)
    }
}
