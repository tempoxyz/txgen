//! `bench send-blocks` - Submit blocks via reth Engine API
//!
//! Reads NDJSON `{raw, key, number, timestamp, gas_used, gas_limit, tx_count}`
//! lines from stdin or file (produced by `txgen extract`). Submits each block
//! via `reth_newPayload` (as `BlockRlp`) and `reth_forkchoiceUpdated`,
//! collecting per-block timing and engine status from [`RethPayloadStatus`].

use crate::{send::parse_metadata, SendBlocksArgs};
use alloy_network::Ethereum;
use alloy_primitives::{Bytes, B256};
use alloy_provider::{Provider, RootProvider};
use alloy_rpc_types_engine::{ForkchoiceState, JwtSecret};
use alloy_transport_http::{AuthLayer, Http, HyperClient};
use bench_core::{
    parse_reporters, start_scraper, BlockStats, ConsoleReporter, FinalReport, ProgressState,
    Reporter, RethApi, RethNewPayloadInput, RunClock, RunStats, Sample, SampleStore, ScraperConfig,
    WaitForPersistence,
};
use eyre::{Context, Result};
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
struct BlockLine {
    /// RLP-encoded block bytes (hex with 0x prefix).
    raw: Bytes,
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

pub async fn execute(args: SendBlocksArgs) -> Result<()> {
    let jwt_secret_hex =
        tokio::fs::read_to_string(&args.jwt_secret).await.wrap_err("failed to read JWT secret")?;
    let jwt_secret =
        JwtSecret::from_hex(jwt_secret_hex.trim()).wrap_err("invalid JWT secret hex")?;

    let metadata = parse_metadata(&args.metadata)?;
    let persistence_policy = args.wait_for_persistence;

    tracing::info!(
        engine = %args.engine,
        input = args.input.as_ref().map_or("<stdin>", |p| p.to_str().unwrap_or("?")),
        wait_for_persistence = ?persistence_policy,
        wait_time_ms = args.wait_time.map(|d| d.as_millis()),
        "Starting block submission"
    );

    let hyper_client = HyperClient::new().layer(AuthLayer::new(jwt_secret));
    let transport = Http::with_client(hyper_client, args.engine.parse()?);
    let provider = RootProvider::<Ethereum>::new(alloy_rpc_client::RpcClient::new(transport, true));

    let mut reporters = parse_reporters(&args.reports, "send-blocks", &metadata)?;
    if reporters.is_empty() {
        reporters.push(Box::new(ConsoleReporter::stderr(false)));
    }

    let clock = RunClock::new();
    let store = SampleStore::new();
    let counters = Arc::new(BlockCounters::default());

    // Start background scraper if metrics URL is configured.
    let scraper_handle = if let Some(ref url) = args.metrics_url {
        let scraper_config =
            ScraperConfig::new(url).with_interval(Duration::from_millis(args.scrape_interval_ms));

        let snap_counters = counters.clone();
        let snap_clock = clock.clone();
        let callback: bench_core::SampleCallback =
            Arc::new(move || snap_counters.snapshot_samples(&snap_clock));

        let handle = start_scraper(scraper_config, clock.clone(), store.clone(), Some(callback));
        tracing::info!(url, "Started metrics scraper");
        Some(handle)
    } else {
        None
    };

    let mut collector = MetricsCollector::new(counters);
    let start = Instant::now();

    if let Some(ref path) = args.input {
        let file = std::fs::File::open(path).wrap_err("failed to open input file")?;
        let reader = std::io::BufReader::new(file);

        for line in reader.lines() {
            let line = line.wrap_err("failed to read line")?;
            let block = parse_block_line(&line)?;

            process_block_and_wait(
                &provider,
                &block,
                &mut collector,
                &persistence_policy,
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

            let block = parse_block_line(&line_buf)?;

            process_block_and_wait(
                &provider,
                &block,
                &mut collector,
                &persistence_policy,
                args.wait_time,
                start,
                &mut reporters,
            )
            .await?;
        }
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

    // Push a final snapshot so counter totals are captured even if the
    // last scraper tick fired before the final block was submitted.
    let final_samples = collector.final_snapshot(&clock);
    store.push_batch(final_samples).await;

    let samples = store.drain().await;

    let blocks = std::mem::take(&mut collector.blocks);
    let run_stats = RunStats::from_blocks_wall_time(&blocks, start.elapsed());

    for block in &blocks {
        for reporter in reporters.iter_mut() {
            reporter.on_block(block)?;
        }
    }

    let mut report = FinalReport {
        metadata: metadata.clone(),
        samples,
        blocks,
        run_stats: Some(run_stats),
        ..Default::default()
    };

    report.apply_labels(&metadata);

    for reporter in reporters.iter_mut() {
        reporter.finalize(&report)?;
    }

    Ok(())
}

fn parse_block_line(line: &str) -> Result<BlockLine> {
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

async fn process_block_and_wait(
    provider: &(impl Provider + RethApi<Ethereum>),
    block: &BlockLine,
    collector: &mut MetricsCollector,
    persistence_policy: &WaitForPersistence,
    wait_time: Option<Duration>,
    start: Instant,
    reporters: &mut [Box<dyn Reporter>],
) -> Result<()> {
    let block_start = Instant::now();

    process_block(provider, block, collector, persistence_policy).await?;
    report_progress(collector, start, reporters)?;

    if let Some(wait_time) = wait_time {
        let remaining = wait_time.saturating_sub(block_start.elapsed());
        if !remaining.is_zero() {
            tokio::time::sleep(remaining).await;
        }
    }

    Ok(())
}

async fn process_block(
    provider: &(impl Provider + RethApi<Ethereum>),
    block: &BlockLine,
    collector: &mut MetricsCollector,
    persistence_policy: &WaitForPersistence,
) -> Result<()> {
    let input = RethNewPayloadInput::BlockRlp(block.raw.clone());
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

    let safe_hash = collector.prev_block_hash.unwrap_or(block.key);
    if collector.finalized_hash.is_none() {
        collector.finalized_hash = Some(block.key);
    }

    let forkchoice_state = ForkchoiceState {
        head_block_hash: block.key,
        safe_block_hash: safe_hash,
        // SAFETY: finalized_hash is always Some after the check above
        finalized_block_hash: collector.finalized_hash.unwrap(),
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
