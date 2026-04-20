//! `bench send-blocks` - Submit blocks via reth Engine API
//!
//! Reads NDJSON `{raw}` lines from stdin or file, where `raw` is
//! RLP-encoded block bytes. Submits each block via `reth_newPayload`
//! (as `BlockRlp`) and `reth_forkchoiceUpdated`, collecting per-block
//! timing and engine status from [`RethPayloadStatus`].

use crate::SendBlocksArgs;
use crate::send::parse_metadata;
use alloy_consensus::{Block as ConsensusBlock, TxEnvelope};
use alloy_network::Ethereum;
use alloy_primitives::{B256, Bytes};
use alloy_provider::{Provider, RootProvider};
use alloy_rlp::Decodable;
use alloy_rpc_types_engine::{ForkchoiceState, JwtSecret};
use alloy_transport_http::{AuthLayer, Http, HyperClient};
use bench_core::{
    BlockStats, ConsoleReporter, FinalReport, RethApi, RethNewPayloadInput, RunClock, RunStats,
    Sample, SampleStore, ScraperConfig, WaitForPersistence, parse_reporters, start_scraper,
};
use eyre::{Context, Result};
use std::collections::BTreeMap;
use std::io::BufRead;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};

/// NDJSON line from the block source.
#[derive(serde::Deserialize)]
struct BlockLine {
    /// RLP-encoded block bytes (hex with 0x prefix).
    raw: Bytes,
}

/// Decoded block metadata extracted from RLP.
pub(crate) struct BlockMeta {
    pub(crate) hash: B256,
    pub(crate) number: u64,
    pub(crate) timestamp: u64,
    pub(crate) gas_used: u64,
    pub(crate) gas_limit: u64,
    pub(crate) tx_count: usize,
}

pub async fn execute(args: SendBlocksArgs) -> Result<()> {
    let jwt_secret_hex = tokio::fs::read_to_string(&args.jwt_secret)
        .await
        .wrap_err("failed to read JWT secret")?;
    let jwt_secret =
        JwtSecret::from_hex(jwt_secret_hex.trim()).wrap_err("invalid JWT secret hex")?;

    let metadata = parse_metadata(&args.metadata)?;
    let persistence_policy = args.wait_for_persistence;

    tracing::info!(
        engine = %args.engine,
        input = args.input.as_ref().map_or("<stdin>", |p| p.to_str().unwrap_or("?")),
        wait_for_persistence = ?persistence_policy,
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

    if let Some(ref path) = args.input {
        let file = std::fs::File::open(path).wrap_err("failed to open input file")?;
        let reader = std::io::BufReader::new(file);

        for line in reader.lines() {
            let line = line.wrap_err("failed to read line")?;
            let block_bytes = parse_block_line(&line)?;
            let meta = decode_block_meta(&block_bytes)?;

            process_block(
                &provider,
                block_bytes,
                &meta,
                &mut collector,
                &persistence_policy,
            )
            .await?;
        }
    } else {
        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin);
        let mut line_buf = String::new();

        loop {
            line_buf.clear();
            let bytes_read = reader
                .read_line(&mut line_buf)
                .await
                .wrap_err("failed to read from stdin")?;
            if bytes_read == 0 {
                break;
            }

            let block_bytes = parse_block_line(&line_buf)?;
            let meta = decode_block_meta(&block_bytes)?;

            process_block(
                &provider,
                block_bytes,
                &meta,
                &mut collector,
                &persistence_policy,
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

    let samples = store.drain().await;

    let blocks = std::mem::take(&mut collector.blocks);
    let run_stats = RunStats::from_blocks(&blocks);

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

fn parse_block_line(line: &str) -> Result<Bytes> {
    let block_line: BlockLine =
        serde_json::from_str(line.trim()).wrap_err("failed to parse NDJSON line")?;
    Ok(block_line.raw)
}

pub(crate) fn decode_block_meta(rlp_bytes: &[u8]) -> Result<BlockMeta> {
    let mut buf = rlp_bytes;
    let block =
        ConsensusBlock::<TxEnvelope>::decode(&mut buf).wrap_err("failed to RLP-decode block")?;
    let hash = block.header.hash_slow();

    Ok(BlockMeta {
        hash,
        number: block.header.number,
        timestamp: block.header.timestamp,
        gas_used: block.header.gas_used,
        gas_limit: block.header.gas_limit,
        tx_count: block.body.transactions.len(),
    })
}

pub(crate) async fn process_block(
    provider: &(impl Provider + RethApi<Ethereum>),
    block_bytes: Bytes,
    meta: &BlockMeta,
    collector: &mut MetricsCollector,
    persistence_policy: &WaitForPersistence,
) -> Result<()> {
    let input = RethNewPayloadInput::BlockRlp(block_bytes);
    let wait = persistence_policy.should_wait(collector.blocks_submitted());

    let new_payload_start = Instant::now();
    let payload_status = provider
        .reth_new_payload(input, wait)
        .await
        .wrap_err("reth_newPayload failed")?;
    let new_payload_latency = new_payload_start.elapsed();

    if !payload_status.status.is_valid() {
        collector.record_failure();
        eyre::bail!(
            "reth_newPayload returned non-VALID status for block {}: {:?}",
            meta.number,
            payload_status.status,
        );
    }

    let safe_hash = collector.prev_block_hash.unwrap_or(meta.hash);
    if collector.finalized_hash.is_none() {
        collector.finalized_hash = Some(meta.hash);
    }

    let forkchoice_state = ForkchoiceState {
        head_block_hash: meta.hash,
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
            meta.number,
            fcu_result.payload_status,
        );
    }

    let total_latency = new_payload_latency + fcu_latency;
    let payload_status_str = payload_status.status.status.to_string();

    let timestamp_ms = meta.timestamp * 1000;
    let block_time_ms = collector
        .prev_timestamp_ms
        .map(|prev| timestamp_ms.saturating_sub(prev));
    collector.prev_timestamp_ms = Some(timestamp_ms);

    let block_stats = BlockStats {
        number: meta.number,
        timestamp_ms,
        tx_count: meta.tx_count,
        gas_used: meta.gas_used,
        gas_limit: meta.gas_limit,
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
        block = meta.number,
        txs = meta.tx_count,
        gas = meta.gas_used,
        new_payload_ms = new_payload_latency.as_millis(),
        forkchoice_updated_ms = fcu_latency.as_millis(),
        total_ms = total_latency.as_millis(),
        new_payload_server_latency_us = payload_status.latency_us,
        status = %payload_status_str,
        "Submitted block"
    );

    collector.prev_block_hash = Some(meta.hash);

    if meta.number % 32 == 0 {
        collector.finalized_hash = Some(meta.hash);
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
pub(crate) struct MetricsCollector {
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

    pub(crate) fn record_success(&mut self, stats: BlockStats) {
        self.counters.submitted.fetch_add(1, Ordering::Relaxed);
        self.counters.success.fetch_add(1, Ordering::Relaxed);
        self.blocks.push(stats);
    }

    pub(crate) fn record_failure(&self) {
        self.counters.submitted.fetch_add(1, Ordering::Relaxed);
        self.counters.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn blocks_submitted(&self) -> u64 {
        self.counters.submitted.load(Ordering::Relaxed)
    }
}
