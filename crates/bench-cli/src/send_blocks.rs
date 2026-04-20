//! `bench send-blocks` - Submit blocks via reth Engine API
//!
//! Reads NDJSON `{raw, key}` lines from stdin or file, where `raw` is
//! RLP-encoded block bytes. Submits each block via `reth_newPayload`
//! (as `BlockRlp`) and `reth_forkchoiceUpdated`, collecting per-block
//! metrics from [`RethPayloadStatus`].

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
    ConsoleReporter, FinalReport, Reporter, RethApi, RethNewPayloadInput, RunClock, SampleStore,
    ScraperConfig, WaitForPersistence, parse_reporters, start_scraper,
};
use eyre::{Context, Result};
use std::io::BufRead;
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
    pub(crate) gas_used: u64,
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
    let mut console_reporter: Box<dyn Reporter> = Box::new(ConsoleReporter::stderr(false));

    let clock = RunClock::new();
    let store = SampleStore::new();

    // Start background scraper if metrics URL is configured.
    let scraper_handle = if let Some(ref url) = args.metrics_url {
        let scraper_config =
            ScraperConfig::new(url).with_interval(Duration::from_millis(args.scrape_interval_ms));
        let handle = start_scraper(scraper_config, clock.clone(), store.clone(), None);
        tracing::info!(url, "Started metrics scraper");
        Some(handle)
    } else {
        None
    };

    let mut collector = MetricsCollector::default();
    let mut prev_block_hash: Option<B256> = None;
    let mut finalized_hash: Option<B256> = None;

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
                &mut prev_block_hash,
                &mut finalized_hash,
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
                &mut prev_block_hash,
                &mut finalized_hash,
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

    let mut report = FinalReport {
        metadata: metadata.clone(),
        samples,
        ..Default::default()
    };

    report.apply_labels(&metadata);

    for reporter in reporters.iter_mut() {
        reporter.finalize(&report)?;
    }
    console_reporter.finalize(&report)?;

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
        gas_used: block.header.gas_used,
        tx_count: block.body.transactions.len(),
    })
}

pub(crate) async fn process_block(
    provider: &(impl Provider + RethApi<Ethereum>),
    block_bytes: Bytes,
    meta: &BlockMeta,
    collector: &mut MetricsCollector,
    prev_block_hash: &mut Option<B256>,
    finalized_hash: &mut Option<B256>,
    persistence_policy: &WaitForPersistence,
) -> Result<()> {
    let input = RethNewPayloadInput::BlockRlp(block_bytes);
    let wait = persistence_policy.should_wait(collector.blocks_submitted);

    let new_payload_start = Instant::now();
    let payload_status = provider
        .reth_new_payload(input, wait)
        .await
        .wrap_err("reth_newPayload failed")?;
    let new_payload_latency = new_payload_start.elapsed();

    if !payload_status.status.is_valid() && !payload_status.status.is_syncing() {
        tracing::warn!(
            block = meta.number,
            status = ?payload_status.status,
            "reth_newPayload returned non-VALID status"
        );
    }

    let safe_hash = prev_block_hash.unwrap_or(meta.hash);
    if finalized_hash.is_none() {
        *finalized_hash = Some(meta.hash);
    }

    let forkchoice_state = ForkchoiceState {
        head_block_hash: meta.hash,
        safe_block_hash: safe_hash,
        // SAFETY: finalized_hash is always Some after the check above
        finalized_block_hash: finalized_hash.unwrap(),
    };

    let fcu_start = Instant::now();
    let fcu_result = provider
        .reth_forkchoice_updated(forkchoice_state)
        .await
        .wrap_err("reth_forkchoiceUpdated failed")?;
    let fcu_latency = fcu_start.elapsed();

    if !fcu_result.is_valid() && !fcu_result.is_syncing() {
        tracing::warn!(
            block = meta.number,
            status = ?fcu_result.payload_status,
            "reth_forkchoiceUpdated returned non-VALID status"
        );
    }

    let total_latency = new_payload_latency + fcu_latency;
    let payload_status_str = payload_status.status.status.to_string();

    collector.record_block();

    tracing::info!(
        block = meta.number,
        txs = meta.tx_count,
        gas = meta.gas_used,
        new_payload_ms = new_payload_latency.as_millis(),
        fcu_ms = fcu_latency.as_millis(),
        total_ms = total_latency.as_millis(),
        server_latency_us = payload_status.latency_us,
        status = %payload_status_str,
        "Submitted block"
    );

    *prev_block_hash = Some(meta.hash);

    if meta.number % 32 == 0 {
        *finalized_hash = Some(meta.hash);
    }

    Ok(())
}

/// Aggregated metrics collector.
///
/// Tracks `blocks_submitted` for the persistence policy.
#[derive(Debug, Default)]
pub(crate) struct MetricsCollector {
    blocks_submitted: u64,
}

impl MetricsCollector {
    pub(crate) fn record_block(&mut self) {
        self.blocks_submitted += 1;
    }
}
