//! `bench send-blocks` - Submit blocks via reth Engine API
//!
//! Reads NDJSON `{raw, key}` lines from stdin or file, where `raw` is
//! RLP-encoded block bytes. Submits each block via `reth_newPayload`
//! (as `BlockRlp`) and `reth_forkchoiceUpdated`, collecting per-block
//! metrics from [`RethPayloadStatus`].

use crate::SendBlocksArgs;
use alloy_consensus::{Block as ConsensusBlock, TxEnvelope};
use alloy_network::Ethereum;
use alloy_primitives::{B256, Bytes};
use alloy_provider::{Provider, RootProvider};
use alloy_rlp::Decodable;
use alloy_rpc_types_engine::{ForkchoiceState, JwtSecret};
use alloy_transport_http::{AuthLayer, Http, HyperClient};
use bench_core::{
    ConsoleReporter, LatencyStats, ReplayBlockStats, ReplayRunStats, Reporter, RethApi,
    RethNewPayloadInput, compute_latency_stats, parse_reporters,
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

    tracing::info!(
        engine = %args.engine,
        input = args.input.as_ref().map_or("<stdin>", |p| p.to_str().unwrap_or("?")),
        "Starting block submission"
    );

    let hyper_client = HyperClient::new().layer(AuthLayer::new(jwt_secret));
    let transport = Http::with_client(hyper_client, args.engine.parse()?);
    let provider = RootProvider::<Ethereum>::new(alloy_rpc_client::RpcClient::new(transport, true));

    let mut reporters = parse_reporters(&args.reports)?;
    let mut console_reporter: Box<dyn Reporter> = Box::new(ConsoleReporter::stderr(false));

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
                &mut reporters,
                &mut prev_block_hash,
                &mut finalized_hash,
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
                &mut reporters,
                &mut prev_block_hash,
                &mut finalized_hash,
            )
            .await?;
        }
    }

    let metrics = collector.finalize();

    let run_stats = ReplayRunStats {
        blocks_replayed: metrics.blocks_submitted,
        total_txs: metrics.total_txs,
        total_gas: metrics.total_gas,
        total_duration_ms: metrics.total_execution_time.as_millis() as u64,
        blocks_per_second: metrics.blocks_per_second(),
        mgas_per_second: metrics.mgas_per_second(),
        ggas_per_second: metrics.ggas_per_second(),
        new_payload_latency: metrics.new_payload_stats.clone(),
        fcu_latency: metrics.fcu_stats.clone(),
        block_time: metrics.block_time_stats.clone(),
    };

    for reporter in reporters.iter_mut() {
        reporter.finalize_replay(&run_stats)?;
    }
    console_reporter.finalize_replay(&run_stats)?;

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

#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_block(
    provider: &(impl Provider + RethApi<Ethereum>),
    block_bytes: Bytes,
    meta: &BlockMeta,
    collector: &mut MetricsCollector,
    reporters: &mut [Box<dyn Reporter>],
    prev_block_hash: &mut Option<B256>,
    finalized_hash: &mut Option<B256>,
) -> Result<()> {
    let input = RethNewPayloadInput::BlockRlp(block_bytes);

    let new_payload_start = Instant::now();
    let payload_status = provider
        .reth_new_payload(input)
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

    collector.record_block(BlockMetrics {
        tx_count: meta.tx_count as u64,
        gas_used: meta.gas_used,
        new_payload_latency,
        fcu_latency,
        total_latency,
        server_latency_us: payload_status.latency_us,
        server_persistence_wait_us: payload_status.persistence_wait_us,
        server_execution_cache_wait_us: payload_status.execution_cache_wait_us,
        server_sparse_trie_wait_us: payload_status.sparse_trie_wait_us,
    });

    for reporter in reporters.iter_mut() {
        reporter.on_replay_block(&ReplayBlockStats {
            number: meta.number,
            timestamp: meta.timestamp,
            tx_count: meta.tx_count,
            gas_used: meta.gas_used,
            gas_limit: meta.gas_limit,
            new_payload_ms: new_payload_latency.as_millis() as u64,
            fcu_ms: fcu_latency.as_millis() as u64,
            total_latency_ms: total_latency.as_millis() as u64,
            payload_status: payload_status_str.clone(),
        })?;
    }

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

/// Metrics for a single submitted block.
#[derive(Debug, Clone)]
pub(crate) struct BlockMetrics {
    tx_count: u64,
    gas_used: u64,
    new_payload_latency: Duration,
    fcu_latency: Duration,
    total_latency: Duration,
    #[allow(dead_code)]
    server_latency_us: u64,
    #[allow(dead_code)]
    server_persistence_wait_us: Option<u64>,
    #[allow(dead_code)]
    server_execution_cache_wait_us: Option<u64>,
    #[allow(dead_code)]
    server_sparse_trie_wait_us: Option<u64>,
}

/// Aggregated metrics collector.
#[derive(Debug, Default)]
pub(crate) struct MetricsCollector {
    blocks_submitted: u64,
    total_txs: u64,
    total_gas: u128,
    total_execution_time: Duration,
    new_payload_latencies: Vec<Duration>,
    fcu_latencies: Vec<Duration>,
    block_times: Vec<Duration>,
}

impl MetricsCollector {
    pub(crate) fn record_block(&mut self, block: BlockMetrics) {
        self.blocks_submitted += 1;
        self.total_txs += block.tx_count;
        self.total_gas += block.gas_used as u128;
        self.total_execution_time += block.total_latency;
        self.new_payload_latencies.push(block.new_payload_latency);
        self.fcu_latencies.push(block.fcu_latency);
        self.block_times.push(block.total_latency);
    }

    pub(crate) fn finalize(self) -> FinalMetrics {
        FinalMetrics {
            blocks_submitted: self.blocks_submitted,
            total_txs: self.total_txs,
            total_gas: self.total_gas,
            total_execution_time: self.total_execution_time,
            new_payload_stats: compute_latency_stats(&self.new_payload_latencies),
            fcu_stats: compute_latency_stats(&self.fcu_latencies),
            block_time_stats: compute_latency_stats(&self.block_times),
        }
    }
}

/// Finalized metrics with computed statistics.
#[derive(Debug)]
pub(crate) struct FinalMetrics {
    pub(crate) blocks_submitted: u64,
    pub(crate) total_txs: u64,
    pub(crate) total_gas: u128,
    pub(crate) total_execution_time: Duration,
    pub(crate) new_payload_stats: LatencyStats,
    pub(crate) fcu_stats: LatencyStats,
    pub(crate) block_time_stats: LatencyStats,
}

impl FinalMetrics {
    pub(crate) fn blocks_per_second(&self) -> f64 {
        if self.total_execution_time.as_secs_f64() > 0.0 {
            self.blocks_submitted as f64 / self.total_execution_time.as_secs_f64()
        } else {
            0.0
        }
    }

    pub(crate) fn mgas_per_second(&self) -> f64 {
        if self.total_execution_time.as_secs_f64() > 0.0 {
            (self.total_gas as f64 / 1_000_000.0) / self.total_execution_time.as_secs_f64()
        } else {
            0.0
        }
    }

    pub(crate) fn ggas_per_second(&self) -> f64 {
        if self.total_execution_time.as_secs_f64() > 0.0 {
            (self.total_gas as f64 / 1_000_000_000.0) / self.total_execution_time.as_secs_f64()
        } else {
            0.0
        }
    }
}
