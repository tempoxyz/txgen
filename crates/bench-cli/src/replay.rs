//! `bench replay` - Block replay via reth Engine API
//!
//! Replays historical blocks by:
//! 1. Fetching raw RLP-encoded block data from an archive node (with prefetching)
//! 2. Submitting via `reth_newPayload` (BlockRlp) + `reth_forkchoiceUpdated`
//! 3. Measuring execution time and collecting metrics

use crate::ReplayArgs;
use alloy_consensus::{Block as ConsensusBlock, TxEnvelope};
use alloy_network::Ethereum;
use alloy_primitives::{B256, Bytes};
use alloy_provider::{Provider, ProviderBuilder, RootProvider};
use alloy_rlp::Decodable;
use alloy_rpc_types_engine::{ForkchoiceState, JwtSecret};
use alloy_transport_http::{AuthLayer, Http, HyperClient};
use bench_core::{
    ConsoleReporter, LatencyStats, ReplayBlockStats, ReplayRunStats, Reporter, RethApi,
    RethNewPayloadInput, compute_latency_stats, parse_reporters,
};
use eyre::{Context, Result, bail};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Number of blocks to prefetch ahead.
const DEFAULT_PREFETCH_SIZE: usize = 20;

pub async fn execute(args: ReplayArgs) -> Result<()> {
    if args.from > args.to {
        bail!("--from must be <= --to");
    }

    let jwt_secret_hex = tokio::fs::read_to_string(&args.jwt_secret)
        .await
        .wrap_err("failed to read JWT secret")?;
    let jwt_secret =
        JwtSecret::from_hex(jwt_secret_hex.trim()).wrap_err("invalid JWT secret hex")?;

    tracing::info!(
        rpc_source = %args.rpc_source,
        engine = %args.engine,
        from = args.from,
        to = args.to,
        "Starting block replay"
    );

    let source_provider = ProviderBuilder::new()
        .connect(&args.rpc_source)
        .await
        .wrap_err("failed to connect to source RPC")?;

    let hyper_client = HyperClient::new().layer(AuthLayer::new(jwt_secret));
    let transport = Http::with_client(hyper_client, args.engine.parse()?);
    let engine_provider =
        RootProvider::<Ethereum>::new(alloy_rpc_client::RpcClient::new(transport, true));

    let mut reporters = parse_reporters(&args.reports)?;
    let mut console_reporter: Box<dyn Reporter> = Box::new(ConsoleReporter::stderr(false));

    let bench = ReplayBench::new(source_provider, engine_provider);
    let mode = BenchMode::Range {
        from: args.from,
        to: args.to,
    };

    let metrics = bench.run(mode, &mut reporters).await?;

    let run_stats = ReplayRunStats {
        blocks_replayed: metrics.blocks_replayed,
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

/// Benchmark mode.
#[derive(Debug, Clone)]
pub enum BenchMode {
    /// Replay a specific range of blocks.
    Range { from: u64, to: u64 },
}

/// Block replay benchmark.
pub struct ReplayBench<S, E> {
    source_provider: S,
    engine_provider: E,
}

impl<S, E> ReplayBench<S, E>
where
    S: Provider + Clone + Send + Sync + 'static,
    E: RethApi<Ethereum> + Send + Sync,
{
    /// Create a new replay benchmark.
    pub fn new(source_provider: S, engine_provider: E) -> Self {
        Self {
            source_provider,
            engine_provider,
        }
    }

    /// Run the benchmark.
    pub async fn run(
        &self,
        mode: BenchMode,
        reporters: &mut [Box<dyn Reporter>],
    ) -> Result<ReplayMetrics> {
        let BenchMode::Range { from, to } = mode;

        let (tx, mut rx) = mpsc::channel::<Result<FetchedBlock>>(DEFAULT_PREFETCH_SIZE);

        let source = self.source_provider.clone();
        let fetch_handle = tokio::spawn(async move { fetch_blocks(source, from, to, tx).await });

        let mut collector = ReplayMetricsCollector::default();
        let mut prev_block_hash: Option<B256> = None;
        let mut finalized_hash: Option<B256> = None;

        while let Some(result) = rx.recv().await {
            let fetched = result?;
            let input = RethNewPayloadInput::BlockRlp(fetched.rlp_bytes.clone());

            let new_payload_start = Instant::now();
            let payload_status = self
                .engine_provider
                .reth_new_payload(input)
                .await
                .wrap_err("reth_newPayload failed")?;
            let new_payload_latency = new_payload_start.elapsed();

            if !payload_status.status.is_valid() && !payload_status.status.is_syncing() {
                tracing::warn!(
                    block = fetched.meta.number,
                    status = ?payload_status.status,
                    "reth_newPayload returned non-VALID status"
                );
            }

            let safe_hash = prev_block_hash.unwrap_or(fetched.meta.hash);
            if finalized_hash.is_none() {
                finalized_hash = Some(fetched.meta.hash);
            }

            let forkchoice_state = ForkchoiceState {
                head_block_hash: fetched.meta.hash,
                safe_block_hash: safe_hash,
                // SAFETY: finalized_hash is always Some after the check above
                finalized_block_hash: finalized_hash.unwrap(),
            };

            let fcu_start = Instant::now();
            let fcu_result = self
                .engine_provider
                .reth_forkchoice_updated(forkchoice_state)
                .await
                .wrap_err("reth_forkchoiceUpdated failed")?;
            let fcu_latency = fcu_start.elapsed();

            if !fcu_result.is_valid() && !fcu_result.is_syncing() {
                tracing::warn!(
                    block = fetched.meta.number,
                    status = ?fcu_result.payload_status,
                    "reth_forkchoiceUpdated returned non-VALID status"
                );
            }

            let total_latency = new_payload_latency + fcu_latency;
            let payload_status_str = payload_status.status.status.to_string();

            collector.record_block(BlockMetrics {
                tx_count: fetched.meta.tx_count as u64,
                gas_used: fetched.meta.gas_used,
                new_payload_latency,
                fcu_latency,
                total_latency,
            });

            for reporter in reporters.iter_mut() {
                reporter.on_replay_block(&ReplayBlockStats {
                    number: fetched.meta.number,
                    timestamp: fetched.meta.timestamp,
                    tx_count: fetched.meta.tx_count,
                    gas_used: fetched.meta.gas_used,
                    gas_limit: fetched.meta.gas_limit,
                    new_payload_ms: new_payload_latency.as_millis() as u64,
                    fcu_ms: fcu_latency.as_millis() as u64,
                    total_latency_ms: total_latency.as_millis() as u64,
                    payload_status: payload_status_str.clone(),
                })?;
            }

            tracing::info!(
                block = fetched.meta.number,
                txs = fetched.meta.tx_count,
                gas = fetched.meta.gas_used,
                new_payload_ms = new_payload_latency.as_millis(),
                fcu_ms = fcu_latency.as_millis(),
                total_ms = total_latency.as_millis(),
                server_latency_us = payload_status.latency_us,
                status = %payload_status_str,
                "Replayed block"
            );

            prev_block_hash = Some(fetched.meta.hash);

            if fetched.meta.number % 32 == 0 {
                finalized_hash = Some(fetched.meta.hash);
            }
        }

        fetch_handle.await?;

        Ok(collector.finalize())
    }
}

/// A fetched block ready for replay.
struct FetchedBlock {
    rlp_bytes: Bytes,
    meta: BlockMeta,
}

/// Decoded block metadata extracted from RLP.
struct BlockMeta {
    hash: B256,
    number: u64,
    timestamp: u64,
    gas_used: u64,
    gas_limit: u64,
    tx_count: usize,
}

/// Fetch raw RLP-encoded blocks from source provider and send to channel.
async fn fetch_blocks<P: Provider>(
    provider: P,
    from: u64,
    to: u64,
    tx: mpsc::Sender<Result<FetchedBlock>>,
) {
    for block_num in from..=to {
        let result = async {
            let rlp_bytes: Bytes = provider
                .raw_request("debug_getRawBlock".into(), (format!("0x{block_num:x}"),))
                .await
                .wrap_err_with(|| format!("failed to fetch raw block {block_num}"))?;
            let meta = decode_block_meta(&rlp_bytes)?;
            Ok(FetchedBlock { rlp_bytes, meta })
        }
        .await;

        let is_err = result.is_err();
        if tx.send(result).await.is_err() {
            tracing::debug!("Channel closed, stopping fetcher");
            break;
        }
        if is_err {
            break;
        }
    }
}

/// Decode block metadata from RLP-encoded block bytes.
fn decode_block_meta(rlp_bytes: &[u8]) -> Result<BlockMeta> {
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

/// Metrics for a single replayed block.
#[derive(Debug, Clone)]
struct BlockMetrics {
    tx_count: u64,
    gas_used: u64,
    new_payload_latency: Duration,
    fcu_latency: Duration,
    total_latency: Duration,
}

/// Aggregated replay metrics (collected during run).
#[derive(Debug, Default)]
struct ReplayMetricsCollector {
    blocks_replayed: u64,
    total_txs: u64,
    total_gas: u128,
    total_execution_time: Duration,
    new_payload_latencies: Vec<Duration>,
    fcu_latencies: Vec<Duration>,
    block_times: Vec<Duration>,
}

impl ReplayMetricsCollector {
    fn record_block(&mut self, block: BlockMetrics) {
        self.blocks_replayed += 1;
        self.total_txs += block.tx_count;
        self.total_gas += block.gas_used as u128;
        self.total_execution_time += block.total_latency;
        self.new_payload_latencies.push(block.new_payload_latency);
        self.fcu_latencies.push(block.fcu_latency);
        self.block_times.push(block.total_latency);
    }

    fn finalize(self) -> ReplayMetrics {
        ReplayMetrics {
            blocks_replayed: self.blocks_replayed,
            total_txs: self.total_txs,
            total_gas: self.total_gas,
            total_execution_time: self.total_execution_time,
            new_payload_stats: compute_latency_stats(&self.new_payload_latencies),
            fcu_stats: compute_latency_stats(&self.fcu_latencies),
            block_time_stats: compute_latency_stats(&self.block_times),
        }
    }
}

/// Finalized replay metrics with computed statistics.
#[derive(Debug)]
pub struct ReplayMetrics {
    pub blocks_replayed: u64,
    pub total_txs: u64,
    pub total_gas: u128,
    pub total_execution_time: Duration,
    pub new_payload_stats: LatencyStats,
    pub fcu_stats: LatencyStats,
    pub block_time_stats: LatencyStats,
}

impl ReplayMetrics {
    fn blocks_per_second(&self) -> f64 {
        if self.total_execution_time.as_secs_f64() > 0.0 {
            self.blocks_replayed as f64 / self.total_execution_time.as_secs_f64()
        } else {
            0.0
        }
    }

    fn mgas_per_second(&self) -> f64 {
        if self.total_execution_time.as_secs_f64() > 0.0 {
            (self.total_gas as f64 / 1_000_000.0) / self.total_execution_time.as_secs_f64()
        } else {
            0.0
        }
    }

    fn ggas_per_second(&self) -> f64 {
        if self.total_execution_time.as_secs_f64() > 0.0 {
            (self.total_gas as f64 / 1_000_000_000.0) / self.total_execution_time.as_secs_f64()
        } else {
            0.0
        }
    }
}
