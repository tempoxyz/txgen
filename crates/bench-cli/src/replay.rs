//! `bench replay` - Engine API block replay
//!
//! Replays historical blocks by:
//! 1. Fetching block data from an archive node (with prefetching)
//! 2. Sending newPayload + forkchoiceUpdated via Engine API
//! 3. Measuring execution time and collecting metrics

use crate::ReplayArgs;
use alloy_consensus::Transaction as TxTrait;
use alloy_network::{Ethereum, eip2718::Encodable2718};
use alloy_primitives::{B256, Bytes, U256};
use alloy_provider::{Provider, ProviderBuilder, RootProvider, ext::EngineApi};
use alloy_rpc_types_engine::{ExecutionPayloadV3, ForkchoiceState, JwtSecret};
use alloy_rpc_types_eth::Block;
use alloy_transport_http::{AuthLayer, Http, HyperClient};
use bench_core::{
    LatencyStats, ReplayBlockStats, Reporter, compute_latency_stats, parse_reporters,
};
use eyre::{Context, Result, bail};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Number of blocks to prefetch ahead.
const DEFAULT_PREFETCH_SIZE: usize = 20;

/// Prague fork timestamp (May 7, 2025 10:05:11 UTC).
const PRAGUE_TIMESTAMP: u64 = 1746612311;

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

    let layer_transport = HyperClient::new().layer(AuthLayer::new(jwt_secret));
    let http_hyper = Http::with_client(layer_transport, args.engine.parse()?);
    let rpc_client = alloy_rpc_client::RpcClient::new(http_hyper, true);
    let engine_provider = RootProvider::<Ethereum>::new(rpc_client);

    let mut reporters = parse_reporters(&args.reports)?;

    let bench = ReplayBench::new(source_provider, engine_provider);
    let mode = BenchMode::Range {
        from: args.from,
        to: args.to,
    };

    let metrics = bench.run(mode, &mut reporters).await?;
    print_replay_summary(&metrics, &mut reporters)?;

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
    E: EngineApi<alloy_network::Ethereum> + Send + Sync,
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
            let block_num = fetched.block.header.number;
            let block_hash = fetched.block.header.hash;

            let payload = block_to_payload(&fetched.block)?;
            let versioned_hashes = extract_versioned_hashes(&fetched.block);
            let parent_beacon_block_root = fetched
                .block
                .header
                .parent_beacon_block_root
                .unwrap_or_default();
            let execution_requests = extract_execution_requests(&fetched.block);

            let new_payload_start = Instant::now();
            let payload_status = if fetched.block.header.timestamp >= PRAGUE_TIMESTAMP {
                self.engine_provider
                    .new_payload_v4(
                        payload,
                        versioned_hashes,
                        parent_beacon_block_root,
                        execution_requests,
                    )
                    .await
                    .wrap_err("newPayloadV4 failed")?
            } else {
                self.engine_provider
                    .new_payload_v3(payload, versioned_hashes, parent_beacon_block_root)
                    .await
                    .wrap_err("newPayloadV3 failed")?
            };
            let new_payload_latency = new_payload_start.elapsed();

            if !payload_status.is_valid() && !payload_status.is_syncing() {
                tracing::warn!(
                    block = block_num,
                    status = ?payload_status,
                    "newPayload returned non-VALID status"
                );
            }

            let safe_hash = prev_block_hash.unwrap_or(block_hash);
            if finalized_hash.is_none() {
                finalized_hash = Some(block_hash);
            }

            let forkchoice_state = ForkchoiceState {
                head_block_hash: block_hash,
                safe_block_hash: safe_hash,
                finalized_block_hash: finalized_hash.unwrap_or(block_hash),
            };

            let fcu_start = Instant::now();
            let fcu_result = self
                .engine_provider
                .fork_choice_updated_v3(forkchoice_state, None)
                .await
                .wrap_err("forkchoiceUpdatedV3 failed")?;
            let fcu_latency = fcu_start.elapsed();

            if !fcu_result.is_valid() && !fcu_result.is_syncing() {
                tracing::warn!(
                    block = block_num,
                    status = ?fcu_result.payload_status,
                    "forkchoiceUpdated returned non-VALID status"
                );
            }

            let tx_count = fetched.block.transactions.len() as u64;
            let gas_used = fetched.block.header.gas_used;
            let total_latency = new_payload_latency + fcu_latency;

            let payload_status_str = payload_status.status.to_string();

            collector.record_block(BlockMetrics {
                tx_count,
                gas_used,
                new_payload_latency,
                fcu_latency,
                total_latency,
            });

            for reporter in reporters.iter_mut() {
                reporter.on_replay_block(&ReplayBlockStats {
                    number: block_num,
                    timestamp: fetched.block.header.timestamp,
                    tx_count: tx_count as usize,
                    gas_used,
                    gas_limit: fetched.block.header.gas_limit,
                    new_payload_ms: new_payload_latency.as_millis() as u64,
                    fcu_ms: fcu_latency.as_millis() as u64,
                    total_latency_ms: total_latency.as_millis() as u64,
                    payload_status: payload_status_str.clone(),
                })?;
            }

            tracing::info!(
                block = block_num,
                txs = tx_count,
                gas = gas_used,
                new_payload_ms = new_payload_latency.as_millis(),
                fcu_ms = fcu_latency.as_millis(),
                total_ms = total_latency.as_millis(),
                status = %payload_status_str,
                "Replayed block"
            );

            prev_block_hash = Some(block_hash);

            if block_num % 32 == 0 {
                finalized_hash = Some(block_hash);
            }
        }

        fetch_handle.await?;

        Ok(collector.finalize())
    }
}

/// A fetched block ready for replay.
struct FetchedBlock {
    block: Block,
}

/// Fetch blocks from source provider and send to channel.
async fn fetch_blocks<P: Provider>(
    provider: P,
    from: u64,
    to: u64,
    tx: mpsc::Sender<Result<FetchedBlock>>,
) {
    for block_num in from..=to {
        let result = async {
            let block = provider
                .get_block_by_number(block_num.into())
                .full()
                .await
                .wrap_err_with(|| format!("failed to fetch block {}", block_num))?
                .ok_or_else(|| eyre::eyre!("block {} not found", block_num))?;
            Ok(FetchedBlock { block })
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

/// Convert a block to an ExecutionPayloadV3.
fn block_to_payload(block: &Block) -> Result<ExecutionPayloadV3> {
    let header = &block.header;
    let txs: Vec<Bytes> = block
        .transactions
        .txns()
        .map(|tx| tx.inner.inner().encoded_2718().into())
        .collect();

    Ok(ExecutionPayloadV3 {
        payload_inner: alloy_rpc_types_engine::ExecutionPayloadV2 {
            payload_inner: alloy_rpc_types_engine::ExecutionPayloadV1 {
                parent_hash: header.parent_hash,
                fee_recipient: header.beneficiary,
                state_root: header.state_root,
                receipts_root: header.receipts_root,
                logs_bloom: header.logs_bloom,
                prev_randao: header.mix_hash,
                block_number: header.number,
                gas_limit: header.gas_limit,
                gas_used: header.gas_used,
                timestamp: header.timestamp,
                extra_data: header.extra_data.clone(),
                base_fee_per_gas: U256::from(header.base_fee_per_gas.unwrap_or_default()),
                block_hash: block.header.hash,
                transactions: txs,
            },
            withdrawals: block.withdrawals.clone().unwrap_or_default().into_inner(),
        },
        blob_gas_used: header.blob_gas_used.unwrap_or_default(),
        excess_blob_gas: header.excess_blob_gas.unwrap_or_default(),
    })
}

/// Extract blob versioned hashes from block transactions.
fn extract_versioned_hashes(block: &Block) -> Vec<B256> {
    block
        .transactions
        .txns()
        .flat_map(|tx| {
            tx.inner
                .inner()
                .blob_versioned_hashes()
                .unwrap_or_default()
                .to_vec()
        })
        .collect()
}

/// Extract EIP-7685 execution requests from block.
///
/// For Prague blocks, this includes deposit requests, withdrawal requests,
/// and consolidation requests. Pre-Prague blocks return empty.
///
/// Note: The execution requests are not available in the RPC block response.
/// When replaying historical blocks, we pass an empty list. The execution layer
/// will compute the requests from transaction execution and validate against
/// the block header's requests root.
fn extract_execution_requests(_block: &Block) -> Vec<Bytes> {
    vec![]
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
    blocks_replayed: u64,
    total_txs: u64,
    total_gas: u128,
    total_execution_time: Duration,
    new_payload_stats: LatencyStats,
    fcu_stats: LatencyStats,
    block_time_stats: LatencyStats,
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

fn print_replay_summary(
    metrics: &ReplayMetrics,
    _reporters: &mut [Box<dyn Reporter>],
) -> Result<()> {
    eprintln!();
    eprintln!("═══════════════════════════════════════════════════════");
    eprintln!("                  Block Replay Results");
    eprintln!("═══════════════════════════════════════════════════════");
    eprintln!();
    eprintln!("  Blocks Replayed: {:>10}", metrics.blocks_replayed);
    eprintln!("  Total Txs:       {:>10}", metrics.total_txs);
    eprintln!(
        "  Total Gas:       {:>10.2} Ggas",
        metrics.total_gas as f64 / 1_000_000_000.0
    );
    eprintln!();
    eprintln!(
        "  Duration:        {:>10.2}s",
        metrics.total_execution_time.as_secs_f64()
    );
    eprintln!("  Blocks/sec:      {:>10.2}", metrics.blocks_per_second());
    eprintln!("  Mgas/sec:        {:>10.2}", metrics.mgas_per_second());
    eprintln!("  Ggas/sec:        {:>10.4}", metrics.ggas_per_second());
    eprintln!();
    eprintln!("  newPayload Latency:");
    eprintln!(
        "    Min:           {:>10.2}ms",
        metrics.new_payload_stats.min.as_secs_f64() * 1000.0
    );
    eprintln!(
        "    Max:           {:>10.2}ms",
        metrics.new_payload_stats.max.as_secs_f64() * 1000.0
    );
    eprintln!(
        "    Mean:          {:>10.2}ms",
        metrics.new_payload_stats.mean.as_secs_f64() * 1000.0
    );
    eprintln!(
        "    P50:           {:>10.2}ms",
        metrics.new_payload_stats.p50.as_secs_f64() * 1000.0
    );
    eprintln!(
        "    P95:           {:>10.2}ms",
        metrics.new_payload_stats.p95.as_secs_f64() * 1000.0
    );
    eprintln!(
        "    P99:           {:>10.2}ms",
        metrics.new_payload_stats.p99.as_secs_f64() * 1000.0
    );
    eprintln!();
    eprintln!("  forkchoiceUpdated Latency:");
    eprintln!(
        "    Min:           {:>10.2}ms",
        metrics.fcu_stats.min.as_secs_f64() * 1000.0
    );
    eprintln!(
        "    Max:           {:>10.2}ms",
        metrics.fcu_stats.max.as_secs_f64() * 1000.0
    );
    eprintln!(
        "    Mean:          {:>10.2}ms",
        metrics.fcu_stats.mean.as_secs_f64() * 1000.0
    );
    eprintln!(
        "    P50:           {:>10.2}ms",
        metrics.fcu_stats.p50.as_secs_f64() * 1000.0
    );
    eprintln!(
        "    P95:           {:>10.2}ms",
        metrics.fcu_stats.p95.as_secs_f64() * 1000.0
    );
    eprintln!(
        "    P99:           {:>10.2}ms",
        metrics.fcu_stats.p99.as_secs_f64() * 1000.0
    );
    eprintln!();
    eprintln!("  Total Block Time:");
    eprintln!(
        "    Min:           {:>10.2}ms",
        metrics.block_time_stats.min.as_secs_f64() * 1000.0
    );
    eprintln!(
        "    Max:           {:>10.2}ms",
        metrics.block_time_stats.max.as_secs_f64() * 1000.0
    );
    eprintln!(
        "    Mean:          {:>10.2}ms",
        metrics.block_time_stats.mean.as_secs_f64() * 1000.0
    );
    eprintln!(
        "    P50:           {:>10.2}ms",
        metrics.block_time_stats.p50.as_secs_f64() * 1000.0
    );
    eprintln!(
        "    P95:           {:>10.2}ms",
        metrics.block_time_stats.p95.as_secs_f64() * 1000.0
    );
    eprintln!(
        "    P99:           {:>10.2}ms",
        metrics.block_time_stats.p99.as_secs_f64() * 1000.0
    );
    eprintln!();
    eprintln!("═══════════════════════════════════════════════════════");

    Ok(())
}
