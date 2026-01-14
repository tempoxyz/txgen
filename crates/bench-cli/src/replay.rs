//! `bench replay` - Engine API block replay
//!
//! Replays historical blocks by:
//! 1. Fetching block data from an archive node
//! 2. Sending newPayload via Engine API
//! 3. Measuring execution time

use crate::ReplayArgs;
use alloy_consensus::Transaction as TxTrait;
use alloy_network::{Ethereum, eip2718::Encodable2718};
use alloy_primitives::{B256, U256};
use alloy_provider::{Provider, ProviderBuilder, RootProvider, ext::EngineApi};
use alloy_rpc_types_engine::{ExecutionPayloadV3, JwtSecret};
use alloy_transport_http::{AuthLayer, Http, HyperClient};
use bench_core::{ConsoleReporter, JsonReporter, Reporter};
use eyre::{Context, Result, bail};
use std::time::{Duration, Instant};

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

    let mut metrics = ReplayMetrics::default();
    let mut reporters = parse_reporters(&args.reports)?;

    for block_num in args.from..=args.to {
        let block_start = Instant::now();

        let block = source_provider
            .get_block_by_number(block_num.into())
            .full()
            .await
            .wrap_err("failed to fetch block")?
            .ok_or_else(|| eyre::eyre!("block {} not found", block_num))?;

        let header = &block.header;
        let txs: Vec<_> = block
            .transactions
            .txns()
            .map(|tx| tx.inner.inner().encoded_2718().into())
            .collect();

        let payload = ExecutionPayloadV3 {
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
        };

        let versioned_hashes: Vec<B256> = block
            .transactions
            .txns()
            .flat_map(|tx| {
                tx.inner
                    .inner()
                    .blob_versioned_hashes()
                    .unwrap_or_default()
                    .to_vec()
            })
            .collect();

        let parent_beacon_block_root = header.parent_beacon_block_root.unwrap_or_default();

        let result = engine_provider
            .new_payload_v3(payload, versioned_hashes, parent_beacon_block_root)
            .await
            .wrap_err("newPayload failed")?;

        let elapsed = block_start.elapsed();

        let tx_count = block.transactions.len() as u64;
        let gas_used = header.gas_used;

        metrics.blocks_replayed += 1;
        metrics.total_txs += tx_count;
        metrics.total_gas += gas_used as u128;
        metrics.total_execution_time += elapsed;
        metrics.block_times.push(elapsed);

        tracing::info!(
            block = block_num,
            txs = tx_count,
            gas = gas_used,
            elapsed_ms = elapsed.as_millis(),
            status = %result.status,
            "Replayed block"
        );
    }

    print_replay_summary(&metrics, &mut reporters)?;

    Ok(())
}

#[derive(Debug, Default)]
struct ReplayMetrics {
    blocks_replayed: u64,
    total_txs: u64,
    total_gas: u128,
    total_execution_time: Duration,
    block_times: Vec<Duration>,
}

impl ReplayMetrics {
    fn avg_block_time(&self) -> Duration {
        if self.blocks_replayed == 0 {
            Duration::ZERO
        } else {
            self.total_execution_time / self.blocks_replayed as u32
        }
    }

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

    fn percentile(&self, p: f64) -> Duration {
        if self.block_times.is_empty() {
            return Duration::ZERO;
        }
        let mut sorted = self.block_times.clone();
        sorted.sort();
        let idx = ((sorted.len() as f64 * p / 100.0) as usize).min(sorted.len() - 1);
        sorted[idx]
    }
}

fn print_replay_summary(
    metrics: &ReplayMetrics,
    _reporters: &mut [Box<dyn Reporter>],
) -> Result<()> {
    eprintln!();
    eprintln!("═══════════════════════════════════════");
    eprintln!("           Block Replay Results");
    eprintln!("═══════════════════════════════════════");
    eprintln!();
    eprintln!("  Blocks Replayed: {:>10}", metrics.blocks_replayed);
    eprintln!("  Total Txs:       {:>10}", metrics.total_txs);
    eprintln!(
        "  Total Gas:       {:>10.2} Mgas",
        metrics.total_gas as f64 / 1_000_000.0
    );
    eprintln!();
    eprintln!(
        "  Duration:        {:>10.2}s",
        metrics.total_execution_time.as_secs_f64()
    );
    eprintln!("  Blocks/sec:      {:>10.2}", metrics.blocks_per_second());
    eprintln!("  Mgas/sec:        {:>10.2}", metrics.mgas_per_second());
    eprintln!();
    eprintln!("  Block Time:");
    eprintln!(
        "    Min:           {:>10.2}ms",
        metrics
            .block_times
            .iter()
            .min()
            .unwrap_or(&Duration::ZERO)
            .as_secs_f64()
            * 1000.0
    );
    eprintln!(
        "    Max:           {:>10.2}ms",
        metrics
            .block_times
            .iter()
            .max()
            .unwrap_or(&Duration::ZERO)
            .as_secs_f64()
            * 1000.0
    );
    eprintln!(
        "    Avg:           {:>10.2}ms",
        metrics.avg_block_time().as_secs_f64() * 1000.0
    );
    eprintln!(
        "    P50:           {:>10.2}ms",
        metrics.percentile(50.0).as_secs_f64() * 1000.0
    );
    eprintln!(
        "    P95:           {:>10.2}ms",
        metrics.percentile(95.0).as_secs_f64() * 1000.0
    );
    eprintln!(
        "    P99:           {:>10.2}ms",
        metrics.percentile(99.0).as_secs_f64() * 1000.0
    );
    eprintln!();
    eprintln!("═══════════════════════════════════════");

    Ok(())
}

fn parse_reporters(specs: &[String]) -> Result<Vec<Box<dyn Reporter>>> {
    let mut reporters: Vec<Box<dyn Reporter>> = Vec::new();

    if specs.is_empty() {
        return Ok(reporters);
    }

    for spec in specs {
        if spec == "console" {
            reporters.push(Box::new(ConsoleReporter::stderr(true)));
        } else if let Some(path) = spec.strip_prefix("json:") {
            let path = std::path::Path::new(path);
            reporters.push(Box::new(
                JsonReporter::file(path).wrap_err("failed to create JSON reporter")?,
            ));
        } else if let Some(_url) = spec.strip_prefix("clickhouse:") {
            tracing::warn!("ClickHouse reporter not yet fully implemented");
        } else {
            bail!("unknown report format: {}", spec);
        }
    }

    Ok(reporters)
}
