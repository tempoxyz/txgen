use alloy_consensus::{BlockHeader, Sealed, Transaction};
use alloy_eips::{eip2718::Encodable2718, eip7685::Requests, BlockNumberOrTag};
use alloy_network::Network;
use alloy_primitives::{Bytes, Sealable, B256};
use alloy_provider::{ext::DebugApi, Provider};
use alloy_rlp::Decodable;
use alloy_rpc_types_engine::{
    CancunPayloadFields, ExecutionData, ExecutionPayload, ExecutionPayloadSidecar,
    PraguePayloadFields,
};
use clap::Args;
use eyre::{bail, Result, WrapErr};
use std::{io::Write, path::PathBuf};
use tokio::sync::mpsc;

#[derive(Args)]
pub struct ExtractArgs {
    /// RPC endpoint URL (archive node with debug_getRawBlock)
    #[arg(long)]
    pub rpc: String,

    /// First block number to fetch (inclusive)
    #[arg(long)]
    pub from: u64,

    /// Last block number to fetch (inclusive)
    #[arg(long)]
    pub to: u64,

    /// Output file (default: stdout)
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Number of blocks to prefetch ahead
    #[arg(long, default_value = "20")]
    pub buffer_size: usize,
}

#[derive(Args)]
pub struct ExtractBigBlocksArgs {
    /// RPC endpoint URL (archive node with debug_getRawBlock)
    #[arg(long)]
    pub rpc: String,

    /// First source block number to fetch
    #[arg(long)]
    pub from: u64,

    /// Number of synthetic big blocks to emit
    #[arg(long)]
    pub count: u64,

    /// Target gas usage per synthetic big block. Accepts K, M, or G suffixes.
    #[arg(long, value_parser = parse_gas_limit)]
    pub target_gas: u64,

    /// Output file (default: stdout)
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Number of source blocks to prefetch ahead.
    ///
    /// Big-block extraction currently fetches sequentially; this flag is accepted for CLI
    /// compatibility with `extract` and future pipelining.
    #[arg(long, default_value = "20")]
    pub buffer_size: usize,
}

fn parse_gas_limit(value: &str) -> Result<u64, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("gas limit cannot be empty".to_string());
    }

    let (number, multiplier) = match value.as_bytes().last().copied() {
        Some(b'k' | b'K') => (&value[..value.len() - 1], 1_000_u64),
        Some(b'm' | b'M') => (&value[..value.len() - 1], 1_000_000_u64),
        Some(b'g' | b'G') => (&value[..value.len() - 1], 1_000_000_000_u64),
        _ => (value, 1_u64),
    };

    let parsed = number.parse::<u64>().map_err(|e| format!("invalid gas limit {value:?}: {e}"))?;
    parsed.checked_mul(multiplier).ok_or_else(|| format!("gas limit {value:?} overflows u64"))
}

// ---------------------------------------------------------------------------
// Extract implementation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize)]
struct BigBlockData<T> {
    env_switches: Vec<(usize, T)>,
    prior_block_hashes: Vec<(u64, B256)>,
}

impl<T> Default for BigBlockData<T> {
    fn default() -> Self {
        Self { env_switches: Vec::new(), prior_block_hashes: Vec::new() }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
struct BigBlockPayload {
    execution_data: ExecutionData,
    #[serde(default)]
    big_block_data: BigBlockData<ExecutionData>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    block_access_list: Option<serde_json::Value>,
}

pub(crate) async fn run_extract<N>(args: ExtractArgs) -> Result<()>
where
    N: Network,
    N::TxEnvelope: Decodable,
    N::Header: Decodable,
{
    if args.from > args.to {
        bail!("--from must be <= --to");
    }

    let provider =
        alloy_provider::RootProvider::<N>::new_http(args.rpc.parse().wrap_err("invalid RPC URL")?);

    let (tx, mut rx) = mpsc::channel::<Result<FetchedBlock>>(args.buffer_size);

    let from = args.from;
    let to = args.to;
    let fetch_handle =
        tokio::spawn(async move { fetch_blocks::<N, _>(provider, from, to, tx).await });

    let total = to - from + 1;
    let write_result = match args.output {
        Some(ref path) => {
            let file = std::fs::File::create(path)
                .wrap_err_with(|| format!("failed to create output file: {}", path.display()))?;
            let mut writer = std::io::BufWriter::new(file);
            let result = write_extracted_blocks(&mut rx, &mut writer, total).await;
            if result.is_ok() {
                eprintln!("wrote {} blocks to {}", total, path.display());
            }
            result
        }
        None => {
            let mut writer = std::io::stdout();
            write_extracted_blocks(&mut rx, &mut writer, total).await
        }
    };

    fetch_handle.await?;
    write_result
}

pub(crate) async fn run_extract_big_blocks<N>(args: ExtractBigBlocksArgs) -> Result<()>
where
    N: Network,
    N::TxEnvelope: Decodable + Encodable2718 + Transaction,
    N::Header: Decodable + BlockHeader + Sealable,
{
    if args.count == 0 {
        bail!("--count must be greater than 0");
    }
    if args.target_gas == 0 {
        bail!("--target-gas must be greater than 0");
    }

    let provider =
        alloy_provider::RootProvider::<N>::new_http(args.rpc.parse().wrap_err("invalid RPC URL")?);

    match args.output {
        Some(ref path) => {
            let file = std::fs::File::create(path)
                .wrap_err_with(|| format!("failed to create output file: {}", path.display()))?;
            let mut writer = std::io::BufWriter::new(file);
            write_big_blocks::<N, _, _>(&provider, &mut writer, &args).await?;
            eprintln!("wrote {} big blocks to {}", args.count, path.display());
        }
        None => {
            let mut writer = std::io::stdout();
            write_big_blocks::<N, _, _>(&provider, &mut writer, &args).await?;
        }
    }

    Ok(())
}

#[derive(serde::Serialize)]
struct BlockOutputLine<'a> {
    raw: &'a str,
    key: &'a str,
    number: u64,
    timestamp: u64,
    gas_used: u64,
    gas_limit: u64,
    tx_count: usize,
}

async fn write_extracted_blocks<W: Write>(
    rx: &mut mpsc::Receiver<Result<FetchedBlock>>,
    writer: &mut W,
    total: u64,
) -> Result<()> {
    let start = std::time::Instant::now();
    let mut last_log = start;
    let mut count = 0u64;

    while let Some(result) = rx.recv().await {
        let block = result?;

        let raw_hex = format!("0x{}", hex::encode(&block.rlp_bytes));
        let key_hex = format!("{}", block.hash);
        let line = BlockOutputLine {
            raw: &raw_hex,
            key: &key_hex,
            number: block.number,
            timestamp: block.timestamp,
            gas_used: block.gas_used,
            gas_limit: block.gas_limit,
            tx_count: block.tx_count,
        };
        serde_json::to_writer(&mut *writer, &line)?;
        writer.write_all(b"\n")?;
        count += 1;

        let now = std::time::Instant::now();
        if count.is_multiple_of(1000) || now.duration_since(last_log).as_secs() >= 5 {
            let elapsed = now.duration_since(start).as_secs_f64();
            let bps = count as f64 / elapsed;
            eprintln!(
                "extracted {}/{} blocks ({:.1}%) - {:.0} blocks/s",
                count,
                total,
                count as f64 / total as f64 * 100.0,
                bps
            );
            last_log = now;
        }
    }

    writer.flush()?;
    Ok(())
}

struct FetchedBlock {
    rlp_bytes: Bytes,
    hash: alloy_primitives::B256,
    number: u64,
    timestamp: u64,
    gas_used: u64,
    gas_limit: u64,
    tx_count: usize,
}

async fn fetch_blocks<N, P>(provider: P, from: u64, to: u64, tx: mpsc::Sender<Result<FetchedBlock>>)
where
    N: Network,
    N::TxEnvelope: Decodable,
    N::Header: Decodable,
    P: Provider<N> + DebugApi<N>,
{
    for block_num in from..=to {
        let result = async {
            let rlp_bytes: Bytes = provider
                .debug_get_raw_block(BlockNumberOrTag::Number(block_num).into())
                .await
                .wrap_err_with(|| format!("failed to fetch raw block {block_num}"))?;

            let sealed: Sealed<alloy_consensus::Block<N::TxEnvelope, N::Header>> =
                alloy_consensus::Block::decode_sealed(&mut rlp_bytes.as_ref())
                    .map_err(|e| eyre::eyre!("failed to decode block {block_num}: {e}"))?;

            let hash = sealed.hash();
            let block = sealed.inner();

            Ok(FetchedBlock {
                rlp_bytes,
                hash,
                number: block.header.number(),
                timestamp: block.header.timestamp(),
                gas_used: block.header.gas_used(),
                gas_limit: block.header.gas_limit(),
                tx_count: block.body.transactions.len(),
            })
        }
        .await;

        let is_err = result.is_err();
        if tx.send(result).await.is_err() {
            break;
        }
        if is_err {
            break;
        }
    }
}

async fn write_big_blocks<N, P, W>(
    provider: &P,
    writer: &mut W,
    args: &ExtractBigBlocksArgs,
) -> Result<()>
where
    N: Network,
    N::TxEnvelope: Decodable + Encodable2718 + Transaction,
    N::Header: Decodable + BlockHeader + Sealable,
    P: Provider<N> + DebugApi<N>,
    W: Write,
{
    let start = std::time::Instant::now();
    let mut source_block = args.from;
    let mut emitted = 0_u64;
    let mut accumulated_block_hashes = Vec::new();
    let mut prev_big_block_hash = None;
    let mut first_source_block = None;

    while emitted < args.count {
        let mut blocks = Vec::new();
        let mut accumulated_gas = 0_u64;

        while accumulated_gas < args.target_gas {
            let execution_data = fetch_execution_data::<N, _>(provider, source_block).await?;
            first_source_block.get_or_insert(execution_data.block_number());
            accumulated_gas =
                accumulated_gas.saturating_add(execution_data.payload.as_v1().gas_used);
            blocks.push(execution_data);
            source_block = source_block.saturating_add(1);
        }

        let big_block = build_big_block(
            blocks,
            emitted,
            first_source_block.unwrap_or(args.from),
            prev_big_block_hash,
            accumulated_block_hashes.clone(),
        )?;

        prev_big_block_hash = Some(big_block.execution_data.block_hash());
        for (_, switch_data) in &big_block.big_block_data.env_switches {
            accumulated_block_hashes.push((switch_data.block_number(), switch_data.block_hash()));
        }
        if accumulated_block_hashes.len() > 256 {
            let excess = accumulated_block_hashes.len() - 256;
            accumulated_block_hashes.drain(..excess);
        }

        serde_json::to_writer(&mut *writer, &big_block)?;
        writer.write_all(b"\n")?;
        emitted += 1;

        let elapsed = start.elapsed().as_secs_f64();
        let rate = if elapsed > 0.0 { emitted as f64 / elapsed } else { 0.0 };
        eprintln!(
            "generated {}/{} big blocks ({:.1}%) - {:.2} big blocks/s",
            emitted,
            args.count,
            emitted as f64 / args.count as f64 * 100.0,
            rate,
        );
    }

    writer.flush()?;
    Ok(())
}

async fn fetch_execution_data<N, P>(provider: &P, block_num: u64) -> Result<ExecutionData>
where
    N: Network,
    N::TxEnvelope: Decodable + Encodable2718 + Transaction,
    N::Header: Decodable + BlockHeader + Sealable,
    P: Provider<N> + DebugApi<N>,
{
    let rlp_bytes: Bytes = provider
        .debug_get_raw_block(BlockNumberOrTag::Number(block_num).into())
        .await
        .wrap_err_with(|| format!("failed to fetch raw block {block_num}"))?;

    let sealed: Sealed<alloy_consensus::Block<N::TxEnvelope, N::Header>> =
        alloy_consensus::Block::decode_sealed(&mut rlp_bytes.as_ref())
            .map_err(|e| eyre::eyre!("failed to decode block {block_num}: {e}"))?;
    let (block, _) = sealed.split();
    let (payload, sidecar) = ExecutionPayload::from_block_slow(&block);
    Ok(ExecutionData { payload, sidecar })
}

fn build_big_block(
    mut blocks: Vec<ExecutionData>,
    big_block_idx: u64,
    first_source_block: u64,
    prev_big_block_hash: Option<B256>,
    prior_block_hashes: Vec<(u64, B256)>,
) -> Result<BigBlockPayload> {
    if blocks.is_empty() {
        bail!("cannot build a big block with no source blocks");
    }

    let mut base = blocks.remove(0);
    let mut env_switches = Vec::new();

    if !blocks.is_empty() {
        env_switches.push((0, base.clone()));
        let mut cumulative_tx_count = base.payload.transactions().len();
        let mut total_gas_used = base.payload.as_v1().gas_used;
        let mut total_gas_limit = base.payload.gas_limit();
        let final_state_root = blocks
            .last()
            .map(|last| last.payload.as_v1().state_root)
            .ok_or_else(|| eyre::eyre!("missing final block"))?;

        for block_data in blocks {
            total_gas_used = total_gas_used.saturating_add(block_data.payload.as_v1().gas_used);
            total_gas_limit = total_gas_limit.saturating_add(block_data.payload.gas_limit());
            env_switches.push((cumulative_tx_count, block_data.clone()));
            let txs = block_data.payload.transactions().clone();
            cumulative_tx_count += txs.len();
            base.payload.transactions_mut().extend(txs);
        }

        let base_v1 = base.payload.as_v1_mut();
        base_v1.state_root = final_state_root;
        base_v1.gas_used = total_gas_used;
        base_v1.gas_limit = total_gas_limit;
        base.sidecar = merged_sidecar(&base.sidecar, &env_switches);
    }

    if let Some(prev_hash) = prev_big_block_hash {
        let synthetic_block_number = first_source_block + big_block_idx;
        let base_v1 = base.payload.as_v1_mut();
        base_v1.parent_hash = prev_hash;
        base_v1.block_number = synthetic_block_number;
    }

    let block_hash = compute_payload_block_hash(&base)?;
    base.payload.as_v1_mut().block_hash = block_hash;

    Ok(BigBlockPayload {
        execution_data: base,
        big_block_data: BigBlockData { env_switches, prior_block_hashes },
        block_access_list: None,
    })
}

fn merged_sidecar(
    base_sidecar: &ExecutionPayloadSidecar,
    env_switches: &[(usize, ExecutionData)],
) -> ExecutionPayloadSidecar {
    let Some(base_cancun) = base_sidecar.cancun() else {
        return ExecutionPayloadSidecar::none();
    };

    let mut versioned_hashes = base_cancun.versioned_hashes.clone();
    for (_, switch_data) in env_switches.iter().skip(1) {
        if let Some(cancun) = switch_data.sidecar.cancun() {
            versioned_hashes.extend_from_slice(&cancun.versioned_hashes);
        }
    }

    let cancun = CancunPayloadFields {
        parent_beacon_block_root: base_cancun.parent_beacon_block_root,
        versioned_hashes,
    };

    if base_sidecar.prague().is_some() {
        ExecutionPayloadSidecar::v4(cancun, PraguePayloadFields::new(Requests::default()))
    } else {
        ExecutionPayloadSidecar::v3(cancun)
    }
}

fn compute_payload_block_hash(data: &ExecutionData) -> Result<B256> {
    let block = data
        .payload
        .clone()
        .into_block_with_sidecar_raw(&data.sidecar)
        .wrap_err("failed to convert payload to block for hash computation")?;
    Ok(block.header.hash_slow())
}
