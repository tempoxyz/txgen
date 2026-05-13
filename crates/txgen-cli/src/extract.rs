use alloy_consensus::{BlockHeader, Sealed, Transaction};
use alloy_eips::{
    eip2718::Encodable2718,
    eip7928::{AccountChanges, BlockAccessList, SlotChanges},
    BlockNumberOrTag,
};
use alloy_network::Network;
use alloy_primitives::{Bytes, Sealable, B256};
use alloy_provider::{ext::DebugApi, Provider};
use alloy_rlp::Decodable;
use alloy_rpc_types_engine::{ExecutionData, ExecutionPayload};
use clap::Args;
use eyre::{bail, Result, WrapErr};
use std::{
    collections::{HashMap, HashSet},
    io::Write,
    path::PathBuf,
};
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

    /// Include RLP-encoded block access lists from eth_getBlockAccessListByBlockNumber.
    #[arg(long, default_value_t = false)]
    pub bal: bool,
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

    /// Include and merge block access lists from eth_getBlockAccessListByBlockNumber.
    #[arg(long, default_value_t = false)]
    pub bal: bool,
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
    env_switches: Vec<T>,
    prior_block_hashes: Vec<(u64, B256)>,
    block_number: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    merged_block_access_list: Option<Bytes>,
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
    let include_bal = args.bal;
    let fetch_handle =
        tokio::spawn(
            async move { fetch_blocks::<N, _>(provider, from, to, include_bal, tx).await },
        );

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
    #[serde(skip_serializing_if = "Option::is_none")]
    bal: Option<&'a str>,
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
        let bal_hex = block.bal_rlp.as_ref().map(|bal| format!("0x{}", hex::encode(bal)));
        let key_hex = format!("{}", block.hash);
        let line = BlockOutputLine {
            raw: &raw_hex,
            bal: bal_hex.as_deref(),
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
    bal_rlp: Option<Bytes>,
    hash: alloy_primitives::B256,
    number: u64,
    timestamp: u64,
    gas_used: u64,
    gas_limit: u64,
    tx_count: usize,
}

async fn fetch_blocks<N, P>(
    provider: P,
    from: u64,
    to: u64,
    include_bal: bool,
    tx: mpsc::Sender<Result<FetchedBlock>>,
) where
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

            let bal_rlp = if include_bal {
                Some(fetch_encoded_block_access_list(&provider, block_num).await?)
            } else {
                None
            };

            let sealed: Sealed<alloy_consensus::Block<N::TxEnvelope, N::Header>> =
                alloy_consensus::Block::decode_sealed(&mut rlp_bytes.as_ref())
                    .map_err(|e| eyre::eyre!("failed to decode block {block_num}: {e}"))?;

            let hash = sealed.hash();
            let block = sealed.inner();

            Ok(FetchedBlock {
                rlp_bytes,
                bal_rlp,
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

async fn fetch_block_access_list<N, P>(provider: &P, block_num: u64) -> Result<BlockAccessList>
where
    N: Network,
    P: Provider<N>,
{
    provider
        .get_block_access_list_by_number(BlockNumberOrTag::Number(block_num))
        .await
        .wrap_err_with(|| format!("failed to fetch block access list {block_num}"))?
        .ok_or_else(|| eyre::eyre!("block access list not found for block {block_num}"))
}

async fn fetch_encoded_block_access_list<N, P>(provider: &P, block_num: u64) -> Result<Bytes>
where
    N: Network,
    P: Provider<N>,
{
    Ok(alloy_rlp::encode(fetch_block_access_list(provider, block_num).await?).into())
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
    let mut first_source_block = None;

    while emitted < args.count {
        let mut blocks = Vec::new();
        let mut block_access_lists = Vec::new();
        let mut accumulated_gas = 0_u64;

        while accumulated_gas < args.target_gas {
            let fetched = fetch_execution_data::<N, _>(provider, source_block, args.bal).await?;
            first_source_block.get_or_insert(fetched.execution_data.block_number());
            accumulated_gas =
                accumulated_gas.saturating_add(fetched.execution_data.payload.as_v1().gas_used);
            blocks.push(fetched.execution_data);
            block_access_lists.push(fetched.block_access_list);
            source_block = source_block.saturating_add(1);
        }

        let merged_block_access_list = merge_block_access_lists(&blocks, block_access_lists);
        let big_block = build_big_block(
            blocks,
            emitted,
            first_source_block.unwrap_or(args.from),
            accumulated_block_hashes.clone(),
            merged_block_access_list,
        )?;

        for switch_data in &big_block.env_switches {
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

struct FetchedExecutionData {
    execution_data: ExecutionData,
    block_access_list: Option<BlockAccessList>,
}

async fn fetch_execution_data<N, P>(
    provider: &P,
    block_num: u64,
    include_bal: bool,
) -> Result<FetchedExecutionData>
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

    let block_access_list =
        if include_bal { Some(fetch_block_access_list(provider, block_num).await?) } else { None };

    let sealed: Sealed<alloy_consensus::Block<N::TxEnvelope, N::Header>> =
        alloy_consensus::Block::decode_sealed(&mut rlp_bytes.as_ref())
            .map_err(|e| eyre::eyre!("failed to decode block {block_num}: {e}"))?;
    let (block, _) = sealed.split();
    let (payload, sidecar) = ExecutionPayload::from_block_slow(&block);
    Ok(FetchedExecutionData {
        execution_data: ExecutionData { payload, sidecar },
        block_access_list,
    })
}

fn merge_block_access_lists(
    blocks: &[ExecutionData],
    block_access_lists: Vec<Option<BlockAccessList>>,
) -> Option<Bytes> {
    let mut merged_block_access_list = None;
    let mut cumulative_tx_count = 0;

    for (block_idx, (block_data, block_access_list)) in
        blocks.iter().zip(block_access_lists).enumerate()
    {
        if let Some(block_access_list) = block_access_list {
            merge_block_access_list(
                merged_block_access_list.get_or_insert_with(Default::default),
                block_access_list,
                cumulative_tx_count as u64,
                block_idx as u64,
            );
        }

        cumulative_tx_count += block_data.transaction_count();
    }

    let mut merged_block_access_list: BlockAccessList = merged_block_access_list?;
    merged_block_access_list.sort_unstable_by_key(|account| account.address);
    for account in &mut merged_block_access_list {
        sort_account_changes(account);
    }

    Some(alloy_rlp::encode(merged_block_access_list).into())
}

fn sort_account_changes(account: &mut AccountChanges) {
    account.storage_changes.sort_unstable_by_key(|slot_changes| slot_changes.slot);
    for slot_changes in &mut account.storage_changes {
        slot_changes.changes.sort_unstable_by_key(|change| change.block_access_index);
    }
    account.storage_reads.sort_unstable();
    account.balance_changes.sort_unstable_by_key(|change| change.block_access_index);
    account.nonce_changes.sort_unstable_by_key(|change| change.block_access_index);
    account.code_changes.sort_unstable_by_key(|change| change.block_access_index);
}

fn merge_block_access_list(
    merged: &mut BlockAccessList,
    incoming: BlockAccessList,
    tx_index_offset: u64,
    segment_idx: u64,
) {
    let mut account_positions = merged
        .iter()
        .enumerate()
        .map(|(idx, account)| (account.address, idx))
        .collect::<HashMap<_, _>>();

    for mut account_changes in incoming {
        shift_account_changes(&mut account_changes, tx_index_offset, segment_idx);

        if let Some(&idx) = account_positions.get(&account_changes.address) {
            merge_account_changes(&mut merged[idx], account_changes);
        } else {
            account_positions.insert(account_changes.address, merged.len());
            merged.push(account_changes);
        }
    }
}

fn shift_account_changes(
    account_changes: &mut AccountChanges,
    tx_index_offset: u64,
    segment_idx: u64,
) {
    // Per-block BALs use block_access_index = 0 for pre-execution writes, 1..tx_count for
    // transaction commits, and tx_count+1 for post-execution. Each big-block segment boundary
    // needs two extra indexes: the previous segment's post-execution writes and the next
    // segment's pre-execution writes.
    let shift = tx_index_offset + 2 * segment_idx;
    for slot_changes in &mut account_changes.storage_changes {
        for change in &mut slot_changes.changes {
            change.block_access_index += shift;
        }
    }
    for change in &mut account_changes.balance_changes {
        change.block_access_index += shift;
    }
    for change in &mut account_changes.nonce_changes {
        change.block_access_index += shift;
    }
    for change in &mut account_changes.code_changes {
        change.block_access_index += shift;
    }
}

fn merge_account_changes(existing: &mut AccountChanges, incoming: AccountChanges) {
    merge_slot_changes(&mut existing.storage_changes, incoming.storage_changes);
    existing.storage_reads.extend(incoming.storage_reads);
    existing.balance_changes.extend(incoming.balance_changes);
    existing.nonce_changes.extend(incoming.nonce_changes);
    existing.code_changes.extend(incoming.code_changes);

    // EIP-7928 requires a slot to appear in either storage_changes or storage_reads, not both.
    // Merging blocks can create read/write overlap, so keep writes and drop shadowed reads.
    let written: HashSet<_> =
        existing.storage_changes.iter().map(|slot_changes| slot_changes.slot).collect();
    existing.storage_reads.retain(|slot| !written.contains(slot));
    let mut seen = HashSet::with_capacity(existing.storage_reads.len());
    existing.storage_reads.retain(|slot| seen.insert(*slot));
}

fn merge_slot_changes(existing: &mut Vec<SlotChanges>, incoming: Vec<SlotChanges>) {
    let mut slot_positions = existing
        .iter()
        .enumerate()
        .map(|(idx, slot_changes)| (slot_changes.slot, idx))
        .collect::<HashMap<_, _>>();

    for slot_changes in incoming {
        if let Some(&idx) = slot_positions.get(&slot_changes.slot) {
            existing[idx].changes.extend(slot_changes.changes);
        } else {
            slot_positions.insert(slot_changes.slot, existing.len());
            existing.push(slot_changes);
        }
    }
}

fn build_big_block(
    blocks: Vec<ExecutionData>,
    big_block_idx: u64,
    first_source_block: u64,
    prior_block_hashes: Vec<(u64, B256)>,
    merged_block_access_list: Option<Bytes>,
) -> Result<BigBlockData<ExecutionData>> {
    if blocks.is_empty() {
        bail!("cannot build a big block with no source blocks");
    }

    Ok(BigBlockData {
        env_switches: blocks,
        prior_block_hashes,
        block_number: first_source_block + big_block_idx,
        merged_block_access_list,
    })
}
