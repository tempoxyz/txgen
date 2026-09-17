use alloy_eips::{
    eip7928::{AccountChanges, BlockAccessIndex, BlockAccessList, SlotChanges},
    BlockId, BlockNumberOrTag,
};
use alloy_network::Network;
use alloy_primitives::Bytes;
use alloy_provider::Provider;
use alloy_rpc_types_engine::ExecutionData;
use eyre::{Result, WrapErr};
use std::collections::{HashMap, HashSet};

pub(crate) async fn fetch_block_access_list<N, P>(
    provider: &P,
    block_num: u64,
) -> Result<BlockAccessList>
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

pub(crate) async fn fetch_encoded_block_access_list<N, P>(
    provider: &P,
    block_num: u64,
) -> Result<Bytes>
where
    N: Network,
    P: Provider<N>,
{
    provider
        .get_block_access_list_raw(BlockId::number(block_num))
        .await
        .wrap_err_with(|| format!("failed to fetch raw block access list {block_num}"))?
        .ok_or_else(|| eyre::eyre!("raw block access list not found for block {block_num}"))
}

pub(crate) fn merge_block_access_lists(
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
            change.block_access_index =
                BlockAccessIndex::new(change.block_access_index.get() + shift);
        }
    }
    for change in &mut account_changes.balance_changes {
        change.block_access_index = BlockAccessIndex::new(change.block_access_index.get() + shift);
    }
    for change in &mut account_changes.nonce_changes {
        change.block_access_index = BlockAccessIndex::new(change.block_access_index.get() + shift);
    }
    for change in &mut account_changes.code_changes {
        change.block_access_index = BlockAccessIndex::new(change.block_access_index.get() + shift);
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
