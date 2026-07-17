//! Reorg state machine and Engine API driver for `bench send-blocks`.

use super::{process_block, report_progress, wait_for_next_block, BlockLine, MetricsCollector};
use alloy_consensus::Header as ConsensusHeader;
use alloy_network::Ethereum;
use alloy_primitives::{Address, Bytes, B256};
use alloy_provider::{ext::TestingApi, Provider};
use alloy_rlp::{Decodable, Header};
use alloy_rpc_types_engine::{
    ExecutionData, ForkchoiceState, PayloadAttributes, TestingBuildBlockRequestV1,
};
use bench_core::{Reporter, RethApi, RethNewPayloadInput, WaitForPersistence};
use eyre::{Context, Result};
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

const REORG_NON_BLOB_TX_DROP_INTERVAL: usize = 10;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReorgPhase {
    AwaitingSideChain,
    Canonical { branch_point_hash: B256, remaining: usize },
}

struct ReorgBatch<'a> {
    synthetic_blocks: Vec<&'a BlockLine>,
    branch_point_hash: B256,
}

struct SyntheticStep<'a> {
    block: &'a BlockLine,
    parent_block_hash: B256,
    branch_point_hash: B256,
    fork_length: usize,
    reorg_depth: usize,
}

impl SyntheticStep<'_> {
    fn prepare_payload(
        &self,
        transactions: Vec<Bytes>,
    ) -> Result<(TestingBuildBlockRequestV1, B256)> {
        let parent_beacon_block_root = extract_block_header_from_block_rlp(self.block.raw.as_ref())
            .wrap_err_with(|| {
                format!(
                    "failed to extract parent beacon block root from block {}",
                    self.block.number
                )
            })?
            .parent_beacon_block_root;

        let payload_attributes = PayloadAttributes {
            timestamp: self.block.timestamp,
            prev_randao: B256::ZERO,
            suggested_fee_recipient: Address::ZERO,
            withdrawals: None,
            parent_beacon_block_root,
            slot_number: None,
            target_gas_limit: None,
        };
        let parent_beacon_block_root =
            payload_attributes.parent_beacon_block_root.unwrap_or_default();
        let request = TestingBuildBlockRequestV1 {
            parent_block_hash: self.parent_block_hash,
            payload_attributes,
            transactions,
            extra_data: Some(Bytes::new()),
        };

        Ok((request, parent_beacon_block_root))
    }

    fn persistence_index(&self, canonical_blocks_submitted: u64) -> u64 {
        let source_offset = u64::try_from(self.fork_length.saturating_sub(1)).unwrap_or(u64::MAX);
        canonical_blocks_submitted.saturating_add(source_offset)
    }

    fn forkchoice_state(&self, synthetic_block_hash: B256) -> ForkchoiceState {
        ForkchoiceState {
            head_block_hash: synthetic_block_hash,
            safe_block_hash: self.branch_point_hash,
            finalized_block_hash: self.branch_point_hash,
        }
    }
}

struct CanonicalStep<'a> {
    block: &'a BlockLine,
    forkchoice_anchor: Option<B256>,
}

/// Pure scheduler for reorg batches.
///
/// The machine decides when a side-chain batch can start and tracks how many
/// canonical blocks must follow it. An EOF tail shorter than `depth` is emitted
/// canonically instead of creating a partial side chain. A final gap may end
/// early at EOF because there is no subsequent side chain to space out.
pub(super) struct ReorgStateMachine {
    depth: usize,
    gap: usize,
    pending: VecDeque<BlockLine>,
    phase: ReorgPhase,
    canonical_head: Option<B256>,
}

impl ReorgStateMachine {
    pub(super) fn new(depth: usize, gap: usize) -> Result<Self> {
        assert!(depth > 0, "reorg depth must be greater than zero");
        eyre::ensure!(depth.checked_add(gap).is_some(), "reorg depth plus gap overflows usize");
        Ok(Self {
            depth,
            gap,
            pending: VecDeque::new(),
            phase: ReorgPhase::AwaitingSideChain,
            canonical_head: None,
        })
    }

    pub(super) fn push(&mut self, block: BlockLine) {
        self.pending.push_back(block);
    }

    fn next_batch(&self) -> Result<Option<ReorgBatch<'_>>> {
        if self.phase != ReorgPhase::AwaitingSideChain || self.pending.len() < self.depth {
            return Ok(None);
        }

        // Before the first side chain, derive the canonical head from the
        // encoded parent of its first source block. Later heads advance only
        // when canonical actions are acknowledged.
        let branch_point_hash = if let Some(canonical_head) = self.canonical_head {
            canonical_head
        } else {
            let first_block = self
                .pending
                .front()
                .ok_or_else(|| eyre::eyre!("reorg state is missing its first buffered block"))?;
            extract_block_header_from_block_rlp(first_block.raw.as_ref())
                .wrap_err_with(|| {
                    format!("failed to extract parent hash from block {}", first_block.number)
                })?
                .parent_hash
        };

        let synthetic_blocks = self.pending.iter().take(self.depth).collect::<Vec<_>>();
        Ok(Some(ReorgBatch { synthetic_blocks, branch_point_hash }))
    }

    fn synthetic_succeeded(&mut self, branch_point_hash: B256) {
        self.canonical_head.get_or_insert(branch_point_hash);
        self.phase = ReorgPhase::Canonical { branch_point_hash, remaining: self.depth + self.gap };
    }

    fn next_canonical(&self, flush: bool) -> Option<CanonicalStep<'_>> {
        let forkchoice_anchor = match self.phase {
            ReorgPhase::Canonical { branch_point_hash, remaining } => {
                (remaining > self.gap).then_some(branch_point_hash)
            }
            ReorgPhase::AwaitingSideChain if flush => None,
            ReorgPhase::AwaitingSideChain => return None,
        };
        self.pending.front().map(|block| CanonicalStep { block, forkchoice_anchor })
    }

    fn canonical_succeeded(&mut self) {
        let block = self.pending.pop_front().expect("canonical work requires a pending block");
        self.canonical_head = Some(block.key);

        if let ReorgPhase::Canonical { remaining, .. } = &mut self.phase {
            *remaining -= 1;
            if *remaining == 0 {
                self.phase = ReorgPhase::AwaitingSideChain;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn drive_reorg_state_machine(
    provider: &(impl Provider + RethApi<Ethereum>),
    testing_provider: &(impl Provider + TestingApi<Ethereum>),
    reorg_state: &mut ReorgStateMachine,
    collector: &mut MetricsCollector,
    persistence_policy: &WaitForPersistence,
    wait_time: Option<Duration>,
    start: Instant,
    reporters: &mut [Box<dyn Reporter>],
    flush: bool,
) -> Result<()> {
    loop {
        if let Some(batch) = reorg_state.next_batch()? {
            tracing::info!(
                reorg_depth = reorg_state.depth,
                reorg_gap = reorg_state.gap,
                side_blocks = batch.synthetic_blocks.len(),
                canonical_blocks = reorg_state.depth + reorg_state.gap,
                branch_point = %batch.branch_point_hash,
                "Starting reorg batch"
            );

            let mut parent_block_hash = batch.branch_point_hash;
            for (index, block) in batch.synthetic_blocks.iter().enumerate() {
                let block_start = Instant::now();
                parent_block_hash = process_synthetic_block(
                    provider,
                    testing_provider,
                    &SyntheticStep {
                        block,
                        parent_block_hash,
                        branch_point_hash: batch.branch_point_hash,
                        fork_length: index + 1,
                        reorg_depth: reorg_state.depth,
                    },
                    collector,
                    persistence_policy,
                )
                .await?;
                wait_for_next_block(block_start, wait_time).await;
            }
            reorg_state.synthetic_succeeded(batch.branch_point_hash);
        }

        let Some(canonical) = reorg_state.next_canonical(flush) else {
            return Ok(());
        };
        let block_start = Instant::now();
        // Replay blocks keep both anchors at the common ancestor; gap blocks
        // resume normal canonical anchors.
        process_block(
            provider,
            canonical.block,
            collector,
            canonical.forkchoice_anchor,
            persistence_policy,
        )
        .await?;
        reorg_state.canonical_succeeded();
        report_progress(collector, start, reporters)?;
        wait_for_next_block(block_start, wait_time).await;
    }
}

async fn process_synthetic_block(
    provider: &(impl Provider + RethApi<Ethereum>),
    testing_provider: &(impl Provider + TestingApi<Ethereum>),
    step: &SyntheticStep<'_>,
    collector: &MetricsCollector,
    persistence_policy: &WaitForPersistence,
) -> Result<B256> {
    let block = step.block;
    let transactions = extract_tx_bytes_from_block_rlp(block.raw.as_ref()).wrap_err_with(|| {
        format!("failed to extract raw transaction bytes from block {}", block.number)
    })?;
    let (request, parent_beacon_block_root) = step.prepare_payload(transactions)?;

    let envelope = testing_provider.build_block_v1(request).await.wrap_err_with(|| {
        format!(
            "testing_buildBlockV1 failed for block {}. Ensure the target exposes the hidden \
             testing RPC, for example: reth node --http --http.api eth,testing ...",
            block.number
        )
    })?;
    let synthetic_block_hash = envelope.execution_payload.payload_inner.payload_inner.block_hash;
    let (payload, sidecar) = envelope.into_payload_and_sidecar(parent_beacon_block_root);
    let execution_data = ExecutionData::new(payload, sidecar);

    // The testing builder only resolves canonical parent state. Submit and
    // select this payload before the state machine asks it to build the child.
    // Synthetic and canonical versions of the same source height use the same
    // persistence cadence even though the entire synthetic batch runs first.
    let wait = persistence_policy.should_wait(step.persistence_index(collector.blocks_submitted()));
    let payload_status = provider
        .reth_new_payload(RethNewPayloadInput::ExecutionData(Box::new(execution_data)), wait)
        .await
        .wrap_err_with(|| {
            format!("reth_newPayload failed for synthetic fork block after block {}", block.number)
        })?;

    if !payload_status.status.is_valid() {
        eyre::bail!(
            "reth_newPayload returned non-VALID status for synthetic fork block after block {}: {:?}",
            block.number,
            payload_status.status,
        );
    }

    let fcu_result = provider
        .reth_forkchoice_updated(step.forkchoice_state(synthetic_block_hash))
        .await
        .wrap_err_with(|| {
            format!(
                "reth_forkchoiceUpdated failed for synthetic fork block after block {}",
                block.number
            )
        })?;

    if !fcu_result.is_valid() {
        eyre::bail!(
            "reth_forkchoiceUpdated returned non-VALID status for synthetic fork block after block {}: {:?}",
            block.number,
            fcu_result.payload_status,
        );
    }

    tracing::info!(
        block = block.number,
        fork_length = step.fork_length,
        reorg_depth = step.reorg_depth,
        branch_point = %step.branch_point_hash,
        synthetic_head = %synthetic_block_hash,
        "Submitted synthetic fork block"
    );

    Ok(synthetic_block_hash)
}

fn extract_block_header_from_block_rlp(raw: &[u8]) -> Result<ConsensusHeader> {
    let mut block_buf = raw;
    let mut block_payload = Header::decode_bytes(&mut block_buf, true)
        .wrap_err("failed to decode outer block RLP list")?;
    if !block_buf.is_empty() {
        eyre::bail!("block RLP has trailing bytes after outer list");
    }

    ConsensusHeader::decode(&mut block_payload).wrap_err("failed to decode block header")
}

fn extract_tx_bytes_from_block_rlp(raw: &[u8]) -> Result<Vec<Bytes>> {
    let mut block_buf = raw;
    let mut block_payload = Header::decode_bytes(&mut block_buf, true)
        .wrap_err("failed to decode outer block RLP list")?;
    if !block_buf.is_empty() {
        eyre::bail!("block RLP has trailing bytes after outer list");
    }

    skip_rlp_item(&mut block_payload).wrap_err("failed to skip block header")?;
    let mut txs_payload = Header::decode_bytes(&mut block_payload, true)
        .wrap_err("failed to decode transactions RLP list")?;

    let mut transactions = Vec::new();
    let mut skipped_blob_txs = 0usize;
    let mut non_blob_txs = 0usize;
    let mut dropped_non_blob_txs = 0usize;
    while !txs_payload.is_empty() {
        let item_start = txs_payload;
        let header =
            Header::decode(&mut txs_payload).wrap_err("failed to decode transaction RLP item")?;
        let header_len = item_start.len() - txs_payload.len();
        let payload = &txs_payload[..header.payload_length];
        let encoded = &item_start[..header_len + header.payload_length];

        if header.list {
            non_blob_txs += 1;
            if should_keep_reorg_transaction(non_blob_txs) {
                transactions.push(Bytes::copy_from_slice(encoded));
            } else {
                dropped_non_blob_txs += 1;
            }
        } else if payload.first() == Some(&0x03) {
            skipped_blob_txs += 1;
        } else {
            non_blob_txs += 1;
            if should_keep_reorg_transaction(non_blob_txs) {
                transactions.push(Bytes::copy_from_slice(payload));
            } else {
                dropped_non_blob_txs += 1;
            }
        }

        txs_payload = &txs_payload[header.payload_length..];
    }

    if skipped_blob_txs > 0 {
        tracing::warn!(skipped_blob_txs, "Skipped blob transactions while building synthetic fork");
    }
    if dropped_non_blob_txs > 0 {
        tracing::info!(
            kept_non_blob_txs = transactions.len(),
            dropped_non_blob_txs,
            "Kept 90% of non-blob transactions while building synthetic fork"
        );
    }

    Ok(transactions)
}

fn should_keep_reorg_transaction(non_blob_tx_index: usize) -> bool {
    !non_blob_tx_index.is_multiple_of(REORG_NON_BLOB_TX_DROP_INTERVAL)
}

fn skip_rlp_item(buf: &mut &[u8]) -> Result<()> {
    let header = Header::decode(buf).wrap_err("failed to decode RLP item header")?;
    *buf = &buf[header.payload_length..];
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_rlp::Encodable;

    #[derive(Debug, Eq, PartialEq)]
    enum ObservedStep {
        Synthetic { number: u64, parent: B256, branch_point: B256 },
        Canonical { number: u64, branch_point: Option<B256> },
    }

    fn test_hash(byte: u8) -> B256 {
        B256::from([byte; 32])
    }

    fn synthetic_hash(number: u64) -> B256 {
        test_hash(u8::try_from(number + 100).unwrap())
    }

    fn rlp_bytes(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        payload.encode(&mut out);
        out
    }

    fn rlp_list_from_encoded(items: &[Vec<u8>]) -> Vec<u8> {
        let payload_length = items.iter().map(Vec::len).sum();
        let mut out = Vec::new();
        Header { list: true, payload_length }.encode(&mut out);
        for item in items {
            out.extend_from_slice(item);
        }
        out
    }

    fn legacy_tx(id: u8) -> Vec<u8> {
        rlp_list_from_encoded(&[rlp_bytes(&[id])])
    }

    fn typed_tx(tx_type: u8, id: u8) -> Vec<u8> {
        rlp_bytes(&[tx_type, id])
    }

    fn block_with_transactions(parent_hash: B256, transactions: &[Vec<u8>]) -> Vec<u8> {
        let header = ConsensusHeader { parent_hash, ..Default::default() };
        let mut encoded_header = Vec::new();
        header.encode(&mut encoded_header);
        let transactions = rlp_list_from_encoded(transactions);
        let ommers = rlp_list_from_encoded(&[]);
        rlp_list_from_encoded(&[encoded_header, transactions, ommers])
    }

    fn scheduler_block(number: u64, parent_hash: B256) -> BlockLine {
        BlockLine {
            raw: Bytes::from(block_with_transactions(parent_hash, &[])),
            bal: None,
            key: test_hash(u8::try_from(number).unwrap()),
            number,
            timestamp: number,
            gas_used: 0,
            gas_limit: 0,
            tx_count: 0,
        }
    }

    fn push_chain(
        state: &mut ReorgStateMachine,
        numbers: impl IntoIterator<Item = u64>,
        initial_parent: B256,
    ) {
        let mut parent = initial_parent;
        for number in numbers {
            let block = scheduler_block(number, parent);
            parent = block.key;
            state.push(block);
        }
    }

    fn run_scheduler(state: &mut ReorgStateMachine, flush: bool) -> Vec<ObservedStep> {
        let mut steps = Vec::new();

        loop {
            if let Some(batch) = state.next_batch().unwrap() {
                let mut parent = batch.branch_point_hash;
                for block in &batch.synthetic_blocks {
                    steps.push(ObservedStep::Synthetic {
                        number: block.number,
                        parent,
                        branch_point: batch.branch_point_hash,
                    });
                    parent = synthetic_hash(block.number);
                }
                state.synthetic_succeeded(batch.branch_point_hash);
            }

            let Some(canonical) = state.next_canonical(flush) else {
                break;
            };
            steps.push(ObservedStep::Canonical {
                number: canonical.block.number,
                branch_point: canonical.forkchoice_anchor,
            });
            state.canonical_succeeded();
        }

        steps
    }

    fn synthetic(number: u64, parent: B256, branch_point: B256) -> ObservedStep {
        ObservedStep::Synthetic { number, parent, branch_point }
    }

    fn canonical(number: u64, branch_point: B256) -> ObservedStep {
        ObservedStep::Canonical { number, branch_point: Some(branch_point) }
    }

    fn shape(steps: &[ObservedStep]) -> Vec<(u64, bool)> {
        steps
            .iter()
            .map(|step| match step {
                ObservedStep::Synthetic { number, .. } => (*number, true),
                ObservedStep::Canonical { number, .. } => (*number, false),
            })
            .collect()
    }

    #[test]
    fn chains_side_blocks_and_anchors_each_batch() {
        let first_branch = test_hash(50);
        let second_branch = test_hash(2);
        let mut state = ReorgStateMachine::new(2, 0).unwrap();
        push_chain(&mut state, 1..=4, first_branch);

        assert_eq!(
            run_scheduler(&mut state, false),
            vec![
                synthetic(1, first_branch, first_branch),
                synthetic(2, synthetic_hash(1), first_branch),
                canonical(1, first_branch),
                canonical(2, first_branch),
                synthetic(3, second_branch, second_branch),
                synthetic(4, synthetic_hash(3), second_branch),
                canonical(3, second_branch),
                canonical(4, second_branch),
            ]
        );
        assert!(state.pending.is_empty());
    }

    #[test]
    fn streams_canonical_gaps_without_overlapping_side_chains() {
        let mut state = ReorgStateMachine::new(2, 1).unwrap();
        push_chain(&mut state, 1..=2, test_hash(50));
        assert_eq!(
            shape(&run_scheduler(&mut state, false)),
            vec![(1, true), (2, true), (1, false), (2, false)]
        );
        assert_eq!(
            state.phase,
            ReorgPhase::Canonical { branch_point_hash: test_hash(50), remaining: 1 }
        );

        push_chain(&mut state, 3..=6, test_hash(2));
        assert_eq!(
            run_scheduler(&mut state, false),
            vec![
                ObservedStep::Canonical { number: 3, branch_point: None },
                synthetic(4, test_hash(3), test_hash(3)),
                synthetic(5, synthetic_hash(4), test_hash(3)),
                canonical(4, test_hash(3)),
                canonical(5, test_hash(3)),
                ObservedStep::Canonical { number: 6, branch_point: None },
            ]
        );
        assert_eq!(state.phase, ReorgPhase::AwaitingSideChain);
    }

    #[test]
    fn handles_eof_without_building_a_partial_side_chain() {
        let mut state = ReorgStateMachine::new(3, 0).unwrap();
        push_chain(&mut state, 1..=2, test_hash(50));

        assert!(run_scheduler(&mut state, false).is_empty());
        assert!(state.next_canonical(true).unwrap().forkchoice_anchor.is_none());
        assert_eq!(shape(&run_scheduler(&mut state, true)), vec![(1, false), (2, false)]);
        assert!(state.pending.is_empty());

        let mut state = ReorgStateMachine::new(3, 2).unwrap();
        push_chain(&mut state, 1..=4, test_hash(50));
        let steps = run_scheduler(&mut state, true);
        assert_eq!(steps.last(), Some(&ObservedStep::Canonical { number: 4, branch_point: None }));
    }

    #[test]
    fn rejects_an_overflowing_canonical_interval() {
        assert!(ReorgStateMachine::new(1, usize::MAX).is_err());
    }

    #[test]
    fn synthetic_persistence_cadence_follows_source_positions() {
        let block = scheduler_block(1, test_hash(50));
        let indexes = [(0, 1), (0, 2), (0, 3), (2, 1), (2, 2), (2, 3)].map(
            |(canonical_blocks, fork_length)| {
                let step = SyntheticStep {
                    block: &block,
                    parent_block_hash: B256::ZERO,
                    branch_point_hash: B256::ZERO,
                    fork_length,
                    reorg_depth: 3,
                };
                step.persistence_index(canonical_blocks)
            },
        );

        assert_eq!(indexes, [0, 1, 2, 2, 3, 4]);
        assert_eq!(
            indexes.map(|index| WaitForPersistence::EveryN(2).should_wait(index)),
            [Some(false), Some(true), Some(false), Some(false), Some(true), Some(false)]
        );
    }

    #[test]
    fn extracts_nine_of_ten_non_blob_transactions_for_reorg_payloads() {
        let source_transactions = (0..10).map(legacy_tx).collect::<Vec<_>>();
        let block = block_with_transactions(B256::ZERO, &source_transactions);

        let extracted = extract_tx_bytes_from_block_rlp(&block)
            .expect("valid block RLP should extract transactions");

        let expected = source_transactions
            .into_iter()
            .take(REORG_NON_BLOB_TX_DROP_INTERVAL - 1)
            .map(Bytes::from)
            .collect::<Vec<_>>();
        assert_eq!(extracted, expected);
    }

    #[test]
    fn blob_transactions_do_not_count_toward_reorg_payload_keep_ratio() {
        let mut source_transactions = (0..9).map(legacy_tx).collect::<Vec<_>>();
        source_transactions.push(typed_tx(0x03, 0));
        source_transactions.push(legacy_tx(9));
        let block = block_with_transactions(B256::ZERO, &source_transactions);

        let extracted = extract_tx_bytes_from_block_rlp(&block)
            .expect("valid block RLP should extract transactions");

        let expected = source_transactions
            .into_iter()
            .take(REORG_NON_BLOB_TX_DROP_INTERVAL - 1)
            .map(Bytes::from)
            .collect::<Vec<_>>();
        assert_eq!(extracted, expected);
    }
}
