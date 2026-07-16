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
    Buffering,
    Synthetic,
    Canonical,
}

#[derive(Clone)]
struct ReorgBatch {
    depth: usize,
    every: usize,
    side_blocks: usize,
    canonical_blocks: usize,
    branch_point_hash: B256,
}

#[derive(Clone)]
struct ScheduledReorgStep {
    id: u64,
    batch_started: Option<ReorgBatch>,
    action: ReorgAction,
}

#[derive(Clone)]
enum ReorgAction {
    Synthetic(SyntheticStep),
    Canonical(CanonicalStep),
}

#[derive(Clone)]
struct SyntheticStep {
    block: BlockLine,
    parent_block_hash: B256,
    branch_point_hash: B256,
    fork_length: usize,
    reorg_depth: usize,
}

impl SyntheticStep {
    fn prepare_payload(&self) -> Result<PreparedSyntheticPayload> {
        let extracted =
            extract_tx_bytes_from_block_rlp(self.block.raw.as_ref()).wrap_err_with(|| {
                format!("failed to extract raw transaction bytes from block {}", self.block.number)
            })?;
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
            transactions: extracted.transactions,
            extra_data: Some(Bytes::new()),
        };

        Ok(PreparedSyntheticPayload {
            request,
            parent_beacon_block_root,
            kept_non_blob_txs: extracted.kept_non_blob_txs,
            skipped_blob_txs: extracted.skipped_blob_txs,
            dropped_non_blob_txs: extracted.dropped_non_blob_txs,
        })
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

struct PreparedSyntheticPayload {
    request: TestingBuildBlockRequestV1,
    parent_beacon_block_root: B256,
    kept_non_blob_txs: usize,
    skipped_blob_txs: usize,
    dropped_non_blob_txs: usize,
}

#[derive(Clone)]
struct CanonicalStep {
    block: BlockLine,
    branch_point_hash: B256,
}

#[derive(Clone, Copy, Debug)]
enum ReorgCompletion {
    Synthetic(B256),
    Canonical,
}

/// Pure scheduler for reorg batches.
///
/// The machine buffers canonical input, derives the initial branch point,
/// tracks the logical canonical head, and emits one action at a time. Callers
/// acknowledge an action only after all of its external work succeeds.
pub(super) struct ReorgStateMachine {
    depth: usize,
    every: usize,
    pending: VecDeque<BlockLine>,
    phase: ReorgPhase,
    next_block: usize,
    batch_side_len: usize,
    batch_canonical_len: usize,
    canonical_head: Option<B256>,
    branch_point_hash: Option<B256>,
    fork_parent_hash: Option<B256>,
    in_flight: Option<ScheduledReorgStep>,
    next_step_id: u64,
}

impl ReorgStateMachine {
    pub(super) fn new(depth: usize, every: usize) -> Self {
        assert!(depth > 0, "reorg depth must be greater than zero");
        assert!(every > 0, "reorg interval must be greater than zero");
        Self {
            depth,
            every,
            pending: VecDeque::new(),
            phase: ReorgPhase::Buffering,
            next_block: 0,
            batch_side_len: 0,
            batch_canonical_len: 0,
            canonical_head: None,
            branch_point_hash: None,
            fork_parent_hash: None,
            in_flight: None,
            next_step_id: 0,
        }
    }

    pub(super) fn push(&mut self, block: BlockLine) {
        self.pending.push_back(block);
    }

    fn next_step(&mut self, flush: bool) -> Result<Option<ScheduledReorgStep>> {
        if let Some(step) = &self.in_flight {
            return Ok(Some(step.clone()));
        }

        let id = self.next_step_id;
        let next_step_id = self
            .next_step_id
            .checked_add(1)
            .ok_or_else(|| eyre::eyre!("reorg step identifier overflow"))?;
        let batch_started = if self.phase == ReorgPhase::Buffering {
            let Some(batch) = self.start_batch(flush)? else {
                return Ok(None);
            };
            Some(batch)
        } else {
            None
        };

        let action = self.current_action()?;
        let step = ScheduledReorgStep { id, batch_started, action };
        self.next_step_id = next_step_id;
        self.in_flight = Some(step.clone());
        Ok(Some(step))
    }

    fn complete(&mut self, step_id: u64, completion: ReorgCompletion) -> Result<()> {
        let issued_step = self
            .in_flight
            .as_ref()
            .ok_or_else(|| eyre::eyre!("reorg state has no action awaiting completion"))?;
        if issued_step.id != step_id {
            eyre::bail!(
                "cannot complete reorg step {step_id}; step {} is awaiting completion",
                issued_step.id
            );
        }

        let completion_matches = matches!(
            (&issued_step.action, completion),
            (ReorgAction::Synthetic(_), ReorgCompletion::Synthetic(_)) |
                (ReorgAction::Canonical(_), ReorgCompletion::Canonical)
        );
        if !completion_matches {
            eyre::bail!("completion {completion:?} does not match reorg step {step_id}");
        }

        match (self.phase, completion) {
            (ReorgPhase::Synthetic, ReorgCompletion::Synthetic(block_hash)) => {
                self.fork_parent_hash = Some(block_hash);
                self.next_block += 1;
                if self.next_block == self.batch_side_len {
                    self.next_block = 0;
                    self.phase = ReorgPhase::Canonical;
                }
            }
            (ReorgPhase::Canonical, ReorgCompletion::Canonical) => {
                let canonical_head = self
                    .pending
                    .get(self.next_block)
                    .ok_or_else(|| eyre::eyre!("reorg state is missing its canonical block"))?
                    .key;
                self.canonical_head = Some(canonical_head);
                self.next_block += 1;
                if self.next_block == self.batch_canonical_len {
                    self.finish_batch();
                }
            }
            (phase, completion) => {
                eyre::bail!("cannot apply {completion:?} completion in {phase:?} phase");
            }
        }

        self.in_flight = None;
        Ok(())
    }

    fn start_batch(&mut self, flush: bool) -> Result<Option<ReorgBatch>> {
        if self.pending.is_empty() || (!flush && self.pending.len() < self.required_lookahead()) {
            return Ok(None);
        }

        // In reorg mode the first submitted block is always buffered, so the
        // initial canonical head comes from its encoded parent. Later heads
        // are advanced only when canonical actions are acknowledged.
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

        let side_blocks = self.depth.min(self.pending.len());
        // At EOF there cannot be another synthetic cycle, so drain the
        // canonical tail in one bounded partial batch.
        let has_full_batch = self.pending.len() >= self.required_lookahead();
        let canonical_blocks = if flush && !has_full_batch {
            self.pending.len()
        } else {
            self.every.min(self.pending.len())
        };

        self.canonical_head.get_or_insert(branch_point_hash);
        self.batch_side_len = side_blocks;
        self.batch_canonical_len = canonical_blocks;
        self.next_block = 0;
        self.branch_point_hash = Some(branch_point_hash);
        self.fork_parent_hash = Some(branch_point_hash);
        self.phase = ReorgPhase::Synthetic;

        Ok(Some(ReorgBatch {
            depth: self.depth,
            every: self.every,
            side_blocks,
            canonical_blocks,
            branch_point_hash,
        }))
    }

    fn current_action(&self) -> Result<ReorgAction> {
        match self.phase {
            ReorgPhase::Buffering => {
                eyre::bail!("reorg state has no current action while buffering");
            }
            ReorgPhase::Synthetic => {
                let block = self
                    .pending
                    .get(self.next_block)
                    .ok_or_else(|| eyre::eyre!("reorg state is missing its synthetic block"))?
                    .clone();
                Ok(ReorgAction::Synthetic(SyntheticStep {
                    block,
                    parent_block_hash: self.fork_parent_hash.ok_or_else(|| {
                        eyre::eyre!("reorg state is missing its synthetic parent")
                    })?,
                    branch_point_hash: self
                        .branch_point_hash
                        .ok_or_else(|| eyre::eyre!("reorg state is missing its branch point"))?,
                    fork_length: self.next_block + 1,
                    reorg_depth: self.depth,
                }))
            }
            ReorgPhase::Canonical => {
                let block = self
                    .pending
                    .get(self.next_block)
                    .ok_or_else(|| eyre::eyre!("reorg state is missing its canonical block"))?
                    .clone();
                Ok(ReorgAction::Canonical(CanonicalStep {
                    block,
                    branch_point_hash: self
                        .branch_point_hash
                        .ok_or_else(|| eyre::eyre!("reorg state is missing its branch point"))?,
                }))
            }
        }
    }

    fn finish_batch(&mut self) {
        for _ in 0..self.batch_canonical_len {
            self.pending.pop_front();
        }
        self.phase = ReorgPhase::Buffering;
        self.next_block = 0;
        self.batch_side_len = 0;
        self.batch_canonical_len = 0;
        self.branch_point_hash = None;
        self.fork_parent_hash = None;
    }

    fn required_lookahead(&self) -> usize {
        self.depth.max(self.every)
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
        let Some(step) = reorg_state.next_step(flush)? else {
            return Ok(());
        };
        let step_id = step.id;

        if let Some(batch) = step.batch_started {
            tracing::info!(
                reorg_depth = batch.depth,
                reorg_every = batch.every,
                side_blocks = batch.side_blocks,
                canonical_blocks = batch.canonical_blocks,
                branch_point = %batch.branch_point_hash,
                "Starting reorg batch"
            );
        }

        match step.action {
            ReorgAction::Synthetic(synthetic) => {
                let block_start = Instant::now();
                let block_hash = process_synthetic_block(
                    provider,
                    testing_provider,
                    &synthetic,
                    collector,
                    persistence_policy,
                )
                .await?;
                reorg_state.complete(step_id, ReorgCompletion::Synthetic(block_hash))?;
                wait_for_next_block(block_start, wait_time).await;
            }
            ReorgAction::Canonical(canonical) => {
                let block_start = Instant::now();
                // Keep both anchors at the common ancestor while switching
                // away from the completed synthetic fork.
                process_block(
                    provider,
                    &canonical.block,
                    collector,
                    Some(canonical.branch_point_hash),
                    persistence_policy,
                )
                .await?;
                reorg_state.complete(step_id, ReorgCompletion::Canonical)?;
                report_progress(collector, start, reporters)?;
                wait_for_next_block(block_start, wait_time).await;
            }
        }
    }
}

async fn process_synthetic_block(
    provider: &(impl Provider + RethApi<Ethereum>),
    testing_provider: &(impl Provider + TestingApi<Ethereum>),
    step: &SyntheticStep,
    collector: &MetricsCollector,
    persistence_policy: &WaitForPersistence,
) -> Result<B256> {
    let PreparedSyntheticPayload {
        request,
        parent_beacon_block_root,
        kept_non_blob_txs,
        skipped_blob_txs,
        dropped_non_blob_txs,
    } = step.prepare_payload()?;

    if skipped_blob_txs > 0 {
        tracing::warn!(skipped_blob_txs, "Skipped blob transactions while building synthetic fork");
    }
    if dropped_non_blob_txs > 0 {
        tracing::info!(
            kept_non_blob_txs,
            dropped_non_blob_txs,
            "Kept 90% of non-blob transactions while building synthetic fork"
        );
    }

    let envelope = testing_provider.build_block_v1(request).await.wrap_err_with(|| {
        format!(
            "testing_buildBlockV1 failed for block {}. Ensure the target exposes the hidden \
             testing RPC, for example: reth node --http --http.api eth,testing ...",
            step.block.number
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
            format!(
                "reth_newPayload failed for synthetic fork block after block {}",
                step.block.number
            )
        })?;

    if !payload_status.status.is_valid() {
        eyre::bail!(
            "reth_newPayload returned non-VALID status for synthetic fork block after block {}: {:?}",
            step.block.number,
            payload_status.status,
        );
    }

    let fcu_result = provider
        .reth_forkchoice_updated(step.forkchoice_state(synthetic_block_hash))
        .await
        .wrap_err_with(|| {
            format!(
                "reth_forkchoiceUpdated failed for synthetic fork block after block {}",
                step.block.number
            )
        })?;

    if !fcu_result.is_valid() {
        eyre::bail!(
            "reth_forkchoiceUpdated returned non-VALID status for synthetic fork block after block {}: {:?}",
            step.block.number,
            fcu_result.payload_status,
        );
    }

    tracing::info!(
        block = step.block.number,
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

struct ExtractedTransactions {
    transactions: Vec<Bytes>,
    kept_non_blob_txs: usize,
    skipped_blob_txs: usize,
    dropped_non_blob_txs: usize,
}

fn extract_tx_bytes_from_block_rlp(raw: &[u8]) -> Result<ExtractedTransactions> {
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

    Ok(ExtractedTransactions {
        kept_non_blob_txs: transactions.len(),
        transactions,
        skipped_blob_txs,
        dropped_non_blob_txs,
    })
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
        Canonical { number: u64, branch_point: B256 },
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

        while let Some(step) = state.next_step(flush).unwrap() {
            let step_id = step.id;
            match step.action {
                ReorgAction::Synthetic(synthetic) => {
                    steps.push(ObservedStep::Synthetic {
                        number: synthetic.block.number,
                        parent: synthetic.parent_block_hash,
                        branch_point: synthetic.branch_point_hash,
                    });
                    state
                        .complete(
                            step_id,
                            ReorgCompletion::Synthetic(synthetic_hash(synthetic.block.number)),
                        )
                        .unwrap();
                }
                ReorgAction::Canonical(canonical) => {
                    steps.push(ObservedStep::Canonical {
                        number: canonical.block.number,
                        branch_point: canonical.branch_point_hash,
                    });
                    state.complete(step_id, ReorgCompletion::Canonical).unwrap();
                }
            }
        }

        steps
    }

    #[test]
    fn builds_each_side_chain_before_canonical_blocks() {
        let initial_branch = test_hash(50);
        let second_branch = test_hash(3);
        let mut state = ReorgStateMachine::new(3, 3);
        push_chain(&mut state, 1..=6, initial_branch);

        let steps = run_scheduler(&mut state, false);

        assert_eq!(
            steps,
            vec![
                ObservedStep::Synthetic {
                    number: 1,
                    parent: initial_branch,
                    branch_point: initial_branch,
                },
                ObservedStep::Synthetic {
                    number: 2,
                    parent: synthetic_hash(1),
                    branch_point: initial_branch,
                },
                ObservedStep::Synthetic {
                    number: 3,
                    parent: synthetic_hash(2),
                    branch_point: initial_branch,
                },
                ObservedStep::Canonical { number: 1, branch_point: initial_branch },
                ObservedStep::Canonical { number: 2, branch_point: initial_branch },
                ObservedStep::Canonical { number: 3, branch_point: initial_branch },
                ObservedStep::Synthetic {
                    number: 4,
                    parent: second_branch,
                    branch_point: second_branch,
                },
                ObservedStep::Synthetic {
                    number: 5,
                    parent: synthetic_hash(4),
                    branch_point: second_branch,
                },
                ObservedStep::Synthetic {
                    number: 6,
                    parent: synthetic_hash(5),
                    branch_point: second_branch,
                },
                ObservedStep::Canonical { number: 4, branch_point: second_branch },
                ObservedStep::Canonical { number: 5, branch_point: second_branch },
                ObservedStep::Canonical { number: 6, branch_point: second_branch },
            ]
        );
        assert!(state.pending.is_empty());
    }

    #[test]
    fn every_controls_canonical_blocks_between_side_chains() {
        let mut state = ReorgStateMachine::new(3, 2);
        push_chain(&mut state, 1..=5, test_hash(50));

        let steps = run_scheduler(&mut state, false);

        assert_eq!(
            steps
                .iter()
                .map(|step| match step {
                    ObservedStep::Synthetic { number, .. } => (*number, true),
                    ObservedStep::Canonical { number, .. } => (*number, false),
                })
                .collect::<Vec<_>>(),
            vec![
                (1, true),
                (2, true),
                (3, true),
                (1, false),
                (2, false),
                (3, true),
                (4, true),
                (5, true),
                (3, false),
                (4, false),
            ]
        );
        assert_eq!(state.pending.front().map(|block| block.number), Some(5));
    }

    #[test]
    fn uses_extra_canonical_blocks_when_every_exceeds_depth() {
        let mut state = ReorgStateMachine::new(2, 3);
        push_chain(&mut state, 1..=3, test_hash(50));

        let steps = run_scheduler(&mut state, false);

        assert_eq!(
            steps
                .iter()
                .map(|step| match step {
                    ObservedStep::Synthetic { number, .. } => (*number, true),
                    ObservedStep::Canonical { number, .. } => (*number, false),
                })
                .collect::<Vec<_>>(),
            vec![(1, true), (2, true), (1, false), (2, false), (3, false)]
        );
        assert!(state.pending.is_empty());
    }

    #[test]
    fn flushes_a_partial_final_batch() {
        let branch_point = test_hash(50);
        let mut state = ReorgStateMachine::new(3, 1);
        push_chain(&mut state, 1..=2, branch_point);

        assert!(state.next_step(false).unwrap().is_none());
        let steps = run_scheduler(&mut state, true);

        assert_eq!(
            steps,
            vec![
                ObservedStep::Synthetic { number: 1, parent: branch_point, branch_point },
                ObservedStep::Synthetic { number: 2, parent: synthetic_hash(1), branch_point },
                ObservedStep::Canonical { number: 1, branch_point },
                ObservedStep::Canonical { number: 2, branch_point },
            ]
        );
        assert!(state.pending.is_empty());
    }

    #[test]
    fn flush_preserves_complete_reorg_cycles() {
        let branch_point = test_hash(50);
        let mut incremental = ReorgStateMachine::new(3, 3);
        let mut flushing = ReorgStateMachine::new(3, 3);
        push_chain(&mut incremental, 1..=6, branch_point);
        push_chain(&mut flushing, 1..=6, branch_point);

        let incremental_steps = run_scheduler(&mut incremental, false);
        let flushing_steps = run_scheduler(&mut flushing, true);

        assert_eq!(flushing_steps, incremental_steps);
        assert!(flushing.pending.is_empty());
    }

    #[test]
    fn derives_initial_branch_then_tracks_accepted_canonical_head() {
        let initial_branch = test_hash(50);
        let wrong_second_parent = test_hash(99);
        let mut state = ReorgStateMachine::new(1, 1);
        state.push(scheduler_block(1, initial_branch));
        state.push(scheduler_block(2, wrong_second_parent));

        let first = state.next_step(false).unwrap().unwrap();
        let first_id = first.id;
        let batch = first.batch_started.unwrap();
        assert_eq!(batch.branch_point_hash, initial_branch);
        let ReorgAction::Synthetic(first) = first.action else {
            panic!("expected first synthetic action");
        };
        assert_eq!(first.parent_block_hash, initial_branch);
        state.complete(first_id, ReorgCompletion::Synthetic(synthetic_hash(1))).unwrap();

        let canonical = state.next_step(false).unwrap().unwrap();
        let canonical_id = canonical.id;
        let ReorgAction::Canonical(canonical) = canonical.action else {
            panic!("expected canonical action");
        };
        assert_eq!(canonical.block.number, 1);
        state.complete(canonical_id, ReorgCompletion::Canonical).unwrap();

        let second = state.next_step(false).unwrap().unwrap();
        let ReorgAction::Synthetic(second) = second.action else {
            panic!("expected second synthetic action");
        };
        assert_eq!(second.parent_block_hash, test_hash(1));
        assert_eq!(second.branch_point_hash, test_hash(1));
    }

    #[test]
    fn does_not_advance_without_a_successful_completion() {
        let branch_point = test_hash(50);
        let mut state = ReorgStateMachine::new(1, 1);
        state.push(scheduler_block(1, branch_point));

        let first = state.next_step(false).unwrap().unwrap();
        let repeated = state.next_step(false).unwrap().unwrap();
        assert_eq!(first.id, repeated.id);
        assert!(first.batch_started.is_some());
        assert!(repeated.batch_started.is_some());
        let first_id = first.id;
        let ReorgAction::Synthetic(first) = first.action else {
            panic!("expected synthetic action");
        };
        let ReorgAction::Synthetic(repeated) = repeated.action else {
            panic!("expected repeated synthetic action");
        };
        assert_eq!(first.block.number, repeated.block.number);
        assert_eq!(first.parent_block_hash, repeated.parent_block_hash);

        state.complete(first_id, ReorgCompletion::Synthetic(synthetic_hash(1))).unwrap();
        let canonical = state.next_step(false).unwrap().unwrap();
        let repeated = state.next_step(false).unwrap().unwrap();
        assert_eq!(canonical.id, repeated.id);
        let ReorgAction::Canonical(canonical) = canonical.action else {
            panic!("expected canonical action");
        };
        let ReorgAction::Canonical(repeated) = repeated.action else {
            panic!("expected repeated canonical action");
        };
        assert_eq!(canonical.block.number, repeated.block.number);
    }

    #[test]
    fn rejects_wrong_duplicate_and_stale_completions() {
        let mut state = ReorgStateMachine::new(1, 1);
        state.push(scheduler_block(1, test_hash(50)));

        let synthetic = state.next_step(false).unwrap().unwrap();
        let wrong =
            state.complete(synthetic.id, ReorgCompletion::Canonical).unwrap_err().to_string();
        assert!(wrong.contains("does not match reorg step"));
        assert_eq!(state.next_step(false).unwrap().unwrap().id, synthetic.id);

        state.complete(synthetic.id, ReorgCompletion::Synthetic(synthetic_hash(1))).unwrap();
        let duplicate = state
            .complete(synthetic.id, ReorgCompletion::Synthetic(synthetic_hash(1)))
            .unwrap_err()
            .to_string();
        assert!(duplicate.contains("no action awaiting completion"));

        let canonical = state.next_step(false).unwrap().unwrap();
        let stale = state
            .complete(synthetic.id, ReorgCompletion::Synthetic(synthetic_hash(1)))
            .unwrap_err()
            .to_string();
        assert!(stale.contains("is awaiting completion"));
        assert_eq!(state.next_step(false).unwrap().unwrap().id, canonical.id);
    }

    #[test]
    #[should_panic(expected = "reorg depth must be greater than zero")]
    fn rejects_zero_depth() {
        ReorgStateMachine::new(0, 1);
    }

    #[test]
    #[should_panic(expected = "reorg interval must be greater than zero")]
    fn rejects_zero_interval() {
        ReorgStateMachine::new(1, 0);
    }

    #[test]
    fn malformed_initial_branch_does_not_mutate_the_machine() {
        let mut state = ReorgStateMachine::new(1, 1);
        let mut block = scheduler_block(1, test_hash(50));
        block.raw = Bytes::from_static(&[0xff]);
        state.push(block);

        let first_error = state.next_step(false).err().unwrap().to_string();
        let second_error = state.next_step(false).err().unwrap().to_string();

        assert!(first_error.contains("failed to extract parent hash from block 1"));
        assert_eq!(first_error, second_error);
        assert_eq!(state.phase, ReorgPhase::Buffering);
    }

    #[test]
    fn synthetic_persistence_cadence_follows_source_block_indexes() {
        let policy = WaitForPersistence::EveryN(2);
        let block = scheduler_block(1, test_hash(50));

        let waits = (1..=4)
            .map(|fork_length| {
                let step = SyntheticStep {
                    block: block.clone(),
                    parent_block_hash: B256::ZERO,
                    branch_point_hash: B256::ZERO,
                    fork_length,
                    reorg_depth: 4,
                };
                policy.should_wait(step.persistence_index(0))
            })
            .collect::<Vec<_>>();

        assert_eq!(waits, vec![Some(false), Some(true), Some(false), Some(true)]);
        let step = SyntheticStep {
            block,
            parent_block_hash: B256::ZERO,
            branch_point_hash: B256::ZERO,
            fork_length: 1,
            reorg_depth: 1,
        };
        assert_eq!(step.persistence_index(2), 2);
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
        assert_eq!(extracted.transactions, expected);
        assert_eq!(extracted.kept_non_blob_txs, 9);
        assert_eq!(extracted.dropped_non_blob_txs, 1);
        assert_eq!(extracted.skipped_blob_txs, 0);
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
        assert_eq!(extracted.transactions, expected);
        assert_eq!(extracted.kept_non_blob_txs, 9);
        assert_eq!(extracted.dropped_non_blob_txs, 1);
        assert_eq!(extracted.skipped_blob_txs, 1);
    }
}
