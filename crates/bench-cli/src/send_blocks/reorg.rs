//! Reorg batch scheduling for `bench send-blocks`.

use super::BlockLine;
use alloy_primitives::B256;
use std::collections::VecDeque;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReorgPhase {
    Buffering,
    Synthetic,
    Canonical,
}

#[derive(Clone)]
pub(super) enum ReorgStep {
    Synthetic {
        block: BlockLine,
        parent_block_hash: B256,
        branch_point_hash: B256,
        fork_length: usize,
        reorg_depth: usize,
    },
    Canonical {
        block: BlockLine,
        branch_point_hash: B256,
    },
}

/// Buffers enough canonical input to run one reorg cycle and exposes the next
/// branch operation. Synthetic blocks are always scheduled before canonical
/// blocks, and a successful synthetic step supplies the parent for the next
/// synthetic build.
pub(super) struct ReorgStateMachine {
    depth: usize,
    every: usize,
    pending: VecDeque<BlockLine>,
    phase: ReorgPhase,
    next_block: usize,
    batch_side_len: usize,
    batch_canonical_len: usize,
    branch_point_hash: Option<B256>,
    fork_parent_hash: Option<B256>,
}

impl ReorgStateMachine {
    pub(super) fn new(depth: usize, every: usize) -> Self {
        debug_assert!(depth > 0);
        debug_assert!(every > 0);
        Self {
            depth,
            every,
            pending: VecDeque::new(),
            phase: ReorgPhase::Buffering,
            next_block: 0,
            batch_side_len: 0,
            batch_canonical_len: 0,
            branch_point_hash: None,
            fork_parent_hash: None,
        }
    }

    pub(super) fn depth(&self) -> usize {
        self.depth
    }

    pub(super) fn every(&self) -> usize {
        self.every
    }

    pub(super) fn batch_side_len(&self) -> usize {
        self.batch_side_len
    }

    pub(super) fn batch_canonical_len(&self) -> usize {
        self.batch_canonical_len
    }

    pub(super) fn push(&mut self, block: BlockLine) {
        self.pending.push_back(block);
    }

    pub(super) fn first_pending(&self) -> Option<&BlockLine> {
        self.pending.front()
    }

    pub(super) fn is_buffering(&self) -> bool {
        self.phase == ReorgPhase::Buffering
    }

    pub(super) fn batch_ready(&self, flush: bool) -> bool {
        !self.pending.is_empty() && (flush || self.pending.len() >= self.required_lookahead())
    }

    pub(super) fn start_batch(&mut self, branch_point_hash: B256, flush: bool) -> bool {
        debug_assert!(self.is_buffering());

        if !self.batch_ready(flush) {
            return false;
        }

        self.batch_side_len = self.depth.min(self.pending.len());
        // At EOF there cannot be another synthetic cycle, so drain the
        // canonical tail in one bounded partial batch.
        self.batch_canonical_len =
            if flush { self.pending.len() } else { self.every.min(self.pending.len()) };
        self.next_block = 0;
        self.branch_point_hash = Some(branch_point_hash);
        self.fork_parent_hash = Some(branch_point_hash);
        self.phase = ReorgPhase::Synthetic;
        true
    }

    pub(super) fn current_step(&self) -> Option<ReorgStep> {
        match self.phase {
            ReorgPhase::Buffering => None,
            ReorgPhase::Synthetic => {
                let block = self.pending.get(self.next_block)?.clone();
                Some(ReorgStep::Synthetic {
                    block,
                    parent_block_hash: self.fork_parent_hash?,
                    branch_point_hash: self.branch_point_hash?,
                    fork_length: self.next_block + 1,
                    reorg_depth: self.depth,
                })
            }
            ReorgPhase::Canonical => Some(ReorgStep::Canonical {
                block: self.pending.get(self.next_block)?.clone(),
                branch_point_hash: self.branch_point_hash?,
            }),
        }
    }

    pub(super) fn synthetic_succeeded(&mut self, block_hash: B256) {
        debug_assert_eq!(self.phase, ReorgPhase::Synthetic);

        self.fork_parent_hash = Some(block_hash);
        self.next_block += 1;
        if self.next_block == self.batch_side_len {
            self.next_block = 0;
            self.phase = ReorgPhase::Canonical;
        }
    }

    pub(super) fn canonical_succeeded(&mut self) {
        debug_assert_eq!(self.phase, ReorgPhase::Canonical);

        self.next_block += 1;
        if self.next_block != self.batch_canonical_len {
            return;
        }

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

pub(super) fn synthetic_block_index(canonical_blocks_submitted: u64, fork_length: usize) -> u64 {
    let source_offset = u64::try_from(fork_length.saturating_sub(1)).unwrap_or(u64::MAX);
    canonical_blocks_submitted.saturating_add(source_offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::Bytes;
    use bench_core::WaitForPersistence;

    #[derive(Debug, Eq, PartialEq)]
    enum ScheduledStep {
        Synthetic { number: u64, parent: B256, branch_point: B256 },
        Canonical(u64),
    }

    fn test_hash(byte: u8) -> B256 {
        B256::from([byte; 32])
    }

    fn synthetic_hash(number: u64) -> B256 {
        test_hash(u8::try_from(number + 100).unwrap())
    }

    fn scheduler_block(number: u64) -> BlockLine {
        BlockLine {
            raw: Bytes::new(),
            bal: None,
            key: test_hash(u8::try_from(number).unwrap()),
            number,
            timestamp: number,
            gas_used: 0,
            gas_limit: 0,
            tx_count: 0,
        }
    }

    fn run_scheduler_batch(
        state: &mut ReorgStateMachine,
        branch_point: B256,
        flush: bool,
    ) -> Vec<ScheduledStep> {
        assert!(state.start_batch(branch_point, flush));
        let mut steps = Vec::new();

        while !state.is_buffering() {
            match state.current_step().unwrap() {
                ReorgStep::Synthetic { block, parent_block_hash, branch_point_hash, .. } => {
                    steps.push(ScheduledStep::Synthetic {
                        number: block.number,
                        parent: parent_block_hash,
                        branch_point: branch_point_hash,
                    });
                    state.synthetic_succeeded(synthetic_hash(block.number));
                }
                ReorgStep::Canonical { block, branch_point_hash } => {
                    assert_eq!(branch_point_hash, branch_point);
                    steps.push(ScheduledStep::Canonical(block.number));
                    state.canonical_succeeded();
                }
            }
        }

        steps
    }

    #[test]
    fn builds_each_side_chain_before_canonical_blocks() {
        let branch_one = test_hash(50);
        let branch_two = test_hash(51);
        let mut state = ReorgStateMachine::new(3, 3);
        for number in 1..=6 {
            state.push(scheduler_block(number));
        }

        let mut steps = run_scheduler_batch(&mut state, branch_one, false);
        steps.extend(run_scheduler_batch(&mut state, branch_two, false));

        assert_eq!(
            steps,
            vec![
                ScheduledStep::Synthetic {
                    number: 1,
                    parent: branch_one,
                    branch_point: branch_one,
                },
                ScheduledStep::Synthetic {
                    number: 2,
                    parent: synthetic_hash(1),
                    branch_point: branch_one,
                },
                ScheduledStep::Synthetic {
                    number: 3,
                    parent: synthetic_hash(2),
                    branch_point: branch_one,
                },
                ScheduledStep::Canonical(1),
                ScheduledStep::Canonical(2),
                ScheduledStep::Canonical(3),
                ScheduledStep::Synthetic {
                    number: 4,
                    parent: branch_two,
                    branch_point: branch_two,
                },
                ScheduledStep::Synthetic {
                    number: 5,
                    parent: synthetic_hash(4),
                    branch_point: branch_two,
                },
                ScheduledStep::Synthetic {
                    number: 6,
                    parent: synthetic_hash(5),
                    branch_point: branch_two,
                },
                ScheduledStep::Canonical(4),
                ScheduledStep::Canonical(5),
                ScheduledStep::Canonical(6),
            ]
        );
        assert!(state.pending.is_empty());
    }

    #[test]
    fn every_controls_canonical_blocks_between_side_chains() {
        let mut state = ReorgStateMachine::new(3, 2);
        for number in 1..=5 {
            state.push(scheduler_block(number));
        }

        let first = run_scheduler_batch(&mut state, test_hash(50), false);
        let second = run_scheduler_batch(&mut state, test_hash(51), false);

        assert_eq!(
            first
                .iter()
                .map(|step| match step {
                    ScheduledStep::Synthetic { number, .. } => (*number, true),
                    ScheduledStep::Canonical(number) => (*number, false),
                })
                .collect::<Vec<_>>(),
            vec![(1, true), (2, true), (3, true), (1, false), (2, false)]
        );
        assert_eq!(
            second
                .iter()
                .map(|step| match step {
                    ScheduledStep::Synthetic { number, .. } => (*number, true),
                    ScheduledStep::Canonical(number) => (*number, false),
                })
                .collect::<Vec<_>>(),
            vec![(3, true), (4, true), (5, true), (3, false), (4, false)]
        );
        assert_eq!(state.pending.front().map(|block| block.number), Some(5));
    }

    #[test]
    fn uses_extra_canonical_blocks_when_every_exceeds_depth() {
        let mut state = ReorgStateMachine::new(2, 3);
        for number in 1..=3 {
            state.push(scheduler_block(number));
        }

        let steps = run_scheduler_batch(&mut state, test_hash(50), false);

        assert_eq!(
            steps
                .iter()
                .map(|step| match step {
                    ScheduledStep::Synthetic { number, .. } => (*number, true),
                    ScheduledStep::Canonical(number) => (*number, false),
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
        state.push(scheduler_block(1));
        state.push(scheduler_block(2));

        assert!(!state.batch_ready(false));
        let steps = run_scheduler_batch(&mut state, branch_point, true);

        assert_eq!(
            steps,
            vec![
                ScheduledStep::Synthetic { number: 1, parent: branch_point, branch_point },
                ScheduledStep::Synthetic { number: 2, parent: synthetic_hash(1), branch_point },
                ScheduledStep::Canonical(1),
                ScheduledStep::Canonical(2),
            ]
        );
        assert!(state.pending.is_empty());
    }

    #[test]
    fn synthetic_persistence_cadence_follows_source_block_indexes() {
        let policy = WaitForPersistence::EveryN(2);

        let waits = (1..=4)
            .map(|fork_length| policy.should_wait(synthetic_block_index(0, fork_length)))
            .collect::<Vec<_>>();

        assert_eq!(waits, vec![Some(false), Some(true), Some(false), Some(true)]);
        assert_eq!(synthetic_block_index(2, 1), 2);
    }
}
