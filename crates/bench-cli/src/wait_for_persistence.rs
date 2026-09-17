/// Policy for when `reth_newPayload` should wait for persistence.
#[derive(Debug, Clone)]
pub(crate) enum WaitForPersistence {
    /// Always wait for persistence on every block.
    Always,
    /// Never wait for persistence.
    Never,
    /// Wait for persistence every N blocks.
    EveryN(u64),
}

impl WaitForPersistence {
    /// Returns whether the request should wait for persistence for a given block index (0-based).
    pub(crate) fn should_wait(&self, block_index: u64) -> bool {
        match self {
            Self::Always => true,
            Self::Never => false,
            Self::EveryN(n) => (block_index + 1).is_multiple_of(*n),
        }
    }
}
