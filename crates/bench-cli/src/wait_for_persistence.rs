use std::{num::NonZeroU64, str::FromStr};

/// Policy for when `reth_newPayload` should wait for persistence.
#[derive(Debug, Clone, Copy)]
pub(crate) enum WaitForPersistence {
    /// Always wait for persistence on every block.
    Always,
    /// Never wait for persistence.
    Never,
    /// Wait for persistence every N blocks.
    EveryN(NonZeroU64),
}

impl WaitForPersistence {
    /// Returns whether the request should wait for persistence for a given block index (0-based).
    pub(crate) fn should_wait(self, block_index: u64) -> bool {
        match self {
            Self::Always => true,
            Self::Never => false,
            Self::EveryN(n) => (block_index + 1).is_multiple_of(n.get()),
        }
    }
}

impl FromStr for WaitForPersistence {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "always" => Ok(Self::Always),
            "never" => Ok(Self::Never),
            _ => {
                let n = s.strip_prefix("every:").ok_or_else(|| {
                    format!("invalid value '{s}': expected 'always', 'never', or 'every:N'")
                })?;
                n.parse().map(Self::EveryN).map_err(|e| format!("invalid number in every:N: {e}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_policies() {
        assert!(matches!("always".parse(), Ok(WaitForPersistence::Always)));
        assert!(matches!("never".parse(), Ok(WaitForPersistence::Never)));
        assert!(matches!("every:3".parse(), Ok(WaitForPersistence::EveryN(n)) if n.get() == 3));
        for invalid in ["every:0", "every:", "every:x", "sometimes"] {
            assert!(invalid.parse::<WaitForPersistence>().is_err(), "{invalid}");
        }
    }
}
