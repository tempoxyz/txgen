use alloy_primitives::Address;
use alloy_signer::Signer;
use alloy_signer_local::{LocalSigner, MnemonicBuilder, coins_bip39::English};
use eyre::{Result, WrapErr, bail};
use k256::ecdsa::SigningKey;
use rand::Rng;
use serde::Deserialize;
use std::collections::HashMap;

/// Type alias for our signer type.
pub type EcdsaSigner = LocalSigner<SigningKey>;

/// Manages account pools derived from mnemonics.
#[derive(Debug)]
pub struct AccountManager {
    pools: HashMap<String, Vec<EcdsaSigner>>,
}

impl AccountManager {
    /// Create an empty account manager (for testing).
    pub fn empty() -> Self {
        Self {
            pools: HashMap::new(),
        }
    }

    /// Create an account manager from spec definitions.
    pub fn from_spec(accounts: &HashMap<String, AccountPoolDef>) -> Result<Self> {
        let mut pools = HashMap::new();

        for (name, def) in accounts {
            let signers = def
                .derive_signers()
                .wrap_err_with(|| format!("failed to derive signers for pool '{name}'"))?;
            pools.insert(name.clone(), signers);
        }

        Ok(Self { pools })
    }

    /// Get all signers in a pool.
    pub fn get_pool(&self, name: &str) -> Result<&[EcdsaSigner]> {
        self.pools
            .get(name)
            .map(|v| v.as_slice())
            .ok_or_else(|| eyre::eyre!("account pool '{}' not found", name))
    }

    /// Get a random signer from a pool.
    pub fn get_random(&self, pool: &str, rng: &mut dyn rand::RngCore) -> Result<&EcdsaSigner> {
        let signers = self.get_pool(pool)?;
        if signers.is_empty() {
            bail!("account pool '{}' is empty", pool);
        }
        let idx = rng.random_range(0..signers.len());
        Ok(&signers[idx])
    }

    /// Get a signer by index from a pool.
    pub fn get_by_index(&self, pool: &str, index: usize) -> Result<&EcdsaSigner> {
        let signers = self.get_pool(pool)?;
        signers
            .get(index)
            .ok_or_else(|| eyre::eyre!("index {} out of range for pool '{}'", index, pool))
    }
}

/// Definition of an account pool in the workload spec.
#[derive(Debug, Clone, Deserialize)]
pub struct AccountPoolDef {
    /// BIP-39 mnemonic phrase (supports `${ENV_VAR}` expansion).
    pub mnemonic: String,

    /// Single account index (mutually exclusive with `range`).
    pub index: Option<u32>,

    /// Range of account indices `[start, end)` (mutually exclusive with `index`).
    pub range: Option<[u32; 2]>,
}

impl AccountPoolDef {
    /// Derive signers from this pool definition.
    pub fn derive_signers(&self) -> Result<Vec<EcdsaSigner>> {
        let indices: Vec<u32> = if let Some(idx) = self.index {
            vec![idx]
        } else if let Some([start, end]) = self.range {
            (start..end).collect()
        } else {
            bail!("account pool must have either 'index' or 'range'");
        };

        indices
            .into_iter()
            .map(|idx| {
                MnemonicBuilder::<English>::default()
                    .phrase(&self.mnemonic)
                    .index(idx)
                    .map_err(|e| eyre::eyre!("failed to set mnemonic: {e}"))?
                    .build()
                    .map_err(|e| eyre::eyre!("failed to derive signer at index {idx}: {e}"))
            })
            .collect()
    }
}

/// Reference to an account in a pool.
#[derive(Debug, Clone, Deserialize)]
pub struct AccountRef {
    /// Pool name.
    pub pool: String,
    /// Selection mode.
    pub select: SelectMode,
}

/// How to select an account from a pool.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectMode {
    /// Select randomly.
    Random,
    /// Select by specific index.
    Index(usize),
}

/// Extension trait for LocalSigner to get the address.
pub trait SignerExt {
    fn address(&self) -> Address;
}

impl SignerExt for EcdsaSigner {
    fn address(&self) -> Address {
        Signer::address(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_MNEMONIC: &str = "test test test test test test test test test test test junk";

    #[test]
    fn test_derive_single_signer() {
        let def = AccountPoolDef {
            mnemonic: TEST_MNEMONIC.to_string(),
            index: Some(0),
            range: None,
        };
        let signers = def.derive_signers().unwrap();
        assert_eq!(signers.len(), 1);
    }

    #[test]
    fn test_derive_range_signers() {
        let def = AccountPoolDef {
            mnemonic: TEST_MNEMONIC.to_string(),
            index: None,
            range: Some([0, 10]),
        };
        let signers = def.derive_signers().unwrap();
        assert_eq!(signers.len(), 10);

        // Verify all addresses are unique
        let addresses: std::collections::HashSet<_> = signers.iter().map(|s| s.address()).collect();
        assert_eq!(addresses.len(), 10);
    }

    #[test]
    fn test_account_manager_get_random() {
        use rand::SeedableRng;

        let mut accounts = HashMap::new();
        accounts.insert(
            "users".to_string(),
            AccountPoolDef {
                mnemonic: TEST_MNEMONIC.to_string(),
                index: None,
                range: Some([0, 5]),
            },
        );
        let manager = AccountManager::from_spec(&accounts).unwrap();

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let signer = manager.get_random("users", &mut rng).unwrap();
        assert!(!signer.address().is_zero());
    }
}
