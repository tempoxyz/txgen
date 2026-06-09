use alloy_primitives::{Address, TxKind};
use eyre::Result;
use rand::rngs::StdRng;
use std::sync::OnceLock;

use crate::{
    AccountManager, AccountRef, AddressPoolManager, ArtifactManager, GasConfig, NonceTracker,
    SelectMode,
};

/// Result of selecting a signer from a pool.
pub struct SelectedSigner {
    /// Signer address.
    pub address: Address,
    /// Pool name.
    pub pool: String,
    /// Index within the pool.
    pub index: usize,
}

/// Context passed to plugins during transaction generation.
pub struct BuildContext<'a> {
    /// Chain ID for transaction signing.
    pub chain_id: u64,

    /// Default gas configuration.
    pub gas: &'a GasConfig,

    /// Account manager for signer access.
    pub accounts: &'a AccountManager,

    /// Destination-only address pool manager.
    pub address_pools: &'a AddressPoolManager,

    /// Artifact manager for ABI access.
    pub artifacts: &'a ArtifactManager,

    /// Nonce tracker for ordering.
    pub nonces: &'a mut NonceTracker,

    /// Random number generator.
    pub rng: &'a mut StdRng,
}

fn empty_address_pools() -> &'static AddressPoolManager {
    static EMPTY_ADDRESS_POOLS: OnceLock<AddressPoolManager> = OnceLock::new();
    EMPTY_ADDRESS_POOLS.get_or_init(AddressPoolManager::empty)
}

impl<'a> BuildContext<'a> {
    /// Create a new build context with no destination-only address pools.
    pub fn new(
        chain_id: u64,
        gas: &'a GasConfig,
        accounts: &'a AccountManager,
        artifacts: &'a ArtifactManager,
        nonces: &'a mut NonceTracker,
        rng: &'a mut StdRng,
    ) -> Self {
        Self::new_with_address_pools(
            chain_id,
            gas,
            accounts,
            empty_address_pools(),
            artifacts,
            nonces,
            rng,
        )
    }

    /// Create a new build context with destination-only address pools.
    pub fn new_with_address_pools(
        chain_id: u64,
        gas: &'a GasConfig,
        accounts: &'a AccountManager,
        address_pools: &'a AddressPoolManager,
        artifacts: &'a ArtifactManager,
        nonces: &'a mut NonceTracker,
        rng: &'a mut StdRng,
    ) -> Self {
        Self { chain_id, gas, accounts, address_pools, artifacts, nonces, rng }
    }

    /// Get the next nonce for a scheduling key.
    pub fn next_nonce(&mut self, key: [u8; 20]) -> u64 {
        self.nonces.next(key)
    }

    /// Create a value resolver for this context.
    pub fn resolver(&mut self) -> crate::ValueResolver<'_> {
        crate::ValueResolver {
            accounts: self.accounts,
            address_pools: self.address_pools,
            rng: self.rng,
        }
    }

    /// Select a signer from a pool based on the account reference.
    pub fn select_signer(&mut self, from: &AccountRef) -> Result<SelectedSigner> {
        match from.select {
            SelectMode::Random => {
                let signer = self.accounts.get_random(&from.pool, self.rng)?;
                let signer_addr = signer.address();
                let pool = self.accounts.get_pool(&from.pool)?;
                // SAFETY: the signer came from this pool, so it must be present
                let idx = pool.iter().position(|s| s.address() == signer_addr).unwrap_or(0);
                let address = self.accounts.get_address_by_index(&from.pool, idx)?;
                Ok(SelectedSigner { address, pool: from.pool.clone(), index: idx })
            }
            SelectMode::Index(idx) => {
                self.accounts.get_by_index(&from.pool, idx)?;
                Ok(SelectedSigner {
                    address: self.accounts.get_address_by_index(&from.pool, idx)?,
                    pool: from.pool.clone(),
                    index: idx,
                })
            }
        }
    }

    /// Encode a contract call definition into calldata.
    pub fn encode_call(&mut self, call_def: &crate::CallDef) -> Result<crate::EncodedCall> {
        let artifacts = self.artifacts;
        let mut resolver = self.resolver();
        call_def.encode(artifacts, &mut resolver)
    }

    /// Resolve an optional address to a transaction target.
    pub fn resolve_to(&mut self, to: &Option<crate::GenValue<Address>>) -> Result<TxKind> {
        match to {
            Some(gen_value) => {
                let mut resolver = self.resolver();
                let addr: Address = resolver.resolve_gen(gen_value)?;
                Ok(TxKind::Call(addr))
            }
            None => Ok(TxKind::Create),
        }
    }

    /// Resolve a generator value to its concrete type.
    pub fn resolve_value<T: Clone + serde::de::DeserializeOwned + crate::FromGenerator>(
        &mut self,
        value: &crate::GenValue<T>,
    ) -> Result<T> {
        let mut resolver = self.resolver();
        resolver.resolve_gen(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AccountAddressKind, AccountPoolDef};
    use rand::SeedableRng;
    use std::collections::HashMap;

    #[test]
    fn test_select_signer_by_index() {
        let pool_def = AccountPoolDef {
            mnemonic: "test test test test test test test test test test test junk".into(),
            index: None,
            range: Some([0, 3]),
            address_kind: AccountAddressKind::Signer,
            native_multisig_1_of_1: None,
        };
        let accounts =
            AccountManager::from_spec(&HashMap::from([("default".to_string(), pool_def)])).unwrap();
        let gas = GasConfig::default();
        let artifacts = ArtifactManager::empty();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);

        let account_ref = AccountRef { pool: "default".into(), select: SelectMode::Index(1) };
        let selected = ctx.select_signer(&account_ref).unwrap();
        assert_eq!(selected.index, 1);
        assert_eq!(selected.pool, "default");

        let expected = accounts.get_by_index("default", 1).unwrap().address();
        assert_eq!(selected.address, expected);
    }

    #[test]
    fn test_select_signer_random() {
        let pool_def = AccountPoolDef {
            mnemonic: "test test test test test test test test test test test junk".into(),
            index: None,
            range: Some([0, 3]),
            address_kind: AccountAddressKind::Signer,
            native_multisig_1_of_1: None,
        };
        let accounts =
            AccountManager::from_spec(&HashMap::from([("default".to_string(), pool_def)])).unwrap();
        let gas = GasConfig::default();
        let artifacts = ArtifactManager::empty();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);

        let account_ref = AccountRef { pool: "default".into(), select: SelectMode::Random };
        let selected = ctx.select_signer(&account_ref).unwrap();
        assert_eq!(selected.pool, "default");
        assert!(selected.index < 3);
    }
}
