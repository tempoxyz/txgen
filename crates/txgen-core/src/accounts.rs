use alloy_primitives::{keccak256, Address, B256};
use alloy_signer::Signer;
use alloy_signer_local::{coins_bip39::English, LocalSigner, MnemonicBuilder};
use eyre::{bail, Result, WrapErr};
use k256::ecdsa::SigningKey;
use rand::Rng;
use serde::{Deserialize, Deserializer};
use std::{collections::HashMap, sync::Mutex};

/// Type alias for our signer type.
pub type EcdsaSigner = LocalSigner<SigningKey>;

/// Manages signer account pools derived from mnemonics.
#[derive(Debug)]
pub struct AccountManager {
    pools: HashMap<String, Vec<EcdsaSigner>>,
}

/// Manages destination-only address pools.
#[derive(Debug)]
pub struct AddressPoolManager {
    pools: HashMap<String, AddressPool>,
}

/// A destination-only address pool.
#[derive(Debug)]
enum AddressPool {
    /// Eager literal address list.
    Literal(Vec<Address>),
    /// Lazily-derived mnemonic address range.
    Mnemonic(MnemonicAddressPool),
    /// Fast deterministic address range.
    Fast(FastAddressPool),
}

/// A lazily-derived mnemonic address range with an on-demand cache.
#[derive(Debug)]
struct MnemonicAddressPool {
    mnemonic: String,
    start: u32,
    len: usize,
    cache: Mutex<HashMap<u32, Address>>,
}

/// A fast deterministic address range matching Tempo state bloat generation.
#[derive(Debug)]
struct FastAddressPool {
    seed: B256,
    start: u64,
    len: usize,
}

impl AccountManager {
    /// Create an empty account manager (for testing).
    pub fn empty() -> Self {
        Self { pools: HashMap::new() }
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

    /// Get all addresses grouped by pool name.
    pub fn all_addresses(&self) -> impl Iterator<Item = (&str, Vec<Address>)> {
        self.pools.iter().map(|(name, signers)| {
            let addresses: Vec<Address> = signers.iter().map(Signer::address).collect();
            (name.as_str(), addresses)
        })
    }
}

impl AddressPoolManager {
    /// Create an empty address pool manager (for testing and specs without destination pools).
    pub fn empty() -> Self {
        Self { pools: HashMap::new() }
    }

    /// Create an address pool manager from spec definitions.
    ///
    /// Mnemonic-backed pools are kept lazy: addresses are derived and cached on first use.
    pub fn from_spec(address_pools: &HashMap<String, AddressPoolDef>) -> Result<Self> {
        let mut pools = HashMap::new();

        for (name, def) in address_pools {
            let pool = def
                .to_pool()
                .wrap_err_with(|| format!("failed to create address pool '{name}'"))?;
            pools.insert(name.clone(), pool);
        }

        Ok(Self { pools })
    }

    /// Get a random address from a destination-only pool.
    pub fn get_random(&self, pool: &str, rng: &mut dyn rand::RngCore) -> Result<Address> {
        let address_pool = self.get_pool(pool)?;
        if address_pool.is_empty() {
            bail!("address pool '{}' is empty", pool);
        }
        let idx = rng.random_range(0..address_pool.len());
        address_pool
            .get_by_index(idx)
            .wrap_err_with(|| format!("failed to get random address from address pool '{pool}'"))
    }

    /// Get an address by index from a destination-only pool.
    pub fn get_by_index(&self, pool: &str, index: usize) -> Result<Address> {
        let address_pool = self.get_pool(pool)?;
        if index >= address_pool.len() {
            bail!("index {} out of range for address pool '{}'", index, pool);
        }
        address_pool
            .get_by_index(index)
            .wrap_err_with(|| format!("failed to get address pool '{pool}' index {index}"))
    }

    fn get_pool(&self, name: &str) -> Result<&AddressPool> {
        self.pools.get(name).ok_or_else(|| eyre::eyre!("address pool '{}' not found", name))
    }
}

impl AddressPool {
    fn len(&self) -> usize {
        match self {
            Self::Literal(addresses) => addresses.len(),
            Self::Mnemonic(pool) => pool.len,
            Self::Fast(pool) => pool.len,
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn get_by_index(&self, index: usize) -> Result<Address> {
        match self {
            Self::Literal(addresses) => addresses
                .get(index)
                .copied()
                .ok_or_else(|| eyre::eyre!("index {index} out of range for literal address pool")),
            Self::Mnemonic(pool) => pool.get_by_index(index),
            Self::Fast(pool) => pool.get_by_index(index),
        }
    }
}

impl MnemonicAddressPool {
    fn new(mnemonic: String, start: u32, len: usize) -> Self {
        Self { mnemonic, start, len, cache: Mutex::new(HashMap::new()) }
    }

    fn get_by_index(&self, index: usize) -> Result<Address> {
        let derivation_offset = u32::try_from(index)
            .map_err(|_| eyre::eyre!("address pool index {index} exceeds u32 derivation limit"))?;
        let derivation_index = self
            .start
            .checked_add(derivation_offset)
            .ok_or_else(|| eyre::eyre!("address pool index {index} overflows derivation path"))?;

        {
            let cache =
                self.cache.lock().map_err(|_| eyre::eyre!("address pool cache lock poisoned"))?;
            if let Some(address) = cache.get(&derivation_index) {
                return Ok(*address);
            }
        }

        let address = derive_address(&self.mnemonic, derivation_index)?;
        let mut cache =
            self.cache.lock().map_err(|_| eyre::eyre!("address pool cache lock poisoned"))?;
        Ok(*cache.entry(derivation_index).or_insert(address))
    }
}

impl FastAddressPool {
    fn new(seed: &str, start: u64, len: usize) -> Self {
        Self { seed: keccak256(seed.as_bytes()), start, len }
    }

    fn get_by_index(&self, index: usize) -> Result<Address> {
        let offset = u64::try_from(index)
            .map_err(|_| eyre::eyre!("address pool index {index} exceeds u64 derivation limit"))?;
        let derivation_index = self
            .start
            .checked_add(offset)
            .ok_or_else(|| eyre::eyre!("address pool index {index} overflows derivation path"))?;
        Ok(derive_fast_address(self.seed, derivation_index))
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
        let indices = selected_indices(self.index, self.range, "account pool")?;

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

/// Definition of a destination-only address pool in the workload spec.
#[derive(Debug, Clone, Deserialize)]
pub struct AddressPoolDef {
    /// Literal destination addresses.
    #[serde(default)]
    pub addresses: Vec<Address>,

    /// BIP-39 mnemonic phrase (supports `${ENV_VAR}` expansion).
    #[serde(default)]
    pub mnemonic: Option<String>,

    /// Single address index when deriving from a mnemonic (mutually exclusive with `range`).
    pub index: Option<u32>,

    /// Range of address indices `[start, end)` when deriving from a mnemonic.
    pub range: Option<[u32; 2]>,

    /// Fast deterministic address pool.
    #[serde(default)]
    pub fast: Option<FastAddressPoolDef>,
}

/// Definition of a fast deterministic destination-only address pool.
#[derive(Debug, Clone, Deserialize)]
pub struct FastAddressPoolDef {
    /// Seed string hashed before deriving addresses.
    pub seed: String,

    /// Single fast address index (mutually exclusive with `range`).
    pub index: Option<u64>,

    /// Range of fast address indices `[start, end)`.
    pub range: Option<[u64; 2]>,
}

impl AddressPoolDef {
    /// Derive destination-only addresses from this pool definition eagerly.
    pub fn derive_addresses(&self) -> Result<Vec<Address>> {
        let pool = self.to_pool()?;
        (0..pool.len()).map(|idx| pool.get_by_index(idx)).collect()
    }

    fn to_pool(&self) -> Result<AddressPool> {
        let has_addresses = !self.addresses.is_empty();
        let has_mnemonic = self.mnemonic.is_some();
        let has_fast = self.fast.is_some();
        let fields_set =
            usize::from(has_addresses) + usize::from(has_mnemonic) + usize::from(has_fast);

        if fields_set != 1 {
            bail!("address pool must set exactly one of 'addresses', 'mnemonic', or 'fast'");
        }

        if has_addresses {
            Ok(AddressPool::Literal(self.addresses.clone()))
        } else if has_mnemonic {
            self.to_mnemonic_pool()
        } else {
            self.to_fast_pool()
        }
    }

    fn to_mnemonic_pool(&self) -> Result<AddressPool> {
        let mnemonic = self
            .mnemonic
            .as_ref()
            .ok_or_else(|| eyre::eyre!("address pool must have a mnemonic"))?;
        let (start, len) = selected_range(self.index, self.range, "address pool")?;
        Ok(AddressPool::Mnemonic(MnemonicAddressPool::new(mnemonic.clone(), start, len)))
    }

    fn to_fast_pool(&self) -> Result<AddressPool> {
        let fast = self
            .fast
            .as_ref()
            .ok_or_else(|| eyre::eyre!("address pool must have a fast definition"))?;
        let (start, len) = selected_fast_range(fast.index, fast.range, "fast address pool")?;
        Ok(AddressPool::Fast(FastAddressPool::new(&fast.seed, start, len)))
    }
}

fn selected_indices(
    index: Option<u32>,
    range: Option<[u32; 2]>,
    context: &str,
) -> Result<Vec<u32>> {
    let (start, len) = selected_range(index, range, context)?;
    let end = start
        .checked_add(u32::try_from(len).map_err(|_| {
            eyre::eyre!("{context} range length {len} exceeds u32 derivation limit")
        })?)
        .ok_or_else(|| eyre::eyre!("{context} range overflows u32 derivation limit"))?;
    Ok((start..end).collect())
}

fn selected_range(
    index: Option<u32>,
    range: Option<[u32; 2]>,
    context: &str,
) -> Result<(u32, usize)> {
    if let Some(idx) = index {
        Ok((idx, 1))
    } else if let Some([start, end]) = range {
        Ok((start, end.saturating_sub(start) as usize))
    } else {
        bail!("{context} must have either 'index' or 'range'");
    }
}

fn selected_fast_range(
    index: Option<u64>,
    range: Option<[u64; 2]>,
    context: &str,
) -> Result<(u64, usize)> {
    if let Some(idx) = index {
        Ok((idx, 1))
    } else if let Some([start, end]) = range {
        if end < start {
            bail!("{context} range end must be greater than or equal to start");
        }
        let len = usize::try_from(end - start)
            .map_err(|_| eyre::eyre!("{context} range length exceeds usize"))?;
        Ok((start, len))
    } else {
        bail!("{context} must have either 'index' or 'range'");
    }
}

fn derive_address(mnemonic: &str, idx: u32) -> Result<Address> {
    MnemonicBuilder::<English>::default()
        .phrase(mnemonic)
        .index(idx)
        .map_err(|e| eyre::eyre!("failed to set mnemonic: {e}"))?
        .build()
        .map(|signer: EcdsaSigner| signer.address())
        .map_err(|e| eyre::eyre!("failed to derive address at index {idx}: {e}"))
}

fn derive_fast_address(seed: B256, index: u64) -> Address {
    let mut buf = [0u8; 40];
    buf[..32].copy_from_slice(seed.as_slice());
    buf[32..].copy_from_slice(&index.to_be_bytes());
    let hash = keccak256(buf);
    Address::from_slice(&hash.as_slice()[12..])
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
#[derive(Debug, Clone)]
pub enum SelectMode {
    /// Select randomly.
    Random,
    /// Select by specific index.
    Index(usize),
}

impl<'de> Deserialize<'de> for SelectMode {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum SelectModeDef {
            Name(String),
            Index { index: usize },
        }

        match SelectModeDef::deserialize(deserializer)? {
            SelectModeDef::Name(name) if name == "random" => Ok(SelectMode::Random),
            SelectModeDef::Name(other) => {
                Err(serde::de::Error::unknown_variant(&other, &["random", "index"]))
            }
            SelectModeDef::Index { index } => Ok(SelectMode::Index(index)),
        }
    }
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
        let def =
            AccountPoolDef { mnemonic: TEST_MNEMONIC.to_string(), index: Some(0), range: None };
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

    #[test]
    fn test_derive_mnemonic_address_pool() -> Result<()> {
        let def = AddressPoolDef {
            addresses: Vec::new(),
            mnemonic: Some(TEST_MNEMONIC.to_string()),
            index: None,
            range: Some([0, 3]),
            fast: None,
        };

        let addresses = def.derive_addresses()?;
        assert_eq!(addresses.len(), 3);
        assert_eq!(
            addresses[0],
            AccountPoolDef { mnemonic: TEST_MNEMONIC.to_string(), index: Some(0), range: None }
                .derive_signers()?[0]
                .address()
        );

        Ok(())
    }

    #[test]
    fn test_address_pool_manager_literal_addresses() -> Result<()> {
        let first = Address::from([1u8; 20]);
        let second = Address::from([2u8; 20]);
        let mut defs = HashMap::new();
        defs.insert(
            "recipients".to_string(),
            AddressPoolDef {
                addresses: vec![first, second],
                mnemonic: None,
                index: None,
                range: None,
                fast: None,
            },
        );

        let manager = AddressPoolManager::from_spec(&defs)?;
        assert_eq!(manager.get_by_index("recipients", 0)?, first);
        assert_eq!(manager.get_by_index("recipients", 1)?, second);

        Ok(())
    }

    #[test]
    fn test_mnemonic_address_pool_derives_large_range_lazily() -> Result<()> {
        let mut defs = HashMap::new();
        defs.insert(
            "recipients".to_string(),
            AddressPoolDef {
                addresses: Vec::new(),
                mnemonic: Some(TEST_MNEMONIC.to_string()),
                index: None,
                range: Some([0, 1_000_000]),
                fast: None,
            },
        );

        let manager = AddressPoolManager::from_spec(&defs)?;
        let address = manager.get_by_index("recipients", 999_999)?;
        let expected = AccountPoolDef {
            mnemonic: TEST_MNEMONIC.to_string(),
            index: Some(999_999),
            range: None,
        }
        .derive_signers()?[0]
            .address();

        assert_eq!(address, expected);
        Ok(())
    }

    #[test]
    fn test_fast_address_pool_matches_state_bloat_derivation() -> Result<()> {
        let mut defs = HashMap::new();
        defs.insert(
            "recipients".to_string(),
            AddressPoolDef {
                addresses: Vec::new(),
                mnemonic: None,
                index: None,
                range: None,
                fast: Some(FastAddressPoolDef {
                    seed: TEST_MNEMONIC.to_string(),
                    index: None,
                    range: Some([10_000, 1_000_000]),
                }),
            },
        );

        let manager = AddressPoolManager::from_spec(&defs)?;
        let address = manager.get_by_index("recipients", 0)?;
        let expected: Address = "0x8dd07b58a16a2f0fcb5e5814c3bd115870785683".parse()?;

        assert_eq!(address, expected);
        Ok(())
    }
}
