use eyre::Result;
use rand::rngs::StdRng;
use serde::de::DeserializeOwned;

use crate::{AccountManager, ArtifactManager, GasConfig, GeneratedTx, NonceTracker};

/// Trait for chain-specific transaction generation plugins.
///
/// Each chain (Ethereum, Tempo, etc.) implements this trait to handle
/// its specific transaction types and encoding.
pub trait ChainPlugin: Send + Sync {
    /// The template type that this plugin can deserialize from YAML.
    type Template: DeserializeOwned;

    /// Returns the plugin name (e.g., "ethereum", "tempo").
    fn name(&self) -> &'static str;

    /// Build a signed transaction from a template.
    fn build(&self, template: Self::Template, ctx: &mut BuildContext<'_>) -> Result<GeneratedTx>;
}

/// Context passed to plugins during transaction generation.
pub struct BuildContext<'a> {
    /// Chain ID for transaction signing.
    pub chain_id: u64,

    /// Default gas configuration.
    pub gas: &'a GasConfig,

    /// Account manager for signer access.
    pub accounts: &'a AccountManager,

    /// Artifact manager for ABI access.
    pub artifacts: &'a ArtifactManager,

    /// Nonce tracker for ordering.
    pub nonces: &'a mut NonceTracker,

    /// Random number generator.
    pub rng: &'a mut StdRng,
}

impl<'a> BuildContext<'a> {
    /// Create a new build context.
    pub fn new(
        chain_id: u64,
        gas: &'a GasConfig,
        accounts: &'a AccountManager,
        artifacts: &'a ArtifactManager,
        nonces: &'a mut NonceTracker,
        rng: &'a mut StdRng,
    ) -> Self {
        Self {
            chain_id,
            gas,
            accounts,
            artifacts,
            nonces,
            rng,
        }
    }

    /// Get the next nonce for a scheduling key.
    pub fn next_nonce(&mut self, key: [u8; 20]) -> u64 {
        self.nonces.next(key)
    }

    /// Create a value resolver for this context.
    pub fn resolver(&mut self) -> crate::ValueResolver<'_> {
        crate::ValueResolver {
            accounts: self.accounts,
            rng: self.rng,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    struct MockPlugin;

    impl ChainPlugin for MockPlugin {
        type Template = serde_yaml::Value;

        fn name(&self) -> &'static str {
            "mock"
        }

        fn build(
            &self,
            _template: Self::Template,
            _ctx: &mut BuildContext<'_>,
        ) -> Result<GeneratedTx> {
            Ok(GeneratedTx {
                raw: alloy_primitives::Bytes::new(),
                key: [0u8; 20],
            })
        }
    }

    #[test]
    fn test_plugin_trait() {
        let plugin = MockPlugin;
        assert_eq!(plugin.name(), "mock");

        let gas = GasConfig::default();
        let accounts = AccountManager::empty();
        let artifacts = ArtifactManager::empty();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);
        let tx = plugin.build(serde_yaml::Value::Null, &mut ctx).unwrap();
        assert!(tx.raw.is_empty());
    }
}
