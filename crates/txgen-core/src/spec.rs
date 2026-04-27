use crate::{AccountPoolDef, AccountRef, GenValue};
use alloy_primitives::{Address, U256};
use eyre::{Result, WrapErr};
use serde::Deserialize;
use std::{collections::HashMap, env, path::PathBuf};

/// Workload specification parsed from YAML.
#[derive(Debug, Clone, Deserialize)]
pub struct WorkloadSpec {
    /// Chain ID for transactions.
    pub chain_id: u64,

    /// Default gas configuration.
    #[serde(default)]
    pub gas: GasConfig,

    /// Account pools keyed by name.
    #[serde(default)]
    pub accounts: HashMap<String, AccountPoolDef>,

    /// ABI artifact paths keyed by name.
    #[serde(default)]
    pub artifacts: HashMap<String, PathBuf>,

    /// Transaction templates keyed by name (opaque to core, parsed by plugins).
    #[serde(default)]
    pub templates: HashMap<String, serde_yaml::Value>,

    /// Transaction sequences keyed by name.
    #[serde(default)]
    pub sequences: HashMap<String, SequenceDef>,

    /// Weighted mix of templates and sequences for generation.
    #[serde(default)]
    pub mix: Vec<MixEntry>,
}

/// Default gas configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct GasConfig {
    #[serde(default = "default_max_fee")]
    pub max_fee_per_gas: u128,
    #[serde(default = "default_priority_fee")]
    pub max_priority_fee_per_gas: u128,
}

impl Default for GasConfig {
    fn default() -> Self {
        Self {
            max_fee_per_gas: default_max_fee(),
            max_priority_fee_per_gas: default_priority_fee(),
        }
    }
}

fn default_max_fee() -> u128 {
    1_000_000_000 // 1 gwei
}

fn default_priority_fee() -> u128 {
    1_000_000_000 // 1 gwei
}

/// Entry in the weighted workload mix.
#[derive(Debug, Clone, Deserialize)]
pub struct MixEntry {
    /// Template name (must exist in templates). Mutually exclusive with `sequence`.
    #[serde(default)]
    pub template: Option<String>,
    /// Sequence name (must exist in sequences). Mutually exclusive with `template`.
    #[serde(default)]
    pub sequence: Option<String>,
    /// Relative weight for random selection.
    pub weight: u64,
}

/// A multi-transaction workload unit.
#[derive(Debug, Clone, Deserialize)]
pub struct SequenceDef {
    /// Values resolved once per sequence instance and reused by steps.
    #[serde(default)]
    pub bindings: HashMap<String, SequenceBinding>,
    /// Ordered transaction steps.
    pub steps: Vec<SequenceStep>,
}

/// A transaction step in a sequence.
#[derive(Debug, Clone, Deserialize)]
pub struct SequenceStep {
    /// Optional human-readable step name for diagnostics.
    #[serde(default)]
    pub name: Option<String>,
    /// Template name to instantiate for this step.
    pub template: String,
    /// Per-step YAML overlay applied over the referenced template.
    #[serde(default, rename = "with")]
    pub with_value: serde_yaml::Value,
}

/// A sequence binding definition.
#[derive(Debug, Clone, Deserialize)]
pub struct SequenceBinding {
    /// Select an account once. Exposes `<name>.ref` and `<name>.address`.
    #[serde(default)]
    pub account: Option<AccountRef>,
    /// Resolve an address once.
    #[serde(default)]
    pub address: Option<GenValue<Address>>,
    /// Resolve a U256 once.
    #[serde(default)]
    pub u256: Option<GenValue<U256>>,
    /// Resolve a u64 once.
    #[serde(default)]
    pub u64: Option<GenValue<u64>>,
    /// Resolve a string once.
    #[serde(default)]
    pub string: Option<GenValue<String>>,
    /// Resolve a Tempo nonce key once.
    #[serde(default)]
    pub nonce_key: Option<NonceKeyBinding>,
}

/// Sequence-scoped Tempo nonce-key binding.
#[derive(Debug, Clone, Deserialize)]
pub struct NonceKeyBinding {
    /// Generate a deterministic unique key per sequence instance.
    #[serde(default)]
    pub unique: bool,
    /// Base added to generated unique keys.
    #[serde(default)]
    pub base: Option<U256>,
    /// Explicit/generated value when `unique` is false.
    #[serde(default)]
    pub value: Option<GenValue<U256>>,
}

impl WorkloadSpec {
    /// Load and parse a workload spec from a YAML file.
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .wrap_err_with(|| format!("failed to read spec file: {}", path.display()))?;
        Self::parse(&content)
    }

    /// Parse a workload spec from YAML string.
    pub fn parse(yaml: &str) -> Result<Self> {
        let expanded = expand_env_vars(yaml);
        let spec: WorkloadSpec =
            serde_yaml::from_str(&expanded).wrap_err("failed to parse workload spec")?;
        Ok(spec)
    }

    /// Get total weight of all mix entries.
    pub fn total_weight(&self) -> u64 {
        self.mix.iter().map(|e| e.weight).sum()
    }
}

/// Expand `${VAR}` patterns with environment variable values.
fn expand_env_vars(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let mut var_name = String::new();
            for c in chars.by_ref() {
                if c == '}' {
                    break;
                }
                var_name.push(c);
            }
            if let Ok(value) = env::var(&var_name) {
                result.push_str(&value);
            }
        } else {
            result.push(c);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_spec() {
        let yaml = r#"
chain_id: 1
"#;
        let spec = WorkloadSpec::parse(yaml).unwrap();
        assert_eq!(spec.chain_id, 1);
        assert!(spec.accounts.is_empty());
        assert!(spec.templates.is_empty());
    }

    #[test]
    fn test_env_var_expansion() {
        // SAFETY: Test is single-threaded and we restore the env var after
        unsafe {
            env::set_var("TEST_MNEMONIC", "test test test");
        }
        let yaml = r#"
chain_id: 1
accounts:
  users:
    mnemonic: "${TEST_MNEMONIC}"
    range: [0, 10]
"#;
        let spec = WorkloadSpec::parse(yaml).unwrap();
        assert_eq!(spec.accounts["users"].mnemonic, "test test test");
        // SAFETY: Test cleanup
        unsafe {
            env::remove_var("TEST_MNEMONIC");
        }
    }

    #[test]
    fn test_total_weight() {
        let yaml = r#"
chain_id: 1
templates:
  a: {}
  b: {}
mix:
  - template: a
    weight: 60
  - template: b
    weight: 40
"#;
        let spec = WorkloadSpec::parse(yaml).unwrap();
        assert_eq!(spec.total_weight(), 100);
    }
}
