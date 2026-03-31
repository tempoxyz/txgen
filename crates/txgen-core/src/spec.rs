use crate::AccountPoolDef;
use alloy_primitives::Address;
use eyre::{Result, WrapErr};
use serde::Deserialize;
use std::{collections::HashMap, env, path::PathBuf};

/// Generation mode: transaction-level or block-level.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkloadMode {
    /// Generate individual transactions (default).
    #[default]
    Txs,
    /// Generate full blocks composed of transactions.
    Blocks,
}

/// Workload specification parsed from YAML.
#[derive(Debug, Clone, Deserialize)]
pub struct WorkloadSpec {
    /// Chain ID for transactions.
    pub chain_id: u64,

    /// Generation mode: `txs` (default) or `blocks`.
    #[serde(default)]
    pub mode: WorkloadMode,

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

    /// Weighted mix of templates for generation.
    #[serde(default)]
    pub mix: Vec<MixEntry>,

    /// Block templates keyed by name.
    #[serde(default)]
    pub block_templates: HashMap<String, BlockTemplate>,

    /// Weighted mix of block templates for generation.
    #[serde(default)]
    pub block_mix: Vec<BlockMixEntry>,
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

/// Entry in the weighted template mix.
#[derive(Debug, Clone, Deserialize)]
pub struct MixEntry {
    /// Template name (must exist in templates).
    pub template: String,
    /// Relative weight for random selection.
    pub weight: u64,
}

/// A block template defining how to compose a block from transaction templates.
#[derive(Debug, Clone, Deserialize)]
pub struct BlockTemplate {
    /// Transaction entries that make up the block.
    pub txs: Vec<BlockTxEntry>,

    /// Engine-level configuration for block production.
    #[serde(default)]
    pub engine: EngineConfig,
}

/// An entry in a block template's transaction list.
///
/// Either references a single template by name, or uses `mix` to randomly
/// select from the weighted tx mix.
#[derive(Debug, Clone, Deserialize)]
pub struct BlockTxEntry {
    /// Explicit template name (mutually exclusive with `mix`).
    pub template: Option<String>,
    /// Use the weighted tx `mix` to pick templates (mutually exclusive with `template`).
    pub mix: Option<bool>,
    /// Number of transactions to generate from this entry.
    #[serde(default = "default_tx_count")]
    pub count: u64,
}

fn default_tx_count() -> u64 {
    1
}

/// Engine-level configuration for block production.
#[derive(Debug, Clone, Deserialize)]
pub struct EngineConfig {
    /// Block gas limit.
    #[serde(default = "default_gas_limit")]
    pub gas_limit: u64,

    /// Timestamp strategy for successive blocks.
    #[serde(default)]
    pub timestamp: TimestampStrategy,

    /// Fee recipient address.
    pub fee_recipient: Option<Address>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            gas_limit: default_gas_limit(),
            timestamp: TimestampStrategy::default(),
            fee_recipient: None,
        }
    }
}

fn default_gas_limit() -> u64 {
    30_000_000
}

/// Strategy for assigning timestamps to generated blocks.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimestampStrategy {
    /// Increment by a fixed number of seconds (default: 12).
    #[default]
    Increment,
    /// Use wall-clock time.
    WallClock,
}

/// Entry in the weighted block template mix.
#[derive(Debug, Clone, Deserialize)]
pub struct BlockMixEntry {
    /// Block template name (must exist in `block_templates`).
    pub template: String,
    /// Relative weight for random selection.
    pub weight: u64,
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

    /// Get total weight of all tx mix entries.
    pub fn total_weight(&self) -> u64 {
        self.mix.iter().map(|e| e.weight).sum()
    }

    /// Get total weight of all block mix entries.
    pub fn block_total_weight(&self) -> u64 {
        self.block_mix.iter().map(|e| e.weight).sum()
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

    #[test]
    fn test_default_mode_is_txs() {
        let yaml = "chain_id: 1\n";
        let spec = WorkloadSpec::parse(yaml).unwrap();
        assert_eq!(spec.mode, WorkloadMode::Txs);
        assert!(spec.block_templates.is_empty());
        assert!(spec.block_mix.is_empty());
    }

    #[test]
    fn test_mode_blocks() {
        let yaml = r#"
chain_id: 1
mode: blocks
"#;
        let spec = WorkloadSpec::parse(yaml).unwrap();
        assert_eq!(spec.mode, WorkloadMode::Blocks);
    }

    #[test]
    fn test_block_templates_full() {
        let yaml = r#"
chain_id: 1
mode: blocks
templates:
  transfer: {}
  swap: {}
mix:
  - template: transfer
    weight: 70
  - template: swap
    weight: 30
block_templates:
  full_block:
    txs:
      - template: transfer
        count: 100
      - template: swap
        count: 50
    engine:
      gas_limit: 36000000
      timestamp: wallclock
      fee_recipient: "0x0000000000000000000000000000000000000001"
  mixed_block:
    txs:
      - mix: true
        count: 200
block_mix:
  - template: full_block
    weight: 80
  - template: mixed_block
    weight: 20
"#;
        let spec = WorkloadSpec::parse(yaml).unwrap();
        assert_eq!(spec.mode, WorkloadMode::Blocks);

        // block_templates
        assert_eq!(spec.block_templates.len(), 2);
        let full = &spec.block_templates["full_block"];
        assert_eq!(full.txs.len(), 2);
        assert_eq!(full.txs[0].template.as_deref(), Some("transfer"));
        assert_eq!(full.txs[0].count, 100);
        assert_eq!(full.txs[1].template.as_deref(), Some("swap"));
        assert_eq!(full.txs[1].count, 50);
        assert_eq!(full.engine.gas_limit, 36_000_000);
        assert_eq!(full.engine.timestamp, TimestampStrategy::WallClock);
        assert!(full.engine.fee_recipient.is_some());

        let mixed = &spec.block_templates["mixed_block"];
        assert_eq!(mixed.txs.len(), 1);
        assert_eq!(mixed.txs[0].mix, Some(true));
        assert_eq!(mixed.txs[0].count, 200);
        // Default engine config
        assert_eq!(mixed.engine.gas_limit, 30_000_000);
        assert_eq!(mixed.engine.timestamp, TimestampStrategy::Increment);
        assert!(mixed.engine.fee_recipient.is_none());

        // block_mix
        assert_eq!(spec.block_mix.len(), 2);
        assert_eq!(spec.block_mix[0].template, "full_block");
        assert_eq!(spec.block_mix[0].weight, 80);
        assert_eq!(spec.block_total_weight(), 100);
    }

    #[test]
    fn test_block_tx_entry_default_count() {
        let yaml = r#"
chain_id: 1
block_templates:
  simple:
    txs:
      - template: transfer
"#;
        let spec = WorkloadSpec::parse(yaml).unwrap();
        let simple = &spec.block_templates["simple"];
        assert_eq!(simple.txs[0].count, 1);
    }

    #[test]
    fn test_backward_compat_no_block_fields() {
        let yaml = r#"
chain_id: 1
templates:
  a: {}
mix:
  - template: a
    weight: 100
"#;
        let spec = WorkloadSpec::parse(yaml).unwrap();
        assert_eq!(spec.mode, WorkloadMode::Txs);
        assert!(spec.block_templates.is_empty());
        assert!(spec.block_mix.is_empty());
        assert_eq!(spec.total_weight(), 100);
        assert_eq!(spec.block_total_weight(), 0);
    }
}
