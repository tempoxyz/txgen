use crate::AccountPoolDef;
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

    /// Weighted mix of templates for generation.
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

/// Entry in the weighted template mix.
#[derive(Debug, Clone, Deserialize)]
pub struct MixEntry {
    /// Template name (must exist in templates).
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
