use crate::{
    yaml, AccountPoolDef, AccountRef, AddressPoolDef, ArtifactDef, GenValue, RecordPoolDef,
    RecordRef,
};
use alloy_primitives::{Address, B256, U256};
use eyre::{Result, WrapErr};
use serde::{Deserialize, Deserializer};
use std::collections::HashMap;

/// Workload specification parsed from YAML.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadSpec {
    /// Chain ID for transactions.
    pub chain_id: u64,

    /// Default gas configuration.
    #[serde(default)]
    pub gas: GasConfig,

    /// Signer account pools keyed by name.
    #[serde(default)]
    pub accounts: HashMap<String, AccountPoolDef>,

    /// Destination-only address pools keyed by name.
    #[serde(default)]
    pub address_pools: HashMap<String, AddressPoolDef>,

    /// External record pools keyed by name.
    #[serde(default)]
    pub record_pools: HashMap<String, RecordPoolDef>,

    /// ABI/deployment artifact definitions keyed by name.
    #[serde(default)]
    pub artifacts: HashMap<String, ArtifactDef>,

    /// Transaction templates keyed by name (opaque to core, parsed by plugins).
    #[serde(default)]
    pub templates: HashMap<String, serde_yaml::Value>,

    /// Deterministic setup transactions emitted before workload generation.
    #[serde(default)]
    pub setup: Option<SetupDef>,

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
#[derive(Debug, Clone)]
pub struct MixEntry {
    /// Workload item to generate.
    pub item: MixItem,
    /// Relative weight for random selection.
    pub weight: u64,
}

/// A named workload item referenced by the mix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MixItem {
    /// Template name (must exist in templates).
    Template(String),
    /// Sequence name (must exist in sequences).
    Sequence(String),
}

impl<'de> Deserialize<'de> for MixEntry {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct MixEntryDef {
            template: Option<String>,
            sequence: Option<String>,
            weight: u64,
        }

        let def = MixEntryDef::deserialize(deserializer)?;
        let item = match (def.template, def.sequence) {
            (Some(template), None) => MixItem::Template(template),
            (None, Some(sequence)) => MixItem::Sequence(sequence),
            (Some(_), Some(_)) => {
                return Err(serde::de::Error::custom(
                    "mix entries must set either `template` or `sequence`, not both",
                ))
            }
            (None, None) => {
                return Err(serde::de::Error::custom(
                    "mix entries must set either `template` or `sequence`",
                ))
            }
        };

        Ok(Self { item, weight: def.weight })
    }
}

/// Deterministic setup phase.
#[derive(Debug, Clone, Deserialize)]
pub struct SetupDef {
    /// Ordered setup steps. All setup transactions are emitted before workload transactions.
    #[serde(default)]
    pub steps: Vec<SetupStep>,
}

/// One deterministic setup step.
#[derive(Debug, Clone, Deserialize)]
pub struct SetupStep {
    /// Step identifier. Exposed to later steps and workload templates as `setup.<id>.*`.
    pub id: String,
    /// Values resolved once for this setup step.
    #[serde(default)]
    pub bindings: HashMap<String, SequenceBinding>,
    /// Contract deployment definition.
    #[serde(default)]
    pub deploy: Option<serde_yaml::Value>,
    /// Transaction definition using the same shape as workload templates.
    #[serde(default)]
    pub tx: Option<serde_yaml::Value>,
    /// Tempo-specific keychain pool authorization setup action.
    #[serde(default)]
    pub keychain_authorize_pool: Option<serde_yaml::Value>,
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
#[derive(Debug, Clone)]
pub enum SequenceBinding {
    /// Select an account once. Exposes `<name>.ref` and `<name>.address`.
    Account(AccountRef),
    /// Select one external record. Exposes its fields as `<name>.<field>`.
    Record(RecordRef),
    /// Resolve an address once.
    Address(GenValue<Address>),
    /// Resolve a bytes32 value once.
    Bytes32(GenValue<B256>),
    /// ABI packed-encode values once.
    AbiEncodePacked(AbiEncodePackedDef),
    /// Resolve a Keccak-256 hash over ABI-encoded values once.
    AbiHash(AbiHashDef),
    /// Resolve a U256 once.
    U256(GenValue<U256>),
    /// Resolve a u64 once.
    U64(GenValue<u64>),
    /// Resolve a string once.
    String(GenValue<String>),
}

/// Values to ABI packed-encode.
#[derive(Debug, Clone, Deserialize)]
pub struct AbiEncodePackedDef {
    /// Solidity ABI types, one per value.
    pub types: Vec<String>,
    /// Values to encode using the corresponding type.
    #[serde(alias = "args")]
    pub values: Vec<serde_yaml::Value>,
}

/// Values to ABI-encode and hash with Keccak-256.
#[derive(Debug, Clone, Deserialize)]
pub struct AbiHashDef {
    /// Solidity ABI types, one per value.
    pub types: Vec<String>,
    /// Values to encode using the corresponding type.
    #[serde(alias = "args")]
    pub values: Vec<serde_yaml::Value>,
}

impl<'de> Deserialize<'de> for SequenceBinding {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct SequenceBindingDef {
            account: Option<AccountRef>,
            record: Option<RecordRef>,
            address: Option<GenValue<Address>>,
            bytes32: Option<GenValue<B256>>,
            abi_encode_packed: Option<AbiEncodePackedDef>,
            abi_hash: Option<AbiHashDef>,
            u256: Option<GenValue<U256>>,
            u64: Option<GenValue<u64>>,
            string: Option<GenValue<String>>,
        }

        let def = SequenceBindingDef::deserialize(deserializer)?;
        let mut fields_set = 0;
        fields_set += usize::from(def.account.is_some());
        fields_set += usize::from(def.record.is_some());
        fields_set += usize::from(def.address.is_some());
        fields_set += usize::from(def.bytes32.is_some());
        fields_set += usize::from(def.abi_encode_packed.is_some());
        fields_set += usize::from(def.abi_hash.is_some());
        fields_set += usize::from(def.u256.is_some());
        fields_set += usize::from(def.u64.is_some());
        fields_set += usize::from(def.string.is_some());

        if fields_set != 1 {
            return Err(serde::de::Error::custom(
                "sequence bindings must set exactly one binding type",
            ));
        }

        if let Some(account) = def.account {
            Ok(Self::Account(account))
        } else if let Some(record) = def.record {
            Ok(Self::Record(record))
        } else if let Some(address) = def.address {
            Ok(Self::Address(address))
        } else if let Some(bytes32) = def.bytes32 {
            Ok(Self::Bytes32(bytes32))
        } else if let Some(abi_encode_packed) = def.abi_encode_packed {
            Ok(Self::AbiEncodePacked(abi_encode_packed))
        } else if let Some(abi_hash) = def.abi_hash {
            Ok(Self::AbiHash(abi_hash))
        } else if let Some(u256) = def.u256 {
            Ok(Self::U256(u256))
        } else if let Some(u64_value) = def.u64 {
            Ok(Self::U64(u64_value))
        } else if let Some(string) = def.string {
            Ok(Self::String(string))
        } else {
            unreachable!("fields_set verified exactly one binding type")
        }
    }
}

impl WorkloadSpec {
    /// Load and parse a workload spec from a YAML file.
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let value = yaml::load_yaml(path)?;
        serde_yaml::from_value(value).wrap_err("failed to parse workload spec")
    }

    /// Parse a workload spec from YAML string.
    pub fn parse(yaml: &str) -> Result<Self> {
        let value = yaml::parse_yaml(yaml)?;
        serde_yaml::from_value(value).wrap_err("failed to parse workload spec")
    }

    /// Get total weight of all mix entries.
    pub fn total_weight(&self) -> u64 {
        self.mix.iter().map(|e| e.weight).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn test_unknown_top_level_key_fails() {
        let yaml = r#"
chain_id: 1
templtes: {}
"#;
        let err = WorkloadSpec::parse(yaml).expect_err("unknown key should fail");
        assert!(err.chain().any(|cause| cause.to_string().contains("unknown field `templtes`")));
    }

    #[test]
    fn test_parse_minimal_spec() {
        let yaml = r#"
chain_id: 1
"#;
        let spec = WorkloadSpec::parse(yaml).unwrap();
        assert_eq!(spec.chain_id, 1);
        assert!(spec.accounts.is_empty());
        assert!(spec.address_pools.is_empty());
        assert!(spec.record_pools.is_empty());
        assert!(spec.templates.is_empty());
    }

    #[test]
    fn test_env_var_expansion() {
        // SAFETY: Test is single-threaded and we restore the env var after
        unsafe {
            std::env::set_var("TEST_MNEMONIC", "test test test");
        }
        let yaml = r#"
chain_id: 1
accounts:
  users:
    mnemonic: "${TEST_MNEMONIC}"
    range: [0, 10]
address_pools:
  recipients:
    mnemonic: "${TEST_MNEMONIC}"
    range: [10, 20]
"#;
        let spec = WorkloadSpec::parse(yaml).unwrap();
        assert_eq!(spec.accounts["users"].mnemonic, "test test test");
        assert_eq!(spec.address_pools["recipients"].mnemonic.as_deref(), Some("test test test"));
        // SAFETY: Test cleanup
        unsafe {
            std::env::remove_var("TEST_MNEMONIC");
        }
    }

    #[test]
    fn test_missing_env_var_fails() {
        // SAFETY: This test uses a unique variable name and removes it before parsing.
        unsafe {
            std::env::remove_var("TXGEN_TEST_MISSING_ENV_VAR");
        }
        let yaml = r#"
chain_id: 1
accounts:
  users:
    mnemonic: "${TXGEN_TEST_MISSING_ENV_VAR}"
    range: [0, 10]
"#;

        let err = WorkloadSpec::parse(yaml).expect_err("missing env var should fail");
        assert!(err.to_string().contains("failed to expand environment variables"));
        assert!(err.chain().any(|cause| cause.to_string().contains("TXGEN_TEST_MISSING_ENV_VAR")));
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
    fn test_parse_sequence_spec() {
        let yaml = r#"
chain_id: 1
templates:
  transfer:
    from: { var: sender.ref }
    to: { var: recipient.address }
    value: { var: amount }
sequences:
  pair:
    bindings:
      sender:
        account: { pool: users, select: random }
      recipient:
        account: { pool: users, select: { index: 1 } }
      amount:
        u256: { uniform: [1, 10] }
      random_recipient:
        address: random
      pooled_recipient:
        address:
          address_pool:
            pool: recipients
            select: random
      salt:
        bytes32: { random_bytes: 32 }
      channel_id:
        abi_hash:
          types: [address, bytes32, uint256]
          values:
            - { var: sender.address }
            - { var: salt }
            - { var: chain_id }
    steps:
      - name: first
        template: transfer
      - name: second
        template: transfer
        with:
          value: 11
mix:
  - sequence: pair
    weight: 100
"#;
        let spec = WorkloadSpec::parse(yaml).unwrap();
        let sequence = &spec.sequences["pair"];
        assert_eq!(sequence.steps.len(), 2);
        assert!(matches!(sequence.bindings["amount"], SequenceBinding::U256(_)));
        assert!(matches!(sequence.bindings["random_recipient"], SequenceBinding::Address(_)));
        assert!(matches!(sequence.bindings["pooled_recipient"], SequenceBinding::Address(_)));
        assert!(matches!(sequence.bindings["salt"], SequenceBinding::Bytes32(_)));
        assert!(matches!(sequence.bindings["channel_id"], SequenceBinding::AbiHash(_)));
        assert_eq!(spec.mix[0].item, MixItem::Sequence("pair".to_string()));
    }

    #[test]
    fn test_parse_record_pool_binding() {
        let yaml = r#"
chain_id: 1
record_pools:
  claims:
    path: claims.json
templates:
  claim: {}
sequences:
  claims:
    bindings:
      claim:
        record:
          pool: claims
          select: shuffled_once
    steps:
      - template: claim
mix:
  - sequence: claims
    weight: 1
"#;

        let spec = WorkloadSpec::parse(yaml).unwrap();
        assert_eq!(spec.record_pools["claims"].path, PathBuf::from("claims.json"));
        assert!(matches!(spec.sequences["claims"].bindings["claim"], SequenceBinding::Record(_)));
    }

    #[test]
    fn test_load_composed_spec_with_merge_and_append() {
        let dir = temp_spec_dir("composed");
        let pieces = dir.join("pieces");
        fs::create_dir_all(&pieces).unwrap();

        write_file(
            &pieces.join("base.yml"),
            r#"
merge:
  chain_id: 1
  artifacts:
    ERC20: erc20.json
  record_pools:
    claims:
      path: claims.json
  templates:
    transfer:
      type: eip1559
      gas_limit: 21000
  mix:
    - template: transfer
      weight: 1
"#,
        );
        write_file(
            &pieces.join("gas.yml"),
            r#"
merge:
  templates:
    transfer:
      gas_limit: 30000
"#,
        );
        write_file(
            &pieces.join("setup-a.yml"),
            r#"
append:
  setup:
    steps:
      - id: first
        tx:
          type: eip1559
"#,
        );
        write_file(
            &pieces.join("setup-b.yml"),
            r#"
append:
  setup:
    steps:
      - id: second
        tx:
          type: eip1559
"#,
        );
        write_file(
            &dir.join("spec.yml"),
            r#"
include:
  - pieces/base.yml
  - pieces/gas.yml
  - pieces/setup-a.yml
  - pieces/setup-b.yml
"#,
        );

        let spec = WorkloadSpec::load(&dir.join("spec.yml")).unwrap();
        assert_eq!(spec.chain_id, 1);
        assert_eq!(spec.templates["transfer"]["gas_limit"], serde_yaml::to_value(30000).unwrap());
        let setup = spec.setup.expect("setup should be appended");
        assert_eq!(setup.steps.len(), 2);
        assert_eq!(setup.steps[0].id, "first");
        assert_eq!(setup.steps[1].id, "second");

        let ArtifactDef::Path(path) = &spec.artifacts["ERC20"] else {
            panic!("expected path artifact");
        };
        assert!(path.is_absolute());
        assert_eq!(path, &pieces.join("erc20.json"));
        assert_eq!(spec.record_pools["claims"].path, pieces.join("claims.json"));

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_append_rejects_non_sequence_leaf() {
        let dir = temp_spec_dir("append-invalid");
        write_file(
            &dir.join("spec.yml"),
            r#"
chain_id: 1
append:
  setup:
    steps:
      id: not-a-list
"#,
        );

        let err = WorkloadSpec::load(&dir.join("spec.yml")).expect_err("append should fail");
        assert!(err
            .chain()
            .any(|cause| cause.to_string().contains("append section leaves must be sequences")));

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_include_cycle_fails() {
        let dir = temp_spec_dir("include-cycle");
        write_file(&dir.join("a.yml"), "include: b.yml\n");
        write_file(&dir.join("b.yml"), "include: a.yml\n");

        let err = WorkloadSpec::load(&dir.join("a.yml")).expect_err("cycle should fail");
        assert!(err.chain().any(|cause| cause.to_string().contains("cyclic spec include")));

        fs::remove_dir_all(dir).unwrap();
    }

    fn temp_spec_dir(name: &str) -> PathBuf {
        let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let dir =
            std::env::temp_dir().join(format!("txgen-{name}-{}-{unique}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        // Canonicalize so path assertions survive symlinked temp dirs (macOS `/var`).
        fs::canonicalize(dir).unwrap()
    }

    fn write_file(path: &Path, content: &str) {
        fs::write(path, content.trim_start()).unwrap();
    }
}
