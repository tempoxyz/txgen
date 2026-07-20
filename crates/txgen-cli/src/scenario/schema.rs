use super::value::{collect_variable_paths, eval_expression, RuntimeContext, RuntimeValue};
use alloy_dyn_abi::DynSolType;
use alloy_primitives::{Address, B256, U256};
use eyre::{bail, Result, WrapErr};
use serde::{Deserialize, Deserializer};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};
use txgen_core::expand_env_vars;

/// Versioned, chain-agnostic scenario document.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioSpec {
    /// Scenario schema version. Only version 1 is currently supported.
    pub version: u64,
    /// Named chain runtimes used by steps.
    #[serde(default)]
    pub chains: BTreeMap<String, ChainDef>,
    /// The workflow executed for each scenario instance.
    pub scenario: ScenarioDef,
}

/// Configuration for one named chain.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainDef {
    /// Network adapter name, for example `tempo` or `ethereum`.
    pub network: String,
    /// JSON-RPC endpoint for transaction submission and sender-scoped requests.
    pub rpc_url: String,
    /// Unauthenticated endpoint for chain, nonce, checkpoint, and log queries.
    /// Defaults to `rpc_url` when omitted.
    #[serde(default)]
    pub query_rpc_url: Option<String>,
    /// Optional request-scoped authentication for the submission endpoint.
    #[serde(default)]
    pub request_auth: Option<RequestAuthDef>,
    /// Effective chain ID, or `auto` to query it from the RPC endpoint.
    #[serde(default)]
    pub chain_id: ChainId,
    /// Existing txgen workload spec supplying accounts, artifacts, and templates.
    pub workload: PathBuf,
}

/// Request-scoped authentication for a scenario chain's submission endpoint.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestAuthDef {
    /// Select a header value from a JSON map keyed by logical transaction sender.
    pub sender_header: SenderHeaderAuthDef,
}

/// Sender-to-header map configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SenderHeaderAuthDef {
    /// HTTP header populated with the selected map value.
    pub name: String,
    /// JSON file mapping sender addresses to header values.
    pub map: PathBuf,
    /// Interval between checks for an atomically replaced map.
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
    pub reload_interval: Option<Duration>,
}

/// Chain ID selection in a scenario chain definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChainId {
    /// Query `eth_chainId` from the configured endpoint.
    #[default]
    Auto,
    /// Use and validate an explicit chain ID.
    Explicit(u64),
}

impl<'de> Deserialize<'de> for ChainId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Name(String),
            Number(u64),
        }

        match Repr::deserialize(deserializer)? {
            Repr::Name(name) if name == "auto" => Ok(Self::Auto),
            Repr::Name(other) => {
                Err(serde::de::Error::unknown_variant(&other, &["auto", "an integer chain ID"]))
            }
            Repr::Number(id) => Ok(Self::Explicit(id)),
        }
    }
}

/// Scenario workflow definition.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioDef {
    /// Human-readable scenario name included in reports.
    pub name: String,
    /// Default timeout inherited by steps without an explicit timeout.
    #[serde(
        default,
        alias = "default_timeout",
        deserialize_with = "deserialize_optional_duration"
    )]
    pub timeout: Option<Duration>,
    /// Values resolved once and retained for the lifetime of one instance.
    #[serde(default)]
    pub bindings: BTreeMap<String, BindingDef>,
    /// Ordered workflow steps.
    pub steps: Vec<StepDef>,
}

/// Scenario-local binding definition.
#[derive(Debug, Clone)]
pub enum BindingDef {
    /// Select an account and expose `<binding>.ref` and `<binding>.address`.
    Account(AccountBindingDef),
}

impl<'de> Deserialize<'de> for BindingDef {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mapping = serde_yaml::Mapping::deserialize(deserializer)?;
        if mapping.len() != 1 {
            return Err(serde::de::Error::custom(
                "scenario binding must contain exactly one binding type",
            ));
        }
        let (kind, value) = mapping.into_iter().next().expect("checked one binding type");
        match kind.as_str() {
            Some("account") => {
                serde_yaml::from_value(value).map(Self::Account).map_err(serde::de::Error::custom)
            }
            Some(other) => Err(serde::de::Error::unknown_variant(other, &["account"])),
            None => Err(serde::de::Error::custom("scenario binding type must be a string")),
        }
    }
}

/// Account binding configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountBindingDef {
    /// Account pool name in each workload that consumes this binding.
    pub pool: String,
    /// Account selection behavior.
    pub select: AccountSelection,
}

/// Account selection behavior for a scenario binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountSelection {
    /// Exclusively lease an account until the scenario instance finishes.
    Lease,
    /// Select an account randomly without exclusivity.
    Random,
    /// Select a fixed account index.
    Index(usize),
}

impl<'de> Deserialize<'de> for AccountSelection {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Name(String),
            Index { index: usize },
        }

        match Repr::deserialize(deserializer)? {
            Repr::Name(name) if name == "lease" => Ok(Self::Lease),
            Repr::Name(name) if name == "random" => Ok(Self::Random),
            Repr::Name(other) => {
                Err(serde::de::Error::unknown_variant(&other, &["lease", "random", "index"]))
            }
            Repr::Index { index } => Ok(Self::Index(index)),
        }
    }
}

/// One ordered scenario step with optional save and timeout metadata.
#[derive(Debug, Clone)]
pub struct StepDef {
    /// Step operation.
    pub action: StepAction,
    /// Immutable runtime root populated after this step completes.
    pub save: Option<String>,
    /// Per-step timeout overriding [`ScenarioDef::timeout`].
    pub timeout: Option<Duration>,
}

/// Initial scenario step operations.
#[derive(Debug, Clone)]
#[expect(clippy::large_enum_variant)]
pub enum StepAction {
    /// Run an explicitly configured local command and capture its output.
    Command(CommandStep),
    /// Capture the current canonical chain cursor.
    Checkpoint(CheckpointStep),
    /// Materialize, sign, and submit a workload template.
    Submit(SubmitStep),
    /// Wait for a transaction receipt.
    WaitReceipt(WaitReceiptStep),
    /// Backfill and poll for the first matching event log.
    WaitLog(WaitLogStep),
}

/// Run a local executable with arguments and environment values resolved at runtime.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandStep {
    /// Executable name or path. Bare names are resolved through the command runner's PATH.
    pub program: PathBuf,
    /// Runtime expressions rendered as individual process arguments.
    #[serde(default)]
    pub args: Vec<serde_yaml::Value>,
    /// Explicit child environment. Values are runtime expressions rendered as scalars.
    #[serde(default)]
    pub env: BTreeMap<String, serde_yaml::Value>,
    /// Optional working directory.
    #[serde(default)]
    pub cwd: Option<PathBuf>,
    /// Expected stdout representation.
    pub stdout: CommandStdout,
}

/// Supported command stdout representations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandStdout {
    /// Parse stdout as exactly one JSON value.
    Json,
}

/// Capture a chain cursor.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointStep {
    /// Named chain to inspect.
    pub chain: String,
}

/// Submit a named workload template.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitStep {
    /// Named destination chain.
    pub chain: String,
    /// Template key in the chain's workload spec.
    pub template: String,
    /// Deep-merged template overlay. Runtime expressions are resolved afterward.
    #[serde(default, rename = "with")]
    pub with_value: serde_yaml::Value,
    /// Optionally wait for the transaction's receipt as part of this step.
    #[serde(default, rename = "await")]
    pub await_mode: Option<SubmitAwait>,
}

/// Completion boundary for a submit step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmitAwait {
    /// Complete only after observing a successful receipt.
    Receipt,
}

/// Wait for a supplied transaction hash to receive a receipt.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitReceiptStep {
    /// Named chain on which the transaction was submitted.
    pub chain: String,
    /// Runtime expression resolving to the transaction hash.
    pub transaction_hash: serde_yaml::Value,
    /// Runtime expression resolving to the logical transaction sender.
    /// Required when request authentication is configured for the chain.
    #[serde(default)]
    pub sender: Option<serde_yaml::Value>,
    /// Accept a reverted receipt rather than failing the scenario step.
    #[serde(default)]
    pub allow_revert: bool,
    /// Polling interval override.
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
    pub poll_interval: Option<Duration>,
    /// Required canonical confirmation count when explicitly configured.
    #[serde(default)]
    pub confirmations: Option<u64>,
}

/// Wait for and decode the first canonical matching event log.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitLogStep {
    /// Named chain to query.
    pub chain: String,
    /// Starting block expression, commonly a saved checkpoint block number.
    #[serde(default)]
    pub from_block: Option<serde_yaml::Value>,
    /// Optional contract-address expression.
    #[serde(default)]
    pub address: Option<serde_yaml::Value>,
    /// Optional transaction-hash expression.
    #[serde(default)]
    pub transaction_hash: Option<serde_yaml::Value>,
    /// Logical transaction sender used when a transaction-hash-only wait must
    /// fetch receipts through an authenticated submission endpoint.
    #[serde(default)]
    pub sender: Option<serde_yaml::Value>,
    /// ABI artifact name in the selected chain's workload spec.
    pub abi: String,
    /// Event name or exact event signature.
    pub event: String,
    /// Decoded event argument equality filters.
    #[serde(default, rename = "where")]
    pub where_value: BTreeMap<String, serde_yaml::Value>,
    /// Polling interval override.
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
    pub poll_interval: Option<Duration>,
    /// Required canonical confirmation count when explicitly configured.
    #[serde(default)]
    pub confirmations: Option<u64>,
    /// Maximum inclusive block span requested from `eth_getLogs` at once.
    #[serde(default, alias = "block_range")]
    pub max_block_range: Option<u64>,
}

impl<'de> Deserialize<'de> for StepDef {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut mapping = serde_yaml::Mapping::deserialize(deserializer)?;
        let save = mapping
            .remove(serde_yaml::Value::String("save".to_string()))
            .map(serde_yaml::from_value)
            .transpose()
            .map_err(serde::de::Error::custom)?;
        let timeout = mapping
            .remove(serde_yaml::Value::String("timeout".to_string()))
            .map(parse_duration_value)
            .transpose()
            .map_err(serde::de::Error::custom)?;

        if mapping.len() != 1 {
            return Err(serde::de::Error::custom(
                "scenario step must contain exactly one of `command`, `checkpoint`, `submit`, `wait_receipt`, or `wait_log`",
            ));
        }

        let (kind, value) = mapping.into_iter().next().expect("checked one step action");
        let kind = kind
            .as_str()
            .ok_or_else(|| serde::de::Error::custom("scenario step action must be a string"))?;
        let action = match kind {
            "command" => serde_yaml::from_value(value)
                .map(StepAction::Command)
                .map_err(serde::de::Error::custom)?,
            "checkpoint" => serde_yaml::from_value(value)
                .map(StepAction::Checkpoint)
                .map_err(serde::de::Error::custom)?,
            "submit" => serde_yaml::from_value(value)
                .map(StepAction::Submit)
                .map_err(serde::de::Error::custom)?,
            "wait_receipt" => serde_yaml::from_value(value)
                .map(StepAction::WaitReceipt)
                .map_err(serde::de::Error::custom)?,
            "wait_log" => serde_yaml::from_value(value)
                .map(StepAction::WaitLog)
                .map_err(serde::de::Error::custom)?,
            other => {
                return Err(serde::de::Error::unknown_variant(
                    other,
                    &["command", "checkpoint", "submit", "wait_receipt", "wait_log"],
                ))
            }
        };

        Ok(Self { action, save, timeout })
    }
}

impl ScenarioSpec {
    /// Return whether any scenario step executes a local command.
    pub fn has_commands(&self) -> bool {
        self.scenario.steps.iter().any(|step| matches!(&step.action, StepAction::Command(_)))
    }

    /// Read, environment-expand, parse, validate, and path-resolve a scenario file.
    pub fn load(path: &Path) -> Result<Self> {
        let yaml = std::fs::read_to_string(path)
            .wrap_err_with(|| format!("failed to read scenario file: {}", path.display()))?;
        let mut spec = Self::parse(&yaml)?;
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        let command_base = if base.is_absolute() {
            base.to_path_buf()
        } else {
            std::env::current_dir()
                .wrap_err("failed to resolve the scenario command directory")?
                .join(base)
        };
        for chain in spec.chains.values_mut() {
            if chain.workload.is_relative() {
                chain.workload = base.join(&chain.workload);
            }
            if let Some(auth) = &mut chain.request_auth &&
                auth.sender_header.map.is_relative()
            {
                auth.sender_header.map = base.join(&auth.sender_header.map);
            }
        }
        for step in &mut spec.scenario.steps {
            let StepAction::Command(command) = &mut step.action else { continue };
            if command.cwd.as_ref().is_some_and(|cwd| cwd.is_relative()) {
                command.cwd = command.cwd.take().map(|cwd| command_base.join(cwd));
            }
            if command.program.is_relative() && is_path_like_program(&command.program) {
                command.program = command_base.join(&command.program);
            }
        }
        Ok(spec)
    }

    /// Environment-expand, parse, and statically validate scenario YAML.
    pub fn parse(yaml: &str) -> Result<Self> {
        let expanded =
            expand_env_vars(yaml).wrap_err("failed to expand scenario environment variables")?;
        let spec: Self = serde_yaml::from_str(&expanded).wrap_err("failed to parse scenario")?;
        spec.validate()?;
        Ok(spec)
    }

    /// Validate constraints and all statically knowable runtime references.
    pub fn validate(&self) -> Result<()> {
        if self.version != 1 {
            bail!("unsupported scenario version {}; expected version 1", self.version);
        }
        for (name, chain) in &self.chains {
            validate_name(name, "chain")?;
            if chain.network.trim().is_empty() {
                bail!("chain '{name}' has an empty network");
            }
            if chain.rpc_url.trim().is_empty() {
                bail!("chain '{name}' has an empty rpc_url");
            }
            if chain.query_rpc_url.as_ref().is_some_and(|url| url.trim().is_empty()) {
                bail!("chain '{name}' has an empty query_rpc_url");
            }
            if chain.workload.as_os_str().is_empty() {
                bail!("chain '{name}' has an empty workload path");
            }
            if matches!(chain.chain_id, ChainId::Explicit(0)) {
                bail!("chain '{name}' chain_id must be greater than zero");
            }
            if let Some(auth) = &chain.request_auth {
                if auth.sender_header.name.trim().is_empty() {
                    bail!("chain '{name}' request_auth sender_header has an empty name");
                }
                if auth.sender_header.map.as_os_str().is_empty() {
                    bail!("chain '{name}' request_auth sender_header has an empty map path");
                }
                validate_optional_duration(
                    auth.sender_header.reload_interval,
                    &format!("chain '{name}' request_auth sender_header"),
                    "reload_interval",
                )?;
            }
        }

        if self.scenario.name.trim().is_empty() {
            bail!("scenario name must not be empty");
        }
        if self.scenario.timeout == Some(Duration::ZERO) {
            bail!("scenario timeout must be greater than zero");
        }
        if self.scenario.steps.is_empty() {
            bail!("scenario '{}' must contain at least one step", self.scenario.name);
        }

        for (name, binding) in &self.scenario.bindings {
            validate_runtime_root(name, "binding")?;
            match binding {
                BindingDef::Account(account) if account.pool.trim().is_empty() => {
                    bail!("account binding '{name}' has an empty pool");
                }
                BindingDef::Account(_) => {}
            }
        }

        let saves = self.collect_saves()?;
        let mut available = BTreeMap::new();
        for name in self.scenario.bindings.keys() {
            available.insert(name.clone(), AvailableRoot::AccountBinding);
        }

        for (index, step) in self.scenario.steps.iter().enumerate() {
            self.validate_step(index, step, &saves, &available)?;
            if let Some(save) = &step.save {
                available.insert(save.clone(), AvailableRoot::Saved(step.saved_kind()));
            }
        }

        Ok(())
    }

    fn collect_saves(&self) -> Result<BTreeMap<String, usize>> {
        let mut saves = BTreeMap::new();
        for (index, step) in self.scenario.steps.iter().enumerate() {
            let Some(save) = &step.save else { continue };
            validate_runtime_root(save, "save")?;
            if self.scenario.bindings.contains_key(save) {
                bail!("step {} save '{save}' conflicts with a scenario binding", index + 1);
            }
            if let Some(previous) = saves.insert(save.clone(), index) {
                bail!("duplicate save name '{save}' at steps {} and {}", previous + 1, index + 1);
            }
        }
        Ok(saves)
    }

    pub(super) fn validate_abi_filter_expression_type(
        &self,
        step_index: usize,
        expression: &serde_yaml::Value,
        expected: &DynSolType,
        accepts_precomputed_hash: bool,
        filter_name: &str,
    ) -> Result<()> {
        let mut available = self
            .scenario
            .bindings
            .keys()
            .map(|name| (name.clone(), AvailableRoot::AccountBinding))
            .collect::<BTreeMap<_, _>>();
        for step in self.scenario.steps.iter().take(step_index) {
            if let Some(save) = &step.save {
                available.insert(save.clone(), AvailableRoot::Saved(step.saved_kind()));
            }
        }

        let Some(actual) = expression_static_type(expression, &available)? else {
            return Ok(());
        };
        let compatible = (accepts_precomputed_hash && actual == StaticValueType::Bytes32) ||
            static_type_can_coerce(actual, expected);
        if !compatible {
            bail!(
                "scenario step {} event filter '{filter_name}' expects ABI type '{expected}', but its runtime expression has type {actual:?}",
                step_index + 1
            );
        }
        Ok(())
    }

    fn validate_step(
        &self,
        index: usize,
        step: &StepDef,
        saves: &BTreeMap<String, usize>,
        available: &BTreeMap<String, AvailableRoot>,
    ) -> Result<()> {
        let label = format!("step {} ({})", index + 1, step.action.name());
        if step.timeout == Some(Duration::ZERO) {
            bail!("{label} timeout must be greater than zero");
        }

        if let Some(chain) = step.action.chain() &&
            !self.chains.contains_key(chain)
        {
            bail!("{label} references unknown chain '{chain}'");
        }

        match &step.action {
            StepAction::Command(command) => {
                if command.program.as_os_str().is_empty() {
                    bail!("{label} has an empty program");
                }
                if command.cwd.as_ref().is_some_and(|cwd| cwd.as_os_str().is_empty()) {
                    bail!("{label} has an empty working directory");
                }
                for (argument, value) in command.args.iter().enumerate() {
                    validate_static_command_scalar(
                        value,
                        &label,
                        &format!("argument {}", argument + 1),
                    )?;
                }
                for (name, value) in &command.env {
                    if name.trim().is_empty() {
                        bail!("{label} contains an empty environment variable name");
                    }
                    if name.contains('=') || name.contains('\0') {
                        bail!(
                            "{label} environment variable name '{name}' contains an invalid character"
                        );
                    }
                    validate_static_command_scalar(
                        value,
                        &label,
                        &format!("environment variable '{name}'"),
                    )?;
                }
            }
            StepAction::Checkpoint(_) => {}
            StepAction::Submit(submit) => {
                if submit.template.trim().is_empty() {
                    bail!("{label} has an empty template name");
                }
            }
            StepAction::WaitReceipt(wait) => {
                validate_optional_duration(wait.poll_interval, &label, "poll_interval")?;
                validate_expression_type(
                    &wait.transaction_hash,
                    StaticValueType::Bytes32,
                    available,
                    &label,
                    "transaction_hash",
                )?;
                if let Some(sender) = &wait.sender {
                    validate_expression_type(
                        sender,
                        StaticValueType::Address,
                        available,
                        &label,
                        "sender",
                    )?;
                } else if self.chains[wait.chain.as_str()].request_auth.is_some() {
                    bail!(
                        "{label} requires `sender` when chain '{}' uses request_auth",
                        wait.chain
                    );
                }
            }
            StepAction::WaitLog(wait) => {
                if wait.abi.trim().is_empty() {
                    bail!("{label} has an empty ABI name");
                }
                if wait.event.trim().is_empty() {
                    bail!("{label} has an empty event name");
                }
                if wait.from_block.is_none() && wait.transaction_hash.is_none() {
                    bail!("{label} requires `from_block` or `transaction_hash`");
                }
                validate_optional_nonzero(wait.max_block_range, &label, "max_block_range")?;
                validate_optional_duration(wait.poll_interval, &label, "poll_interval")?;
                if let Some(from_block) = &wait.from_block {
                    validate_expression_type(
                        from_block,
                        StaticValueType::Uint,
                        available,
                        &label,
                        "from_block",
                    )?;
                }
                if let Some(address) = &wait.address {
                    validate_expression_type(
                        address,
                        StaticValueType::Address,
                        available,
                        &label,
                        "address",
                    )?;
                }
                if let Some(transaction_hash) = &wait.transaction_hash {
                    validate_expression_type(
                        transaction_hash,
                        StaticValueType::Bytes32,
                        available,
                        &label,
                        "transaction_hash",
                    )?;
                }
                if let Some(sender) = &wait.sender {
                    validate_expression_type(
                        sender,
                        StaticValueType::Address,
                        available,
                        &label,
                        "sender",
                    )?;
                } else if wait.from_block.is_none() &&
                    self.chains[wait.chain.as_str()].request_auth.is_some()
                {
                    bail!(
                        "{label} requires `sender` for a transaction-hash-only wait when chain '{}' uses request_auth",
                        wait.chain
                    );
                }
                for name in wait.where_value.keys() {
                    if name.trim().is_empty() {
                        bail!("{label} contains an empty decoded argument filter name");
                    }
                }
            }
        }

        step.visit_expressions(|expression| {
            let paths = collect_variable_paths(expression)
                .map_err(|error| eyre::eyre!("invalid runtime expression in {label}: {error}"))?;
            for path in paths {
                validate_reference(&path, index, saves, available).map_err(|error| {
                    eyre::eyre!("invalid runtime reference in {label}: {error}")
                })?;
            }
            Ok(())
        })
    }
}

impl StepDef {
    fn saved_kind(&self) -> SavedKind {
        match &self.action {
            StepAction::Command(_) => SavedKind::Command,
            StepAction::Checkpoint(_) => SavedKind::Checkpoint,
            StepAction::Submit(step) => {
                SavedKind::Submit { receipt: step.await_mode == Some(SubmitAwait::Receipt) }
            }
            StepAction::WaitReceipt(_) => SavedKind::Receipt,
            StepAction::WaitLog(_) => SavedKind::Log,
        }
    }

    fn visit_expressions(
        &self,
        mut visit: impl FnMut(&serde_yaml::Value) -> Result<()>,
    ) -> Result<()> {
        match &self.action {
            StepAction::Command(step) => {
                for value in &step.args {
                    visit(value)?;
                }
                for value in step.env.values() {
                    visit(value)?;
                }
            }
            StepAction::Checkpoint(_) => {}
            StepAction::Submit(step) => visit(&step.with_value)?,
            StepAction::WaitReceipt(step) => {
                visit(&step.transaction_hash)?;
                if let Some(value) = &step.sender {
                    visit(value)?;
                }
            }
            StepAction::WaitLog(step) => {
                if let Some(value) = &step.from_block {
                    visit(value)?;
                }
                if let Some(value) = &step.address {
                    visit(value)?;
                }
                if let Some(value) = &step.transaction_hash {
                    visit(value)?;
                }
                if let Some(value) = &step.sender {
                    visit(value)?;
                }
                for value in step.where_value.values() {
                    visit(value)?;
                }
            }
        }
        Ok(())
    }
}

impl StepAction {
    /// Stable operation name used in diagnostics and reports.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Command(_) => "command",
            Self::Checkpoint(_) => "checkpoint",
            Self::Submit(_) => "submit",
            Self::WaitReceipt(_) => "wait_receipt",
            Self::WaitLog(_) => "wait_log",
        }
    }

    /// Named chain selected by this operation.
    pub fn chain(&self) -> Option<&str> {
        match self {
            Self::Command(_) => None,
            Self::Checkpoint(step) => Some(&step.chain),
            Self::Submit(step) => Some(&step.chain),
            Self::WaitReceipt(step) => Some(&step.chain),
            Self::WaitLog(step) => Some(&step.chain),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum AvailableRoot {
    AccountBinding,
    Saved(SavedKind),
}

#[derive(Debug, Clone, Copy)]
enum SavedKind {
    Command,
    Checkpoint,
    Submit { receipt: bool },
    Receipt,
    Log,
}

fn validate_reference(
    path: &str,
    current_step: usize,
    saves: &BTreeMap<String, usize>,
    available: &BTreeMap<String, AvailableRoot>,
) -> Result<()> {
    let (root, tail) = path.split_once('.').map_or((path, None), |(root, tail)| (root, Some(tail)));
    let Some(kind) = available.get(root) else {
        if let Some(save_step) = saves.get(root) &&
            *save_step >= current_step
        {
            bail!("forward reference '{path}' targets save '{root}' from step {}", save_step + 1);
        }
        bail!("unknown runtime root '{root}' in reference '{path}'");
    };

    match kind {
        AvailableRoot::AccountBinding => validate_account_path(root, tail),
        AvailableRoot::Saved(kind) => validate_saved_path(root, *kind, tail),
    }
}

fn validate_account_path(root: &str, tail: Option<&str>) -> Result<()> {
    let Some(tail) = tail else { return Ok(()) };
    if matches!(
        tail,
        "pool" | "index" | "address" | "ref" | "ref.pool" | "ref.select" | "ref.select.index"
    ) {
        return Ok(());
    }
    bail!("account binding '{root}' has no field '{tail}'");
}

fn validate_saved_path(root: &str, kind: SavedKind, tail: Option<&str>) -> Result<()> {
    let Some(tail) = tail else { return Ok(()) };
    let valid = match kind {
        SavedKind::Command => true,
        SavedKind::Checkpoint => {
            matches!(tail, "chain" | "block_number" | "block_hash" | "captured_at")
        }
        SavedKind::Submit { receipt } => {
            if (tail == "receipt" || tail.starts_with("receipt.")) && !receipt {
                bail!("saved submit result '{root}' has no receipt because the step does not await one");
            }
            matches!(
                tail,
                "chain" |
                    "template" |
                    "id" |
                    "sender" |
                    "tx_hash" |
                    "submitted_at" |
                    "acceptance_latency" |
                    "receipt" |
                    "receipt.chain" |
                    "receipt.transaction_hash" |
                    "receipt.tx_hash" |
                    "receipt.block_hash" |
                    "receipt.block_number" |
                    "receipt.status" |
                    "receipt.gas_used" |
                    "receipt.observed_at"
            )
        }
        SavedKind::Receipt => {
            matches!(
                tail,
                "chain" |
                    "transaction_hash" |
                    "tx_hash" |
                    "block_hash" |
                    "block_number" |
                    "status" |
                    "gas_used" |
                    "observed_at"
            )
        }
        SavedKind::Log => {
            tail == "args" ||
                tail.starts_with("args.") ||
                matches!(
                    tail,
                    "chain" |
                        "address" |
                        "contract_address" |
                        "transaction_hash" |
                        "tx_hash" |
                        "block_hash" |
                        "block_number" |
                        "log_index" |
                        "event" |
                        "event_name" |
                        "first_observed_at" |
                        "observed_at"
                )
        }
    };
    if valid {
        Ok(())
    } else {
        bail!("saved step result '{root}' has no field '{tail}'");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StaticValueType {
    Address,
    Bytes32,
    Uint,
    Bool,
    String,
}

fn static_type_can_coerce(actual: StaticValueType, expected: &DynSolType) -> bool {
    let candidates = match actual {
        StaticValueType::Address => vec![RuntimeValue::Address(Address::ZERO)],
        StaticValueType::Bytes32 => vec![RuntimeValue::Bytes32(B256::ZERO)],
        StaticValueType::Uint => vec![RuntimeValue::Uint(U256::ZERO)],
        StaticValueType::Bool => vec![RuntimeValue::Bool(false)],
        // A saved string's contents are not generally statically known. Cover
        // representative textual scalar encodings and reject only ABI shapes
        // that none of them can inhabit.
        StaticValueType::String => vec![
            RuntimeValue::String("value".to_string()),
            RuntimeValue::String("0".to_string()),
            RuntimeValue::String("true".to_string()),
            RuntimeValue::String(Address::ZERO.to_string()),
            RuntimeValue::String(B256::ZERO.to_string()),
        ],
    };
    candidates.iter().any(|value| value.coerce_dyn_sol(expected).is_ok())
}

fn validate_expression_type(
    expression: &serde_yaml::Value,
    expected: StaticValueType,
    available: &BTreeMap<String, AvailableRoot>,
    label: &str,
    field: &str,
) -> Result<()> {
    let Some(actual) = expression_static_type(expression, available)? else {
        return Ok(());
    };
    if actual != expected {
        bail!(
            "{label} {field} expects {expected:?}, but its runtime reference has type {actual:?}"
        );
    }
    Ok(())
}

fn expression_static_type(
    expression: &serde_yaml::Value,
    available: &BTreeMap<String, AvailableRoot>,
) -> Result<Option<StaticValueType>> {
    let serde_yaml::Value::Mapping(mapping) = expression else {
        return Ok(match expression {
            serde_yaml::Value::Bool(_) => Some(StaticValueType::Bool),
            serde_yaml::Value::Number(_) => Some(StaticValueType::Uint),
            _ => None,
        });
    };
    if mapping.len() != 1 {
        return Ok(None);
    }

    let key = |name: &str| serde_yaml::Value::String(name.to_string());
    if let Some(path) = mapping.get(key("var")).and_then(serde_yaml::Value::as_str) {
        return Ok(static_reference_type(path, available));
    }
    if mapping.contains_key(key("keccak256")) || mapping.contains_key(key("keccak256_packed")) {
        return Ok(Some(StaticValueType::Bytes32));
    }
    Ok(None)
}

fn static_reference_type(
    path: &str,
    available: &BTreeMap<String, AvailableRoot>,
) -> Option<StaticValueType> {
    let (root, tail) = path.split_once('.').map_or((path, ""), |(root, tail)| (root, tail));
    match available.get(root)? {
        AvailableRoot::AccountBinding => match tail {
            "address" => Some(StaticValueType::Address),
            "index" | "ref.select.index" => Some(StaticValueType::Uint),
            "pool" | "ref.pool" => Some(StaticValueType::String),
            _ => None,
        },
        AvailableRoot::Saved(SavedKind::Command) => None,
        AvailableRoot::Saved(SavedKind::Checkpoint) => match tail {
            "chain" => Some(StaticValueType::String),
            "block_number" | "captured_at" => Some(StaticValueType::Uint),
            "block_hash" => Some(StaticValueType::Bytes32),
            _ => None,
        },
        AvailableRoot::Saved(SavedKind::Submit { .. }) => match tail {
            "chain" | "template" | "id" | "receipt.chain" => Some(StaticValueType::String),
            "sender" => Some(StaticValueType::Address),
            "tx_hash" | "receipt.transaction_hash" | "receipt.tx_hash" | "receipt.block_hash" => {
                Some(StaticValueType::Bytes32)
            }
            "submitted_at" |
            "acceptance_latency" |
            "receipt.block_number" |
            "receipt.gas_used" |
            "receipt.observed_at" => Some(StaticValueType::Uint),
            "receipt.status" => Some(StaticValueType::Bool),
            _ => None,
        },
        AvailableRoot::Saved(SavedKind::Receipt) => match tail {
            "chain" => Some(StaticValueType::String),
            "transaction_hash" | "tx_hash" | "block_hash" => Some(StaticValueType::Bytes32),
            "block_number" | "gas_used" | "observed_at" => Some(StaticValueType::Uint),
            "status" => Some(StaticValueType::Bool),
            _ => None,
        },
        AvailableRoot::Saved(SavedKind::Log) => match tail {
            "chain" | "event" | "event_name" => Some(StaticValueType::String),
            "address" | "contract_address" => Some(StaticValueType::Address),
            "transaction_hash" | "tx_hash" | "block_hash" => Some(StaticValueType::Bytes32),
            "block_number" | "log_index" | "first_observed_at" | "observed_at" => {
                Some(StaticValueType::Uint)
            }
            _ => None,
        },
    }
}

fn validate_optional_nonzero(value: Option<u64>, label: &str, field: &str) -> Result<()> {
    if value == Some(0) {
        bail!("{label} {field} must be greater than zero when configured");
    }
    Ok(())
}

fn validate_optional_duration(value: Option<Duration>, label: &str, field: &str) -> Result<()> {
    if value == Some(Duration::ZERO) {
        bail!("{label} {field} must be greater than zero");
    }
    Ok(())
}

fn validate_static_command_scalar(
    expression: &serde_yaml::Value,
    label: &str,
    field: &str,
) -> Result<()> {
    if collect_variable_paths(expression)?.is_empty() {
        eval_expression(expression, &RuntimeContext::empty())
            .and_then(|value| value.to_process_arg())
            .wrap_err_with(|| format!("{label} {field} must resolve to a scalar value"))?;
    }
    Ok(())
}

pub(super) fn is_path_like_program(program: &Path) -> bool {
    if program.is_absolute() {
        return true;
    }
    if !matches!(program.components().next(), Some(std::path::Component::Normal(_))) {
        return !program.as_os_str().is_empty();
    }
    let program = program.to_string_lossy();
    program.contains(std::path::MAIN_SEPARATOR) || (cfg!(windows) && program.contains('/'))
}

fn validate_name(name: &str, context: &str) -> Result<()> {
    if name.trim().is_empty() {
        bail!("{context} name must not be empty");
    }
    Ok(())
}

fn validate_runtime_root(name: &str, context: &str) -> Result<()> {
    validate_name(name, context)?;
    if name.contains('.') {
        bail!("{context} name '{name}' must not contain '.'");
    }
    Ok(())
}

fn deserialize_optional_duration<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?
        .map(|value| humantime::parse_duration(&value).map_err(serde::de::Error::custom))
        .transpose()
}

fn parse_duration_value(value: serde_yaml::Value) -> Result<Duration> {
    let text: String = serde_yaml::from_value(value).wrap_err("duration must be a string")?;
    humantime::parse_duration(&text).wrap_err_with(|| format!("invalid duration '{text}'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMMAND_ONLY: &str = r#"
version: 1
scenario:
  name: encrypt
  bindings:
    user:
      account:
        pool: users
        select: lease
  steps:
    - command:
        program: ./bin/encrypt-deposit
        args:
          - --recipient
          - { var: user.address }
        env:
          RECIPIENT: { var: user.address }
        cwd: ./helpers
        stdout: json
      save: encrypted
      timeout: 10s
    - command:
        program: consume-output
        args:
          - { var: encrypted.payload.ciphertext }
        stdout: json
"#;

    const BASE: &str = r#"
version: 1
chains:
  l1:
    network: tempo
    rpc_url: http://l1.invalid
    chain_id: auto
    workload: ./l1.yml
  zone:
    network: tempo
    rpc_url: http://zone.invalid
    chain_id: 1337
    workload: ./zone.yml
scenario:
  name: roundtrip
  timeout: 5m
  bindings:
    user:
      account:
        pool: users
        select: lease
  steps:
    - checkpoint:
        chain: zone
      save: zone_before
    - submit:
        chain: l1
        template: deposit
        with:
          from: { var: user.ref }
        await: receipt
      save: deposit
      timeout: 20s
    - wait_receipt:
        chain: l1
        transaction_hash: { var: deposit.tx_hash }
        poll_interval: 100ms
        confirmations: 1
      save: deposit_receipt
    - wait_log:
        chain: zone
        from_block: { var: zone_before.block_number }
        address: "0x0000000000000000000000000000000000000001"
        abi: Inbox
        event: Processed(bytes32)
        where:
          depositHash: { var: deposit.tx_hash }
        poll_interval: 250ms
        confirmations: 2
        max_block_range: 500
      save: processed
"#;

    #[test]
    fn parses_representative_scenario() {
        let spec = ScenarioSpec::parse(BASE).unwrap();
        assert_eq!(spec.version, 1);
        assert_eq!(spec.chains["l1"].chain_id, ChainId::Auto);
        assert_eq!(spec.chains["zone"].chain_id, ChainId::Explicit(1337));
        assert_eq!(spec.scenario.timeout, Some(Duration::from_secs(300)));
        assert_eq!(spec.scenario.steps.len(), 4);
        assert_eq!(spec.scenario.steps[1].timeout, Some(Duration::from_secs(20)));
        assert!(matches!(
            spec.scenario.bindings["user"],
            BindingDef::Account(AccountBindingDef { select: AccountSelection::Lease, .. })
        ));
    }

    #[test]
    fn parses_chainless_commands_and_dynamic_saved_paths() {
        let spec = ScenarioSpec::parse(COMMAND_ONLY).unwrap();
        assert!(spec.chains.is_empty());
        assert!(spec.has_commands());
        assert_eq!(spec.scenario.steps[0].action.chain(), None);

        let StepAction::Command(command) = &spec.scenario.steps[0].action else {
            panic!("expected command step");
        };
        assert_eq!(command.program, PathBuf::from("./bin/encrypt-deposit"));
        assert_eq!(command.args.len(), 2);
        assert_eq!(command.env.len(), 1);
        assert_eq!(command.cwd.as_deref(), Some(Path::new("./helpers")));
        assert_eq!(command.stdout, CommandStdout::Json);
    }

    #[test]
    fn command_arguments_and_environment_validate_references() {
        let forward = COMMAND_ONLY.replace("{ var: user.address }", "{ var: later.output }") +
            r#"
    - command:
        program: later
        stdout: json
      save: later
"#;
        let error = ScenarioSpec::parse(&forward).unwrap_err().to_string();
        assert!(error.contains("forward reference"), "unexpected error: {error}");

        let unknown = COMMAND_ONLY.replacen(
            "RECIPIENT: { var: user.address }",
            "RECIPIENT: { var: missing.value }",
            1,
        );
        let error = ScenarioSpec::parse(&unknown).unwrap_err().to_string();
        assert!(error.contains("unknown runtime root 'missing'"), "unexpected error: {error}");
    }

    #[test]
    fn command_output_has_unknown_static_type() {
        let yaml = r#"
version: 1
chains:
  l1: { network: ethereum, rpc_url: http://rpc.invalid, workload: ./workload.yml }
scenario:
  name: dynamic-command-output
  steps:
    - command:
        program: derive-hash
        stdout: json
      save: derived
    - wait_receipt:
        chain: l1
        transaction_hash: { var: derived.nested.tx_hash }
"#;
        ScenarioSpec::parse(yaml).unwrap();
    }

    #[test]
    fn rejects_invalid_command_configuration() {
        let empty_program =
            COMMAND_ONLY.replacen("program: ./bin/encrypt-deposit", "program: ''", 1);
        assert!(ScenarioSpec::parse(&empty_program)
            .unwrap_err()
            .to_string()
            .contains("empty program"));

        let empty_cwd = COMMAND_ONLY.replacen("cwd: ./helpers", "cwd: ''", 1);
        assert!(ScenarioSpec::parse(&empty_cwd)
            .unwrap_err()
            .to_string()
            .contains("empty working directory"));

        let composite_argument =
            COMMAND_ONLY.replacen("          - --recipient", "          - [not, scalar]", 1);
        assert!(ScenarioSpec::parse(&composite_argument)
            .unwrap_err()
            .to_string()
            .contains("must resolve to a scalar value"));

        let empty_env = COMMAND_ONLY.replacen(
            "RECIPIENT: { var: user.address }",
            "'': { var: user.address }",
            1,
        );
        assert!(ScenarioSpec::parse(&empty_env)
            .unwrap_err()
            .to_string()
            .contains("empty environment variable name"));

        let invalid_env = COMMAND_ONLY.replacen(
            "RECIPIENT: { var: user.address }",
            "'BAD=NAME': { var: user.address }",
            1,
        );
        assert!(ScenarioSpec::parse(&invalid_env)
            .unwrap_err()
            .to_string()
            .contains("invalid character"));

        let unknown_field =
            COMMAND_ONLY.replacen("stdout: json", "stdout: json\n        shell: true", 1);
        assert!(ScenarioSpec::parse(&unknown_field).is_err());
    }

    #[test]
    fn parses_separate_query_rpc_and_sender_authentication() {
        let yaml = BASE.replace(
            "    rpc_url: http://l1.invalid",
            r#"    rpc_url: http://l1-submit.invalid
    query_rpc_url: http://l1-query.invalid
    request_auth:
      sender_header:
        name: X-Authorization-Token
        map: ./sender-auth.json
        reload_interval: 250ms"#,
        );
        let yaml = yaml.replace(
            "        transaction_hash: { var: deposit.tx_hash }",
            "        transaction_hash: { var: deposit.tx_hash }\n        sender: { var: deposit.sender }",
        );

        let spec = ScenarioSpec::parse(&yaml).unwrap();
        let chain = &spec.chains["l1"];
        assert_eq!(chain.rpc_url, "http://l1-submit.invalid");
        assert_eq!(chain.query_rpc_url.as_deref(), Some("http://l1-query.invalid"));
        let sender_header = &chain.request_auth.as_ref().unwrap().sender_header;
        assert_eq!(sender_header.name, "X-Authorization-Token");
        assert_eq!(sender_header.map, PathBuf::from("./sender-auth.json"));
        assert_eq!(sender_header.reload_interval, Some(Duration::from_millis(250)));
    }

    #[test]
    fn authenticated_receipt_wait_requires_sender() {
        let yaml = BASE.replace(
            "    rpc_url: http://l1.invalid",
            r#"    rpc_url: http://l1.invalid
    request_auth:
      sender_header:
        name: X-Authorization-Token
        map: ./sender-auth.json"#,
        );
        let error = ScenarioSpec::parse(&yaml).unwrap_err().to_string();
        assert!(error.contains("requires `sender`"), "unexpected error: {error}");
    }

    #[test]
    fn parses_random_and_index_account_selection() {
        let random: AccountSelection = serde_yaml::from_str("random").unwrap();
        let indexed: AccountSelection = serde_yaml::from_str("{ index: 7 }").unwrap();
        assert_eq!(random, AccountSelection::Random);
        assert_eq!(indexed, AccountSelection::Index(7));
        assert!(serde_yaml::from_str::<AccountSelection>("shared").is_err());
    }

    #[test]
    fn rejects_unsupported_version() {
        let yaml = BASE.replacen("version: 1", "version: 2", 1);
        let error = ScenarioSpec::parse(&yaml).unwrap_err().to_string();
        assert!(error.contains("unsupported scenario version"));
    }

    #[test]
    fn rejects_unknown_chain_reference() {
        let yaml = BASE.replacen("chain: zone", "chain: missing", 1);
        let error = ScenarioSpec::parse(&yaml).unwrap_err().to_string();
        assert!(error.contains("unknown chain 'missing'"));
    }

    #[test]
    fn rejects_duplicate_save_names() {
        let yaml = BASE.replacen("save: deposit_receipt", "save: deposit", 1);
        let error = ScenarioSpec::parse(&yaml).unwrap_err().to_string();
        assert!(error.contains("duplicate save name 'deposit'"));
    }

    #[test]
    fn rejects_save_binding_collision() {
        let yaml = BASE.replacen("save: zone_before", "save: user", 1);
        let error = ScenarioSpec::parse(&yaml).unwrap_err().to_string();
        assert!(error.contains("conflicts with a scenario binding"));
    }

    #[test]
    fn rejects_forward_and_self_references() {
        let forward =
            BASE.replacen("from: { var: user.ref }", "from: { var: processed.args.sender }", 1);
        let error = ScenarioSpec::parse(&forward).unwrap_err().to_string();
        assert!(error.contains("forward reference"));

        let self_ref =
            BASE.replacen("from: { var: user.ref }", "from: { var: deposit.tx_hash }", 1);
        let error = ScenarioSpec::parse(&self_ref).unwrap_err().to_string();
        assert!(error.contains("forward reference"));
    }

    #[test]
    fn rejects_unknown_runtime_root_and_field() {
        let unknown = BASE.replacen("{ var: user.ref }", "{ var: stranger.ref }", 1);
        let error = ScenarioSpec::parse(&unknown).unwrap_err().to_string();
        assert!(error.contains("unknown runtime root 'stranger'"));

        let field = BASE.replacen("{ var: user.ref }", "{ var: user.private_key }", 1);
        let error = ScenarioSpec::parse(&field).unwrap_err().to_string();
        assert!(error.contains("has no field 'private_key'"));
    }

    #[test]
    fn rejects_statically_wrong_saved_value_type() {
        let yaml = BASE.replace(
            "transaction_hash: { var: deposit.tx_hash }",
            "transaction_hash: { var: zone_before.block_number }",
        );
        let error = ScenarioSpec::parse(&yaml).unwrap_err().to_string();
        assert!(error.contains("expects Bytes32"), "unexpected error: {error}");
    }

    #[test]
    fn rejects_statically_wrong_saved_value_type_for_abi_filter() {
        let spec = ScenarioSpec::parse(BASE).unwrap();
        let block_number: serde_yaml::Value =
            serde_yaml::from_str("{ var: zone_before.block_number }").unwrap();
        let error = spec
            .validate_abi_filter_expression_type(
                3,
                &block_number,
                &DynSolType::Address,
                false,
                "depositHash",
            )
            .unwrap_err()
            .to_string();
        assert!(error.contains("expects ABI type 'address'"), "unexpected error: {error}");
        assert!(error.contains("Uint"), "unexpected error: {error}");

        let tx_hash: serde_yaml::Value = serde_yaml::from_str("{ var: deposit.tx_hash }").unwrap();
        assert!(spec
            .validate_abi_filter_expression_type(
                3,
                &tx_hash,
                &DynSolType::FixedBytes(4),
                false,
                "tag",
            )
            .is_err());

        let status: serde_yaml::Value =
            serde_yaml::from_str("{ var: deposit.receipt.status }").unwrap();
        assert!(
            spec.validate_abi_filter_expression_type(
                3,
                &status,
                &DynSolType::Int(256),
                false,
                "amount",
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_receipt_reference_when_submit_does_not_await_receipt() {
        let yaml = BASE.replace("        await: receipt\n", "").replace(
            "transaction_hash: { var: deposit.tx_hash }",
            "transaction_hash: { var: deposit.receipt.tx_hash }",
        );
        let error = ScenarioSpec::parse(&yaml).unwrap_err().to_string();
        assert!(error.contains("does not await one"), "unexpected error: {error}");

        let yaml = BASE.replace("        await: receipt\n", "").replace(
            "transaction_hash: { var: deposit.tx_hash }",
            "transaction_hash: { keccak256: { var: deposit.receipt } }",
        );
        let error = ScenarioSpec::parse(&yaml).unwrap_err().to_string();
        assert!(error.contains("does not await one"), "unexpected error: {error}");
    }

    #[test]
    fn accepts_prior_log_argument_reference() {
        let yaml = BASE.replace(
            "depositHash: { var: deposit.tx_hash }",
            "depositHash: { var: processed.args.depositHash }",
        );
        // `processed` is the current step's save, so this remains a self-reference.
        let error = ScenarioSpec::parse(&yaml).unwrap_err().to_string();
        assert!(error.contains("forward reference"));

        let yaml = BASE.replace(
            "transaction_hash: { var: deposit.tx_hash }",
            "transaction_hash: { var: deposit.receipt.transaction_hash }",
        );
        ScenarioSpec::parse(&yaml).unwrap();
    }

    #[test]
    fn rejects_zero_timing_and_range_values() {
        for (needle, replacement, expected) in [
            ("timeout: 5m", "timeout: 0s", "scenario timeout"),
            ("timeout: 20s", "timeout: 0s", "step 2"),
            ("poll_interval: 100ms", "poll_interval: 0s", "poll_interval"),
            ("max_block_range: 500", "max_block_range: 0", "max_block_range"),
        ] {
            let yaml = BASE.replacen(needle, replacement, 1);
            let error = ScenarioSpec::parse(&yaml).unwrap_err().to_string();
            assert!(error.contains(expected), "unexpected error: {error}");
        }
    }

    #[test]
    fn accepts_zero_confirmations() {
        let yaml = BASE.replace("confirmations: 1", "confirmations: 0");
        ScenarioSpec::parse(&yaml).unwrap();
    }

    #[test]
    fn rejects_unbounded_wait_log() {
        let yaml = r#"
version: 1
chains:
  l1: { network: ethereum, rpc_url: http://rpc.invalid, workload: ./workload.yml }
scenario:
  name: invalid
  steps:
    - wait_log:
        chain: l1
        abi: Token
        event: Transfer
"#;
        let error = ScenarioSpec::parse(yaml).unwrap_err().to_string();
        assert!(error.contains("requires `from_block` or `transaction_hash`"));
    }

    #[test]
    fn rejects_multiple_or_unknown_step_actions() {
        let multiple = BASE.replacen(
            "    - checkpoint:\n        chain: zone",
            "    - checkpoint:\n        chain: zone\n      submit:\n        chain: l1\n        template: x",
            1,
        );
        assert!(ScenarioSpec::parse(&multiple).is_err());

        let unknown = BASE.replacen("checkpoint:", "wait_call:", 1);
        assert!(ScenarioSpec::parse(&unknown).is_err());
    }

    #[test]
    fn variable_transforms_participate_in_static_validation() {
        let yaml = BASE.replacen(
            "depositHash: { var: deposit.tx_hash }",
            "depositHash:\n            keccak256_packed:\n              - { var: user.address }\n              - { var: future.tx_hash }",
            1,
        );
        let error = ScenarioSpec::parse(&yaml).unwrap_err().to_string();
        assert!(error.contains("unknown runtime root 'future'"));
    }

    #[test]
    fn duration_must_use_humantime_string() {
        let yaml = BASE.replacen("timeout: 5m", "timeout: 30", 1);
        assert!(ScenarioSpec::parse(&yaml).is_err());
    }

    #[test]
    fn load_resolves_workloads_relative_to_scenario_file() {
        let unique = format!(
            "txgen-scenario-schema-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        );
        let dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("scenario.yml");
        let yaml = BASE.replace(
            "    rpc_url: http://zone.invalid",
            r#"    rpc_url: http://zone.invalid
    request_auth:
      sender_header:
        name: X-Authorization-Token
        map: ./zone-auth.json"#,
        );
        std::fs::write(&path, yaml).unwrap();

        let spec = ScenarioSpec::load(&path).unwrap();
        assert_eq!(spec.chains["l1"].workload, dir.join("./l1.yml"));
        assert_eq!(spec.chains["zone"].workload, dir.join("./zone.yml"));
        assert_eq!(
            spec.chains["zone"].request_auth.as_ref().unwrap().sender_header.map,
            dir.join("./zone-auth.json")
        );

        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }

    #[test]
    fn load_resolves_command_paths_relative_to_scenario_file() {
        let unique = format!(
            "txgen-command-schema-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        );
        let dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("scenario.yml");
        std::fs::write(&path, COMMAND_ONLY).unwrap();

        let spec = ScenarioSpec::load(&path).unwrap();
        let StepAction::Command(path_command) = &spec.scenario.steps[0].action else {
            panic!("expected command step");
        };
        assert_eq!(path_command.program, dir.join("./bin/encrypt-deposit"));
        assert_eq!(path_command.cwd.as_deref(), Some(dir.join("./helpers").as_path()));

        let StepAction::Command(bare_command) = &spec.scenario.steps[1].action else {
            panic!("expected command step");
        };
        assert_eq!(bare_command.program, PathBuf::from("consume-output"));

        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }
}
