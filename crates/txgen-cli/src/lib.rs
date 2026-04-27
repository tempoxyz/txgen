use alloy_consensus::{BlockHeader, Sealed, SignableTransaction, Signed};
use alloy_eips::{eip2718::Encodable2718, BlockNumberOrTag};
use alloy_network::{Network, TransactionBuilder, TxSignerSync};
use alloy_primitives::{keccak256, Address, Bytes, U256};
use alloy_provider::{ext::DebugApi, Provider};
use alloy_rlp::Decodable;
use clap::{Args, Parser, Subcommand};
use eyre::{bail, Result, WrapErr};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::{io::Write, path::PathBuf};
use tokio::sync::mpsc;
use txgen_core::{
    AccountManager, ArtifactManager, BuildContext, GeneratedTx, NdjsonWriter, NonceKeyBinding,
    NonceTracker, SequenceBinding, WorkloadSpec,
};

// ---------------------------------------------------------------------------
// Public CLI arg structs
// ---------------------------------------------------------------------------

#[derive(Args)]
pub struct GenerateArgs {
    /// Workload spec file (YAML)
    #[arg(short, long)]
    pub spec: PathBuf,

    /// Number of transactions to generate
    #[arg(short = 'n', long)]
    pub count: u64,

    /// Output file (default: stdout)
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// RPC endpoint URL (to fetch current nonces from chain)
    #[arg(long)]
    pub rpc: Option<String>,

    /// Rate limit for RPC requests per second (0 = unbounded)
    #[arg(long, default_value = "0")]
    pub rpc_rps: u64,

    /// RNG seed for reproducibility
    #[arg(long)]
    pub seed: Option<u64>,
}

#[derive(Args)]
pub struct AddressesArgs {
    /// Workload spec file (YAML)
    #[arg(short, long)]
    pub spec: PathBuf,

    /// Output format: plain (one per line), json, or shell (for xargs)
    #[arg(short, long, default_value = "plain")]
    pub format: String,
}

#[derive(Args)]
pub struct ExtractArgs {
    /// RPC endpoint URL (archive node with debug_getRawBlock)
    #[arg(long)]
    pub rpc: String,

    /// First block number to fetch (inclusive)
    #[arg(long)]
    pub from: u64,

    /// Last block number to fetch (inclusive)
    #[arg(long)]
    pub to: u64,

    /// Output file (default: stdout)
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Number of blocks to prefetch ahead
    #[arg(long, default_value = "20")]
    pub buffer_size: usize,
}

// ---------------------------------------------------------------------------
// GenerateContext — bundles common setup for generation
// ---------------------------------------------------------------------------

pub struct GenerateContext {
    spec: WorkloadSpec,
    accounts: AccountManager,
    artifacts: ArtifactManager,
    nonces: NonceTracker,
    rng: StdRng,
}

impl GenerateContext {
    pub fn from_args(args: &GenerateArgs) -> Result<Self> {
        let spec = WorkloadSpec::load(&args.spec)
            .wrap_err_with(|| format!("failed to load spec: {}", args.spec.display()))?;
        let base_path = args.spec.parent().unwrap_or_else(|| std::path::Path::new("."));
        let accounts = AccountManager::from_spec(&spec.accounts)?;
        let artifacts = ArtifactManager::load(&spec.artifacts, base_path)?;
        let nonces = NonceTracker::new();
        let rng = match args.seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::from_os_rng(),
        };
        Ok(Self { spec, accounts, artifacts, nonces, rng })
    }

    pub fn spec(&self) -> &WorkloadSpec {
        &self.spec
    }

    pub fn accounts(&self) -> &AccountManager {
        &self.accounts
    }

    pub fn nonces_mut(&mut self) -> &mut NonceTracker {
        &mut self.nonces
    }

    /// Borrow accounts and nonces simultaneously for prefetching.
    pub fn accounts_and_nonces(&mut self) -> (&AccountManager, &mut NonceTracker) {
        (&self.accounts, &mut self.nonces)
    }

    /// Borrow spec, accounts, and nonces simultaneously for prefetching.
    pub fn prefetch_state(&mut self) -> (&WorkloadSpec, &AccountManager, &mut NonceTracker) {
        (&self.spec, &self.accounts, &mut self.nonces)
    }
}

// ---------------------------------------------------------------------------
// NetworkAdapter trait — implemented by per-network binaries
// ---------------------------------------------------------------------------

/// Output from [`NetworkAdapter::into_request`].
pub struct TxRequest<R> {
    /// The network-specific transaction request.
    pub request: R,
    /// Signer pool name.
    pub signer_pool: String,
    /// Signer index within the pool.
    pub signer_index: usize,
    /// Scheduling key (e.g. sender address or hash of sender+nonce_key).
    pub key: [u8; 20],
}

/// Trait for network-specific transaction generation.
///
/// Each network (Ethereum, Tempo, etc.) implements this trait to map
/// templates into network-specific transaction requests. The generic
/// generation loop handles building, signing, and encoding.
pub trait NetworkAdapter: Send + Sync {
    /// The template type deserialized from YAML.
    type Template: serde::de::DeserializeOwned + Send;

    /// The alloy [`Network`] whose types are used.
    type Network: Network;

    /// Map a template to a network-specific transaction request.
    fn build_request(
        &self,
        template: Self::Template,
        ctx: &mut BuildContext<'_>,
    ) -> Result<TxRequest<<Self::Network as Network>::TransactionRequest>>;

    /// Prefetch nonces from the chain before generation.
    ///
    /// Called when `--rpc` is provided. Default is no-op.
    fn prefetch_nonces<'a>(
        &'a self,
        _ctx: &'a mut GenerateContext,
        _rpc: &'a str,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
        async { Ok(()) }
    }
}

// ---------------------------------------------------------------------------
// Private CLI plumbing
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "txgen", about = "Chain-agnostic transaction generator")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Generate transactions from a workload spec
    Generate(GenerateArgs),
    /// List all addresses from a workload spec (for funding)
    Addresses(AddressesArgs),
    /// Extract raw RLP blocks from an archive RPC as NDJSON
    Extract(ExtractArgs),
}

// ---------------------------------------------------------------------------
// Public entrypoint
// ---------------------------------------------------------------------------

pub async fn run<A: NetworkAdapter>(adapter: A) -> Result<()>
where
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718 + Decodable,
    <A::Network as Network>::Header: Decodable,
{
    let cli = Cli::parse();
    match cli.command {
        Command::Generate(args) => run_generate(adapter, args).await,
        Command::Addresses(args) => run_addresses(args),
        Command::Extract(args) => run_extract::<A::Network>(args).await,
    }
}

async fn run_generate<A: NetworkAdapter>(adapter: A, args: GenerateArgs) -> Result<()>
where
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
{
    let count = args.count;
    let output = args.output.clone();
    let rpc = args.rpc.clone();
    let mut ctx = GenerateContext::from_args(&args)?;

    if let Some(ref rpc) = rpc {
        adapter.prefetch_nonces(&mut ctx, rpc).await?;
    }

    generate_loop(&adapter, &mut ctx, count, output)
}

// ---------------------------------------------------------------------------
// Public helpers — used by per-network generate implementations
// ---------------------------------------------------------------------------

/// Deserialize a named template from the spec.
pub fn load_template<T: serde::de::DeserializeOwned>(spec: &WorkloadSpec, name: &str) -> Result<T> {
    let value =
        spec.templates.get(name).ok_or_else(|| eyre::eyre!("template '{}' not found", name))?;
    serde_yaml::from_value(value.clone())
        .wrap_err_with(|| format!("failed to parse template '{}'", name))
}

/// Fetch protocol nonces (nonce_key=0) for all accounts from an EVM RPC.
///
/// Uses `eth_getTransactionCount` to fetch the current nonce for each
/// account address and stores it in the tracker with the sender address
/// as the scheduling key.
pub async fn fetch_protocol_nonces(
    accounts: &AccountManager,
    nonces: &mut NonceTracker,
    rpc_url: &str,
) -> Result<()> {
    let provider =
        alloy_provider::ProviderBuilder::<_, _, alloy_provider::network::Ethereum>::new()
            .connect_http(rpc_url.parse().wrap_err("invalid RPC URL")?);

    for (pool_name, addresses) in accounts.all_addresses() {
        let total = addresses.len();
        for (idx, address) in addresses.iter().enumerate() {
            eprintln!("fetching nonce for {}[{}/{}] ({})...", pool_name, idx + 1, total, address);

            let nonce = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                Provider::get_transaction_count(&provider, *address),
            )
            .await
            .wrap_err_with(|| format!("timeout fetching nonce for {}[{}]", pool_name, idx))?
            .wrap_err_with(|| {
                format!("failed to fetch nonce for {}[{}] ({})", pool_name, idx, address)
            })?;

            let scheduling_key = address.0 .0;
            nonces.reset(scheduling_key, nonce);

            eprintln!(
                "fetched nonce for {}[{}/{}] ({}): {}",
                pool_name,
                idx + 1,
                total,
                address,
                nonce
            );
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

fn generate_loop<A: NetworkAdapter>(
    adapter: &A,
    ctx: &mut GenerateContext,
    count: u64,
    output: Option<PathBuf>,
) -> Result<()>
where
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
{
    if ctx.spec.total_weight() == 0 {
        bail!("no workload entries in mix (total weight is 0)");
    }

    let mut build_ctx = BuildContext::new(
        ctx.spec.chain_id,
        &ctx.spec.gas,
        &ctx.accounts,
        &ctx.artifacts,
        &mut ctx.nonces,
        &mut ctx.rng,
    );

    match output {
        Some(path) => {
            let mut writer = txgen_core::output::file_writer(&path)?;
            let written = generate_txs(adapter, &ctx.spec, count, &mut build_ctx, &mut writer)?;
            eprintln!("wrote {} transactions to {}", written, path.display());
        }
        None => {
            let mut writer = txgen_core::output::stdout_writer();
            generate_txs(adapter, &ctx.spec, count, &mut build_ctx, &mut writer)?;
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
enum WorkloadItem {
    Template(String),
    Sequence(String),
}

#[derive(Debug, Clone)]
enum ResolvedBinding {
    Account { pool: String, index: usize, address: Address },
    Address(Address),
    U256(U256),
    U64(u64),
    String(String),
}

const SEQUENCE_COUNTER_KEY: [u8; 20] = *b"txgen-sequence-key!!";

fn pick_workload_item(
    spec: &WorkloadSpec,
    rng: &mut StdRng,
    remaining_txs: u64,
) -> Result<Option<WorkloadItem>> {
    let mut total_weight = 0u64;
    let mut candidates = Vec::new();

    for entry in &spec.mix {
        let item = match (&entry.template, &entry.sequence) {
            (Some(template), None) => WorkloadItem::Template(template.clone()),
            (None, Some(sequence)) => WorkloadItem::Sequence(sequence.clone()),
            (Some(_), Some(_)) => {
                bail!("mix entries must set either `template` or `sequence`, not both")
            }
            (None, None) => bail!("mix entries must set either `template` or `sequence`"),
        };

        let tx_count = workload_item_tx_count(spec, &item)?;
        if tx_count > 0 && tx_count <= remaining_txs && entry.weight > 0 {
            total_weight = total_weight
                .checked_add(entry.weight)
                .ok_or_else(|| eyre::eyre!("mix weights overflowed u64"))?;
            candidates.push((item, entry.weight));
        }
    }

    if total_weight == 0 {
        return Ok(None);
    }

    let roll = rng.random_range(0..total_weight);
    let mut cumulative = 0;
    for (item, weight) in candidates {
        cumulative += weight;
        if roll < cumulative {
            return Ok(Some(item));
        }
    }

    unreachable!("workload selection failed with roll={roll} total_weight={total_weight}")
}

fn workload_item_tx_count(spec: &WorkloadSpec, item: &WorkloadItem) -> Result<u64> {
    match item {
        WorkloadItem::Template(name) => {
            if !spec.templates.contains_key(name) {
                bail!("template '{}' not found", name);
            }
            Ok(1)
        }
        WorkloadItem::Sequence(name) => {
            let sequence = spec
                .sequences
                .get(name)
                .ok_or_else(|| eyre::eyre!("sequence '{}' not found", name))?;
            if sequence.steps.is_empty() {
                bail!("sequence '{}' has no steps", name);
            }
            Ok(sequence.steps.len() as u64)
        }
    }
}

fn generate_txs<A: NetworkAdapter, W: Write>(
    adapter: &A,
    spec: &WorkloadSpec,
    count: u64,
    ctx: &mut BuildContext<'_>,
    writer: &mut NdjsonWriter<W>,
) -> Result<u64>
where
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
{
    let mut written = 0u64;

    while written < count {
        let remaining = count - written;
        let Some(item) = pick_workload_item(spec, ctx.rng, remaining)? else {
            break;
        };

        match item {
            WorkloadItem::Template(name) => {
                let value = spec
                    .templates
                    .get(&name)
                    .ok_or_else(|| eyre::eyre!("template '{}' not found", name))?
                    .clone();
                emit_template_value(adapter, &name, value, &[], ctx, writer)?;
                written += 1;
            }
            WorkloadItem::Sequence(name) => {
                let sequence = spec
                    .sequences
                    .get(&name)
                    .ok_or_else(|| eyre::eyre!("sequence '{}' not found", name))?;
                let sequence_instance = ctx.next_nonce(SEQUENCE_COUNTER_KEY);
                let sequence_key = compute_sequence_key(&name, sequence_instance);
                let bindings =
                    resolve_sequence_bindings(&sequence.bindings, sequence_instance, ctx)
                        .wrap_err_with(|| {
                            format!("failed to resolve bindings for sequence '{name}'")
                        })?;

                for (idx, step) in sequence.steps.iter().enumerate() {
                    let label = step.name.as_deref().unwrap_or(&step.template);
                    let base = spec
                        .templates
                        .get(&step.template)
                        .ok_or_else(|| eyre::eyre!("template '{}' not found", step.template))?
                        .clone();
                    let merged = merge_template_overlay(base, step.with_value.clone());
                    let materialized = substitute_vars(merged, &bindings).wrap_err_with(|| {
                        format!("failed to materialize sequence '{name}' step {idx} ('{label}')")
                    })?;
                    emit_template_value(
                        adapter,
                        &format!("{name}.{label}"),
                        materialized,
                        &[sequence_key],
                        ctx,
                        writer,
                    )?;
                    written += 1;
                }
            }
        }
    }

    writer.flush()?;
    Ok(written)
}

fn emit_template_value<A: NetworkAdapter, W: Write>(
    adapter: &A,
    name: &str,
    value: serde_yaml::Value,
    extra_scheduling_keys: &[[u8; 20]],
    ctx: &mut BuildContext<'_>,
    writer: &mut NdjsonWriter<W>,
) -> Result<()>
where
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
{
    let template: A::Template = serde_yaml::from_value(value)
        .wrap_err_with(|| format!("failed to parse template '{name}'"))?;

    let tx_req = adapter
        .build_request(template, ctx)
        .wrap_err_with(|| format!("failed to build request from template '{name}'"))?;

    let mut unsigned = tx_req
        .request
        .build_unsigned()
        .map_err(|e| eyre::eyre!("failed to build unsigned tx from template '{name}': {e}"))?;

    let signer = ctx.accounts.get_by_index(&tx_req.signer_pool, tx_req.signer_index)?;
    let sig = signer
        .sign_transaction_sync(&mut unsigned)
        .map_err(|e| eyre::eyre!("failed to sign tx from template '{name}': {e}"))?;

    let signed = unsigned.into_signed(sig);
    let envelope = <A::Network as Network>::TxEnvelope::from(signed);
    let raw = Bytes::from(envelope.encoded_2718());

    let mut scheduling_keys = Vec::with_capacity(1 + extra_scheduling_keys.len());
    scheduling_keys.push(tx_req.key);
    for key in extra_scheduling_keys {
        if !scheduling_keys.contains(key) {
            scheduling_keys.push(*key);
        }
    }

    writer.write(&GeneratedTx { raw, scheduling_keys })?;
    Ok(())
}

fn resolve_sequence_bindings(
    bindings: &std::collections::HashMap<String, SequenceBinding>,
    sequence_instance: u64,
    ctx: &mut BuildContext<'_>,
) -> Result<std::collections::HashMap<String, ResolvedBinding>> {
    let mut resolved = std::collections::HashMap::new();

    for (name, binding) in bindings {
        let fields_set = [
            binding.account.is_some(),
            binding.address.is_some(),
            binding.u256.is_some(),
            binding.u64.is_some(),
            binding.string.is_some(),
            binding.nonce_key.is_some(),
        ]
        .into_iter()
        .filter(|set| *set)
        .count();

        if fields_set != 1 {
            bail!("binding '{name}' must set exactly one binding type");
        }

        let value = if let Some(account) = &binding.account {
            let selected = ctx.select_signer(account)?;
            ResolvedBinding::Account {
                pool: selected.pool,
                index: selected.index,
                address: selected.address,
            }
        } else if let Some(address) = &binding.address {
            ResolvedBinding::Address(ctx.resolve_value(address)?)
        } else if let Some(u256) = &binding.u256 {
            ResolvedBinding::U256(ctx.resolve_value(u256)?)
        } else if let Some(u64_value) = &binding.u64 {
            ResolvedBinding::U64(ctx.resolve_value(u64_value)?)
        } else if let Some(string) = &binding.string {
            ResolvedBinding::String(ctx.resolve_value(string)?)
        } else if let Some(nonce_key) = &binding.nonce_key {
            ResolvedBinding::U256(resolve_nonce_key_binding(nonce_key, sequence_instance, ctx)?)
        } else {
            unreachable!("fields_set verified exactly one binding type")
        };

        resolved.insert(name.clone(), value);
    }

    Ok(resolved)
}

fn resolve_nonce_key_binding(
    binding: &NonceKeyBinding,
    sequence_instance: u64,
    ctx: &mut BuildContext<'_>,
) -> Result<U256> {
    if binding.unique {
        if binding.value.is_some() {
            bail!("unique nonce_key bindings must not also set `value`");
        }
        return binding
            .base
            .unwrap_or(U256::ZERO)
            .checked_add(U256::from(sequence_instance))
            .ok_or_else(|| eyre::eyre!("unique nonce_key binding overflowed U256"));
    }

    let value = binding.value.as_ref().ok_or_else(|| {
        eyre::eyre!("nonce_key bindings require either `unique: true` or `value`")
    })?;
    ctx.resolve_value(value)
}

fn compute_sequence_key(sequence_name: &str, sequence_instance: u64) -> [u8; 20] {
    let mut data = Vec::with_capacity(16 + sequence_name.len() + 8);
    data.extend_from_slice(b"txgen:sequence:");
    data.extend_from_slice(sequence_name.as_bytes());
    data.extend_from_slice(&sequence_instance.to_be_bytes());

    let hash = keccak256(data);
    let mut key = [0u8; 20];
    key.copy_from_slice(&hash[..20]);
    key
}

fn merge_template_overlay(
    mut base: serde_yaml::Value,
    overlay: serde_yaml::Value,
) -> serde_yaml::Value {
    if matches!(overlay, serde_yaml::Value::Null) {
        return base;
    }
    merge_yaml(&mut base, overlay);
    base
}

fn merge_yaml(base: &mut serde_yaml::Value, overlay: serde_yaml::Value) {
    match (base, overlay) {
        (serde_yaml::Value::Mapping(base_map), serde_yaml::Value::Mapping(overlay_map)) => {
            for (key, value) in overlay_map {
                match base_map.get_mut(&key) {
                    Some(base_value) => merge_yaml(base_value, value),
                    None => {
                        base_map.insert(key, value);
                    }
                }
            }
        }
        (base_value, overlay_value) => {
            *base_value = overlay_value;
        }
    }
}

fn substitute_vars(
    value: serde_yaml::Value,
    bindings: &std::collections::HashMap<String, ResolvedBinding>,
) -> Result<serde_yaml::Value> {
    match value {
        serde_yaml::Value::Mapping(mapping) if mapping.len() == 1 => {
            let var_key = serde_yaml::Value::String("var".to_string());
            if let Some(serde_yaml::Value::String(path)) = mapping.get(&var_key) {
                return binding_to_value(path, bindings);
            }
            let substituted = mapping
                .into_iter()
                .map(|(key, value)| Ok((key, substitute_vars(value, bindings)?)))
                .collect::<Result<serde_yaml::Mapping>>()?;
            Ok(serde_yaml::Value::Mapping(substituted))
        }
        serde_yaml::Value::Mapping(mapping) => {
            let substituted = mapping
                .into_iter()
                .map(|(key, value)| Ok((key, substitute_vars(value, bindings)?)))
                .collect::<Result<serde_yaml::Mapping>>()?;
            Ok(serde_yaml::Value::Mapping(substituted))
        }
        serde_yaml::Value::Sequence(values) => {
            let substituted = values
                .into_iter()
                .map(|value| substitute_vars(value, bindings))
                .collect::<Result<Vec<_>>>()?;
            Ok(serde_yaml::Value::Sequence(substituted))
        }
        other => Ok(other),
    }
}

fn binding_to_value(
    path: &str,
    bindings: &std::collections::HashMap<String, ResolvedBinding>,
) -> Result<serde_yaml::Value> {
    let (name, field) =
        path.split_once('.').map_or((path, None), |(name, field)| (name, Some(field)));
    let binding =
        bindings.get(name).ok_or_else(|| eyre::eyre!("unknown sequence binding '{name}'"))?;

    match (binding, field) {
        (ResolvedBinding::Account { pool, index, .. }, Some("ref")) => {
            account_ref_value(pool, *index)
        }
        (ResolvedBinding::Account { address, .. }, Some("address")) => {
            Ok(serde_yaml::Value::String(address.to_string()))
        }
        (ResolvedBinding::Account { .. }, None) => {
            bail!("account binding '{name}' requires `.ref` or `.address`")
        }
        (ResolvedBinding::Address(address), None) => {
            Ok(serde_yaml::Value::String(address.to_string()))
        }
        (ResolvedBinding::U256(value), None) => Ok(serde_yaml::Value::String(value.to_string())),
        (ResolvedBinding::U64(value), None) => Ok(serde_yaml::to_value(value)?),
        (ResolvedBinding::String(value), None) => Ok(serde_yaml::Value::String(value.clone())),
        (_, Some(field)) => bail!("binding '{name}' has no field '{field}'"),
    }
}

fn account_ref_value(pool: &str, index: usize) -> Result<serde_yaml::Value> {
    let mut select = serde_yaml::Mapping::new();
    select.insert(serde_yaml::Value::String("index".to_string()), serde_yaml::to_value(index)?);

    let mut account = serde_yaml::Mapping::new();
    account.insert(
        serde_yaml::Value::String("pool".to_string()),
        serde_yaml::Value::String(pool.to_string()),
    );
    account.insert(
        serde_yaml::Value::String("select".to_string()),
        serde_yaml::Value::Mapping(select),
    );

    Ok(serde_yaml::Value::Mapping(account))
}

// ---------------------------------------------------------------------------
// Private — addresses subcommand
// ---------------------------------------------------------------------------

fn run_addresses(args: AddressesArgs) -> Result<()> {
    let spec = WorkloadSpec::load(&args.spec)
        .wrap_err_with(|| format!("failed to load spec: {}", args.spec.display()))?;

    let accounts = AccountManager::from_spec(&spec.accounts)?;

    let all_addresses: Vec<_> = accounts.all_addresses().flat_map(|(_, addrs)| addrs).collect();

    match args.format.as_str() {
        "plain" => {
            for addr in &all_addresses {
                println!("{addr}");
            }
        }
        "json" => {
            let json = serde_json::to_string_pretty(&all_addresses)?;
            println!("{json}");
        }
        "shell" => {
            let line: Vec<_> = all_addresses.iter().map(|a| a.to_string()).collect();
            println!("{}", line.join(" "));
        }
        other => bail!("unknown format: {}", other),
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Private — extract subcommand
// ---------------------------------------------------------------------------

async fn run_extract<N>(args: ExtractArgs) -> Result<()>
where
    N: Network,
    N::TxEnvelope: Decodable,
    N::Header: Decodable,
{
    if args.from > args.to {
        bail!("--from must be <= --to");
    }

    let provider =
        alloy_provider::RootProvider::<N>::new_http(args.rpc.parse().wrap_err("invalid RPC URL")?);

    let (tx, mut rx) = mpsc::channel::<Result<FetchedBlock>>(args.buffer_size);

    let from = args.from;
    let to = args.to;
    let fetch_handle =
        tokio::spawn(async move { fetch_blocks::<N, _>(provider, from, to, tx).await });

    let total = to - from + 1;
    let write_result = match args.output {
        Some(ref path) => {
            let file = std::fs::File::create(path)
                .wrap_err_with(|| format!("failed to create output file: {}", path.display()))?;
            let mut writer = std::io::BufWriter::new(file);
            let result = write_extracted_blocks(&mut rx, &mut writer, total).await;
            if result.is_ok() {
                eprintln!("wrote {} blocks to {}", total, path.display());
            }
            result
        }
        None => {
            let mut writer = std::io::stdout();
            write_extracted_blocks(&mut rx, &mut writer, total).await
        }
    };

    fetch_handle.await?;
    write_result
}

#[derive(serde::Serialize)]
struct BlockOutputLine<'a> {
    raw: &'a str,
    key: &'a str,
    number: u64,
    timestamp: u64,
    gas_used: u64,
    gas_limit: u64,
    tx_count: usize,
}

async fn write_extracted_blocks<W: Write>(
    rx: &mut mpsc::Receiver<Result<FetchedBlock>>,
    writer: &mut W,
    total: u64,
) -> Result<()> {
    let start = std::time::Instant::now();
    let mut last_log = start;
    let mut count = 0u64;

    while let Some(result) = rx.recv().await {
        let block = result?;

        let raw_hex = format!("0x{}", hex::encode(&block.rlp_bytes));
        let key_hex = format!("{}", block.hash);
        let line = BlockOutputLine {
            raw: &raw_hex,
            key: &key_hex,
            number: block.number,
            timestamp: block.timestamp,
            gas_used: block.gas_used,
            gas_limit: block.gas_limit,
            tx_count: block.tx_count,
        };
        serde_json::to_writer(&mut *writer, &line)?;
        writer.write_all(b"\n")?;
        count += 1;

        let now = std::time::Instant::now();
        if count % 1000 == 0 || now.duration_since(last_log).as_secs() >= 5 {
            let elapsed = now.duration_since(start).as_secs_f64();
            let bps = count as f64 / elapsed;
            eprintln!(
                "extracted {}/{} blocks ({:.1}%) - {:.0} blocks/s",
                count,
                total,
                count as f64 / total as f64 * 100.0,
                bps
            );
            last_log = now;
        }
    }

    writer.flush()?;
    Ok(())
}

struct FetchedBlock {
    rlp_bytes: Bytes,
    hash: alloy_primitives::B256,
    number: u64,
    timestamp: u64,
    gas_used: u64,
    gas_limit: u64,
    tx_count: usize,
}

async fn fetch_blocks<N, P>(provider: P, from: u64, to: u64, tx: mpsc::Sender<Result<FetchedBlock>>)
where
    N: Network,
    N::TxEnvelope: Decodable,
    N::Header: Decodable,
    P: Provider<N> + DebugApi<N>,
{
    for block_num in from..=to {
        let result = async {
            let rlp_bytes: Bytes = provider
                .debug_get_raw_block(BlockNumberOrTag::Number(block_num).into())
                .await
                .wrap_err_with(|| format!("failed to fetch raw block {block_num}"))?;

            let sealed: Sealed<alloy_consensus::Block<N::TxEnvelope, N::Header>> =
                alloy_consensus::Block::decode_sealed(&mut rlp_bytes.as_ref())
                    .map_err(|e| eyre::eyre!("failed to decode block {block_num}: {e}"))?;

            let hash = sealed.hash();
            let block = sealed.inner();

            Ok(FetchedBlock {
                rlp_bytes,
                hash,
                number: block.header.number(),
                timestamp: block.header.timestamp(),
                gas_used: block.header.gas_used(),
                gas_limit: block.header.gas_limit(),
                tx_count: block.body.transactions.len(),
            })
        }
        .await;

        let is_err = result.is_err();
        if tx.send(result).await.is_err() {
            break;
        }
        if is_err {
            break;
        }
    }
}
