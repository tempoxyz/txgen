use alloy_consensus::{SignableTransaction, Signed};
use alloy_dyn_abi::{DynSolType, DynSolValue};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::{Network, NetworkTransactionBuilder, TransactionBuilder, TxSignerSync};
use alloy_primitives::{keccak256, Address, Bytes, TxKind, B256, U256};
use alloy_provider::Provider;
use clap::{ArgGroup, Args};
use eyre::{bail, Result, WrapErr};
use rand::{rngs::StdRng, Rng, SeedableRng};
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::{
    collections::{BTreeMap, HashSet},
    io::Write,
    path::PathBuf,
    sync::mpsc,
    time::{Duration, Instant},
};
use txgen_core::{
    dedup_scheduling_keys, merge_yaml, AbiEncodePackedDef, AbiHashDef, AccountManager,
    AddressPoolManager, ArtifactManager, BuildContext, EcdsaSigner, FixturePoolManager,
    GeneratedTx, MixItem, NdjsonWriter, NonceTracker, SchedulingKey, SequenceBinding, SetupStep,
    TxPhase, WorkloadSpec,
};

fn default_signing_workers() -> usize {
    2
}

#[derive(Args)]
#[command(group(
    ArgGroup::new("limit")
        .required(true)
        .multiple(true)
        .args(["count", "duration"])
))]
pub struct GenerateArgs {
    /// Workload spec file (YAML)
    #[arg(short, long)]
    pub spec: PathBuf,

    /// Number of transactions to generate
    #[arg(short = 'n', long)]
    pub count: Option<u64>,

    /// Maximum workload generation duration.
    ///
    /// The timer starts after setup transactions are emitted. Txgen checks the
    /// deadline before starting each workload item, so transaction sequences
    /// are never emitted partially.
    #[arg(long, value_parser = humantime::parse_duration)]
    pub duration: Option<Duration>,

    /// Output file (default: stdout)
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// RPC endpoint URL (to fetch current nonces from chain)
    #[arg(long)]
    pub rpc: Option<String>,

    /// RNG seed for reproducibility
    #[arg(long)]
    pub seed: Option<u64>,

    /// Number of worker threads used to sign and encode workload transactions.
    #[arg(long, visible_alias = "workers", default_value_t = default_signing_workers())]
    pub signing_workers: usize,
}

// ---------------------------------------------------------------------------
// GenerateContext — bundles common setup for generation
// ---------------------------------------------------------------------------

pub struct GenerateContext {
    spec: WorkloadSpec,
    accounts: AccountManager,
    address_pools: AddressPoolManager,
    fixture_pools: FixturePoolManager,
    artifacts: ArtifactManager,
    nonces: NonceTracker,
    rng: StdRng,
    limit: GenerationLimit,
    signing_workers: usize,
}

impl GenerateContext {
    pub fn from_args(args: &GenerateArgs) -> Result<Self> {
        if args.signing_workers == 0 {
            bail!("--signing-workers must be at least 1");
        }

        let spec = WorkloadSpec::load(&args.spec)
            .wrap_err_with(|| format!("failed to load spec: {}", args.spec.display()))?;
        let base_path = args.spec.parent().unwrap_or_else(|| std::path::Path::new("."));
        let accounts = AccountManager::from_spec(&spec.accounts)?;
        let address_pools = AddressPoolManager::from_spec(&spec.address_pools)?;
        let artifacts = ArtifactManager::load(&spec.artifacts, base_path)?;
        let nonces = NonceTracker::new();
        let mut rng = match args.seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::from_os_rng(),
        };
        let fixture_pools = FixturePoolManager::load(&spec.fixture_pools, base_path, &mut rng)?;
        let limit = GenerationLimit { count: args.count, duration: args.duration };
        Ok(Self {
            spec,
            accounts,
            address_pools,
            fixture_pools,
            artifacts,
            nonces,
            rng,
            limit,
            signing_workers: args.signing_workers,
        })
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

/// Output from [`NetworkAdapter::build_request`].
pub struct TxRequest<R, C = ()> {
    /// The network-specific transaction request.
    pub request: R,
    /// Signer pool name.
    pub signer_pool: String,
    /// Signer index within the pool.
    pub signer_index: usize,
    /// Scheduling key (e.g. sender address or hash of sender+nonce_key).
    pub key: [u8; 20],
    /// State the signing worker needs in addition to the selected account.
    ///
    /// Most adapters use `()`. Tempo keychain auth uses this to carry the
    /// access-key signer and authorized user address without adding
    /// Tempo-specific branches to the generic generation loop.
    pub sign_context: C,
}

/// Converts an adapter-built request into the raw transaction emitted by txgen.
///
/// The default implementation signs with the selected account. Adapters can
/// carry request-local context when the final signature is not the network's
/// standard account signature, such as Tempo keychain access-key signing.
pub trait RequestSignContext<N: Network>: Send + 'static {
    /// Sign and encode one request with its scheduling metadata.
    fn sign_request(
        self,
        name: String,
        phase: TxPhase,
        request: <N as Network>::TransactionRequest,
        signer: EcdsaSigner,
        key: [u8; 20],
        inclusion_keys: Vec<SchedulingKey>,
    ) -> Result<GeneratedTx>
    where
        <N as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
        <N as Network>::TxEnvelope: From<Signed<<N as Network>::UnsignedTx>> + Encodable2718;
}

impl<N: Network> RequestSignContext<N> for () {
    fn sign_request(
        self,
        name: String,
        phase: TxPhase,
        request: <N as Network>::TransactionRequest,
        signer: EcdsaSigner,
        key: [u8; 20],
        inclusion_keys: Vec<SchedulingKey>,
    ) -> Result<GeneratedTx>
    where
        <N as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
        <N as Network>::TxEnvelope: From<Signed<<N as Network>::UnsignedTx>> + Encodable2718,
    {
        sign_standard_request::<N>(name, phase, request, signer, key, inclusion_keys)
    }
}

/// Sign and encode a request with the network's standard secp256k1 transaction signature.
pub fn sign_standard_request<N: Network>(
    name: String,
    phase: TxPhase,
    request: <N as Network>::TransactionRequest,
    signer: EcdsaSigner,
    key: [u8; 20],
    inclusion_keys: Vec<SchedulingKey>,
) -> Result<GeneratedTx>
where
    <N as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <N as Network>::TxEnvelope: From<Signed<<N as Network>::UnsignedTx>> + Encodable2718,
{
    let mut unsigned = request
        .build_unsigned()
        .map_err(|e| eyre::eyre!("failed to build unsigned tx from template '{name}': {e}"))?;

    let sig = signer
        .sign_transaction_sync(&mut unsigned)
        .map_err(|e| eyre::eyre!("failed to sign tx from template '{name}': {e}"))?;

    let signed = unsigned.into_signed(sig);
    let envelope = <N as Network>::TxEnvelope::from(signed);
    let raw = Bytes::from(envelope.encoded_2718());

    Ok(GeneratedTx {
        phase,
        id: Some(name),
        raw,
        submission_keys: vec![SchedulingKey::from(key)],
        inclusion_keys,
    })
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

    /// Extra per-request state needed by the signing worker.
    type SignContext: RequestSignContext<Self::Network>;

    /// Map a template to a network-specific transaction request.
    fn build_request(
        &self,
        template: Self::Template,
        ctx: &mut BuildContext<'_>,
    ) -> Result<TxRequest<<Self::Network as Network>::TransactionRequest, Self::SignContext>>;

    /// Expand an adapter-specific setup step into ordinary transaction templates.
    ///
    /// Returning `Ok(None)` means the adapter does not support the extension.
    fn expand_setup_extension(
        &mut self,
        _step_id: &str,
        _extension_name: &str,
        _value: serde_yaml::Value,
        _ctx: &mut BuildContext<'_>,
    ) -> Result<Option<Vec<serde_yaml::Value>>> {
        Ok(None)
    }

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

pub(crate) async fn run_generate<A>(mut adapter: A, args: GenerateArgs) -> Result<()>
where
    A: NetworkAdapter + 'static,
    <A::Network as Network>::TransactionRequest: Send + 'static,
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
{
    let output = args.output.clone();
    let rpc = args.rpc.clone();
    let mut ctx = GenerateContext::from_args(&args)?;

    if let Some(ref rpc) = rpc {
        adapter.prefetch_nonces(&mut ctx, rpc).await?;
    }

    generate_loop(&mut adapter, &mut ctx, output)
}

// ---------------------------------------------------------------------------
// Public helpers — used by per-network generate implementations
// ---------------------------------------------------------------------------

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
        eprintln!("fetching nonces for {} ({} accounts)...", pool_name, total);
        for (idx, address) in addresses.iter().enumerate() {
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
        }
        eprintln!("fetched nonces for {} ({} accounts)", pool_name, total);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct GenerationLimit {
    count: Option<u64>,
    duration: Option<Duration>,
}

fn generate_loop<A>(
    adapter: &mut A,
    ctx: &mut GenerateContext,
    output: Option<PathBuf>,
) -> Result<()>
where
    A: NetworkAdapter + 'static,
    <A::Network as Network>::TransactionRequest: Send + 'static,
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
{
    if ctx.spec.total_weight() == 0 {
        bail!("no workload entries in mix (total weight is 0)");
    }

    let mut build_ctx = BuildContext::new_with_pools(
        ctx.spec.chain_id,
        &ctx.spec.gas,
        &ctx.accounts,
        &ctx.address_pools,
        &ctx.fixture_pools,
        &ctx.artifacts,
        &mut ctx.nonces,
        &mut ctx.rng,
    );

    match output {
        Some(path) => {
            let mut writer = txgen_core::output::file_writer(&path)?;
            let setup_bindings = emit_setup(adapter, &ctx.spec, &mut build_ctx, &mut writer)?;
            let written = generate_txs(
                adapter,
                &ctx.spec,
                ctx.limit,
                ctx.signing_workers,
                &setup_bindings,
                &mut build_ctx,
                &mut writer,
            )?;
            eprintln!("wrote {} workload transactions to {}", written, path.display());
        }
        None => {
            let mut writer = txgen_core::output::stdout_writer();
            let setup_bindings = emit_setup(adapter, &ctx.spec, &mut build_ctx, &mut writer)?;
            generate_txs(
                adapter,
                &ctx.spec,
                ctx.limit,
                ctx.signing_workers,
                &setup_bindings,
                &mut build_ctx,
                &mut writer,
            )?;
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
enum ResolvedBinding {
    Account { pool: String, index: usize, address: Address },
    Fixture(serde_yaml::Value),
    Address(Address),
    Bytes32(B256),
    Bytes(Bytes),
    U256(U256),
    U64(u64),
    String(String),
    SetupTx { address: Option<Address>, tx_hash: B256, sender: Address, nonce: u64 },
}

fn pick_workload_item(
    spec: &WorkloadSpec,
    rng: &mut StdRng,
    remaining_txs: u64,
) -> Result<Option<MixItem>> {
    let mut total_weight = 0u64;
    let mut candidates = Vec::new();

    for entry in &spec.mix {
        let item = entry.item.clone();
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

fn workload_item_tx_count(spec: &WorkloadSpec, item: &MixItem) -> Result<u64> {
    match item {
        MixItem::Template(name) => {
            if !spec.templates.contains_key(name) {
                bail!("template '{}' not found", name);
            }
            Ok(1)
        }
        MixItem::Sequence(name) => {
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

#[derive(Debug, Clone)]
struct EmittedTxInfo {
    sender: Address,
    nonce: u64,
    tx_hash: B256,
    created_address: Option<Address>,
}

fn emit_setup<A: NetworkAdapter, W: Write>(
    adapter: &mut A,
    spec: &WorkloadSpec,
    ctx: &mut BuildContext<'_>,
    writer: &mut NdjsonWriter<W>,
) -> Result<std::collections::HashMap<String, ResolvedBinding>>
where
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
{
    let mut bindings = std::collections::HashMap::new();
    bindings.insert("chain_id".to_string(), ResolvedBinding::U64(ctx.chain_id));

    let Some(setup) = &spec.setup else {
        return Ok(bindings);
    };

    let setup_key = compute_setup_key();
    for step in &setup.steps {
        emit_setup_step(adapter, step, &mut bindings, setup_key, ctx, writer)
            .wrap_err_with(|| format!("failed to emit setup step '{}'", step.id))?;
    }

    Ok(bindings)
}

fn emit_setup_step<A: NetworkAdapter, W: Write>(
    adapter: &mut A,
    step: &SetupStep,
    setup_bindings: &mut std::collections::HashMap<String, ResolvedBinding>,
    setup_key: SchedulingKey,
    ctx: &mut BuildContext<'_>,
    writer: &mut NdjsonWriter<W>,
) -> Result<()>
where
    A::SignContext: RequestSignContext<A::Network>,
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
{
    let has_deploy = step.deploy.is_some();
    let has_tx = step.tx.is_some();
    let has_keychain_authorize_pool = step.keychain_authorize_pool.is_some();
    let action_count =
        usize::from(has_deploy) + usize::from(has_tx) + usize::from(has_keychain_authorize_pool);
    if action_count != 1 {
        bail!("setup step must set exactly one of `deploy`, `tx`, or `keychain_authorize_pool`");
    }

    let local_bindings = resolve_sequence_bindings(&step.bindings, ctx, setup_bindings)?;
    if let Some(keychain_authorize_pool) = &step.keychain_authorize_pool {
        let materialized = substitute_vars(keychain_authorize_pool.clone(), &local_bindings)?;
        let templates = adapter
            .expand_setup_extension(&step.id, "keychain_authorize_pool", materialized, ctx)?
            .ok_or_else(|| {
                eyre::eyre!("adapter does not support setup step `keychain_authorize_pool`")
            })?;
        if templates.is_empty() {
            bail!("setup step `keychain_authorize_pool` produced no transactions");
        }
        for (idx, value) in templates.into_iter().enumerate() {
            let inclusion_key = compute_setup_extension_key(&step.id, idx);
            emit_template_value(
                adapter,
                &format!("setup.{}[{idx}]", step.id),
                value,
                TxPhase::Setup,
                &[inclusion_key],
                ctx,
                writer,
            )?;
        }
        return Ok(());
    }

    let info = if let Some(deploy) = &step.deploy {
        let materialized = substitute_vars(deploy.clone(), &local_bindings)?;
        let value = build_deploy_template_value(materialized, ctx)?;
        let info = emit_template_value(
            adapter,
            &format!("setup.{}", step.id),
            value,
            TxPhase::Setup,
            &[setup_key],
            ctx,
            writer,
        )?
        .expect("setup emissions request tx info");
        if info.created_address.is_none() {
            bail!("deploy setup step did not produce a contract creation transaction");
        }
        info
    } else {
        let tx = step.tx.as_ref().expect("checked exactly one setup action");
        let materialized = substitute_vars(tx.clone(), &local_bindings)?;
        emit_template_value(
            adapter,
            &format!("setup.{}", step.id),
            materialized,
            TxPhase::Setup,
            &[setup_key],
            ctx,
            writer,
        )?
        .expect("setup emissions request tx info")
    };

    setup_bindings.insert(
        format!("setup.{}", step.id),
        ResolvedBinding::SetupTx {
            address: info.created_address,
            tx_hash: info.tx_hash,
            sender: info.sender,
            nonce: info.nonce,
        },
    );

    Ok(())
}

fn build_deploy_template_value(
    value: serde_yaml::Value,
    ctx: &mut BuildContext<'_>,
) -> Result<serde_yaml::Value> {
    let serde_yaml::Value::Mapping(mut mapping) = value else {
        bail!("deploy setup step must be a mapping");
    };

    let artifact_key = serde_yaml::Value::String("artifact".to_string());
    let constructor_args_key = serde_yaml::Value::String("constructor_args".to_string());
    let input_key = serde_yaml::Value::String("input".to_string());
    let to_key = serde_yaml::Value::String("to".to_string());

    if mapping.contains_key(&input_key) || mapping.contains_key(&to_key) {
        bail!("deploy setup steps must not set `to` or `input`");
    }

    let artifact_value = mapping
        .remove(&artifact_key)
        .ok_or_else(|| eyre::eyre!("deploy setup step requires `artifact`"))?;
    let artifact: String = serde_yaml::from_value(artifact_value)?;
    let constructor_args: Vec<serde_yaml::Value> = mapping
        .remove(&constructor_args_key)
        .map(serde_yaml::from_value)
        .transpose()?
        .unwrap_or_default();

    let initcode = {
        let artifacts = ctx.artifacts;
        let mut resolver = ctx.resolver();
        artifacts.encode_constructor(&artifact, &constructor_args, &mut resolver)?
    };
    mapping.insert(input_key, serde_yaml::Value::String(initcode.to_string()));

    Ok(serde_yaml::Value::Mapping(mapping))
}

type NetworkRequest<A> = <<A as NetworkAdapter>::Network as Network>::TransactionRequest;
type AdapterTxRequest<A> = TxRequest<NetworkRequest<A>, <A as NetworkAdapter>::SignContext>;

struct SigningJob<A: NetworkAdapter> {
    sequence: u64,
    name: String,
    phase: TxPhase,
    tx_req: AdapterTxRequest<A>,
    signer: EcdsaSigner,
    inclusion_keys: Vec<SchedulingKey>,
}

struct SigningResult {
    sequence: u64,
    result: Result<GeneratedTx>,
}

struct SigningPool {
    pool: ThreadPool,
    result_tx: mpsc::Sender<SigningResult>,
    result_rx: mpsc::Receiver<SigningResult>,
    completed: BTreeMap<u64, GeneratedTx>,
    next_sequence: u64,
    next_to_write: u64,
    in_flight: usize,
    max_in_flight: usize,
}

impl SigningPool {
    fn new(worker_count: usize) -> Result<Self> {
        if worker_count == 0 {
            bail!("signing worker count must be at least 1");
        }

        let (result_tx, result_rx) = mpsc::channel();
        let pool = ThreadPoolBuilder::new()
            .num_threads(worker_count)
            .thread_name(|worker_id| format!("txgen-sign-{worker_id}"))
            .build()
            .wrap_err("failed to create signing worker pool")?;

        Ok(Self {
            pool,
            result_tx,
            result_rx,
            completed: BTreeMap::new(),
            next_sequence: 0,
            next_to_write: 0,
            in_flight: 0,
            max_in_flight: worker_count.saturating_mul(64).max(1),
        })
    }

    fn next_sequence(&mut self) -> Result<u64> {
        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| eyre::eyre!("signing job sequence counter overflowed u64"))?;
        Ok(sequence)
    }

    fn submit<A: NetworkAdapter + 'static, W: Write>(
        &mut self,
        job: SigningJob<A>,
        writer: &mut NdjsonWriter<W>,
    ) -> Result<()>
    where
        NetworkRequest<A>: Send + 'static,
        <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
        <A::Network as Network>::TxEnvelope:
            From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
    {
        self.drain_available(writer)?;
        while self.in_flight >= self.max_in_flight {
            self.recv_one(writer)?;
        }

        let result_tx = self.result_tx.clone();
        self.pool.spawn_fifo(move || submit_signing_job::<A>(job, result_tx));
        self.in_flight += 1;

        Ok(())
    }

    fn finish<W: Write>(&mut self, writer: &mut NdjsonWriter<W>) -> Result<()> {
        while self.in_flight > 0 {
            self.recv_one(writer)?;
        }
        Ok(())
    }

    fn drain_available<W: Write>(&mut self, writer: &mut NdjsonWriter<W>) -> Result<()> {
        loop {
            match self.result_rx.try_recv() {
                Ok(result) => self.handle_result(result, writer)?,
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    if self.in_flight > 0 {
                        bail!("signing worker pool stopped before completing all jobs");
                    }
                    break;
                }
            }
        }
        Ok(())
    }

    fn recv_one<W: Write>(&mut self, writer: &mut NdjsonWriter<W>) -> Result<()> {
        let result = self
            .result_rx
            .recv()
            .map_err(|_| eyre::eyre!("signing worker pool stopped before completing all jobs"))?;
        self.handle_result(result, writer)
    }

    fn handle_result<W: Write>(
        &mut self,
        result: SigningResult,
        writer: &mut NdjsonWriter<W>,
    ) -> Result<()> {
        let tx = result.result?;
        if self.completed.insert(result.sequence, tx).is_some() {
            bail!("received duplicate signing result for sequence {}", result.sequence);
        }
        self.write_ready(writer)
    }

    fn write_ready<W: Write>(&mut self, writer: &mut NdjsonWriter<W>) -> Result<()> {
        while let Some(tx) = self.completed.remove(&self.next_to_write) {
            if self.in_flight == 0 {
                bail!("received unexpected signing result");
            }
            writer.write(&tx)?;
            self.in_flight -= 1;
            self.next_to_write = self
                .next_to_write
                .checked_add(1)
                .ok_or_else(|| eyre::eyre!("written signing result counter overflowed u64"))?;
        }
        Ok(())
    }
}

fn submit_signing_job<A: NetworkAdapter>(job: SigningJob<A>, result_tx: mpsc::Sender<SigningResult>)
where
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
{
    let sequence = job.sequence;
    let result =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sign_workload_job::<A>(job)))
            .unwrap_or_else(|panic| {
                Err(eyre::eyre!("signing worker panicked: {}", panic_msg(&panic)))
            });
    let _ = result_tx.send(SigningResult { sequence, result });
}

fn panic_msg(panic: &(dyn std::any::Any + Send)) -> &str {
    if let Some(message) = panic.downcast_ref::<&'static str>() {
        message
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.as_str()
    } else {
        "unknown panic payload"
    }
}

fn prepare_signing_job<A: NetworkAdapter>(
    adapter: &A,
    name: String,
    value: serde_yaml::Value,
    phase: TxPhase,
    inclusion_keys: &[SchedulingKey],
    sequence: u64,
    ctx: &mut BuildContext<'_>,
) -> Result<SigningJob<A>> {
    let template: A::Template = serde_yaml::from_value(value)
        .wrap_err_with(|| format!("failed to parse template '{name}'"))?;

    let tx_req = adapter
        .build_request(template, ctx)
        .wrap_err_with(|| format!("failed to build request from template '{name}'"))?;

    let signer = ctx.accounts.get_by_index(&tx_req.signer_pool, tx_req.signer_index)?.clone();

    Ok(SigningJob {
        sequence,
        name,
        phase,
        tx_req,
        signer,
        inclusion_keys: dedup_scheduling_keys(inclusion_keys.iter().copied()),
    })
}

fn sign_workload_job<A: NetworkAdapter>(job: SigningJob<A>) -> Result<GeneratedTx>
where
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
{
    let SigningJob { sequence: _, name, phase, tx_req, signer, inclusion_keys } = job;
    let TxRequest { request, signer_pool: _, signer_index: _, key, sign_context } = tx_req;

    sign_context.sign_request(name, phase, request, signer, key, inclusion_keys)
}

fn generate_txs<A, W: Write>(
    adapter: &A,
    spec: &WorkloadSpec,
    limit: GenerationLimit,
    signing_workers: usize,
    setup_bindings: &std::collections::HashMap<String, ResolvedBinding>,
    ctx: &mut BuildContext<'_>,
    writer: &mut NdjsonWriter<W>,
) -> Result<u64>
where
    A: NetworkAdapter + 'static,
    NetworkRequest<A>: Send + 'static,
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
{
    let mut signing_pool = SigningPool::new(signing_workers)?;
    let mut written = 0u64;
    let mut sequence_instances = 0u64;
    let start = Instant::now();

    while limit.count.is_none_or(|count| written < count) {
        if limit.duration.is_some_and(|duration| start.elapsed() > duration) {
            break;
        }

        let remaining = limit.count.map(|count| count - written).unwrap_or(u64::MAX);
        let Some(item) = pick_workload_item(spec, ctx.rng, remaining)? else {
            break;
        };

        match item {
            MixItem::Template(name) => {
                let value = spec
                    .templates
                    .get(&name)
                    .ok_or_else(|| eyre::eyre!("template '{}' not found", name))?
                    .clone();
                let materialized = substitute_vars(value, setup_bindings)
                    .wrap_err_with(|| format!("failed to materialize template '{name}'"))?;
                let sequence = signing_pool.next_sequence()?;
                let job = prepare_signing_job(
                    adapter,
                    name,
                    materialized,
                    TxPhase::Workload,
                    &[],
                    sequence,
                    ctx,
                )?;
                signing_pool.submit(job, writer)?;
                written += 1;
            }
            MixItem::Sequence(name) => {
                let sequence = spec
                    .sequences
                    .get(&name)
                    .ok_or_else(|| eyre::eyre!("sequence '{}' not found", name))?;
                let sequence_instance = sequence_instances;
                sequence_instances = sequence_instances
                    .checked_add(1)
                    .ok_or_else(|| eyre::eyre!("sequence instance counter overflowed u64"))?;
                let sequence_key = compute_sequence_key(&name, sequence_instance);
                let inclusion_keys = sequence_inclusion_keys(sequence_key, sequence.steps.len());
                let bindings = resolve_sequence_bindings(&sequence.bindings, ctx, setup_bindings)
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
                    let sequence = signing_pool.next_sequence()?;
                    let job = prepare_signing_job(
                        adapter,
                        format!("{name}.{label}"),
                        materialized,
                        TxPhase::Workload,
                        &inclusion_keys,
                        sequence,
                        ctx,
                    )?;
                    signing_pool.submit(job, writer)?;
                    written += 1;
                }
            }
        }
    }

    signing_pool.finish(writer)?;
    writer.flush()?;
    Ok(written)
}

fn emit_template_value<A: NetworkAdapter, W: Write>(
    adapter: &A,
    name: &str,
    value: serde_yaml::Value,
    phase: TxPhase,
    inclusion_keys: &[SchedulingKey],
    ctx: &mut BuildContext<'_>,
    writer: &mut NdjsonWriter<W>,
) -> Result<Option<EmittedTxInfo>>
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

    let signer = ctx.accounts.get_by_index(&tx_req.signer_pool, tx_req.signer_index)?;
    let captured = if phase == TxPhase::Setup {
        let sender = signer.address();
        let nonce = tx_req
            .request
            .nonce()
            .ok_or_else(|| eyre::eyre!("template '{name}' did not set a nonce"))?;
        let created_address =
            matches!(tx_req.request.kind(), Some(TxKind::Create)).then(|| sender.create(nonce));
        Some((sender, nonce, created_address))
    } else {
        None
    };

    let TxRequest { request, signer_pool: _, signer_index: _, key, sign_context } = tx_req;
    let generated = sign_context.sign_request(
        name.to_string(),
        phase,
        request,
        signer.clone(),
        key,
        dedup_scheduling_keys(inclusion_keys.iter().copied()),
    )?;
    let raw = generated.raw.clone();
    let info = captured.map(|(sender, nonce, created_address)| EmittedTxInfo {
        sender,
        nonce,
        tx_hash: keccak256(&raw),
        created_address,
    });

    writer.write(&generated)?;
    Ok(info)
}

fn resolve_sequence_bindings(
    bindings: &std::collections::HashMap<String, SequenceBinding>,
    ctx: &mut BuildContext<'_>,
    globals: &std::collections::HashMap<String, ResolvedBinding>,
) -> Result<std::collections::HashMap<String, ResolvedBinding>> {
    let mut resolved = globals.clone();
    for name in bindings.keys() {
        resolved.remove(name);
    }
    if !bindings.contains_key("chain_id") && !resolved.contains_key("chain_id") {
        resolved.insert("chain_id".to_string(), ResolvedBinding::U64(ctx.chain_id));
    }

    let mut resolving = HashSet::new();
    for name in bindings.keys() {
        resolve_sequence_binding(name, bindings, &mut resolved, &mut resolving, ctx)?;
    }

    Ok(resolved)
}

fn resolve_sequence_binding(
    name: &str,
    bindings: &std::collections::HashMap<String, SequenceBinding>,
    resolved: &mut std::collections::HashMap<String, ResolvedBinding>,
    resolving: &mut HashSet<String>,
    ctx: &mut BuildContext<'_>,
) -> Result<()> {
    if resolved.contains_key(name) {
        return Ok(());
    }

    let binding =
        bindings.get(name).ok_or_else(|| eyre::eyre!("unknown sequence binding '{name}'"))?;
    if !resolving.insert(name.to_string()) {
        bail!("circular sequence binding dependency involving '{name}'");
    }

    resolve_binding_dependencies(binding, bindings, resolved, resolving, ctx)?;

    let value = match binding {
        SequenceBinding::Account(account) => {
            let selected = ctx.select_signer(account)?;
            ResolvedBinding::Account {
                pool: selected.pool,
                index: selected.index,
                address: selected.address,
            }
        }
        SequenceBinding::Fixture(fixture) => ResolvedBinding::Fixture(ctx.select_fixture(fixture)?),
        SequenceBinding::Address(address) => ResolvedBinding::Address(ctx.resolve_value(address)?),
        SequenceBinding::Bytes32(bytes32) => ResolvedBinding::Bytes32(ctx.resolve_value(bytes32)?),
        SequenceBinding::AbiEncodePacked(def) => {
            ResolvedBinding::Bytes(resolve_abi_encode_packed(def, resolved)?)
        }
        SequenceBinding::AbiHash(abi_hash) => {
            ResolvedBinding::Bytes32(resolve_abi_hash(abi_hash, resolved)?)
        }
        SequenceBinding::U256(u256) => ResolvedBinding::U256(ctx.resolve_value(u256)?),
        SequenceBinding::U64(u64_value) => ResolvedBinding::U64(ctx.resolve_value(u64_value)?),
        SequenceBinding::String(string) => ResolvedBinding::String(ctx.resolve_value(string)?),
    };

    resolving.remove(name);
    resolved.insert(name.to_string(), value);
    Ok(())
}

fn resolve_binding_dependencies(
    binding: &SequenceBinding,
    bindings: &std::collections::HashMap<String, SequenceBinding>,
    resolved: &mut std::collections::HashMap<String, ResolvedBinding>,
    resolving: &mut HashSet<String>,
    ctx: &mut BuildContext<'_>,
) -> Result<()> {
    for dep in binding_dependency_names(binding, bindings) {
        if resolved.contains_key(&dep) {
            continue;
        }
        resolve_sequence_binding(&dep, bindings, resolved, resolving, ctx)?;
    }
    Ok(())
}

fn binding_dependency_names(
    binding: &SequenceBinding,
    bindings: &std::collections::HashMap<String, SequenceBinding>,
) -> HashSet<String> {
    let mut deps = HashSet::new();
    match binding {
        SequenceBinding::AbiEncodePacked(def) => {
            for value in &def.values {
                collect_var_names(value, bindings, &mut deps);
            }
        }
        SequenceBinding::AbiHash(def) => {
            for value in &def.values {
                collect_var_names(value, bindings, &mut deps);
            }
        }
        SequenceBinding::Fixture(_) => {}
        _ => {}
    }
    deps
}

fn resolve_abi_encode_packed(
    def: &AbiEncodePackedDef,
    bindings: &std::collections::HashMap<String, ResolvedBinding>,
) -> Result<Bytes> {
    let values = resolve_abi_values(&def.types, &def.values, bindings, "abi_encode_packed")?;
    Ok(Bytes::from(DynSolValue::Tuple(values).abi_encode_packed()))
}

fn resolve_abi_hash(
    def: &AbiHashDef,
    bindings: &std::collections::HashMap<String, ResolvedBinding>,
) -> Result<B256> {
    let values = resolve_abi_values(&def.types, &def.values, bindings, "abi_hash")?;
    Ok(keccak256(DynSolValue::Tuple(values).abi_encode_params()))
}

fn resolve_abi_values(
    types: &[String],
    raw_values: &[serde_yaml::Value],
    bindings: &std::collections::HashMap<String, ResolvedBinding>,
    context: &str,
) -> Result<Vec<DynSolValue>> {
    if types.len() != raw_values.len() {
        bail!(
            "{context} expects the same number of types and values, got {} types and {} values",
            types.len(),
            raw_values.len()
        );
    }

    let mut values = Vec::with_capacity(raw_values.len());
    for (idx, (sol_type, value)) in types.iter().zip(raw_values).enumerate() {
        let substituted = substitute_vars(value.clone(), bindings)?;
        let json = yaml_to_json(substituted)?;
        let ty = DynSolType::parse(sol_type)
            .wrap_err_with(|| format!("failed to parse {context} type {idx} ('{sol_type}')"))?;
        let value = ty
            .coerce_json(&json)
            .wrap_err_with(|| format!("failed to coerce {context} value {idx} as '{sol_type}'"))?;
        values.push(value);
    }

    Ok(values)
}

fn yaml_to_json(value: serde_yaml::Value) -> Result<serde_json::Value> {
    Ok(serde_json::to_value(value)?)
}

fn collect_var_names(
    value: &serde_yaml::Value,
    bindings: &std::collections::HashMap<String, SequenceBinding>,
    names: &mut HashSet<String>,
) {
    match value {
        serde_yaml::Value::Mapping(mapping) if mapping.len() == 1 => {
            let var_key = serde_yaml::Value::String("var".to_string());
            if let Some(serde_yaml::Value::String(path)) = mapping.get(&var_key) {
                if let Some(name) = referenced_local_binding(path, bindings) {
                    names.insert(name);
                }
                return;
            }
            for value in mapping.values() {
                collect_var_names(value, bindings, names);
            }
        }
        serde_yaml::Value::Mapping(mapping) => {
            for value in mapping.values() {
                collect_var_names(value, bindings, names);
            }
        }
        serde_yaml::Value::Sequence(values) => {
            for value in values {
                collect_var_names(value, bindings, names);
            }
        }
        _ => {}
    }
}

fn referenced_local_binding(
    path: &str,
    bindings: &std::collections::HashMap<String, SequenceBinding>,
) -> Option<String> {
    let first = path.split('.').next().unwrap_or(path);
    if bindings.contains_key(first) {
        Some(first.to_string())
    } else {
        None
    }
}

fn compute_setup_key() -> SchedulingKey {
    scheduling_key_from_hash(keccak256(b"txgen:setup"))
}

fn compute_setup_extension_key(step_id: &str, idx: usize) -> SchedulingKey {
    let material = format!("txgen:setup:{step_id}:{idx}");
    scheduling_key_from_hash(keccak256(material.as_bytes()))
}

fn scheduling_key_from_hash(hash: B256) -> SchedulingKey {
    let mut key = [0u8; 20];
    key.copy_from_slice(&hash[..20]);
    SchedulingKey::from(key)
}

fn compute_sequence_key(sequence_name: &str, sequence_instance: u64) -> SchedulingKey {
    let mut data = Vec::with_capacity(16 + sequence_name.len() + 8);
    data.extend_from_slice(b"txgen:sequence:");
    data.extend_from_slice(sequence_name.as_bytes());
    data.extend_from_slice(&sequence_instance.to_be_bytes());

    scheduling_key_from_hash(keccak256(data))
}

fn sequence_inclusion_keys(key: SchedulingKey, step_count: usize) -> Vec<SchedulingKey> {
    if step_count > 1 {
        vec![key]
    } else {
        Vec::new()
    }
}

fn merge_template_overlay(
    mut base: serde_yaml::Value,
    overlay: serde_yaml::Value,
) -> serde_yaml::Value {
    merge_yaml(&mut base, overlay);
    base
}

fn substitute_vars(
    value: serde_yaml::Value,
    bindings: &std::collections::HashMap<String, ResolvedBinding>,
) -> Result<serde_yaml::Value> {
    match value {
        serde_yaml::Value::Mapping(mapping) if mapping.len() == 1 => {
            let var_key = serde_yaml::Value::String("var".to_string());
            if let Some(serde_yaml::Value::String(path)) = mapping.get(&var_key) {
                // If we don't yet have a binding for this variable, leave it for later parsing.
                if split_binding_path(path, bindings).is_none() {
                    return Ok(serde_yaml::Value::Mapping(mapping));
                }
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
    let (name, field) = split_binding_path(path, bindings)
        .ok_or_else(|| eyre::eyre!("unknown binding '{path}'"))?;
    let binding = bindings.get(name).ok_or_else(|| eyre::eyre!("unknown binding '{name}'"))?;

    match (binding, field) {
        (ResolvedBinding::Account { pool, index, .. }, Some("ref")) => {
            account_ref_value(pool, *index)
        }
        (ResolvedBinding::Account { address, .. }, Some("address")) => {
            Ok(serde_yaml::Value::String(address.to_string()))
        }
        (ResolvedBinding::Account { .. }, None) => {
            bail!("account binding '{name}' requires `.ref` or `.address`");
        }
        (ResolvedBinding::Fixture(fixture), field) => project_fixture_binding(name, fixture, field),
        (ResolvedBinding::Address(address), None) => {
            Ok(serde_yaml::Value::String(address.to_string()))
        }
        (ResolvedBinding::Bytes32(value), None) => Ok(serde_yaml::Value::String(value.to_string())),
        (ResolvedBinding::Bytes(value), None) => Ok(serde_yaml::Value::String(value.to_string())),
        (ResolvedBinding::U256(value), None) => Ok(serde_yaml::Value::String(value.to_string())),
        (ResolvedBinding::U64(value), None) => Ok(serde_yaml::to_value(value)?),
        (ResolvedBinding::String(value), None) => Ok(serde_yaml::Value::String(value.clone())),
        (ResolvedBinding::SetupTx { address: Some(address), .. }, Some("address")) => {
            Ok(serde_yaml::Value::String(address.to_string()))
        }
        (ResolvedBinding::SetupTx { address: None, .. }, Some("address")) => {
            bail!("setup transaction binding '{name}' has no deployed address");
        }
        (ResolvedBinding::SetupTx { tx_hash, .. }, Some("tx_hash")) => {
            Ok(serde_yaml::Value::String(tx_hash.to_string()))
        }
        (ResolvedBinding::SetupTx { sender, .. }, Some("sender")) => {
            Ok(serde_yaml::Value::String(sender.to_string()))
        }
        (ResolvedBinding::SetupTx { nonce, .. }, Some("nonce")) => Ok(serde_yaml::to_value(nonce)?),
        (ResolvedBinding::SetupTx { .. }, None) => {
            bail!("setup transaction binding '{name}' requires a field");
        }
        (_, Some(field)) => {
            bail!("binding '{name}' has no field '{field}'");
        }
    }
}

fn project_fixture_binding(
    name: &str,
    fixture: &serde_yaml::Value,
    field: Option<&str>,
) -> Result<serde_yaml::Value> {
    let Some(field) = field else {
        return Ok(fixture.clone());
    };

    let mut value = fixture;
    for segment in field.split('.') {
        value = match value {
            serde_yaml::Value::Mapping(mapping) => mapping
                .get(serde_yaml::Value::String(segment.to_string()))
                .ok_or_else(|| eyre::eyre!("fixture binding '{name}' has no field '{field}'"))?,
            serde_yaml::Value::Sequence(values) => {
                let index: usize = segment.parse().map_err(|_| {
                    eyre::eyre!(
                        "fixture binding '{name}' field '{field}' uses non-numeric array index '{segment}'"
                    )
                })?;
                values
                    .get(index)
                    .ok_or_else(|| eyre::eyre!("fixture binding '{name}' has no field '{field}'"))?
            }
            _ => bail!("fixture binding '{name}' has no field '{field}'"),
        };
    }

    Ok(value.clone())
}

fn split_binding_path<'a>(
    path: &'a str,
    bindings: &std::collections::HashMap<String, ResolvedBinding>,
) -> Option<(&'a str, Option<&'a str>)> {
    if bindings.contains_key(path) {
        return Some((path, None));
    }

    for (idx, _) in path.match_indices('.').rev() {
        let name = &path[..idx];
        let field = &path[idx + 1..];
        if bindings.contains_key(name) {
            return Some((name, Some(field)));
        }
    }

    None
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn var(path: &str) -> serde_yaml::Value {
        let mut mapping = serde_yaml::Mapping::new();
        mapping.insert(
            serde_yaml::Value::String("var".to_string()),
            serde_yaml::Value::String(path.to_string()),
        );
        serde_yaml::Value::Mapping(mapping)
    }

    fn yaml_get<'a>(value: &'a serde_yaml::Value, key: &str) -> &'a serde_yaml::Value {
        value
            .as_mapping()
            .and_then(|mapping| mapping.get(serde_yaml::Value::String(key.to_string())))
            .unwrap_or_else(|| panic!("missing YAML key '{key}'"))
    }

    #[test]
    fn substitute_vars_preserves_call_arg_local_vars() -> Result<()> {
        let token = Address::from([0x20; 20]);
        let value = serde_yaml::from_str::<serde_yaml::Value>(
            r#"
call:
  args:
    vars:
      is_bid:
        choice: [true, false]
      tick:
        uniform: { min: { var: min_tick }, max: 30, step: 10 }
    values:
      - { var: token }
      - { uniform: { min: { var: global_amount }, max: 500, step: 100 } }
      - { var: is_bid }
      - { var: tick }
      - if:
          cond: { var: is_bid }
          then: { uniform: { min: { var: tick }, max: 30, step: 10 } }
          else: { uniform: { min: -30, max: { var: tick }, step: 10 } }
"#,
        )?;
        let bindings = HashMap::from([
            ("global_amount".to_string(), ResolvedBinding::U64(123)),
            ("min_tick".to_string(), ResolvedBinding::U64(0)),
            ("token".to_string(), ResolvedBinding::Address(token)),
        ]);

        let materialized = substitute_vars(value, &bindings)?;

        let args = yaml_get(yaml_get(&materialized, "call"), "args");
        let vars = yaml_get(args, "vars");

        let tick_uniform = yaml_get(yaml_get(vars, "tick"), "uniform");
        assert_eq!(yaml_get(tick_uniform, "min"), &serde_yaml::to_value(0u64)?);

        let values = yaml_get(args, "values").as_sequence().expect("values sequence");
        assert_eq!(values[0], serde_yaml::Value::String(token.to_string()));
        assert_eq!(
            yaml_get(yaml_get(&values[1], "uniform"), "min"),
            &serde_yaml::to_value(123u64)?
        );
        assert_eq!(values[2], var("is_bid"));
        assert_eq!(values[3], var("tick"));
        assert_eq!(yaml_get(yaml_get(&values[4], "if"), "cond"), &var("is_bid"));
        Ok(())
    }

    #[test]
    fn fixture_binding_projects_correlated_from_and_input_fields() -> Result<()> {
        let base = serde_yaml::from_str::<serde_yaml::Value>(
            r#"
from:
  pool: users
  select: { index: 0 }
input: "0x"
first_proof: { var: claim.metadata.proof.0 }
"#,
        )?;
        let overlay = serde_yaml::from_str::<serde_yaml::Value>(
            r#"
from: { var: claim.from }
input: { var: claim.input }
"#,
        )?;
        let fixture = serde_yaml::from_str::<serde_yaml::Value>(
            r#"
from:
  pool: users
  select: { index: 17 }
input: "0x1234"
metadata:
  proof: ["0xabcd"]
"#,
        )?;
        let bindings = HashMap::from([("claim".to_string(), ResolvedBinding::Fixture(fixture))]);

        let materialized = substitute_vars(merge_template_overlay(base, overlay), &bindings)?;

        assert_eq!(yaml_get(&materialized, "input").as_str(), Some("0x1234"));
        assert_eq!(yaml_get(&materialized, "first_proof").as_str(), Some("0xabcd"));
        let from = yaml_get(&materialized, "from");
        assert_eq!(yaml_get(from, "pool").as_str(), Some("users"));
        assert_eq!(yaml_get(yaml_get(from, "select"), "index").as_u64(), Some(17));
        Ok(())
    }

    #[test]
    fn template_overlay_still_deep_merges_regular_mappings() -> Result<()> {
        let base = serde_yaml::from_str::<serde_yaml::Value>(
            r#"
call:
  to: "0x0000000000000000000000000000000000000001"
  args:
    values: [1]
    vars:
      amount: 1
      recipient: alice
"#,
        )?;
        let overlay = serde_yaml::from_str::<serde_yaml::Value>(
            r#"
call:
  args:
    vars:
      amount: 2
"#,
        )?;

        let merged = merge_template_overlay(base, overlay);
        let call = yaml_get(&merged, "call");
        assert_eq!(
            yaml_get(call, "to").as_str(),
            Some("0x0000000000000000000000000000000000000001")
        );
        let args = yaml_get(call, "args");
        assert_eq!(yaml_get(args, "values").as_sequence().unwrap().len(), 1);
        let vars = yaml_get(args, "vars");
        assert_eq!(yaml_get(vars, "amount").as_u64(), Some(2));
        assert_eq!(yaml_get(vars, "recipient").as_str(), Some("alice"));
        Ok(())
    }

    #[test]
    fn fixture_binding_rejects_missing_projection() {
        let bindings = HashMap::from([(
            "claim".to_string(),
            ResolvedBinding::Fixture(serde_yaml::from_str("input: 0x1234").unwrap()),
        )]);

        let err = binding_to_value("claim.from", &bindings)
            .expect_err("missing fixture fields must fail during materialization");
        assert!(err.to_string().contains("fixture binding 'claim' has no field 'from'"));
    }

    #[test]
    fn single_step_sequences_do_not_wait_for_inclusion() {
        let key = SchedulingKey::from([0x42; 20]);
        assert!(sequence_inclusion_keys(key, 1).is_empty());
        assert_eq!(sequence_inclusion_keys(key, 2), vec![key]);
        assert_eq!(sequence_inclusion_keys(key, 3), vec![key]);
    }
}
