use alloy_consensus::{Block as ConsensusBlock, TxEnvelope};
use alloy_eips::BlockNumberOrTag;
use alloy_primitives::Bytes;
use alloy_provider::{Provider, ext::DebugApi};
use alloy_rlp::Decodable;
use clap::{Args, Parser, Subcommand};
use eyre::{Result, WrapErr, bail};
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::io::Write;
use std::path::PathBuf;
use tokio::sync::mpsc;
use txgen_core::{
    AccountManager, ArtifactManager, BuildContext, ChainPlugin, GeneratedTx, NdjsonWriter,
    NonceTracker, WorkloadSpec,
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
        let base_path = args
            .spec
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let accounts = AccountManager::from_spec(&spec.accounts)?;
        let artifacts = ArtifactManager::load(&spec.artifacts, base_path)?;
        let nonces = NonceTracker::new();
        let rng = match args.seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::from_os_rng(),
        };
        Ok(Self {
            spec,
            accounts,
            artifacts,
            nonces,
            rng,
        })
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
// TxgenNetwork trait — implemented by per-network binaries
// ---------------------------------------------------------------------------

pub trait TxgenNetwork {
    fn generate(
        &self,
        args: GenerateArgs,
    ) -> impl std::future::Future<Output = Result<()>> + Send + '_;
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

pub async fn run(network: impl TxgenNetwork) -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Generate(args) => network.generate(args).await,
        Command::Addresses(args) => run_addresses(args),
        Command::Extract(args) => run_extract(args).await,
    }
}

// ---------------------------------------------------------------------------
// Public helpers — used by per-network generate implementations
// ---------------------------------------------------------------------------

/// Pick a random template and deserialize it from the spec.
pub fn load_template<T: serde::de::DeserializeOwned>(
    spec: &WorkloadSpec,
    rng: &mut StdRng,
    total_weight: u64,
) -> Result<(String, T)> {
    let name = pick_template(spec, rng, total_weight)?;
    let value = spec
        .templates
        .get(&name)
        .ok_or_else(|| eyre::eyre!("template '{}' not found", name))?;
    let template: T = serde_yaml::from_value(value.clone())
        .wrap_err_with(|| format!("failed to parse template '{}'", name))?;
    Ok((name, template))
}

pub fn generate_with_plugin<P, T>(
    plugin: P,
    ctx: &mut GenerateContext,
    count: u64,
    output: Option<PathBuf>,
) -> Result<()>
where
    P: ChainPlugin<Template = T>,
    T: serde::de::DeserializeOwned,
{
    let total_weight = ctx.spec.total_weight();
    if total_weight == 0 {
        bail!("no templates in mix (total weight is 0)");
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
            generate_txs(
                &plugin,
                &ctx.spec,
                count,
                total_weight,
                &mut build_ctx,
                &mut writer,
            )?;
            eprintln!("wrote {} transactions to {}", count, path.display());
        }
        None => {
            let mut writer = txgen_core::output::stdout_writer();
            generate_txs(
                &plugin,
                &ctx.spec,
                count,
                total_weight,
                &mut build_ctx,
                &mut writer,
            )?;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

fn pick_template(spec: &WorkloadSpec, rng: &mut StdRng, total_weight: u64) -> Result<String> {
    let roll = rng.random_range(0..total_weight);
    let mut cumulative = 0;
    for entry in &spec.mix {
        cumulative += entry.weight;
        if roll < cumulative {
            return Ok(entry.template.clone());
        }
    }
    // SAFETY: Should not reach here if total_weight > 0 and mix is non-empty
    unreachable!(
        "template selection failed with roll={} total_weight={}",
        roll, total_weight
    )
}

fn generate_txs<P, T, W>(
    plugin: &P,
    spec: &WorkloadSpec,
    count: u64,
    total_weight: u64,
    ctx: &mut BuildContext<'_>,
    writer: &mut NdjsonWriter<W>,
) -> Result<()>
where
    P: ChainPlugin<Template = T>,
    T: serde::de::DeserializeOwned,
    W: Write,
{
    for _ in 0..count {
        let (name, template) = load_template::<T>(spec, ctx.rng, total_weight)?;

        let tx: GeneratedTx = plugin
            .build(template, ctx)
            .wrap_err_with(|| format!("failed to build tx from template '{}'", name))?;

        writer.write(&tx)?;
    }
    writer.flush()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Private — addresses subcommand
// ---------------------------------------------------------------------------

fn run_addresses(args: AddressesArgs) -> Result<()> {
    let spec = WorkloadSpec::load(&args.spec)
        .wrap_err_with(|| format!("failed to load spec: {}", args.spec.display()))?;

    let accounts = AccountManager::from_spec(&spec.accounts)?;

    let all_addresses: Vec<_> = accounts
        .all_addresses()
        .flat_map(|(_, addrs)| addrs)
        .collect();

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

async fn run_extract(args: ExtractArgs) -> Result<()> {
    if args.from > args.to {
        bail!("--from must be <= --to");
    }

    let provider =
        alloy_provider::ProviderBuilder::<_, _, alloy_provider::network::Ethereum>::new()
            .connect_http(args.rpc.parse().wrap_err("invalid RPC URL")?);

    let (tx, mut rx) = mpsc::channel::<Result<FetchedBlock>>(args.buffer_size);

    let from = args.from;
    let to = args.to;
    let fetch_handle = tokio::spawn(async move { fetch_blocks(provider, from, to, tx).await });

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
}

async fn fetch_blocks<P: Provider + DebugApi>(
    provider: P,
    from: u64,
    to: u64,
    tx: mpsc::Sender<Result<FetchedBlock>>,
) {
    for block_num in from..=to {
        let result = async {
            let rlp_bytes: Bytes = provider
                .debug_get_raw_block(BlockNumberOrTag::Number(block_num).into())
                .await
                .wrap_err_with(|| format!("failed to fetch raw block {block_num}"))?;

            let mut buf = rlp_bytes.as_ref();
            let block = ConsensusBlock::<TxEnvelope>::decode(&mut buf)
                .wrap_err_with(|| format!("failed to RLP-decode block {block_num}"))?;
            let hash = block.header.hash_slow();

            Ok(FetchedBlock { rlp_bytes, hash })
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
