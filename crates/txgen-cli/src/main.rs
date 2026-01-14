use alloy_provider::{Provider, ProviderBuilder, network::Ethereum};
use clap::{Args, Parser, Subcommand};
use eyre::{Result, WrapErr, bail};
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::io::Write;
use std::path::PathBuf;
use txgen_core::{
    AccountManager, ArtifactManager, BuildContext, ChainPlugin, GasConfig, GeneratedTx,
    NdjsonWriter, NonceTracker, WorkloadSpec,
};
use txgen_ethereum::{EthereumPlugin, EthereumTemplate};
use txgen_tempo::{TempoNonceProvider, TempoPlugin, TempoTemplate};

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
}

#[derive(Args)]
struct AddressesArgs {
    /// Workload spec file (YAML)
    #[arg(short, long)]
    spec: PathBuf,

    /// Output format: plain (one per line), json, or shell (for xargs)
    #[arg(short, long, default_value = "plain")]
    format: String,
}

#[derive(Args)]
struct GenerateArgs {
    /// Workload spec file (YAML)
    #[arg(short, long)]
    spec: PathBuf,

    /// Chain plugin: ethereum, tempo
    #[arg(short, long)]
    chain: String,

    /// Number of transactions to generate
    #[arg(short = 'n', long)]
    count: u64,

    /// Output file (default: stdout)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// RPC endpoint URL (to fetch current nonces from chain)
    #[arg(long)]
    rpc: Option<String>,

    /// Rate limit for RPC requests per second (0 = unbounded)
    #[arg(long, default_value = "0")]
    rpc_rps: u64,

    /// RNG seed for reproducibility
    #[arg(long)]
    seed: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Generate(args) => run_generate(args).await,
        Command::Addresses(args) => run_addresses(args),
    }
}

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
            // Space-separated for use with xargs
            let line: Vec<_> = all_addresses.iter().map(|a| a.to_string()).collect();
            println!("{}", line.join(" "));
        }
        other => bail!("unknown format: {}", other),
    }

    Ok(())
}

async fn run_generate(args: GenerateArgs) -> Result<()> {
    let spec = WorkloadSpec::load(&args.spec)
        .wrap_err_with(|| format!("failed to load spec: {}", args.spec.display()))?;

    let base_path = args
        .spec
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));

    let accounts = AccountManager::from_spec(&spec.accounts)?;
    let artifacts = ArtifactManager::load(&spec.artifacts, base_path)?;
    let gas = spec.gas.clone();
    let mut nonces = NonceTracker::new();

    // Fetch protocol nonces from RPC if provided
    if let Some(ref rpc_url) = args.rpc {
        fetch_protocol_nonces(&accounts, &mut nonces, rpc_url).await?;
    }

    let mut rng = match args.seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::from_os_rng(),
    };

    match args.chain.as_str() {
        "ethereum" => generate_with_plugin::<EthereumPlugin, EthereumTemplate>(
            EthereumPlugin,
            &spec,
            args.count,
            args.output,
            &accounts,
            &artifacts,
            &gas,
            &mut nonces,
            &mut rng,
        ),
        "tempo" => {
            // For Tempo, use async generation with nonce provider for parallel lanes
            let nonce_provider = args.rpc.as_ref().map(|rpc_url| {
                let provider = ProviderBuilder::<_, _, Ethereum>::new()
                    .connect_http(rpc_url.parse().expect("invalid RPC URL"));
                if args.rpc_rps > 0 {
                    TempoNonceProvider::with_rate_limit(provider, args.rpc_rps)
                } else {
                    TempoNonceProvider::new(provider)
                }
            });

            generate_tempo(
                &spec,
                args.count,
                args.output,
                &accounts,
                &artifacts,
                &gas,
                &mut nonces,
                &mut rng,
                nonce_provider.as_ref(),
            )
            .await
        }
        other => bail!("unsupported chain plugin: {}", other),
    }
}

/// Generate Tempo transactions with async nonce fetching for parallel lanes.
#[allow(clippy::too_many_arguments)]
async fn generate_tempo<P: txgen_core::NonceProvider>(
    spec: &WorkloadSpec,
    count: u64,
    output: Option<PathBuf>,
    accounts: &AccountManager,
    artifacts: &ArtifactManager,
    gas: &GasConfig,
    nonces: &mut NonceTracker,
    rng: &mut StdRng,
    nonce_provider: Option<&P>,
) -> Result<()> {
    let plugin = TempoPlugin::default();
    let total_weight = spec.total_weight();
    if total_weight == 0 {
        bail!("no templates in mix (total weight is 0)");
    }

    let mut ctx = BuildContext::new(spec.chain_id, gas, accounts, artifacts, nonces, rng);

    match output {
        Some(path) => {
            let mut writer = txgen_core::output::file_writer(&path)?;
            generate_tempo_txs(
                &plugin,
                spec,
                count,
                total_weight,
                &mut ctx,
                &mut writer,
                nonce_provider,
            )
            .await?;
            eprintln!("wrote {} transactions to {}", count, path.display());
        }
        None => {
            let mut writer = txgen_core::output::stdout_writer();
            generate_tempo_txs(
                &plugin,
                spec,
                count,
                total_weight,
                &mut ctx,
                &mut writer,
                nonce_provider,
            )
            .await?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn generate_tempo_txs<W: Write, P: txgen_core::NonceProvider>(
    plugin: &TempoPlugin,
    spec: &WorkloadSpec,
    count: u64,
    total_weight: u64,
    ctx: &mut BuildContext<'_>,
    writer: &mut NdjsonWriter<W>,
    nonce_provider: Option<&P>,
) -> Result<()> {
    let start = std::time::Instant::now();
    let mut last_log = start;

    for i in 0..count {
        let template_name = pick_template(spec, ctx.rng, total_weight)?;
        let template_value = spec
            .templates
            .get(&template_name)
            .ok_or_else(|| eyre::eyre!("template '{}' not found", template_name))?;

        let template: TempoTemplate = serde_yaml::from_value(template_value.clone())
            .wrap_err_with(|| format!("failed to parse template '{}'", template_name))?;

        let tx = plugin
            .build_with_nonce_provider(template, ctx, nonce_provider)
            .await
            .wrap_err_with(|| format!("failed to build tx from template '{}'", template_name))?;

        writer.write(&tx)?;

        // Log progress every 10k txs or every 5 seconds
        let now = std::time::Instant::now();
        if (i + 1) % 10000 == 0 || now.duration_since(last_log).as_secs() >= 5 {
            let elapsed = now.duration_since(start).as_secs_f64();
            let tps = (i + 1) as f64 / elapsed;
            eprintln!(
                "generated {}/{} txs ({:.1}%) - {:.0} tx/s",
                i + 1,
                count,
                (i + 1) as f64 / count as f64 * 100.0,
                tps
            );
            last_log = now;
        }
    }
    writer.flush()?;
    Ok(())
}

/// Fetch protocol nonces (nonce_key=0) for all accounts from the RPC.
async fn fetch_protocol_nonces(
    accounts: &AccountManager,
    nonces: &mut NonceTracker,
    rpc_url: &str,
) -> Result<()> {
    let provider = ProviderBuilder::<_, _, Ethereum>::new()
        .connect_http(rpc_url.parse().wrap_err("invalid RPC URL")?);

    for (pool_name, addresses) in accounts.all_addresses() {
        let total = addresses.len();
        for (idx, address) in addresses.iter().enumerate() {
            eprintln!(
                "fetching nonce for {}[{}/{}] ({})...",
                pool_name,
                idx + 1,
                total,
                address
            );

            let nonce = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                provider.get_transaction_count(*address),
            )
            .await
            .wrap_err_with(|| format!("timeout fetching nonce for {}[{}]", pool_name, idx))?
            .wrap_err_with(|| {
                format!(
                    "failed to fetch nonce for {}[{}] ({})",
                    pool_name, idx, address
                )
            })?;

            // Protocol nonce scheduling key = sender address
            let scheduling_key = address.0.0;
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

#[allow(clippy::too_many_arguments)]
fn generate_with_plugin<P, T>(
    plugin: P,
    spec: &WorkloadSpec,
    count: u64,
    output: Option<PathBuf>,
    accounts: &AccountManager,
    artifacts: &ArtifactManager,
    gas: &GasConfig,
    nonces: &mut NonceTracker,
    rng: &mut StdRng,
) -> Result<()>
where
    P: ChainPlugin<Template = T>,
    T: serde::de::DeserializeOwned,
{
    let total_weight = spec.total_weight();
    if total_weight == 0 {
        bail!("no templates in mix (total weight is 0)");
    }

    let mut ctx = BuildContext::new(spec.chain_id, gas, accounts, artifacts, nonces, rng);

    match output {
        Some(path) => {
            let mut writer = txgen_core::output::file_writer(&path)?;
            generate_txs(&plugin, spec, count, total_weight, &mut ctx, &mut writer)?;
            eprintln!("wrote {} transactions to {}", count, path.display());
        }
        None => {
            let mut writer = txgen_core::output::stdout_writer();
            generate_txs(&plugin, spec, count, total_weight, &mut ctx, &mut writer)?;
        }
    }

    Ok(())
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
        let template_name = pick_template(spec, ctx.rng, total_weight)?;
        let template_value = spec
            .templates
            .get(&template_name)
            .ok_or_else(|| eyre::eyre!("template '{}' not found", template_name))?;

        let template: T = serde_yaml::from_value(template_value.clone())
            .wrap_err_with(|| format!("failed to parse template '{}'", template_name))?;

        let tx: GeneratedTx = plugin
            .build(template, ctx)
            .wrap_err_with(|| format!("failed to build tx from template '{}'", template_name))?;

        writer.write(&tx)?;
    }
    writer.flush()?;
    Ok(())
}

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
