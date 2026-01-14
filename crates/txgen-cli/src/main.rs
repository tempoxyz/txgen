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
use txgen_tempo::{TempoPlugin, TempoTemplate};

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

    /// RNG seed for reproducibility
    #[arg(long)]
    seed: Option<u64>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Generate(args) => run_generate(args),
    }
}

fn run_generate(args: GenerateArgs) -> Result<()> {
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
        "tempo" => generate_with_plugin::<TempoPlugin, TempoTemplate>(
            TempoPlugin::default(),
            &spec,
            args.count,
            args.output,
            &accounts,
            &artifacts,
            &gas,
            &mut nonces,
            &mut rng,
        ),
        other => bail!("unsupported chain plugin: {}", other),
    }
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
