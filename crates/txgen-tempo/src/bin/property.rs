use std::{path::PathBuf, time::Duration};

use alloy_primitives::Address;
use clap::Parser;
use eyre::{bail, Result, WrapErr};
use serde::Deserialize;
use txgen_core::EcdsaSigner;
use txgen_property::{CampaignRegistry, RunConfig, WorkloadGenerator};
use txgen_tempo::property::{LiveZoneBackend, ZoneLiveConfig, ZonePropertyHarness, ZoneWorkload};

#[derive(Debug, Parser)]
#[command(name = "txgen-tempo-property", about = "Swarm-based Tempo/Zone solvency property runner")]
struct Args {
    /// Generated Zones zone.json; supplies portal, zoneId, and chainId.
    #[arg(long)]
    zone_config: Option<PathBuf>,

    /// Tempo L1 HTTP RPC URL. Falls back to L1_RPC_URL.
    #[arg(long)]
    l1_rpc_url: Option<String>,

    /// Full operator Zone HTTP RPC used for global verification. Falls back to ZONE_RPC_URL.
    #[arg(long)]
    zone_rpc_url: Option<String>,

    /// Authenticated redacted Zone HTTP RPC used for user operations.
    #[arg(long)]
    zone_private_rpc_url: Option<String>,

    /// Zone ID. Overrides zone.json.
    #[arg(long)]
    zone_id: Option<u32>,

    /// Zone chain ID. Overrides zone.json.
    #[arg(long)]
    zone_chain_id: Option<u64>,

    /// L1 ZonePortal address. Overrides zone.json.
    #[arg(long)]
    portal: Option<Address>,

    /// TIP-20 token address. Falls back to ZONE_TOKEN.
    #[arg(long)]
    token: Option<Address>,

    /// Environment variable containing the signing key.
    #[arg(long, default_value = "PRIVATE_KEY")]
    private_key_env: String,

    /// Number of independently configured swarm cases.
    #[arg(long, default_value_t = 100)]
    cases: u64,

    /// Keep generating fresh swarm cases until the first failure or process shutdown.
    #[arg(long)]
    continuous: bool,

    /// Maximum actions in one case.
    #[arg(long, default_value_t = 50)]
    max_steps: usize,

    /// Optional deterministic seed for replay. Normal runs use OS randomness.
    #[arg(long)]
    seed: Option<u64>,

    /// Independent inclusion probability for optional swarm capabilities.
    #[arg(long, default_value_t = 0.5)]
    swarm_density: f64,

    /// Maximum seconds to wait for receipts and cross-layer convergence.
    #[arg(long, default_value_t = 120)]
    settlement_timeout_secs: u64,

    /// Run the independent backing verifier every N actions; zero disables interval checks.
    #[arg(long, default_value_t = 25)]
    verify_every_steps: usize,

    /// First L1 block covering the Portal's complete event history.
    #[arg(long, default_value_t = 0)]
    l1_from_block: u64,

    /// First Zone block covering complete Inbox and Outbox event history.
    #[arg(long, default_value_t = 0)]
    zone_from_block: u64,

    /// Directory for first-failure YAML artifacts.
    #[arg(long, default_value = "property-failures")]
    failure_directory: PathBuf,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ZoneFile {
    portal: Option<Address>,
    zone_id: Option<u32>,
    chain_id: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let zone_file = args
        .zone_config
        .as_ref()
        .map(|path| {
            let bytes = std::fs::read(path)
                .wrap_err_with(|| format!("failed to read {}", path.display()))?;
            serde_json::from_slice::<ZoneFile>(&bytes)
                .wrap_err_with(|| format!("failed to parse {}", path.display()))
        })
        .transpose()?
        .unwrap_or_default();

    let l1_rpc_url = option_or_env(args.l1_rpc_url, "L1_RPC_URL")?;
    let zone_rpc_url = option_or_env(args.zone_rpc_url, "ZONE_RPC_URL")?;
    let zone_private_rpc_url = option_or_env(args.zone_private_rpc_url, "ZONE_PRIVATE_RPC_URL")?;
    let zone_id = args
        .zone_id
        .or(zone_file.zone_id)
        .ok_or_else(|| eyre::eyre!("set --zone-id or provide it through --zone-config"))?;
    let zone_chain_id = args.zone_chain_id.or(zone_file.chain_id).ok_or_else(|| {
        eyre::eyre!("set --zone-chain-id or provide chainId through --zone-config")
    })?;
    let portal = args
        .portal
        .or(zone_file.portal)
        .ok_or_else(|| eyre::eyre!("set --portal or provide it through --zone-config"))?;
    let token = match args.token {
        Some(token) => token,
        None => std::env::var("ZONE_TOKEN")
            .wrap_err("set --token or ZONE_TOKEN")?
            .parse()
            .wrap_err("ZONE_TOKEN is not a valid address")?,
    };
    let signer: EcdsaSigner = std::env::var(&args.private_key_env)
        .wrap_err_with(|| {
            format!("missing signing-key environment variable {}", args.private_key_env)
        })?
        .parse()
        .wrap_err_with(|| {
            format!("{} does not contain a valid private key", args.private_key_env)
        })?;

    let mut live = ZoneLiveConfig::new(
        l1_rpc_url,
        zone_rpc_url,
        zone_private_rpc_url,
        zone_id,
        zone_chain_id,
        portal,
        token,
    );
    live.settlement_timeout = Duration::from_secs(args.settlement_timeout_secs);
    live.l1_from_block = args.l1_from_block;
    live.zone_from_block = args.zone_from_block;

    let backend = LiveZoneBackend::connect(live, signer).await?;
    let harness = ZonePropertyHarness::new(backend);
    let mut campaigns = CampaignRegistry::new();
    campaigns.register(ZoneWorkload, harness)?;

    let mut run = match args.seed {
        Some(seed) => RunConfig::seeded(args.cases, args.max_steps, seed),
        None => RunConfig::random(args.cases, args.max_steps),
    };
    run.swarm.density = args.swarm_density;
    run.continuous = args.continuous;
    run.verify_every_steps = args.verify_every_steps;
    run.failure_directory = Some(args.failure_directory);
    eprintln!(
        "[zone-property] start campaign={} seed={} cases={} continuous={} max_steps={} swarm_density={} verify_every_steps={}",
        ZoneWorkload::NAME,
        run.seed,
        run.cases,
        run.continuous,
        run.max_steps,
        run.swarm.density,
        run.verify_every_steps,
    );

    let result = campaigns.run(ZoneWorkload::NAME, run).await?;
    if let Some(failure) = result.failure {
        eprintln!(
            "[zone-property] FAIL seed={} case={} step={:?} error={}",
            failure.seed, failure.case_index, failure.step_index, failure.error
        );
        if let Some(path) = result.failure_path {
            eprintln!("[zone-property] artifact={}", path.display());
        }
        bail!("Zone property invariant failed");
    }

    eprintln!(
        "[zone-property] PASS seed={} completed_cases={} completed_steps={} verifications={}",
        result.report.seed,
        result.report.completed_cases,
        result.report.completed_steps,
        result.report.completed_verifications,
    );
    Ok(())
}

fn option_or_env(value: Option<String>, variable: &str) -> Result<String> {
    value.map(Ok).unwrap_or_else(|| {
        std::env::var(variable)
            .wrap_err_with(|| format!("set the corresponding flag or {variable}"))
    })
}
