//! Reusable multi-chain scenario schema, runtime, execution, and reporting.

use alloy_consensus::{SignableTransaction, Signed};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::Network;
use clap::{Args, Subcommand, ValueEnum};
use eyre::{bail, Result, WrapErr};
use rand::Rng;
use std::{io::Write, path::PathBuf, time::Duration};

use crate::NetworkAdapter;

mod engine;
mod error;
mod report;
pub mod schema;
pub mod value;
mod wait;

pub use engine::{execute_scenario, FailurePolicy, ScenarioExecutionConfig};
pub use report::{
    ChainReportConfig, FailureReport, InstanceLifecycle, LatencyDistribution, LifecycleStep,
    ScenarioReport, ScenarioReportConfig, StepReport,
};
pub use schema::*;
pub use value::{
    coerce_event_filter, collect_variable_paths, eval_expression, event_value_matches,
    materialize_yaml, RuntimeContext, RuntimeValue,
};

/// Nested `scenario` command.
#[derive(Debug, Args)]
pub struct ScenarioArgs {
    #[command(subcommand)]
    command: ScenarioCommand,
}

#[derive(Debug, Subcommand)]
enum ScenarioCommand {
    /// Execute a versioned multi-chain scenario.
    Run(ScenarioRunArgs),
}

/// Controls for `scenario run`.
#[derive(Debug, Args)]
pub struct ScenarioRunArgs {
    /// Scenario YAML file.
    #[arg(long)]
    pub scenario: PathBuf,

    /// Maximum number of scenario instances to start.
    #[arg(short = 'n', long)]
    pub count: Option<u64>,

    /// Window during which new scenario instances may start.
    #[arg(long, value_parser = humantime::parse_duration)]
    pub duration: Option<Duration>,

    /// Scenario instances started per second (0 = unlimited).
    #[arg(long, default_value_t = 0.0)]
    pub starts_per_second: f64,

    /// Maximum active scenario instances.
    #[arg(long, default_value_t = 1)]
    pub max_in_flight: usize,

    /// Default timeout for steps without an explicit `timeout`.
    #[arg(long, value_parser = humantime::parse_duration)]
    pub step_timeout: Option<Duration>,

    /// RNG seed for deterministic binding and template materialization.
    #[arg(long)]
    pub seed: Option<u64>,

    /// Behavior after an instance fails.
    #[arg(long, value_enum, default_value_t = FailurePolicyArg::Continue)]
    failure_policy: FailurePolicyArg,

    /// Maximum transaction submissions per second on each chain (0 = unlimited).
    #[arg(long, default_value_t = 0)]
    pub tx_rate: u64,

    /// Maximum RPC transaction submissions in flight on each chain.
    #[arg(long, default_value_t = 100)]
    pub max_rpc_in_flight: usize,

    /// Write the JSON report to this path instead of stdout.
    #[arg(long)]
    pub report: Option<PathBuf>,

    /// Include the first N individual lifecycle records in the report.
    #[arg(long, default_value_t = 0)]
    pub sample_instances: usize,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum FailurePolicyArg {
    FailFast,
    Continue,
}

impl From<FailurePolicyArg> for FailurePolicy {
    fn from(value: FailurePolicyArg) -> Self {
        match value {
            FailurePolicyArg::FailFast => Self::FailFast,
            FailurePolicyArg::Continue => Self::Continue,
        }
    }
}

pub(crate) async fn run_scenario_command<A>(args: ScenarioArgs) -> Result<()>
where
    A: NetworkAdapter + Default + Send + Sync + 'static,
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
{
    match args.command {
        ScenarioCommand::Run(args) => run_scenario::<A>(args).await,
    }
}

async fn run_scenario<A>(args: ScenarioRunArgs) -> Result<()>
where
    A: NetworkAdapter + Default + Send + Sync + 'static,
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
{
    if args.count == Some(0) {
        bail!("--count must be greater than zero");
    }
    let spec = ScenarioSpec::load(&args.scenario)?;
    let count = args.count.or_else(|| args.duration.is_none().then_some(1));
    let seed = args.seed.unwrap_or_else(|| rand::rng().random());
    let report = execute_scenario::<A>(
        spec,
        ScenarioExecutionConfig {
            count,
            duration: args.duration,
            starts_per_second: args.starts_per_second,
            max_in_flight: args.max_in_flight,
            step_timeout: args.step_timeout,
            seed,
            failure_policy: args.failure_policy.into(),
            transaction_rate: args.tx_rate,
            max_rpc_in_flight: args.max_rpc_in_flight,
            sample_instances: args.sample_instances,
        },
    )
    .await?;

    match args.report {
        Some(path) => {
            let file = std::fs::File::create(&path).wrap_err_with(|| {
                format!("failed to create scenario report: {}", path.display())
            })?;
            let mut writer = std::io::BufWriter::new(file);
            serde_json::to_writer_pretty(&mut writer, &report)?;
            writer.write_all(b"\n")?;
            writer.flush()?;
        }
        None => {
            let stdout = std::io::stdout();
            let mut writer = stdout.lock();
            serde_json::to_writer_pretty(&mut writer, &report)?;
            writer.write_all(b"\n")?;
            writer.flush()?;
        }
    }
    Ok(())
}
