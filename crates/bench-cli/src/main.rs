//! CLI binary for the bench tool.
//!
//! Provides subcommands:
//! - `run` - All-in-one: generate + send + report
//! - `send` - Send from file/stdin
//! - `replay` - Engine API block replay
//! - `plot` - Generate plots from JSON report

use clap::{Args, Parser, Subcommand, ValueEnum};
use eyre::Result;
use std::path::PathBuf;
use std::time::Duration;

mod plot;
mod replay;
mod run;
mod send;

#[derive(Parser)]
#[command(name = "bench", about = "Transaction benchmarking tool")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// All-in-one: generate + send + report
    Run(RunArgs),
    /// Send transactions from file or stdin
    Send(SendArgs),
    /// Replay blocks via Engine API
    Replay(ReplayArgs),
    /// Generate plots from JSON report
    Plot(PlotArgs),
}

/// Arguments for the `run` subcommand.
#[derive(Args)]
pub struct RunArgs {
    /// Workload spec file (YAML)
    #[arg(short, long)]
    pub spec: PathBuf,

    /// Chain plugin: ethereum, tempo
    #[arg(short, long)]
    pub chain: ChainType,

    /// RPC endpoint URL
    #[arg(long, default_value = "http://localhost:8545")]
    pub rpc: String,

    /// Target transactions per second (0 = unlimited)
    #[arg(long, default_value = "0")]
    pub tps: u64,

    /// Benchmark duration
    #[arg(long, value_parser = parse_duration)]
    pub duration: Option<Duration>,

    /// Number of transactions to generate (alternative to duration)
    #[arg(short = 'n', long)]
    pub count: Option<u64>,

    /// Report output destinations (can be specified multiple times)
    /// Format: console, json:<path>, clickhouse:<url>
    #[arg(long = "report", value_name = "FORMAT")]
    pub reports: Vec<String>,

    /// Maximum concurrent requests
    #[arg(long, default_value = "100")]
    pub max_concurrent: usize,

    /// Request timeout
    #[arg(long, default_value = "30s", value_parser = parse_duration)]
    pub timeout: Duration,

    /// RNG seed for reproducibility
    #[arg(long)]
    pub seed: Option<u64>,
}

/// Arguments for the `send` subcommand.
#[derive(Args)]
pub struct SendArgs {
    /// Input file (NDJSON). If not specified, reads from stdin.
    #[arg(short, long)]
    pub input: Option<PathBuf>,

    /// RPC endpoint URL
    #[arg(long, default_value = "http://localhost:8545")]
    pub rpc: String,

    /// Target transactions per second (0 = unlimited)
    #[arg(long, default_value = "0")]
    pub tps: u64,

    /// Maximum concurrent requests
    #[arg(long, default_value = "100")]
    pub max_concurrent: usize,

    /// Request timeout
    #[arg(long, default_value = "30s", value_parser = parse_duration)]
    pub timeout: Duration,

    /// Report output destinations
    #[arg(long = "report", value_name = "FORMAT")]
    pub reports: Vec<String>,
}

/// Arguments for the `replay` subcommand.
#[derive(Args)]
pub struct ReplayArgs {
    /// Source RPC endpoint (archive node) for fetching block data
    #[arg(long)]
    pub rpc_source: String,

    /// Engine API endpoint
    #[arg(long)]
    pub engine: String,

    /// Path to JWT secret file
    #[arg(long)]
    pub jwt_secret: PathBuf,

    /// Starting block number
    #[arg(long)]
    pub from: u64,

    /// Ending block number
    #[arg(long)]
    pub to: u64,

    /// Report output destinations
    #[arg(long = "report", value_name = "FORMAT")]
    pub reports: Vec<String>,
}

/// Arguments for the `plot` subcommand.
#[derive(Args)]
pub struct PlotArgs {
    /// Input JSON report file
    #[arg(short, long)]
    pub input: PathBuf,

    /// Output directory for PNG files
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Type of plot to generate
    #[arg(short = 't', long, default_value = "all")]
    pub plot_type: plot::PlotType,

    /// Chart width in pixels
    #[arg(long, default_value = "1200")]
    pub width: u32,

    /// Chart height in pixels
    #[arg(long, default_value = "600")]
    pub height: u32,
}

/// Supported chain types.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ChainType {
    Ethereum,
    Tempo,
}

fn parse_duration(s: &str) -> Result<Duration, humantime::DurationError> {
    humantime::parse_duration(s)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Run(args) => run::execute(args).await,
        Command::Send(args) => send::execute(args).await,
        Command::Replay(args) => replay::execute(args).await,
        Command::Plot(args) => plot::execute(args),
    }
}
