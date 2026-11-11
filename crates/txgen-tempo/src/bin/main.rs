use clap::{Parser, Subcommand};
use eyre::Result;
use txgen_tempo::{
    auth_token_map::{run_auth_token_map, AuthTokenMapArgs},
    fund::{run_fund, FundArgs},
    TempoAdapter,
};

#[derive(Parser)]
#[command(name = "txgen-tempo", about = "Tempo transaction generator")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(flatten)]
    Txgen(txgen_cli::Command),
    /// Generate or refresh a Tempo Zone private-RPC authorization-token map
    AuthTokenMap(AuthTokenMapArgs),
    /// Fund the spec's accounts with the Tempo faucet, verify their balances and refund the ones
    /// still short
    Fund(FundArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Txgen(command) => txgen_cli::run_command(TempoAdapter::new(), command).await,
        Command::AuthTokenMap(args) => run_auth_token_map(args).await,
        Command::Fund(args) => run_fund(args).await,
    }
}
