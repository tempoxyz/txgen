use alloy_consensus::{BlockHeader, SignableTransaction, Signed, Transaction};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::Network;
use alloy_primitives::Sealable;
use alloy_rlp::Decodable;
use clap::{Parser, Subcommand};
use eyre::Result;

mod addresses;
mod bal;
mod extract;
mod generate;

use addresses::run_addresses;
pub use addresses::AddressesArgs;
use extract::{run_extract, run_extract_big_blocks};
pub use extract::{ExtractArgs, ExtractBigBlocksArgs};
use generate::run_generate;
pub use generate::{
    fetch_protocol_nonces, GenerateArgs, GenerateContext, NetworkAdapter, TxRequest,
};

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
    /// Generate synthetic big-block payloads from source blocks
    ExtractBigBlocks(ExtractBigBlocksArgs),
}

// ---------------------------------------------------------------------------
// Public entrypoint
// ---------------------------------------------------------------------------

pub async fn run<A: NetworkAdapter + 'static>(adapter: A) -> Result<()>
where
    <A::Network as Network>::TransactionRequest: Send + 'static,
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718 + Decodable + Transaction,
    <A::Network as Network>::Header: Decodable + BlockHeader + Sealable,
{
    let cli = Cli::parse();
    match cli.command {
        Command::Generate(args) => run_generate(adapter, args).await,
        Command::Addresses(args) => run_addresses(args),
        Command::Extract(args) => run_extract::<A::Network>(args).await,
        Command::ExtractBigBlocks(args) => run_extract_big_blocks::<A::Network>(args).await,
    }
}
