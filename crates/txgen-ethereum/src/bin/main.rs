use eyre::Result;
use txgen_ethereum::EthereumAdapter;

#[tokio::main]
async fn main() -> Result<()> {
    txgen_cli::run(EthereumAdapter).await
}
