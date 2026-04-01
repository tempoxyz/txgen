use eyre::Result;
use txgen_tempo::TempoAdapter;

#[tokio::main]
async fn main() -> Result<()> {
    txgen_cli::run(TempoAdapter).await
}
