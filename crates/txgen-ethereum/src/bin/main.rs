use eyre::Result;
use txgen_cli::{GenerateArgs, GenerateContext, TxgenNetwork};
use txgen_ethereum::EthereumPlugin;

struct EthereumNetwork;

impl TxgenNetwork for EthereumNetwork {
    async fn generate(&self, args: GenerateArgs) -> Result<()> {
        let mut ctx = GenerateContext::from_args(&args)?;

        if let Some(ref rpc_url) = args.rpc {
            let (accounts, nonces) = ctx.accounts_and_nonces();
            txgen_ethereum::fetch_protocol_nonces(accounts, nonces, rpc_url).await?;
        }

        txgen_cli::generate_with_plugin(EthereumPlugin, &mut ctx, args.count, args.output)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    txgen_cli::run(EthereumNetwork).await
}
