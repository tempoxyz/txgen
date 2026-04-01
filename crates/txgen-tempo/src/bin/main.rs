use alloy_provider::{ProviderBuilder, network::Ethereum};
use eyre::{Result, WrapErr};
use txgen_cli::{GenerateArgs, GenerateContext, TxgenNetwork};
use txgen_tempo::TempoPlugin;

struct TempoNetwork;

impl TxgenNetwork for TempoNetwork {
    async fn generate(&self, args: GenerateArgs) -> Result<()> {
        let mut ctx = GenerateContext::from_args(&args)?;

        if let Some(ref rpc_url) = args.rpc {
            let provider = ProviderBuilder::<_, _, Ethereum>::new()
                .connect_http(rpc_url.parse().wrap_err("invalid RPC URL")?);

            let (accounts, nonces) = ctx.accounts_and_nonces();
            txgen_tempo::fetch_protocol_nonces(accounts, nonces, rpc_url).await?;

            let (spec, accounts, nonces) = ctx.prefetch_state();
            txgen_tempo::prefetch_parallel_nonces(&provider, accounts, spec, nonces).await?;
        }

        txgen_cli::generate_with_plugin(TempoPlugin::default(), &mut ctx, args.count, args.output)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    txgen_cli::run(TempoNetwork).await
}
