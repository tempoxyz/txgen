use alloy_provider::{ProviderBuilder, network::Ethereum};
use eyre::{Result, WrapErr, bail};
use std::io::Write;
use txgen_cli::{GenerateArgs, GenerateContext, TxgenNetwork};
use txgen_core::{BuildContext, NdjsonWriter, NonceProvider, WorkloadSpec};
use txgen_tempo::{TempoNonceProvider, TempoPlugin, TempoTemplate};

struct TempoNetwork;

impl TxgenNetwork for TempoNetwork {
    async fn generate(&self, args: GenerateArgs) -> Result<()> {
        let nonce_provider = args.rpc.as_ref().map(|rpc_url| {
            let provider = ProviderBuilder::<_, _, Ethereum>::new()
                // SAFETY: expect is fine here — CLI validation ensures valid URL
                .connect_http(rpc_url.parse().expect("invalid RPC URL"));
            if args.rpc_rps > 0 {
                TempoNonceProvider::with_rate_limit(provider, args.rpc_rps)
            } else {
                TempoNonceProvider::new(provider)
            }
        });

        let count = args.count;
        let output = args.output.clone();

        let GenerateContext {
            spec,
            accounts,
            artifacts,
            gas,
            mut nonces,
            mut rng,
        } = GenerateContext::from_args(&args)?;

        let plugin = TempoPlugin::default();
        let total_weight = spec.total_weight();
        if total_weight == 0 {
            bail!("no templates in mix (total weight is 0)");
        }

        let mut build_ctx = BuildContext::new(
            spec.chain_id,
            &gas,
            &accounts,
            &artifacts,
            &mut nonces,
            &mut rng,
        );

        match output {
            Some(path) => {
                let mut writer = txgen_core::output::file_writer(&path)?;
                generate_tempo_txs(
                    &plugin,
                    &spec,
                    count,
                    total_weight,
                    &mut build_ctx,
                    &mut writer,
                    nonce_provider.as_ref(),
                )
                .await?;
                eprintln!("wrote {} transactions to {}", count, path.display());
            }
            None => {
                let mut writer = txgen_core::output::stdout_writer();
                generate_tempo_txs(
                    &plugin,
                    &spec,
                    count,
                    total_weight,
                    &mut build_ctx,
                    &mut writer,
                    nonce_provider.as_ref(),
                )
                .await?;
            }
        }

        Ok(())
    }
}

async fn generate_tempo_txs<W: Write, P: NonceProvider>(
    plugin: &TempoPlugin,
    spec: &WorkloadSpec,
    count: u64,
    total_weight: u64,
    ctx: &mut BuildContext<'_>,
    writer: &mut NdjsonWriter<W>,
    nonce_provider: Option<&P>,
) -> Result<()> {
    let start = std::time::Instant::now();
    let mut last_log = start;

    for i in 0..count {
        let template_name = txgen_cli::pick_template(spec, ctx.rng, total_weight)?;
        let template_value = spec
            .templates
            .get(&template_name)
            .ok_or_else(|| eyre::eyre!("template '{}' not found", template_name))?;

        let template: TempoTemplate = serde_yaml::from_value(template_value.clone())
            .wrap_err_with(|| format!("failed to parse template '{}'", template_name))?;

        let tx = plugin
            .build_with_nonce_provider(template, ctx, nonce_provider)
            .await
            .wrap_err_with(|| format!("failed to build tx from template '{}'", template_name))?;

        writer.write(&tx)?;

        let now = std::time::Instant::now();
        if (i + 1) % 10000 == 0 || now.duration_since(last_log).as_secs() >= 5 {
            let elapsed = now.duration_since(start).as_secs_f64();
            let tps = (i + 1) as f64 / elapsed;
            eprintln!(
                "generated {}/{} txs ({:.1}%) - {:.0} tx/s",
                i + 1,
                count,
                (i + 1) as f64 / count as f64 * 100.0,
                tps
            );
            last_log = now;
        }
    }
    writer.flush()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    txgen_cli::run(TempoNetwork).await
}
