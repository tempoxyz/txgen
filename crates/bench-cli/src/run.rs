//! `bench run` - All-in-one: generate + send + report

use crate::{ChainType, RunArgs};
use bench_core::{
    ConsoleReporter, MetricsCollector, Sender, SenderConfig, TxSource, TxgenSource,
    parse_reporters,
};
use eyre::{Context, Result, bail};
use std::time::Instant;

pub async fn execute(args: RunArgs) -> Result<()> {
    if args.duration.is_none() && args.count.is_none() {
        bail!("must specify either --duration or --count");
    }

    let spec_path = args
        .spec
        .canonicalize()
        .wrap_err("failed to resolve spec path")?;

    let chain = match args.chain {
        ChainType::Ethereum => "ethereum",
        ChainType::Tempo => "tempo",
    };

    let mut txgen_args = vec![
        "generate".to_string(),
        "--spec".to_string(),
        spec_path.display().to_string(),
        "--chain".to_string(),
        chain.to_string(),
    ];

    if let Some(count) = args.count {
        txgen_args.push("--count".to_string());
        txgen_args.push(count.to_string());
    } else if let Some(duration) = args.duration {
        // For duration-based, we generate a large number and stop when time is up.
        // Use TPS as a hint for how many to generate.
        let estimate = if args.tps > 0 {
            args.tps * duration.as_secs() * 2
        } else {
            1_000_000
        };
        txgen_args.push("--count".to_string());
        txgen_args.push(estimate.to_string());
    }

    if let Some(seed) = args.seed {
        txgen_args.push("--seed".to_string());
        txgen_args.push(seed.to_string());
    }

    tracing::info!(
        spec = %spec_path.display(),
        chain = chain,
        tps = args.tps,
        "Starting benchmark"
    );

    let mut source = TxgenSource::spawn("txgen", &txgen_args)
        .await
        .wrap_err("failed to spawn txgen")?;

    let metrics = MetricsCollector::new();
    let config = SenderConfig {
        rpc_url: args.rpc.clone(),
        rate_limit: args.tps,
        max_concurrent: args.max_concurrent,
        timeout: args.timeout,
    };
    let mut sender = Sender::new(config, metrics.clone())?;

    let mut reporters = parse_reporters(&args.reports)?;
    if reporters.is_empty() {
        reporters.push(Box::new(ConsoleReporter::stderr(true)));
    }

    metrics.start().await;
    let start = Instant::now();
    let deadline = args.duration.map(|d| start + d);

    loop {
        if let Some(d) = deadline {
            if Instant::now() >= d {
                tracing::info!("Duration reached, stopping");
                break;
            }
        }

        match source.next_tx().await? {
            Some(tx) => {
                sender.send(tx).await?;

                let (sent, success, failed) = metrics.counts();
                for reporter in &mut reporters {
                    reporter.on_progress(sent, success, failed)?;
                }
            }
            None => {
                tracing::info!("Source exhausted");
                break;
            }
        }
    }

    sender.flush().await;
    source.wait().await?;

    let final_metrics = metrics.finalize().await;
    let time_series = metrics.time_series().await;

    for reporter in &mut reporters {
        reporter.finalize(&final_metrics, Some(&time_series), None)?;
    }

    Ok(())
}
