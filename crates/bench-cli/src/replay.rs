//! `bench replay` - Convenience wrapper for extract + send-blocks
//!
//! Fetches raw RLP-encoded blocks from an archive node and pipes them
//! directly into the `send-blocks` submission pipeline. This is equivalent
//! to `txgen extract ... | bench send-blocks ...` but avoids the
//! NDJSON serialization/deserialization overhead.

use crate::ReplayArgs;
use crate::send::parse_metadata;
use crate::send_blocks;
use alloy_network::Ethereum;
use alloy_primitives::Bytes;
use alloy_provider::{Provider, ProviderBuilder, RootProvider};
use alloy_rpc_types_engine::JwtSecret;
use alloy_transport_http::{AuthLayer, Http, HyperClient};
use bench_core::{
    ConsoleReporter, FinalReport, Reporter, RunClock, SampleStore, ScraperConfig,
    WaitForPersistence, parse_reporters, start_scraper,
};
use eyre::{Context, Result, bail};
use std::time::Duration;
use tokio::sync::mpsc;

/// Number of blocks to prefetch ahead.
const DEFAULT_PREFETCH_SIZE: usize = 20;

pub async fn execute(args: ReplayArgs) -> Result<()> {
    if args.from > args.to {
        bail!("--from must be <= --to");
    }

    let metadata = parse_metadata(&args.metadata)?;

    let jwt_secret_hex = tokio::fs::read_to_string(&args.jwt_secret)
        .await
        .wrap_err("failed to read JWT secret")?;
    let jwt_secret =
        JwtSecret::from_hex(jwt_secret_hex.trim()).wrap_err("invalid JWT secret hex")?;

    tracing::info!(
        rpc_source = %args.rpc_source,
        engine = %args.engine,
        from = args.from,
        to = args.to,
        "Starting block replay"
    );

    let source_provider = ProviderBuilder::new()
        .connect(&args.rpc_source)
        .await
        .wrap_err("failed to connect to source RPC")?;

    let hyper_client = HyperClient::new().layer(AuthLayer::new(jwt_secret));
    let transport = Http::with_client(hyper_client, args.engine.parse()?);
    let engine_provider =
        RootProvider::<Ethereum>::new(alloy_rpc_client::RpcClient::new(transport, true));

    let mut reporters = parse_reporters(&args.reports, "replay", &metadata)?;
    let mut console_reporter: Box<dyn Reporter> = Box::new(ConsoleReporter::stderr(false));

    let clock = RunClock::new();
    let store = SampleStore::new();

    // Start background scraper if metrics URL is configured.
    let scraper_handle = if let Some(ref url) = args.metrics_url {
        let scraper_config =
            ScraperConfig::new(url).with_interval(Duration::from_millis(args.scrape_interval_ms));
        let handle = start_scraper(scraper_config, clock.clone(), store.clone(), None);
        tracing::info!(url, "Started metrics scraper");
        Some(handle)
    } else {
        None
    };

    // Fetch blocks in background (same pattern as `txgen extract`)
    let (tx, mut rx) = mpsc::channel::<Result<Bytes>>(DEFAULT_PREFETCH_SIZE);
    let fetch_handle = tokio::spawn(async move {
        fetch_blocks(source_provider, args.from, args.to, tx).await;
    });

    // Process blocks through the send-blocks pipeline
    let mut collector = send_blocks::MetricsCollector::default();
    let mut prev_block_hash = None;
    let mut finalized_hash = None;
    let persistence_policy = WaitForPersistence::Always;

    while let Some(result) = rx.recv().await {
        let rlp_bytes = result?;
        let meta = send_blocks::decode_block_meta(&rlp_bytes)?;

        send_blocks::process_block(
            &engine_provider,
            rlp_bytes,
            &meta,
            &mut collector,
            &mut prev_block_hash,
            &mut finalized_hash,
            &persistence_policy,
        )
        .await?;
    }

    fetch_handle.await?;

    // Stop the scraper before finalizing.
    if let Some(handle) = scraper_handle {
        tracing::info!(
            scrapes = handle.scrape_count(),
            errors = handle.error_count(),
            "Stopping metrics scraper"
        );
        handle.stop().await;
    }

    let samples = store.drain().await;

    let mut report = FinalReport {
        metadata: metadata.clone(),
        samples,
        ..Default::default()
    };

    report.apply_labels(&metadata);

    for reporter in reporters.iter_mut() {
        reporter.finalize(&report)?;
    }
    console_reporter.finalize(&report)?;

    Ok(())
}

/// Fetch raw RLP-encoded blocks from source provider and send to channel.
async fn fetch_blocks<P: Provider>(
    provider: P,
    from: u64,
    to: u64,
    tx: mpsc::Sender<Result<Bytes>>,
) {
    for block_num in from..=to {
        let result = async {
            let rlp_bytes: Bytes = provider
                .raw_request("debug_getRawBlock".into(), (format!("0x{block_num:x}"),))
                .await
                .wrap_err_with(|| format!("failed to fetch raw block {block_num}"))?;
            Ok(rlp_bytes)
        }
        .await;

        let is_err = result.is_err();
        if tx.send(result).await.is_err() {
            tracing::debug!("Channel closed, stopping fetcher");
            break;
        }
        if is_err {
            break;
        }
    }
}
