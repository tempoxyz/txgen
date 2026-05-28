use bench_core::{
    PrometheusConfig, PrometheusForwarder, PrometheusForwarderHandle, Sample, SampleStore,
    ScraperConfig,
};
use eyre::{bail, Context, Result};
use std::collections::HashMap;

pub(crate) fn build_metrics_forwarder(
    url: Option<&str>,
    metadata: &HashMap<String, String>,
    scraper_configs: &[ScraperConfig],
) -> Result<Option<PrometheusForwarder>> {
    let Some(url) = url else {
        return Ok(None);
    };

    if scraper_configs.is_empty() {
        bail!("--metrics-forward requires at least one --metrics-url");
    }

    let config = PrometheusConfig::from_metadata(url, metadata)?;
    PrometheusForwarder::spawn(config).map(Some).wrap_err("failed to create metrics forwarder")
}

pub(crate) async fn push_samples(
    store: &SampleStore,
    forwarder: Option<&PrometheusForwarderHandle>,
    samples: Vec<Sample>,
) -> Result<()> {
    let Some(forwarder) = forwarder else {
        store.push_batch(samples).await?;
        return Ok(());
    };

    let samples = store.push_batch_and_collect(samples).await?;
    forwarder.push_batch(samples).await?;
    Ok(())
}

pub(crate) async fn finish_metrics_forwarder(forwarder: Option<PrometheusForwarder>) -> Result<()> {
    if let Some(forwarder) = forwarder {
        let summary = forwarder.finish().await.wrap_err("metrics forwarding failed")?;
        tracing::info!(
            batches = summary.batches,
            samples = summary.samples,
            "Metrics forwarding complete"
        );
    }

    Ok(())
}
