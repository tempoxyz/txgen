//! Reporter, sample archive, scraper and forwarder lifecycle shared by all commands.

use crate::{
    load_metric_names,
    metrics_forwarder::{build_metrics_forwarder, finish_metrics_forwarder, push_samples},
    metrics_url::metrics_scraper_configs,
    ReportArgs,
};
use bench_core::{
    parse_reporters, start_scrapers, BlockStats, ConsoleReporter, FinalReport, PrometheusForwarder,
    Reporter, RunClock, Sample, SampleCallback, SampleStore, ScraperConfig, ScraperHandle,
};
use eyre::Result;
use std::{collections::HashMap, time::Duration};

impl ReportArgs {
    pub(crate) fn scraper_configs(&self) -> Result<Vec<ScraperConfig>> {
        metrics_scraper_configs(&self.metrics_url, Duration::from_millis(self.scrape_interval_ms))
    }
}

/// Reporters plus the running metrics pipeline for one benchmark run.
pub(crate) struct Reporting {
    pub(crate) reporters: Vec<Box<dyn Reporter>>,
    pub(crate) store: SampleStore,
    forwarder: Option<PrometheusForwarder>,
    scrapers: Vec<ScraperHandle>,
}

impl Reporting {
    /// Build reporters (console when none are configured) and start scraping.
    pub(crate) fn start(
        args: &ReportArgs,
        command: &str,
        metadata: &HashMap<String, String>,
        scraper_configs: &[ScraperConfig],
        clock: &RunClock,
        snapshot: SampleCallback,
        console_progress: bool,
    ) -> Result<Self> {
        let clickhouse_metric_names = load_metric_names(args.clickhouse_metrics_file.as_ref())?;
        let mut reporters =
            parse_reporters(&args.reports, command, metadata, clickhouse_metric_names)?;
        if reporters.is_empty() {
            reporters.push(Box::new(ConsoleReporter::stderr(console_progress)));
        }
        let store = SampleStore::with_labels(metadata.clone())?;
        let forwarder = build_metrics_forwarder(args.metrics_forward.as_deref(), metadata)?;
        let scrapers = if scraper_configs.is_empty() {
            Vec::new()
        } else {
            let forwarder_handle = forwarder.as_ref().map(|forwarder| forwarder.handle());
            start_scrapers(
                scraper_configs,
                clock.clone(),
                store.clone(),
                snapshot,
                forwarder_handle,
            )
        };
        Ok(Self { reporters, store, forwarder, scrapers })
    }

    /// Record samples in the archive and forward them.
    pub(crate) async fn push_samples(&self, samples: Vec<Sample>) -> Result<()> {
        let forwarder_handle = self.forwarder.as_ref().map(|forwarder| forwarder.handle());
        push_samples(&self.store, forwarder_handle.as_ref(), samples).await
    }

    /// Stop the metrics scrapers. Later calls do nothing.
    pub(crate) async fn stop_scrapers(&mut self) {
        if self.scrapers.is_empty() {
            return;
        }
        let scrapers = std::mem::take(&mut self.scrapers);
        let scrapes = scrapers.iter().map(|h| h.scrape_count()).sum::<u64>();
        let errors = scrapers.iter().map(|h| h.error_count()).sum::<u64>();
        let count = scrapers.len();
        for handle in scrapers {
            handle.stop().await;
        }
        tracing::info!(scrapers = count, scrapes, errors, "Metrics scrapers stopped");
    }

    pub(crate) fn on_blocks(&mut self, blocks: &[BlockStats]) -> Result<()> {
        for block in blocks {
            for reporter in &mut self.reporters {
                reporter.on_block(block)?;
            }
        }
        Ok(())
    }

    /// Finalize every reporter, then flush the metrics forwarder.
    pub(crate) async fn finish(mut self, report: &FinalReport) -> Result<()> {
        self.stop_scrapers().await;
        let finalize_result =
            self.reporters.iter_mut().try_for_each(|reporter| reporter.finalize(report));
        let forwarder_result = finish_metrics_forwarder(self.forwarder).await;
        finalize_result?;
        forwarder_result
    }
}
