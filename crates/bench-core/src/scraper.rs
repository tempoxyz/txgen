//! Background Prometheus metrics scraper.
//!
//! Periodically fetches metrics from a node's `/metrics` endpoint,
//! parses them, and writes [`Sample`]s into a shared [`SampleStore`].

use crate::{
    clock::RunClock,
    prometheus::parse_prometheus_text,
    sample::{Sample, SampleStore},
};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::watch;

/// Callback that produces additional samples on each scrape tick.
///
/// Called at the same offset as the Prometheus scrape so that internal
/// and node metrics share the same timestamp.
pub type SampleCallback = Arc<dyn Fn() -> Vec<Sample> + Send + Sync>;

/// Configuration for the background scraper.
#[derive(Debug, Clone)]
pub struct ScraperConfig {
    /// URL of the Prometheus metrics endpoint (e.g. `http://127.0.0.1:9001/metrics`).
    pub url: String,
    /// Scrape interval.
    pub interval: Duration,
    /// HTTP request timeout per scrape.
    pub timeout: Duration,
}

impl ScraperConfig {
    /// Create a config with default interval (500ms) and timeout (2s).
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            interval: Duration::from_millis(500),
            timeout: Duration::from_secs(2),
        }
    }

    /// Set the scrape interval.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Set the HTTP timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

/// Handle returned by [`start_scraper`]. Stops the scraper on drop.
pub struct ScraperHandle {
    stop_tx: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<()>,
    scrape_count: Arc<AtomicU64>,
    error_count: Arc<AtomicU64>,
}

impl ScraperHandle {
    /// Stop the scraper and wait for it to finish.
    pub async fn stop(self) {
        let _ = self.stop_tx.send(true);
        let _ = self.handle.await;
    }

    /// Number of successful scrapes so far.
    pub fn scrape_count(&self) -> u64 {
        self.scrape_count.load(Ordering::Relaxed)
    }

    /// Number of failed scrapes so far.
    pub fn error_count(&self) -> u64 {
        self.error_count.load(Ordering::Relaxed)
    }
}

/// Start a background scraper task.
///
/// Returns a [`ScraperHandle`] to stop the scraper and query stats.
/// Scrape failures are logged but never propagated — they do not
/// affect the benchmark.
///
/// An optional `extra_samples` callback is invoked on every tick at the
/// same offset as the Prometheus scrape, ensuring internal and node
/// metrics share identical timestamps.
pub fn start_scraper(
    config: ScraperConfig,
    clock: RunClock,
    store: SampleStore,
    extra_samples: Option<SampleCallback>,
) -> ScraperHandle {
    let (stop_tx, stop_rx) = watch::channel(false);
    let scrape_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));

    let handle = tokio::spawn(scraper_loop(
        config,
        clock,
        store,
        extra_samples,
        stop_rx,
        scrape_count.clone(),
        error_count.clone(),
    ));

    ScraperHandle { stop_tx, handle, scrape_count, error_count }
}

async fn scraper_loop(
    config: ScraperConfig,
    clock: RunClock,
    store: SampleStore,
    extra_samples: Option<SampleCallback>,
    mut stop_rx: watch::Receiver<bool>,
    scrape_count: Arc<AtomicU64>,
    error_count: Arc<AtomicU64>,
) {
    let client = reqwest::Client::builder().timeout(config.timeout).build().unwrap_or_default();

    let mut interval = tokio::time::interval(config.interval);
    // Don't try to catch up if a scrape takes longer than the interval.
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            result = stop_rx.changed() => {
                if result.is_err() || *stop_rx.borrow() {
                    break;
                }
            }
        }

        // Capture timestamps once so all samples from this tick share them.
        let offset_ms = clock.offset_ms();
        let unix_ms = clock.unix_ms();

        // Collect extra samples (e.g. internal metrics) at the same offset.
        if let Some(ref cb) = extra_samples {
            let extra = cb();
            if !extra.is_empty() {
                store.push_batch(extra).await;
            }
        }

        match client.get(&config.url).send().await {
            Ok(resp) => match resp.text().await {
                Ok(text) => {
                    let samples = parse_prometheus_text(&text, offset_ms, unix_ms);
                    if !samples.is_empty() {
                        store.push_batch(samples).await;
                    }
                    scrape_count.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    tracing::debug!(error = %e, url = %config.url, "Failed to read metrics response body");
                    error_count.fetch_add(1, Ordering::Relaxed);
                }
            },
            Err(e) => {
                tracing::debug!(error = %e, url = %config.url, "Failed to scrape metrics");
                error_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}
