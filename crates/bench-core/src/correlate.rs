//! Sample-to-block correlation.
//!
//! Correlates scraped [`Sample`]s to blocks by chain height, producing
//! per-block metric snapshots for ClickHouse storage.
//!
//! The correlation model matches `plot.py`: samples are grouped by scrape
//! offset, the chain height is read from each scrape
//! (`reth_blockchain_tree_canonical_chain_height`), and the last scrape
//! per block height is kept. This ensures every block with at least one
//! scrape gets a full metric snapshot.

use crate::sample::Sample;
use crate::timeline::BlockMarker;
use serde::Serialize;
use std::collections::BTreeMap;

/// The Prometheus metric name that reports the current canonical chain height.
const HEIGHT_METRIC: &str = "reth_blockchain_tree_canonical_chain_height";

/// A block with its correlated metric snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct CorrelatedBlock {
    /// 0-based position within the run.
    pub block_index: u32,
    /// Chain block number.
    pub block_number: u64,
    /// Block timestamp (unix seconds).
    pub chain_timestamp: Option<u64>,
    /// Whether the window is precise (replay) or observed (send).
    pub window_kind: WindowKind,
    /// Window start offset in ms from run start.
    pub window_start_offset_ms: u64,
    /// Window end offset in ms from run start.
    pub window_end_offset_ms: u64,
    /// Metric snapshot for this block (last scrape at this height).
    pub metrics: Vec<BlockMetricAggregate>,
}

/// Whether the block timing window is precisely known or approximately
/// observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WindowKind {
    /// Replay/send-blocks mode: exact `[offset_ms, fcu_done_offset_ms]`.
    Precise,
    /// Send mode: approximate `[prev_marker.offset_ms, marker.offset_ms]`.
    Observed,
}

impl WindowKind {
    /// String representation for ClickHouse storage.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Precise => "precise",
            Self::Observed => "observed",
        }
    }
}

/// A single metric value from the last scrape at a given block height.
#[derive(Debug, Clone, Serialize)]
pub struct BlockMetricAggregate {
    /// Metric name (e.g. `reth_jemalloc_resident_bytes`).
    pub metric_name: String,
    /// Canonical JSON of label map.
    pub labels_json: String,
    /// Source of the metric.
    pub source: MetricSource,
    /// Always 1 for height-based correlation.
    pub sample_count: u16,
    /// The metric value from the last scrape at this block height.
    pub first_value: f64,
    /// Same as `first_value` (single snapshot).
    pub last_value: f64,
    /// Same as `first_value` (single snapshot).
    pub min_value: f64,
    /// Same as `first_value` (single snapshot).
    pub max_value: f64,
    /// Same as `first_value` (single snapshot).
    pub avg_value: f64,
    /// Difference from the previous block's value (`current - previous`).
    /// `None` for the first block or if the metric wasn't present in the
    /// previous block's snapshot. Useful for cumulative counters and
    /// histogram `_sum`/`_count` metrics.
    pub delta_value: Option<f64>,
}

/// Source of a metric sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricSource {
    /// Scraped from node Prometheus endpoint.
    Prometheus,
    /// Internal txgen metric.
    Txgen,
}

impl MetricSource {
    /// String representation for ClickHouse storage.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Prometheus => "prometheus",
            Self::Txgen => "txgen",
        }
    }
}

/// Key for grouping samples by metric series.
type SeriesKey = (String, String);

/// A single scrape row: all metric values at one offset.
type ScrapeRow = BTreeMap<SeriesKey, f64>;

/// Group samples by scrape offset into rows, then deduplicate by block
/// height (keeping the last scrape per height), matching `plot.py`.
fn build_height_snapshots(samples: &[Sample]) -> BTreeMap<u64, ScrapeRow> {
    // Group all samples by offset_ms into scrape rows.
    let mut by_offset: BTreeMap<u64, ScrapeRow> = BTreeMap::new();
    for s in samples {
        let labels_json = serde_json::to_string(&s.labels).unwrap_or_default();
        let key = (s.name.clone(), labels_json);
        by_offset
            .entry(s.offset_ms)
            .or_default()
            .insert(key, s.value);
    }

    // For each scrape row, read the chain height and keep the last row per height.
    // The height metric may have injected metadata labels, so we match by name only.
    let mut by_height: BTreeMap<u64, ScrapeRow> = BTreeMap::new();

    for row in by_offset.values() {
        let height = row
            .iter()
            .find(|((name, _), _)| name == HEIGHT_METRIC)
            .map(|(_, &v)| v as u64);

        if let Some(height) = height {
            if height > 0 {
                by_height.insert(height, row.clone());
            }
        }
    }

    by_height
}

/// Convert a scrape row into a vec of [`BlockMetricAggregate`], computing
/// deltas from the previous block's snapshot.
fn row_to_aggregates(row: &ScrapeRow, prev_row: Option<&ScrapeRow>) -> Vec<BlockMetricAggregate> {
    row.iter()
        .map(|(key, &value)| {
            let (metric_name, labels_json) = key;
            let source = if metric_name.starts_with("txgen_") {
                MetricSource::Txgen
            } else {
                MetricSource::Prometheus
            };

            let delta_value = prev_row
                .and_then(|prev| prev.get(key))
                .map(|&prev_value| value - prev_value);

            BlockMetricAggregate {
                metric_name: metric_name.clone(),
                labels_json: labels_json.clone(),
                source,
                sample_count: 1,
                first_value: value,
                last_value: value,
                min_value: value,
                max_value: value,
                avg_value: value,
                delta_value,
            }
        })
        .collect()
}

/// Correlate samples to blocks using chain height from scraped metrics.
///
/// Groups samples by scrape offset, reads `reth_blockchain_tree_canonical_chain_height`
/// from each scrape, and keeps the last scrape per block height. Each block marker
/// is matched to its height snapshot to produce per-block metric data.
///
/// # Arguments
///
/// * `markers` — Block markers with timing information.
/// * `samples` — All collected samples (internal + scraped), sorted by offset.
/// * `precise` — Whether markers have precise windows (replay mode) or
///   approximate observed windows (send mode).
pub fn correlate_samples(
    markers: &[BlockMarker],
    samples: &[Sample],
    precise: bool,
) -> Vec<CorrelatedBlock> {
    if markers.is_empty() {
        return Vec::new();
    }

    let by_height = build_height_snapshots(samples);
    let mut results = Vec::with_capacity(markers.len());
    let mut prev_row: Option<&ScrapeRow> = None;

    for (idx, marker) in markers.iter().enumerate() {
        let (window_start, window_end, kind) = if precise {
            let start = marker.offset_ms;
            let end = marker
                .fcu_done_offset_ms
                .or(marker.new_payload_done_offset_ms)
                .unwrap_or(marker.offset_ms);
            (start, end, WindowKind::Precise)
        } else {
            let start = if idx > 0 {
                markers[idx - 1].offset_ms
            } else {
                0
            };
            let end = marker.offset_ms;
            (start, end, WindowKind::Observed)
        };

        let current_row = by_height.get(&marker.number);
        let metrics = current_row
            .map(|row| row_to_aggregates(row, prev_row))
            .unwrap_or_default();

        // Track previous row for inter-block deltas.
        if current_row.is_some() {
            prev_row = current_row;
        }

        results.push(CorrelatedBlock {
            block_index: idx as u32,
            block_number: marker.number,
            chain_timestamp: marker.chain_timestamp,
            window_kind: kind,
            window_start_offset_ms: window_start,
            window_end_offset_ms: window_end,
            metrics,
        });
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(name: &str, value: f64, offset_ms: u64) -> Sample {
        Sample {
            name: name.to_string(),
            labels: BTreeMap::new(),
            value,
            offset_ms,
            unix_ms: 1_700_000_000_000 + offset_ms,
        }
    }

    fn sample_with_labels(
        name: &str,
        value: f64,
        offset_ms: u64,
        labels: &[(&str, &str)],
    ) -> Sample {
        Sample {
            name: name.to_string(),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            value,
            offset_ms,
            unix_ms: 1_700_000_000_000 + offset_ms,
        }
    }

    fn marker(number: u64, offset_ms: u64) -> BlockMarker {
        BlockMarker {
            number,
            chain_timestamp: Some(1_700_000_000 + number),
            offset_ms,
            new_payload_done_offset_ms: None,
            fcu_done_offset_ms: None,
        }
    }

    fn replay_marker(number: u64, offset_ms: u64, np_done: u64, fcu_done: u64) -> BlockMarker {
        BlockMarker {
            number,
            chain_timestamp: Some(1_700_000_000 + number),
            offset_ms,
            new_payload_done_offset_ms: Some(np_done),
            fcu_done_offset_ms: Some(fcu_done),
        }
    }

    /// Build a set of samples that simulate a scrape at a given offset
    /// with a specific chain height, plus some metric values.
    fn scrape_at(offset_ms: u64, height: u64, metrics: &[(&str, f64)]) -> Vec<Sample> {
        let mut samples = vec![sample(HEIGHT_METRIC, height as f64, offset_ms)];
        for &(name, value) in metrics {
            samples.push(sample(name, value, offset_ms));
        }
        samples
    }

    #[test]
    fn empty_markers_returns_empty() {
        let result = correlate_samples(&[], &[], false);
        assert!(result.is_empty());
    }

    #[test]
    fn height_based_correlation() {
        let markers = vec![marker(100, 1000), marker(101, 2000), marker(102, 3000)];

        let mut samples = Vec::new();
        samples.extend(scrape_at(500, 100, &[("mem", 100.0)]));
        samples.extend(scrape_at(1500, 101, &[("mem", 200.0)]));
        samples.extend(scrape_at(2500, 102, &[("mem", 300.0)]));

        let blocks = correlate_samples(&markers, &samples, false);

        assert_eq!(blocks.len(), 3);

        // Each block gets its height-matched snapshot.
        let mem_val = |b: &CorrelatedBlock| -> f64 {
            b.metrics
                .iter()
                .find(|m| m.metric_name == "mem")
                .unwrap()
                .last_value
        };

        assert_eq!(mem_val(&blocks[0]), 100.0);
        assert_eq!(mem_val(&blocks[1]), 200.0);
        assert_eq!(mem_val(&blocks[2]), 300.0);
    }

    #[test]
    fn last_scrape_per_height_wins() {
        let markers = vec![marker(100, 2000)];

        // Two scrapes at height 100; second should win.
        let mut samples = Vec::new();
        samples.extend(scrape_at(500, 100, &[("mem", 100.0)]));
        samples.extend(scrape_at(1500, 100, &[("mem", 999.0)]));

        let blocks = correlate_samples(&markers, &samples, false);

        assert_eq!(blocks.len(), 1);
        let mem = blocks[0]
            .metrics
            .iter()
            .find(|m| m.metric_name == "mem")
            .unwrap();
        assert_eq!(mem.last_value, 999.0);
    }

    #[test]
    fn no_height_metric_means_no_metrics() {
        let markers = vec![marker(100, 1000)];
        // Samples without the height metric.
        let samples = vec![sample("mem", 100.0, 500)];

        let blocks = correlate_samples(&markers, &samples, false);

        assert_eq!(blocks.len(), 1);
        assert!(blocks[0].metrics.is_empty());
    }

    #[test]
    fn height_mismatch_means_no_metrics() {
        let markers = vec![marker(100, 1000)];
        // Scrape reports height 99, not 100.
        let samples = scrape_at(500, 99, &[("mem", 100.0)]);

        let blocks = correlate_samples(&markers, &samples, false);

        assert_eq!(blocks.len(), 1);
        assert!(blocks[0].metrics.is_empty());
    }

    #[test]
    fn precise_mode_preserves_windows() {
        let markers = vec![
            replay_marker(100, 1000, 1100, 1150),
            replay_marker(101, 1200, 1350, 1400),
        ];

        let mut samples = Vec::new();
        samples.extend(scrape_at(1050, 100, &[("cpu", 50.0)]));
        samples.extend(scrape_at(1250, 101, &[("cpu", 60.0)]));

        let blocks = correlate_samples(&markers, &samples, true);

        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].window_kind, WindowKind::Precise);
        assert_eq!(blocks[0].window_start_offset_ms, 1000);
        assert_eq!(blocks[0].window_end_offset_ms, 1150);

        let cpu_val = |b: &CorrelatedBlock| -> f64 {
            b.metrics
                .iter()
                .find(|m| m.metric_name == "cpu")
                .unwrap()
                .last_value
        };
        assert_eq!(cpu_val(&blocks[0]), 50.0);
        assert_eq!(cpu_val(&blocks[1]), 60.0);
    }

    #[test]
    fn groups_by_labels() {
        let markers = vec![marker(100, 1000)];

        let mut samples = vec![sample(HEIGHT_METRIC, 100.0, 500)];
        samples.push(sample_with_labels(
            "pool",
            10.0,
            500,
            &[("type", "pending")],
        ));
        samples.push(sample_with_labels("pool", 20.0, 500, &[("type", "queued")]));

        let blocks = correlate_samples(&markers, &samples, false);
        // height metric + 2 pool series = 3
        let pool_metrics: Vec<_> = blocks[0]
            .metrics
            .iter()
            .filter(|m| m.metric_name == "pool")
            .collect();
        assert_eq!(pool_metrics.len(), 2);
    }

    #[test]
    fn txgen_source_detected() {
        let markers = vec![marker(100, 1000)];
        let mut samples = scrape_at(500, 100, &[("txgen_sent_total", 100.0)]);
        samples.push(sample("reth_jemalloc_resident", 500.0, 500));

        let blocks = correlate_samples(&markers, &samples, false);
        let txgen_metric = blocks[0]
            .metrics
            .iter()
            .find(|m| m.metric_name == "txgen_sent_total")
            .unwrap();
        let prom_metric = blocks[0]
            .metrics
            .iter()
            .find(|m| m.metric_name == "reth_jemalloc_resident")
            .unwrap();

        assert_eq!(txgen_metric.source, MetricSource::Txgen);
        assert_eq!(prom_metric.source, MetricSource::Prometheus);
    }

    #[test]
    fn height_metric_with_injected_labels() {
        let markers = vec![marker(100, 1000)];

        // Height metric has injected metadata labels (like apply_labels does).
        let mut samples = vec![sample_with_labels(
            HEIGHT_METRIC,
            100.0,
            500,
            &[("scenario", "tip20-10k"), ("platform", "tempo")],
        )];
        samples.push(sample_with_labels(
            "mem",
            42.0,
            500,
            &[("scenario", "tip20-10k"), ("platform", "tempo")],
        ));

        let blocks = correlate_samples(&markers, &samples, false);

        assert_eq!(blocks.len(), 1);
        let mem = blocks[0]
            .metrics
            .iter()
            .find(|m| m.metric_name == "mem")
            .unwrap();
        assert_eq!(mem.last_value, 42.0);
    }

    #[test]
    fn inter_block_deltas() {
        let markers = vec![marker(100, 1000), marker(101, 2000), marker(102, 3000)];

        let mut samples = Vec::new();
        // Cumulative counter increases across blocks.
        samples.extend(scrape_at(500, 100, &[("counter", 10.0)]));
        samples.extend(scrape_at(1500, 101, &[("counter", 25.0)]));
        samples.extend(scrape_at(2500, 102, &[("counter", 50.0)]));

        let blocks = correlate_samples(&markers, &samples, false);

        let delta = |b: &CorrelatedBlock| -> Option<f64> {
            b.metrics
                .iter()
                .find(|m| m.metric_name == "counter")
                .and_then(|m| m.delta_value)
        };

        // First block has no previous, so delta is None.
        assert_eq!(delta(&blocks[0]), None);
        // Second block: 25 - 10 = 15.
        assert_eq!(delta(&blocks[1]), Some(15.0));
        // Third block: 50 - 25 = 25.
        assert_eq!(delta(&blocks[2]), Some(25.0));
    }

    #[test]
    fn sample_count_is_always_one() {
        let markers = vec![marker(100, 1000)];
        let samples = scrape_at(500, 100, &[("mem", 42.0)]);

        let blocks = correlate_samples(&markers, &samples, false);
        let mem = blocks[0]
            .metrics
            .iter()
            .find(|m| m.metric_name == "mem")
            .unwrap();
        assert_eq!(mem.sample_count, 1);
        assert_eq!(mem.delta_value, None);
    }
}
