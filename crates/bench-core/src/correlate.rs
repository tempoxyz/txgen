//! Sample-to-block correlation.
//!
//! Correlates scraped [`Sample`]s to blocks using [`BlockMarker`] time
//! windows, producing per-block metric aggregates for ClickHouse storage.

use crate::sample::Sample;
use crate::timeline::BlockMarker;
use serde::Serialize;
use std::collections::BTreeMap;

/// A block with its correlation window and associated metric aggregates.
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
    /// Aggregated metrics within this block's window.
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

/// Aggregated metric values for a single (metric_name, labels) series
/// within a block's time window.
#[derive(Debug, Clone, Serialize)]
pub struct BlockMetricAggregate {
    /// Metric name (e.g. `reth_jemalloc_resident_bytes`).
    pub metric_name: String,
    /// Canonical JSON of label map.
    pub labels_json: String,
    /// Source of the metric.
    pub source: MetricSource,
    /// Number of samples in the window.
    pub sample_count: u16,
    /// First sample value in the window.
    pub first_value: f64,
    /// Last sample value in the window.
    pub last_value: f64,
    /// Minimum value in the window.
    pub min_value: f64,
    /// Maximum value in the window.
    pub max_value: f64,
    /// Average value in the window.
    pub avg_value: f64,
    /// Delta (last - first), useful for counters.
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

/// Correlate samples to blocks using block marker time windows.
///
/// For each block marker, finds all samples within that block's time window
/// and computes per-metric aggregates (min, max, avg, delta, etc.).
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

    let mut results = Vec::with_capacity(markers.len());

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

        let metrics = aggregate_samples_in_window(samples, window_start, window_end);

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

/// Key for grouping samples by metric series.
type SeriesKey = (String, String);

/// Aggregate all samples within a time window, grouped by (metric_name, labels).
fn aggregate_samples_in_window(
    samples: &[Sample],
    window_start: u64,
    window_end: u64,
) -> Vec<BlockMetricAggregate> {
    // Group samples by (name, canonical labels json).
    let mut groups: BTreeMap<SeriesKey, Vec<f64>> = BTreeMap::new();

    for sample in samples {
        if sample.offset_ms >= window_start && sample.offset_ms <= window_end {
            let labels_json = serde_json::to_string(&sample.labels).unwrap_or_default();
            let key = (sample.name.clone(), labels_json);
            groups.entry(key).or_default().push(sample.value);
        }
    }

    groups
        .into_iter()
        .map(|((metric_name, labels_json), values)| {
            let count = values.len();
            let first = values[0];
            let last = values[count - 1];
            let min = values.iter().copied().fold(f64::INFINITY, f64::min);
            let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            let sum: f64 = values.iter().sum();
            let avg = sum / count as f64;
            let delta = if count > 1 { Some(last - first) } else { None };

            let source = if metric_name.starts_with("txgen_") {
                MetricSource::Txgen
            } else {
                MetricSource::Prometheus
            };

            BlockMetricAggregate {
                metric_name,
                labels_json,
                source,
                sample_count: count.min(u16::MAX as usize) as u16,
                first_value: first,
                last_value: last,
                min_value: min,
                max_value: max,
                avg_value: avg,
                delta_value: delta,
            }
        })
        .collect()
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

    #[test]
    fn empty_markers_returns_empty() {
        let result = correlate_samples(&[], &[], false);
        assert!(result.is_empty());
    }

    #[test]
    fn observed_mode_windows() {
        let markers = vec![marker(100, 1000), marker(101, 2000), marker(102, 3000)];
        let samples = vec![
            sample("mem", 100.0, 500),
            sample("mem", 200.0, 1500),
            sample("mem", 300.0, 2500),
        ];

        let blocks = correlate_samples(&markers, &samples, false);

        assert_eq!(blocks.len(), 3);

        // First block: window [0, 1000]
        assert_eq!(blocks[0].window_kind, WindowKind::Observed);
        assert_eq!(blocks[0].window_start_offset_ms, 0);
        assert_eq!(blocks[0].window_end_offset_ms, 1000);
        assert_eq!(blocks[0].metrics.len(), 1);
        assert_eq!(blocks[0].metrics[0].first_value, 100.0);

        // Second block: window [1000, 2000]
        assert_eq!(blocks[1].window_start_offset_ms, 1000);
        assert_eq!(blocks[1].window_end_offset_ms, 2000);
        assert_eq!(blocks[1].metrics.len(), 1);
        assert_eq!(blocks[1].metrics[0].first_value, 200.0);
    }

    #[test]
    fn precise_mode_windows() {
        let markers = vec![
            replay_marker(100, 1000, 1100, 1150),
            replay_marker(101, 1200, 1350, 1400),
        ];
        let samples = vec![
            sample("cpu", 50.0, 1050),
            sample("cpu", 60.0, 1250),
            sample("cpu", 70.0, 1300),
        ];

        let blocks = correlate_samples(&markers, &samples, true);

        assert_eq!(blocks.len(), 2);

        assert_eq!(blocks[0].window_kind, WindowKind::Precise);
        assert_eq!(blocks[0].window_start_offset_ms, 1000);
        assert_eq!(blocks[0].window_end_offset_ms, 1150);
        assert_eq!(blocks[0].metrics.len(), 1);
        assert_eq!(blocks[0].metrics[0].first_value, 50.0);

        assert_eq!(blocks[1].window_start_offset_ms, 1200);
        assert_eq!(blocks[1].window_end_offset_ms, 1400);
        assert_eq!(blocks[1].metrics.len(), 1);
        assert_eq!(blocks[1].metrics[0].sample_count, 2);
    }

    #[test]
    fn aggregates_multiple_samples() {
        let markers = vec![replay_marker(100, 0, 80, 100)];
        let samples = vec![
            sample("mem", 10.0, 0),
            sample("mem", 30.0, 50),
            sample("mem", 20.0, 100),
        ];

        let blocks = correlate_samples(&markers, &samples, true);
        let m = &blocks[0].metrics[0];

        assert_eq!(m.sample_count, 3);
        assert_eq!(m.first_value, 10.0);
        assert_eq!(m.last_value, 20.0);
        assert_eq!(m.min_value, 10.0);
        assert_eq!(m.max_value, 30.0);
        assert!((m.avg_value - 20.0).abs() < f64::EPSILON);
        assert_eq!(m.delta_value, Some(10.0));
    }

    #[test]
    fn groups_by_labels() {
        let markers = vec![replay_marker(100, 0, 50, 100)];
        let samples = vec![
            sample_with_labels("pool", 10.0, 50, &[("type", "pending")]),
            sample_with_labels("pool", 20.0, 50, &[("type", "queued")]),
        ];

        let blocks = correlate_samples(&markers, &samples, true);
        assert_eq!(blocks[0].metrics.len(), 2);
    }

    #[test]
    fn txgen_source_detected() {
        let markers = vec![replay_marker(100, 0, 50, 100)];
        let samples = vec![
            sample("txgen_sent_total", 100.0, 50),
            sample("reth_jemalloc_resident", 500.0, 50),
        ];

        let blocks = correlate_samples(&markers, &samples, true);
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
    fn single_sample_has_no_delta() {
        let markers = vec![replay_marker(100, 0, 50, 100)];
        let samples = vec![sample("mem", 42.0, 50)];

        let blocks = correlate_samples(&markers, &samples, true);
        assert_eq!(blocks[0].metrics[0].delta_value, None);
    }
}
