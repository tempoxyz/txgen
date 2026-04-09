//! Unified metric sample type and store.
//!
//! Both internal benchmark metrics and scraped node Prometheus metrics
//! are stored as [`Sample`]s in a shared [`SampleStore`].

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A single metric data point.
///
/// This is the unified shape for all metrics — both txgen internal
/// counters (e.g. `txgen_transactions_sent_total`) and scraped node
/// Prometheus metrics (e.g. `reth_jemalloc_resident_bytes`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    /// Metric name (e.g. `txgen_transactions_sent_total`).
    pub name: String,
    /// Label key-value pairs. Ordered for deterministic serialization.
    pub labels: BTreeMap<String, String>,
    /// Metric value.
    pub value: f64,
    /// Monotonic offset in milliseconds since benchmark start ([`RunClock::offset_ms`]).
    pub offset_ms: u64,
    /// Wall-clock time in Unix milliseconds ([`RunClock::unix_ms`]).
    pub unix_ms: u64,
}

/// An append-only, thread-safe store for metric samples.
///
/// Shared between the internal metrics snapshotter and the Prometheus
/// scraper via `Arc`. Reporters consume samples at finalization time.
#[derive(Debug, Clone)]
pub struct SampleStore {
    inner: Arc<RwLock<Vec<Sample>>>,
}

impl SampleStore {
    /// Create a new empty store.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Append a batch of samples.
    pub async fn push_batch(&self, samples: Vec<Sample>) {
        self.inner.write().await.extend(samples);
    }

    /// Clone all stored samples for reporters to consume.
    pub async fn snapshot(&self) -> Vec<Sample> {
        self.inner.read().await.clone()
    }

    /// Drain all stored samples, leaving the store empty.
    pub async fn drain(&self) -> Vec<Sample> {
        std::mem::take(&mut *self.inner.write().await)
    }

    /// Number of samples currently stored.
    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }

    /// Whether the store is empty.
    pub async fn is_empty(&self) -> bool {
        self.inner.read().await.is_empty()
    }
}

impl Default for SampleStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sample(name: &str, value: f64, offset_ms: u64) -> Sample {
        Sample {
            name: name.to_string(),
            labels: BTreeMap::new(),
            value,
            offset_ms,
            unix_ms: 1_700_000_000_000 + offset_ms,
        }
    }

    #[tokio::test]
    async fn push_and_snapshot() {
        let store = SampleStore::new();
        assert!(store.is_empty().await);

        store
            .push_batch(vec![make_sample("a", 1.0, 0), make_sample("b", 2.0, 100)])
            .await;

        assert_eq!(store.len().await, 2);

        let snap = store.snapshot().await;
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].name, "a");
        assert_eq!(snap[1].value, 2.0);

        // Snapshot is non-destructive.
        assert_eq!(store.len().await, 2);
    }

    #[tokio::test]
    async fn drain_empties_store() {
        let store = SampleStore::new();
        store.push_batch(vec![make_sample("x", 42.0, 0)]).await;

        let drained = store.drain().await;
        assert_eq!(drained.len(), 1);
        assert!(store.is_empty().await);
    }

    #[tokio::test]
    async fn clone_shares_data() {
        let store = SampleStore::new();
        let clone = store.clone();

        store.push_batch(vec![make_sample("shared", 1.0, 0)]).await;

        assert_eq!(clone.len().await, 1);
    }

    #[test]
    fn sample_serde_roundtrip() {
        let sample = Sample {
            name: "test_metric".to_string(),
            labels: BTreeMap::from([("host".to_string(), "node-1".to_string())]),
            value: 3.125,
            offset_ms: 500,
            unix_ms: 1_700_000_000_500,
        };

        let json = serde_json::to_string(&sample).unwrap();
        let parsed: Sample = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.name, "test_metric");
        assert_eq!(parsed.labels["host"], "node-1");
        assert!((parsed.value - 3.125).abs() < f64::EPSILON);
        assert_eq!(parsed.offset_ms, 500);
    }
}
