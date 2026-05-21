//! Unified metric sample type and disk-backed store.
//!
//! Both internal benchmark metrics and scraped node Prometheus metrics are
//! streamed into an uncompressed NDJSON file. Reporters read the file back in
//! batches at finalization time, which avoids retaining all metric samples in
//! memory for long benchmark runs. File JSON reports compress the archive only
//! when writing the final sidecar.

use eyre::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::{BufRead, BufReader, BufWriter, Lines, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

/// A single metric data point.
///
/// This is the unified shape for all metrics -- both txgen internal
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

/// A finalized NDJSON sample archive.
#[derive(Debug)]
pub struct SampleArchive {
    path: PathBuf,
    len: usize,
    retain_until_unix_ms: Option<u64>,
}

impl SampleArchive {
    /// Path to the NDJSON sample file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Number of samples in the archive.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the archive contains no samples.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Set a lazy cutoff so reads skip samples after `cutoff_ms`.
    pub fn retain_until(&mut self, cutoff_ms: u64) {
        self.retain_until_unix_ms =
            Some(self.retain_until_unix_ms.map_or(cutoff_ms, |current| current.min(cutoff_ms)));
    }

    /// Iterate over samples in the archive.
    pub fn iter(&self) -> Result<SampleArchiveIter> {
        SampleArchiveIter::open(&self.path, self.retain_until_unix_ms)
    }

    /// Write NDJSON samples to `writer`, applying any lazy cutoff.
    pub fn write_ndjson_to<W: Write>(&self, writer: &mut W) -> Result<usize> {
        if self.retain_until_unix_ms.is_none() {
            let mut reader = BufReader::new(File::open(&self.path).wrap_err_with(|| {
                format!("failed to open sample archive {}", self.path.display())
            })?);
            std::io::copy(&mut reader, writer).wrap_err_with(|| {
                format!("failed to copy sample archive {}", self.path.display())
            })?;
            return Ok(self.len);
        }

        let mut written = 0usize;
        for line in self.filtered_lines()? {
            let line = line?;
            writer.write_all(line.as_bytes())?;
            writer.write_all(b"\n")?;
            written += 1;
        }
        Ok(written)
    }

    fn filtered_lines(&self) -> Result<SampleArchiveLineIter> {
        SampleArchiveLineIter::open(&self.path, self.retain_until_unix_ms)
    }
}

impl Drop for SampleArchive {
    fn drop(&mut self) {
        std::fs::remove_file(&self.path).ok();
    }
}

/// Iterator over an NDJSON sample archive.
pub struct SampleArchiveIter {
    lines: Lines<BufReader<File>>,
    retain_until_unix_ms: Option<u64>,
}

impl SampleArchiveIter {
    fn open(path: &Path, retain_until_unix_ms: Option<u64>) -> Result<Self> {
        let file = File::open(path)
            .wrap_err_with(|| format!("failed to open sample archive {}", path.display()))?;
        Ok(Self { lines: BufReader::new(file).lines(), retain_until_unix_ms })
    }
}

impl Iterator for SampleArchiveIter {
    type Item = Result<Sample>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let line = match self.lines.next()? {
                Ok(line) => line,
                Err(err) => return Some(Err(err).context("failed to read sample archive line")),
            };

            let sample: Sample = match serde_json::from_str(&line) {
                Ok(sample) => sample,
                Err(err) => {
                    return Some(Err(err).context("failed to parse sample archive line"));
                }
            };
            if self.retain_until_unix_ms.is_none_or(|cutoff_ms| sample.unix_ms <= cutoff_ms) {
                return Some(Ok(sample));
            }
        }
    }
}

struct SampleArchiveLineIter {
    lines: Lines<BufReader<File>>,
    retain_until_unix_ms: Option<u64>,
}

impl SampleArchiveLineIter {
    fn open(path: &Path, retain_until_unix_ms: Option<u64>) -> Result<Self> {
        let file = File::open(path)
            .wrap_err_with(|| format!("failed to open sample archive {}", path.display()))?;
        Ok(Self { lines: BufReader::new(file).lines(), retain_until_unix_ms })
    }
}

impl Iterator for SampleArchiveLineIter {
    type Item = Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let line = match self.lines.next()? {
                Ok(line) => line,
                Err(err) => return Some(Err(err).context("failed to read sample archive line")),
            };

            let Some(cutoff_ms) = self.retain_until_unix_ms else {
                return Some(Ok(line));
            };

            let sample = match serde_json::from_str::<Sample>(&line) {
                Ok(sample) => sample,
                Err(err) => {
                    return Some(Err(err).context("failed to parse sample archive line"));
                }
            };
            if sample.unix_ms <= cutoff_ms {
                return Some(Ok(line));
            }
        }
    }
}

type SampleWriter = BufWriter<File>;

#[derive(Debug)]
struct SampleStoreInner {
    path: PathBuf,
    writer: Option<SampleWriter>,
    len: usize,
    labels: HashMap<String, String>,
}

/// Append-only sample store.
///
/// Shared between the internal metrics snapshotter and Prometheus scrapers via
/// `Arc`. Batches are serialized to an uncompressed NDJSON file immediately
/// instead of being retained in memory.
#[derive(Debug, Clone)]
pub struct SampleStore {
    inner: Arc<Mutex<SampleStoreInner>>,
}

impl SampleStore {
    /// Create a new empty sample store.
    pub fn new() -> Result<Self> {
        Self::with_labels(HashMap::new())
    }

    /// Create a new empty sample store that adds run-level labels as samples
    /// are written. Existing sample labels win on key collisions.
    pub fn with_labels(labels: HashMap<String, String>) -> Result<Self> {
        let path = temporary_sample_path();
        let writer = sample_writer(&path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(SampleStoreInner {
                path,
                writer: Some(writer),
                len: 0,
                labels,
            })),
        })
    }

    /// Append a batch of samples.
    pub async fn push_batch(&self, samples: Vec<Sample>) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        let mut inner = self.inner.lock().await;
        let labels = inner.labels.clone();

        let Some(writer) = inner.writer.as_mut() else {
            eyre::bail!("sample store has already been finalized");
        };

        let mut written = 0usize;
        let mut skipped_non_finite = 0usize;
        for mut sample in samples {
            if !sample.value.is_finite() {
                skipped_non_finite += 1;
                continue;
            }

            apply_labels(&mut sample, &labels);
            serde_json::to_writer(&mut *writer, &sample)?;
            writeln!(writer)?;
            written += 1;
        }
        if skipped_non_finite > 0 {
            tracing::debug!(
                samples = skipped_non_finite,
                "Skipped non-finite metric samples while writing sample archive"
            );
        }
        writer.flush()?;
        inner.len += written;
        Ok(())
    }

    /// Finalize the sample archive.
    pub async fn finish(&self) -> Result<SampleArchive> {
        let mut inner = self.inner.lock().await;
        if let Some(mut writer) = inner.writer.take() {
            writer.flush()?;
        }

        Ok(SampleArchive { path: inner.path.clone(), len: inner.len, retain_until_unix_ms: None })
    }

    /// Number of samples currently stored.
    pub async fn len(&self) -> usize {
        self.inner.lock().await.len
    }

    /// Whether the store is empty.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

fn apply_labels(sample: &mut Sample, labels: &HashMap<String, String>) {
    for (key, value) in labels {
        sample.labels.entry(key.clone()).or_insert_with(|| value.clone());
    }
}

fn sample_writer(path: &Path) -> Result<SampleWriter> {
    let file = File::create(path)
        .wrap_err_with(|| format!("failed to create sample archive {}", path.display()))?;
    Ok(BufWriter::new(file))
}

fn temporary_sample_path() -> PathBuf {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

    std::env::temp_dir()
        .join(format!("txgen-samples-{}-{nanos}-{id}.samples.ndjson", std::process::id()))
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
    async fn push_and_iterate_archive() {
        let store = SampleStore::new().unwrap();
        assert!(store.is_empty().await);

        store.push_batch(vec![make_sample("a", 1.0, 0), make_sample("b", 2.0, 100)]).await.unwrap();

        assert_eq!(store.len().await, 2);

        let archive = store.finish().await.unwrap();
        let samples = archive.iter().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].name, "a");
        assert_eq!(samples[1].value, 2.0);
    }

    #[tokio::test]
    async fn labels_are_applied_while_writing() {
        let store = SampleStore::with_labels(HashMap::from([
            ("run_id".to_string(), "abc".to_string()),
            ("node".to_string(), "fallback".to_string()),
        ]))
        .unwrap();

        let mut sample = make_sample("x", 42.0, 0);
        sample.labels.insert("node".to_string(), "a".to_string());
        store.push_batch(vec![sample]).await.unwrap();

        let archive = store.finish().await.unwrap();
        let samples = archive.iter().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(samples[0].labels["run_id"], "abc");
        assert_eq!(samples[0].labels["node"], "a");
    }

    #[tokio::test]
    async fn non_finite_values_are_not_archived() {
        let store = SampleStore::new().unwrap();
        store
            .push_batch(vec![
                make_sample("finite", 1.0, 0),
                make_sample("nan", f64::NAN, 100),
                make_sample("inf", f64::INFINITY, 200),
                make_sample("neg_inf", f64::NEG_INFINITY, 300),
            ])
            .await
            .unwrap();

        assert_eq!(store.len().await, 1);

        let archive = store.finish().await.unwrap();
        let samples = archive.iter().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].name, "finite");
        assert_eq!(samples[0].value, 1.0);
    }

    #[tokio::test]
    async fn retain_until_filters_reads_without_rewriting_archive() {
        let store = SampleStore::new().unwrap();
        store
            .push_batch(vec![make_sample("old", 1.0, 0), make_sample("new", 2.0, 100)])
            .await
            .unwrap();

        let mut archive = store.finish().await.unwrap();
        let original_path = archive.path().to_path_buf();
        let original_content = std::fs::read_to_string(&original_path).unwrap();

        archive.retain_until(1_700_000_000_000);

        assert_eq!(archive.path(), original_path);
        assert_eq!(std::fs::read_to_string(&original_path).unwrap(), original_content);

        let samples = archive.iter().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].name, "old");

        let mut ndjson = Vec::new();
        let count = archive.write_ndjson_to(&mut ndjson).unwrap();
        assert_eq!(count, 1);
        assert_eq!(String::from_utf8(ndjson).unwrap().lines().count(), 1);
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
