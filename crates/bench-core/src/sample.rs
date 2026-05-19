//! Unified metric sample type and disk-backed store.
//!
//! Both internal benchmark metrics and scraped node Prometheus metrics are
//! streamed into a gzip-compressed NDJSON file. Reporters read the file back in
//! batches at finalization time, which avoids retaining all metric samples in
//! memory for long benchmark runs.

use eyre::{Context, Result};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
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

/// A finalized gzip-compressed NDJSON sample archive.
#[derive(Debug)]
pub struct SampleArchive {
    path: PathBuf,
    len: usize,
}

impl SampleArchive {
    /// Path to the compressed NDJSON sample file.
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

    /// Iterate over samples by decompressing the archive.
    pub fn iter(&self) -> Result<SampleArchiveIter> {
        SampleArchiveIter::open(&self.path)
    }

    /// Rewrite the archive, retaining only samples matching `keep`.
    pub fn retain<F>(&mut self, mut keep: F) -> Result<()>
    where
        F: FnMut(&Sample) -> bool,
    {
        let filtered_path = temporary_sample_path();
        let mut writer = gzip_writer(&filtered_path)?;
        let mut retained = 0usize;

        for sample in self.iter()? {
            let sample = sample?;
            if keep(&sample) {
                serde_json::to_writer(&mut writer, &sample)?;
                writeln!(writer)?;
                retained += 1;
            }
        }

        writer.finish()?.flush()?;
        std::fs::remove_file(&self.path).ok();
        self.path = filtered_path;
        self.len = retained;
        Ok(())
    }
}

impl Drop for SampleArchive {
    fn drop(&mut self) {
        std::fs::remove_file(&self.path).ok();
    }
}

/// Iterator over a compressed NDJSON sample archive.
pub struct SampleArchiveIter {
    lines: Lines<BufReader<GzDecoder<File>>>,
}

impl SampleArchiveIter {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)
            .wrap_err_with(|| format!("failed to open sample archive {}", path.display()))?;
        let decoder = GzDecoder::new(file);
        Ok(Self { lines: BufReader::new(decoder).lines() })
    }
}

impl Iterator for SampleArchiveIter {
    type Item = Result<Sample>;

    fn next(&mut self) -> Option<Self::Item> {
        let line = match self.lines.next()? {
            Ok(line) => line,
            Err(err) => return Some(Err(err).context("failed to read sample archive line")),
        };

        Some(serde_json::from_str(&line).context("failed to parse sample archive line"))
    }
}

type SampleWriter = GzEncoder<BufWriter<File>>;

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
/// `Arc`. Batches are serialized to a gzip-compressed NDJSON file immediately
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
        let writer = gzip_writer(&path)?;
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
        for mut sample in samples {
            apply_labels(&mut sample, &labels);
            serde_json::to_writer(&mut *writer, &sample)?;
            writeln!(writer)?;
            written += 1;
        }
        writer.flush()?;
        inner.len += written;
        Ok(())
    }

    /// Finalize the compressed sample archive.
    pub async fn finish(&self) -> Result<SampleArchive> {
        let mut inner = self.inner.lock().await;
        if let Some(writer) = inner.writer.take() {
            writer.finish()?.flush()?;
        }

        Ok(SampleArchive { path: inner.path.clone(), len: inner.len })
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

fn gzip_writer(path: &Path) -> Result<SampleWriter> {
    let file = File::create(path)
        .wrap_err_with(|| format!("failed to create sample archive {}", path.display()))?;
    Ok(GzEncoder::new(BufWriter::new(file), Compression::default()))
}

fn temporary_sample_path() -> PathBuf {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

    std::env::temp_dir()
        .join(format!("txgen-samples-{}-{nanos}-{id}.samples.ndjson.gz", std::process::id()))
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
    async fn retain_rewrites_archive() {
        let store = SampleStore::new().unwrap();
        store
            .push_batch(vec![make_sample("old", 1.0, 0), make_sample("new", 2.0, 100)])
            .await
            .unwrap();

        let mut archive = store.finish().await.unwrap();
        archive.retain(|sample| sample.offset_ms >= 100).unwrap();

        let samples = archive.iter().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].name, "new");
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
