//! Prometheus remote write reporter.
//!
//! Pushes the unified [`Sample`] stream from a benchmark run to any
//! Prometheus-compatible remote write endpoint (Prometheus, VictoriaMetrics,
//! Cortex, Thanos, etc.) via `/api/v1/write` using the
//! standard remote write protocol (protobuf + snappy compression).
//!
//! User-provided metadata (`-m key=value`) are already applied to samples before
//! reporters run, so the remote-write request carries run labels in the protobuf
//! payload. The reporter intentionally does not also send them as
//! VictoriaMetrics `extra_label` query parameters, since duplicating labels can
//! make VictoriaMetrics accept the request but drop the affected series.
//!
//! Connection knobs (auth, tenant, batching) are read from environment
//! variables so secrets never end up on the command line:
//!
//! | Env var               | Purpose                                              |
//! |-----------------------|------------------------------------------------------|
//! | `PROMETHEUS_BEARER_TOKEN`     | `Authorization: Bearer …` header                    |
//! | `PROMETHEUS_USER`             | HTTP basic auth username (used with `PROMETHEUS_PASSWORD`)   |
//! | `PROMETHEUS_PASSWORD`         | HTTP basic auth password                             |
//! | `PROMETHEUS_TENANT_ID`        | Cluster tenant / accountID query param               |
//! | `PROMETHEUS_BATCH_SIZE`       | Samples per HTTP request (default: 50_000)           |
//! | `PROMETHEUS_ENCODE_WORKERS`   | Parallel final-report encode workers (default: up to 4) |
//! | `PROMETHEUS_TIMEOUT_SECS`     | Per-request timeout in seconds (default: 60)         |
//! | `PROMETHEUS_QUEUE_SIZE`       | Real-time forwarder queue size (default: 16 batches) |

use crate::{reporter::FinalReport, sample::Sample, Reporter};
use eyre::{bail, eyre, Context, Result};
use prometheus_remote_write::{
    Label, Sample as PromSample, TimeSeries, WriteRequest, CONTENT_TYPE,
    HEADER_NAME_REMOTE_WRITE_VERSION, LABEL_NAME, REMOTE_WRITE_VERSION_01,
};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use std::{collections::BTreeMap, time::Duration};
use tokio::{sync::mpsc, task::JoinSet};

/// Default samples per ingestion request.
const DEFAULT_BATCH_SIZE: usize = 50_000;
/// Maximum default number of CPU workers used to prepare final-report batches.
const MAX_DEFAULT_ENCODE_WORKERS: usize = 4;
/// Default per-request HTTP timeout.
const DEFAULT_TIMEOUT_SECS: u64 = 60;
/// Default number of scrape batches buffered by the real-time forwarder.
const DEFAULT_QUEUE_SIZE: usize = 16;

/// Configuration for the Prometheus remote write reporter.
#[derive(Debug, Clone)]
pub struct PrometheusConfig {
    /// Base URL of the remote write endpoint, e.g. `http://localhost:8428`.
    pub base_url: String,
    /// Optional bearer token (`Authorization: Bearer …`).
    pub bearer_token: Option<String>,
    /// Optional HTTP basic auth `(user, password)`.
    pub basic_auth: Option<(String, String)>,
    /// Optional cluster tenant id (sent as `accountID` query param).
    pub tenant_id: Option<String>,
    /// Samples per HTTP request.
    pub batch_size: usize,
    /// Parallel workers used to build and compress final-report remote-write requests.
    pub encode_workers: usize,
    /// Per-request HTTP timeout.
    pub timeout: Duration,
}

impl PrometheusConfig {
    /// Build a config from a base URL and the user `--metadata` map.
    ///
    /// Metadata labels are applied to samples before reporters run; this
    /// config only reads connection knobs (auth, tenant, batching) from
    /// environment variables. See the module docs for the list.
    pub fn from_metadata(
        base_url: &str,
        _metadata: &std::collections::HashMap<String, String>,
    ) -> Result<Self> {
        let bearer_token = std::env::var("PROMETHEUS_BEARER_TOKEN").ok().filter(|s| !s.is_empty());
        let basic_auth = match (
            std::env::var("PROMETHEUS_USER").ok(),
            std::env::var("PROMETHEUS_PASSWORD").ok(),
        ) {
            (Some(u), Some(p)) if !u.is_empty() => Some((u, p)),
            _ => None,
        };
        let tenant_id = std::env::var("PROMETHEUS_TENANT_ID").ok().filter(|s| !s.is_empty());
        let batch_size = std::env::var("PROMETHEUS_BATCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|n: &usize| *n > 0)
            .unwrap_or(DEFAULT_BATCH_SIZE);
        let encode_workers = std::env::var("PROMETHEUS_ENCODE_WORKERS")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|n: &usize| *n > 0)
            .unwrap_or_else(default_encode_workers);
        let timeout_secs = std::env::var("PROMETHEUS_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|n: &u64| *n > 0)
            .unwrap_or(DEFAULT_TIMEOUT_SECS);

        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            bearer_token,
            basic_auth,
            tenant_id,
            batch_size,
            encode_workers,
            timeout: Duration::from_secs(timeout_secs),
        })
    }
}

/// Prometheus remote write reporter.
pub struct PrometheusReporter {
    config: PrometheusConfig,
    client: reqwest::Client,
    import_url: String,
}

impl PrometheusReporter {
    /// Construct a new reporter.
    pub fn new(config: PrometheusConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .context("failed to create HTTP client")?;

        // Build the import URL with accountID query params. Run labels are encoded in
        // the remote-write payload itself; don't duplicate them via `extra_label`.
        let mut url = format!("{}/api/v1/write", config.base_url);
        let mut params: Vec<(String, String)> = Vec::new();
        if let Some(tenant) = &config.tenant_id {
            params.push(("accountID".to_string(), tenant.clone()));
        }
        if !params.is_empty() {
            let qs: Vec<String> = params
                .into_iter()
                .map(|(k, v)| format!("{}={}", urlencoding::encode(&k), urlencoding::encode(&v)))
                .collect();
            url.push('?');
            url.push_str(&qs.join("&"));
        }

        tracing::info!(
            url = %config.base_url,
            batch_size = config.batch_size,
            tenant = ?config.tenant_id,
            "Prometheus remote write client initialized"
        );

        Ok(Self { config, client, import_url: url })
    }

    /// Build common request headers (auth + content-type).
    fn headers(&self) -> Result<HeaderMap> {
        let mut h = HeaderMap::new();
        h.insert(reqwest::header::CONTENT_TYPE, HeaderValue::from_static(CONTENT_TYPE));
        h.insert("Content-Encoding", HeaderValue::from_static("snappy"));
        h.insert(
            HEADER_NAME_REMOTE_WRITE_VERSION,
            HeaderValue::from_static(REMOTE_WRITE_VERSION_01),
        );
        if let Some(token) = &self.config.bearer_token {
            let v = HeaderValue::from_str(&format!("Bearer {token}"))
                .context("invalid PROMETHEUS_BEARER_TOKEN")?;
            h.insert(AUTHORIZATION, v);
        }
        Ok(h)
    }

    /// Send a single batch of samples as a snappy-compressed protobuf WriteRequest.
    async fn send_batch_async(&self, batch: &[Sample], batch_idx: usize) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let prepared = prepare_batch(batch, batch_idx)?;
        self.send_prepared_batch_async(prepared).await
    }

    /// Send an already encoded remote-write request.
    async fn send_prepared_batch_async(&self, prepared: PreparedBatch) -> Result<()> {
        let PreparedBatch { idx, samples, timeseries, body } = prepared;
        let body_len = body.len();

        tracing::info!(
            batch = idx,
            samples,
            timeseries,
            body_bytes = body_len,
            url = %self.import_url,
            "Sending remote write batch"
        );

        let headers = self.headers()?;
        let mut req = self.client.post(&self.import_url).headers(headers).body(body);
        if let Some((user, password)) = &self.config.basic_auth {
            req = req.basic_auth(user, Some(password));
        }

        let resp = req.send().await.wrap_err("failed to POST remote write")?;

        let status = resp.status();
        let resp_body = resp.text().await.unwrap_or_else(|_| "<no body>".to_string());

        if !status.is_success() {
            tracing::error!(
                batch = idx,
                %status,
                body = %resp_body,
                url = %self.import_url,
                "Remote write batch failed"
            );
            bail!("remote write failed (HTTP {status}): {resp_body}");
        }

        tracing::info!(
            batch = idx,
            %status,
            body = %resp_body,
            "Remote write batch accepted"
        );
        Ok(())
    }

    /// Push final-report samples with parallel encode/compress workers and ordered upload.
    fn push_report(&self, report: &FinalReport) -> Result<usize> {
        let rt = tokio::runtime::Handle::current();
        tokio::task::block_in_place(|| rt.block_on(self.push_report_async(report)))
    }

    async fn push_report_async(&self, report: &FinalReport) -> Result<usize> {
        let mut chunks = report.sample_chunks(self.config.batch_size)?.enumerate();
        let workers = self.config.encode_workers.max(1);
        let mut tasks = JoinSet::new();
        let mut prepared: BTreeMap<usize, PreparedBatch> = BTreeMap::new();
        let mut next_upload = 0usize;
        let mut pushed = 0usize;
        let mut input_done = false;

        loop {
            while !input_done && tasks.len() < workers {
                match chunks.next() {
                    Some((idx, Ok(chunk))) => {
                        tasks.spawn_blocking(move || prepare_owned_batch(chunk, idx));
                    }
                    Some((_, Err(err))) => return Err(err),
                    None => input_done = true,
                }
            }

            if let Some(batch) = prepared.remove(&next_upload) {
                pushed += batch.samples;
                self.send_prepared_batch_async(batch).await?;
                next_upload += 1;
                continue;
            }

            if input_done && tasks.is_empty() && prepared.is_empty() {
                break;
            }

            if let Some(result) = tasks.join_next().await {
                let batch = result.wrap_err("remote write encode worker panicked")??;
                prepared.insert(batch.idx, batch);
            }
        }

        Ok(pushed)
    }
}

impl Reporter for PrometheusReporter {
    fn finalize(&mut self, report: &FinalReport) -> Result<()> {
        if !report.has_samples() {
            tracing::info!("remote write: no samples to push");
            return Ok(());
        }

        let total_batches = report.sample_count().div_ceil(self.config.batch_size);

        tracing::info!(
            samples = report.sample_count(),
            batches = total_batches,
            batch_size = self.config.batch_size,
            encode_workers = self.config.encode_workers,
            url = %self.import_url,
            "Pushing samples via Prometheus remote write"
        );

        let pushed = self.push_report(report)?;

        tracing::info!(samples = pushed, "remote write push complete");
        Ok(())
    }
}

/// Summary returned after a real-time remote-write forwarder drains.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PrometheusForwarderSummary {
    /// Number of HTTP remote-write requests accepted by the endpoint.
    pub batches: usize,
    /// Number of samples submitted to those requests.
    pub samples: usize,
}

/// Background real-time Prometheus remote-write forwarder.
///
/// Scrapers enqueue each archived sample batch as it is collected. The worker
/// preserves the same remote-write payload format used by [`PrometheusReporter`]
/// while keeping HTTP uploads out of the scraper task.
pub struct PrometheusForwarder {
    tx: mpsc::Sender<Vec<Sample>>,
    handle: tokio::task::JoinHandle<Result<PrometheusForwarderSummary>>,
}

/// Cloneable enqueue handle for [`PrometheusForwarder`].
#[derive(Debug, Clone)]
pub struct PrometheusForwarderHandle {
    tx: mpsc::Sender<Vec<Sample>>,
}

impl PrometheusForwarder {
    /// Spawn a real-time forwarder task.
    pub fn spawn(config: PrometheusConfig) -> Result<Self> {
        let queue_size = prometheus_queue_size();
        let writer = PrometheusReporter::new(config)?;
        let (tx, rx) = mpsc::channel(queue_size);
        let handle = tokio::spawn(run_forwarder(writer, rx));

        tracing::info!(queue_size, "Prometheus remote write forwarder started");

        Ok(Self { tx, handle })
    }

    /// Return a cloneable handle for scraper tasks.
    pub fn handle(&self) -> PrometheusForwarderHandle {
        PrometheusForwarderHandle { tx: self.tx.clone() }
    }

    /// Close the queue, wait for all enqueued samples to upload, and return upload stats.
    pub async fn finish(self) -> Result<PrometheusForwarderSummary> {
        let Self { tx, handle } = self;
        drop(tx);
        handle.await.wrap_err("Prometheus remote write forwarder task failed")?
    }
}

impl PrometheusForwarderHandle {
    /// Enqueue a sample batch for real-time remote write.
    pub async fn push_batch(&self, samples: Vec<Sample>) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }
        self.tx.send(samples).await.map_err(|_| eyre!("Prometheus remote write forwarder stopped"))
    }
}

async fn run_forwarder(
    writer: PrometheusReporter,
    mut rx: mpsc::Receiver<Vec<Sample>>,
) -> Result<PrometheusForwarderSummary> {
    let mut summary = PrometheusForwarderSummary::default();
    let mut batch_idx = 0usize;

    while let Some(samples) = rx.recv().await {
        for chunk in samples.chunks(writer.config.batch_size) {
            writer.send_batch_async(chunk, batch_idx).await?;
            summary.batches += 1;
            summary.samples += chunk.len();
            batch_idx += 1;
        }
    }

    tracing::info!(
        batches = summary.batches,
        samples = summary.samples,
        "Prometheus remote write forwarder drained"
    );

    Ok(summary)
}

fn prometheus_queue_size() -> usize {
    std::env::var("PROMETHEUS_QUEUE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n: &usize| *n > 0)
        .unwrap_or(DEFAULT_QUEUE_SIZE)
}

fn default_encode_workers() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().clamp(1, MAX_DEFAULT_ENCODE_WORKERS))
        .unwrap_or(1)
}

struct PreparedBatch {
    idx: usize,
    samples: usize,
    timeseries: usize,
    body: Vec<u8>,
}

fn prepare_owned_batch(batch: Vec<Sample>, idx: usize) -> Result<PreparedBatch> {
    prepare_batch(&batch, idx)
}

fn prepare_batch(batch: &[Sample], idx: usize) -> Result<PreparedBatch> {
    let write_req = build_write_request(batch);
    let timeseries = write_req.timeseries.len();
    let body = write_req.encode_compressed().context("snappy compression failed")?;

    Ok(PreparedBatch { idx, samples: batch.len(), timeseries, body })
}

/// Build a [`WriteRequest`] from a batch of [`Sample`]s.
///
/// Each unique combination of (metric name + labels) becomes one `TimeSeries`
/// entry. Samples with non-finite values or invalid metric names are skipped.
fn build_write_request(samples: &[Sample]) -> WriteRequest {
    use std::collections::HashMap;

    // Group samples by their time series identity (name + sorted labels).
    let mut series_map: HashMap<String, TimeSeries> = HashMap::new();

    for s in samples {
        if !is_valid_metric_name(&s.name) || !s.value.is_finite() {
            continue;
        }

        // Build the label set: __name__ + user labels.
        let mut labels: Vec<Label> = Vec::with_capacity(s.labels.len() + 1);
        labels.push(Label { name: LABEL_NAME.to_string(), value: s.name.clone() });
        for (k, v) in &s.labels {
            let key = sanitize_label_name(k);
            if !key.is_empty() {
                labels.push(Label { name: key, value: v.clone() });
            }
        }
        // Labels must be sorted by name per the remote write spec.
        labels.sort_by(|a, b| a.name.cmp(&b.name));

        // Build a stable key for grouping.
        let series_key: String =
            labels.iter().map(|l| format!("{}={}", l.name, l.value)).collect::<Vec<_>>().join(",");

        let prom_sample = PromSample { value: s.value, timestamp: s.unix_ms as i64 };

        series_map
            .entry(series_key)
            .or_insert_with(|| TimeSeries { labels: labels.clone(), samples: Vec::new() })
            .samples
            .push(prom_sample);
    }

    // Sort samples within each time series by timestamp.
    let timeseries: Vec<TimeSeries> = series_map
        .into_values()
        .map(|mut ts| {
            ts.samples.sort_by_key(|s| s.timestamp);
            ts
        })
        .collect();

    WriteRequest { timeseries }
}

/// Whether a metric name matches `[a-zA-Z_:][a-zA-Z0-9_:]*`.
fn is_valid_metric_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else { return false };
    if !(first.is_ascii_alphabetic() || first == '_' || first == ':') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

/// Coerce an arbitrary string into a valid Prometheus label name.
///
/// Replaces invalid characters with `_`. Returns an empty string if the
/// input is empty or starts with a digit and contains no other valid
/// leading char (in which case a `_` prefix is added).
fn sanitize_label_name(name: &str) -> String {
    if name.is_empty() {
        return String::new();
    }
    let mut out = String::with_capacity(name.len());
    for (i, c) in name.chars().enumerate() {
        let ok = if i == 0 {
            c.is_ascii_alphabetic() || c == '_'
        } else {
            c.is_ascii_alphanumeric() || c == '_'
        };
        if ok {
            out.push(c);
        } else if i == 0 {
            out.push('_');
            if c.is_ascii_alphanumeric() {
                out.push(c);
            } else {
                out.push('_');
            }
        } else {
            out.push('_');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FinalReport, SampleStore};
    use prost::Message as _;
    use std::io::{Read, Write};

    fn sample(name: &str, value: f64, labels: &[(&str, &str)]) -> Sample {
        Sample {
            name: name.to_string(),
            labels: labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            value,
            offset_ms: 0,
            unix_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn encodes_basic_sample() {
        let s = sample("txgen_sent_total", 42.0, &[]);
        let wr = build_write_request(&[s]);
        assert_eq!(wr.timeseries.len(), 1);
        let ts = &wr.timeseries[0];
        assert_eq!(ts.labels.len(), 1);
        assert_eq!(ts.labels[0].name, "__name__");
        assert_eq!(ts.labels[0].value, "txgen_sent_total");
        assert_eq!(ts.samples.len(), 1);
        assert!((ts.samples[0].value - 42.0).abs() < f64::EPSILON);
        assert_eq!(ts.samples[0].timestamp, 1_700_000_000_000);
    }

    #[test]
    fn encodes_labels_sorted() {
        let s = sample("reth_metric", 3.5, &[("zeta", "z"), ("alpha", "a")]);
        let wr = build_write_request(&[s]);
        let ts = &wr.timeseries[0];
        // __name__ comes first, then alpha, then zeta.
        assert_eq!(ts.labels[0].name, "__name__");
        assert_eq!(ts.labels[1].name, "alpha");
        assert_eq!(ts.labels[2].name, "zeta");
    }

    #[test]
    fn groups_same_series() {
        let s1 = Sample {
            name: "m".to_string(),
            labels: [("host".to_string(), "a".to_string())].into(),
            value: 1.0,
            offset_ms: 0,
            unix_ms: 1000,
        };
        let s2 = Sample {
            name: "m".to_string(),
            labels: [("host".to_string(), "a".to_string())].into(),
            value: 2.0,
            offset_ms: 100,
            unix_ms: 2000,
        };
        let wr = build_write_request(&[s1, s2]);
        assert_eq!(wr.timeseries.len(), 1);
        assert_eq!(wr.timeseries[0].samples.len(), 2);
    }

    #[test]
    fn skips_invalid_names_and_non_finite() {
        let bad = sample("1bad-name", 1.0, &[]);
        let nan = sample("ok_metric", f64::NAN, &[]);
        let inf = sample("ok_metric", f64::INFINITY, &[]);
        let good = sample("ok_metric", 7.0, &[]);
        let wr = build_write_request(&[bad, nan, inf, good]);
        assert_eq!(wr.timeseries.len(), 1);
        assert_eq!(wr.timeseries[0].samples[0].value, 7.0);
    }

    #[test]
    fn encode_compressed_succeeds() {
        let s = sample("test_metric", 99.0, &[("env", "dev")]);
        let wr = build_write_request(&[s]);
        // Verify that protobuf + snappy encoding doesn't panic or error.
        let compressed = wr.encode_compressed().unwrap();
        assert!(!compressed.is_empty());
    }

    #[test]
    fn sanitize_label_name_basic() {
        assert_eq!(sanitize_label_name("git-sha"), "git_sha");
        assert_eq!(sanitize_label_name("scenario"), "scenario");
        assert_eq!(sanitize_label_name("123abc"), "_123abc");
        assert_eq!(sanitize_label_name(""), "");
        assert_eq!(sanitize_label_name("a.b.c"), "a_b_c");
    }

    #[test]
    fn config_trims_url_and_does_not_forward_metadata_as_extra_labels() {
        let metadata = std::collections::HashMap::from([
            ("git-sha".to_string(), "abc".to_string()),
            ("scenario".to_string(), "tip20".to_string()),
        ]);
        let cfg = PrometheusConfig::from_metadata("http://prometheus:8428/", &metadata).unwrap();
        let reporter = PrometheusReporter::new(cfg.clone()).unwrap();

        assert_eq!(cfg.base_url, "http://prometheus:8428");
        assert_eq!(reporter.import_url, "http://prometheus:8428/api/v1/write");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn forwarder_uploads_enqueued_samples_in_batches() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let mut bodies = Vec::new();
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                bodies.push(read_http_request_body(&mut stream));
                stream
                    .write_all(
                        b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
            }
            bodies
        });

        let mut cfg =
            PrometheusConfig::from_metadata(&base_url, &std::collections::HashMap::new()).unwrap();
        cfg.batch_size = 1;
        cfg.timeout = Duration::from_secs(5);

        let forwarder = PrometheusForwarder::spawn(cfg).unwrap();
        let handle = forwarder.handle();
        handle
            .push_batch(vec![sample("metric_a", 1.0, &[]), sample("metric_b", 2.0, &[])])
            .await
            .unwrap();
        drop(handle);

        let summary = tokio::time::timeout(Duration::from_secs(5), forwarder.finish())
            .await
            .unwrap()
            .unwrap();
        let bodies = server.join().unwrap();

        assert_eq!(summary, PrometheusForwarderSummary { batches: 2, samples: 2 });
        assert_eq!(bodies.len(), 2);
        assert!(bodies.iter().all(|body| !body.is_empty()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn final_report_uploads_parallel_prepared_batches_in_order() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let mut batches = Vec::new();
            for _ in 0..3 {
                let (mut stream, _) = listener.accept().unwrap();
                let body = read_http_request_body(&mut stream);
                stream
                    .write_all(
                        b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                batches.push(remote_write_metric_name(&body));
            }
            batches
        });

        let store = SampleStore::new().unwrap();
        store
            .push_batch(vec![
                sample("metric_0", 0.0, &[]),
                sample("metric_1", 1.0, &[]),
                sample("metric_2", 2.0, &[]),
            ])
            .await
            .unwrap();

        let mut cfg =
            PrometheusConfig::from_metadata(&base_url, &std::collections::HashMap::new()).unwrap();
        cfg.batch_size = 1;
        cfg.encode_workers = 2;
        cfg.timeout = Duration::from_secs(5);

        let mut reporter = PrometheusReporter::new(cfg).unwrap();
        let report = FinalReport {
            sample_archive: Some(store.finish().await.unwrap()),
            ..Default::default()
        };

        tokio::time::timeout(Duration::from_secs(5), async move { reporter.finalize(&report) })
            .await
            .unwrap()
            .unwrap();

        assert_eq!(server.join().unwrap(), ["metric_0", "metric_1", "metric_2"]);
    }

    fn read_http_request_body(stream: &mut std::net::TcpStream) -> Vec<u8> {
        let mut headers = Vec::new();
        let mut byte = [0u8; 1];
        while !headers.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap();
            headers.push(byte[0]);
        }

        let headers = String::from_utf8(headers).unwrap();
        assert!(headers.starts_with("POST /api/v1/write "));
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap();

        let mut body = vec![0u8; content_length];
        stream.read_exact(&mut body).unwrap();
        body
    }

    fn remote_write_metric_name(body: &[u8]) -> String {
        let decompressed = snap::raw::Decoder::new().decompress_vec(body).unwrap();
        let request = WriteRequest::decode(decompressed.as_slice()).unwrap();
        assert_eq!(request.timeseries.len(), 1);
        request.timeseries[0]
            .labels
            .iter()
            .find(|label| label.name == LABEL_NAME)
            .unwrap()
            .value
            .clone()
    }
}
