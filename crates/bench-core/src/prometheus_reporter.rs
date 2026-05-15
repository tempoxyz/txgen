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
//! | `PROMETHEUS_BATCH_SIZE`       | Samples per HTTP request (default: 10_000)           |
//! | `PROMETHEUS_TIMEOUT_SECS`     | Per-request timeout in seconds (default: 60)         |

use crate::{reporter::FinalReport, sample::Sample, Reporter};
use eyre::{bail, Context, Result};
use prometheus_remote_write::{
    Label, Sample as PromSample, TimeSeries, WriteRequest, CONTENT_TYPE,
    HEADER_NAME_REMOTE_WRITE_VERSION, LABEL_NAME, REMOTE_WRITE_VERSION_01,
};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use std::time::Duration;

/// Default samples per ingestion request.
const DEFAULT_BATCH_SIZE: usize = 10_000;
/// Default per-request HTTP timeout.
const DEFAULT_TIMEOUT_SECS: u64 = 60;

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
            "Prometheus remote write reporter initialized"
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
    fn send_batch(&self, batch: &[Sample], batch_idx: usize) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let write_req = build_write_request(batch);
        let timeseries_count = write_req.timeseries.len();
        let body = write_req.encode_compressed().context("snappy compression failed")?;
        let body_len = body.len();

        tracing::debug!(
            batch = batch_idx,
            samples = batch.len(),
            timeseries = timeseries_count,
            body_bytes = body_len,
            url = %self.import_url,
            "Sending remote write batch"
        );

        let headers = self.headers()?;
        let rt = tokio::runtime::Handle::current();

        let mut req = self.client.post(&self.import_url).headers(headers).body(body);
        if let Some((user, password)) = &self.config.basic_auth {
            req = req.basic_auth(user, Some(password));
        }

        let resp = tokio::task::block_in_place(|| rt.block_on(req.send()))
            .wrap_err("failed to POST remote write")?;

        let status = resp.status();
        let resp_body = tokio::task::block_in_place(|| rt.block_on(resp.text()))
            .unwrap_or_else(|_| "<no body>".to_string());

        if !status.is_success() {
            tracing::error!(
                batch = batch_idx,
                %status,
                body = %resp_body,
                url = %self.import_url,
                "Remote write batch failed"
            );
            bail!("remote write failed (HTTP {status}): {resp_body}");
        }

        tracing::debug!(
            batch = batch_idx,
            %status,
            body = %resp_body,
            "Remote write batch accepted"
        );
        Ok(())
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
            url = %self.import_url,
            "Pushing samples via Prometheus remote write"
        );

        let mut pushed = 0usize;
        for (idx, chunk) in report.sample_chunks(self.config.batch_size)?.enumerate() {
            let chunk = chunk?;
            self.send_batch(&chunk, idx)?;
            pushed += chunk.len();
        }

        tracing::info!(samples = pushed, "remote write push complete");
        Ok(())
    }
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
}
