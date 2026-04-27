//! VictoriaMetrics reporter.
//!
//! Pushes the unified [`Sample`] stream from a benchmark run to a
//! VictoriaMetrics instance via the `/api/v1/import/prometheus`
//! endpoint, which accepts Prometheus text exposition format.
//!
//! User-provided metadata (`-m key=value`) is forwarded as `extra_label`
//! query parameters so VM stamps every sample in the request with those
//! labels server-side. This keeps request bodies small and avoids
//! mutating the in-memory [`FinalReport`].
//!
//! Connection knobs (auth, tenant, batching) are read from environment
//! variables so secrets never end up on the command line:
//!
//! | Env var               | Purpose                                              |
//! |-----------------------|------------------------------------------------------|
//! | `VM_BEARER_TOKEN`     | `Authorization: Bearer …` header (e.g. VM Cloud)    |
//! | `VM_USER`             | HTTP basic auth username (used with `VM_PASSWORD`)   |
//! | `VM_PASSWORD`         | HTTP basic auth password                             |
//! | `VM_TENANT_ID`        | Cluster VM accountID, sent as `?accountID=…`         |
//! | `VM_BATCH_SIZE`       | Samples per HTTP request (default: 10_000)           |
//! | `VM_TIMEOUT_SECS`     | Per-request timeout in seconds (default: 60)         |

use crate::{reporter::FinalReport, sample::Sample, Reporter};
use eyre::{bail, Context, Result};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use std::{collections::BTreeMap, time::Duration};

/// Default samples per ingestion request.
const DEFAULT_BATCH_SIZE: usize = 10_000;
/// Default per-request HTTP timeout.
const DEFAULT_TIMEOUT_SECS: u64 = 60;

/// Configuration for the VictoriaMetrics reporter.
#[derive(Debug, Clone)]
pub struct VictoriaMetricsConfig {
    /// Base URL of the VM instance, e.g. `http://localhost:8428`.
    pub base_url: String,
    /// Run-level labels added to every sample (server-side via `extra_label`).
    pub extra_labels: BTreeMap<String, String>,
    /// Optional bearer token (`Authorization: Bearer …`).
    pub bearer_token: Option<String>,
    /// Optional HTTP basic auth `(user, password)`.
    pub basic_auth: Option<(String, String)>,
    /// Optional cluster VM tenant id (sent as `accountID` query param).
    pub tenant_id: Option<String>,
    /// Samples per HTTP request.
    pub batch_size: usize,
    /// Per-request HTTP timeout.
    pub timeout: Duration,
}

impl VictoriaMetricsConfig {
    /// Build a config from a base URL and the user `--metadata` map.
    ///
    /// Connection knobs (auth, tenant, batching) are read from environment
    /// variables; see the module docs for the list.
    pub fn from_metadata(
        base_url: &str,
        metadata: &std::collections::HashMap<String, String>,
    ) -> Result<Self> {
        let bearer_token = std::env::var("VM_BEARER_TOKEN").ok().filter(|s| !s.is_empty());
        let basic_auth = match (std::env::var("VM_USER").ok(), std::env::var("VM_PASSWORD").ok()) {
            (Some(u), Some(p)) if !u.is_empty() => Some((u, p)),
            _ => None,
        };
        let tenant_id = std::env::var("VM_TENANT_ID").ok().filter(|s| !s.is_empty());
        let batch_size = std::env::var("VM_BATCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|n: &usize| *n > 0)
            .unwrap_or(DEFAULT_BATCH_SIZE);
        let timeout_secs = std::env::var("VM_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|n: &u64| *n > 0)
            .unwrap_or(DEFAULT_TIMEOUT_SECS);

        let extra_labels: BTreeMap<String, String> = metadata
            .iter()
            .filter_map(|(k, v)| {
                let key = sanitize_label_name(k);
                if key.is_empty() {
                    None
                } else {
                    Some((key, v.clone()))
                }
            })
            .collect();

        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            extra_labels,
            bearer_token,
            basic_auth,
            tenant_id,
            batch_size,
            timeout: Duration::from_secs(timeout_secs),
        })
    }
}

/// VictoriaMetrics reporter.
pub struct VictoriaMetricsReporter {
    config: VictoriaMetricsConfig,
    client: reqwest::Client,
    import_url: String,
}

impl VictoriaMetricsReporter {
    /// Construct a new reporter.
    pub fn new(config: VictoriaMetricsConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .context("failed to create HTTP client")?;

        // Build the import URL once with extra_label + accountID query params.
        let mut url = format!("{}/api/v1/import/prometheus", config.base_url);
        let mut params: Vec<(String, String)> = Vec::new();
        for (k, v) in &config.extra_labels {
            params.push(("extra_label".to_string(), format!("{k}={v}")));
        }
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
            extra_labels = config.extra_labels.len(),
            batch_size = config.batch_size,
            tenant = ?config.tenant_id,
            "VictoriaMetrics reporter initialized"
        );

        Ok(Self { config, client, import_url: url })
    }

    /// Build common request headers (auth + content-type).
    fn headers(&self) -> Result<HeaderMap> {
        let mut h = HeaderMap::new();
        h.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=utf-8"));
        if let Some(token) = &self.config.bearer_token {
            let v = HeaderValue::from_str(&format!("Bearer {token}"))
                .context("invalid VM_BEARER_TOKEN")?;
            h.insert(AUTHORIZATION, v);
        }
        Ok(h)
    }

    /// Send a single batch of samples as Prometheus text.
    fn send_batch(&self, batch: &[Sample]) -> Result<()> {
        let body = encode_samples(batch);
        if body.is_empty() {
            return Ok(());
        }

        let headers = self.headers()?;
        let rt = tokio::runtime::Handle::current();

        let mut req = self.client.post(&self.import_url).headers(headers).body(body);
        if let Some((user, password)) = &self.config.basic_auth {
            req = req.basic_auth(user, Some(password));
        }

        let resp = tokio::task::block_in_place(|| rt.block_on(req.send()))
            .wrap_err("failed to POST to VictoriaMetrics")?;

        let status = resp.status();
        if !status.is_success() {
            let body = tokio::task::block_in_place(|| rt.block_on(resp.text()))
                .unwrap_or_else(|_| "<no body>".to_string());
            bail!("VictoriaMetrics import failed (HTTP {status}): {body}");
        }
        Ok(())
    }
}

impl Reporter for VictoriaMetricsReporter {
    fn finalize(&mut self, report: &FinalReport) -> Result<()> {
        if report.samples.is_empty() {
            tracing::info!("VictoriaMetrics: no samples to push");
            return Ok(());
        }

        tracing::info!(
            samples = report.samples.len(),
            url = %self.config.base_url,
            "Pushing samples to VictoriaMetrics"
        );

        let mut pushed = 0usize;
        for chunk in report.samples.chunks(self.config.batch_size) {
            self.send_batch(chunk)?;
            pushed += chunk.len();
        }

        tracing::info!(samples = pushed, "VictoriaMetrics push complete");
        Ok(())
    }
}

/// Encode a batch of samples as Prometheus text exposition format.
///
/// Skips samples whose name is not a valid Prometheus metric identifier
/// or whose value is non-finite (NaN/±Inf are not representable in the
/// text format).
fn encode_samples(samples: &[Sample]) -> String {
    let mut out = String::with_capacity(samples.len() * 96);
    for s in samples {
        if !is_valid_metric_name(&s.name) || !s.value.is_finite() {
            continue;
        }
        out.push_str(&s.name);
        if !s.labels.is_empty() {
            out.push('{');
            let mut first = true;
            for (k, v) in &s.labels {
                let key = sanitize_label_name(k);
                if key.is_empty() {
                    continue;
                }
                if !first {
                    out.push(',');
                }
                first = false;
                out.push_str(&key);
                out.push_str("=\"");
                escape_label_value(v, &mut out);
                out.push('"');
            }
            out.push('}');
        }
        // Format value compactly; Prometheus text format allows decimal
        // and scientific notation. f64::to_string handles both.
        out.push(' ');
        out.push_str(&format_value(s.value));
        out.push(' ');
        out.push_str(&s.unix_ms.to_string());
        out.push('\n');
    }
    out
}

/// Format a finite f64 for Prometheus text exposition.
fn format_value(v: f64) -> String {
    // Avoid `inf`/`-inf`/`NaN` (already filtered) and scientific edge
    // cases by relying on Rust's default Display for f64, which is
    // accepted by VictoriaMetrics' parser.
    let mut s = v.to_string();
    if !s.contains('.') && !s.contains('e') && !s.contains('E') {
        // Keep integer-valued floats unambiguous (`1` is fine for
        // Prometheus, but a trailing `.0` makes intent explicit).
        s.push_str(".0");
    }
    s
}

/// Escape a Prometheus label value into `out`.
///
/// Per the spec, only `\\`, `\"` and `\n` need escaping inside `"…"`.
fn escape_label_value(v: &str, out: &mut String) {
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
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
            // Prepend `_` and re-evaluate the first char as a body char.
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
        let out = encode_samples(&[s]);
        assert_eq!(out, "txgen_sent_total 42.0 1700000000000\n");
    }

    #[test]
    fn encodes_labels_in_sorted_order() {
        let s = sample("reth_metric", 3.5, &[("zeta", "z"), ("alpha", "a")]);
        let out = encode_samples(&[s]);
        // BTreeMap orders keys alphabetically.
        assert_eq!(out, "reth_metric{alpha=\"a\",zeta=\"z\"} 3.5 1700000000000\n");
    }

    #[test]
    fn escapes_label_values() {
        let s = sample("m", 1.0, &[("path", "a\"b\\c\nd")]);
        let out = encode_samples(&[s]);
        assert!(out.contains(r#"path="a\"b\\c\nd""#), "got: {out}");
    }

    #[test]
    fn skips_invalid_names_and_non_finite() {
        let bad = sample("1bad-name", 1.0, &[]);
        let nan = sample("ok_metric", f64::NAN, &[]);
        let inf = sample("ok_metric", f64::INFINITY, &[]);
        let good = sample("ok_metric", 7.0, &[]);
        let out = encode_samples(&[bad, nan, inf, good]);
        assert_eq!(out, "ok_metric 7.0 1700000000000\n");
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
    fn config_trims_url_and_sanitizes_metadata_labels() {
        let metadata = std::collections::HashMap::from([
            ("git-sha".to_string(), "abc".to_string()),
            ("scenario".to_string(), "tip20".to_string()),
        ]);
        let cfg = VictoriaMetricsConfig::from_metadata("http://vm:8428/", &metadata).unwrap();

        assert_eq!(cfg.base_url, "http://vm:8428");
        // metadata key "git-sha" sanitized to "git_sha".
        assert!(cfg.extra_labels.contains_key("git_sha"));
        assert!(cfg.extra_labels.contains_key("scenario"));
    }
}
