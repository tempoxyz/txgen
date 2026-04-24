//! Prometheus text exposition format parser.
//!
//! Parses raw text from a node's `/metrics` endpoint into [`Sample`]s.
//! Handles metric lines with and without labels, skips `# HELP` and
//! `# TYPE` comments, and tolerates `NaN` / `+Inf` / `-Inf` values.

use crate::sample::Sample;
use std::collections::BTreeMap;

/// Parse Prometheus text exposition format into a flat list of samples.
///
/// `offset_ms` and `unix_ms` are stamped onto every returned sample
/// (caller provides them from [`RunClock`]).
pub fn parse_prometheus_text(text: &str, offset_ms: u64, unix_ms: u64) -> Vec<Sample> {
    let mut samples = Vec::new();

    for line in text.lines() {
        let line = line.trim();

        // Skip empty lines and comments.
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some(sample) = parse_line(line, offset_ms, unix_ms) {
            samples.push(sample);
        }
    }

    samples
}

/// Parse a single Prometheus metric line.
///
/// Expected formats:
///   `metric_name value [timestamp]`
///   `metric_name{label="val",...} value [timestamp]`
fn parse_line(line: &str, offset_ms: u64, unix_ms: u64) -> Option<Sample> {
    let (name, labels, rest) = if let Some(brace_start) = line.find('{') {
        let name = &line[..brace_start];
        let brace_end = line.find('}')?;
        let labels_str = &line[brace_start + 1..brace_end];
        let rest = line[brace_end + 1..].trim();
        (name, parse_labels(labels_str), rest)
    } else {
        // No labels — split on first whitespace.
        let space = line.find(|c: char| c.is_ascii_whitespace())?;
        let name = &line[..space];
        let rest = line[space..].trim();
        (name, BTreeMap::new(), rest)
    };

    // The rest is `value [timestamp]` — we ignore any Prometheus timestamp
    // since we use our own RunClock timestamps.
    let value_str = rest.split_whitespace().next()?;
    let value = parse_value(value_str)?;

    Some(Sample { name: name.to_string(), labels, value, offset_ms, unix_ms })
}

/// Parse the label portion inside `{...}`.
///
/// Input: `label1="val1",label2="val2"`
fn parse_labels(s: &str) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    if s.is_empty() {
        return labels;
    }

    // Simple state machine to handle commas inside quoted values.
    let mut key = String::new();
    let mut value = String::new();
    let mut in_value = false;
    let mut in_quotes = false;

    for ch in s.chars() {
        match ch {
            '=' if !in_value => {
                in_value = true;
            }
            '"' if in_value => {
                in_quotes = !in_quotes;
                if !in_quotes {
                    // End of quoted value — commit pair.
                    labels.insert(
                        std::mem::take(&mut key).trim().to_string(),
                        std::mem::take(&mut value),
                    );
                    in_value = false;
                }
            }
            ',' if !in_quotes => {
                // Reset for next pair. If we have a non-quoted value pending,
                // commit it.
                if in_value {
                    labels.insert(
                        std::mem::take(&mut key).trim().to_string(),
                        std::mem::take(&mut value).trim().to_string(),
                    );
                    in_value = false;
                }
            }
            _ if in_value => {
                value.push(ch);
            }
            _ => {
                key.push(ch);
            }
        }
    }

    // Handle trailing unquoted value (rare but valid).
    if in_value && !key.is_empty() {
        labels.insert(key.trim().to_string(), value.trim().to_string());
    }

    labels
}

/// Parse a Prometheus value string, handling special float values.
fn parse_value(s: &str) -> Option<f64> {
    match s {
        "+Inf" => Some(f64::INFINITY),
        "-Inf" => Some(f64::NEG_INFINITY),
        "Inf" => Some(f64::INFINITY),
        "NaN" => Some(f64::NAN),
        _ => s.parse::<f64>().ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_metric() {
        let text = "go_goroutines 42\n";
        let samples = parse_prometheus_text(text, 100, 1000);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].name, "go_goroutines");
        assert!(samples[0].labels.is_empty());
        assert_eq!(samples[0].value, 42.0);
        assert_eq!(samples[0].offset_ms, 100);
    }

    #[test]
    fn metric_with_labels() {
        let text = r#"http_requests_total{method="GET",code="200"} 1027"#;
        let samples = parse_prometheus_text(text, 0, 0);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].name, "http_requests_total");
        assert_eq!(samples[0].labels["method"], "GET");
        assert_eq!(samples[0].labels["code"], "200");
        assert_eq!(samples[0].value, 1027.0);
    }

    #[test]
    fn histogram_buckets() {
        let text = r#"
# HELP http_duration_seconds HTTP duration
# TYPE http_duration_seconds histogram
http_duration_seconds_bucket{le="0.5"} 100
http_duration_seconds_bucket{le="1.0"} 150
http_duration_seconds_bucket{le="+Inf"} 200
http_duration_seconds_sum 53.2
http_duration_seconds_count 200
"#;
        let samples = parse_prometheus_text(text, 0, 0);
        assert_eq!(samples.len(), 5);
        assert_eq!(samples[0].labels["le"], "0.5");
        assert_eq!(samples[2].labels["le"], "+Inf");
        assert_eq!(samples[2].value, 200.0);
    }

    #[test]
    fn summary_quantiles() {
        let text = r#"
rpc_duration_seconds{quantile="0.5"} 0.042
rpc_duration_seconds{quantile="0.99"} 0.58
rpc_duration_seconds_sum 1234.5
rpc_duration_seconds_count 5000
"#;
        let samples = parse_prometheus_text(text, 0, 0);
        assert_eq!(samples.len(), 4);
        assert_eq!(samples[0].labels["quantile"], "0.5");
        assert_eq!(samples[0].value, 0.042);
    }

    #[test]
    fn special_values() {
        let text = "a +Inf\nb -Inf\nc NaN\n";
        let samples = parse_prometheus_text(text, 0, 0);
        assert_eq!(samples.len(), 3);
        assert!(samples[0].value.is_infinite() && samples[0].value > 0.0);
        assert!(samples[1].value.is_infinite() && samples[1].value < 0.0);
        assert!(samples[2].value.is_nan());
    }

    #[test]
    fn skips_comments_and_empty_lines() {
        let text = "# HELP foo A counter\n# TYPE foo counter\n\nfoo 1\n";
        let samples = parse_prometheus_text(text, 0, 0);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].name, "foo");
    }

    #[test]
    fn metric_with_timestamp() {
        // Prometheus format allows optional timestamp — we ignore it.
        let text = "foo 42 1700000000000\n";
        let samples = parse_prometheus_text(text, 500, 1500);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].value, 42.0);
        assert_eq!(samples[0].offset_ms, 500);
        assert_eq!(samples[0].unix_ms, 1500);
    }

    #[test]
    fn empty_labels() {
        let text = "foo{} 1\n";
        let samples = parse_prometheus_text(text, 0, 0);
        assert_eq!(samples.len(), 1);
        assert!(samples[0].labels.is_empty());
    }

    #[test]
    fn real_reth_metrics() {
        let text = r#"
# HELP reth_jemalloc_resident Resident memory
# TYPE reth_jemalloc_resident gauge
reth_jemalloc_resident 1073741824
reth_db_table_size{table="Headers"} 52428800
reth_db_table_size{table="Transactions"} 209715200
"#;
        let samples = parse_prometheus_text(text, 250, 1250);
        assert_eq!(samples.len(), 3);
        assert_eq!(samples[0].name, "reth_jemalloc_resident");
        assert_eq!(samples[0].value, 1073741824.0);
        assert_eq!(samples[1].labels["table"], "Headers");
        assert_eq!(samples[2].labels["table"], "Transactions");
    }
}
