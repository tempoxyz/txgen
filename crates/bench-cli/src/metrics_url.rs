use bench_core::ScraperConfig;
use eyre::{bail, Result};
use std::{
    collections::{BTreeMap, HashSet},
    time::Duration,
};

/// A parsed `--metrics-url` value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricsURL {
    /// A single unlabeled metrics URL.
    Unlabeled(String),
    /// A metrics URL tagged with arbitrary sample labels.
    Labeled { labels: BTreeMap<String, String>, url: String },
}

impl MetricsURL {
    fn url(&self) -> &str {
        match self {
            Self::Unlabeled(url) | Self::Labeled { url, .. } => url,
        }
    }
}

/// Parse one comma-delimited `--metrics-url` value.
pub(crate) fn parse_metrics_url(value: &str) -> Result<MetricsURL, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("--metrics-url cannot be empty".to_string());
    }

    if let Some((label_spec, url)) = split_labeled_entry(value) {
        return Ok(MetricsURL::Labeled { labels: parse_labels(label_spec)?, url: url.to_string() });
    }

    Ok(MetricsURL::Unlabeled(value.to_string()))
}

pub(crate) fn metrics_scraper_configs(
    values: &[MetricsURL],
    interval: Duration,
) -> Result<Vec<ScraperConfig>> {
    let require_labels = values.len() > 1;
    let mut seen_labels = HashSet::new();
    let mut configs = Vec::with_capacity(values.len());

    for value in values {
        let labels = match value {
            MetricsURL::Labeled { labels, .. } => {
                let identity = labels
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect::<Vec<_>>()
                    .join(";");
                if !seen_labels.insert(identity.clone()) {
                    bail!("duplicate --metrics-url labels `{identity}`");
                }
                labels.clone()
            }
            MetricsURL::Unlabeled(url) if require_labels => {
                bail!(
                    "multiple --metrics-url endpoints must use labeled entries; invalid entry `{url}`"
                );
            }
            MetricsURL::Unlabeled(_) => BTreeMap::new(),
        };

        let mut config = ScraperConfig::new(value.url()).with_interval(interval);
        config = config.with_labels(labels);
        configs.push(config);
    }

    Ok(configs)
}

fn split_labeled_entry(entry: &str) -> Option<(&str, &str)> {
    let (label, url) = entry.split_once('@').or_else(|| entry.split_once(':'))?;
    let label = label.trim();
    let url = url.trim();

    if label.is_empty() || url.is_empty() || !starts_with_http_scheme(url) {
        return None;
    }

    Some((label, url))
}

fn parse_labels(value: &str) -> Result<BTreeMap<String, String>, String> {
    if !value.contains('=') {
        return Ok(BTreeMap::from([("node".to_string(), value.trim().to_string())]));
    }

    let mut labels = BTreeMap::new();
    for entry in value.split(';') {
        let (key, label_value) = entry
            .split_once('=')
            .ok_or_else(|| format!("invalid metrics label `{entry}`; expected KEY=VALUE"))?;
        let key = key.trim();
        let label_value = label_value.trim();
        if key.is_empty() || label_value.is_empty() {
            return Err(format!("invalid metrics label `{entry}`; key and value are required"));
        }
        if !key.chars().enumerate().all(|(index, character)| {
            (index == 0 && (character.is_ascii_alphabetic() || character == '_')) ||
                (index > 0 && (character.is_ascii_alphanumeric() || character == '_'))
        }) {
            return Err(format!("invalid metrics label key `{key}`"));
        }
        if labels.insert(key.to_string(), label_value.to_string()).is_some() {
            return Err(format!("duplicate metrics label key `{key}`"));
        }
    }
    Ok(labels)
}

fn starts_with_http_scheme(value: &str) -> bool {
    value.starts_with("http://") || value.starts_with("https://")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_unlabeled_url_value() {
        let value = parse_metrics_url("http://127.0.0.1:9001/metrics").unwrap();

        assert_eq!(value, MetricsURL::Unlabeled("http://127.0.0.1:9001/metrics".to_string()));
    }

    #[test]
    fn parses_labeled_url_value() {
        let value = parse_metrics_url("a:http://127.0.0.1:9001/metrics").unwrap();

        assert_eq!(
            value,
            MetricsURL::Labeled {
                labels: BTreeMap::from([("node".to_string(), "a".to_string())]),
                url: "http://127.0.0.1:9001/metrics".to_string()
            }
        );
    }

    #[test]
    fn parses_rich_labeled_url_value() {
        let value = parse_metrics_url(
            "validator=v0;validator_pubkey=0xabc;region=us-east-1@http://node-a:9001/metrics",
        )
        .unwrap();

        assert_eq!(
            value,
            MetricsURL::Labeled {
                labels: BTreeMap::from([
                    ("region".to_string(), "us-east-1".to_string()),
                    ("validator".to_string(), "v0".to_string()),
                    ("validator_pubkey".to_string(), "0xabc".to_string()),
                ]),
                url: "http://node-a:9001/metrics".to_string(),
            }
        );
    }

    #[test]
    fn converts_single_unlabeled_url_to_config() {
        let configs = metrics_scraper_configs(
            &[MetricsURL::Unlabeled("http://127.0.0.1:9001/metrics".to_string())],
            Duration::from_millis(200),
        )
        .unwrap();

        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].url, "http://127.0.0.1:9001/metrics");
        assert!(configs[0].labels.is_empty());
        assert_eq!(configs[0].interval, Duration::from_millis(200));
    }

    #[test]
    fn converts_multiple_labeled_urls_to_configs() {
        let values = vec![
            parse_metrics_url("a:http://node-a:9001/metrics").unwrap(),
            parse_metrics_url("b:https://node-b:9001/metrics").unwrap(),
        ];
        let configs = metrics_scraper_configs(&values, Duration::from_millis(500)).unwrap();

        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0].url, "http://node-a:9001/metrics");
        assert_eq!(configs[0].labels.get("node").map(String::as_str), Some("a"));
        assert_eq!(configs[1].url, "https://node-b:9001/metrics");
        assert_eq!(configs[1].labels.get("node").map(String::as_str), Some("b"));
    }

    #[test]
    fn rejects_multiple_urls_without_labels() {
        let values = vec![
            parse_metrics_url("http://node-a:9001/metrics").unwrap(),
            parse_metrics_url("http://node-b:9001/metrics").unwrap(),
        ];
        let err = metrics_scraper_configs(&values, Duration::from_millis(500)).unwrap_err();

        assert!(err.to_string().contains("must use labeled entries"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_duplicate_labels() {
        let values = vec![
            parse_metrics_url("a:http://node-a:9001/metrics").unwrap(),
            parse_metrics_url("a:http://node-b:9001/metrics").unwrap(),
        ];
        let err = metrics_scraper_configs(&values, Duration::from_millis(500)).unwrap_err();

        assert!(
            err.to_string().contains("duplicate --metrics-url labels"),
            "unexpected error: {err}"
        );
    }
}
