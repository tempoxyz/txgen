use bench_core::ScraperConfig;
use eyre::{bail, Result};
use std::{collections::HashSet, time::Duration};

/// A parsed `--metrics-url` value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricsURL {
    /// A single unlabeled metrics URL.
    Unlabeled(String),
    /// A metrics URL tagged with a node label.
    Labeled { node: String, url: String },
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

    if let Some((node, url)) = split_labeled_entry(value) {
        return Ok(MetricsURL::Labeled { node: node.to_string(), url: url.to_string() });
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
        let node_label = match value {
            MetricsURL::Labeled { node, .. } => {
                if !seen_labels.insert(node.clone()) {
                    bail!("duplicate --metrics-url node label `{node}`");
                }
                Some(node.clone())
            }
            MetricsURL::Unlabeled(url) if require_labels => {
                bail!(
                    "multiple --metrics-url endpoints must use `node:URL` entries; invalid entry `{url}`"
                );
            }
            MetricsURL::Unlabeled(_) => None,
        };

        let mut config = ScraperConfig::new(value.url()).with_interval(interval);
        if let Some(node_label) = node_label {
            config = config.with_node_label(node_label);
        }
        configs.push(config);
    }

    Ok(configs)
}

fn split_labeled_entry(entry: &str) -> Option<(&str, &str)> {
    let (label, url) = entry.split_once(':')?;
    let label = label.trim();
    let url = url.trim();

    if label.is_empty() || url.is_empty() || !starts_with_http_scheme(url) {
        return None;
    }

    Some((label, url))
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
                node: "a".to_string(),
                url: "http://127.0.0.1:9001/metrics".to_string()
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
        assert_eq!(configs[0].node_label, None);
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
        assert_eq!(configs[0].node_label.as_deref(), Some("a"));
        assert_eq!(configs[1].url, "https://node-b:9001/metrics");
        assert_eq!(configs[1].node_label.as_deref(), Some("b"));
    }

    #[test]
    fn rejects_multiple_urls_without_labels() {
        let values = vec![
            parse_metrics_url("http://node-a:9001/metrics").unwrap(),
            parse_metrics_url("http://node-b:9001/metrics").unwrap(),
        ];
        let err = metrics_scraper_configs(&values, Duration::from_millis(500)).unwrap_err();

        assert!(err.to_string().contains("must use `node:URL`"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_duplicate_labels() {
        let values = vec![
            parse_metrics_url("a:http://node-a:9001/metrics").unwrap(),
            parse_metrics_url("a:http://node-b:9001/metrics").unwrap(),
        ];
        let err = metrics_scraper_configs(&values, Duration::from_millis(500)).unwrap_err();

        assert!(
            err.to_string().contains("duplicate --metrics-url node label"),
            "unexpected error: {err}"
        );
    }
}
