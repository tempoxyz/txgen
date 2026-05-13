use bench_core::ScraperConfig;
use eyre::{bail, Result};
use std::{collections::HashSet, time::Duration};

/// Parse `--metrics-url`.
///
/// A single URL keeps the historical behavior:
/// `--metrics-url http://127.0.0.1:9001/metrics`
///
/// Multiple endpoints must be comma-separated `node_label:URL` entries:
/// `--metrics-url a:http://node-a:9001/metrics,b:http://node-b:9001/metrics`
pub(crate) fn parse_metrics_scraper_configs(
    value: &str,
    interval: Duration,
) -> Result<Vec<ScraperConfig>> {
    let value = value.trim();
    if value.is_empty() {
        bail!("--metrics-url cannot be empty");
    }

    let entries: Vec<_> = value.split(',').map(str::trim).collect();
    let require_labels = entries.len() > 1;
    let mut seen_labels = HashSet::new();
    let mut configs = Vec::with_capacity(entries.len());

    for entry in entries {
        if entry.is_empty() {
            bail!("--metrics-url contains an empty endpoint");
        }

        let (url, node_label) = match split_labeled_entry(entry) {
            Some((label, url)) => {
                if !seen_labels.insert(label.to_string()) {
                    bail!("duplicate --metrics-url node label `{label}`");
                }
                (url.to_string(), Some(label.to_string()))
            }
            None if require_labels => {
                bail!(
                    "multiple --metrics-url endpoints must use `node:URL` entries; invalid entry `{entry}`"
                );
            }
            None => (entry.to_string(), None),
        };

        let mut config = ScraperConfig::new(url).with_interval(interval);
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
    fn parses_single_url_without_node_label() {
        let configs = parse_metrics_scraper_configs(
            "http://127.0.0.1:9001/metrics",
            Duration::from_millis(200),
        )
        .unwrap();

        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].url, "http://127.0.0.1:9001/metrics");
        assert_eq!(configs[0].node_label, None);
        assert_eq!(configs[0].interval, Duration::from_millis(200));
    }

    #[test]
    fn parses_labeled_single_url() {
        let configs = parse_metrics_scraper_configs(
            "a:http://127.0.0.1:9001/metrics",
            Duration::from_millis(500),
        )
        .unwrap();

        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].url, "http://127.0.0.1:9001/metrics");
        assert_eq!(configs[0].node_label.as_deref(), Some("a"));
    }

    #[test]
    fn parses_multiple_labeled_urls() {
        let configs = parse_metrics_scraper_configs(
            "a:http://node-a:9001/metrics,b:https://node-b:9001/metrics",
            Duration::from_millis(500),
        )
        .unwrap();

        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0].url, "http://node-a:9001/metrics");
        assert_eq!(configs[0].node_label.as_deref(), Some("a"));
        assert_eq!(configs[1].url, "https://node-b:9001/metrics");
        assert_eq!(configs[1].node_label.as_deref(), Some("b"));
    }

    #[test]
    fn rejects_multiple_urls_without_labels() {
        let err = parse_metrics_scraper_configs(
            "http://node-a:9001/metrics,http://node-b:9001/metrics",
            Duration::from_millis(500),
        )
        .unwrap_err();

        assert!(err.to_string().contains("must use `node:URL`"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_duplicate_labels() {
        let err = parse_metrics_scraper_configs(
            "a:http://node-a:9001/metrics,a:http://node-b:9001/metrics",
            Duration::from_millis(500),
        )
        .unwrap_err();

        assert!(
            err.to_string().contains("duplicate --metrics-url node label"),
            "unexpected error: {err}"
        );
    }
}
