//! Validator readiness gates for benchmark warm-up and cooldown.
use alloy_network::AnyNetwork;
use alloy_provider::{ext::TxPoolApi, DynProvider, Provider, ProviderBuilder};
use bench_core::parse_prometheus_text;
use eyre::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{path::Path, time::Duration};
use tokio::{task::JoinSet, time::sleep};

#[derive(Clone, Deserialize)]
struct Validator {
    validator_name: String,
    rpc_url: reqwest::Url,
    execution_metrics_url: String,
    consensus_metrics_url: String,
}

impl Validator {
    fn provider(&self, client: &reqwest::Client) -> DynProvider<AnyNetwork> {
        ProviderBuilder::new_with_network::<AnyNetwork>()
            .connect_reqwest(client.clone(), self.rpc_url.clone())
            .erased()
    }
}

pub(crate) struct Preparation {
    validators: Vec<Validator>,
    client: reqwest::Client,
}

#[derive(Debug, Serialize)]
struct Readiness {
    validator: String,
    pending: u64,
    queued: u64,
    head: u64,
    finish: u64,
}

impl Preparation {
    pub(crate) fn load(path: &Path) -> Result<Self> {
        let validators: Vec<Validator> = serde_json::from_reader(std::fs::File::open(path)?)?;
        ensure!(!validators.is_empty(), "warmup validator list is empty");
        let names: std::collections::HashSet<_> =
            validators.iter().map(|v| &v.validator_name).collect();
        ensure!(names.len() == validators.len(), "duplicate warmup validator names");
        Ok(Self {
            validators,
            client: reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?,
        })
    }

    pub(crate) async fn proposer_counts(&self) -> Result<Vec<(String, u64)>> {
        let mut tasks = JoinSet::new();
        for validator in &self.validators {
            let client = self.client.clone();
            let validator = validator.clone();
            tasks.spawn(async move {
                let count = fetch_metric(
                    &client,
                    &validator.consensus_metrics_url,
                    "finalized_blocks_proposed_by_self_total",
                    None,
                )
                .await
                .wrap_err_with(|| validator.validator_name.clone())?;
                Ok::<_, eyre::Report>((validator.validator_name, count))
            });
        }
        let mut counts = Vec::new();
        while let Some(result) = tasks.join_next().await {
            counts.push(result??);
        }
        counts.sort();
        Ok(counts)
    }

    pub(crate) async fn wait_for_proposers(&self, baseline: Vec<(String, u64)>) -> Result<Value> {
        loop {
            sleep(Duration::from_secs(1)).await;
            let counts = self.proposer_counts().await?;
            let mut missing = Vec::new();
            for ((name, count), (base_name, base)) in counts.iter().zip(&baseline) {
                ensure!(
                    name == base_name && count >= base,
                    "{name}: proposer counter reset during warmup"
                );
                if count == base {
                    missing.push(name);
                }
            }
            if missing.is_empty() {
                return Ok(json!({"before": baseline, "after": counts}));
            }
            tracing::info!(?missing, "Waiting for finalized self-proposals");
        }
    }

    /// Hold the post-drain target fixed while empty blocks advance persistence.
    /// Finish is the block-data checkpoint; state masking must be disabled.
    pub(crate) async fn cooldown(&self) -> Result<Value> {
        let mut clears = JoinSet::new();
        for validator in &self.validators {
            let client = self.client.clone();
            let validator = validator.clone();
            clears.spawn(async move {
                validator
                    .provider(&client)
                    .raw_request::<_, ()>("debug_clearTxpool".into(), ())
                    .await
                    .wrap_err_with(|| {
                        format!("{}: failed to clear txpool", validator.validator_name)
                    })
            });
        }
        while let Some(result) = clears.join_next().await {
            result??;
        }
        tracing::info!("Cleared every validator txpool; confirming pending and queued are empty");
        let mut target = None;
        let mut empty_polls = 0;
        loop {
            let mut tasks = JoinSet::new();
            for validator in &self.validators {
                let client = self.client.clone();
                let validator = validator.clone();
                tasks.spawn(async move {
                    let provider = validator.provider(&client);
                    let pool = provider.txpool_status().await?;
                    let head = provider.get_block_number().await?;
                    let finish = fetch_metric(
                        &client,
                        &validator.execution_metrics_url,
                        "reth_sync_checkpoint",
                        Some(("stage", "Finish")),
                    )
                    .await
                    .wrap_err_with(|| validator.validator_name.clone())?;
                    Ok::<_, eyre::Report>(Readiness {
                        validator: validator.validator_name,
                        pending: pool.pending,
                        queued: pool.queued,
                        head,
                        finish,
                    })
                });
            }
            let mut rows = Vec::new();
            while let Some(result) = tasks.join_next().await {
                rows.push(result??);
            }
            rows.sort_by(|a, b| a.validator.cmp(&b.validator));
            let empty = rows.iter().all(|r| r.pending == 0 && r.queued == 0);
            // Require consecutive all-node empty observations to avoid a transient gap.
            if empty {
                empty_polls += 1;
            } else {
                empty_polls = 0;
                target = None;
            }
            if empty_polls >= 3 && target.is_none() {
                target = rows.iter().map(|r| r.head).max();
                tracing::info!(
                    ?target,
                    "All validator pools drained; waiting for Finish checkpoints"
                );
            }
            if let Some(height) = target &&
                rows.iter().all(|r| r.finish >= height)
            {
                return Ok(json!({"target_block": height, "validators": rows}));
            }
            tracing::info!(?target, ?rows, "Waiting for empty pools and persisted warmup blocks");
            sleep(Duration::from_secs(1)).await;
        }
    }
}

async fn fetch_metric(
    client: &reqwest::Client,
    url: &str,
    suffix: &str,
    label: Option<(&str, &str)>,
) -> Result<u64> {
    let text = client.get(url).send().await?.error_for_status()?.text().await?;
    metric(&text, suffix, label)
}

fn metric(text: &str, suffix: &str, label: Option<(&str, &str)>) -> Result<u64> {
    let qualified_suffix = format!("_{suffix}");
    let values: Vec<_> = parse_prometheus_text(text, 0, 0)
        .into_iter()
        .filter(|s| {
            (s.name == suffix || s.name.ends_with(&qualified_suffix)) &&
                label.is_none_or(|(key, value)| s.labels.get(key).is_some_and(|v| v == value))
        })
        .collect();
    ensure!(values.len() == 1, "expected exactly one {suffix} metric, got {}", values.len());
    let value = values[0].value;
    if !value.is_finite() || value < 0.0 || value.fract() != 0.0 || value >= u64::MAX as f64 {
        bail!("invalid {suffix} value: {value}");
    }
    Ok(value as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn readiness_metrics_must_be_present_unique_and_valid() {
        assert_eq!(
            metric(
                "tempo_executor_finalized_blocks_proposed_by_self_total 4\n",
                "finalized_blocks_proposed_by_self_total",
                None
            )
            .unwrap(),
            4
        );
        let text = "reth_sync_checkpoint{stage=\"Execution\"} 20\nreth_sync_checkpoint{stage=\"Finish\"} 15\n";
        assert_eq!(metric(text, "reth_sync_checkpoint", Some(("stage", "Finish"))).unwrap(), 15);
        for text in ["", "x NaN", "x -1", "x 1.5", "x 1\nx 2"] {
            assert!(metric(text, "x", None).is_err());
        }
    }
}
