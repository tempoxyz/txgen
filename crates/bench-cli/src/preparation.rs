//! Validator readiness gates for benchmark warm-up and cooldown.
use bench_core::parse_prometheus_text;
use eyre::{bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{path::Path, time::Duration};
use tokio::{task::JoinSet, time::sleep};

#[derive(Clone, Deserialize)]
struct Validator {
    validator_name: String,
    rpc_url: String,
    execution_metrics_url: String,
    consensus_metrics_url: String,
}

#[derive(Clone)]
pub(crate) struct Preparation {
    validators: Vec<Validator>,
    client: reqwest::Client,
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
                let text = client
                    .get(&validator.consensus_metrics_url)
                    .send()
                    .await?
                    .error_for_status()?
                    .text()
                    .await?;
                let count = metric(&text, "finalized_blocks_proposed_by_self_total", None)
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
                rpc(&client, &validator.rpc_url, "debug_clearTxpool").await.wrap_err_with(|| {
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
                    let pool = rpc(&client, &validator.rpc_url, "txpool_status").await?;
                    let pending = hex(&pool["pending"])?;
                    let queued = hex(&pool["queued"])?;
                    let head = hex(&rpc(&client, &validator.rpc_url, "eth_blockNumber").await?)?;
                    let text = client
                        .get(&validator.execution_metrics_url)
                        .send()
                        .await?
                        .error_for_status()?
                        .text()
                        .await?;
                    let finish = metric(&text, "reth_sync_checkpoint", Some(("stage", "Finish")))
                        .wrap_err_with(|| validator.validator_name.clone())?;
                    Ok::<_, eyre::Report>((validator.validator_name, pending, queued, head, finish))
                });
            }
            let mut rows = Vec::new();
            while let Some(result) = tasks.join_next().await {
                rows.push(result??);
            }
            rows.sort();
            let empty = rows.iter().all(|r| r.1 == 0 && r.2 == 0);
            // Require consecutive all-node empty observations to avoid a transient gap.
            if empty {
                empty_polls += 1;
            } else {
                empty_polls = 0;
                target = None;
            }
            if empty_polls >= 3 && target.is_none() {
                target = rows.iter().map(|r| r.3).max();
                tracing::info!(
                    ?target,
                    "All validator pools drained; waiting for Finish checkpoints"
                );
            }
            if let Some(height) = target &&
                rows.iter().all(|r| r.4 >= height)
            {
                return Ok(json!({"target_block": height, "validators": rows.iter().map(|r| {
                    json!({"validator": r.0, "pending": r.1, "queued": r.2, "head": r.3, "finish": r.4})
                }).collect::<Vec<_>>()}));
            }
            tracing::info!(?target, ?rows, "Waiting for empty pools and persisted warmup blocks");
            sleep(Duration::from_secs(1)).await;
        }
    }
}

async fn rpc(client: &reqwest::Client, url: &str, method: &str) -> Result<Value> {
    let text = client
        .post(url)
        .header("content-type", "application/json")
        .body(json!({"jsonrpc":"2.0", "id":1, "method":method, "params":[]}).to_string())
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let response: Value = serde_json::from_str(&text)?;
    ensure!(response.get("error").is_none(), "{url} {method}: {}", response["error"]);
    response.get("result").cloned().ok_or_else(|| eyre::eyre!("{url} {method}: missing result"))
}

fn hex(value: &Value) -> Result<u64> {
    let value = value.as_str().ok_or_else(|| eyre::eyre!("missing RPC quantity"))?;
    Ok(u64::from_str_radix(value.strip_prefix("0x").unwrap_or(value), 16)?)
}

fn metric(text: &str, suffix: &str, label: Option<(&str, &str)>) -> Result<u64> {
    let values: Vec<_> = parse_prometheus_text(text, 0, 0)
        .into_iter()
        .filter(|s| {
            (s.name == suffix || s.name.ends_with(&format!("_{suffix}"))) &&
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
