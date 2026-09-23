//! Validator readiness gates for benchmark warm-up and pool drain.
use crate::SendArgs;
use alloy_network::AnyNetwork;
use alloy_provider::{ext::TxPoolApi, DynProvider, Provider, ProviderBuilder};
use bench_core::{parse_prometheus_text, GeneratedTx, RunClock, Sender, TxPhase, TxSource};
use eyre::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::HashMap, path::Path, time::Duration};
use tokio::{task::JoinSet, time::sleep};

/// Preparation preserves the sender and returns any transaction not consumed by warm-up.
pub(crate) struct PreparedWorkload {
    pub(crate) first_workload: Option<GeneratedTx>,
    pub(crate) metadata: HashMap<String, String>,
}

pub(crate) async fn prepare_workload<S: TxSource>(
    args: &SendArgs,
    source: &mut S,
    sender: &mut Sender,
    first_workload: Option<GeneratedTx>,
) -> Result<PreparedWorkload> {
    let mut preparation_metadata = HashMap::new();
    let first_workload = if let Some(path) = &args.warmup_validators {
        let preparation = Preparation::load(path)?;
        let baseline = preparation.proposer_counts().await?;
        let warmup_clock = RunClock::new();
        let mut observers = tokio::task::JoinSet::new();
        observers.spawn(async move {
            let evidence = preparation.wait_for_proposers(baseline).await?;
            Ok::<_, eyre::Report>((preparation, evidence))
        });
        let mut next = first_workload;
        let (preparation, evidence) = tokio::time::timeout(args.warmup_timeout, async {
            loop {
                if let Some(result) = observers.try_join_next() {
                    return result?;
                }
                let tx = match next.take() {
                    Some(tx) => tx,
                    None => source
                        .next_tx()
                        .await?
                        .ok_or_else(|| eyre::eyre!("input ended during warmup"))?,
                };
                if tx.phase == TxPhase::Setup {
                    bail!("setup transaction appeared during warmup");
                }
                sender.send(tx).await?;
            }
        })
        .await
        .wrap_err("proposer warmup timed out; see waiting validators above")??;
        preparation_metadata
            .insert("warmup_start_unix_ms".into(), warmup_clock.start_unix_ms().to_string());
        preparation_metadata
            .insert("warmup_secs".into(), warmup_clock.elapsed().as_secs_f64().to_string());
        preparation_metadata.insert("proposer_coverage".into(), evidence.to_string());
        let pool_drain_clock = RunClock::new();
        let evidence = tokio::time::timeout(args.cooldown_timeout, async {
            sender.flush().await?;
            preparation.drain_pools().await
        })
        .await
        .wrap_err("validator pool drain timed out; see pools and checkpoints above")??;
        preparation_metadata
            .insert("cooldown_start_unix_ms".into(), pool_drain_clock.start_unix_ms().to_string());
        preparation_metadata
            .insert("cooldown_secs".into(), pool_drain_clock.elapsed().as_secs_f64().to_string());
        preparation_metadata.insert("cooldown_readiness".into(), evidence.to_string());
        // The drain emptied the pools. Resume traffic before recording so the
        // first measured block is not the initial, partially filled block.
        let ramp_up_clock = RunClock::new();
        warm_up(source, sender, None, args.measurement_delay)
            .await
            .wrap_err("post-drain ramp-up failed")?;
        preparation_metadata
            .insert("ramp_up_start_unix_ms".into(), ramp_up_clock.start_unix_ms().to_string());
        preparation_metadata
            .insert("ramp_up_secs".into(), ramp_up_clock.elapsed().as_secs_f64().to_string());
        None
    } else {
        warm_up(source, sender, first_workload, args.warmup).await?
    };

    Ok(PreparedWorkload { first_workload, metadata: preparation_metadata })
}

/// Consume warmup workload without flushing or replacing the sender. Requests already
/// dispatched keep their warmup collector even if they complete during measurement.
async fn warm_up<S: TxSource>(
    source: &mut S,
    sender: &mut Sender,
    mut next: Option<GeneratedTx>,
    duration: Duration,
) -> Result<Option<GeneratedTx>> {
    if duration.is_zero() {
        return Ok(next);
    }
    let start = std::time::Instant::now();
    tracing::info!(?duration, "Starting workload warmup");
    while start.elapsed() < duration {
        let tx = match next.take() {
            Some(tx) => tx,
            None => source.next_tx().await?.ok_or_else(|| {
                eyre::eyre!("input ended during warmup; generate warmup plus measurement duration")
            })?,
        };
        if tx.phase == TxPhase::Setup {
            bail!("setup transaction appeared after workload started");
        }
        sender.send(tx).await?;
    }
    tracing::info!(elapsed = ?start.elapsed(), "Warmup complete; starting measurement");
    Ok(None)
}

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

struct Preparation {
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
    fn load(path: &Path) -> Result<Self> {
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

    async fn proposer_counts(&self) -> Result<Vec<(String, u64)>> {
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

    async fn wait_for_proposers(&self, baseline: Vec<(String, u64)>) -> Result<Value> {
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
    async fn drain_pools(&self) -> Result<Value> {
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
