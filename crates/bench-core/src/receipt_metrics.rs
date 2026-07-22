//! Receipt-based transaction gas metric collection and aggregation.

use crate::sender::RpcSubmitter;
use alloy_primitives::{Address, TxHash, U256};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, Semaphore},
    task::{JoinHandle, JoinSet},
};

const RECEIPT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(300);

/// Stable labels used to group receipt metrics by workload or scenario input.
pub type ReceiptMetricLabels = BTreeMap<String, String>;

/// Gas fields retained from a confirmed transaction receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptGasSample {
    /// Gas consumed by the outer transaction.
    pub gas_used: U256,
    /// Effective gas price, when the receipt supplied a fee field.
    pub effective_gas_price: Option<U256>,
}

impl ReceiptGasSample {
    /// Calculate the transaction fee when an effective gas price is available.
    ///
    /// Valid Ethereum receipt field widths fit exactly in a `U256`. An
    /// out-of-range RPC response is omitted instead of wrapping the fee.
    pub fn fee_paid(self) -> Option<U256> {
        self.effective_gas_price.and_then(|price| self.gas_used.checked_mul(price))
    }
}

/// Aggregate statistics for one receipt quantity.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReceiptMetricDistribution {
    /// Number of receipts that supplied this quantity.
    pub count: u64,
    /// Minimum observed value.
    pub min: Option<f64>,
    /// Arithmetic mean of observed values.
    pub mean: Option<f64>,
    /// 50th percentile using the benchmark latency percentile convention.
    pub p50: Option<f64>,
    /// 95th percentile using the benchmark latency percentile convention.
    pub p95: Option<f64>,
    /// 99th percentile using the benchmark latency percentile convention.
    pub p99: Option<f64>,
}

impl ReceiptMetricDistribution {
    fn from_samples(mut samples: Vec<U256>) -> Self {
        if samples.is_empty() {
            return Self::default();
        }

        samples.sort_unstable();
        let count = samples.len() as u64;
        let mean = samples.iter().copied().map(u256_to_f64).sum::<f64>() / count as f64;

        Self {
            count,
            min: Some(u256_to_f64(samples[0])),
            mean: Some(mean),
            p50: Some(u256_to_f64(percentile(&samples, 50))),
            p95: Some(u256_to_f64(percentile(&samples, 95))),
            p99: Some(u256_to_f64(percentile(&samples, 99))),
        }
    }
}

/// Receipt gas metrics for one unique label set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReceiptMetricGroup {
    /// Workload or scenario labels supplied when the transaction was accepted.
    pub labels: ReceiptMetricLabels,
    /// Outer transaction gas consumed.
    pub gas_used: ReceiptMetricDistribution,
    /// Effective gas price from receipts that supplied a fee field.
    pub effective_gas_price: ReceiptMetricDistribution,
    /// `gas_used * effective_gas_price` for receipts that supplied a fee field.
    pub fee_paid: ReceiptMetricDistribution,
}

/// Deterministically ordered receipt gas metric groups.
pub type ReceiptMetrics = Vec<ReceiptMetricGroup>;

#[derive(Debug, Default)]
struct ReceiptMetricSamples {
    gas_used: Vec<U256>,
    effective_gas_price: Vec<U256>,
    fee_paid: Vec<U256>,
}

/// In-memory accumulator for receipt gas samples grouped by caller labels.
#[derive(Debug, Default)]
pub struct ReceiptMetricsAccumulator {
    groups: BTreeMap<ReceiptMetricLabels, ReceiptMetricSamples>,
}

impl ReceiptMetricsAccumulator {
    /// Record a confirmed receipt under a label set.
    ///
    /// Gas usage is retained even when the RPC response omitted its fee fields.
    pub fn record(&mut self, labels: ReceiptMetricLabels, sample: ReceiptGasSample) {
        let group = self.groups.entry(labels).or_default();
        group.gas_used.push(sample.gas_used);
        if let Some(effective_gas_price) = sample.effective_gas_price {
            group.effective_gas_price.push(effective_gas_price);
        }
        if let Some(fee_paid) = sample.fee_paid() {
            group.fee_paid.push(fee_paid);
        }
    }

    /// Build deterministic, serializable metric groups.
    pub fn into_metrics(self) -> ReceiptMetrics {
        self.groups
            .into_iter()
            .map(|(labels, samples)| ReceiptMetricGroup {
                labels,
                gas_used: ReceiptMetricDistribution::from_samples(samples.gas_used),
                effective_gas_price: ReceiptMetricDistribution::from_samples(
                    samples.effective_gas_price,
                ),
                fee_paid: ReceiptMetricDistribution::from_samples(samples.fee_paid),
            })
            .collect()
    }
}

#[derive(Debug)]
struct ReceiptRequest {
    sender: Option<Address>,
    tx_hash: TxHash,
    labels: ReceiptMetricLabels,
    tracked_at: tokio::time::Instant,
}

/// Cloneable handle used by submission paths to register accepted transactions.
#[derive(Debug, Clone)]
pub struct ReceiptCollectorHandle {
    requests: mpsc::UnboundedSender<ReceiptRequest>,
}

impl ReceiptCollectorHandle {
    /// Register an accepted transaction for receipt collection.
    ///
    /// Calls made after the collector starts finishing are ignored.
    pub fn track(&self, sender: Option<Address>, tx_hash: TxHash, labels: ReceiptMetricLabels) {
        let _ = self.requests.send(ReceiptRequest {
            sender,
            tx_hash,
            labels,
            tracked_at: tokio::time::Instant::now(),
        });
    }
}

/// Background collector for confirmed transaction receipt gas fields.
pub struct ReceiptCollector {
    handle: ReceiptCollectorHandle,
    finish: Option<oneshot::Sender<()>>,
    task: JoinHandle<ReceiptMetrics>,
}

impl ReceiptCollector {
    /// Start receipt polling with bounded worker concurrency.
    pub fn start(submitter: RpcSubmitter, workers: usize) -> Self {
        let (requests, receiver) = mpsc::unbounded_channel();
        let (finish, finished) = oneshot::channel();
        let handle = ReceiptCollectorHandle { requests };
        let task = tokio::spawn(run_collector(receiver, finished, submitter, workers.max(1)));

        Self { handle, finish: Some(finish), task }
    }

    /// Return a cloneable registration handle for submission paths.
    pub fn handle(&self) -> ReceiptCollectorHandle {
        self.handle.clone()
    }

    /// Stop accepting transactions, drain queued receipt polling, and aggregate results.
    pub async fn finish(mut self) -> ReceiptMetrics {
        if let Some(finish) = self.finish.take() {
            let _ = finish.send(());
        }
        drop(self.handle);

        match self.task.await {
            Ok(metrics) => metrics,
            Err(error) => {
                tracing::warn!(%error, "receipt collector task failed");
                Vec::new()
            }
        }
    }
}

async fn run_collector(
    mut receiver: mpsc::UnboundedReceiver<ReceiptRequest>,
    mut finish: oneshot::Receiver<()>,
    submitter: RpcSubmitter,
    workers: usize,
) -> ReceiptMetrics {
    let mut accumulator = ReceiptMetricsAccumulator::default();
    let mut tasks = JoinSet::new();
    let mut seen = BTreeSet::new();
    let semaphore = Arc::new(Semaphore::new(workers));
    let mut finishing = false;

    loop {
        if finishing && receiver.is_empty() && tasks.is_empty() {
            break;
        }

        tokio::select! {
            _ = &mut finish, if !finishing => {
                receiver.close();
                finishing = true;
            }
            result = tasks.join_next(), if !tasks.is_empty() => {
                record_task_result(&mut accumulator, result);
            }
            request = receiver.recv(), if !finishing || !receiver.is_empty() => {
                match request {
                    Some(request) => {
                        if !seen.insert((request.tx_hash, request.labels.clone())) {
                            continue;
                        }
                        let submitter = submitter.clone();
                        let semaphore = semaphore.clone();
                        tasks.spawn(async move {
                            collect_receipt(submitter, request, semaphore).await
                        });
                    }
                    None => finishing = true,
                }
            }
        }
    }

    accumulator.into_metrics()
}

fn record_task_result(
    accumulator: &mut ReceiptMetricsAccumulator,
    result: Option<Result<Option<(ReceiptMetricLabels, ReceiptGasSample)>, tokio::task::JoinError>>,
) {
    match result {
        Some(Ok(Some((labels, sample)))) => accumulator.record(labels, sample),
        Some(Ok(None)) | None => {}
        Some(Err(error)) => tracing::warn!(%error, "receipt polling task failed"),
    }
}

async fn collect_receipt(
    submitter: RpcSubmitter,
    request: ReceiptRequest,
    semaphore: Arc<Semaphore>,
) -> Option<(ReceiptMetricLabels, ReceiptGasSample)> {
    let deadline = request.tracked_at + RECEIPT_TIMEOUT;

    loop {
        let permit = match tokio::time::timeout_at(deadline, semaphore.acquire()).await {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => return None,
            Err(_) => {
                tracing::warn!(tx_hash = %request.tx_hash, "timed out collecting transaction receipt");
                return None;
            }
        };
        let response = tokio::time::timeout_at(
            deadline,
            submitter.get_transaction_receipt_details(request.sender, request.tx_hash),
        )
        .await;
        drop(permit);
        match response {
            Ok(Ok(Some(receipt))) => {
                return Some((
                    request.labels,
                    ReceiptGasSample {
                        gas_used: receipt.gas_used,
                        effective_gas_price: receipt.effective_gas_price,
                    },
                ));
            }
            Ok(Ok(None)) => {}
            Ok(Err(error)) => {
                tracing::debug!(%error, tx_hash = %request.tx_hash, "receipt lookup failed");
            }
            Err(_) => {
                tracing::warn!(tx_hash = %request.tx_hash, "timed out collecting transaction receipt");
                return None;
            }
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            tracing::warn!(tx_hash = %request.tx_hash, "timed out collecting transaction receipt");
            return None;
        }
        tokio::time::sleep(RECEIPT_POLL_INTERVAL.min(deadline - now)).await;
    }
}

fn percentile(samples: &[U256], percentile: usize) -> U256 {
    let index = (samples.len() * percentile / 100).min(samples.len() - 1);
    samples[index]
}

fn u256_to_f64(value: U256) -> f64 {
    value.to_string().parse().expect("a U256 decimal value always fits in a finite f64")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn labels(input: &str) -> ReceiptMetricLabels {
        BTreeMap::from([("input".to_string(), input.to_string())])
    }

    #[test]
    fn records_successful_receipt_metrics() {
        let mut accumulator = ReceiptMetricsAccumulator::default();
        accumulator.record(
            labels("transfer"),
            ReceiptGasSample {
                gas_used: U256::from(21_000),
                effective_gas_price: Some(U256::from(2)),
            },
        );

        let metrics = accumulator.into_metrics();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].gas_used.count, 1);
        assert_eq!(metrics[0].gas_used.mean, Some(21_000.0));
        assert_eq!(metrics[0].effective_gas_price.min, Some(2.0));
        assert_eq!(metrics[0].fee_paid.p99, Some(42_000.0));
    }

    #[test]
    fn missing_fee_fields_only_record_gas_used() {
        let mut accumulator = ReceiptMetricsAccumulator::default();
        accumulator.record(
            labels("deposit"),
            ReceiptGasSample { gas_used: U256::from(30_000), effective_gas_price: None },
        );

        let metrics = accumulator.into_metrics();
        assert_eq!(metrics[0].gas_used.count, 1);
        assert_eq!(metrics[0].effective_gas_price, ReceiptMetricDistribution::default());
        assert_eq!(metrics[0].fee_paid, ReceiptMetricDistribution::default());

        let value = serde_json::to_value(&metrics[0]).unwrap();
        assert_eq!(value["effective_gas_price"]["count"], 0);
        assert_eq!(value["effective_gas_price"]["min"], serde_json::Value::Null);
        assert_eq!(value["fee_paid"]["mean"], serde_json::Value::Null);
    }

    #[test]
    fn percentiles_match_latency_convention() {
        let mut accumulator = ReceiptMetricsAccumulator::default();
        for value in 1..=100 {
            accumulator.record(
                labels("activity"),
                ReceiptGasSample {
                    gas_used: U256::from(value),
                    effective_gas_price: Some(U256::from(value)),
                },
            );
        }

        let distribution = &accumulator.into_metrics()[0].gas_used;
        assert_eq!(distribution.min, Some(1.0));
        assert_eq!(distribution.mean, Some(50.5));
        assert_eq!(distribution.p50, Some(51.0));
        assert_eq!(distribution.p95, Some(96.0));
        assert_eq!(distribution.p99, Some(100.0));
    }

    #[test]
    fn groups_multiple_inputs_in_deterministic_serialized_order() {
        let mut accumulator = ReceiptMetricsAccumulator::default();
        accumulator.record(
            labels("withdrawal"),
            ReceiptGasSample {
                gas_used: U256::from(50_000),
                effective_gas_price: Some(U256::from(3)),
            },
        );
        accumulator.record(
            labels("deposit"),
            ReceiptGasSample {
                gas_used: U256::from(25_000),
                effective_gas_price: Some(U256::from(2)),
            },
        );

        let metrics = accumulator.into_metrics();
        assert_eq!(metrics[0].labels["input"], "deposit");
        assert_eq!(metrics[1].labels["input"], "withdrawal");
        assert_eq!(
            serde_json::to_value(metrics).unwrap(),
            json!([
                {
                    "labels": {"input": "deposit"},
                    "gas_used": {
                        "count": 1, "min": 25000.0, "mean": 25000.0,
                        "p50": 25000.0, "p95": 25000.0, "p99": 25000.0
                    },
                    "effective_gas_price": {
                        "count": 1, "min": 2.0, "mean": 2.0,
                        "p50": 2.0, "p95": 2.0, "p99": 2.0
                    },
                    "fee_paid": {
                        "count": 1, "min": 50000.0, "mean": 50000.0,
                        "p50": 50000.0, "p95": 50000.0, "p99": 50000.0
                    }
                },
                {
                    "labels": {"input": "withdrawal"},
                    "gas_used": {
                        "count": 1, "min": 50000.0, "mean": 50000.0,
                        "p50": 50000.0, "p95": 50000.0, "p99": 50000.0
                    },
                    "effective_gas_price": {
                        "count": 1, "min": 3.0, "mean": 3.0,
                        "p50": 3.0, "p95": 3.0, "p99": 3.0
                    },
                    "fee_paid": {
                        "count": 1, "min": 150000.0, "mean": 150000.0,
                        "p50": 150000.0, "p95": 150000.0, "p99": 150000.0
                    }
                }
            ])
        );
    }
}
