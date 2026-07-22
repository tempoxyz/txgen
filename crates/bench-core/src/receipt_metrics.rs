//! Receipt-based transaction gas metric collection and aggregation.

use crate::sender::RpcSubmitter;
use alloy_network::primitives::ReceiptResponse;
use alloy_primitives::{Address, TxHash, B256, U256};
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

/// Granular gas data retained for one confirmed transaction receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptGasRecord {
    /// Hash of the confirmed transaction.
    pub tx_hash: TxHash,
    /// Transaction sender, when supplied.
    pub sender: Option<Address>,
    /// Workload or scenario labels supplied when the transaction was accepted.
    pub labels: ReceiptMetricLabels,
    /// Scenario instance that submitted the transaction, when applicable.
    pub scenario_instance: Option<u64>,
    /// Whether the outer transaction completed successfully.
    pub success: bool,
    /// Block number containing the transaction, when supplied by the receipt.
    pub block_number: Option<u64>,
    /// Block hash containing the transaction, when supplied by the receipt.
    pub block_hash: Option<B256>,
    /// Gas consumed by the outer transaction.
    pub gas_used: U256,
    /// Effective gas price, when the receipt supplied a fee field.
    pub effective_gas_price: Option<U256>,
}

impl ReceiptGasRecord {
    /// Calculate the transaction fee when an effective gas price is available.
    ///
    /// Valid Ethereum receipt field widths fit exactly in a `U256`. An
    /// out-of-range RPC response is omitted instead of wrapping the fee.
    pub fn fee_paid(&self) -> Option<U256> {
        self.effective_gas_price.and_then(|price| self.gas_used.checked_mul(price))
    }
}

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

/// Aggregated gas metrics and their underlying confirmed receipt records.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReceiptCollection {
    /// Gas distributions grouped by workload or scenario labels.
    pub metrics: ReceiptMetrics,
    /// One granular record for every collected confirmed transaction receipt.
    pub records: Vec<ReceiptGasRecord>,
}

impl ReceiptCollection {
    fn from_records(records: Vec<ReceiptGasRecord>) -> Self {
        let mut accumulator = ReceiptMetricsAccumulator::default();
        for record in &records {
            accumulator.record(
                record.labels.clone(),
                ReceiptGasSample {
                    gas_used: record.gas_used,
                    effective_gas_price: record.effective_gas_price,
                },
            );
        }

        Self { metrics: accumulator.into_metrics(), records }
    }
}

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
    scenario_instance: Option<u64>,
    tracked_at: tokio::time::Instant,
}

impl ReceiptRequest {
    fn dedup_key(&self) -> (TxHash, ReceiptMetricLabels, Option<u64>) {
        (self.tx_hash, self.labels.clone(), self.scenario_instance)
    }
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
            scenario_instance: None,
            tracked_at: tokio::time::Instant::now(),
        });
    }

    /// Register an accepted scenario transaction for receipt collection.
    ///
    /// The instance is retained on the granular record and participates in
    /// deduplication, but does not alter aggregate metric labels.
    pub fn track_for_instance(
        &self,
        sender: Option<Address>,
        tx_hash: TxHash,
        labels: ReceiptMetricLabels,
        scenario_instance: u64,
    ) {
        let _ = self.requests.send(ReceiptRequest {
            sender,
            tx_hash,
            labels,
            scenario_instance: Some(scenario_instance),
            tracked_at: tokio::time::Instant::now(),
        });
    }
}

/// Background collector for confirmed transaction receipt gas fields.
pub struct ReceiptCollector {
    handle: ReceiptCollectorHandle,
    finish: Option<oneshot::Sender<()>>,
    task: JoinHandle<ReceiptCollection>,
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
    pub async fn finish(mut self) -> ReceiptCollection {
        if let Some(finish) = self.finish.take() {
            let _ = finish.send(());
        }
        drop(self.handle);

        match self.task.await {
            Ok(metrics) => metrics,
            Err(error) => {
                tracing::warn!(%error, "receipt collector task failed");
                ReceiptCollection::default()
            }
        }
    }
}

async fn run_collector(
    mut receiver: mpsc::UnboundedReceiver<ReceiptRequest>,
    mut finish: oneshot::Receiver<()>,
    submitter: RpcSubmitter,
    workers: usize,
) -> ReceiptCollection {
    let mut records = Vec::new();
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
                record_task_result(&mut records, result);
            }
            request = receiver.recv(), if !finishing || !receiver.is_empty() => {
                match request {
                    Some(request) => {
                        if !seen.insert(request.dedup_key()) {
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

    ReceiptCollection::from_records(records)
}

fn record_task_result(
    records: &mut Vec<ReceiptGasRecord>,
    result: Option<Result<Option<ReceiptGasRecord>, tokio::task::JoinError>>,
) {
    match result {
        Some(Ok(Some(record))) => records.push(record),
        Some(Ok(None)) | None => {}
        Some(Err(error)) => tracing::warn!(%error, "receipt polling task failed"),
    }
}

async fn collect_receipt(
    submitter: RpcSubmitter,
    request: ReceiptRequest,
    semaphore: Arc<Semaphore>,
) -> Option<ReceiptGasRecord> {
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
                return Some(ReceiptGasRecord {
                    tx_hash: request.tx_hash,
                    sender: request.sender,
                    labels: request.labels,
                    scenario_instance: request.scenario_instance,
                    success: receipt.receipt.status(),
                    block_number: receipt.receipt.block_number(),
                    block_hash: receipt.receipt.block_hash(),
                    gas_used: receipt.gas_used,
                    effective_gas_price: receipt.effective_gas_price,
                });
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

    fn granular_record(
        input: &str,
        scenario_instance: Option<u64>,
        effective_gas_price: Option<U256>,
    ) -> ReceiptGasRecord {
        ReceiptGasRecord {
            tx_hash: TxHash::repeat_byte(0x11),
            sender: Some(Address::repeat_byte(0x22)),
            labels: labels(input),
            scenario_instance,
            success: true,
            block_number: Some(42),
            block_hash: Some(B256::repeat_byte(0x33)),
            gas_used: U256::from(21_000),
            effective_gas_price,
        }
    }

    #[test]
    fn retains_granular_confirmed_receipt_fields() {
        let record = granular_record("transfer", Some(7), Some(U256::from(2)));
        let collection = ReceiptCollection::from_records(vec![record.clone()]);

        assert_eq!(collection.records, vec![record.clone()]);
        assert_eq!(record.fee_paid(), Some(U256::from(42_000)));
        assert_eq!(collection.metrics.len(), 1);
        assert_eq!(collection.metrics[0].labels, labels("transfer"));
        assert_eq!(collection.metrics[0].gas_used.count, 1);
        assert_eq!(collection.metrics[0].fee_paid.mean, Some(42_000.0));
    }

    #[test]
    fn instance_is_retained_without_changing_aggregate_labels() {
        let first = granular_record("transfer", Some(1), Some(U256::from(2)));
        let mut second = granular_record("transfer", Some(2), Some(U256::from(3)));
        second.tx_hash = TxHash::repeat_byte(0x44);

        let collection = ReceiptCollection::from_records(vec![first, second]);

        assert_eq!(collection.records[0].scenario_instance, Some(1));
        assert_eq!(collection.records[1].scenario_instance, Some(2));
        assert_eq!(collection.metrics.len(), 1);
        assert_eq!(collection.metrics[0].labels, labels("transfer"));
        assert_eq!(collection.metrics[0].gas_used.count, 2);
    }

    #[test]
    fn deduplication_distinguishes_scenario_instances() {
        let request = |scenario_instance| ReceiptRequest {
            sender: Some(Address::repeat_byte(0x22)),
            tx_hash: TxHash::repeat_byte(0x11),
            labels: labels("transfer"),
            scenario_instance,
            tracked_at: tokio::time::Instant::now(),
        };

        assert_ne!(request(Some(1)).dedup_key(), request(Some(2)).dedup_key());
        assert_eq!(request(Some(1)).dedup_key(), request(Some(1)).dedup_key());
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
