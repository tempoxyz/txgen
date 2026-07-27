//! Receipt-based transaction gas metric collection and aggregation.

use crate::sender::RpcSubmitter;
use alloy_network::primitives::ReceiptResponse;
use alloy_primitives::{Address, TxHash, B256, U256};
use futures::{stream::FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
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
        let _ =
            self.requests.send(ReceiptRequest { sender, tx_hash, labels, scenario_instance: None });
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
    /// Start deferred receipt collection with bounded RPC concurrency.
    pub fn start(submitter: RpcSubmitter, workers: usize) -> Self {
        Self::start_with_timing(submitter, workers, RECEIPT_POLL_INTERVAL, RECEIPT_TIMEOUT)
    }

    fn start_with_timing(
        submitter: RpcSubmitter,
        workers: usize,
        poll_interval: Duration,
        timeout: Duration,
    ) -> Self {
        let (requests, receiver) = mpsc::unbounded_channel();
        let (finish, finished) = oneshot::channel();
        let handle = ReceiptCollectorHandle { requests };
        let task = tokio::spawn(run_collector(
            receiver,
            finished,
            submitter,
            workers.max(1),
            poll_interval,
            timeout,
        ));

        Self { handle, finish: Some(finish), task }
    }

    /// Return a cloneable registration handle for submission paths.
    pub fn handle(&self) -> ReceiptCollectorHandle {
        self.handle.clone()
    }

    /// Stop accepting transactions, poll queued receipts, and aggregate results.
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
    finish: oneshot::Receiver<()>,
    submitter: RpcSubmitter,
    workers: usize,
    poll_interval: Duration,
    timeout: Duration,
) -> ReceiptCollection {
    // Receipt metrics are post-processing. Do not compete with the measured
    // submission workload for RPC, CPU, or connection capacity. Registrations
    // remain queued until the sender signals that the benchmark has finished.
    if finish.await.is_err() {
        return ReceiptCollection::default();
    }
    receiver.close();

    let ready_at = tokio::time::Instant::now();
    let mut records = Vec::new();
    let mut attempts = FuturesUnordered::new();
    let mut seen = BTreeSet::new();
    let mut pending = VecDeque::new();

    while let Some(request) = receiver.recv().await {
        if seen.insert(request.dedup_key()) {
            pending.push_back(ScheduledReceiptRequest { ready_at, request });
        }
    }
    drop(seen);

    // Submission and queue preparation may run longer than the receipt
    // timeout. Start the deadline only when the first RPC can be dispatched.
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline && !pending.is_empty() {
            tracing::warn!(
                pending_receipts = pending.len(),
                "receipt collection deadline elapsed with queued requests"
            );
            pending.clear();
        }
        while attempts.len() < workers &&
            pending.front().is_some_and(|request| request.ready_at <= now)
        {
            let request = pending.pop_front().expect("front was checked").request;
            let submitter = submitter.clone();
            attempts.push(collect_receipt_attempt(submitter, request, deadline));
        }

        if pending.is_empty() && attempts.is_empty() {
            break;
        }

        let next_ready = pending.front().map(|request| request.ready_at);
        tokio::select! {
            result = attempts.next(), if !attempts.is_empty() => {
                match result {
                    Some(ReceiptAttempt::Collected(record)) => records.push(record),
                    Some(ReceiptAttempt::Pending(request)) => {
                        let now = tokio::time::Instant::now();
                        if now < deadline {
                            pending.push_back(ScheduledReceiptRequest {
                                ready_at: (now + poll_interval).min(deadline),
                                request,
                            });
                        } else {
                            warn_receipt_timeout(request.tx_hash);
                        }
                    }
                    Some(ReceiptAttempt::TimedOut(tx_hash)) => {
                        warn_receipt_timeout(tx_hash);
                    }
                    None => {}
                }
            }
            _ = async {
                if let Some(next_ready) = next_ready {
                    tokio::time::sleep_until(next_ready).await;
                }
            }, if attempts.len() < workers && next_ready.is_some() => {}
        }
    }

    ReceiptCollection::from_records(records)
}

struct ScheduledReceiptRequest {
    ready_at: tokio::time::Instant,
    request: ReceiptRequest,
}

enum ReceiptAttempt {
    Collected(ReceiptGasRecord),
    Pending(ReceiptRequest),
    TimedOut(TxHash),
}

async fn collect_receipt_attempt(
    submitter: RpcSubmitter,
    request: ReceiptRequest,
    deadline: tokio::time::Instant,
) -> ReceiptAttempt {
    let response = tokio::time::timeout_at(
        deadline,
        submitter.get_transaction_receipt_details(request.sender, request.tx_hash),
    )
    .await;
    match response {
        Ok(Ok(Some(receipt))) => ReceiptAttempt::Collected(ReceiptGasRecord {
            tx_hash: request.tx_hash,
            sender: request.sender,
            labels: request.labels,
            scenario_instance: request.scenario_instance,
            success: receipt.receipt.status(),
            block_number: receipt.receipt.block_number(),
            block_hash: receipt.receipt.block_hash(),
            gas_used: receipt.gas_used,
            effective_gas_price: receipt.effective_gas_price,
        }),
        Ok(Ok(None)) => ReceiptAttempt::Pending(request),
        Ok(Err(error)) => {
            tracing::debug!(%error, tx_hash = %request.tx_hash, "receipt lookup failed");
            ReceiptAttempt::Pending(request)
        }
        Err(_) => ReceiptAttempt::TimedOut(request.tx_hash),
    }
}

fn warn_receipt_timeout(tx_hash: TxHash) {
    tracing::warn!(%tx_hash, "timed out collecting transaction receipt");
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
    use crate::sender::SenderConfig;
    use alloy_network::AnyNetwork;
    use alloy_provider::{DynProvider, Provider, ProviderBuilder};
    use alloy_transport::mock::Asserter;
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

    fn mocked_submitter(asserter: Asserter) -> RpcSubmitter {
        let provider: DynProvider<AnyNetwork> = ProviderBuilder::new_with_network::<AnyNetwork>()
            .connect_mocked_client(asserter)
            .erased();
        RpcSubmitter::new(vec![provider], SenderConfig { rate_limit: 0, max_concurrent: 1 })
            .unwrap()
    }

    fn receipt_json(transaction_hash: TxHash) -> serde_json::Value {
        json!({
            "transactionHash": transaction_hash,
            "transactionIndex": "0x0",
            "blockHash": TxHash::repeat_byte(0x44),
            "blockNumber": "0x1",
            "from": Address::repeat_byte(0x55),
            "to": Address::repeat_byte(0x66),
            "cumulativeGasUsed": "0x5208",
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x2",
            "contractAddress": null,
            "logs": [],
            "logsBloom": format!("0x{}", "00".repeat(256)),
            "status": "0x1",
            "type": "0x2"
        })
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
        };

        assert_ne!(request(Some(1)).dedup_key(), request(Some(2)).dedup_key());
        assert_eq!(request(Some(1)).dedup_key(), request(Some(1)).dedup_key());
    }

    #[tokio::test(start_paused = true)]
    async fn queued_request_gets_full_timeout_after_deferred_collection_starts() {
        let asserter = Asserter::new();
        let tx_hash = TxHash::repeat_byte(0x11);
        asserter.push_success(&Option::<serde_json::Value>::None);
        asserter.push_success(&receipt_json(tx_hash));
        let collector = ReceiptCollector::start_with_timing(
            mocked_submitter(asserter.clone()),
            1,
            Duration::from_millis(10),
            Duration::from_millis(100),
        );
        collector.handle().track(None, tx_hash, labels("aged"));

        tokio::time::advance(Duration::from_millis(200)).await;

        // Tracking remains RPC-free even after the eventual polling timeout has
        // elapsed; the timeout begins only once finish starts post-processing.
        assert_eq!(asserter.read_q().len(), 2);
        let collection = collector.finish().await;

        assert_eq!(collection.records.len(), 1);
        assert_eq!(collection.records[0].tx_hash, tx_hash);
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn missing_receipt_does_not_starve_ready_receipt_with_one_worker() {
        let asserter = Asserter::new();
        let missing_hash = TxHash::repeat_byte(0x11);
        let ready_hash = TxHash::repeat_byte(0x22);
        asserter.push_success(&Option::<serde_json::Value>::None);
        asserter.push_success(&receipt_json(ready_hash));
        let collector = ReceiptCollector::start_with_timing(
            mocked_submitter(asserter.clone()),
            1,
            Duration::from_millis(10),
            Duration::from_millis(30),
        );
        let handle = collector.handle();
        handle.track(None, missing_hash, labels("missing"));
        handle.track(None, ready_hash, labels("ready"));

        assert_eq!(asserter.read_q().len(), 2);
        let collection = collector.finish().await;

        assert_eq!(collection.records.len(), 1);
        assert_eq!(collection.records[0].tx_hash, ready_hash);
        assert_eq!(collection.records[0].labels, labels("ready"));
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn canceled_finish_discards_queued_requests_without_rpc_work() {
        let asserter = Asserter::new();
        let tx_hash = TxHash::repeat_byte(0x11);
        asserter.push_success(&receipt_json(tx_hash));
        let (requests, receiver) = mpsc::unbounded_channel();
        requests
            .send(ReceiptRequest {
                sender: None,
                tx_hash,
                labels: labels("canceled"),
                scenario_instance: None,
            })
            .unwrap();
        let (finish, finished) = oneshot::channel();
        drop(finish);

        let collection = run_collector(
            receiver,
            finished,
            mocked_submitter(asserter.clone()),
            1,
            Duration::from_millis(10),
            Duration::from_millis(100),
        )
        .await;

        assert!(collection.records.is_empty());
        assert_eq!(asserter.read_q().len(), 1);
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
