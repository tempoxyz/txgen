//! Receipt-based transaction gas metric collection and aggregation.

use crate::{metrics::percentile, sender::RpcSubmitter};
use alloy_eips::BlockId;
use alloy_network::{primitives::ReceiptResponse, AnyNetwork};
use alloy_primitives::{Address, TxHash, B256, U256, U64};
use alloy_provider::{DynProvider, Provider};
use eyre::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, Semaphore},
    task::{JoinHandle, JoinSet},
};

const RECEIPT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(300);
const BLOCK_RECEIPT_FETCH_CONCURRENCY: usize = 8;

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
    /// Load-generator wall-clock time, in Unix milliseconds, when the
    /// submission request started. `None` for untracked transactions.
    #[serde(default)]
    pub submitted_at_unix_ms: Option<u64>,
    /// Timestamp of the including block in Unix milliseconds, when known.
    #[serde(default)]
    pub block_timestamp_ms: Option<u64>,
}

impl ReceiptGasRecord {
    /// The gas fields of this record.
    pub fn sample(&self) -> ReceiptGasSample {
        ReceiptGasSample { gas_used: self.gas_used, effective_gas_price: self.effective_gas_price }
    }

    /// Including block timestamp minus submission time, in milliseconds.
    ///
    /// Signed because the two timestamps come from different clocks; see
    /// [`crate::inclusion_latency`].
    pub fn inclusion_latency_ms(&self) -> Option<i64> {
        let included = i64::try_from(self.block_timestamp_ms?).ok()?;
        let submitted = i64::try_from(self.submitted_at_unix_ms?).ok()?;
        included.checked_sub(submitted)
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
        let mean = samples.iter().map(f64::from).sum::<f64>() / count as f64;

        Self {
            count,
            min: Some(f64::from(samples[0])),
            mean: Some(mean),
            p50: Some(f64::from(percentile(&samples, 50))),
            p95: Some(f64::from(percentile(&samples, 95))),
            p99: Some(f64::from(percentile(&samples, 99))),
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
        let metrics = Self::metrics_for_records(&records);
        Self { metrics, records }
    }

    pub fn metrics_for_records(records: &[ReceiptGasRecord]) -> ReceiptMetrics {
        let mut accumulator = ReceiptMetricsAccumulator::default();
        for record in records {
            accumulator.record_ref(&record.labels, record.sample());
        }

        accumulator.into_metrics()
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
        Self::push_sample(group, sample);
    }

    /// Record a confirmed receipt without cloning `labels` when the group already exists.
    pub fn record_ref(&mut self, labels: &ReceiptMetricLabels, sample: ReceiptGasSample) {
        if let Some(group) = self.groups.get_mut(labels) {
            Self::push_sample(group, sample);
        } else {
            let group = self.groups.entry(labels.clone()).or_default();
            Self::push_sample(group, sample);
        }
    }

    fn push_sample(group: &mut ReceiptMetricSamples, sample: ReceiptGasSample) {
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
    submitted_at_unix_ms: Option<u64>,
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
        self.track_submission(sender, tx_hash, labels, None);
    }

    /// Register an accepted transaction with the wall-clock time, in Unix
    /// milliseconds, at which its submission started.
    ///
    /// The time is retained on the granular record for submit-to-inclusion
    /// latency. Calls made after the collector starts finishing are ignored.
    pub fn track_submission(
        &self,
        sender: Option<Address>,
        tx_hash: TxHash,
        labels: ReceiptMetricLabels,
        submitted_at_unix_ms: Option<u64>,
    ) {
        let _ = self.requests.send(ReceiptRequest {
            sender,
            tx_hash,
            labels,
            scenario_instance: None,
            submitted_at_unix_ms,
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
            submitted_at_unix_ms: None,
            tracked_at: tokio::time::Instant::now(),
        });
    }
}

/// Deferred receipt collector for benchmark-wide gas metrics.
///
/// Unlike [`ReceiptCollector`], this collector performs no RPC work while the
/// workload is running. It only retains the labels attached to accepted
/// transactions so the post-run block receipts can preserve the existing
/// report shape. [`finish`](Self::finish) fetches all receipts in the selected
/// block range and excludes Tempo system transactions.
pub struct BlockReceiptCollector {
    handle: ReceiptCollectorHandle,
    receiver: mpsc::UnboundedReceiver<ReceiptRequest>,
}

impl BlockReceiptCollector {
    /// Start a collector that does not issue any requests until [`finish`](Self::finish).
    pub fn start() -> Self {
        let (requests, receiver) = mpsc::unbounded_channel();
        Self { handle: ReceiptCollectorHandle { requests }, receiver }
    }

    /// Return a cloneable registration handle for submission paths.
    pub fn handle(&self) -> ReceiptCollectorHandle {
        self.handle.clone()
    }

    /// Fetch and aggregate receipts for the block range after submission ends.
    pub async fn finish(
        self,
        provider: &DynProvider<AnyNetwork>,
        start_block: u64,
        end_block: u64,
    ) -> Result<ReceiptCollection> {
        self.finish_with_providers(std::slice::from_ref(provider), start_block, end_block).await
    }

    /// Finish receipt collection, fanning out block-receipt RPCs across all
    /// provided endpoints so multi-validator runs do not bottleneck on one node.
    pub async fn finish_with_providers(
        self,
        providers: &[DynProvider<AnyNetwork>],
        start_block: u64,
        end_block: u64,
    ) -> Result<ReceiptCollection> {
        let Self { handle, mut receiver } = self;
        drop(handle);

        let mut tracked = HashMap::<TxHash, Vec<ReceiptRequest>>::new();
        let mut seen = HashSet::new();
        while let Some(request) = receiver.recv().await {
            if seen.insert(request.dedup_key()) {
                tracked.entry(request.tx_hash).or_default().push(request);
            }
        }

        collect_block_receipts(providers, start_block, end_block, tracked).await
    }
}

/// Lightweight deserialization target for `eth_getBlockReceipts` that skips
/// `logs`, `logsBloom`, and extra network-specific fields unused by gas metrics.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(untagged)]
enum ReceiptStatus {
    Bool(bool),
    Quantity(U64),
}

impl ReceiptStatus {
    fn is_success(self) -> bool {
        match self {
            Self::Bool(flag) => flag,
            Self::Quantity(value) => !value.is_zero(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MinimalBlockReceipt {
    transaction_hash: TxHash,
    from: Address,
    #[serde(default)]
    status: Option<ReceiptStatus>,
    #[serde(default)]
    block_number: Option<U64>,
    #[serde(default)]
    block_hash: Option<B256>,
    gas_used: U256,
    #[serde(default)]
    effective_gas_price: Option<U256>,
}

impl MinimalBlockReceipt {
    fn is_success(&self) -> bool {
        self.status.is_none_or(ReceiptStatus::is_success)
    }
}

async fn collect_block_receipts(
    providers: &[DynProvider<AnyNetwork>],
    start_block: u64,
    end_block: u64,
    mut tracked: HashMap<TxHash, Vec<ReceiptRequest>>,
) -> Result<ReceiptCollection> {
    if start_block > end_block || providers.is_empty() {
        return Ok(ReceiptCollection::default());
    }

    // Dynamic work-stealing across providers: each provider runs a small pool of
    // concurrent fetchers pulling block numbers from a shared counter so fast
    // local validators absorb most blocks instead of stalling on high-RTT regions.
    let per_provider = if providers.len() == 1 {
        BLOCK_RECEIPT_FETCH_CONCURRENCY
    } else {
        (64 / providers.len()).clamp(2, 6)
    };
    let primary = providers[0].clone();
    let next_block = Arc::new(AtomicU64::new(start_block));
    let mut workers = JoinSet::new();
    for (provider_index, provider) in providers.iter().enumerate() {
        let is_primary = provider_index == 0;
        for _ in 0..per_provider {
            let provider = provider.clone();
            let primary = primary.clone();
            let next_block = Arc::clone(&next_block);
            workers.spawn(async move {
                let mut batches = Vec::new();
                loop {
                    let block_number = next_block.fetch_add(1, Ordering::Relaxed);
                    if block_number > end_block {
                        break;
                    }
                    let mut retire_secondary = false;
                    let receipts = match fetch_minimal_block_receipts(&provider, block_number).await
                    {
                        Ok(Some(receipts)) => receipts,
                        Ok(None) if !is_primary => {
                            fetch_minimal_block_receipts(&primary, block_number).await?.ok_or_else(
                                || eyre::eyre!("receipts for block {block_number} not found"),
                            )?
                        }
                        Err(error) if !is_primary => {
                            tracing::debug!(
                                %error,
                                block_number,
                                "secondary receipt provider failed; falling back to primary"
                            );
                            retire_secondary = true;
                            fetch_minimal_block_receipts(&primary, block_number).await?.ok_or_else(
                                || eyre::eyre!("receipts for block {block_number} not found"),
                            )?
                        }
                        Ok(None) => {
                            eyre::bail!("receipts for block {block_number} not found");
                        }
                        Err(error) => return Err(error),
                    };
                    batches.push((block_number, receipts));
                    if retire_secondary {
                        break;
                    }
                }
                Ok::<_, eyre::Report>(batches)
            });
        }
    }

    let mut ordered_blocks = Vec::with_capacity((end_block - start_block + 1) as usize);
    while let Some(worker_result) = workers.join_next().await {
        let batches = worker_result.wrap_err("block receipt worker task panicked")??;
        ordered_blocks.extend(batches);
    }
    ordered_blocks.sort_unstable_by_key(|(block_number, _)| *block_number);

    let total_receipts: usize = ordered_blocks.iter().map(|(_, r)| r.len()).sum();
    let mut records = Vec::with_capacity(total_receipts);
    for (_, block_receipts) in ordered_blocks {
        for receipt in block_receipts {
            if receipt.gas_used.is_zero() {
                continue;
            }

            let tx_hash = receipt.transaction_hash;
            if let Some(requests) = tracked.remove(&tx_hash) {
                for request in requests {
                    records.push(ReceiptGasRecord {
                        tx_hash,
                        sender: request.sender.or(Some(receipt.from)),
                        labels: request.labels,
                        scenario_instance: request.scenario_instance,
                        success: receipt.is_success(),
                        block_number: receipt.block_number.map(|n| n.to::<u64>()),
                        block_hash: receipt.block_hash,
                        gas_used: receipt.gas_used,
                        effective_gas_price: receipt.effective_gas_price,
                        submitted_at_unix_ms: request.submitted_at_unix_ms,
                        block_timestamp_ms: None,
                    });
                }
            } else {
                records.push(ReceiptGasRecord {
                    tx_hash,
                    sender: Some(receipt.from),
                    labels: BTreeMap::new(),
                    scenario_instance: None,
                    success: receipt.is_success(),
                    block_number: receipt.block_number.map(|n| n.to::<u64>()),
                    block_hash: receipt.block_hash,
                    gas_used: receipt.gas_used,
                    effective_gas_price: receipt.effective_gas_price,
                    submitted_at_unix_ms: None,
                    block_timestamp_ms: None,
                });
            }
        }
    }

    Ok(ReceiptCollection::from_records(records))
}

async fn fetch_minimal_block_receipts(
    provider: &DynProvider<AnyNetwork>,
    block_number: u64,
) -> Result<Option<Vec<MinimalBlockReceipt>>> {
    provider
        .raw_request("eth_getBlockReceipts".into(), (BlockId::number(block_number),))
        .await
        .wrap_err_with(|| format!("failed to fetch receipts for block {block_number}"))
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
                    submitted_at_unix_ms: request.submitted_at_unix_ms,
                    block_timestamp_ms: None,
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

/// Sum the exact fees from all records that supplied an effective gas price.
///
/// Returns `None` when no record has a calculable fee or when the aggregate
/// exceeds `U256`.
pub fn total_fees_paid(records: &[ReceiptGasRecord]) -> Option<U256> {
    let mut fees = records.iter().filter_map(|record| record.sample().fee_paid());
    let first = fees.next()?;
    fees.try_fold(first, U256::checked_add)
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
            submitted_at_unix_ms: None,
            block_timestamp_ms: None,
        }
    }

    #[test]
    fn retains_granular_confirmed_receipt_fields() {
        let record = granular_record("transfer", Some(7), Some(U256::from(2)));
        let collection = ReceiptCollection::from_records(vec![record.clone()]);

        assert_eq!(collection.records, vec![record.clone()]);
        assert_eq!(record.sample().fee_paid(), Some(U256::from(42_000)));
        assert_eq!(collection.metrics.len(), 1);
        assert_eq!(collection.metrics[0].labels, labels("transfer"));
        assert_eq!(collection.metrics[0].gas_used.count, 1);
        assert_eq!(collection.metrics[0].fee_paid.mean, Some(42_000.0));
    }

    #[test]
    fn totals_exact_fees_and_skips_records_without_prices() {
        let first = granular_record("transfer", None, Some(U256::from(2)));
        let second = granular_record("transfer", None, Some(U256::from(3)));
        let missing_price = granular_record("transfer", None, None);

        assert_eq!(total_fees_paid(&[first, second, missing_price]), Some(U256::from(105_000)));
        assert_eq!(total_fees_paid(&[]), None);
    }

    #[test]
    fn omits_overflowing_fee_total() {
        let mut first = granular_record("transfer", None, Some(U256::from(1)));
        first.gas_used = U256::MAX;
        let second = granular_record("transfer", None, Some(U256::from(1)));

        assert_eq!(total_fees_paid(&[first, second]), None);
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
            submitted_at_unix_ms: None,
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
    fn minimal_block_receipt_ignores_logs_and_extra_fields() {
        let raw = json!({
            "transactionHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
            "from": "0x2222222222222222222222222222222222222222",
            "to": "0x3333333333333333333333333333333333333333",
            "status": "0x1",
            "blockNumber": "0x2a",
            "blockHash": "0x4444444444444444444444444444444444444444444444444444444444444444",
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x3b9aca00",
            "cumulativeGasUsed": "0x5208",
            "logsBloom": "0x00",
            "logs": [{"address": "0x3333333333333333333333333333333333333333", "topics": [], "data": "0x"}],
            "feeToken": "0x5555555555555555555555555555555555555555",
            "feePayer": "0x2222222222222222222222222222222222222222"
        });
        let parsed: MinimalBlockReceipt = serde_json::from_value(raw).unwrap();
        assert_eq!(parsed.transaction_hash, TxHash::repeat_byte(0x11));
        assert_eq!(parsed.from, Address::repeat_byte(0x22));
        assert!(parsed.is_success());
        assert_eq!(parsed.block_number.map(|n| n.to::<u64>()), Some(42));
        assert_eq!(parsed.block_hash, Some(B256::repeat_byte(0x44)));
        assert_eq!(parsed.gas_used, U256::from(21_000));
        assert_eq!(parsed.effective_gas_price, Some(U256::from(1_000_000_000u64)));

        let failed: MinimalBlockReceipt = serde_json::from_value(json!({
            "transactionHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
            "from": "0x2222222222222222222222222222222222222222",
            "status": "0x0",
            "gasUsed": "0x5208"
        }))
        .unwrap();
        assert!(!failed.is_success());
        assert_eq!(failed.effective_gas_price, None);
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
