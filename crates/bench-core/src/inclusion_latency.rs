//! Submit-to-inclusion latency derived from collected receipts.
//!
//! The latency of one transaction is the timestamp of the block that included
//! it minus the load generator's wall-clock time when it started the
//! `eth_sendRawTransaction` request. This is the latency a customer sees from
//! submitting a transaction until it is on chain.
//!
//! The two clocks belong to different machines: block timestamps come from the
//! proposing validator, submission times from the load generator. Clock skew
//! between them shifts every sample by the same offset and is accepted. Values
//! are signed and never clamped, since skew or a block whose timestamp was set
//! before a late transaction joined its payload can make a sample negative.
//!
//! Block timestamps come from the per-block statistics already fetched for the
//! report (millisecond precision on Tempo via `timestampMillisPart`), so this
//! adds no RPC requests.

use crate::{
    metrics::{percentile, BlockStats},
    receipt_metrics::{ReceiptGasRecord, ReceiptMetricLabels},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// Distribution of submit-to-inclusion latencies, in milliseconds.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct InclusionLatencyDistribution {
    /// Number of included transactions with a known submission time.
    pub count: u64,
    /// Minimum latency.
    pub min_ms: Option<f64>,
    /// Arithmetic mean latency.
    pub mean_ms: Option<f64>,
    /// 50th percentile using the benchmark latency percentile convention.
    pub p50_ms: Option<f64>,
    /// 90th percentile using the benchmark latency percentile convention.
    pub p90_ms: Option<f64>,
    /// 95th percentile using the benchmark latency percentile convention.
    pub p95_ms: Option<f64>,
    /// 99th percentile using the benchmark latency percentile convention.
    pub p99_ms: Option<f64>,
    /// Maximum latency.
    pub max_ms: Option<f64>,
}

impl InclusionLatencyDistribution {
    /// Summarize unsorted latency samples in milliseconds.
    pub fn from_samples(mut samples: Vec<i64>) -> Self {
        if samples.is_empty() {
            return Self::default();
        }

        samples.sort_unstable();
        let count = samples.len() as u64;
        let mean = samples.iter().map(|&value| value as f64).sum::<f64>() / count as f64;
        let at = |p| Some(percentile(&samples, p) as f64);

        Self {
            count,
            min_ms: Some(samples[0] as f64),
            mean_ms: Some(mean),
            p50_ms: at(50),
            p90_ms: at(90),
            p95_ms: at(95),
            p99_ms: at(99),
            max_ms: Some(samples[samples.len() - 1] as f64),
        }
    }
}

/// Submit-to-inclusion latency for one unique label set, such as one template.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InclusionLatencyGroup {
    /// Workload labels supplied when the transaction was submitted.
    pub labels: ReceiptMetricLabels,
    /// Latency distribution for this label set.
    #[serde(flatten)]
    pub latency: InclusionLatencyDistribution,
}

/// Submit-to-inclusion latency overall and per label set.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct InclusionLatency {
    /// Every included transaction with a known submission time.
    pub overall: InclusionLatencyDistribution,
    /// One distribution per label set, in deterministic label order.
    pub groups: Vec<InclusionLatencyGroup>,
}

impl InclusionLatency {
    /// Aggregate records that carry both a submission time and a block timestamp.
    ///
    /// Returns `None` when no record does, so reports omit the section.
    pub fn from_records(records: &[ReceiptGasRecord]) -> Option<Self> {
        let mut overall = Vec::new();
        let mut groups = BTreeMap::<&ReceiptMetricLabels, Vec<i64>>::new();
        for record in records {
            if let Some(latency) = record.inclusion_latency_ms() {
                overall.push(latency);
                groups.entry(&record.labels).or_default().push(latency);
            }
        }

        if overall.is_empty() {
            return None;
        }

        Some(Self {
            overall: InclusionLatencyDistribution::from_samples(overall),
            groups: groups
                .into_iter()
                .map(|(labels, samples)| InclusionLatencyGroup {
                    labels: labels.clone(),
                    latency: InclusionLatencyDistribution::from_samples(samples),
                })
                .collect(),
        })
    }
}

/// Set each record's `block_timestamp_ms` from the already-collected block stats.
///
/// Records whose block is not among `blocks` keep their current value.
pub fn attach_block_timestamps(records: &mut [ReceiptGasRecord], blocks: &[BlockStats]) {
    let timestamps: HashMap<u64, u64> =
        blocks.iter().map(|block| (block.number, block.timestamp_ms)).collect();
    for record in records {
        if let Some(&timestamp) = record.block_number.and_then(|number| timestamps.get(&number)) {
            record.block_timestamp_ms = Some(timestamp);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{TxHash, U256};

    fn record(input: &str, block_number: u64, submitted_at: Option<u64>) -> ReceiptGasRecord {
        ReceiptGasRecord {
            tx_hash: TxHash::repeat_byte(block_number as u8),
            sender: None,
            labels: BTreeMap::from([("input".to_string(), input.to_string())]),
            scenario_instance: None,
            success: true,
            block_number: Some(block_number),
            block_hash: None,
            gas_used: U256::from(21_000),
            effective_gas_price: None,
            submitted_at_unix_ms: submitted_at,
            block_timestamp_ms: None,
        }
    }

    fn block(number: u64, timestamp_ms: u64) -> BlockStats {
        BlockStats {
            number,
            timestamp_ms,
            tx_count: 1,
            gas_used: 21_000,
            gas_limit: 30_000_000,
            block_time_ms: None,
            new_payload_ms: None,
            forkchoice_updated_ms: None,
            new_payload_server_latency_us: None,
            persistence_wait_us: None,
            execution_cache_wait_us: None,
            sparse_trie_wait_us: None,
        }
    }

    #[test]
    fn percentiles_follow_latency_convention() {
        let distribution = InclusionLatencyDistribution::from_samples((1..=100).rev().collect());

        assert_eq!(distribution.count, 100);
        assert_eq!(distribution.min_ms, Some(1.0));
        assert_eq!(distribution.mean_ms, Some(50.5));
        assert_eq!(distribution.p50_ms, Some(51.0));
        assert_eq!(distribution.p90_ms, Some(91.0));
        assert_eq!(distribution.p95_ms, Some(96.0));
        assert_eq!(distribution.p99_ms, Some(100.0));
        assert_eq!(distribution.max_ms, Some(100.0));
        assert_eq!(
            InclusionLatencyDistribution::from_samples(Vec::new()),
            InclusionLatencyDistribution::default()
        );
    }

    #[test]
    fn subtracts_submission_time_from_block_timestamp_per_template_and_overall() {
        let mut records = vec![
            record("transfer", 10, Some(1_000)),
            record("transfer", 11, Some(1_100)),
            record("swap", 11, Some(1_450)),
            // Clock skew can place submission after the block timestamp.
            record("swap", 10, Some(1_300)),
            // Untracked transactions and blocks outside the report are skipped.
            record("transfer", 10, None),
            record("transfer", 99, Some(1_000)),
        ];
        attach_block_timestamps(&mut records, &[block(10, 1_250), block(11, 1_500)]);

        assert_eq!(records[0].inclusion_latency_ms(), Some(250));
        assert_eq!(records[3].inclusion_latency_ms(), Some(-50));
        assert_eq!(records[5].block_timestamp_ms, None);

        let latency = InclusionLatency::from_records(&records).unwrap();
        assert_eq!(latency.overall.count, 4);
        assert_eq!(latency.overall.min_ms, Some(-50.0));
        assert_eq!(latency.overall.max_ms, Some(400.0));
        assert_eq!(latency.groups.len(), 2);
        assert_eq!(latency.groups[0].labels["input"], "swap");
        assert_eq!(latency.groups[0].latency.count, 2);
        assert_eq!(latency.groups[0].latency.p50_ms, Some(50.0));
        assert_eq!(latency.groups[1].labels["input"], "transfer");
        assert_eq!(latency.groups[1].latency.p50_ms, Some(400.0));
        assert_eq!(latency.groups[1].latency.p99_ms, Some(400.0));

        let value = serde_json::to_value(&latency).unwrap();
        assert_eq!(value["overall"]["p90_ms"], 400.0);
        assert_eq!(value["groups"][0]["labels"]["input"], "swap");
        assert_eq!(value["groups"][0]["count"], 2);

        assert_eq!(InclusionLatency::from_records(&records[4..5]), None);
    }
}
