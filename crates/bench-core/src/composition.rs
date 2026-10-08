//! Included non-system transaction composition for the measured benchmark blocks.
//! Gas shares use fee-paying receipt gas, which can differ from header execution gas.

use crate::{BlockStats, ReceiptGasRecord};
use alloy_primitives::{TxHash, U256};
use eyre::Result;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KindComposition {
    pub input: Option<String>,
    pub tx_count: u64,
    pub tx_count_pct: f64,
    pub gas_used: String,
    pub gas_pct: f64,
    pub reverted_tx_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TransactionComposition {
    pub tx_count: u64,
    pub gas_used: String,
    pub kinds: Vec<KindComposition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlockComposition {
    pub block_number: u64,
    #[serde(flatten)]
    pub composition: TransactionComposition,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunComposition {
    pub block_count: u64,
    pub summary: TransactionComposition,
    pub blocks: Vec<BlockComposition>,
}

#[derive(Default)]
struct KindTotals {
    tx_count: u64,
    gas_used: U256,
    reverted_tx_count: u64,
}

impl KindTotals {
    fn record(&mut self, receipt: &ReceiptGasRecord) -> Result<()> {
        self.tx_count += 1;
        self.gas_used = self
            .gas_used
            .checked_add(receipt.gas_used)
            .ok_or_else(|| eyre::eyre!("composition gas total overflow"))?;
        self.reverted_tx_count += u64::from(!receipt.success);
        Ok(())
    }
}

fn summarize(kinds: BTreeMap<Option<String>, KindTotals>) -> Result<TransactionComposition> {
    let tx_count = kinds.values().map(|kind| kind.tx_count).sum::<u64>();
    let gas_used = kinds.values().try_fold(U256::ZERO, |total, kind| {
        total
            .checked_add(kind.gas_used)
            .ok_or_else(|| eyre::eyre!("composition gas total overflow"))
    })?;
    let gas_total = f64::from(&gas_used);
    Ok(TransactionComposition {
        tx_count,
        gas_used: gas_used.to_string(),
        kinds: kinds
            .into_iter()
            .map(|(input, kind)| KindComposition {
                input,
                tx_count: kind.tx_count,
                tx_count_pct: if tx_count == 0 {
                    0.0
                } else {
                    kind.tx_count as f64 * 100.0 / tx_count as f64
                },
                gas_used: kind.gas_used.to_string(),
                gas_pct: if gas_total == 0.0 {
                    0.0
                } else {
                    f64::from(&kind.gas_used) * 100.0 / gas_total
                },
                reverted_tx_count: kind.reverted_tx_count,
            })
            .collect(),
    })
}

/// Aggregate records from validated block receipts. Completeness is checked by the collector
/// before system receipts are filtered and receipts are expanded into labeled records.
pub fn block_composition(
    receipts: &[ReceiptGasRecord],
    blocks: &[BlockStats],
) -> Result<RunComposition> {
    let mut per_block = blocks
        .iter()
        .map(|block| (block.number, BTreeMap::<Option<String>, KindTotals>::new()))
        .collect::<BTreeMap<_, _>>();
    let mut summary = BTreeMap::<Option<String>, KindTotals>::new();
    let mut seen = HashSet::<TxHash>::new();
    for receipt in receipts {
        if receipt.gas_used.is_zero() {
            continue;
        }
        if let Some(number) = receipt.block_number &&
            let Some(kinds) = per_block.get_mut(&number) &&
            seen.insert(receipt.tx_hash)
        {
            let input = receipt.labels.get("input").cloned();
            kinds.entry(input.clone()).or_default().record(receipt)?;
            summary.entry(input).or_default().record(receipt)?;
        }
    }
    let compositions = per_block
        .into_iter()
        .map(|(block_number, kinds)| {
            Ok(BlockComposition { block_number, composition: summarize(kinds)? })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RunComposition {
        block_count: blocks.len() as u64,
        summary: summarize(summary)?,
        blocks: compositions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;

    fn block(number: u64, gas_used: u64, tx_count: usize) -> BlockStats {
        BlockStats {
            number,
            timestamp_ms: 0,
            tx_count,
            gas_used,
            gas_limit: 1_000_000,
            block_time_ms: None,
            new_payload_ms: None,
            forkchoice_updated_ms: None,
            new_payload_server_latency_us: None,
            persistence_wait_us: None,
            execution_cache_wait_us: None,
            sparse_trie_wait_us: None,
        }
    }

    fn receipt(
        identity: u8,
        number: u64,
        input: Option<&str>,
        gas: u64,
        success: bool,
    ) -> ReceiptGasRecord {
        ReceiptGasRecord {
            tx_hash: B256::repeat_byte(identity),
            sender: None,
            labels: input
                .map(|input| BTreeMap::from([("input".into(), input.into())]))
                .unwrap_or_default(),
            scenario_instance: None,
            success,
            block_number: Some(number),
            block_hash: None,
            gas_used: U256::from(gas),
            effective_gas_price: None,
        }
    }

    #[test]
    fn reports_actual_count_and_gas_shares_with_future_labels_and_reverts() {
        let receipts = vec![
            receipt(1, 10, Some("vault_deposit"), 30, true),
            receipt(2, 10, Some("zone_deposit"), 10, false),
            receipt(3, 11, Some("mpp_open_only.open"), 10, true),
            receipt(4, 11, Some("future_preset"), 50, true),
        ];
        let composition =
            block_composition(&receipts, &[block(10, 40, 2), block(11, 60, 2)]).unwrap();
        assert_eq!(composition.block_count, 2);
        assert_eq!(composition.summary.tx_count, 4);
        assert_eq!(composition.summary.gas_used, "100");
        let vault = composition
            .summary
            .kinds
            .iter()
            .find(|kind| kind.input.as_deref() == Some("vault_deposit"))
            .unwrap();
        assert_eq!((vault.tx_count_pct, vault.gas_pct), (25.0, 30.0));
        let zone = composition
            .summary
            .kinds
            .iter()
            .find(|kind| kind.input.as_deref() == Some("zone_deposit"))
            .unwrap();
        assert_eq!(zone.reverted_tx_count, 1);
        assert_eq!(composition.blocks[0].composition.kinds[0].gas_pct, 75.0);
    }

    #[test]
    fn excludes_outside_measurement_and_system_receipts_and_deduplicates_hashes() {
        let tracked = receipt(1, 10, Some("transfer"), 10, true);
        let receipts = vec![
            tracked.clone(),
            tracked,
            receipt(2, 9, Some("setup"), 100, true),
            receipt(3, 10, Some("system"), 0, true),
            receipt(4, 10, None, 30, true),
        ];
        let composition =
            block_composition(&receipts, &[block(10, 40, 3), block(11, 0, 0)]).unwrap();
        assert_eq!(composition.summary.tx_count, 2);
        assert_eq!(composition.summary.kinds[0].input, None);
        assert_eq!(composition.summary.kinds[0].gas_pct, 75.0);
        assert_eq!(composition.blocks[1].composition.tx_count, 0);
        assert_eq!(composition.blocks[1].composition.gas_used, "0");
    }

    #[test]
    fn tip1016_composition_uses_receipt_gas_not_header_execution_gas() {
        // Values from the failed Tempo PR #8042 benchmark: the difference is 1,715,000 state gas.
        let receipts = [
            receipt(1, 8, Some("transfer"), 3_000_000, true),
            receipt(2, 8, Some("storage"), 1_967_361, false),
        ];
        let blocks = [block(8, 3_252_361, 2)];
        let composition = block_composition(&receipts, &blocks).unwrap();
        assert_eq!(composition.summary.gas_used, "4967361");
        assert_eq!(composition.summary.tx_count, 2);
        assert_eq!(composition.summary.kinds[0].reverted_tx_count, 1);
        assert_eq!(composition.summary.kinds[0].gas_pct, 1_967_361.0 * 100.0 / 4_967_361.0);
        assert_eq!(blocks[0].gas_used, 3_252_361);
    }

    #[test]
    fn execution_floor_does_not_change_receipt_composition() {
        // 25k execution + 245k state is 270k fee gas; the execution floor raises
        // header gas to 50k, so adding state gas to header gas would overcount.
        let receipts = [receipt(1, 10, Some("storage"), 270_000, true)];
        let composition = block_composition(&receipts, &[block(10, 50_000, 1)]).unwrap();
        assert_eq!(composition.summary.gas_used, "270000");
        assert_eq!(composition.summary.kinds[0].gas_pct, 100.0);
    }
}
