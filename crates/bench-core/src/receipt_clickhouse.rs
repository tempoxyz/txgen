//! ClickHouse storage for granular transaction receipt gas records.

use crate::{clickhouse::ClickHouseClient, receipt_metrics::ReceiptGasRecord};
use eyre::{bail, Result, WrapErr};
use serde::Serialize;

/// Default number of granular receipt rows written per ClickHouse insert.
pub const DEFAULT_CLICKHOUSE_RECEIPT_BATCH_SIZE: usize = 50_000;

/// Insert granular receipt gas records into `txgen_receipt_gas` in synchronous batches.
///
/// Gas quantities are encoded as decimal strings so JSON serialization does not
/// lose precision before ClickHouse parses them as `UInt256` values.
pub fn insert_receipt_gas_records(
    client: &ClickHouseClient,
    run_id: uuid::Uuid,
    records: &[ReceiptGasRecord],
    batch_size: usize,
) -> Result<()> {
    if batch_size == 0 {
        bail!("ClickHouse receipt gas batch size must be greater than zero");
    }

    for (batch_index, records) in records.chunks(batch_size).enumerate() {
        let rows = records
            .iter()
            .map(|record| ReceiptGasRow::new(run_id, record))
            .collect::<Result<Vec<_>>>()?;
        client.insert_rows_synchronous("txgen_receipt_gas", &rows).wrap_err_with(|| {
            format!("failed to insert ClickHouse receipt gas batch {batch_index}")
        })?;
    }

    Ok(())
}

/// Insert granular receipt gas records using [`DEFAULT_CLICKHOUSE_RECEIPT_BATCH_SIZE`].
pub fn insert_receipt_gas_records_with_default_batch_size(
    client: &ClickHouseClient,
    run_id: uuid::Uuid,
    records: &[ReceiptGasRecord],
) -> Result<()> {
    insert_receipt_gas_records(client, run_id, records, DEFAULT_CLICKHOUSE_RECEIPT_BATCH_SIZE)
}

#[derive(Debug, Serialize)]
struct ReceiptGasRow {
    run_id: uuid::Uuid,
    tx_hash: String,
    sender: Option<String>,
    labels_json: String,
    scenario_instance: Option<u64>,
    success: bool,
    block_number: Option<u64>,
    block_hash: Option<String>,
    gas_used: String,
    effective_gas_price: Option<String>,
    fee_paid: Option<String>,
}

impl ReceiptGasRow {
    fn new(run_id: uuid::Uuid, record: &ReceiptGasRecord) -> Result<Self> {
        Ok(Self {
            run_id,
            tx_hash: record.tx_hash.to_string(),
            sender: record.sender.map(|sender| sender.to_string()),
            labels_json: serde_json::to_string(&record.labels)
                .wrap_err("failed to serialize receipt gas labels")?,
            scenario_instance: record.scenario_instance,
            success: record.success,
            block_number: record.block_number,
            block_hash: record.block_hash.map(|hash| hash.to_string()),
            gas_used: record.gas_used.to_string(),
            effective_gas_price: record.effective_gas_price.map(|price| price.to_string()),
            fee_paid: record.fee_paid().map(|fee| fee.to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, TxHash, B256, U256};
    use std::{
        collections::BTreeMap,
        io::{Read, Write},
        net::TcpListener,
        sync::mpsc,
        thread,
    };

    fn record(tx_byte: u8, gas_used: U256) -> ReceiptGasRecord {
        ReceiptGasRecord {
            tx_hash: TxHash::repeat_byte(tx_byte),
            sender: Some(Address::repeat_byte(0x11)),
            labels: BTreeMap::from([
                ("step".to_string(), "submit".to_string()),
                ("chain".to_string(), "zone".to_string()),
            ]),
            scenario_instance: Some(7),
            success: true,
            block_number: Some(42),
            block_hash: Some(B256::repeat_byte(0x22)),
            gas_used,
            effective_gas_price: Some(U256::from(3)),
        }
    }

    fn serve_requests(count: usize) -> (String, mpsc::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, receiver) = mpsc::channel();

        thread::spawn(move || {
            for _ in 0..count {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 4096];
                let mut expected_len = None;

                loop {
                    let read = stream.read(&mut buffer).unwrap();
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);

                    if let Some(header_end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        let header_end = header_end + 4;
                        let headers = String::from_utf8_lossy(&request[..header_end]);
                        let content_length = headers.lines().find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        });
                        expected_len = Some(header_end + content_length.unwrap_or(0));
                    }

                    if expected_len.is_some_and(|len| request.len() >= len) {
                        break;
                    }
                }

                sender.send(String::from_utf8(request).unwrap()).unwrap();
                write!(stream, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .unwrap();
            }
        });

        (format!("http://{address}"), receiver)
    }

    #[test]
    fn serializes_exact_decimal_values_and_nullable_fields() {
        let run_id = uuid::Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap();
        let gas_used = U256::from(1) << 200;
        let row = ReceiptGasRow::new(run_id, &record(0x33, gas_used)).unwrap();
        let value = serde_json::to_value(row).unwrap();

        assert_eq!(value["run_id"], run_id.to_string());
        assert_eq!(value["tx_hash"], TxHash::repeat_byte(0x33).to_string());
        assert_eq!(value["sender"], Address::repeat_byte(0x11).to_string());
        assert_eq!(value["labels_json"], r#"{"chain":"zone","step":"submit"}"#);
        assert_eq!(value["scenario_instance"], 7);
        assert_eq!(value["success"], true);
        assert_eq!(value["block_number"], 42);
        assert_eq!(value["block_hash"], B256::repeat_byte(0x22).to_string());
        assert_eq!(value["gas_used"], gas_used.to_string());
        assert_eq!(value["effective_gas_price"], "3");
        assert_eq!(value["fee_paid"], (gas_used * U256::from(3)).to_string());

        let mut missing = record(0x44, U256::from(21_000));
        missing.sender = None;
        missing.scenario_instance = None;
        missing.block_number = None;
        missing.block_hash = None;
        missing.effective_gas_price = None;
        let value = serde_json::to_value(ReceiptGasRow::new(run_id, &missing).unwrap()).unwrap();
        assert!(value["sender"].is_null());
        assert!(value["scenario_instance"].is_null());
        assert!(value["block_number"].is_null());
        assert!(value["block_hash"].is_null());
        assert!(value["effective_gas_price"].is_null());
        assert!(value["fee_paid"].is_null());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn inserts_synchronous_bounded_batches() {
        let (url, requests) = serve_requests(2);
        let client = ClickHouseClient::new(url, "analytics", None, None).unwrap();
        let run_id = uuid::Uuid::new_v4();
        let records = [
            record(0x01, U256::from(21_000)),
            record(0x02, U256::from(22_000)),
            record(0x03, U256::from(23_000)),
        ];

        insert_receipt_gas_records(&client, run_id, &records, 2).unwrap();

        let first = requests.recv().unwrap();
        let second = requests.recv().unwrap();
        for request in [&first, &second] {
            assert!(request.contains("analytics.txgen_receipt_gas"));
            assert!(request.contains("async_insert=0"));
            assert!(request.contains("wait_for_async_insert=1"));
        }
        assert_eq!(first.matches("\"tx_hash\"").count(), 2);
        assert_eq!(second.matches("\"tx_hash\"").count(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rejects_zero_batch_size_without_making_a_request() {
        let client = ClickHouseClient::new("http://127.0.0.1:1", "default", None, None).unwrap();
        let error = insert_receipt_gas_records(
            &client,
            uuid::Uuid::new_v4(),
            &[record(0x01, U256::from(21_000))],
            0,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("batch size must be greater than zero"));
    }
}
