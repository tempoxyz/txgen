use super::*;
use alloy_network::{AnyNetwork, Ethereum, TransactionBuilder};
use alloy_provider::ProviderBuilder;
use alloy_rpc_types_eth::TransactionRequest;
use axum::{extract::State, routing::post, Json, Router};
use bench_core::{MetricsCollector, RunClock, Sender, SenderConfig, SourceTx};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

struct TestAdapter;

impl NetworkAdapter for TestAdapter {
    type Template = serde_yaml::Value;
    type Network = Ethereum;
    type SignContext = ();

    fn network_name() -> &'static str {
        "sequence-test"
    }

    fn build_request(
        &self,
        _template: Self::Template,
        ctx: &mut BuildContext<'_>,
    ) -> Result<TxRequest<TransactionRequest>> {
        let address = ctx.accounts.get_by_index("users", 0)?.address();
        let key = address.0 .0;
        let request = TransactionRequest::default()
            .with_chain_id(ctx.chain_id)
            .with_nonce(ctx.next_nonce(key))
            .with_gas_limit(21_000)
            .with_max_fee_per_gas(1)
            .with_max_priority_fee_per_gas(1)
            .with_to(address);
        Ok(TxRequest {
            request,
            signer_pool: "users".into(),
            signer_index: 0,
            key,
            sign_context: (),
        })
    }
}

fn generate_sequence(step_count: usize) -> Result<Vec<GeneratedTx>> {
    let steps = "      - template: transfer\n".repeat(step_count);
    let spec = WorkloadSpec::parse(&format!(
        r#"
chain_id: 1337
accounts:
  users:
    mnemonic: "test test test test test test test test test test test junk"
    range: [0, 1]
templates:
  transfer: {{}}
sequences:
  transfers:
    steps:
{steps}
mix:
  - sequence: transfers
    weight: 1
"#
    ))?;
    let accounts = AccountManager::from_spec(&spec.accounts)?;
    let artifacts = ArtifactManager::empty();
    let mut nonces = NonceTracker::new();
    let mut rng = StdRng::seed_from_u64(1);
    let mut context =
        BuildContext::new(spec.chain_id, &spec.gas, &accounts, &artifacts, &mut nonces, &mut rng);
    let mut output = Vec::new();
    generate_txs(
        &TestAdapter,
        &spec,
        GenerationLimit { count: Some(step_count as u64), duration: None },
        1,
        &HashMap::new(),
        &mut context,
        &mut NdjsonWriter::new(&mut output),
    )?;
    String::from_utf8(output)?
        .lines()
        .map(|line| serde_json::from_str::<SourceTx>(line)?.into_generated_tx())
        .collect()
}

type Requests = Arc<Mutex<Vec<String>>>;

async fn rpc(State(requests): State<Requests>, Json(request): Json<Value>) -> Json<Value> {
    let method = request["method"].as_str().unwrap();
    let mut requests = requests.lock().unwrap();
    requests.push(method.to_owned());
    let result = match method {
        "eth_sendRawTransaction" => {
            let raw = request["params"][0].as_str().unwrap().parse::<Bytes>().unwrap();
            json!(alloy_primitives::keccak256(raw))
        }
        "eth_getTransactionReceipt" => {
            // Keep each transaction pending for one poll, so ordering must wait
            // for an actual receipt instead of merely dispatching the query.
            if requests.iter().filter(|method| *method == "eth_getTransactionReceipt").count() % 2 ==
                1
            {
                Value::Null
            } else {
                json!({
                    "transactionHash": request["params"][0],
                    "transactionIndex": "0x0",
                    "blockHash": alloy_primitives::B256::repeat_byte(0x44),
                    "blockNumber": "0x1",
                    "from": Address::repeat_byte(0x55),
                    "to": Address::repeat_byte(0x66),
                    "cumulativeGasUsed": "0x5208",
                    "gasUsed": "0x5208",
                    "contractAddress": null,
                    "logs": [],
                    "logsBloom": format!("0x{}", "00".repeat(256)),
                    "status": "0x1",
                    "effectiveGasPrice": "0x1",
                    "type": "0x2"
                })
            }
        }
        other => panic!("unexpected RPC method: {other}"),
    };
    Json(json!({ "jsonrpc": "2.0", "id": request["id"], "result": result }))
}

async fn send_sequence(step_count: usize) -> Result<Vec<String>> {
    let transactions = generate_sequence(step_count)?;
    assert_eq!(transactions.len(), step_count);
    let requests = Requests::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let app = Router::new().route("/", post(rpc)).with_state(requests.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let provider =
        ProviderBuilder::new_with_network::<AnyNetwork>().connect_http(url.parse()?).erased();
    let mut sender = Sender::new(
        vec![provider],
        SenderConfig { rate_limit: 0, max_concurrent: 2 },
        MetricsCollector::new_with_latencies(RunClock::new(), true),
    );
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        for transaction in transactions {
            sender.send(transaction).await?;
        }
        sender.flush().await
    })
    .await;
    server.abort();
    result??;
    let recorded = requests.lock().unwrap().clone();
    Ok(recorded)
}

#[tokio::test]
async fn single_step_sequence_does_not_poll_receipts() -> Result<()> {
    assert_eq!(send_sequence(1).await?, ["eth_sendRawTransaction"]);
    Ok(())
}

#[tokio::test]
async fn multi_step_sequence_waits_for_previous_receipt() -> Result<()> {
    assert_eq!(
        send_sequence(2).await?,
        [
            "eth_sendRawTransaction",
            "eth_getTransactionReceipt",
            "eth_getTransactionReceipt",
            "eth_sendRawTransaction",
            "eth_getTransactionReceipt",
            "eth_getTransactionReceipt",
        ]
    );
    Ok(())
}
