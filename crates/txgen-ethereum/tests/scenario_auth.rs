use alloy_primitives::{keccak256, Address, Bytes, B256};
use axum::{extract::State, http::HeaderMap, routing::post, Json, Router};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{net::TcpListener, task::JoinHandle};
use txgen_cli::scenario::{execute_scenario, FailurePolicy, ScenarioExecutionConfig, ScenarioSpec};
use txgen_core::derive_mnemonic_signer;
use txgen_ethereum::EthereumAdapter;

const TEST_MNEMONIC: &str = "test test test test test test test test test test test junk";
const AUTH_HEADER: &str = "x-zone-auth";
const SENDER_ZERO_TOKEN: &str = "zone-token-zero";
const SENDER_ONE_TOKEN: &str = "zone-token-one";
const CHAIN_ID: u64 = 1;
const HEAD_BLOCK: u64 = 100;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RpcRole {
    Submission,
    Query,
}

#[derive(Clone)]
struct RpcState {
    role: RpcRole,
    shared: Arc<Mutex<SharedState>>,
}

#[derive(Default)]
struct SharedState {
    requests: Vec<ObservedRequest>,
    transaction_tokens: HashMap<B256, String>,
}

#[derive(Debug)]
struct ObservedRequest {
    role: RpcRole,
    method: String,
    auth: Option<String>,
    transaction_hash: Option<B256>,
}

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let suffix =
            SystemTime::now().duration_since(UNIX_EPOCH).expect("clock after epoch").as_nanos();
        let path = std::env::temp_dir()
            .join(format!("txgen-scenario-auth-{}-{suffix}", std::process::id()));
        fs::create_dir_all(&path).expect("create temporary test directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routes_sender_authenticated_requests_separately_from_queries() {
    let shared = Arc::new(Mutex::new(SharedState::default()));
    let (submission_url, submission_server) = spawn_rpc(RpcRole::Submission, shared.clone()).await;
    let (query_url, query_server) = spawn_rpc(RpcRole::Query, shared.clone()).await;
    let directory = TempDir::new();
    let sender_zero = derive_mnemonic_signer(TEST_MNEMONIC, 0).unwrap().address();
    let sender_one = derive_mnemonic_signer(TEST_MNEMONIC, 1).unwrap().address();

    write_fixture_files(directory.path(), &submission_url, &query_url, sender_zero, sender_one);
    let scenario = ScenarioSpec::load(&directory.path().join("scenario.yaml"))
        .expect("load authenticated scenario");
    let report = tokio::time::timeout(
        Duration::from_secs(10),
        execute_scenario::<EthereumAdapter>(
            scenario,
            ScenarioExecutionConfig {
                count: Some(1),
                duration: None,
                starts_per_second: 0.0,
                max_in_flight: 1,
                step_timeout: Some(Duration::from_secs(2)),
                seed: 42,
                failure_policy: FailurePolicy::Continue,
                transaction_rate: 0,
                max_rpc_in_flight: 2,
                sample_instances: 1,
            },
        ),
    )
    .await
    .expect("authenticated scenario execution timed out")
    .expect("authenticated scenario execution failed");

    submission_server.abort();
    query_server.abort();

    assert_eq!(report.started, 1);
    assert_eq!(report.completed, 1);
    assert_eq!(report.failed, 0);
    assert_eq!(report.timed_out, 0);
    assert_eq!(report.steps.len(), 5);
    assert!(report.steps.iter().all(|step| step.success == 1 && step.failed == 0));
    assert_eq!(report.sampled_instances.len(), 1);
    assert_eq!(report.sampled_instances[0].outcome, "completed");
    assert_eq!(report.receipt_metrics.len(), 2);
    for (input, step) in [("sender_zero", "zero_submission"), ("sender_one", "one_submission")] {
        let metrics = report
            .receipt_metrics
            .iter()
            .find(|metrics| metrics.labels.get("input").map(String::as_str) == Some(input))
            .expect("missing labeled receipt metrics");
        assert_eq!(metrics.labels.get("chain").map(String::as_str), Some("test"));
        assert_eq!(metrics.labels.get("step").map(String::as_str), Some(step));
        assert_eq!(metrics.gas_used.count, 1);
        assert_eq!(metrics.gas_used.p95, Some(21_000.0));
        assert_eq!(metrics.effective_gas_price.mean, Some(1.0));
        assert_eq!(metrics.fee_paid.p99, Some(21_000.0));
    }

    let shared = shared.lock().expect("RPC state lock");
    let submission_requests = shared
        .requests
        .iter()
        .filter(|request| request.role == RpcRole::Submission)
        .collect::<Vec<_>>();
    assert!(submission_requests.iter().all(|request| {
        matches!(request.method.as_str(), "eth_sendRawTransaction" | "eth_getTransactionReceipt")
    }));
    assert!(submission_requests.iter().all(|request| request.auth.is_some()));

    let send_tokens = submission_requests
        .iter()
        .filter(|request| request.method == "eth_sendRawTransaction")
        .map(|request| request.auth.as_deref())
        .collect::<Vec<_>>();
    assert_eq!(send_tokens, vec![Some(SENDER_ZERO_TOKEN), Some(SENDER_ONE_TOKEN)]);

    for (transaction_hash, expected_token) in &shared.transaction_tokens {
        let receipts = submission_requests
            .iter()
            .filter(|request| {
                request.method == "eth_getTransactionReceipt" &&
                    request.transaction_hash == Some(*transaction_hash)
            })
            .collect::<Vec<_>>();
        assert!(!receipts.is_empty(), "missing receipt request for {transaction_hash}");
        assert!(receipts
            .iter()
            .all(|request| request.auth.as_deref() == Some(expected_token.as_str())));
    }
    assert_eq!(shared.transaction_tokens.len(), 2);

    let query_requests =
        shared.requests.iter().filter(|request| request.role == RpcRole::Query).collect::<Vec<_>>();
    assert!(query_requests.iter().all(|request| request.auth.is_none()));
    assert!(query_requests.iter().all(|request| {
        matches!(
            request.method.as_str(),
            "eth_chainId" | "eth_getTransactionCount" | "eth_blockNumber" | "eth_getBlockByNumber"
        )
    }));
    assert!(query_requests.iter().any(|request| request.method == "eth_chainId"));
    assert!(query_requests.iter().any(|request| request.method == "eth_getTransactionCount"));
    assert!(query_requests.iter().any(|request| request.method == "eth_blockNumber"));
    assert!(query_requests.iter().any(|request| request.method == "eth_getBlockByNumber"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preflights_all_setup_sender_auth_before_dispatch() {
    let shared = Arc::new(Mutex::new(SharedState::default()));
    let (submission_url, submission_server) = spawn_rpc(RpcRole::Submission, shared.clone()).await;
    let (query_url, query_server) = spawn_rpc(RpcRole::Query, shared.clone()).await;
    let directory = TempDir::new();
    let sender_zero = derive_mnemonic_signer(TEST_MNEMONIC, 0).unwrap().address();
    let sender_one = derive_mnemonic_signer(TEST_MNEMONIC, 1).unwrap().address();

    write_fixture_files(directory.path(), &submission_url, &query_url, sender_zero, sender_one);
    fs::write(
        directory.path().join("sender-auth.json"),
        serde_json::to_vec_pretty(&json!({ sender_zero.to_string(): SENDER_ZERO_TOKEN })).unwrap(),
    )
    .unwrap();
    let workload_path = directory.path().join("workload.yaml");
    let workload = fs::read_to_string(&workload_path).unwrap();
    let setup = r#"setup:
  steps:
    - id: first
      tx:
        type: eip1559
        from: { pool: users, select: { index: 0 } }
        to: "0x0000000000000000000000000000000000000001"
        value: 0
        gas_limit: 21000
        max_fee_per_gas: 1000000000
        max_priority_fee_per_gas: 1000000000
    - id: second
      tx:
        type: eip1559
        from: { pool: users, select: { index: 1 } }
        to: "0x0000000000000000000000000000000000000002"
        value: 0
        gas_limit: 21000
        max_fee_per_gas: 1000000000
        max_priority_fee_per_gas: 1000000000

"#;
    fs::write(&workload_path, workload.replacen("templates:", &format!("{setup}templates:"), 1))
        .unwrap();

    let scenario = ScenarioSpec::load(&directory.path().join("scenario.yaml")).unwrap();
    let error = execute_scenario::<EthereumAdapter>(scenario, ScenarioExecutionConfig::default())
        .await
        .unwrap_err();
    submission_server.abort();
    query_server.abort();

    assert!(format!("{error:?}").contains("no sender authentication mapping"));
    assert!(shared.lock().unwrap().requests.iter().all(|request| {
        request.role != RpcRole::Submission || request.method != "eth_sendRawTransaction"
    }));
}

async fn spawn_rpc(role: RpcRole, shared: Arc<Mutex<SharedState>>) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock RPC");
    let address = listener.local_addr().expect("mock RPC address");
    let app = Router::new().route("/", post(handle_rpc)).with_state(RpcState { role, shared });
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve mock RPC");
    });
    (format!("http://{address}"), server)
}

async fn handle_rpc(
    State(state): State<RpcState>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Json<Value> {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let method = request.get("method").and_then(Value::as_str).unwrap_or_default().to_string();
    let params = request.get("params").cloned().unwrap_or_else(|| json!([]));
    let auth = headers
        .get(AUTH_HEADER)
        .map(|value| value.to_str().expect("authentication header is ASCII").to_string());
    let transaction_hash = match method.as_str() {
        "eth_sendRawTransaction" => params
            .get(0)
            .and_then(Value::as_str)
            .and_then(|raw| raw.parse::<Bytes>().ok())
            .map(|raw| keccak256(&raw)),
        "eth_getTransactionReceipt" => {
            params.get(0).and_then(Value::as_str).and_then(|hash| hash.parse::<B256>().ok())
        }
        _ => None,
    };

    state.shared.lock().expect("RPC state lock").requests.push(ObservedRequest {
        role: state.role,
        method: method.clone(),
        auth: auth.clone(),
        transaction_hash,
    });

    let result = match (state.role, method.as_str()) {
        (RpcRole::Query, "eth_chainId") => json!(quantity(CHAIN_ID)),
        (RpcRole::Query, "eth_getTransactionCount") => json!("0x0"),
        (RpcRole::Query, "eth_blockNumber") => json!(quantity(HEAD_BLOCK)),
        (RpcRole::Query, "eth_getBlockByNumber") => {
            block_value(HEAD_BLOCK, B256::repeat_byte(0x55))
        }
        (RpcRole::Submission, "eth_sendRawTransaction") => {
            let Some(transaction_hash) = transaction_hash else {
                return rpc_error(id, -32602, "missing raw transaction");
            };
            let Some(auth) = auth else {
                return rpc_error(id, -32001, "missing sender authentication");
            };
            state
                .shared
                .lock()
                .expect("RPC state lock")
                .transaction_tokens
                .insert(transaction_hash, auth);
            json!(transaction_hash)
        }
        (RpcRole::Submission, "eth_getTransactionReceipt") => {
            let Some(transaction_hash) = transaction_hash else {
                return rpc_error(id, -32602, "missing transaction hash");
            };
            let expected_token = state
                .shared
                .lock()
                .expect("RPC state lock")
                .transaction_tokens
                .get(&transaction_hash)
                .cloned();
            if auth != expected_token {
                return rpc_error(id, -32001, "incorrect sender authentication");
            }
            let sender = match expected_token.as_deref() {
                Some(SENDER_ZERO_TOKEN) => {
                    derive_mnemonic_signer(TEST_MNEMONIC, 0).unwrap().address()
                }
                Some(SENDER_ONE_TOKEN) => {
                    derive_mnemonic_signer(TEST_MNEMONIC, 1).unwrap().address()
                }
                _ => return rpc_error(id, -32001, "unknown sender authentication"),
            };
            receipt_value(transaction_hash, sender)
        }
        _ => return rpc_error(id, -32601, &format!("unsupported {method} on {:?} RPC", state.role)),
    };

    Json(json!({ "jsonrpc": "2.0", "id": id, "result": result }))
}

fn rpc_error(id: Value, code: i64, message: &str) -> Json<Value> {
    Json(json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message }
    }))
}

fn receipt_value(transaction_hash: B256, sender: Address) -> Value {
    json!({
        "status": "0x1",
        "cumulativeGasUsed": "0x5208",
        "logs": [],
        "logsBloom": format!("0x{}", "00".repeat(256)),
        "type": "0x2",
        "transactionHash": transaction_hash,
        "transactionIndex": "0x0",
        "blockHash": B256::repeat_byte(0x55),
        "blockNumber": quantity(HEAD_BLOCK),
        "gasUsed": "0x5208",
        "effectiveGasPrice": "0x1",
        "from": sender,
        "to": Address::repeat_byte(0x22),
        "contractAddress": null
    })
}

fn block_value(number: u64, hash: B256) -> Value {
    json!({
        "hash": hash,
        "parentHash": B256::ZERO,
        "sha3Uncles": B256::ZERO,
        "miner": Address::ZERO,
        "stateRoot": B256::ZERO,
        "transactionsRoot": B256::ZERO,
        "receiptsRoot": B256::ZERO,
        "logsBloom": format!("0x{}", "00".repeat(256)),
        "difficulty": "0x0",
        "number": quantity(number),
        "gasLimit": "0x1c9c380",
        "gasUsed": "0x0",
        "timestamp": "0x0",
        "extraData": "0x",
        "mixHash": B256::ZERO,
        "nonce": "0x0000000000000000",
        "baseFeePerGas": "0x0",
        "transactions": [],
        "uncles": []
    })
}

fn quantity(value: u64) -> String {
    format!("0x{value:x}")
}

fn write_fixture_files(
    directory: &Path,
    submission_url: &str,
    query_url: &str,
    sender_zero: Address,
    sender_one: Address,
) {
    fs::write(
        directory.join("sender-auth.json"),
        serde_json::to_vec_pretty(&json!({
            sender_zero.to_string(): SENDER_ZERO_TOKEN,
            sender_one.to_string(): SENDER_ONE_TOKEN,
        }))
        .expect("serialize sender map"),
    )
    .expect("write sender map");

    fs::write(
        directory.join("workload.yaml"),
        format!(
            r#"chain_id: {CHAIN_ID}
accounts:
  users:
    mnemonic: "{TEST_MNEMONIC}"
    range: [0, 2]
templates:
  sender_zero:
    type: eip1559
    from:
      pool: users
      select: {{ index: 0 }}
    to: "0x0000000000000000000000000000000000000001"
    value: 0
    gas_limit: 21000
    max_fee_per_gas: 1000000000
    max_priority_fee_per_gas: 1000000000
  sender_one:
    type: eip1559
    from:
      pool: users
      select: {{ index: 1 }}
    to: "0x0000000000000000000000000000000000000002"
    value: 0
    gas_limit: 21000
    max_fee_per_gas: 1000000000
    max_priority_fee_per_gas: 1000000000
"#,
        ),
    )
    .expect("write workload fixture");

    fs::write(
        directory.join("scenario.yaml"),
        format!(
            r#"version: 1
chains:
  test:
    network: ethereum
    rpc_url: "{submission_url}"
    query_rpc_url: "{query_url}"
    request_auth:
      sender_header:
        name: "{AUTH_HEADER}"
        map: ./sender-auth.json
        reload_interval: 1ms
    chain_id: auto
    workload: ./workload.yaml
scenario:
  name: authenticated-two-sender
  timeout: 2s
  steps:
    - checkpoint:
        chain: test
      save: before
    - submit:
        chain: test
        template: sender_zero
      save: zero_submission
    - wait_receipt:
        chain: test
        transaction_hash: {{ var: zero_submission.tx_hash }}
        sender: {{ var: zero_submission.sender }}
        poll_interval: 1ms
      save: zero_receipt
    - submit:
        chain: test
        template: sender_one
      save: one_submission
    - wait_receipt:
        chain: test
        transaction_hash: {{ var: one_submission.tx_hash }}
        sender: {{ var: one_submission.sender }}
        poll_interval: 1ms
      save: one_receipt
"#,
        ),
    )
    .expect("write scenario fixture");
}
