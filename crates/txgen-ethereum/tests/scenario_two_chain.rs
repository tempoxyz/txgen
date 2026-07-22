use alloy_primitives::{keccak256, Address, Bytes, B256};
use axum::{extract::State, routing::post, Json, Router};
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{net::TcpListener, task::JoinHandle};
use txgen_cli::scenario::{execute_scenario, FailurePolicy, ScenarioExecutionConfig, ScenarioSpec};
use txgen_ethereum::EthereumAdapter;

const TEST_MNEMONIC: &str = "test test test test test test test test test test test junk";
const EVENT_ADDRESS: Address = Address::repeat_byte(0x42);
const START_BLOCK: u64 = 10;
const EVENT_BLOCK: u64 = 11;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MockChain {
    X,
    Y,
}

#[derive(Clone)]
struct RpcState {
    chain: MockChain,
    chain_id: u64,
    receipt_status: bool,
    bridge: Arc<Mutex<BridgeState>>,
}

#[derive(Default)]
struct BridgeState {
    request_id: Option<B256>,
    x: ChainState,
    y: ChainState,
}

struct ChainState {
    head: u64,
    log: Option<Value>,
    submissions: usize,
    raw_submissions: Vec<Bytes>,
    log_queries: usize,
    log_ranges: Vec<(u64, u64)>,
    queried_before_event: bool,
}

impl Default for ChainState {
    fn default() -> Self {
        Self {
            head: START_BLOCK,
            log: None,
            submissions: 0,
            raw_submissions: Vec::new(),
            log_queries: 0,
            log_ranges: Vec::new(),
            queried_before_event: false,
        }
    }
}

impl BridgeState {
    fn chain(&self, chain: MockChain) -> &ChainState {
        match chain {
            MockChain::X => &self.x,
            MockChain::Y => &self.y,
        }
    }

    fn chain_mut(&mut self, chain: MockChain) -> &mut ChainState {
        match chain {
            MockChain::X => &mut self.x,
            MockChain::Y => &mut self.y,
        }
    }

    fn accept_submission(&mut self, chain: MockChain, raw: Bytes, tx_hash: B256) {
        let state = self.chain_mut(chain);
        state.submissions += 1;
        state.raw_submissions.push(raw);
        match chain {
            MockChain::X => {
                self.request_id = Some(tx_hash);
                self.y.head = EVENT_BLOCK;
                self.y.log = Some(event_log("RequestObserved(bytes32)", tx_hash, 0x71));
            }
            MockChain::Y => {
                let request_id = self.request_id.expect("chain X must submit before chain Y");
                self.x.head = EVENT_BLOCK;
                self.x.log = Some(event_log("ResponseObserved(bytes32)", request_id, 0x72));
            }
        }
    }
}

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let suffix =
            SystemTime::now().duration_since(UNIX_EPOCH).expect("clock after epoch").as_nanos();
        let path = std::env::temp_dir()
            .join(format!("txgen-two-chain-scenario-{}-{suffix}", std::process::id()));
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
async fn executes_two_chain_roundtrip_with_backfilled_events() {
    let bridge = Arc::new(Mutex::new(BridgeState::default()));
    let (x_url, x_server) = spawn_rpc(MockChain::X, 1_001, bridge.clone()).await;
    let (y_url, y_server) = spawn_rpc(MockChain::Y, 1_002, bridge.clone()).await;
    let directory = TempDir::new();

    write_fixture_files(directory.path(), &x_url, &y_url);
    let scenario = ScenarioSpec::load(&directory.path().join("scenario.yaml"))
        .expect("load two-chain scenario");
    let configuration = ScenarioExecutionConfig {
        count: Some(1),
        duration: None,
        starts_per_second: 0.0,
        max_in_flight: 1,
        step_timeout: Some(Duration::from_secs(2)),
        seed: 42,
        failure_policy: FailurePolicy::Continue,
        transaction_rate: 0,
        max_rpc_in_flight: 4,
        sample_instances: 1,
    };

    let report = tokio::time::timeout(
        Duration::from_secs(10),
        execute_scenario::<EthereumAdapter>(scenario, configuration),
    )
    .await
    .expect("scenario execution timed out")
    .expect("scenario execution failed");

    x_server.abort();
    y_server.abort();

    assert_eq!(report.started, 1);
    assert_eq!(report.completed, 1);
    assert_eq!(report.failed, 0);
    assert_eq!(report.timed_out, 0);
    assert_eq!(report.steps.len(), 6);
    assert!(report.steps.iter().all(|step| step.success == 1 && step.failed == 0));
    assert_eq!(report.sampled_instances.len(), 1);
    assert_eq!(report.sampled_instances[0].outcome, "completed");

    let bridge = bridge.lock().expect("bridge state lock");
    assert_eq!(bridge.x.submissions, 1);
    assert_eq!(bridge.y.submissions, 1);
    assert!(bridge.x.log_queries >= 2, "X event should be backfilled and rechecked");
    assert!(bridge.y.log_queries >= 2, "Y event should be backfilled and rechecked");
    assert_eq!(bridge.x.log_ranges.first(), Some(&(START_BLOCK, EVENT_BLOCK)));
    assert_eq!(bridge.y.log_ranges.first(), Some(&(START_BLOCK, EVENT_BLOCK)));
    assert!(!bridge.x.queried_before_event);
    assert!(!bridge.y.queried_before_event);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeded_runs_produce_the_same_signed_transactions() {
    let first = execute_seeded_roundtrip(0x5eed).await;
    let second = execute_seeded_roundtrip(0x5eed).await;
    assert_eq!(first, second);
}

async fn execute_seeded_roundtrip(seed: u64) -> (Vec<Bytes>, Vec<Bytes>) {
    let bridge = Arc::new(Mutex::new(BridgeState::default()));
    let (x_url, x_server) = spawn_rpc(MockChain::X, 1_001, bridge.clone()).await;
    let (y_url, y_server) = spawn_rpc(MockChain::Y, 1_002, bridge.clone()).await;
    let directory = TempDir::new();
    write_fixture_files(directory.path(), &x_url, &y_url);
    let scenario = ScenarioSpec::load(&directory.path().join("scenario.yaml")).unwrap();
    let report = execute_scenario::<EthereumAdapter>(
        scenario,
        ScenarioExecutionConfig {
            count: Some(1),
            duration: None,
            starts_per_second: 0.0,
            max_in_flight: 1,
            step_timeout: Some(Duration::from_secs(2)),
            seed,
            failure_policy: FailurePolicy::Continue,
            transaction_rate: 0,
            max_rpc_in_flight: 4,
            sample_instances: 0,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.completed, 1);
    x_server.abort();
    y_server.abort();
    let bridge = bridge.lock().unwrap();
    (bridge.x.raw_submissions.clone(), bridge.y.raw_submissions.clone())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn releases_account_lease_after_instance_timeout() {
    let bridge = Arc::new(Mutex::new(BridgeState::default()));
    let (rpc_url, server) = spawn_rpc(MockChain::X, 1_001, bridge).await;
    let directory = TempDir::new();
    write_fixture_files(directory.path(), &rpc_url, &rpc_url);
    fs::write(
        directory.path().join("lease-timeout.yaml"),
        format!(
            r#"version: 1
chains:
  x:
    network: ethereum
    rpc_url: "{rpc_url}"
    chain_id: auto
    workload: ./x-workload.yaml
scenario:
  name: lease-timeout
  bindings:
    user:
      account:
        pool: users
        select: lease
  steps:
    - wait_log:
        chain: x
        from_block: 0
        abi: BridgeEvents
        event: RequestObserved
        poll_interval: 1ms
        max_block_range: 100
      save: never_observed
"#
        ),
    )
    .expect("write timeout scenario");

    // The fixture has one account. The second instance can finish only if the
    // first timeout releases its lifetime lease.
    let scenario = ScenarioSpec::load(&directory.path().join("lease-timeout.yaml"))
        .expect("load timeout scenario");
    let configuration = ScenarioExecutionConfig {
        count: Some(2),
        duration: None,
        starts_per_second: 0.0,
        max_in_flight: 2,
        step_timeout: Some(Duration::from_millis(25)),
        seed: 7,
        failure_policy: FailurePolicy::Continue,
        transaction_rate: 0,
        max_rpc_in_flight: 2,
        sample_instances: 2,
    };
    let report = tokio::time::timeout(
        Duration::from_secs(2),
        execute_scenario::<EthereumAdapter>(scenario, configuration),
    )
    .await
    .expect("second instance remained blocked on a leaked lease")
    .expect("execute timeout scenario");
    server.abort();

    assert_eq!(report.started, 2);
    assert_eq!(report.completed, 0);
    assert_eq!(report.failed, 2);
    assert_eq!(report.timed_out, 2);
    assert_eq!(report.maximum_in_flight, 2);
    assert!(report
        .sampled_instances
        .iter()
        .all(|instance| instance.failure_classification.as_deref() == Some("timeout")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handles_reverted_receipts_according_to_step_policy() {
    let bridge = Arc::new(Mutex::new(BridgeState::default()));
    let (rpc_url, server) = spawn_rpc_with_receipt_status(MockChain::X, 1_001, false, bridge).await;
    let directory = TempDir::new();
    write_fixture_files(directory.path(), &rpc_url, &rpc_url);
    fs::write(
        directory.path().join("reverted-submit.yaml"),
        format!(
            r#"version: 1
chains:
  x:
    network: ethereum
    rpc_url: "{rpc_url}"
    chain_id: auto
    workload: ./x-workload.yaml
scenario:
  name: reverted-submit
  steps:
    - submit:
        chain: x
        template: relay
        await: receipt
      save: rejected
"#
        ),
    )
    .expect("write reverted-submit scenario");
    fs::write(
        directory.path().join("allow-revert.yaml"),
        format!(
            r#"version: 1
chains:
  x:
    network: ethereum
    rpc_url: "{rpc_url}"
    chain_id: auto
    workload: ./x-workload.yaml
scenario:
  name: allow-revert
  steps:
    - submit:
        chain: x
        template: relay
      save: submitted
    - wait_receipt:
        chain: x
        transaction_hash: {{ var: submitted.tx_hash }}
        allow_revert: true
        poll_interval: 1ms
      save: reverted
"#
        ),
    )
    .expect("write allow-revert scenario");

    let configuration = ScenarioExecutionConfig {
        count: Some(1),
        duration: None,
        starts_per_second: 0.0,
        max_in_flight: 1,
        step_timeout: Some(Duration::from_secs(2)),
        seed: 17,
        failure_policy: FailurePolicy::Continue,
        transaction_rate: 0,
        max_rpc_in_flight: 2,
        sample_instances: 1,
    };
    let reverted = execute_scenario::<EthereumAdapter>(
        ScenarioSpec::load(&directory.path().join("reverted-submit.yaml")).unwrap(),
        configuration.clone(),
    )
    .await
    .unwrap();
    assert_eq!(reverted.completed, 0);
    assert_eq!(reverted.failed, 1);
    assert_eq!(
        reverted.sampled_instances[0].failure_classification.as_deref(),
        Some("reverted_receipt")
    );

    let allowed = execute_scenario::<EthereumAdapter>(
        ScenarioSpec::load(&directory.path().join("allow-revert.yaml")).unwrap(),
        configuration,
    )
    .await
    .unwrap();
    server.abort();
    assert_eq!(allowed.completed, 1);
    assert_eq!(allowed.failed, 0);
    assert!(allowed.steps.iter().all(|step| step.success == 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_invalid_literal_overlay_before_submission() {
    let bridge = Arc::new(Mutex::new(BridgeState::default()));
    let (rpc_url, server) = spawn_rpc(MockChain::X, 1_001, bridge.clone()).await;
    let directory = TempDir::new();
    write_fixture_files(directory.path(), &rpc_url, &rpc_url);
    fs::write(
        directory.path().join("invalid-overlay.yaml"),
        format!(
            r#"version: 1
chains:
  x:
    network: ethereum
    rpc_url: "{rpc_url}"
    chain_id: auto
    workload: ./x-workload.yaml
scenario:
  name: invalid-overlay
  steps:
    - submit:
        chain: x
        template: relay
        with:
          type: not-a-transaction-type
"#
        ),
    )
    .expect("write invalid overlay scenario");

    let error = execute_scenario::<EthereumAdapter>(
        ScenarioSpec::load(&directory.path().join("invalid-overlay.yaml")).unwrap(),
        ScenarioExecutionConfig {
            count: Some(1),
            duration: None,
            starts_per_second: 0.0,
            max_in_flight: 1,
            step_timeout: Some(Duration::from_secs(2)),
            seed: 9,
            failure_policy: FailurePolicy::Continue,
            transaction_rate: 0,
            max_rpc_in_flight: 2,
            sample_instances: 0,
        },
    )
    .await
    .unwrap_err();
    server.abort();

    assert!(format!("{error:?}").contains("invalid static overlay"));
    assert_eq!(bridge.lock().unwrap().x.submissions, 0);
}

async fn spawn_rpc(
    chain: MockChain,
    chain_id: u64,
    bridge: Arc<Mutex<BridgeState>>,
) -> (String, JoinHandle<()>) {
    spawn_rpc_with_receipt_status(chain, chain_id, true, bridge).await
}

async fn spawn_rpc_with_receipt_status(
    chain: MockChain,
    chain_id: u64,
    receipt_status: bool,
    bridge: Arc<Mutex<BridgeState>>,
) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock RPC");
    let address = listener.local_addr().expect("mock RPC address");
    let app = Router::new().route("/", post(handle_rpc)).with_state(RpcState {
        chain,
        chain_id,
        receipt_status,
        bridge,
    });
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve mock RPC");
    });
    (format!("http://{address}"), server)
}

async fn handle_rpc(State(state): State<RpcState>, Json(request): Json<Value>) -> Json<Value> {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let method = request.get("method").and_then(Value::as_str).unwrap_or_default();
    let params = request.get("params").cloned().unwrap_or_else(|| json!([]));

    let result = match method {
        "eth_chainId" => json!(quantity(state.chain_id)),
        "eth_getTransactionCount" => json!("0x0"),
        "eth_blockNumber" => {
            let bridge = state.bridge.lock().expect("bridge state lock");
            json!(quantity(bridge.chain(state.chain).head))
        }
        "eth_getBlockByNumber" => Value::Null,
        "eth_sendRawTransaction" => {
            let raw = params
                .get(0)
                .and_then(Value::as_str)
                .expect("raw transaction parameter")
                .parse::<Bytes>()
                .expect("hex raw transaction");
            let tx_hash = keccak256(&raw);
            state.bridge.lock().expect("bridge state lock").accept_submission(
                state.chain,
                raw,
                tx_hash,
            );
            json!(tx_hash)
        }
        "eth_getTransactionReceipt" => {
            let tx_hash = params
                .get(0)
                .and_then(Value::as_str)
                .and_then(|value| value.parse::<B256>().ok())
                .expect("transaction hash parameter");
            receipt_value(tx_hash, state.receipt_status)
        }
        "eth_getLogs" => {
            let filter = params.get(0).expect("eth_getLogs filter");
            let from = filter.get("fromBlock").and_then(parse_quantity).unwrap_or_default();
            let to = filter.get("toBlock").and_then(parse_quantity).unwrap_or(u64::MAX);
            let mut bridge = state.bridge.lock().expect("bridge state lock");
            let chain = bridge.chain_mut(state.chain);
            chain.log_queries += 1;
            chain.log_ranges.push((from, to));
            if chain.log.is_none() {
                chain.queried_before_event = true;
            }
            let logs = chain
                .log
                .as_ref()
                .filter(|_| from <= EVENT_BLOCK && EVENT_BLOCK <= to)
                .cloned()
                .into_iter()
                .collect::<Vec<_>>();
            json!(logs)
        }
        _ => {
            return Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": -32601, "message": format!("unsupported method {method}") }
            }));
        }
    };

    Json(json!({ "jsonrpc": "2.0", "id": id, "result": result }))
}

fn receipt_value(transaction_hash: B256, status: bool) -> Value {
    json!({
        "status": if status { "0x1" } else { "0x0" },
        "cumulativeGasUsed": "0x5208",
        "logs": [],
        "logsBloom": format!("0x{}", "00".repeat(256)),
        "type": "0x2",
        "transactionHash": transaction_hash,
        "transactionIndex": "0x0",
        "blockHash": B256::repeat_byte(0x55),
        "blockNumber": quantity(START_BLOCK),
        "gasUsed": "0x5208",
        "effectiveGasPrice": "0x1",
        "from": Address::repeat_byte(0x11),
        "to": Address::repeat_byte(0x22),
        "contractAddress": null
    })
}

fn event_log(signature: &str, request_id: B256, transaction_byte: u8) -> Value {
    json!({
        "address": EVENT_ADDRESS,
        "topics": [keccak256(signature.as_bytes())],
        "data": Bytes::copy_from_slice(request_id.as_slice()),
        "blockHash": B256::repeat_byte(transaction_byte.wrapping_add(0x10)),
        "blockNumber": quantity(EVENT_BLOCK),
        "transactionHash": B256::repeat_byte(transaction_byte),
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "removed": false
    })
}

fn quantity(value: u64) -> String {
    format!("0x{value:x}")
}

fn parse_quantity(value: &Value) -> Option<u64> {
    let value = value.as_str()?.strip_prefix("0x")?;
    u64::from_str_radix(value, 16).ok()
}

fn write_fixture_files(directory: &Path, x_url: &str, y_url: &str) {
    fs::write(
        directory.join("bridge-abi.json"),
        r#"[
  {"type":"event","name":"RequestObserved","anonymous":false,"inputs":[{"name":"requestId","type":"bytes32","indexed":false}]},
  {"type":"event","name":"ResponseObserved","anonymous":false,"inputs":[{"name":"requestId","type":"bytes32","indexed":false}]}
]"#,
    )
    .expect("write ABI fixture");

    let workload = format!(
        r#"chain_id: 1
accounts:
  users:
    mnemonic: "{TEST_MNEMONIC}"
    range: [0, 1]
artifacts:
  BridgeEvents: ./bridge-abi.json
templates:
  relay:
    type: eip1559
    from:
      pool: users
      select: {{ index: 0 }}
    to: "0x0000000000000000000000000000000000000001"
    value: 0
    gas_limit: 21000
    max_fee_per_gas: 1000000000
    max_priority_fee_per_gas: 1000000000
"#
    );
    fs::write(directory.join("x-workload.yaml"), &workload).expect("write X workload");
    fs::write(directory.join("y-workload.yaml"), workload).expect("write Y workload");

    let scenario = format!(
        r#"version: 1
chains:
  x:
    network: ethereum
    rpc_url: "{x_url}"
    chain_id: auto
    workload: ./x-workload.yaml
  y:
    network: ethereum
    rpc_url: "{y_url}"
    chain_id: auto
    workload: ./y-workload.yaml
scenario:
  name: two-chain-backfill
  timeout: 2s
  bindings:
    user:
      account:
        pool: users
        select: lease
  steps:
    - checkpoint:
        chain: y
      save: y_before_request
    - submit:
        chain: x
        template: relay
        with:
          from: {{ var: user.ref }}
      save: request
    - wait_log:
        chain: y
        from_block: {{ var: y_before_request.block_number }}
        address: "{EVENT_ADDRESS}"
        abi: BridgeEvents
        event: RequestObserved
        where:
          requestId: {{ var: request.tx_hash }}
        poll_interval: 1ms
        max_block_range: 100
      save: request_observed
    - checkpoint:
        chain: x
      save: x_before_response
    - submit:
        chain: y
        template: relay
        with:
          from: {{ var: user.ref }}
          input: {{ var: request_observed.args.requestId }}
      save: response
    - wait_log:
        chain: x
        from_block: {{ var: x_before_response.block_number }}
        address: "{EVENT_ADDRESS}"
        abi: BridgeEvents
        event: ResponseObserved
        where:
          requestId: {{ var: request_observed.args.requestId }}
        poll_interval: 1ms
        max_block_range: 100
      save: response_observed
"#
    );
    fs::write(directory.join("scenario.yaml"), scenario).expect("write scenario fixture");
}
