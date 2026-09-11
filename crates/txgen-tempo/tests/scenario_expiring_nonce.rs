use alloy_consensus::{SignableTransaction, Signed};
use alloy_eips::eip2718::{Decodable2718, Encodable2718};
use alloy_network::{Network, TransactionBuilder};
use alloy_primitives::{keccak256, Address, Bytes, B256, U256};
use axum::{extract::State, routing::post, Json, Router};
use eyre::Result;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, LazyLock, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tempo_alloy::{rpc::TempoTransactionRequest, TempoNetwork};
use tempo_primitives::{transaction::TEMPO_EXPIRING_NONCE_KEY, TempoTxEnvelope};
use tokio::{net::TcpListener, task::JoinHandle};
use txgen_cli::{
    scenario::{
        execute_scenario, FailurePolicy, ScenarioExecutionConfig, ScenarioReport, ScenarioSpec,
    },
    NetworkAdapter, RequestSignContext, TxRequest,
};
use txgen_core::{
    AccountManager, BuildContext, GeneratedTx, NonceTracker, SchedulingKey, TxPhase, WorkloadSpec,
};
use txgen_tempo::{TempoAdapter, TempoSignContext, TempoTemplate};

const TEST_MNEMONIC: &str = "test test test test test test test test test test test junk";
const BASE_FEE: u128 = 1_000_000_000;
static ACTIVE_SIGNERS: AtomicUsize = AtomicUsize::new(0);
static MAX_ACTIVE_SIGNERS: AtomicUsize = AtomicUsize::new(0);
static SIGN_RENDEZVOUS: LazyLock<(Mutex<SignRendezvous>, Condvar)> =
    LazyLock::new(|| (Mutex::new(SignRendezvous::default()), Condvar::new()));

#[derive(Default)]
struct SignRendezvous {
    waiting: usize,
    generation: usize,
}

#[derive(Default)]
struct ProbeAdapter(TempoAdapter);

#[derive(Clone)]
struct ProbeSignContext(TempoSignContext);

struct ActiveSigner;

impl Drop for ActiveSigner {
    fn drop(&mut self) {
        ACTIVE_SIGNERS.fetch_sub(1, Ordering::SeqCst);
    }
}

impl RequestSignContext<TempoNetwork> for ProbeSignContext {
    fn sign_request(
        self,
        name: String,
        phase: TxPhase,
        request: TempoTransactionRequest,
        signer: txgen_core::EcdsaSigner,
        key: [u8; 20],
        inclusion_keys: Vec<SchedulingKey>,
    ) -> Result<GeneratedTx>
    where
        <TempoNetwork as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
        <TempoNetwork as Network>::TxEnvelope:
            From<Signed<<TempoNetwork as Network>::UnsignedTx>> + Encodable2718,
    {
        let scenario_identity = request
            .max_fee_per_gas()
            .is_some_and(|fee| fee > BASE_FEE && fee.saturating_sub(BASE_FEE) % 2 == 0);
        if !scenario_identity {
            return self.0.sign_request(name, phase, request, signer, key, inclusion_keys);
        }
        let active = ACTIVE_SIGNERS.fetch_add(1, Ordering::SeqCst) + 1;
        MAX_ACTIVE_SIGNERS.fetch_max(active, Ordering::SeqCst);
        let _active = ActiveSigner;
        let (state, ready) = &*SIGN_RENDEZVOUS;
        let mut state = state.lock().expect("sign rendezvous lock");
        let generation = state.generation;
        state.waiting += 1;
        if state.waiting == 2 {
            state.waiting = 0;
            state.generation += 1;
            ready.notify_all();
            drop(state);
        } else {
            let (mut state, timeout) = ready
                .wait_timeout_while(state, Duration::from_secs(1), |state| {
                    state.generation == generation
                })
                .expect("sign rendezvous wait");
            if timeout.timed_out() && state.generation == generation {
                state.waiting = state.waiting.saturating_sub(1);
            }
            drop(state);
        }
        self.0.sign_request(name, phase, request, signer, key, inclusion_keys)
    }
}

impl NetworkAdapter for ProbeAdapter {
    type Template = TempoTemplate;
    type Network = TempoNetwork;
    type SignContext = ProbeSignContext;

    fn network_name() -> &'static str {
        TempoAdapter::network_name()
    }

    fn build_request(
        &self,
        template: Self::Template,
        ctx: &mut BuildContext<'_>,
    ) -> Result<TxRequest<TempoTransactionRequest, Self::SignContext>> {
        let TxRequest { request, signer_pool, signer_index, key, sign_context, late_sign } =
            self.0.build_request(template, ctx)?;
        Ok(TxRequest {
            request,
            signer_pool,
            signer_index,
            key,
            sign_context: ProbeSignContext(sign_context),
            late_sign,
        })
    }

    async fn prepare_request(
        &self,
        value: &serde_yaml::Value,
        ctx: &mut BuildContext<'_>,
    ) -> Result<()> {
        self.0.prepare_request(value, ctx).await
    }

    async fn prepare_nonces(
        &self,
        spec: &WorkloadSpec,
        accounts: &AccountManager,
        nonces: &mut NonceTracker,
        rpc: &str,
    ) -> Result<()> {
        self.0.prepare_nonces(spec, accounts, nonces, rpc).await
    }
}

#[derive(Clone, Default)]
struct RpcState {
    submissions: Arc<Mutex<Vec<Bytes>>>,
    active_submissions: Arc<AtomicUsize>,
    max_active_submissions: Arc<AtomicUsize>,
}

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let suffix =
            SystemTime::now().duration_since(UNIX_EPOCH).expect("clock after epoch").as_nanos();
        let path = std::env::temp_dir()
            .join(format!("txgen-tempo-expiring-scenario-{}-{suffix}", std::process::id()));
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
async fn dag_submits_distinct_expiring_nonce_transactions_concurrently() {
    ACTIVE_SIGNERS.store(0, Ordering::SeqCst);
    MAX_ACTIVE_SIGNERS.store(0, Ordering::SeqCst);
    *SIGN_RENDEZVOUS.0.lock().expect("sign rendezvous lock") = SignRendezvous::default();
    let directory = TempDir::new();
    let (state, report) = execute_fixture(directory.path()).await;

    assert_eq!(report.completed, 1);
    assert_eq!(report.failed, 0);
    assert_eq!(report.steps.len(), 2);
    assert!(report.steps.iter().all(|step| step.success == 1 && step.failed == 0));
    assert_eq!(
        state.max_active_submissions.load(Ordering::SeqCst),
        2,
        "independent expiring-nonce branches should submit concurrently"
    );
    assert_eq!(
        MAX_ACTIVE_SIGNERS.load(Ordering::SeqCst),
        2,
        "independent expiring-nonce branches should sign concurrently"
    );

    let first_identities = {
        let submissions = state.submissions.lock().expect("submission state lock");
        assert_eq!(submissions.len(), 2);
        assert_ne!(submissions[0], submissions[1]);

        let first = decode_expiring_transaction(&submissions[0]);
        let second = decode_expiring_transaction(&submissions[1]);
        assert_eq!(first.nonce_key, TEMPO_EXPIRING_NONCE_KEY);
        assert_eq!(second.nonce_key, TEMPO_EXPIRING_NONCE_KEY);
        assert_eq!(first.nonce, 0);
        assert_eq!(second.nonce, 0);
        assert_ne!(first.max_fee_per_gas, second.max_fee_per_gas);
        assert_eq!(first.max_priority_fee_per_gas, 0);
        assert_eq!(second.max_priority_fee_per_gas, 0);

        payload_identities_by_step(&report, &submissions)
    };
    let (repeat_state, repeat_report) = execute_fixture(directory.path()).await;
    let repeat_submissions = repeat_state.submissions.lock().expect("repeat submission state lock");
    assert_eq!(
        first_identities,
        payload_identities_by_step(&repeat_report, &repeat_submissions),
        "a fixed seed must preserve each stable step's signed payload identity"
    );
}

async fn execute_fixture(directory: &Path) -> (RpcState, ScenarioReport) {
    let state = RpcState::default();
    let (rpc_url, server) = spawn_rpc(state.clone()).await;
    write_fixture_files(directory, &rpc_url);
    let scenario = ScenarioSpec::load(&directory.join("scenario.yaml"))
        .expect("load expiring nonce DAG scenario");
    let report = execute_scenario::<ProbeAdapter>(
        scenario,
        ScenarioExecutionConfig {
            count: Some(1),
            duration: None,
            starts_per_second: 0.0,
            max_in_flight: 1,
            step_timeout: Some(Duration::from_secs(2)),
            seed: 0x5eed,
            failure_policy: FailurePolicy::Continue,
            transaction_rate: 0,
            max_rpc_in_flight: 2,
            sample_instances: 1,
        },
    )
    .await
    .expect("execute expiring nonce DAG scenario");
    server.abort();
    (state, report)
}

fn payload_identities_by_step(
    report: &ScenarioReport,
    submissions: &[Bytes],
) -> BTreeMap<String, B256> {
    let payloads = submissions.iter().map(|raw| (keccak256(raw), raw)).collect::<BTreeMap<_, _>>();
    report.sampled_instances[0]
        .steps
        .iter()
        .map(|step| {
            let hash = step
                .milestones
                .iter()
                .find(|milestone| milestone.kind == "submit")
                .and_then(|milestone| milestone.transaction_hash)
                .expect("sampled submit milestone hash");
            assert!(payloads.contains_key(&hash), "submitted payload for sampled step");
            (step.id.clone(), hash)
        })
        .collect()
}

struct ExpiringTransactionFields {
    nonce_key: U256,
    nonce: u64,
    max_fee_per_gas: u128,
    max_priority_fee_per_gas: u128,
}

fn decode_expiring_transaction(raw: &Bytes) -> ExpiringTransactionFields {
    let envelope =
        TempoTxEnvelope::decode_2718(&mut raw.as_ref()).expect("decode Tempo transaction");
    let TempoTxEnvelope::AA(signed) = envelope else {
        panic!("expected Tempo AA transaction");
    };
    let transaction = signed.tx();
    ExpiringTransactionFields {
        nonce_key: transaction.nonce_key,
        nonce: transaction.nonce,
        max_fee_per_gas: transaction.max_fee_per_gas,
        max_priority_fee_per_gas: transaction.max_priority_fee_per_gas,
    }
}

async fn spawn_rpc(state: RpcState) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock RPC");
    let address = listener.local_addr().expect("mock RPC address");
    let app = Router::new().route("/", post(handle_rpc)).with_state(state);
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
        "eth_chainId" => json!("0x1"),
        "eth_getTransactionCount" => json!("0x0"),
        "eth_sendRawTransaction" => {
            let active = state.active_submissions.fetch_add(1, Ordering::SeqCst) + 1;
            state.max_active_submissions.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(75)).await;

            let raw = params
                .get(0)
                .and_then(Value::as_str)
                .expect("raw transaction parameter")
                .parse::<Bytes>()
                .expect("hex raw transaction");
            let tx_hash = keccak256(&raw);
            state.submissions.lock().expect("submission state lock").push(raw);
            state.active_submissions.fetch_sub(1, Ordering::SeqCst);
            json!(tx_hash)
        }
        "eth_getTransactionReceipt" => {
            let tx_hash = params
                .get(0)
                .and_then(Value::as_str)
                .and_then(|value| value.parse::<B256>().ok())
                .expect("transaction hash parameter");
            receipt_value(tx_hash)
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

fn receipt_value(transaction_hash: B256) -> Value {
    json!({
        "status": "0x1",
        "cumulativeGasUsed": "0x5208",
        "logs": [],
        "logsBloom": format!("0x{}", "00".repeat(256)),
        "type": "0x2",
        "transactionHash": transaction_hash,
        "transactionIndex": "0x0",
        "blockHash": B256::repeat_byte(0x55),
        "blockNumber": "0x1",
        "gasUsed": "0x5208",
        "effectiveGasPrice": "0x1",
        "from": Address::repeat_byte(0x11),
        "to": Address::repeat_byte(0x22),
        "contractAddress": null
    })
}

fn write_fixture_files(directory: &Path, rpc_url: &str) {
    fs::write(
        directory.join("workload.yaml"),
        format!(
            r#"chain_id: 1
accounts:
  users:
    mnemonic: "{TEST_MNEMONIC}"
    range: [0, 1]
templates:
  expiring:
    type: tempo
    from:
      pool: users
      select: {{ index: 0 }}
    to: "0x0000000000000000000000000000000000000001"
    value: 1
    gas_limit: 21000
    max_fee_per_gas: {BASE_FEE}
    max_priority_fee_per_gas: 0
    expiring_nonce: true
    valid_before: 4102444800
"#
        ),
    )
    .expect("write Tempo workload");
    fs::write(
        directory.join("scenario.yaml"),
        format!(
            r#"version: 1
chains:
  tempo:
    network: tempo
    rpc_url: "{rpc_url}"
    chain_id: auto
    workload: ./workload.yaml
scenario:
  name: parallel-expiring-nonces
  execution: dag
  steps:
    - id: first
      submit:
        chain: tempo
        template: expiring
      save: first_tx
    - id: second
      submit:
        chain: tempo
        template: expiring
      save: second_tx
"#
        ),
    )
    .expect("write expiring nonce DAG scenario");
}
