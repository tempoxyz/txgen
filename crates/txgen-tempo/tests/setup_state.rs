use alloy_consensus::{Transaction, TxEnvelope};
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::{Address, TxKind};
use axum::{extract::State, routing::post, Json, Router};
use serde_json::{json, Value};
use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;

struct TestDir(PathBuf);
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn assert_success(output: &Output) {
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resumed_workload_keeps_deployment_address_and_uses_current_chain_nonces() {
    let dir = TestDir(std::env::temp_dir().join(format!(
        "txgen-setup-state-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    )));
    fs::create_dir_all(&dir.0).unwrap();
    let spec_path = dir.0.join("spec.yaml");
    let state_path = dir.0.join("setup.json");
    fs::write(
        &spec_path,
        r#"
chain_id: 1337
accounts:
  users:
    mnemonic: "test test test test test test test test test test test junk"
    index: 0
setup:
  steps:
    - id: contract
      tx:
        type: eip1559
        from: {pool: users, select: {index: 0}}
        gas_limit: 100000
        input: "0x60006000f3"
    - id: configure
      tx:
        type: eip1559
        from: {pool: users, select: {index: 0}}
        gas_limit: 100000
        to: {var: setup.contract.address}
templates:
  call:
    type: eip1559
    from: {pool: users, select: {index: 0}}
    gas_limit: 100000
    to: {var: setup.contract.address}
mix:
  - template: call
    weight: 1
"#,
    )
    .unwrap();
    let nonce = Arc::new(AtomicU64::new(5));
    let app = Router::new().route("/", post(|State(nonce): State<Arc<AtomicU64>>, Json(request): Json<Value>| async move {
        assert_eq!(request["method"], "eth_getTransactionCount");
        Json(json!({"jsonrpc":"2.0", "id":request["id"], "result":format!("0x{:x}", nonce.load(Ordering::SeqCst))}))
    })).with_state(nonce.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rpc = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let command = || {
        let mut command = Command::new(env!("CARGO_BIN_EXE_txgen-tempo"));
        command.args(["generate", "--spec"]).arg(&spec_path).args(["--seed", "1", "--rpc", &rpc]);
        command
    };
    let setup =
        command().args(["--count", "0", "--setup-state-out"]).arg(&state_path).output().unwrap();
    assert_success(&setup);
    assert_eq!(String::from_utf8_lossy(&setup.stdout).lines().count(), 2);
    let saved: Value = serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
    let sender: Address =
        saved["transactions"]["setup.contract"]["sender"].as_str().unwrap().parse().unwrap();
    let deployed = sender.create(5);
    assert_eq!(saved["transactions"]["setup.contract"]["created_address"], json!(deployed));

    // The caller has now successfully submitted both setup transactions.
    nonce.store(7, Ordering::SeqCst);
    let resumed = command()
        .args(["--count", "3", "--duration", "1s", "--setup-state-in"])
        .arg(&state_path)
        .output()
        .unwrap();
    assert_success(&resumed);
    let transactions: Vec<Value> = String::from_utf8(resumed.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(transactions.len(), 3);
    for (index, tx) in transactions.iter().enumerate() {
        assert_eq!(tx["phase"], "workload");
        let raw = hex::decode(tx["raw"].as_str().unwrap().trim_start_matches("0x")).unwrap();
        let envelope = TxEnvelope::decode_2718(&mut raw.as_slice()).unwrap();
        assert_eq!(envelope.nonce(), 7 + index as u64);
        assert_eq!(envelope.kind(), TxKind::Call(deployed));
    }

    // Refuse incompatible state before any workload is emitted.
    for invalid in [
        {
            let mut s = saved.clone();
            s["chain_id"] = json!(1);
            s
        },
        {
            let mut s = saved.clone();
            s["transactions"].as_object_mut().unwrap().remove("setup.configure");
            s
        },
        {
            let mut s = saved.clone();
            s["version"] = json!(999);
            s
        },
    ] {
        fs::write(&state_path, serde_json::to_vec(&invalid).unwrap()).unwrap();
        let output =
            command().args(["--count", "1", "--setup-state-in"]).arg(&state_path).output().unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8_lossy(&output.stderr).contains("setup state"));
    }
    let output =
        command().args(["--count", "1", "--setup-state-out"]).arg(&state_path).output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires --count 0"));
    server.abort();
}
