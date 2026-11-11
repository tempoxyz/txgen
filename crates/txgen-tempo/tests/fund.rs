use alloy_primitives::{Address, U256};
use axum::{extract::State, routing::post, Json, Router};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    fs,
    process::{Command, Output},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use tokio::net::TcpListener;

const SPEC: &str = r#"
chain_id: 1337
accounts:
  users:
    mnemonic: "test test test test test test test test test test test junk"
    range: [0, 4]
templates:
  noop:
    type: eip1559
    from: {pool: users}
    to: "0x0000000000000000000000000000000000000001"
    gas_limit: 21000
mix:
  - template: noop
    weight: 1
"#;

/// A faucet whose first `dropped` requests answer with a hash but never land, as a mint whose
/// transaction leaves the pool unmined does.
#[derive(Default)]
struct Faucet {
    balances: Mutex<HashMap<Address, U256>>,
    requests: Mutex<HashMap<Address, u64>>,
    dropped: u64,
    /// Balance of an account the faucet never funded.
    initial: U256,
    block: AtomicU64,
}

async fn rpc(State(faucet): State<Arc<Faucet>>, Json(request): Json<Value>) -> Json<Value> {
    let params = &request["params"];
    let result = match request["method"].as_str().unwrap() {
        "tempo_fundAddress" => {
            let address: Address = params[0].as_str().unwrap().parse().unwrap();
            let mut requests = faucet.requests.lock().unwrap();
            let count = requests.entry(address).or_default();
            *count += 1;
            if *count > faucet.dropped {
                *faucet.balances.lock().unwrap().entry(address).or_default() += U256::from(1_000);
            }
            json!([format!("0x{:064x}", 1)])
        }
        "eth_call" => {
            let data = params[0]["data"].as_str().unwrap();
            let address: Address = format!("0x{}", &data[data.len() - 40..]).parse().unwrap();
            let balance =
                faucet.balances.lock().unwrap().get(&address).copied().unwrap_or(faucet.initial);
            json!(format!("0x{balance:064x}"))
        }
        "txpool_status" => json!({"pending": "0x0", "queued": "0x0"}),
        "eth_blockNumber" => json!(format!("0x{:x}", faucet.block.fetch_add(1, Ordering::SeqCst))),
        method => panic!("unexpected method {method}"),
    };
    Json(json!({"jsonrpc": "2.0", "id": request["id"], "result": result}))
}

async fn fund(dropped: u64, initial: u64, rounds: u32) -> (Output, Arc<Faucet>) {
    let dir = tempfile::tempdir().unwrap();
    let spec = dir.path().join("spec.yaml");
    fs::write(&spec, SPEC).unwrap();
    let faucet = Arc::new(Faucet { dropped, initial: U256::from(initial), ..Default::default() });
    let app = Router::new().route("/", post(rpc)).with_state(faucet.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let output = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_txgen-tempo"))
            .args(["fund", "--spec"])
            .arg(&spec)
            .args(["--rpc", &url, "--rounds", &rounds.to_string()])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    server.abort();
    (output, faucet)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refunds_accounts_whose_first_mint_did_not_land() {
    let (output, faucet) = fund(1, 0, 3).await;
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let requests = faucet.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert!(requests.values().all(|count| *count == 2), "{requests:?}");
    assert!(faucet.balances.lock().unwrap().values().all(|balance| *balance > U256::ZERO));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fails_when_accounts_stay_short_after_every_round() {
    let (output, faucet) = fund(u64::MAX, 0, 2).await;
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("4 of 4 account(s) still hold less than 1"), "{stderr}");
    assert!(faucet.requests.lock().unwrap().values().all(|count| *count == 2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skips_accounts_that_are_already_funded() {
    let (output, faucet) = fund(0, 1_000, 1).await;
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(faucet.requests.lock().unwrap().is_empty());
}
