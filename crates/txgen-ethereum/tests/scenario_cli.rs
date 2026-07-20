use axum::{routing::post, Json, Router};
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{net::TcpListener, task::JoinHandle};

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let suffix =
            SystemTime::now().duration_since(UNIX_EPOCH).expect("clock after epoch").as_nanos();
        let path = std::env::temp_dir()
            .join(format!("txgen-scenario-cli-{}-{suffix}", std::process::id()));
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
async fn failed_scenario_writes_report_and_exits_nonzero() {
    let (rpc_url, server) = spawn_failing_checkpoint_rpc().await;
    let directory = TempDir::new();
    let workload_path = directory.path().join("workload.yaml");
    let scenario_path = directory.path().join("scenario.yaml");

    fs::write(&workload_path, "chain_id: 1\n").expect("write workload fixture");
    fs::write(
        &scenario_path,
        format!(
            r#"version: 1
chains:
  test:
    network: ethereum
    rpc_url: "{rpc_url}"
    chain_id: auto
    workload: ./workload.yaml
scenario:
  name: failed-checkpoint
  steps:
    - checkpoint:
        chain: test
"#
        ),
    )
    .expect("write scenario fixture");

    let output = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_txgen-ethereum"))
            .arg("scenario")
            .arg("run")
            .arg("--scenario")
            .arg(scenario_path)
            .arg("--seed")
            .arg("1")
            .output()
            .expect("run txgen-ethereum scenario")
    })
    .await
    .expect("join txgen-ethereum process");
    server.abort();

    let stdout = String::from_utf8(output.stdout).expect("scenario report is UTF-8");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "failed scenario exited successfully\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    let report: Value = serde_json::from_str(&stdout).unwrap_or_else(|error| {
        panic!("stdout did not contain a scenario report: {error}\nstdout:\n{stdout}\nstderr:\n{stderr}")
    });
    assert_eq!(report["failed"], 1);
    assert_eq!(report["completed"], 0);
}

async fn spawn_failing_checkpoint_rpc() -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock RPC");
    let address = listener.local_addr().expect("mock RPC address");
    let server = tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/", post(handle_rpc)))
            .await
            .expect("serve mock RPC");
    });
    (format!("http://{address}"), server)
}

async fn handle_rpc(Json(request): Json<Value>) -> Json<Value> {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    match request.get("method").and_then(Value::as_str).unwrap_or_default() {
        "eth_chainId" => Json(json!({ "jsonrpc": "2.0", "id": id, "result": "0x1" })),
        "eth_blockNumber" => Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": -32000, "message": "checkpoint unavailable" }
        })),
        method => Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": -32601, "message": format!("unsupported method {method}") }
        })),
    }
}
