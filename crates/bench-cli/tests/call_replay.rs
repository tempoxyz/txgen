//! End-to-end `bench call` replay against a local development node.
//!
//! Skipped when `anvil` is not installed. Install Foundry to run it.

use serde_json::{json, Value};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

/// Account 0 of anvil's default mnemonic.
const SENDER: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";
/// Account 1 of anvil's default mnemonic.
const RECIPIENT: &str = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8";
/// Holds runtime code that returns storage slot 0, for the override record.
const STORAGE_READER: &str = "0x00000000000000000000000000000000000c0de0";
/// Holds a single INVALID opcode, so every call to it fails.
const ALWAYS_FAILS: &str = "0x00000000000000000000000000000000000dead0";

#[test]
fn replays_a_mixed_corpus_identically_twice() {
    let Some(anvil) = anvil_binary() else {
        eprintln!("skipping: anvil not found on PATH or in ~/.foundry/bin");
        return;
    };

    let node = Node::start(&anvil);
    let corpus = build_corpus(&node);

    let first = node.replay(&corpus, "run-1");
    let second = node.replay(&corpus, "run-2");

    // Byte-for-byte parity is the point of the digest: two runs against an
    // unchanged node must agree on every record.
    assert_eq!(first.responses, second.responses, "response digests diverged between runs");
    assert_eq!(first.responses.lines().count(), 6);

    // Every record was answered, and the reverting call is reported as a
    // JSON-RPC error rather than a failure.
    let kinds = first
        .responses
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .map(|row| row["kind"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(kinds.iter().filter(|kind| *kind == "ok").count(), 5);
    assert_eq!(kinds.iter().filter(|kind| *kind == "rpc_error").count(), 1);

    let call = &first.report["call"];
    assert_eq!(call["phase"], "measure");
    assert_eq!(call["corpus"]["records"], 6);
    assert_eq!(call["nondeterministic_total"], 0);
    assert_eq!(call["identity"]["head_hash"], second.report["call"]["identity"]["head_hash"]);
    assert!(call["closed_loop_rps"].as_f64().unwrap() > 0.0);

    for method in
        ["eth_call", "debug_traceCall", "trace_call", "debug_traceTransaction", "trace_transaction"]
    {
        let stats = &call["methods"][method];
        assert!(stats.is_object(), "no per-method entry for {method}");
        assert!(stats["requests"].as_u64().unwrap() > 0, "{method} was not replayed");
        assert!(stats["response_bytes"]["median"].as_u64().unwrap() > 0, "{method} had no bytes");
    }
    assert_eq!(call["totals"]["statuses"]["http_error"], 0);
    assert_eq!(call["totals"]["statuses"]["transport_error"], 0);
    assert_eq!(call["totals"]["statuses"]["timeout"], 0);

    assert_eq!(
        first.requests_csv.lines().next().unwrap(),
        "offset_ms,record_index,method,latency_us,status"
    );
    assert_eq!(
        first.record_csv.lines().next().unwrap(),
        "record_index,method,pass,latency_us,status"
    );
    // Two passes over six records.
    assert_eq!(first.record_csv.lines().count(), 13);
}

/// Outputs of one `bench call` run.
struct RunOutputs {
    report: Value,
    responses: String,
    record_csv: String,
    requests_csv: String,
}

/// A development node owned by this test.
struct Node {
    port: u16,
    directory: tempfile::TempDir,
    child: Child,
}

impl Node {
    fn start(anvil: &PathBuf) -> Self {
        let port = free_port();
        let child = Command::new(anvil)
            .args(["--port", &port.to_string(), "--silent"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to start anvil");
        let node = Self { port, directory: tempfile::tempdir().unwrap(), child };

        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if node.try_rpc("eth_chainId", json!([])).is_some() {
                return node;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("anvil did not become ready");
    }

    fn address(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    fn url(&self) -> String {
        format!("http://{}", self.address())
    }

    fn rpc(&self, method: &str, params: Value) -> Value {
        let response = self.try_rpc(method, params).expect("RPC request failed");
        assert!(response.get("error").is_none(), "{method} failed: {response}");
        response["result"].clone()
    }

    fn try_rpc(&self, method: &str, params: Value) -> Option<Value> {
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let body = serde_json::to_string(&body).unwrap();
        let request = format!(
            "POST / HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            self.address(),
            body.len()
        );

        let mut stream = TcpStream::connect(self.address()).ok()?;
        stream.write_all(request.as_bytes()).ok()?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).ok()?;
        let response = String::from_utf8_lossy(&response);
        serde_json::from_str(response.split("\r\n\r\n").nth(1)?).ok()
    }

    fn replay(&self, corpus: &PathBuf, name: &str) -> RunOutputs {
        let directory = self.directory.path().join(name);
        std::fs::create_dir_all(&directory).unwrap();
        let path = |file: &str| directory.join(file);

        let status = Command::new(env!("CARGO_BIN_EXE_bench"))
            .arg("call")
            .args(["--input", corpus.to_str().unwrap()])
            .args(["--rpc-url", &self.url()])
            .args(["--rps", "50", "--requests", "12", "--max-concurrent", "8"])
            .args(["--passes", "2", "--concurrency", "4", "--seed", "1"])
            .args(["--responses", path("responses.ndjson").to_str().unwrap()])
            .args(["--record-csv", path("record_timings.csv").to_str().unwrap()])
            .args(["--requests-csv", path("requests.csv").to_str().unwrap()])
            .arg("--report")
            .arg(format!("json:{}", path("report.json").display()))
            .args(["-m", "scenario=rpc-replay"])
            .status()
            .expect("failed to run bench call");
        assert!(status.success(), "bench call exited with {status}");

        let read = |file: &str| std::fs::read_to_string(path(file)).unwrap();
        RunOutputs {
            report: serde_json::from_str(&read("report.json")).unwrap(),
            responses: read("responses.ndjson"),
            record_csv: read("record_timings.csv"),
            requests_csv: read("requests.csv"),
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Seed the node and write a corpus covering every request shape.
fn build_corpus(node: &Node) -> PathBuf {
    // Runtime code that returns storage slot 0, so a state override is
    // observable in the response.
    node.rpc("anvil_setCode", json!([STORAGE_READER, "0x60005460005260206000f3"]));
    // A single INVALID opcode, so calls to this address fail.
    node.rpc("anvil_setCode", json!([ALWAYS_FAILS, "0xfe"]));

    let hash = node.rpc(
        "eth_sendTransaction",
        json!([{"from": SENDER, "to": RECIPIENT, "value": "0x1"}]),
    );
    let hash = hash.as_str().expect("no transaction hash");

    let transfer = json!({"from": SENDER, "to": RECIPIENT, "gas": "0x5208", "value": "0x1", "input": "0x"});
    let records = [
        json!({
            "method": "eth_call",
            "params": [
                {"to": STORAGE_READER, "gas": "0x186a0", "value": "0x0", "input": "0x"},
                "latest",
                {STORAGE_READER: {"stateDiff": {
                    "0x0000000000000000000000000000000000000000000000000000000000000000":
                    "0x0000000000000000000000000000000000000000000000000000000000000042"
                }}}
            ],
            "meta": {"block": 1, "index": 0}
        }),
        json!({
            "method": "eth_call",
            "params": [{"to": ALWAYS_FAILS, "gas": "0x186a0", "input": "0x"}, "latest"],
            "meta": {"block": 1, "index": 1}
        }),
        json!({
            "method": "debug_traceCall",
            "params": [transfer, "latest", {"tracer": "callTracer"}]
        }),
        json!({"method": "trace_call", "params": [transfer, ["trace"], "latest"]}),
        json!({
            "method": "debug_traceTransaction",
            "params": [hash, {"tracer": "callTracer"}],
            "meta": {"block": 1, "index": 0, "hash": hash}
        }),
        json!({"method": "trace_transaction", "params": [hash]}),
    ];

    let path = node.directory.path().join("corpus.jsonl");
    let mut file = std::fs::File::create(&path).unwrap();
    for record in records {
        writeln!(file, "{record}").unwrap();
    }
    file.flush().unwrap();
    path
}

fn anvil_binary() -> Option<PathBuf> {
    if Command::new("anvil").arg("--version").stdout(Stdio::null()).status().is_ok() {
        return Some(PathBuf::from("anvil"));
    }
    let home = std::env::var_os("HOME")?;
    let path = PathBuf::from(home).join(".foundry/bin/anvil");
    path.is_file().then_some(path)
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}
