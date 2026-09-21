use serde_json::{json, Value};
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};
use tempfile::TempDir;

const AUTH_HEADER: &str = "x-fixture-sender-auth";
const SECRET_FIXTURE: &str = "fixture-confidential-value";
const SENDER: &str = "0x1111111111111111111111111111111111111111";

#[derive(Debug, Clone)]
struct RecordedRequest {
    method: String,
    auth: Option<String>,
}

#[derive(Clone, Copy)]
enum ServerKind {
    Submission,
    Query,
    Readiness { stuck_proposer: bool, stuck_finish: bool },
}

struct MockServer {
    url: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl MockServer {
    fn start(kind: ServerKind) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_requests = requests.clone();
        let thread_stop = stop.clone();
        let server_thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let requests = thread_requests.clone();
                        thread::spawn(move || serve(stream, kind, requests));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("mock server accept failed: {error}"),
                }
            }
        });

        Self { url: format!("http://{address}"), requests, stop, thread: Some(server_thread) }
    }

    fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.url.trim_start_matches("http://"));
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

fn serve(mut stream: TcpStream, kind: ServerKind, requests: Arc<Mutex<Vec<RecordedRequest>>>) {
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let (headers, body) = read_request(&mut stream);
    if let ServerKind::Readiness { stuck_proposer, stuck_finish } = kind &&
        body.is_empty()
    {
        let recorded = requests.lock().unwrap();
        let sent = recorded.iter().any(|r| r.method == "eth_sendRawTransaction");
        let polls = recorded.iter().filter(|r| r.method == "txpool_status").count();
        let count = u8::from(sent && !stuck_proposer);
        let finish = if polls >= 5 && !stuck_finish { 10 } else { 9 };
        let body = format!("executor_finalized_blocks_proposed_by_self_total {count}\nreth_sync_checkpoint{{stage=\"Finish\"}} {finish}\n");
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .unwrap();
        return;
    }
    let request: Value = serde_json::from_slice(&body).unwrap();
    let method = request["method"].as_str().unwrap().to_string();
    requests
        .lock()
        .unwrap()
        .push(RecordedRequest { method: method.clone(), auth: headers.get(AUTH_HEADER).cloned() });

    let id = request["id"].clone();
    let response = match (kind, method.as_str()) {
        (ServerKind::Submission | ServerKind::Readiness { .. }, "eth_sendRawTransaction") => {
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32000,
                    "message": format!("server deliberately echoed {SECRET_FIXTURE}")
                }
            })
        }
        (ServerKind::Query, "eth_blockNumber") => {
            json!({ "jsonrpc": "2.0", "id": id, "result": "0x0" })
        }
        (ServerKind::Readiness { .. }, "debug_clearTxpool") => {
            json!({"jsonrpc":"2.0", "id":id, "result":null})
        }
        (ServerKind::Readiness { .. }, "eth_blockNumber") => {
            json!({"jsonrpc":"2.0", "id":id, "result":"0xa"})
        }
        (ServerKind::Readiness { .. }, "txpool_status") => {
            let polls =
                requests.lock().unwrap().iter().filter(|r| r.method == "txpool_status").count();
            json!({"jsonrpc":"2.0", "id":id, "result":{"pending":"0x0", "queued": if polls == 1 { "0x1" } else { "0x0" }}})
        }
        _ => panic!("unexpected method {method}"),
    };
    let body = response.to_string();
    write!(
        stream,
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    )
    .unwrap();
}

fn read_request(stream: &mut TcpStream) -> (HashMap<String, String>, Vec<u8>) {
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut chunk = [0u8; 1024];
        let read = stream.read(&mut chunk).unwrap();
        assert!(read > 0);
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let headers = std::str::from_utf8(&bytes[..header_end])
        .unwrap()
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect::<HashMap<_, _>>();
    let content_length = headers.get("content-length").map_or(0, |v| v.parse::<usize>().unwrap());
    while bytes.len() - header_end < content_length {
        let mut chunk = [0u8; 1024];
        let read = stream.read(&mut chunk).unwrap();
        assert!(read > 0);
        bytes.extend_from_slice(&chunk[..read]);
    }
    (headers, bytes[header_end..header_end + content_length].to_vec())
}

#[test]
fn query_rpc_is_separate_and_credentials_are_redacted_from_outputs() {
    let submission = MockServer::start(ServerKind::Submission);
    let query = MockServer::start(ServerKind::Query);
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("transactions.ndjson");
    let sender_map = temp.path().join("sender-map.json");
    let report = temp.path().join("report.json");

    std::fs::write(
        &input,
        format!(
            "{}\n",
            json!({
                "phase": "workload",
                "raw": "0x01",
                "sender": SENDER,
                "submission_keys": [SENDER],
                "inclusion_keys": []
            })
        ),
    )
    .unwrap();
    std::fs::write(&sender_map, json!({ SENDER: SECRET_FIXTURE }).to_string()).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&sender_map, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    let output = Command::new(env!("CARGO_BIN_EXE_bench"))
        .env(
            "RUST_LOG",
            "alloy_transport_http::reqwest_transport[request]=trace,alloy_transport_http::reqwest_transport=trace,alloy_transport::layers::retry=trace,alloy_json_rpc::result=trace",
        )
        .args([
            "send",
            "--input",
            input.to_str().unwrap(),
            "--rpc-url",
            &submission.url,
            "--query-rpc-url",
            &query.url,
            "--sender-header-name",
            AUTH_HEADER,
            "--sender-header-map",
            sender_map.to_str().unwrap(),
            "--retries",
            "0",
            "--timeout",
            "2s",
            "--report",
            &format!("json:{}", report.display()),
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "bench failed: {}", String::from_utf8_lossy(&output.stderr));

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let report = std::fs::read_to_string(report).unwrap();
    assert!(!stdout.contains(SECRET_FIXTURE));
    assert!(!stderr.contains(SECRET_FIXTURE));
    assert!(!report.contains(SECRET_FIXTURE));

    let submission_requests = submission.requests();
    assert_eq!(submission_requests.len(), 1);
    assert_eq!(submission_requests[0].method, "eth_sendRawTransaction");
    assert_eq!(submission_requests[0].auth.as_deref(), Some(SECRET_FIXTURE));

    let query_requests = query.requests();
    assert_eq!(query_requests.len(), 2);
    assert!(query_requests.iter().all(|request| request.method == "eth_blockNumber"));
    assert!(query_requests.iter().all(|request| request.auth.is_none()));
}

#[test]
fn warmup_excludes_early_requests_from_report() {
    let submission = MockServer::start(ServerKind::Submission);
    let query = MockServer::start(ServerKind::Query);
    let temp = tempfile::tempdir().unwrap();
    let report = temp.path().join("report.json");
    let tx = json!({"phase":"workload", "raw":"0x01", "sender":SENDER,
        "submission_keys":[SENDER], "inclusion_keys":[]});
    let mut child = Command::new(env!("CARGO_BIN_EXE_bench"))
        .args([
            "send",
            "--rpc-url",
            &submission.url,
            "--query-rpc-url",
            &query.url,
            "--tps",
            "5",
            "--max-concurrent",
            "1",
            "--max-pending",
            "0",
            "--warmup",
            "100ms",
            "--retries",
            "0",
            "--drain-timeout",
            "0",
            "--report",
            &format!("json:{}", report.display()),
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let producer = thread::spawn(move || {
        for _ in 0..5 {
            writeln!(input, "{tx}").unwrap();
            thread::sleep(Duration::from_millis(80));
        }
    });
    let output = child.wait_with_output().unwrap();
    producer.join().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let report: Value = serde_json::from_slice(&std::fs::read(report).unwrap()).unwrap();
    let sent = report["sent"].as_u64().unwrap();
    assert_eq!(submission.requests().len(), 5);
    assert!(sent > 0 && sent < 5, "warmup must exclude only early requests: {report}");
    assert_eq!(report["failed"].as_u64(), Some(sent));
    assert_eq!(report["metadata"]["warmup_secs"], "0.1");
}

#[test]
fn readiness_requires_all_proposers_empty_pools_and_persisted_target() {
    for (stuck_proposer, stuck_finish) in [(false, false), (true, false), (false, true)] {
        let nodes = [
            MockServer::start(ServerKind::Readiness { stuck_proposer: false, stuck_finish: false }),
            MockServer::start(ServerKind::Readiness { stuck_proposer, stuck_finish }),
        ];
        let query = MockServer::start(ServerKind::Query);
        let temp = TempDir::new().unwrap();
        let validators = temp.path().join("validators.json");
        std::fs::write(
            &validators,
            serde_json::to_vec(
                &nodes
                    .iter()
                    .enumerate()
                    .map(|(i, n)| {
                        json!({"validator_name":format!("node-{i}"), "rpc_url":n.url,
                "consensus_metrics_url":n.url, "execution_metrics_url":n.url})
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        )
        .unwrap();
        let report = temp.path().join("report.json");
        let mut child = Command::new(env!("CARGO_BIN_EXE_bench"))
            .args([
                "send",
                "--rpc-url",
                &format!("{},{}", nodes[0].url, nodes[1].url),
                "--query-rpc-url",
                &query.url,
                "--warmup-validators",
                validators.to_str().unwrap(),
                "--warmup-timeout",
                "3s",
                "--cooldown-timeout",
                "6s",
                "--duration",
                "300ms",
                "--tps",
                "20",
                "--max-concurrent",
                "2",
                "--max-pending",
                "0",
                "--retries",
                "0",
                "--drain-timeout",
                "0",
                "--report",
                &format!("json:{}", report.display()),
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        let producer = thread::spawn(move || {
            for i in 0..1000 {
                let tx = json!({"phase":"workload", "raw":format!("0x{:04x}", i),
                    "sender":SENDER, "submission_keys":[SENDER], "inclusion_keys":[]});
                if writeln!(input, "{tx}").is_err() {
                    break;
                }
                thread::sleep(Duration::from_millis(10));
            }
        });
        let output = child.wait_with_output().unwrap();
        producer.join().unwrap();
        if stuck_proposer || stuck_finish {
            assert!(!output.status.success());
            assert!(String::from_utf8_lossy(&output.stderr).contains("timed out"));
            assert!(!report.exists());
        } else {
            assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
            let report: Value = serde_json::from_slice(&std::fs::read(&report).unwrap()).unwrap();
            let evidence: Value =
                serde_json::from_str(report["metadata"]["cooldown_readiness"].as_str().unwrap())
                    .unwrap();
            assert_eq!(evidence["target_block"], 10);
            assert_eq!(evidence["validators"].as_array().unwrap().len(), 2);
            for (i, validator) in evidence["validators"].as_array().unwrap().iter().enumerate() {
                assert_eq!(validator["validator"], format!("node-{i}"));
                assert_eq!(validator["pending"], 0);
                assert_eq!(validator["queued"], 0);
                assert_eq!(validator["head"], 10);
                assert_eq!(validator["finish"], 10);
            }
            assert!(
                report["metadata"]["cooldown_secs"].as_str().unwrap().parse::<f64>().unwrap() >=
                    4.0
            );
        }
        for node in &nodes {
            let requests = node.requests();
            let clear = requests.iter().position(|r| r.method == "debug_clearTxpool");
            if stuck_proposer {
                assert!(clear.is_none());
            } else {
                let clear = clear.expect("every pool must be cleared");
                let last_pool = requests.iter().rposition(|r| r.method == "txpool_status").unwrap();
                assert!(requests[clear + 1..=last_pool]
                    .iter()
                    .all(|r| r.method != "eth_sendRawTransaction"));
            }
        }
    }
}
