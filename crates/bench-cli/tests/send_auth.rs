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
    let request: Value = serde_json::from_slice(&body).unwrap();
    let method = request["method"].as_str().unwrap().to_string();
    requests
        .lock()
        .unwrap()
        .push(RecordedRequest { method: method.clone(), auth: headers.get(AUTH_HEADER).cloned() });

    let id = request["id"].clone();
    let response = match (kind, method.as_str()) {
        (ServerKind::Submission, "eth_sendRawTransaction") => json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": -32000,
                "message": format!("server deliberately echoed {SECRET_FIXTURE}")
            }
        }),
        (ServerKind::Query, "eth_blockNumber") => {
            json!({ "jsonrpc": "2.0", "id": id, "result": "0x0" })
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
    let content_length = headers["content-length"].parse::<usize>().unwrap();
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
