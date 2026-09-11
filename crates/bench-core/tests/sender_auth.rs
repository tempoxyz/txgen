use alloy_network::{AnyNetwork, AnyTransactionReceipt};
use alloy_primitives::{keccak256, Address, Bytes, TxHash};
use alloy_provider::{DynProvider, Provider, ProviderBuilder};
use alloy_rpc_client::RpcClient;
use alloy_transport::layers::RetryBackoffLayer;
use bench_core::{
    LateSigner, MetricsCollector, ReceiptTracker, RequestAuthProvider, RpcEndpoint,
    RpcRequestContext, RpcSubmitter, RunClock, Sender, SenderConfig, SenderHeaderAuthProvider,
};
use eyre::Result;
use reqwest::header::HeaderMap;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};
use tempfile::TempDir;
use txgen_core::{GeneratedTx, LateSignSpec, SchedulingKey, TxPhase};

const AUTH_HEADER: &str = "x-fixture-sender-auth";
const SENDER_ONE_VALUE: &str = "fixture-value-for-sender-one";
const SENDER_TWO_VALUE: &str = "fixture-value-for-sender-two";

#[derive(Debug, Clone)]
struct RecordedRequest {
    method: String,
    params: Value,
    auth: Option<String>,
}

#[derive(Default)]
struct MockState {
    requests: Mutex<Vec<RecordedRequest>>,
    attempts: Mutex<HashMap<String, usize>>,
    sends_in_flight: AtomicUsize,
    concurrent_sends: AtomicBool,
    automine: AtomicBool,
    response_delay_ms: AtomicUsize,
    head_delay_ms: AtomicUsize,
    head_ready: AtomicBool,
    chain: Mutex<MockChain>,
}

#[derive(Default)]
struct MockChain {
    pending: Vec<TxHash>,
    blocks: Vec<Vec<Value>>,
}

impl MockState {
    fn mine(&self) {
        let mut chain = self.chain.lock().unwrap();
        let number = chain.blocks.len() + 1;
        let receipts = chain
            .pending
            .drain(..)
            .map(|hash| {
                let mut value = receipt(hash);
                value["blockNumber"] = json!(format!("0x{number:x}"));
                value
            })
            .collect();
        chain.blocks.push(receipts);
    }

    fn respond(&self, request: RecordedRequest) -> HttpResponse {
        self.requests.lock().unwrap().push(request.clone());

        match request.method.as_str() {
            "eth_sendRawTransaction" => {
                let raw = request.params[0].as_str().unwrap().to_string();
                let previous = self.sends_in_flight.fetch_add(1, Ordering::SeqCst);
                if previous > 0 {
                    self.concurrent_sends.store(true, Ordering::SeqCst);
                }
                thread::sleep(Duration::from_millis(75));

                let attempt = {
                    let mut attempts = self.attempts.lock().unwrap();
                    let attempt = attempts.entry(raw.clone()).or_default();
                    *attempt += 1;
                    *attempt
                };
                self.sends_in_flight.fetch_sub(1, Ordering::SeqCst);

                if raw == "0x01" && attempt == 1 {
                    return HttpResponse::new(429, json!({ "error": "fixture rate limit" }));
                }

                let hash = keccak256(hex::decode(raw.trim_start_matches("0x")).unwrap());
                self.chain.lock().unwrap().pending.push(hash);
                if self.automine.load(Ordering::SeqCst) {
                    self.mine();
                }
                thread::sleep(Duration::from_millis(
                    self.response_delay_ms.load(Ordering::SeqCst) as u64
                ));
                HttpResponse::ok(json!(hash))
            }
            "eth_blockNumber" => {
                thread::sleep(Duration::from_millis(
                    self.head_delay_ms.load(Ordering::SeqCst) as u64
                ));
                self.head_ready.store(true, Ordering::SeqCst);
                HttpResponse::ok(json!(format!("0x{:x}", self.chain.lock().unwrap().blocks.len())))
            }
            "eth_getBlockReceipts" => {
                let number = usize::from_str_radix(
                    request.params[0].as_str().unwrap().trim_start_matches("0x"),
                    16,
                )
                .unwrap();
                HttpResponse::ok(json!(self.chain.lock().unwrap().blocks.get(number - 1)))
            }
            other => panic!("unexpected RPC method: {other}"),
        }
    }

    fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().unwrap().clone()
    }
}

struct HttpResponse {
    status: u16,
    body: Value,
}

impl HttpResponse {
    fn new(status: u16, body: Value) -> Self {
        Self { status, body }
    }

    fn ok(body: Value) -> Self {
        Self::new(200, body)
    }
}

struct MockRpc {
    url: String,
    state: Arc<MockState>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl MockRpc {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(MockState::default());
        state.automine.store(true, Ordering::SeqCst);
        let stop = Arc::new(AtomicBool::new(false));
        let thread_state = state.clone();
        let thread_stop = stop.clone();
        let server_thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let state = thread_state.clone();
                        thread::spawn(move || serve_connection(stream, state));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("mock RPC accept failed: {error}"),
                }
            }
        });

        Self { url: format!("http://{address}"), state, stop, thread: Some(server_thread) }
    }
}

impl Drop for MockRpc {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.url.trim_start_matches("http://"));
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

fn serve_connection(mut stream: TcpStream, state: Arc<MockState>) {
    // Accepted sockets can inherit the listener's nonblocking mode on macOS.
    stream.set_nonblocking(false).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let (headers, body) = read_http_request(&mut stream);
    let request_json: Value = serde_json::from_slice(&body).unwrap();
    let request = RecordedRequest {
        method: request_json["method"].as_str().unwrap().to_string(),
        params: request_json["params"].clone(),
        auth: headers.get(AUTH_HEADER).cloned(),
    };
    let id = request_json["id"].clone();
    let response = state.respond(request);
    let body = if response.status == 200 {
        json!({ "jsonrpc": "2.0", "id": id, "result": response.body }).to_string()
    } else {
        response.body.to_string()
    };
    let reason = if response.status == 200 { "OK" } else { "Too Many Requests" };
    write!(
        stream,
        "HTTP/1.1 {} {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        response.status,
        reason,
        body.len(),
        body
    )
    .unwrap();
}

fn read_http_request(stream: &mut TcpStream) -> (HashMap<String, String>, Vec<u8>) {
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut chunk = [0u8; 1024];
        let read = stream.read(&mut chunk).unwrap();
        assert!(read > 0, "connection closed before request headers");
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let header_text = std::str::from_utf8(&bytes[..header_end]).unwrap();
    let headers = header_text
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect::<HashMap<_, _>>();
    let content_length = headers.get("content-length").unwrap().parse::<usize>().unwrap();
    while bytes.len() - header_end < content_length {
        let mut chunk = [0u8; 1024];
        let read = stream.read(&mut chunk).unwrap();
        assert!(read > 0, "connection closed before request body");
        bytes.extend_from_slice(&chunk[..read]);
    }
    (headers, bytes[header_end..header_end + content_length].to_vec())
}

fn receipt(hash: TxHash) -> Value {
    serde_json::to_value(
        serde_json::from_value::<AnyTransactionReceipt>(json!({
            "transactionHash": hash,
            "transactionIndex": "0x0",
            "blockHash": TxHash::repeat_byte(0x44),
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
        }))
        .unwrap(),
    )
    .unwrap()
}

fn provider(url: &str, http_client: reqwest::Client) -> DynProvider<AnyNetwork> {
    let retry_layer = RetryBackoffLayer::new(2, 1, u64::MAX);
    let client =
        RpcClient::builder().layer(retry_layer).http_with_client(http_client, url.parse().unwrap());
    ProviderBuilder::new_with_network::<AnyNetwork>().connect_client(client).erased()
}

fn auth_file(temp: &TempDir) -> std::path::PathBuf {
    let path = temp.path().join("sender-map.json");
    std::fs::write(
        &path,
        json!({
            Address::repeat_byte(0x01).to_string(): SENDER_ONE_VALUE,
            Address::repeat_byte(0x02).to_string(): SENDER_TWO_VALUE,
        })
        .to_string(),
    )
    .unwrap();
    path
}

fn auth(path: &Path) -> Arc<dyn RequestAuthProvider> {
    Arc::new(
        SenderHeaderAuthProvider::from_file(AUTH_HEADER, path, Duration::from_secs(60)).unwrap(),
    )
}

fn replace_auth_file(path: &Path, contents: &str) {
    let replacement = path.with_extension("replacement");
    std::fs::write(&replacement, contents).unwrap();
    std::fs::rename(replacement, path).unwrap();
}

struct DelayedAuth {
    inner: SenderHeaderAuthProvider,
    delayed_sender: Address,
}

impl RequestAuthProvider for DelayedAuth {
    fn headers_for(&self, context: &RpcRequestContext<'_>) -> Result<HeaderMap> {
        if context.method == "eth_sendRawTransaction" && context.sender == Some(self.delayed_sender)
        {
            thread::sleep(Duration::from_millis(200));
        }
        self.inner.headers_for(context)
    }
}

fn transaction(raw: u8, sender: Option<Address>, key: u8, wait_for_receipt: bool) -> GeneratedTx {
    GeneratedTx {
        phase: TxPhase::Workload,
        id: Some(format!("tx-{raw}")),
        raw: Bytes::from(vec![raw]),
        late_sign: None,
        sender,
        submission_keys: vec![SchedulingKey::from([key; 20])],
        inclusion_keys: wait_for_receipt
            .then(|| SchedulingKey::from([key.wrapping_add(10); 20]))
            .into_iter()
            .collect(),
    }
}

fn sender(
    rpc: &MockRpc,
    request_auth: Option<Arc<dyn RequestAuthProvider>>,
    max_concurrent: usize,
) -> Sender {
    let http_client = reqwest::Client::builder().timeout(Duration::from_secs(3)).build().unwrap();
    let endpoint = RpcEndpoint::new(rpc.url.clone(), provider(&rpc.url, http_client));
    let metrics = MetricsCollector::new_with_latencies(RunClock::new(), false);
    Sender::new_with_request_auth(
        vec![endpoint],
        SenderConfig { rate_limit: 0, max_concurrent },
        metrics,
        request_auth,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_senders_keep_submission_headers_and_share_aggregate_receipts() {
    let rpc = MockRpc::start();
    let temp = TempDir::new().unwrap();
    let mut sender = sender(&rpc, Some(auth(&auth_file(&temp))), 2);

    sender.send(transaction(1, Some(Address::repeat_byte(0x01)), 1, true)).await.unwrap();
    sender.send(transaction(2, Some(Address::repeat_byte(0x02)), 2, true)).await.unwrap();
    sender.flush().await.unwrap();

    assert!(rpc.state.concurrent_sends.load(Ordering::SeqCst));
    let requests = rpc.state.requests();
    let sends_one = requests
        .iter()
        .filter(|request| request.method == "eth_sendRawTransaction" && request.params[0] == "0x01")
        .collect::<Vec<_>>();
    assert_eq!(sends_one.len(), 2, "the first sender should be retried once");
    assert!(sends_one.iter().all(|request| request.auth.as_deref() == Some(SENDER_ONE_VALUE)));

    let sends_two = requests
        .iter()
        .filter(|request| request.method == "eth_sendRawTransaction" && request.params[0] == "0x02")
        .collect::<Vec<_>>();
    assert_eq!(sends_two.len(), 1);
    assert_eq!(sends_two[0].auth.as_deref(), Some(SENDER_TWO_VALUE));

    let receipts = requests
        .iter()
        .filter(|request| request.method == "eth_getBlockReceipts")
        .collect::<Vec<_>>();
    assert_eq!(receipts.len(), 2);
    assert!(requests
        .iter()
        .filter(|r| r.method != "eth_sendRawTransaction")
        .all(|r| r.auth.is_none()));
    assert!(requests.iter().all(|r| r.method != "eth_getTransactionReceipt"));
}

#[tokio::test]
async fn missing_sender_or_mapping_fails_before_submission() {
    let rpc = MockRpc::start();
    let temp = TempDir::new().unwrap();

    let mut missing_sender = sender(&rpc, Some(auth(&auth_file(&temp))), 1);
    let error = missing_sender.send(transaction(1, None, 1, false)).await.unwrap_err();
    assert!(error.to_string().contains("authenticate transaction"));

    let mut missing_mapping = sender(&rpc, Some(auth(&auth_file(&temp))), 1);
    let error = missing_mapping
        .send(transaction(1, Some(Address::repeat_byte(0x09)), 1, false))
        .await
        .unwrap_err();
    assert!(format!("{error:?}").contains("no sender authentication mapping"));

    // An errored send is removed from the queue and cannot be submitted later
    // if the caller fixes credentials and reuses the sender.
    missing_mapping.flush().await.unwrap();

    assert!(rpc.state.requests().is_empty());
}

#[tokio::test]
async fn legacy_transaction_without_sender_still_submits_when_auth_is_disabled() {
    let rpc = MockRpc::start();
    let mut sender = sender(&rpc, None, 1);

    sender.send(transaction(2, None, 1, false)).await.unwrap();
    sender.flush().await.unwrap();

    let requests = rpc.state.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "eth_sendRawTransaction");
    assert_eq!(requests[0].auth, None);
}

#[tokio::test]
async fn atomic_map_reload_updates_requests_and_malformed_map_keeps_last_value() {
    let rpc = MockRpc::start();
    let temp = TempDir::new().unwrap();
    let path = auth_file(&temp);
    let request_auth: Arc<dyn RequestAuthProvider> =
        Arc::new(SenderHeaderAuthProvider::from_file(AUTH_HEADER, &path, Duration::ZERO).unwrap());
    let mut sender = sender(&rpc, Some(request_auth), 1);

    sender.send(transaction(5, Some(Address::repeat_byte(0x01)), 5, false)).await.unwrap();
    sender.flush().await.unwrap();

    const REFRESHED_VALUE: &str = "fixture-refreshed-value-for-sender-one";
    replace_auth_file(
        &path,
        &json!({ Address::repeat_byte(0x01).to_string(): REFRESHED_VALUE }).to_string(),
    );
    sender.send(transaction(6, Some(Address::repeat_byte(0x01)), 6, false)).await.unwrap();
    sender.flush().await.unwrap();

    replace_auth_file(&path, "{ malformed fixture map");
    sender.send(transaction(7, Some(Address::repeat_byte(0x01)), 7, false)).await.unwrap();
    sender.flush().await.unwrap();

    let requests = rpc.state.requests();
    let auth_for = |raw: &str| {
        requests
            .iter()
            .find(|request| request.method == "eth_sendRawTransaction" && request.params[0] == raw)
            .and_then(|request| request.auth.as_deref())
    };
    assert_eq!(auth_for("0x05"), Some(SENDER_ONE_VALUE));
    assert_eq!(auth_for("0x06"), Some(REFRESHED_VALUE));
    assert_eq!(auth_for("0x07"), Some(REFRESHED_VALUE));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn older_auth_failure_does_not_report_error_after_current_tx_is_dispatched() {
    let rpc = MockRpc::start();
    let temp = TempDir::new().unwrap();
    let request_auth: Arc<dyn RequestAuthProvider> = Arc::new(DelayedAuth {
        inner: SenderHeaderAuthProvider::from_file(
            AUTH_HEADER,
            auth_file(&temp),
            Duration::from_secs(60),
        )
        .unwrap(),
        delayed_sender: Address::repeat_byte(0x02),
    });
    let mut sender = sender(&rpc, Some(request_auth), 3);

    // A is active on key 7. B queues behind A but has no auth mapping. C is
    // disjoint and its auth lookup pauses long enough for A to complete. Pump
    // dispatches C, then discovers B's older auth failure.
    sender.send(transaction(2, Some(Address::repeat_byte(0x01)), 7, false)).await.unwrap();
    sender.send(transaction(1, Some(Address::repeat_byte(0x09)), 7, false)).await.unwrap();
    sender
        .send(transaction(3, Some(Address::repeat_byte(0x02)), 8, false))
        .await
        .expect("C was dispatched, so it must not be reported as rejected");

    let error = sender.flush().await.unwrap_err();
    assert!(format!("{error:?}").contains("no sender authentication mapping"));
    let requests = rpc.state.requests();
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.method == "eth_sendRawTransaction" && request.params[0] == "0x03"
            })
            .count(),
        1
    );
}

#[tokio::test]
async fn flush_cancels_queued_transactions_after_authentication_failure() {
    let rpc = MockRpc::start();
    let temp = TempDir::new().unwrap();
    let mut sender = sender(&rpc, Some(auth(&auth_file(&temp))), 1);

    sender.send(transaction(2, Some(Address::repeat_byte(0x01)), 7, false)).await.unwrap();
    sender.send(transaction(3, Some(Address::repeat_byte(0x09)), 7, false)).await.unwrap();
    sender.send(transaction(4, Some(Address::repeat_byte(0x01)), 7, false)).await.unwrap();

    let error = sender.flush().await.unwrap_err();
    assert!(format!("{error:?}").contains("no sender authentication mapping"));

    let requests = rpc.state.requests();
    assert_eq!(
        requests.iter().filter(|request| request.method == "eth_sendRawTransaction").count(),
        1
    );
    assert!(requests.iter().all(|request| request.params[0] != "0x04"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deferred_authentication_failure_rejects_the_next_send_before_submission() {
    let rpc = MockRpc::start();
    let temp = TempDir::new().unwrap();
    let request_auth: Arc<dyn RequestAuthProvider> = Arc::new(DelayedAuth {
        inner: SenderHeaderAuthProvider::from_file(
            AUTH_HEADER,
            auth_file(&temp),
            Duration::from_secs(60),
        )
        .unwrap(),
        delayed_sender: Address::repeat_byte(0x02),
    });
    let mut sender = sender(&rpc, Some(request_auth), 3);

    sender.send(transaction(2, Some(Address::repeat_byte(0x01)), 7, false)).await.unwrap();
    sender.send(transaction(1, Some(Address::repeat_byte(0x09)), 7, false)).await.unwrap();
    sender.send(transaction(3, Some(Address::repeat_byte(0x02)), 8, false)).await.unwrap();

    let error =
        sender.send(transaction(4, Some(Address::repeat_byte(0x01)), 9, false)).await.unwrap_err();
    assert!(format!("{error:?}").contains("no sender authentication mapping"));
    sender.flush().await.unwrap();

    let requests = rpc.state.requests();
    assert!(requests.iter().all(|request| request.params[0] != "0x04"));
}

async fn wait_for_pending(rpc: &MockRpc, count: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if rpc.state.chain.lock().unwrap().pending.len() == count {
                break;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("transactions were not submitted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequences_share_one_receipt_request_per_block_and_wait_for_inclusion() {
    let rpc = MockRpc::start();
    rpc.state.automine.store(false, Ordering::SeqCst);

    let mut sender = sender(&rpc, None, 32);

    for key in 1..=32 {
        for raw in [key + 2, key + 100] {
            let mut tx = transaction(raw, None, key, true);
            tx.submission_keys.clear();
            tx.inclusion_keys = vec![SchedulingKey::from([key; 20])];

            sender.send(tx).await.unwrap();
        }
    }

    let flush = tokio::spawn(async move { sender.flush().await.unwrap() });
    wait_for_pending(&rpc, 32).await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    let requests = rpc.state.requests();
    assert_eq!(requests.iter().filter(|r| r.method == "eth_sendRawTransaction").count(), 32);
    assert_eq!(requests.iter().filter(|r| r.method == "eth_getBlockReceipts").count(), 0);

    rpc.state.mine();
    wait_for_pending(&rpc, 32).await;

    let requests = rpc.state.requests();
    assert_eq!(requests.iter().filter(|r| r.method == "eth_getBlockReceipts").count(), 1);
    assert!(!flush.is_finished(), "successors still need inclusion");

    rpc.state.mine();
    tokio::time::timeout(Duration::from_secs(5), flush).await.unwrap().unwrap();

    let requests = rpc.state.requests();
    assert_eq!(requests.iter().filter(|r| r.method == "eth_getBlockReceipts").count(), 2);
    assert!(requests.iter().all(|r| r.method != "eth_getTransactionReceipt"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rpc_submitter_uses_query_tracker_before_releasing_inclusion_keys() {
    let rpc = MockRpc::start();
    rpc.state.automine.store(false, Ordering::SeqCst);

    let submission = provider(&rpc.url, reqwest::Client::new());
    let query = provider(&rpc.url, reqwest::Client::new());
    let submitter = RpcSubmitter::new(vec![submission], SenderConfig::default())
        .unwrap()
        .with_receipt_tracker(ReceiptTracker::new(query));

    submitter.submit(&transaction(2, None, 1, true)).await.unwrap();

    let second = tokio::spawn(async move {
        submitter.submit(&transaction(3, None, 1, true)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(250)).await;

    assert!(!second.is_finished());
    assert_eq!(rpc.state.chain.lock().unwrap().pending.len(), 1);

    rpc.state.mine();
    tokio::time::timeout(Duration::from_secs(5), second).await.unwrap().unwrap();
    assert_eq!(rpc.state.chain.lock().unwrap().pending.len(), 1);

    rpc.state.mine();
    assert!(rpc.state.requests().iter().all(|r| r.method != "eth_getTransactionReceipt"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn setup_receipt_arriving_before_submission_response_is_retained() {
    let rpc = MockRpc::start();
    rpc.state.response_delay_ms.store(300, Ordering::SeqCst);

    let mut sender = sender(&rpc, None, 1);

    // Setup without inclusion keys must still wait for its receipt.
    let mut tx = transaction(2, None, 1, false);
    tx.phase = TxPhase::Setup;

    sender.send(tx).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), sender.flush()).await.unwrap().unwrap();

    let requests = rpc.state.requests();
    assert_eq!(requests.iter().filter(|r| r.method == "eth_getBlockReceipts").count(), 1);
    assert!(requests.iter().all(|r| r.method != "eth_getTransactionReceipt"));
}

struct DeferredSigner {
    state: Arc<MockState>,
    calls: AtomicUsize,
}

impl LateSigner for DeferredSigner {
    fn sign(&self, spec: &LateSignSpec) -> Result<Bytes> {
        assert!(self.state.head_ready.load(Ordering::SeqCst), "signed before tracker was ready");

        self.calls.fetch_add(1, Ordering::SeqCst);

        Ok(Bytes::from(vec![spec.payload.as_u64().unwrap() as u8]))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deferred_sequences_wait_for_tracker_before_signing_and_track_signed_hashes() {
    for use_rpc_submitter in [false, true] {
        let rpc = MockRpc::start();
        rpc.state.automine.store(false, Ordering::SeqCst);
        rpc.state.head_delay_ms.store(200, Ordering::SeqCst);

        let signer =
            Arc::new(DeferredSigner { state: rpc.state.clone(), calls: AtomicUsize::new(0) });

        let txs: Vec<_> = [2, 3]
            .into_iter()
            .map(|raw| {
                let mut tx = transaction(raw, None, 1, true);
                tx.raw = Bytes::new();
                tx.late_sign =
                    Some(LateSignSpec { format: "test".to_string(), payload: json!(raw) });

                tx
            })
            .collect();

        let mut sender = sender(&rpc, None, 2).with_late_signer(signer.clone());
        let submitter = RpcSubmitter::new(
            vec![provider(&rpc.url, reqwest::Client::new())],
            SenderConfig::default(),
        )
        .unwrap()
        .with_late_signer(signer.clone());

        let task = tokio::spawn(async move {
            if use_rpc_submitter {
                for tx in txs {
                    submitter.submit(&tx).await.unwrap();
                }
            } else {
                for tx in txs {
                    sender.send(tx).await.unwrap();
                }

                sender.flush().await.unwrap();
            }
        });
        wait_for_pending(&rpc, 1).await;

        assert_eq!(signer.calls.load(Ordering::SeqCst), 1);
        assert_eq!(rpc.state.chain.lock().unwrap().pending, [keccak256([2])]);

        rpc.state.head_delay_ms.store(0, Ordering::SeqCst);

        rpc.state.mine();
        wait_for_pending(&rpc, 1).await;

        assert_eq!(signer.calls.load(Ordering::SeqCst), 2);
        assert_eq!(rpc.state.chain.lock().unwrap().pending, [keccak256([3])]);

        rpc.state.mine();
        tokio::time::timeout(Duration::from_secs(5), task).await.unwrap().unwrap();

        assert!(rpc.state.requests().iter().all(|r| r.method != "eth_getTransactionReceipt"));
    }
}
