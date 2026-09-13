//! `bench call` - Replay an RPC corpus against a node.
//!
//! Two phases run against the same corpus and report separately. The open-loop
//! cell paces requests at a fixed rate regardless of how fast the node answers,
//! which is what exposes queueing. The closed-loop passes walk the corpus in
//! order with a fixed worker count, which measures per-call service time.
//!
//! Every response is digested as it streams in so two runs can be compared byte
//! for byte without any output ever carrying a request or response body.

use crate::{
    load_metric_names,
    metrics_forwarder::{build_metrics_forwarder, finish_metrics_forwarder},
    metrics_url::metrics_scraper_configs,
    send::parse_metadata,
    CallArgs, CallPhase,
};
use bench_core::{
    digest_response, parse_reporters, start_scrapers, CallMethod, CallReport, CallRunConfig,
    ConsoleReporter, Corpus, CorpusOptions, FinalReport, NodeIdentity, ReplayRecorder,
    ReplayResults, RequestOutcome, RequestStatus, ResponseKind, ResponseSummary, RunClock,
    SampleStore,
};
use eyre::{bail, Context, Result};
use rand::{Rng, SeedableRng};
use std::{
    collections::BTreeSet,
    io::{BufWriter, Write},
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{sync::Semaphore, task::JoinSet};

/// Upper bound on the nondeterministic record list carried in the report.
const MAX_REPORTED_NONDETERMINISTIC: usize = 200;

/// How often each phase logs progress.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(5);

pub async fn execute(args: CallArgs) -> Result<()> {
    let corpus = Arc::new(load_corpus(&args)?);
    tracing::info!(
        input = %args.input.display(),
        rpc_url = %args.rpc_url,
        phase = ?args.phase,
        records = corpus.len(),
        skipped = corpus.skipped(),
        rps = args.rps,
        passes = args.passes,
        concurrency = args.concurrency,
        "Starting RPC corpus replay"
    );

    let metadata = parse_metadata(&args.metadata)?;
    let scraper_configs =
        metrics_scraper_configs(&args.metrics_url, Duration::from_millis(args.scrape_interval_ms))?;

    let replay = Replay::new(&args, corpus.clone())?;
    let identity = replay.identity().await?;
    tracing::info!(
        chain_id = identity.chain_id,
        head = identity.head,
        head_hash = %identity.head_hash,
        "Node identity"
    );
    replay.prime(args.concurrency).await?;

    let clickhouse_metric_names = load_metric_names(args.clickhouse_metrics_file.as_ref())?;
    let mut reporters = parse_reporters(&args.reports, "call", &metadata, clickhouse_metric_names)?;
    if reporters.is_empty() {
        reporters.push(Box::new(ConsoleReporter::stderr(false)));
    }

    let clock = match args.metrics_align {
        Some(start) => RunClock::new_with_start_unix_ms(start),
        None => RunClock::new(),
    };
    let store = SampleStore::with_labels(metadata.clone())?;
    let metrics_forwarder =
        build_metrics_forwarder(args.metrics_forward.as_deref(), &metadata, &scraper_configs)?;
    let scraper_handles = if scraper_configs.is_empty() {
        Vec::new()
    } else {
        let callback: bench_core::SampleCallback = Arc::new(Vec::new);
        start_scrapers(
            &scraper_configs,
            clock.clone(),
            store.clone(),
            callback,
            metrics_forwarder.as_ref().map(|forwarder| forwarder.handle()),
        )
    };

    let open_loop_secs = replay.run_open_loop(&args, &clock).await?;
    let closed_loop_secs = match args.phase {
        CallPhase::Warmup => 0.0,
        CallPhase::Measure => replay.run_closed_loop(&args).await?,
    };

    if !scraper_handles.is_empty() {
        let scrapes = scraper_handles.iter().map(|handle| handle.scrape_count()).sum::<u64>();
        let errors = scraper_handles.iter().map(|handle| handle.error_count()).sum::<u64>();
        for handle in scraper_handles {
            handle.stop().await;
        }
        tracing::info!(scrapes, errors, "Metrics scrapers stopped");
    }

    let results = replay.recorder.finish();
    if args.phase == CallPhase::Measure {
        write_outputs(&args, &results)?;
    }

    let (totals, methods) = results.stats(open_loop_secs, closed_loop_secs);
    let failure_rate_pct = totals.failure_rate_pct;
    let call = CallReport {
        phase: args.phase.as_str().to_string(),
        corpus: corpus.summary(),
        identity,
        config: run_config(&args),
        latency: totals.open_loop.latency.clone(),
        dropped: totals.dropped,
        closed_loop_rps: totals.closed_loop.rps,
        totals,
        methods,
        nondeterministic: results.nondeterministic,
        nondeterministic_total: results.nondeterministic_total as u64,
    };

    if call.nondeterministic_total > 0 {
        tracing::warn!(
            records = call.nondeterministic_total,
            "Responses changed within the run; the node or the corpus is nondeterministic"
        );
    }

    let report = FinalReport {
        metadata,
        sample_archive: Some(store.finish().await?),
        call: Some(call),
        ..Default::default()
    };

    let mut finalize_result = Ok(());
    for reporter in &mut reporters {
        if let Err(err) = reporter.finalize(&report) {
            finalize_result = Err(err);
            break;
        }
    }
    let forwarder_result = finish_metrics_forwarder(metrics_forwarder).await;
    finalize_result?;
    forwarder_result?;

    if failure_rate_pct > args.max_fail_rate_pct {
        bail!(
            "HTTP and transport failure rate {failure_rate_pct:.2}% exceeds --max-fail-rate-pct {:.2}%",
            args.max_fail_rate_pct
        );
    }

    Ok(())
}

/// One replay against one endpoint.
struct Replay {
    client: reqwest::Client,
    url: String,
    corpus: Arc<Corpus>,
    recorder: Arc<ReplayRecorder>,
}

impl Replay {
    fn new(args: &CallArgs, corpus: Arc<Corpus>) -> Result<Self> {
        let pool = args.max_concurrent.max(args.concurrency);
        let client = reqwest::Client::builder()
            .timeout(args.timeout)
            .pool_max_idle_per_host(pool)
            .build()
            .wrap_err("failed to build the replay HTTP client")?;
        let recorder = Arc::new(ReplayRecorder::new(&corpus, MAX_REPORTED_NONDETERMINISTIC));
        Ok(Self { client, url: args.rpc_url.clone(), corpus, recorder })
    }

    /// Read the chain and head the corpus is replayed against.
    ///
    /// Runs whose head hash differs are not comparable, so the values are
    /// carried into the report for the comparison step to check.
    async fn identity(&self) -> Result<NodeIdentity> {
        let chain_id = self.rpc("eth_chainId", serde_json::json!([])).await?;
        let chain_id = chain_id
            .as_str()
            .and_then(|value| u64::from_str_radix(value.trim_start_matches("0x"), 16).ok())
            .ok_or_else(|| eyre::eyre!("eth_chainId returned an unexpected value"))?;

        let head = self.rpc("eth_getBlockByNumber", serde_json::json!(["latest", false])).await?;
        let number = head["number"]
            .as_str()
            .and_then(|value| u64::from_str_radix(value.trim_start_matches("0x"), 16).ok())
            .ok_or_else(|| eyre::eyre!("eth_getBlockByNumber returned no block number"))?;
        let hash = head["hash"]
            .as_str()
            .ok_or_else(|| eyre::eyre!("eth_getBlockByNumber returned no block hash"))?;

        Ok(NodeIdentity { chain_id, head: number, head_hash: hash.to_string() })
    }

    /// Open the connections the replay will use before anything is timed.
    async fn prime(&self, connections: usize) -> Result<()> {
        let mut opened = JoinSet::new();
        for _ in 0..connections {
            let replay = self.clone_handles();
            opened.spawn(async move { replay.rpc("eth_chainId", serde_json::json!([])).await });
        }
        while let Some(result) = opened.join_next().await {
            result??;
        }
        tracing::info!(connections, "Primed connection pool");
        Ok(())
    }

    /// Pace requests at a fixed rate, independent of how fast the node answers.
    ///
    /// Returns the phase's wall-clock duration in seconds. A request that would
    /// exceed `--max-concurrent` is counted as dropped rather than delayed, so
    /// the offered rate stays the configured one.
    async fn run_open_loop(&self, args: &CallArgs, clock: &RunClock) -> Result<f64> {
        if args.rps == 0 {
            tracing::info!(reason = "--rps 0", "Skipped open-loop phase");
            return Ok(0.0);
        }

        let record = args.phase == CallPhase::Measure;
        let semaphore = Arc::new(Semaphore::new(args.max_concurrent));
        let mut rng = rand::rngs::StdRng::seed_from_u64(args.seed);
        let started = tokio::time::Instant::now();
        let mut progress = started + PROGRESS_INTERVAL;
        let mut issued = 0u64;
        let mut dropped = 0u64;

        loop {
            match args.requests {
                Some(limit) if issued >= limit => break,
                None if started.elapsed() >= args.duration => break,
                _ => {}
            }

            tokio::time::sleep_until(started + request_offset(issued, args.rps)).await;
            if args.requests.is_none() && started.elapsed() >= args.duration {
                break;
            }

            let slot = rng.random_range(0..self.corpus.len());
            let offset_ms = clock.offset_ms();
            match Arc::clone(&semaphore).try_acquire_owned() {
                Ok(permit) => {
                    let replay = self.clone_handles();
                    let body = self.corpus.records()[slot].body().to_string();
                    tokio::spawn(async move {
                        let outcome = replay.send(&body).await;
                        if record {
                            replay.recorder.record_open_loop(slot, offset_ms, outcome);
                        }
                        drop(permit);
                    });
                }
                Err(_) => {
                    dropped += 1;
                    if record {
                        self.recorder.record_dropped(slot);
                    }
                }
            }
            issued += 1;

            if tokio::time::Instant::now() >= progress {
                let elapsed = started.elapsed().as_secs_f64();
                tracing::info!(
                    issued,
                    dropped,
                    rps = issued as f64 / elapsed.max(f64::MIN_POSITIVE),
                    in_flight = args.max_concurrent - semaphore.available_permits(),
                    "Open-loop progress"
                );
                progress = tokio::time::Instant::now() + PROGRESS_INTERVAL;
            }
        }

        drain(&semaphore, args.max_concurrent).await;
        let elapsed = started.elapsed().as_secs_f64();
        tracing::info!(issued, dropped, elapsed_secs = elapsed, "Open-loop phase complete");
        Ok(elapsed)
    }

    /// Walk the corpus in order with a fixed worker count.
    ///
    /// Returns the phase's wall-clock duration in seconds.
    async fn run_closed_loop(&self, args: &CallArgs) -> Result<f64> {
        let total = args.passes.saturating_mul(self.corpus.len() as u64);
        if total == 0 {
            tracing::info!(reason = "--passes 0", "Skipped closed-loop phase");
            return Ok(0.0);
        }

        let records = self.corpus.len() as u64;
        let cursor = Arc::new(AtomicU64::new(0));
        let started = Instant::now();
        let mut workers = JoinSet::new();

        for _ in 0..args.concurrency {
            let replay = self.clone_handles();
            let cursor = cursor.clone();
            workers.spawn(async move {
                loop {
                    let ticket = cursor.fetch_add(1, Ordering::Relaxed);
                    if ticket >= total {
                        break;
                    }
                    let slot = (ticket % records) as usize;
                    let pass = (ticket / records) as u32;
                    let body = replay.corpus.records()[slot].body().to_string();
                    let outcome = replay.send(&body).await;
                    replay.recorder.record_closed_loop(slot, pass, outcome);
                }
            });
        }

        let mut progress = tokio::time::interval(PROGRESS_INTERVAL);
        progress.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        progress.tick().await;
        loop {
            tokio::select! {
                joined = workers.join_next() => match joined {
                    Some(result) => result.wrap_err("closed-loop worker panicked")?,
                    None => break,
                },
                _ = progress.tick() => {
                    let done = cursor.load(Ordering::Relaxed).min(total);
                    tracing::info!(
                        done,
                        total,
                        rps = done as f64 / started.elapsed().as_secs_f64().max(f64::MIN_POSITIVE),
                        "Closed-loop progress"
                    );
                }
            }
        }

        let elapsed = started.elapsed().as_secs_f64();
        tracing::info!(
            requests = total,
            passes = args.passes,
            elapsed_secs = elapsed,
            "Closed-loop phase complete"
        );
        Ok(elapsed)
    }

    /// Send one request and digest its response.
    ///
    /// Never logs or returns the body: failures are reported as a status and
    /// counted per method.
    async fn send(&self, body: &str) -> RequestOutcome {
        let started = Instant::now();
        let response = self
            .client
            .post(&self.url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.to_string())
            .send()
            .await;

        let (status, response) = match response {
            Ok(response) if response.status().is_success() => match digest_response(response).await
            {
                Ok(summary) => (status_of(&summary), Some(summary)),
                Err(_) => (RequestStatus::TransportError, None),
            },
            Ok(_) => (RequestStatus::HttpError, None),
            Err(error) if error.is_timeout() => (RequestStatus::Timeout, None),
            Err(_) => (RequestStatus::TransportError, None),
        };

        RequestOutcome { latency: started.elapsed(), status, response }
    }

    async fn rpc(&self, method: &str, params: serde_json::Value) -> Result<serde_json::Value> {
        let request =
            serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let response = self
            .client
            .post(&self.url)
            .json(&request)
            .send()
            .await
            .wrap_err_with(|| format!("{method} request failed"))?
            .error_for_status()
            .wrap_err_with(|| format!("{method} returned an HTTP error"))?
            .json::<serde_json::Value>()
            .await
            .wrap_err_with(|| format!("{method} returned a malformed response"))?;

        if let Some(error) = response.get("error") {
            bail!("{method} failed: {}", error["message"].as_str().unwrap_or("unknown error"));
        }
        Ok(response["result"].clone())
    }

    fn clone_handles(&self) -> Self {
        Self {
            client: self.client.clone(),
            url: self.url.clone(),
            corpus: self.corpus.clone(),
            recorder: self.recorder.clone(),
        }
    }
}

fn load_corpus(args: &CallArgs) -> Result<Corpus> {
    let mut methods = BTreeSet::new();
    for name in &args.methods {
        methods.insert(name.parse::<CallMethod>().wrap_err("invalid --methods entry")?);
    }
    let options = CorpusOptions {
        methods: (!methods.is_empty()).then_some(methods),
        block_tag: args.block_tag.clone(),
        strip_fees: args.strip_fees,
    };

    let corpus = Corpus::load(&args.input, &options)?;
    if corpus.is_empty() {
        bail!("corpus {} has no replayable records", args.input.display());
    }
    Ok(corpus)
}

fn run_config(args: &CallArgs) -> CallRunConfig {
    CallRunConfig {
        rps: args.rps,
        duration_secs: args.requests.is_none().then_some(args.duration.as_secs_f64()),
        requests: args.requests,
        max_concurrent: args.max_concurrent as u64,
        passes: if args.phase == CallPhase::Measure { args.passes } else { 0 },
        concurrency: args.concurrency as u64,
        seed: args.seed,
        timeout_secs: args.timeout.as_secs_f64(),
        methods: (!args.methods.is_empty()).then(|| args.methods.clone()),
        block_tag: args.block_tag.clone(),
        strip_fees: args.strip_fees,
    }
}

fn write_outputs(args: &CallArgs, results: &ReplayResults) -> Result<()> {
    if let Some(path) = &args.responses {
        write_file(path, |writer| {
            for row in &results.responses {
                writeln!(writer, "{}", row.to_json())?;
            }
            Ok(())
        })?;
        tracing::info!(path = %path.display(), records = results.responses.len(), "Wrote response digests");
    }

    if let Some(path) = &args.record_csv {
        write_file(path, |writer| {
            writeln!(writer, "record_index,method,pass,latency_us,status")?;
            for row in &results.closed_loop {
                writeln!(
                    writer,
                    "{},{},{},{},{}",
                    row.record_index,
                    row.method,
                    row.pass,
                    row.latency_us,
                    row.status.as_str()
                )?;
            }
            Ok(())
        })?;
        tracing::info!(path = %path.display(), rows = results.closed_loop.len(), "Wrote closed-loop timings");
    }

    if let Some(path) = &args.requests_csv {
        write_file(path, |writer| {
            writeln!(writer, "offset_ms,record_index,method,latency_us,status")?;
            for row in &results.open_loop {
                writeln!(
                    writer,
                    "{},{},{},{},{}",
                    row.offset_ms,
                    row.record_index,
                    row.method,
                    row.latency_us,
                    row.status.as_str()
                )?;
            }
            Ok(())
        })?;
        tracing::info!(path = %path.display(), rows = results.open_loop.len(), "Wrote open-loop requests");
    }

    Ok(())
}

fn write_file(
    path: &Path,
    write: impl FnOnce(&mut BufWriter<std::fs::File>) -> Result<()>,
) -> Result<()> {
    let file = std::fs::File::create(path)
        .wrap_err_with(|| format!("failed to create {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    write(&mut writer)?;
    writer.flush().wrap_err_with(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

/// Wait until every in-flight open-loop request has released its permit.
async fn drain(semaphore: &Arc<Semaphore>, permits: usize) {
    if let Ok(permits) = u32::try_from(permits) {
        let _ = semaphore.acquire_many(permits).await;
    }
}

fn request_offset(index: u64, rps: u64) -> Duration {
    Duration::from_secs_f64(index as f64 / rps as f64)
}

fn status_of(summary: &ResponseSummary) -> RequestStatus {
    match summary.kind {
        ResponseKind::Ok => RequestStatus::Ok,
        ResponseKind::RpcError => RequestStatus::RpcError,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashMap,
        io::{BufRead, BufReader as StdBufReader, Read},
        net::{TcpListener, TcpStream},
        sync::{
            atomic::{AtomicBool, AtomicUsize},
            Mutex,
        },
        thread,
    };

    /// How the stub server answers a replayed request.
    #[derive(Clone, Copy)]
    enum StubBehavior {
        /// Answer immediately with a fixed result.
        Fast,
        /// Accept the request and never answer it.
        Withhold,
        /// Answer with a different result from the second time an id is seen.
        ChangeAfterFirst,
    }

    struct StubServer {
        url: String,
        served: Arc<AtomicUsize>,
        stop: Arc<AtomicBool>,
    }

    impl StubServer {
        fn start(behavior: StubBehavior) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let served = Arc::new(AtomicUsize::new(0));
            let stop = Arc::new(AtomicBool::new(false));
            let seen: Arc<Mutex<HashMap<u64, usize>>> = Arc::new(Mutex::new(HashMap::new()));

            let accept_served = served.clone();
            let accept_stop = stop.clone();
            thread::spawn(move || {
                while !accept_stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let served = accept_served.clone();
                            let seen = seen.clone();
                            let stop = accept_stop.clone();
                            thread::spawn(move || serve(stream, behavior, served, seen, stop));
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(2))
                        }
                        Err(error) => panic!("stub accept failed: {error}"),
                    }
                }
            });

            Self { url, served, stop }
        }

        fn served(&self) -> usize {
            self.served.load(Ordering::SeqCst)
        }
    }

    impl Drop for StubServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
        }
    }

    fn serve(
        stream: TcpStream,
        behavior: StubBehavior,
        served: Arc<AtomicUsize>,
        seen: Arc<Mutex<HashMap<u64, usize>>>,
        stop: Arc<AtomicBool>,
    ) {
        // The listener is non-blocking so the accept loop can poll the stop
        // flag; accepted streams inherit that on some platforms, which would
        // turn a partially arrived request into a closed connection.
        stream.set_nonblocking(false).unwrap();
        let mut reader = StdBufReader::new(stream.try_clone().unwrap());
        let mut stream = stream;

        loop {
            let Some(id) = read_request_id(&mut reader) else {
                return;
            };
            served.fetch_add(1, Ordering::SeqCst);

            if matches!(behavior, StubBehavior::Withhold) {
                while !stop.load(Ordering::SeqCst) {
                    thread::sleep(Duration::from_millis(5));
                }
                return;
            }

            let result = match behavior {
                StubBehavior::ChangeAfterFirst => {
                    let mut seen = seen.lock().unwrap();
                    let count = seen.entry(id).or_insert(0);
                    *count += 1;
                    if *count == 1 {
                        "0x01"
                    } else {
                        "0x02"
                    }
                }
                _ => "0x01",
            };

            let body = format!(r#"{{"jsonrpc":"2.0","id":{id},"result":"{result}"}}"#);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            if stream.write_all(response.as_bytes()).is_err() {
                return;
            }
        }
    }

    /// Read one HTTP request and return its JSON-RPC id.
    fn read_request_id(reader: &mut StdBufReader<TcpStream>) -> Option<u64> {
        let mut content_length = 0usize;
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).ok()? == 0 {
                return None;
            }
            if line == "\r\n" {
                break;
            }
            if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                content_length = value.trim().parse().ok()?;
            }
        }

        let mut body = vec![0u8; content_length];
        reader.read_exact(&mut body).ok()?;
        let request: serde_json::Value = serde_json::from_slice(&body).ok()?;
        request["id"].as_u64()
    }

    fn corpus_file(records: usize) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        for index in 0..records {
            writeln!(
                file,
                r#"{{"method":"eth_call","params":[{{"to":"0x{:040x}"}},"latest"]}}"#,
                index + 1
            )
            .unwrap();
        }
        file.flush().unwrap();
        file
    }

    /// A corpus of one method whose records carry different labels.
    fn labelled_corpus_file() -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        for label in ["callTracer", "structlog"] {
            writeln!(
                file,
                r#"{{"method":"debug_traceTransaction","params":["0x{:064x}"],"meta":{{"label":"{label}"}}}}"#,
                1
            )
            .unwrap();
        }
        file.flush().unwrap();
        file
    }

    fn args(url: &str, input: &Path) -> CallArgs {
        CallArgs {
            input: input.to_path_buf(),
            rpc_url: url.to_string(),
            phase: CallPhase::Measure,
            rps: 0,
            duration: Duration::from_millis(200),
            requests: None,
            max_concurrent: 16,
            passes: 0,
            concurrency: 2,
            seed: 1,
            block_tag: None,
            strip_fees: false,
            methods: Vec::new(),
            timeout: Duration::from_secs(2),
            responses: None,
            record_csv: None,
            requests_csv: None,
            max_fail_rate_pct: 100.0,
            reports: Vec::new(),
            metadata: Vec::new(),
            metrics_url: Vec::new(),
            clickhouse_metrics_file: None,
            scrape_interval_ms: 500,
            metrics_align: None,
            metrics_forward: None,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn open_loop_holds_the_configured_rate() {
        let server = StubServer::start(StubBehavior::Fast);
        let file = corpus_file(8);
        let mut args = args(&server.url, file.path());
        args.rps = 100;
        args.requests = Some(100);

        let replay = Replay::new(&args, Arc::new(load_corpus(&args).unwrap())).unwrap();
        let elapsed = replay.run_open_loop(&args, &RunClock::new()).await.unwrap();
        let results = replay.recorder.finish();
        let (totals, _) = results.stats(elapsed, 0.0);

        assert_eq!(totals.open_loop.requests, 100);
        assert_eq!(totals.dropped, 0);
        let error = (totals.open_loop.rps - 100.0).abs() / 100.0;
        assert!(error < 0.05, "achieved {:.1} req/s over {elapsed:.3}s", totals.open_loop.rps);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn open_loop_drops_instead_of_stalling_at_the_concurrency_cap() {
        let server = StubServer::start(StubBehavior::Withhold);
        let file = corpus_file(8);
        let mut args = args(&server.url, file.path());
        args.rps = 100;
        args.requests = Some(40);
        args.max_concurrent = 4;
        args.timeout = Duration::from_millis(400);

        let replay = Replay::new(&args, Arc::new(load_corpus(&args).unwrap())).unwrap();
        let started = Instant::now();
        let elapsed = replay.run_open_loop(&args, &RunClock::new()).await.unwrap();
        let results = replay.recorder.finish();
        let (totals, _) = results.stats(elapsed, 0.0);

        // The pacer never waited on the server: 40 requests were offered in the
        // 0.4s the rate implies, and everything past the cap was dropped.
        assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
        assert_eq!(totals.dropped + totals.open_loop.requests, 40);
        assert!(totals.dropped >= 30, "dropped {}", totals.dropped);
        assert_eq!(totals.open_loop.statuses.timeout, totals.open_loop.requests);
        assert!(server.served() >= 4, "served {}", server.served());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn closed_loop_reports_a_changed_response_with_its_record_index() {
        let server = StubServer::start(StubBehavior::ChangeAfterFirst);
        let file = corpus_file(3);
        let mut args = args(&server.url, file.path());
        args.passes = 2;
        args.concurrency = 1;

        let replay = Replay::new(&args, Arc::new(load_corpus(&args).unwrap())).unwrap();
        let elapsed = replay.run_closed_loop(&args).await.unwrap();
        let results = replay.recorder.finish();

        assert_eq!(results.closed_loop.len(), 6);
        assert_eq!(results.nondeterministic_total, 3);
        let flagged =
            results.nondeterministic.iter().map(|entry| entry.record_index).collect::<Vec<_>>();
        assert_eq!(flagged, vec![1, 2, 3]);
        assert!(results.nondeterministic.iter().all(|entry| entry.method == "eth_call"));

        // Every record keeps the digest of its first pass.
        let first_pass = alloy_primitives::keccak256(br#""0x01""#);
        assert!(results.responses.iter().all(|row| row.digest == first_pass && row.len == 6));
        let (totals, _) = results.stats(0.0, elapsed);
        assert_eq!(totals.closed_loop.statuses.ok, 6);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn writes_csv_headers_and_response_digests() {
        let server = StubServer::start(StubBehavior::Fast);
        let file = corpus_file(2);
        let dir = tempfile::tempdir().unwrap();
        let mut args = args(&server.url, file.path());
        args.rps = 200;
        args.requests = Some(4);
        args.passes = 2;
        args.responses = Some(dir.path().join("responses.ndjson"));
        args.record_csv = Some(dir.path().join("record_timings.csv"));
        args.requests_csv = Some(dir.path().join("requests.csv"));

        let replay = Replay::new(&args, Arc::new(load_corpus(&args).unwrap())).unwrap();
        replay.run_open_loop(&args, &RunClock::new()).await.unwrap();
        replay.run_closed_loop(&args).await.unwrap();
        write_outputs(&args, &replay.recorder.finish()).unwrap();

        let requests = std::fs::read_to_string(args.requests_csv.unwrap()).unwrap();
        assert_eq!(
            requests.lines().next().unwrap(),
            "offset_ms,record_index,method,latency_us,status"
        );
        assert_eq!(requests.lines().count(), 5);

        let timings = std::fs::read_to_string(args.record_csv.unwrap()).unwrap();
        assert_eq!(timings.lines().next().unwrap(), "record_index,method,pass,latency_us,status");
        assert_eq!(timings.lines().count(), 5);
        assert!(timings.lines().nth(1).unwrap().starts_with("1,eth_call,0,"));

        let responses = std::fs::read_to_string(args.responses.unwrap()).unwrap();
        assert_eq!(responses.lines().count(), 2);
        let first: serde_json::Value =
            serde_json::from_str(responses.lines().next().unwrap()).unwrap();
        assert_eq!(first["record_index"], 1);
        assert_eq!(first["method"], "eth_call");
        assert_eq!(first["kind"], "ok");
        assert_eq!(first["len"], 6);
        assert!(first["digest"].as_str().unwrap().starts_with("0x"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn labels_key_every_output() {
        let server = StubServer::start(StubBehavior::Fast);
        let file = labelled_corpus_file();
        let dir = tempfile::tempdir().unwrap();
        let mut args = args(&server.url, file.path());
        args.rps = 200;
        args.requests = Some(4);
        args.passes = 1;
        args.responses = Some(dir.path().join("responses.ndjson"));
        args.record_csv = Some(dir.path().join("record_timings.csv"));
        args.requests_csv = Some(dir.path().join("requests.csv"));

        let replay = Replay::new(&args, Arc::new(load_corpus(&args).unwrap())).unwrap();
        let open_loop_secs = replay.run_open_loop(&args, &RunClock::new()).await.unwrap();
        let closed_loop_secs = replay.run_closed_loop(&args).await.unwrap();
        let results = replay.recorder.finish();
        write_outputs(&args, &results).unwrap();

        let key = |line: &str| line.split(',').nth(2).unwrap().to_string();
        let requests = std::fs::read_to_string(args.requests_csv.unwrap()).unwrap();
        assert!(
            requests
                .lines()
                .skip(1)
                .map(key)
                .all(|method| method.starts_with("debug_traceTransaction:")),
            "{requests}"
        );

        let timings = std::fs::read_to_string(args.record_csv.unwrap()).unwrap();
        let mut replayed = timings.lines().skip(1).map(|line| line.split(',').nth(1).unwrap());
        assert_eq!(replayed.next(), Some("debug_traceTransaction:callTracer"));
        assert_eq!(replayed.next(), Some("debug_traceTransaction:structlog"));

        let responses = std::fs::read_to_string(args.responses.unwrap()).unwrap();
        let methods = responses
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .map(|row| row["method"].as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            vec!["debug_traceTransaction:callTracer", "debug_traceTransaction:structlog"]
        );

        let (_, per_key) = results.stats(open_loop_secs, closed_loop_secs);
        assert_eq!(
            per_key.keys().collect::<Vec<_>>(),
            vec!["debug_traceTransaction:callTracer", "debug_traceTransaction:structlog"]
        );
    }
}
