//! Corpus-driven RPC replay primitives.
//!
//! A replay corpus is NDJSON, one request per line, optionally gzip-compressed.
//! Each line names an allowlisted read-only method and carries the parameters
//! verbatim:
//!
//! ```json
//! {"method":"eth_call","params":[{"to":"0x..","input":"0x.."},"latest"],"meta":{"block":1,"index":0}}
//! ```
//!
//! This module owns the pieces that are independent of how requests are
//! scheduled: the method allowlist and its parameter-position table, the
//! optional request rewrites, the streaming response digest, and the
//! per-key accounting that both replay phases feed.
//!
//! Request and response bodies are never logged or written to any output.
//! Diagnostics identify records by their 1-based line number so a corpus of
//! captured traffic stays usable in a public CI log.

use crate::reporter::JsonLatency;
use alloy_primitives::{hex, keccak256, Keccak256, B256};
use eyre::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufRead, BufReader, Read},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

/// Maximum bytes of a JSON-RPC `error` member retained for digesting.
const MAX_ERROR_CAPTURE: usize = 64 * 1024;

/// The shape a `meta.label` must have, as documented in errors.
const LABEL_PATTERN: &str = "^[A-Za-z0-9._+:-]{1,48}$";

/// Maximum length of a `meta.label`.
const MAX_LABEL_LEN: usize = 48;

/// Maximum length of a member key tracked while scanning a response.
const MAX_KEY_LEN: usize = 32;

/// Fee fields removed from a call object by `--strip-fees`.
const FEE_FIELDS: [&str; 4] =
    ["gasPrice", "maxFeePerGas", "maxPriorityFeePerGas", "maxFeePerBlobGas"];

/// A replay corpus held in memory.
///
/// Records keep their 1-based line number as their identity: every output row
/// of a replay keys on it, so a corpus file and a result set can be correlated
/// without ever reproducing request contents.
#[derive(Debug, Default)]
pub struct Corpus {
    records: Vec<CorpusRecord>,
    counts: BTreeMap<Arc<str>, usize>,
    skipped: BTreeMap<Arc<str>, usize>,
}

impl Corpus {
    /// Load a corpus from a `.jsonl` or `.jsonl.gz` file.
    ///
    /// Compression is detected from the file contents, not the extension.
    pub fn load(path: &Path, options: &CorpusOptions) -> Result<Self> {
        let file = std::fs::File::open(path)
            .wrap_err_with(|| format!("failed to open corpus {}", path.display()))?;
        let mut reader = BufReader::new(file);
        let gzip = reader
            .fill_buf()
            .wrap_err_with(|| format!("failed to read corpus {}", path.display()))?
            .starts_with(&[0x1f, 0x8b]);

        if gzip {
            Self::read(BufReader::new(flate2::read::GzDecoder::new(reader)), options)
        } else {
            Self::read(reader, options)
        }
    }

    /// Read a corpus from any NDJSON reader.
    pub fn read<R: Read>(reader: BufReader<R>, options: &CorpusOptions) -> Result<Self> {
        let mut corpus = Self::default();

        for (offset, line) in reader.lines().enumerate() {
            let line_number = offset + 1;
            let line = line.wrap_err("failed to read corpus line")?;
            if line.trim().is_empty() {
                continue;
            }

            let raw: RawRecord = serde_json::from_str(&line).map_err(|err| {
                eyre::eyre!(
                    "corpus line {line_number}: malformed record ({:?} at column {})",
                    err.classify(),
                    err.column()
                )
            })?;

            let Some(method) = CallMethod::from_name(&raw.method) else {
                bail!("corpus line {line_number}: unsupported method `{}`", raw.method);
            };

            let meta = raw.meta.unwrap_or_default();
            let key = reporting_key(line_number, method, meta.label.as_deref())?;

            if !options.replays(method) {
                *corpus.skipped.entry(key).or_default() += 1;
                continue;
            }

            let serde_json::Value::Array(mut params) = raw.params else {
                bail!("corpus line {line_number}: `params` must be an array");
            };
            options.rewrite(method, &mut params);

            *corpus.counts.entry(key.clone()).or_default() += 1;
            corpus.records.push(CorpusRecord {
                record_index: line_number,
                method,
                key,
                body: request_body(line_number, method, &params)?,
                meta_block: meta.block,
                meta_index: meta.index,
            });
        }

        Ok(corpus)
    }

    /// Records retained after method filtering, in file order.
    pub fn records(&self) -> &[CorpusRecord] {
        &self.records
    }

    /// Number of retained records.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the corpus retained no records.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Retained record count per reporting key.
    pub fn counts_per_method(&self) -> &BTreeMap<Arc<str>, usize> {
        &self.counts
    }

    /// Record count per reporting key dropped by the `--methods` filter.
    pub fn skipped_per_method(&self) -> &BTreeMap<Arc<str>, usize> {
        &self.skipped
    }

    /// Total records dropped by the `--methods` filter.
    pub fn skipped(&self) -> usize {
        self.skipped.values().sum()
    }

    /// Summary of the loaded corpus for the report.
    pub fn summary(&self) -> CorpusSummary {
        CorpusSummary {
            records: self.records.len() as u64,
            records_per_method: named_counts(&self.counts),
            skipped: self.skipped() as u64,
            skipped_per_method: named_counts(&self.skipped),
        }
    }
}

/// One replayable request.
#[derive(Debug, Clone)]
pub struct CorpusRecord {
    /// 1-based line number in the corpus file; the record's identity.
    pub record_index: usize,
    /// The allowlisted method this record replays.
    pub method: CallMethod,
    /// The key this record is reported under: the method name, or
    /// `method:label` when the record carries a `meta.label`.
    pub key: Arc<str>,
    /// `meta.block` passed through from the corpus, when present.
    pub meta_block: Option<u64>,
    /// `meta.index` passed through from the corpus, when present.
    pub meta_index: Option<u64>,
    /// The serialized JSON-RPC request, built once at load time.
    body: String,
}

impl CorpusRecord {
    /// The serialized JSON-RPC request body sent for this record.
    pub fn body(&self) -> &str {
        &self.body
    }

    /// Parse the request parameters back out of the stored body.
    ///
    /// Re-parses on every call; intended for diagnostics and tests, not for
    /// the replay path, which sends [`body`](Self::body) as is.
    pub fn params(&self) -> Result<serde_json::Value> {
        let request: serde_json::Value = serde_json::from_str(&self.body)?;
        Ok(request["params"].clone())
    }
}

/// Corpus load options: the method filter and the optional request rewrites.
#[derive(Debug, Clone, Default)]
pub struct CorpusOptions {
    /// Replay only these methods; `None` replays every method in the corpus.
    pub methods: Option<BTreeSet<CallMethod>>,
    /// Replace the block parameter of every record that has a rewritable one.
    pub block_tag: Option<String>,
    /// Remove fee fields from the call object of call-shaped methods.
    pub strip_fees: bool,
}

impl CorpusOptions {
    /// Whether records of `method` are replayed rather than skipped.
    pub fn replays(&self, method: CallMethod) -> bool {
        self.methods.as_ref().is_none_or(|methods| methods.contains(&method))
    }

    /// Apply the configured rewrites to one record's parameters in place.
    fn rewrite(&self, method: CallMethod, params: &mut [serde_json::Value]) {
        if let Some(tag) = &self.block_tag &&
            method.block_param_rewritable() &&
            let Some(block) = method.block_param_index().and_then(|index| params.get_mut(index))
        {
            *block = serde_json::Value::String(tag.clone());
        }

        if self.strip_fees &&
            let Some(call) = method.call_object_index().and_then(|index| params.get_mut(index)) &&
            let Some(call) = call.as_object_mut()
        {
            for field in FEE_FIELDS {
                call.remove(field);
            }
        }
    }
}

/// The allowlisted read-only methods a corpus may replay.
///
/// Each method declares where its call object and block parameter sit, which
/// is the only interpretation the replay applies to a record's parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CallMethod {
    /// `eth_call`
    EthCall,
    /// `eth_estimateGas`
    EthEstimateGas,
    /// `eth_createAccessList`
    EthCreateAccessList,
    /// `debug_traceCall`
    DebugTraceCall,
    /// `trace_call`
    TraceCall,
    /// `debug_traceTransaction`
    DebugTraceTransaction,
    /// `trace_transaction`
    TraceTransaction,
    /// `trace_replayTransaction`
    TraceReplayTransaction,
    /// `debug_traceBlockByNumber`
    DebugTraceBlockByNumber,
    /// `trace_block`
    TraceBlock,
    /// `trace_replayBlockTransactions`
    TraceReplayBlockTransactions,
}

impl CallMethod {
    /// Every allowlisted method.
    pub const ALL: [Self; 11] = [
        Self::EthCall,
        Self::EthEstimateGas,
        Self::EthCreateAccessList,
        Self::DebugTraceCall,
        Self::TraceCall,
        Self::DebugTraceTransaction,
        Self::TraceTransaction,
        Self::TraceReplayTransaction,
        Self::DebugTraceBlockByNumber,
        Self::TraceBlock,
        Self::TraceReplayBlockTransactions,
    ];

    /// Resolve a JSON-RPC method name against the allowlist.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|method| method.as_str() == name)
    }

    /// The JSON-RPC method name.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::EthCall => "eth_call",
            Self::EthEstimateGas => "eth_estimateGas",
            Self::EthCreateAccessList => "eth_createAccessList",
            Self::DebugTraceCall => "debug_traceCall",
            Self::TraceCall => "trace_call",
            Self::DebugTraceTransaction => "debug_traceTransaction",
            Self::TraceTransaction => "trace_transaction",
            Self::TraceReplayTransaction => "trace_replayTransaction",
            Self::DebugTraceBlockByNumber => "debug_traceBlockByNumber",
            Self::TraceBlock => "trace_block",
            Self::TraceReplayBlockTransactions => "trace_replayBlockTransactions",
        }
    }

    /// Parameter position of the call object, for call-shaped methods.
    pub const fn call_object_index(&self) -> Option<usize> {
        match self {
            Self::EthCall |
            Self::EthEstimateGas |
            Self::EthCreateAccessList |
            Self::DebugTraceCall |
            Self::TraceCall => Some(0),
            _ => None,
        }
    }

    /// Parameter position of the block parameter, when the method has one.
    pub const fn block_param_index(&self) -> Option<usize> {
        match self {
            Self::EthCall |
            Self::EthEstimateGas |
            Self::EthCreateAccessList |
            Self::DebugTraceCall => Some(1),
            Self::TraceCall => Some(2),
            Self::DebugTraceBlockByNumber |
            Self::TraceBlock |
            Self::TraceReplayBlockTransactions => Some(0),
            _ => None,
        }
    }

    /// Whether `--block-tag` may replace this method's block parameter.
    ///
    /// Block-addressed tracing methods take a concrete block number that
    /// identifies the record, so their parameter is never rewritten.
    pub const fn block_param_rewritable(&self) -> bool {
        !matches!(
            self,
            Self::DebugTraceBlockByNumber | Self::TraceBlock | Self::TraceReplayBlockTransactions
        )
    }
}

impl std::fmt::Display for CallMethod {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for CallMethod {
    type Err = eyre::Report;

    fn from_str(value: &str) -> Result<Self> {
        Self::from_name(value).ok_or_else(|| eyre::eyre!("unsupported method `{value}`"))
    }
}

/// Incremental scanner over a JSON-RPC response body.
///
/// Hashes the raw bytes of the top-level `result` member as they arrive, so a
/// trace response of tens of megabytes is never buffered or parsed. The much
/// smaller `error` member is captured instead, up to a fixed cap, so its code
/// and message can be digested.
#[derive(Debug)]
pub struct ResponseScanner {
    phase: Phase,
    key: String,
    key_overflow: bool,
    value: ValueScan,
    member: Member,
    result: Option<Digested>,
    error: Option<Vec<u8>>,
}

impl Default for ResponseScanner {
    fn default() -> Self {
        Self::new()
    }
}

impl ResponseScanner {
    /// Create a scanner positioned before the response's opening brace.
    pub fn new() -> Self {
        Self {
            phase: Phase::BeforeObject,
            key: String::new(),
            key_overflow: false,
            value: ValueScan::default(),
            member: Member::Other,
            result: None,
            error: None,
        }
    }

    /// Feed the next chunk of the response body.
    pub fn feed(&mut self, chunk: &[u8]) -> Result<()> {
        let mut index = 0;
        while index < chunk.len() {
            match self.phase {
                Phase::BeforeObject => {
                    let byte = chunk[index];
                    index += 1;
                    if byte == b'{' {
                        self.phase = Phase::BeforeKey;
                    } else if !byte.is_ascii_whitespace() {
                        bail!("response is not a JSON object");
                    }
                }
                Phase::BeforeKey => {
                    let byte = chunk[index];
                    index += 1;
                    match byte {
                        b'"' => {
                            self.key.clear();
                            self.key_overflow = false;
                            self.phase = Phase::InKey { escaped: false };
                        }
                        b'}' => self.phase = Phase::Finished,
                        b',' => {}
                        byte if byte.is_ascii_whitespace() => {}
                        _ => return Err(eyre::eyre!("malformed response object")),
                    }
                }
                Phase::InKey { escaped } => {
                    let byte = chunk[index];
                    index += 1;
                    if escaped {
                        self.push_key(byte);
                        self.phase = Phase::InKey { escaped: false };
                    } else if byte == b'\\' {
                        self.phase = Phase::InKey { escaped: true };
                    } else if byte == b'"' {
                        self.member = Member::classify(&self.key, self.key_overflow);
                        self.phase = Phase::BeforeColon;
                    } else {
                        self.push_key(byte);
                    }
                }
                Phase::BeforeColon => {
                    let byte = chunk[index];
                    index += 1;
                    if byte == b':' {
                        self.value = ValueScan::default();
                        self.start_member();
                        self.phase = Phase::InValue;
                    } else if !byte.is_ascii_whitespace() {
                        bail!("malformed response object");
                    }
                }
                Phase::InValue => {
                    let progress = self.value.feed(&chunk[index..]);
                    self.consume_member(&chunk[index + progress.skipped..index + progress.end]);
                    index += progress.end;
                    if progress.complete {
                        self.finish_member();
                        self.phase = Phase::AfterValue;
                    }
                }
                Phase::AfterValue => {
                    let byte = chunk[index];
                    index += 1;
                    match byte {
                        b',' => self.phase = Phase::BeforeKey,
                        b'}' => self.phase = Phase::Finished,
                        byte if byte.is_ascii_whitespace() => {}
                        _ => return Err(eyre::eyre!("malformed response object")),
                    }
                }
                Phase::Finished => return Ok(()),
            }
        }

        Ok(())
    }

    /// Bytes the scanner currently retains.
    ///
    /// Bounded regardless of response size: a `result` member is hashed as it
    /// arrives and never accumulated, and a captured `error` member stops at
    /// [`MAX_ERROR_CAPTURE`].
    pub fn retained_bytes(&self) -> usize {
        self.key.capacity() + self.error.as_ref().map_or(0, Vec::capacity)
    }

    /// Classify and digest the scanned response.
    pub fn finish(mut self) -> Result<ResponseSummary> {
        if !matches!(self.phase, Phase::Finished) {
            bail!("response body ended before the JSON-RPC object closed");
        }

        if let Some(error) = self.error.take() {
            return Ok(digest_rpc_error(&error));
        }

        let Some(result) = self.result.take() else {
            bail!("response carried neither a result nor an error");
        };

        Ok(ResponseSummary {
            kind: ResponseKind::Ok,
            digest: result.hasher.finalize(),
            len: result.len,
        })
    }

    fn push_key(&mut self, byte: u8) {
        if self.key.len() >= MAX_KEY_LEN {
            self.key_overflow = true;
            return;
        }
        self.key.push(byte as char);
    }

    fn start_member(&mut self) {
        match self.member {
            Member::Result => self.result = Some(Digested::default()),
            Member::Error => self.error = Some(Vec::new()),
            Member::Other => {}
        }
    }

    fn consume_member(&mut self, bytes: &[u8]) {
        match self.member {
            Member::Result => {
                if let Some(result) = self.result.as_mut() {
                    result.hasher.update(bytes);
                    result.len += bytes.len();
                }
            }
            Member::Error => {
                if let Some(error) = self.error.as_mut() {
                    let room = MAX_ERROR_CAPTURE.saturating_sub(error.len());
                    error.extend_from_slice(&bytes[..bytes.len().min(room)]);
                }
            }
            Member::Other => {}
        }
    }

    fn finish_member(&mut self) {
        self.member = Member::Other;
    }
}

/// The classification and digest of one replayed response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseSummary {
    /// Whether the node answered with a result or a JSON-RPC error.
    pub kind: ResponseKind,
    /// `keccak256` of the digested bytes.
    pub digest: B256,
    /// Number of bytes digested: the raw `result` value, or the error's code
    /// and message.
    pub len: usize,
}

/// Whether a response carried a result or a JSON-RPC error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseKind {
    /// A JSON-RPC success.
    Ok,
    /// A JSON-RPC error object.
    RpcError,
}

impl ResponseKind {
    /// Lowercase name used in outputs.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::RpcError => "rpc_error",
        }
    }
}

/// How a replayed request finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RequestStatus {
    /// A JSON-RPC success.
    Ok,
    /// The node answered with a JSON-RPC error object.
    RpcError,
    /// The server answered with a non-success HTTP status.
    HttpError,
    /// The request never produced a usable response.
    TransportError,
    /// The request exceeded `--timeout`.
    Timeout,
}

impl RequestStatus {
    /// Lowercase name used in CSV rows and counters.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::RpcError => "rpc_error",
            Self::HttpError => "http_error",
            Self::TransportError => "transport_error",
            Self::Timeout => "timeout",
        }
    }

    /// Whether the node answered at all, so the latency is meaningful.
    pub const fn answered(&self) -> bool {
        matches!(self, Self::Ok | Self::RpcError)
    }

    /// Whether this counts toward the HTTP and transport failure rate.
    pub const fn failed(&self) -> bool {
        matches!(self, Self::HttpError | Self::TransportError | Self::Timeout)
    }
}

/// One replayed request's outcome.
#[derive(Debug, Clone, Copy)]
pub struct RequestOutcome {
    /// Wall-clock time from request start to fully digested response.
    pub latency: Duration,
    /// How the request finished.
    pub status: RequestStatus,
    /// The response digest, when the node answered.
    pub response: Option<ResponseSummary>,
}

/// Shared accounting for both replay phases.
///
/// Every counter, latency sample and output row is keyed by the record's
/// reporting key, so a mixed corpus reports per key as well as in total. Two
/// records of the same method that carry different labels are separate keys
/// and are never pooled. Digests are compared
/// as they arrive: the first closed-loop pass sets each record's reference
/// digest, and any later disagreement within the same run is reported as
/// nondeterminism rather than silently averaged away.
#[derive(Debug)]
pub struct ReplayRecorder {
    records: Vec<RecordKey>,
    max_nondeterministic: usize,
    state: Mutex<RecorderState>,
}

impl ReplayRecorder {
    /// Create a recorder sized for `corpus`, capping the reported
    /// nondeterministic record list at `max_nondeterministic` entries.
    pub fn new(corpus: &Corpus, max_nondeterministic: usize) -> Self {
        let records = corpus
            .records()
            .iter()
            .map(|record| RecordKey { record_index: record.record_index, key: record.key.clone() })
            .collect::<Vec<_>>();
        let state = RecorderState { references: vec![None; records.len()], ..Default::default() };
        Self { records, max_nondeterministic, state: Mutex::new(state) }
    }

    /// Record one open-loop request issued at `offset_ms` after the run start.
    ///
    /// `slot` is the record's position in the loaded corpus.
    pub fn record_open_loop(&self, slot: usize, offset_ms: u64, outcome: RequestOutcome) {
        let key = &self.records[slot];
        let mut state = self.lock();
        state.open_loop.push(OpenLoopRow {
            offset_ms,
            record_index: key.record_index,
            method: key.key.clone(),
            latency_us: outcome.latency.as_micros() as u64,
            status: outcome.status,
        });
        self.observe(&mut state, slot, outcome, false);
    }

    /// Record one closed-loop request from `pass` (0-based).
    pub fn record_closed_loop(&self, slot: usize, pass: u32, outcome: RequestOutcome) {
        let key = &self.records[slot];
        let mut state = self.lock();
        state.closed_loop.push(ClosedLoopRow {
            record_index: key.record_index,
            method: key.key.clone(),
            pass,
            latency_us: outcome.latency.as_micros() as u64,
            status: outcome.status,
        });
        self.observe(&mut state, slot, outcome, pass == 0);
    }

    /// Record an open-loop request that was never issued because it would
    /// have exceeded `--max-concurrent`.
    pub fn record_dropped(&self, slot: usize) {
        let key = self.records[slot].key.clone();
        let mut state = self.lock();
        *state.dropped.entry(key).or_default() += 1;
    }

    /// Take the collected rows and counters, leaving the recorder empty.
    pub fn finish(&self) -> ReplayResults {
        let mut state = std::mem::take(&mut *self.lock());
        state.open_loop.sort_by_key(|row| (row.offset_ms, row.record_index));
        state.closed_loop.sort_by_key(|row| (row.record_index, row.pass));

        let responses = self
            .records
            .iter()
            .zip(state.references.iter())
            .filter_map(|(key, reference)| {
                reference.as_ref().map(|reference| ResponseRow {
                    record_index: key.record_index,
                    method: key.key.clone(),
                    kind: reference.kind,
                    digest: reference.digest,
                    len: reference.len,
                })
            })
            .collect();

        ReplayResults {
            open_loop: state.open_loop,
            closed_loop: state.closed_loop,
            responses,
            dropped: state.dropped,
            nondeterministic: state.nondeterministic,
            nondeterministic_total: state.nondeterministic_total,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RecorderState> {
        self.state.lock().unwrap_or_else(|err| err.into_inner())
    }

    /// Compare a response against the record's reference digest.
    ///
    /// `authoritative` marks the first closed-loop pass, whose digest is the
    /// one written to the responses file.
    fn observe(
        &self,
        state: &mut RecorderState,
        slot: usize,
        outcome: RequestOutcome,
        authoritative: bool,
    ) {
        let Some(response) = outcome.response else {
            return;
        };
        let key = &self.records[slot];
        let reference =
            Reference { kind: response.kind, digest: response.digest, len: response.len as u64 };

        match &state.references[slot] {
            Some(existing) if existing.digest == reference.digest => return,
            Some(_) => {
                state.nondeterministic_total += 1;
                if state.nondeterministic.len() < self.max_nondeterministic {
                    state.nondeterministic.push(Nondeterministic {
                        record_index: key.record_index,
                        method: key.key.to_string(),
                    });
                }
                if !authoritative {
                    return;
                }
            }
            None => {}
        }

        state.references[slot] = Some(reference);
    }
}

/// Rows and counters collected by a [`ReplayRecorder`].
#[derive(Debug, Default)]
pub struct ReplayResults {
    /// Open-loop requests, ordered by offset.
    pub open_loop: Vec<OpenLoopRow>,
    /// Closed-loop requests, ordered by record then pass.
    pub closed_loop: Vec<ClosedLoopRow>,
    /// Reference digest per record that was answered at least once.
    pub responses: Vec<ResponseRow>,
    /// Open-loop requests dropped at the concurrency cap, per reporting key.
    pub dropped: BTreeMap<Arc<str>, u64>,
    /// Records whose digest changed within the run, capped for reporting.
    pub nondeterministic: Vec<Nondeterministic>,
    /// Total disagreements observed, including those beyond the cap.
    pub nondeterministic_total: usize,
}

impl ReplayResults {
    /// Build per-key and total statistics.
    ///
    /// `open_loop_secs` and `closed_loop_secs` are the wall-clock durations of
    /// the two phases, used for the achieved request rates.
    pub fn stats(
        &self,
        open_loop_secs: f64,
        closed_loop_secs: f64,
    ) -> (MethodStats, BTreeMap<String, MethodStats>) {
        let mut builders: BTreeMap<Arc<str>, StatsBuilder> = BTreeMap::new();
        let mut total = StatsBuilder::default();

        for row in &self.open_loop {
            let builder = builders.entry(row.method.clone()).or_default();
            builder.open.push(row.status, row.latency_us);
            total.open.push(row.status, row.latency_us);
        }
        for row in &self.closed_loop {
            let builder = builders.entry(row.method.clone()).or_default();
            builder.closed.push(row.status, row.latency_us);
            total.closed.push(row.status, row.latency_us);
        }
        for row in &self.responses {
            let builder = builders.entry(row.method.clone()).or_default();
            builder.response_bytes.push(row.len);
            total.response_bytes.push(row.len);
        }
        for (method, dropped) in &self.dropped {
            builders.entry(method.clone()).or_default().dropped += dropped;
            total.dropped += dropped;
        }

        let methods = builders
            .into_iter()
            .map(|(method, builder)| {
                (method.to_string(), builder.build(open_loop_secs, closed_loop_secs))
            })
            .collect();
        (total.build(open_loop_secs, closed_loop_secs), methods)
    }
}

/// One row of `requests.csv`.
#[derive(Debug, Clone)]
pub struct OpenLoopRow {
    /// Milliseconds from the run start to the request being issued.
    pub offset_ms: u64,
    /// The replayed record.
    pub record_index: usize,
    /// The record's reporting key.
    pub method: Arc<str>,
    /// Request latency in microseconds.
    pub latency_us: u64,
    /// How the request finished.
    pub status: RequestStatus,
}

/// One row of `record_timings.csv`.
#[derive(Debug, Clone)]
pub struct ClosedLoopRow {
    /// The replayed record.
    pub record_index: usize,
    /// The record's reporting key.
    pub method: Arc<str>,
    /// 0-based closed-loop pass.
    pub pass: u32,
    /// Request latency in microseconds.
    pub latency_us: u64,
    /// How the request finished.
    pub status: RequestStatus,
}

/// One row of `responses.ndjson`.
#[derive(Debug, Clone)]
pub struct ResponseRow {
    /// The replayed record.
    pub record_index: usize,
    /// The record's reporting key.
    pub method: Arc<str>,
    /// Whether the reference response was a result or a JSON-RPC error.
    pub kind: ResponseKind,
    /// `keccak256` of the digested response bytes.
    pub digest: B256,
    /// Number of bytes digested.
    pub len: u64,
}

impl ResponseRow {
    /// Serialize the row as one NDJSON line.
    pub fn to_json(&self) -> String {
        format!(
            r#"{{"record_index":{},"method":"{}","kind":"{}","digest":"0x{}","len":{}}}"#,
            self.record_index,
            self.method,
            self.kind.as_str(),
            hex::encode(self.digest),
            self.len
        )
    }
}

/// A record whose response digest changed within one run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Nondeterministic {
    /// The corpus record.
    pub record_index: usize,
    /// The record's reporting key.
    pub method: String,
}

/// Summary of the loaded corpus written to the report.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CorpusSummary {
    /// Records retained after method filtering.
    pub records: u64,
    /// Retained records per reporting key.
    pub records_per_method: BTreeMap<String, u64>,
    /// Records dropped by the method filter.
    pub skipped: u64,
    /// Dropped records per reporting key.
    pub skipped_per_method: BTreeMap<String, u64>,
}

/// Node identity captured before the measured phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeIdentity {
    /// `eth_chainId`.
    pub chain_id: u64,
    /// Latest block number.
    pub head: u64,
    /// Latest block hash; runs with differing hashes are not comparable.
    pub head_hash: String,
}

/// The replay configuration echoed into the report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallRunConfig {
    /// Open-loop target rate.
    pub rps: u64,
    /// Open-loop wall clock, when bounded by time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_secs: Option<f64>,
    /// Open-loop request count, when bounded by count.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requests: Option<u64>,
    /// In-flight cap for the open-loop phase.
    pub max_concurrent: u64,
    /// Closed-loop passes over the corpus.
    pub passes: u64,
    /// Closed-loop workers.
    pub concurrency: u64,
    /// Seed of the open-loop record sequence.
    pub seed: u64,
    /// Per-request timeout.
    pub timeout_secs: f64,
    /// Active `--methods` filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub methods: Option<Vec<String>>,
    /// Active `--block-tag` rewrite.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_tag: Option<String>,
    /// Whether `--strip-fees` was applied.
    pub strip_fees: bool,
}

/// The `call` section of a `bench call` report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallReport {
    /// `warmup` or `measure`.
    pub phase: String,
    /// What was loaded and what was filtered out.
    pub corpus: CorpusSummary,
    /// The node the corpus was replayed against.
    pub identity: NodeIdentity,
    /// The replay configuration.
    pub config: CallRunConfig,
    /// Open-loop latency across the whole corpus.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency: Option<JsonLatency>,
    /// Open-loop requests dropped at the concurrency cap.
    pub dropped: u64,
    /// Achieved closed-loop request rate.
    pub closed_loop_rps: f64,
    /// Counters and latencies across the whole corpus.
    pub totals: MethodStats,
    /// Counters and latencies per reporting key.
    pub methods: BTreeMap<String, MethodStats>,
    /// Records whose digest changed within the run.
    pub nondeterministic: Vec<Nondeterministic>,
    /// Total disagreements, including those beyond the reported cap.
    pub nondeterministic_total: u64,
}

/// Counters and latency distributions for one reporting key, or for a whole
/// corpus.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MethodStats {
    /// Requests issued across both phases.
    pub requests: u64,
    /// Per-status counts across both phases.
    pub statuses: StatusCounts,
    /// Open-loop requests dropped at the concurrency cap.
    pub dropped: u64,
    /// HTTP and transport failures as a percentage of requests.
    pub failure_rate_pct: f64,
    /// Open-loop phase.
    pub open_loop: PhaseStats,
    /// Closed-loop phase.
    pub closed_loop: PhaseStats,
    /// Digested response sizes.
    pub response_bytes: ResponseBytes,
}

/// Counters and latency for one replay phase.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PhaseStats {
    /// Requests issued in this phase.
    pub requests: u64,
    /// Achieved requests per second over the phase's wall clock.
    pub rps: f64,
    /// Per-status counts.
    pub statuses: StatusCounts,
    /// Latency of answered requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency: Option<JsonLatency>,
}

/// Per-status request counts.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct StatusCounts {
    /// JSON-RPC successes.
    pub ok: u64,
    /// JSON-RPC error responses.
    pub rpc_error: u64,
    /// Non-success HTTP statuses.
    pub http_error: u64,
    /// Requests that produced no usable response.
    pub transport_error: u64,
    /// Requests that exceeded the timeout.
    pub timeout: u64,
}

impl StatusCounts {
    /// Total requests counted.
    pub const fn total(&self) -> u64 {
        self.ok + self.rpc_error + self.http_error + self.transport_error + self.timeout
    }

    /// HTTP and transport failures.
    pub const fn failed(&self) -> u64 {
        self.http_error + self.transport_error + self.timeout
    }

    fn push(&mut self, status: RequestStatus) {
        match status {
            RequestStatus::Ok => self.ok += 1,
            RequestStatus::RpcError => self.rpc_error += 1,
            RequestStatus::HttpError => self.http_error += 1,
            RequestStatus::TransportError => self.transport_error += 1,
            RequestStatus::Timeout => self.timeout += 1,
        }
    }

    fn merge(&mut self, other: &Self) {
        self.ok += other.ok;
        self.rpc_error += other.rpc_error;
        self.http_error += other.http_error;
        self.transport_error += other.transport_error;
        self.timeout += other.timeout;
    }
}

/// Digested response sizes for one method.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct ResponseBytes {
    /// Records contributing a reference response.
    pub records: u64,
    /// Median digested response size.
    pub median: u64,
    /// Total digested response size across records.
    pub total: u64,
}

/// Summarize latency samples, adding the p90 the open-loop cell reports on.
pub fn latency_summary(latencies_us: &[u64]) -> Option<JsonLatency> {
    if latencies_us.is_empty() {
        return None;
    }

    let mut sorted = latencies_us.to_vec();
    sorted.sort_unstable();
    let sum = sorted.iter().map(|value| *value as u128).sum::<u128>();
    let mean = (sum / sorted.len() as u128) as u64;

    Some(JsonLatency {
        min_ms: us_to_ms(sorted[0]),
        max_ms: us_to_ms(sorted[sorted.len() - 1]),
        mean_ms: us_to_ms(mean),
        p50_ms: us_to_ms(percentile(&sorted, 50)),
        p90_ms: Some(us_to_ms(percentile(&sorted, 90))),
        p95_ms: us_to_ms(percentile(&sorted, 95)),
        p99_ms: us_to_ms(percentile(&sorted, 99)),
    })
}

/// Digest the raw body of a JSON-RPC response without buffering it.
pub async fn digest_response(mut response: reqwest::Response) -> Result<ResponseSummary> {
    let mut scanner = ResponseScanner::new();
    while let Some(chunk) = response.chunk().await.wrap_err("failed to read response body")? {
        scanner.feed(&chunk)?;
    }
    scanner.finish()
}

#[derive(Debug)]
enum Phase {
    BeforeObject,
    BeforeKey,
    InKey { escaped: bool },
    BeforeColon,
    InValue,
    AfterValue,
    Finished,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Member {
    Result,
    Error,
    Other,
}

impl Member {
    fn classify(key: &str, overflow: bool) -> Self {
        if overflow {
            return Self::Other;
        }
        match key {
            "result" => Self::Result,
            "error" => Self::Error,
            _ => Self::Other,
        }
    }
}

/// Incremental scan of one JSON value.
#[derive(Debug, Default)]
struct ValueScan {
    /// Nesting depth of objects and arrays; zero before the value starts.
    depth: usize,
    in_string: bool,
    escaped: bool,
    /// Scanning a bare literal (number, `true`, `false`, `null`).
    literal: bool,
    started: bool,
}

/// How far a [`ValueScan`] got through one chunk.
struct ValueProgress {
    /// Leading whitespace bytes, which are not part of the value.
    skipped: usize,
    /// Bytes of the chunk belonging to this member, including the whitespace.
    end: usize,
    /// Whether the value is complete.
    complete: bool,
}

impl ValueScan {
    fn feed(&mut self, chunk: &[u8]) -> ValueProgress {
        let mut index = 0;
        let mut skipped = 0;

        while index < chunk.len() {
            let byte = chunk[index];

            if !self.started {
                if byte.is_ascii_whitespace() {
                    index += 1;
                    skipped += 1;
                    continue;
                }
                self.started = true;
                match byte {
                    b'{' | b'[' => self.depth = 1,
                    b'"' => self.in_string = true,
                    _ => self.literal = true,
                }
                index += 1;
                continue;
            }

            if self.in_string {
                index += 1;
                if self.escaped {
                    self.escaped = false;
                } else if byte == b'\\' {
                    self.escaped = true;
                } else if byte == b'"' {
                    self.in_string = false;
                    if self.depth == 0 {
                        return ValueProgress { skipped, end: index, complete: true };
                    }
                }
                continue;
            }

            if self.literal {
                // A literal ends at the first byte that cannot belong to it;
                // that byte belongs to the enclosing object.
                if byte.is_ascii_whitespace() || byte == b',' || byte == b'}' || byte == b']' {
                    return ValueProgress { skipped, end: index, complete: true };
                }
                index += 1;
                continue;
            }

            index += 1;
            match byte {
                b'"' => self.in_string = true,
                b'{' | b'[' => self.depth += 1,
                b'}' | b']' => {
                    self.depth -= 1;
                    if self.depth == 0 {
                        return ValueProgress { skipped, end: index, complete: true };
                    }
                }
                _ => {}
            }
        }

        ValueProgress { skipped, end: index, complete: false }
    }
}

#[derive(Debug, Default)]
struct Digested {
    hasher: Keccak256,
    len: usize,
}

#[derive(Debug, Default)]
struct RecorderState {
    open_loop: Vec<OpenLoopRow>,
    closed_loop: Vec<ClosedLoopRow>,
    references: Vec<Option<Reference>>,
    dropped: BTreeMap<Arc<str>, u64>,
    nondeterministic: Vec<Nondeterministic>,
    nondeterministic_total: usize,
}

#[derive(Debug, Clone)]
struct RecordKey {
    record_index: usize,
    key: Arc<str>,
}

#[derive(Debug, Clone, Copy)]
struct Reference {
    kind: ResponseKind,
    digest: B256,
    len: u64,
}

#[derive(Debug, Default)]
struct StatsBuilder {
    open: PhaseBuilder,
    closed: PhaseBuilder,
    dropped: u64,
    response_bytes: Vec<u64>,
}

impl StatsBuilder {
    fn build(mut self, open_loop_secs: f64, closed_loop_secs: f64) -> MethodStats {
        let mut statuses = self.open.statuses;
        statuses.merge(&self.closed.statuses);

        self.response_bytes.sort_unstable();
        let response_bytes = ResponseBytes {
            records: self.response_bytes.len() as u64,
            median: median(&self.response_bytes),
            total: self.response_bytes.iter().sum(),
        };

        let requests = statuses.total();
        MethodStats {
            requests,
            statuses,
            dropped: self.dropped,
            failure_rate_pct: rate_pct(statuses.failed(), requests),
            open_loop: self.open.build(open_loop_secs),
            closed_loop: self.closed.build(closed_loop_secs),
            response_bytes,
        }
    }
}

#[derive(Debug, Default)]
struct PhaseBuilder {
    statuses: StatusCounts,
    latencies_us: Vec<u64>,
}

impl PhaseBuilder {
    fn push(&mut self, status: RequestStatus, latency_us: u64) {
        self.statuses.push(status);
        if status.answered() {
            self.latencies_us.push(latency_us);
        }
    }

    fn build(self, secs: f64) -> PhaseStats {
        let requests = self.statuses.total();
        PhaseStats {
            requests,
            rps: if secs > 0.0 { requests as f64 / secs } else { 0.0 },
            statuses: self.statuses,
            latency: latency_summary(&self.latencies_us),
        }
    }
}

#[derive(Deserialize)]
struct RawRecord {
    method: String,
    params: serde_json::Value,
    #[serde(default)]
    meta: Option<RecordMeta>,
}

/// The opaque `meta` object; only `block`, `index` and `label` are read.
#[derive(Default, Deserialize)]
struct RecordMeta {
    #[serde(default)]
    block: Option<u64>,
    #[serde(default)]
    index: Option<u64>,
    #[serde(default)]
    label: Option<String>,
}

/// The key a record is reported under.
///
/// A label separates variants of the same method - one tracer from another,
/// say - so their latencies and digests are never pooled. The label is kept
/// out of the error text: a corpus of captured traffic must stay usable in a
/// public log.
fn reporting_key(line: usize, method: CallMethod, label: Option<&str>) -> Result<Arc<str>> {
    let Some(label) = label else {
        return Ok(Arc::from(method.as_str()));
    };

    let valid = (1..=MAX_LABEL_LEN).contains(&label.len()) &&
        label.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"._+:-".contains(&byte));
    if !valid {
        bail!("corpus line {line}: `meta.label` must match {LABEL_PATTERN}");
    }

    Ok(Arc::from(format!("{}:{label}", method.as_str())))
}

fn request_body(id: usize, method: CallMethod, params: &[serde_json::Value]) -> Result<String> {
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method.as_str(),
        "params": params,
    });
    serde_json::to_string(&request).wrap_err("failed to serialize corpus request")
}

fn digest_rpc_error(error: &[u8]) -> ResponseSummary {
    #[derive(Deserialize)]
    struct RpcError {
        #[serde(default)]
        code: i64,
        #[serde(default)]
        message: String,
    }

    // An error member that was truncated at the capture cap still digests
    // deterministically, just over the raw bytes instead of the parsed pair.
    let digested = serde_json::from_slice::<RpcError>(error)
        .map(|error| format!("{}\n{}", error.code, error.message).into_bytes())
        .unwrap_or_else(|_| error.to_vec());

    ResponseSummary {
        kind: ResponseKind::RpcError,
        digest: keccak256(&digested),
        len: digested.len(),
    }
}

fn named_counts(counts: &BTreeMap<Arc<str>, usize>) -> BTreeMap<String, u64> {
    counts.iter().map(|(key, count)| (key.to_string(), *count as u64)).collect()
}

fn percentile(sorted: &[u64], p: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(sorted.len() * p / 100).min(sorted.len() - 1)]
}

fn median(sorted: &[u64]) -> u64 {
    percentile(sorted, 50)
}

fn rate_pct(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        part as f64 / total as f64 * 100.0
    }
}

fn us_to_ms(micros: u64) -> f64 {
    micros as f64 / 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn load(lines: &str, options: &CorpusOptions) -> Result<Corpus> {
        Corpus::read(BufReader::new(Cursor::new(lines.as_bytes().to_vec())), options)
    }

    fn scan(body: &str) -> Result<ResponseSummary> {
        let mut scanner = ResponseScanner::new();
        scanner.feed(body.as_bytes())?;
        scanner.finish()
    }

    fn scan_chunked(body: &str, chunk: usize) -> Result<ResponseSummary> {
        let mut scanner = ResponseScanner::new();
        for piece in body.as_bytes().chunks(chunk) {
            scanner.feed(piece)?;
        }
        scanner.finish()
    }

    #[test]
    fn accepts_every_allowlisted_method() {
        let lines = CallMethod::ALL
            .iter()
            .map(|method| format!(r#"{{"method":"{method}","params":[]}}"#))
            .collect::<Vec<_>>()
            .join("\n");

        let corpus = load(&lines, &CorpusOptions::default()).unwrap();
        assert_eq!(corpus.len(), CallMethod::ALL.len());
        for (index, record) in corpus.records().iter().enumerate() {
            assert_eq!(record.method, CallMethod::ALL[index]);
            assert_eq!(record.record_index, index + 1);
        }
    }

    #[test]
    fn rejects_unknown_method_by_line_and_name() {
        let error = load(
            "{\"method\":\"eth_call\",\"params\":[]}\n{\"method\":\"eth_sendRawTransaction\",\"params\":[\"0xdeadbeef\"]}",
            &CorpusOptions::default(),
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("corpus line 2"), "{error}");
        assert!(error.contains("eth_sendRawTransaction"), "{error}");
        assert!(!error.contains("0xdeadbeef"), "{error}");
    }

    #[test]
    fn rejects_invalid_json_without_echoing_the_line() {
        let error = load(
            r#"{"method":"eth_call","params":[{"secret":"0xc0ffee"#,
            &CorpusOptions::default(),
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("corpus line 1"), "{error}");
        assert!(!error.contains("c0ffee"), "{error}");
    }

    #[test]
    fn rejects_non_array_params() {
        let error = load(r#"{"method":"eth_call","params":{}}"#, &CorpusOptions::default())
            .unwrap_err()
            .to_string();
        assert!(error.contains("must be an array"), "{error}");
    }

    #[test]
    fn reads_gzip_corpus() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corpus.jsonl.gz");
        let file = std::fs::File::create(&path).unwrap();
        let mut encoder = flate2::write::GzEncoder::new(file, flate2::Compression::fast());
        encoder.write_all(br#"{"method":"eth_call","params":[{},"latest"]}"#).unwrap();
        encoder.write_all(b"\n").unwrap();
        encoder.finish().unwrap();

        let corpus = Corpus::load(&path, &CorpusOptions::default()).unwrap();
        assert_eq!(corpus.len(), 1);
        assert_eq!(corpus.records()[0].method, CallMethod::EthCall);
    }

    #[test]
    fn passes_meta_through() {
        let corpus = load(
            r#"{"method":"trace_block","params":["0x1"],"meta":{"block":25490001,"index":3,"hash":"0xaa"}}"#,
            &CorpusOptions::default(),
        )
        .unwrap();

        assert_eq!(corpus.records()[0].meta_block, Some(25_490_001));
        assert_eq!(corpus.records()[0].meta_index, Some(3));
    }

    #[test]
    fn block_tag_rewrites_the_documented_position() {
        let lines = concat!(
            r#"{"method":"eth_call","params":[{},"0x10"]}"#,
            "\n",
            r#"{"method":"debug_traceCall","params":[{},"0x10",{"tracer":"callTracer"}]}"#,
            "\n",
            r#"{"method":"trace_call","params":[{},["trace"],"0x10"]}"#,
            "\n",
            r#"{"method":"debug_traceTransaction","params":["0xaa"]}"#,
            "\n",
            r#"{"method":"trace_block","params":["0x10"]}"#,
        );
        let options = CorpusOptions { block_tag: Some("latest".into()), ..Default::default() };
        let corpus = load(lines, &options).unwrap();
        let params =
            corpus.records().iter().map(|record| record.params().unwrap()).collect::<Vec<_>>();

        assert_eq!(params[0][1], "latest");
        assert_eq!(params[1][1], "latest");
        assert_eq!(params[1][2]["tracer"], "callTracer");
        assert_eq!(params[2][1][0], "trace");
        assert_eq!(params[2][2], "latest");
        assert_eq!(params[3][0], "0xaa");
        assert_eq!(params[4][0], "0x10");
    }

    #[test]
    fn strip_fees_removes_only_fee_fields_of_the_call_object() {
        let lines = concat!(
            r#"{"method":"eth_call","params":[{"to":"0xaa","gas":"0x1","gasPrice":"0x2","maxFeePerGas":"0x3","maxPriorityFeePerGas":"0x4","maxFeePerBlobGas":"0x5"},"latest",{"0xbb":{"balance":"0x1","gasPrice":"0x9"}}]}"#,
            "\n",
            r#"{"method":"trace_call","params":[{"gasPrice":"0x2"},["trace"],"latest"]}"#,
        );
        let options = CorpusOptions { strip_fees: true, ..Default::default() };
        let corpus = load(lines, &options).unwrap();
        let params =
            corpus.records().iter().map(|record| record.params().unwrap()).collect::<Vec<_>>();

        let call = params[0][0].as_object().unwrap();
        assert_eq!(call.len(), 2);
        assert_eq!(call["to"], "0xaa");
        assert_eq!(call["gas"], "0x1");
        assert_eq!(params[0][2]["0xbb"]["gasPrice"], "0x9");
        assert!(params[1][0].as_object().unwrap().is_empty());
    }

    #[test]
    fn methods_filter_skips_and_counts() {
        let lines = concat!(
            r#"{"method":"eth_call","params":[{},"latest"]}"#,
            "\n",
            r#"{"method":"eth_estimateGas","params":[{},"latest"]}"#,
            "\n",
            r#"{"method":"eth_call","params":[{},"latest"]}"#,
        );
        let options = CorpusOptions {
            methods: Some(BTreeSet::from([CallMethod::EthCall])),
            ..Default::default()
        };
        let corpus = load(lines, &options).unwrap();

        assert_eq!(corpus.len(), 2);
        assert_eq!(corpus.skipped(), 1);
        assert_eq!(corpus.skipped_per_method()["eth_estimateGas"], 1);
        // Record identity stays the corpus line number.
        assert_eq!(corpus.records()[1].record_index, 3);
    }

    #[test]
    fn labels_split_one_method_into_several_reporting_keys() {
        let lines = concat!(
            r#"{"method":"debug_traceTransaction","params":["0xaa"],"meta":{"label":"callTracer"}}"#,
            "\n",
            r#"{"method":"debug_traceTransaction","params":["0xaa"],"meta":{"label":"structlog"}}"#,
            "\n",
            r#"{"method":"debug_traceTransaction","params":["0xaa"]}"#,
        );
        let corpus = load(lines, &CorpusOptions::default()).unwrap();

        let keys = corpus.records().iter().map(|record| &*record.key).collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                "debug_traceTransaction:callTracer",
                "debug_traceTransaction:structlog",
                "debug_traceTransaction",
            ]
        );
        // Every record still replays the same allowlisted method.
        assert!(corpus
            .records()
            .iter()
            .all(|record| record.method == CallMethod::DebugTraceTransaction));
        assert_eq!(corpus.summary().records_per_method.len(), 3);
    }

    #[test]
    fn unlabelled_records_report_under_the_bare_method_name() {
        let lines = concat!(
            r#"{"method":"eth_call","params":[{},"latest"],"meta":{"block":1}}"#,
            "\n",
            r#"{"method":"eth_call","params":[{},"latest"]}"#,
            "\n",
            r#"{"method":"trace_block","params":["0x1"]}"#,
        );
        let corpus = load(lines, &CorpusOptions::default()).unwrap();

        assert_eq!(
            corpus.summary().records_per_method,
            BTreeMap::from([("eth_call".to_string(), 2), ("trace_block".to_string(), 1)])
        );
    }

    #[test]
    fn rejects_an_invalid_label_by_line_without_echoing_it() {
        for label in ["", "has space", "semi;colon", &"x".repeat(49)] {
            let lines = format!(
                "{{\"method\":\"eth_call\",\"params\":[]}}\n{{\"method\":\"eth_call\",\"params\":[],\"meta\":{{\"label\":\"{label}\"}}}}"
            );
            let error = load(&lines, &CorpusOptions::default()).unwrap_err().to_string();

            assert!(error.contains("corpus line 2"), "{label:?}: {error}");
            assert!(error.contains(LABEL_PATTERN), "{label:?}: {error}");
            assert!(!error.contains(label) || label.is_empty(), "{label:?}: {error}");
        }
    }

    #[test]
    fn accepts_every_character_the_label_pattern_allows() {
        let label = "prestateTracer.diff+2:v1-x";
        let lines = format!(r#"{{"method":"eth_call","params":[],"meta":{{"label":"{label}"}}}}"#);
        let corpus = load(&lines, &CorpusOptions::default()).unwrap();
        assert_eq!(&*corpus.records()[0].key, format!("eth_call:{label}"));
    }

    #[test]
    fn methods_filter_selects_by_the_base_method_of_a_labelled_record() {
        let lines = concat!(
            r#"{"method":"debug_traceCall","params":[{},"latest",{}],"meta":{"label":"structlog"}}"#,
            "\n",
            r#"{"method":"debug_traceCall","params":[{},"latest",{}],"meta":{"label":"callTracer"}}"#,
            "\n",
            r#"{"method":"eth_call","params":[{},"latest"],"meta":{"label":"plain"}}"#,
        );
        let options = CorpusOptions {
            methods: Some(BTreeSet::from([CallMethod::DebugTraceCall])),
            ..Default::default()
        };
        let corpus = load(lines, &options).unwrap();

        assert_eq!(corpus.len(), 2);
        assert_eq!(corpus.summary().records_per_method.len(), 2);
        // Skipped records are counted under their own reporting key too.
        assert_eq!(corpus.skipped_per_method()["eth_call:plain"], 1);
    }

    #[test]
    fn identical_results_digest_identically() {
        let a = scan(r#"{"jsonrpc":"2.0","id":1,"result":"0x2a"}"#).unwrap();
        let b = scan(r#"{"id":7,"result":"0x2a","jsonrpc":"2.0"}"#).unwrap();
        assert_eq!(a.digest, b.digest);
        assert_eq!(a.kind, ResponseKind::Ok);
        assert_eq!(a.len, 6);
    }

    #[test]
    fn whitespace_differences_are_not_normalized() {
        let dense = scan(r#"{"result":{"a":1,"b":[2,3]}}"#).unwrap();
        let spaced = scan(r#"{"result": {"a": 1, "b": [2, 3]}}"#).unwrap();
        assert_ne!(dense.digest, spaced.digest);
        assert_eq!(dense.digest, keccak256(br#"{"a":1,"b":[2,3]}"#));
    }

    #[test]
    fn chunk_boundaries_do_not_change_the_digest() {
        let body = r#"{"jsonrpc":"2.0","id":1,"result":{"calls":[{"input":"0x\"escaped\\","to":"0xaa"}],"gas":"0x1"}}"#;
        let whole = scan(body).unwrap();
        for size in 1..=17 {
            assert_eq!(scan_chunked(body, size).unwrap(), whole, "chunk size {size}");
        }
    }

    #[test]
    fn scalar_and_null_results_are_digested() {
        assert_eq!(scan(r#"{"result":null}"#).unwrap().digest, keccak256(b"null"));
        assert_eq!(scan(r#"{"result":12345}"#).unwrap().digest, keccak256(b"12345"));
        assert_eq!(scan(r#"{"result":true,"id":1}"#).unwrap().digest, keccak256(b"true"));
    }

    #[test]
    fn error_digest_covers_code_and_message() {
        let reverted = scan(r#"{"error":{"code":3,"message":"execution reverted"}}"#).unwrap();
        assert_eq!(reverted.kind, ResponseKind::RpcError);
        assert_eq!(reverted.digest, keccak256(b"3\nexecution reverted"));

        let other_code =
            scan(r#"{"error":{"code":-32000,"message":"execution reverted"}}"#).unwrap();
        let other_message = scan(r#"{"error":{"code":3,"message":"out of gas"}}"#).unwrap();
        assert_ne!(reverted.digest, other_code.digest);
        assert_ne!(reverted.digest, other_message.digest);
    }

    #[test]
    fn ignores_unrelated_members_before_the_result() {
        let summary =
            scan(r#"{"jsonrpc":"2.0","id":1,"extra":{"result":"decoy"},"result":"0x01"}"#).unwrap();
        assert_eq!(summary.digest, keccak256(br#""0x01""#));
    }

    #[test]
    fn rejects_a_body_that_is_not_a_response_object() {
        assert!(scan("[1,2,3]").is_err());
        assert!(scan(r#"{"jsonrpc":"2.0","id":1}"#).is_err());
    }

    #[test]
    fn large_results_are_hashed_without_being_retained() {
        let payload = "a".repeat(8 * 1024 * 1024);
        let body = format!(r#"{{"result":"{payload}"}}"#);

        let mut scanner = ResponseScanner::new();
        for piece in body.as_bytes().chunks(64 * 1024) {
            scanner.feed(piece).unwrap();
            // The payload is folded into the hasher instead of accumulating.
            assert!(scanner.retained_bytes() < 1024, "{}", scanner.retained_bytes());
        }
        let summary = scanner.finish().unwrap();

        assert_eq!(summary.len, payload.len() + 2);
        assert_eq!(summary.digest, keccak256(format!(r#""{payload}""#)));
    }

    #[test]
    fn a_truncated_body_is_not_digested() {
        let mut scanner = ResponseScanner::new();
        scanner.feed(br#"{"result":{"calls":[{"to":"0xaa""#).unwrap();
        assert!(scanner.finish().is_err());
    }

    #[test]
    fn latency_summary_reports_p90() {
        let samples = (1..=100).map(|value| value * 1000).collect::<Vec<_>>();
        let latency = latency_summary(&samples).unwrap();

        assert_eq!(latency.min_ms, 1.0);
        assert_eq!(latency.max_ms, 100.0);
        assert_eq!(latency.p50_ms, 51.0);
        assert_eq!(latency.p90_ms, Some(91.0));
        assert_eq!(latency.p99_ms, 100.0);
        assert!(latency_summary(&[]).is_none());
    }

    #[test]
    fn recorder_flags_a_changed_digest_with_its_record_index() {
        let corpus = load(
            concat!(
                r#"{"method":"eth_call","params":[{},"latest"]}"#,
                "\n",
                r#"{"method":"eth_call","params":[{},"latest"]}"#,
            ),
            &CorpusOptions::default(),
        )
        .unwrap();
        let recorder = ReplayRecorder::new(&corpus, 200);

        let outcome = |digest: u8| RequestOutcome {
            latency: Duration::from_millis(1),
            status: RequestStatus::Ok,
            response: Some(ResponseSummary {
                kind: ResponseKind::Ok,
                digest: B256::repeat_byte(digest),
                len: 16,
            }),
        };

        recorder.record_closed_loop(0, 0, outcome(0xaa));
        recorder.record_closed_loop(1, 0, outcome(0xbb));
        recorder.record_closed_loop(0, 1, outcome(0xaa));
        recorder.record_closed_loop(1, 1, outcome(0xcc));

        let results = recorder.finish();
        assert_eq!(results.nondeterministic_total, 1);
        assert_eq!(results.nondeterministic[0].record_index, 2);
        assert_eq!(results.nondeterministic[0].method, "eth_call");
        // The responses file keeps the first closed-loop pass's digest.
        assert_eq!(results.responses[1].digest, B256::repeat_byte(0xbb));
    }

    #[test]
    fn recorder_keeps_the_first_closed_loop_digest_over_an_earlier_one() {
        let corpus =
            load(r#"{"method":"eth_call","params":[{},"latest"]}"#, &CorpusOptions::default())
                .unwrap();
        let recorder = ReplayRecorder::new(&corpus, 200);
        let outcome = |digest: u8| RequestOutcome {
            latency: Duration::from_millis(1),
            status: RequestStatus::Ok,
            response: Some(ResponseSummary {
                kind: ResponseKind::Ok,
                digest: B256::repeat_byte(digest),
                len: 8,
            }),
        };

        recorder.record_open_loop(0, 5, outcome(0xaa));
        recorder.record_closed_loop(0, 0, outcome(0xbb));

        let results = recorder.finish();
        assert_eq!(results.responses[0].digest, B256::repeat_byte(0xbb));
        assert_eq!(results.nondeterministic_total, 1);
    }

    #[test]
    fn stats_and_rows_carry_the_reporting_key() {
        let corpus = load(
            concat!(
                r#"{"method":"debug_traceTransaction","params":["0xaa"],"meta":{"label":"callTracer"}}"#,
                "\n",
                r#"{"method":"debug_traceTransaction","params":["0xaa"],"meta":{"label":"structlog"}}"#,
            ),
            &CorpusOptions::default(),
        )
        .unwrap();
        let recorder = ReplayRecorder::new(&corpus, 200);

        let outcome = |digest: u8| RequestOutcome {
            latency: Duration::from_millis(1),
            status: RequestStatus::Ok,
            response: Some(ResponseSummary {
                kind: ResponseKind::Ok,
                digest: B256::repeat_byte(digest),
                len: 32,
            }),
        };

        recorder.record_open_loop(0, 0, outcome(0xaa));
        recorder.record_closed_loop(0, 0, outcome(0xaa));
        recorder.record_closed_loop(1, 0, outcome(0xbb));
        recorder.record_closed_loop(1, 1, outcome(0xcc));
        recorder.record_dropped(1);

        let results = recorder.finish();
        assert_eq!(&*results.open_loop[0].method, "debug_traceTransaction:callTracer");
        assert_eq!(&*results.closed_loop[0].method, "debug_traceTransaction:callTracer");
        assert_eq!(&*results.responses[1].method, "debug_traceTransaction:structlog");
        assert!(results.responses[1]
            .to_json()
            .contains(r#""method":"debug_traceTransaction:structlog""#));
        assert_eq!(results.nondeterministic[0].method, "debug_traceTransaction:structlog");

        let (totals, methods) = results.stats(1.0, 1.0);
        assert_eq!(totals.requests, 4);
        assert_eq!(
            methods.keys().collect::<Vec<_>>(),
            vec!["debug_traceTransaction:callTracer", "debug_traceTransaction:structlog",]
        );
        assert_eq!(methods["debug_traceTransaction:callTracer"].requests, 2);
        assert_eq!(methods["debug_traceTransaction:structlog"].dropped, 1);
    }

    #[test]
    fn stats_are_keyed_by_method() {
        let corpus = load(
            concat!(
                r#"{"method":"eth_call","params":[{},"latest"]}"#,
                "\n",
                r#"{"method":"debug_traceCall","params":[{},"latest",{"tracer":"callTracer"}]}"#,
            ),
            &CorpusOptions::default(),
        )
        .unwrap();
        let recorder = ReplayRecorder::new(&corpus, 200);

        let outcome = |slot: usize, status: RequestStatus, latency_ms: u64| RequestOutcome {
            latency: Duration::from_millis(latency_ms),
            status,
            response: status.answered().then_some(ResponseSummary {
                kind: ResponseKind::Ok,
                digest: B256::repeat_byte(slot as u8),
                len: (slot + 1) * 100,
            }),
        };

        recorder.record_open_loop(0, 0, outcome(0, RequestStatus::Ok, 2));
        recorder.record_open_loop(1, 10, outcome(1, RequestStatus::Timeout, 30));
        recorder.record_closed_loop(0, 0, outcome(0, RequestStatus::Ok, 4));
        recorder.record_closed_loop(1, 0, outcome(1, RequestStatus::Ok, 8));
        recorder.record_dropped(1);

        let results = recorder.finish();
        let (totals, methods) = results.stats(1.0, 2.0);

        assert_eq!(totals.requests, 4);
        assert_eq!(totals.dropped, 1);
        assert_eq!(totals.statuses.timeout, 1);
        assert_eq!(totals.closed_loop.rps, 1.0);

        let eth_call = &methods["eth_call"];
        assert_eq!(eth_call.requests, 2);
        assert_eq!(eth_call.dropped, 0);
        assert_eq!(eth_call.failure_rate_pct, 0.0);
        assert_eq!(eth_call.response_bytes.median, 100);

        let trace_call = &methods["debug_traceCall"];
        assert_eq!(trace_call.requests, 2);
        assert_eq!(trace_call.dropped, 1);
        assert_eq!(trace_call.failure_rate_pct, 50.0);
        assert_eq!(trace_call.open_loop.statuses.timeout, 1);
        assert_eq!(trace_call.open_loop.latency, None);
        assert_eq!(trace_call.closed_loop.latency.as_ref().unwrap().p50_ms, 8.0);
    }
}
