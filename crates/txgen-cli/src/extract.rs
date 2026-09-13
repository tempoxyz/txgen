use crate::bal::{
    fetch_block_access_list, fetch_encoded_block_access_list, merge_block_access_lists,
};
use alloy_consensus::{transaction::SignerRecoverable, BlockHeader, Sealed, Transaction};
use alloy_eips::{eip2718::Encodable2718, eip7928::BlockAccessList, BlockNumberOrTag};
use alloy_network::Network;
use alloy_primitives::{Address, Bytes, Sealable, B256};
use alloy_provider::{ext::DebugApi, Provider, RootProvider};
use alloy_rlp::Decodable;
use alloy_rpc_client::RpcClient;
use alloy_rpc_types_engine::{ExecutionData, ExecutionPayload};
use alloy_transport::layers::RetryBackoffLayer;
use bench_core::CallMethod;
use clap::{Args, ValueEnum};
use eyre::{bail, Result, WrapErr};
use futures::{stream, StreamExt};
use std::{
    io::Write,
    path::{Path, PathBuf},
};
use tokio::sync::mpsc;

const RAW_BLOCK_FETCH_ATTEMPTS: u32 = 5;
const RAW_BLOCK_FETCH_INITIAL_BACKOFF_MS: u64 = 250;

/// The tracer emitted when `--tracer` is omitted.
const DEFAULT_TRACER: &str = "callTracer";

/// The `--tracer` spec that leaves the `tracer` field out, so the node runs
/// its own struct logger.
const STRUCTLOG_TRACER: &str = "structlog";

/// The `--tracer` prefix naming a JavaScript tracer file.
const JS_TRACER_PREFIX: &str = "js:";

/// The trace types of the plain parity-style variant.
const PARITY_TRACE: [&str; 1] = ["trace"];

/// The trace types of the state-diff parity-style variant.
const PARITY_TRACE_STATE_DIFF: [&str; 2] = ["trace", "stateDiff"];

/// The shape `bench call` requires of a `meta.label`.
const LABEL_PATTERN: &str = "^[A-Za-z0-9._+:-]{1,48}$";

/// Maximum length of a `meta.label`.
const MAX_LABEL_LEN: usize = 48;

#[derive(Args)]
pub struct ExtractArgs {
    /// RPC endpoint URL (archive node with debug_getRawBlock)
    #[arg(long)]
    pub rpc: String,

    /// First block number to fetch (inclusive)
    #[arg(long)]
    pub from: u64,

    /// Last block number to fetch (inclusive)
    #[arg(long)]
    pub to: u64,

    /// Output file (default: stdout)
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Number of blocks to prefetch ahead
    #[arg(long, default_value = "20")]
    pub buffer_size: usize,

    /// Include RLP-encoded block access lists from eth_getBlockAccessListByBlockNumber.
    #[arg(long, default_value_t = false)]
    pub bal: bool,

    /// Output raw blocks, the signed transactions contained in them, or an
    /// RPC replay corpus built from them.
    ///
    /// Transaction output is compatible with `bench send` and therefore
    /// replays the source transactions through `eth_sendRawTransaction` and
    /// the node's transaction pool. Corpus output is compatible with
    /// `bench call`.
    #[arg(long, value_enum, default_value_t = ExtractFormat::Blocks)]
    pub format: ExtractFormat,

    /// Methods to emit for `--format calls` and `--format traces`.
    ///
    /// Defaults to `eth_call` for calls and
    /// `debug_traceTransaction,trace_transaction` for traces. Records are
    /// emitted in a fixed method order regardless of the order given here.
    #[arg(long, value_delimiter = ',', value_name = "METHOD")]
    pub methods: Vec<String>,

    /// For `--format calls` and `--format traces`, keep only the N transactions
    /// with the highest gas limit in each block.
    ///
    /// Ties keep the earlier transaction, block order is preserved, and
    /// block-level records are unaffected.
    #[arg(long, value_name = "N")]
    pub top_gas: Option<usize>,

    /// Tracer to emit for the `debug_trace*` methods of `--format calls` and
    /// `--format traces`; repeatable, defaults to `callTracer`.
    ///
    /// A spec is a tracer name passed verbatim to the node (`callTracer`,
    /// `prestateTracer`, `flatCallTracer`, ...), `structlog` for the node's
    /// struct logger, or `js:<path>` to send the contents of a JavaScript
    /// tracer file. Each spec emits its own record, labelled with the spec.
    #[arg(long = "tracer", value_name = "SPEC")]
    pub tracer: Vec<String>,

    /// JSON object merged as `tracerConfig` into every named-tracer record.
    #[arg(long, value_name = "JSON")]
    pub tracer_config: Option<String>,

    /// JSON object merged into the top level of every `debug_trace*` record's
    /// tracing options, for `timeout`, `disableStorage`, `limit` and the rest.
    #[arg(long, value_name = "JSON")]
    pub trace_options: Option<String>,
}

/// Output produced by `extract`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ExtractFormat {
    /// One raw RLP-encoded block per line, for `bench send-blocks`.
    Blocks,
    /// One signed transaction per line, for `bench send`.
    Transactions,
    /// A replay corpus of calls built from each transaction, for `bench call`.
    Calls,
    /// A replay corpus addressing the transactions and blocks themselves,
    /// for `bench call`.
    Traces,
}

impl ExtractFormat {
    /// Methods this format can emit, in the order records are written.
    const fn available_methods(&self) -> &'static [CallMethod] {
        match self {
            Self::Blocks | Self::Transactions => &[],
            Self::Calls => &[
                CallMethod::EthCall,
                CallMethod::EthEstimateGas,
                CallMethod::EthCreateAccessList,
                CallMethod::DebugTraceCall,
                CallMethod::TraceCall,
            ],
            Self::Traces => &[
                CallMethod::DebugTraceTransaction,
                CallMethod::TraceTransaction,
                CallMethod::TraceReplayTransaction,
                CallMethod::DebugTraceBlockByNumber,
                CallMethod::TraceBlock,
                CallMethod::TraceReplayBlockTransactions,
            ],
        }
    }

    /// Methods emitted when `--methods` is omitted.
    const fn default_methods(&self) -> &'static [CallMethod] {
        match self {
            Self::Blocks | Self::Transactions => &[],
            Self::Calls => &[CallMethod::EthCall],
            Self::Traces => &[CallMethod::DebugTraceTransaction, CallMethod::TraceTransaction],
        }
    }

    /// Whether the format emits a replay corpus.
    const fn is_corpus(&self) -> bool {
        matches!(self, Self::Calls | Self::Traces)
    }
}

#[derive(Args)]
pub struct ExtractBigBlocksArgs {
    /// RPC endpoint URL (archive node with debug_getRawBlock)
    #[arg(long)]
    pub rpc: String,

    /// First source block number to fetch
    #[arg(long)]
    pub from: u64,

    /// Number of synthetic big blocks to emit
    #[arg(long)]
    pub count: u64,

    /// Target gas usage per synthetic big block. Accepts K, M, or G suffixes.
    #[arg(long, value_parser = parse_gas_limit)]
    pub target_gas: u64,

    /// Output file (default: stdout)
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Number of source blocks to prefetch ahead.
    ///
    /// Big-block extraction currently fetches sequentially; this flag is accepted for CLI
    /// compatibility with `extract` and future pipelining.
    #[arg(long, default_value = "20")]
    pub buffer_size: usize,

    /// Include and merge block access lists from eth_getBlockAccessListByBlockNumber.
    #[arg(long, default_value_t = false)]
    pub bal: bool,
}

fn parse_gas_limit(value: &str) -> Result<u64, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("gas limit cannot be empty".to_string());
    }

    let (number, multiplier) = match value.as_bytes().last().copied() {
        Some(b'k' | b'K') => (&value[..value.len() - 1], 1_000_u64),
        Some(b'm' | b'M') => (&value[..value.len() - 1], 1_000_000_u64),
        Some(b'g' | b'G') => (&value[..value.len() - 1], 1_000_000_000_u64),
        _ => (value, 1_u64),
    };

    let parsed = number.parse::<u64>().map_err(|e| format!("invalid gas limit {value:?}: {e}"))?;
    parsed.checked_mul(multiplier).ok_or_else(|| format!("gas limit {value:?} overflows u64"))
}

// ---------------------------------------------------------------------------
// Extract implementation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize)]
struct BigBlockData<T> {
    env_switches: Vec<T>,
    prior_block_hashes: Vec<(u64, B256)>,
    block_number: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    merged_block_access_list: Option<Bytes>,
}

pub(crate) async fn run_extract<N>(args: ExtractArgs) -> Result<()>
where
    N: Network,
    N::TxEnvelope: Decodable + Encodable2718 + SignerRecoverable + Transaction + 'static,
    N::Header: Decodable + 'static,
{
    if args.from > args.to {
        bail!("--from must be <= --to");
    }

    let corpus = resolve_corpus_config(&args)?;
    let provider = retrying_http_provider::<N>(&args.rpc)?;

    let (tx, mut rx) = mpsc::channel::<Result<FetchedBlock>>(args.buffer_size);

    let from = args.from;
    let to = args.to;
    let include_bal = args.bal;
    let buffer_size = args.buffer_size;
    let format = args.format;
    let fetch_handle = tokio::spawn(async move {
        fetch_blocks::<N, _>(provider, from, to, include_bal, buffer_size, format, tx).await
    });

    let total = to - from + 1;
    let write_result = match args.output {
        Some(ref path) => {
            let file = std::fs::File::create(path)
                .wrap_err_with(|| format!("failed to create output file: {}", path.display()))?;
            let mut writer = std::io::BufWriter::new(file);
            let result = write_extracted_blocks(&mut rx, &mut writer, total, format, &corpus).await;
            if let Ok(item_count) = &result {
                match format {
                    ExtractFormat::Blocks => {
                        eprintln!("wrote {item_count} blocks to {}", path.display())
                    }
                    ExtractFormat::Transactions => eprintln!(
                        "wrote {item_count} transactions from {total} blocks to {}",
                        path.display()
                    ),
                    ExtractFormat::Calls | ExtractFormat::Traces => eprintln!(
                        "wrote {item_count} records from {total} blocks to {}",
                        path.display()
                    ),
                }
            }
            result.map(|_| ())
        }
        None => {
            let mut writer = std::io::stdout();
            write_extracted_blocks(&mut rx, &mut writer, total, format, &corpus).await.map(|_| ())
        }
    };

    fetch_handle.await?;
    write_result
}

/// Resolve everything that decides which corpus records a block contributes.
fn resolve_corpus_config(args: &ExtractArgs) -> Result<CorpusConfig> {
    let format = args.format;
    if !format.is_corpus() {
        for (flag, given) in [
            ("--tracer", !args.tracer.is_empty()),
            ("--tracer-config", args.tracer_config.is_some()),
            ("--trace-options", args.trace_options.is_some()),
        ] {
            if given {
                bail!("{flag} only applies to --format calls and --format traces");
            }
        }
    }

    let tracers = resolve_tracers(format, &args.tracer)?;
    let tracer_config = parse_options_object("--tracer-config", args.tracer_config.as_deref())?;
    if tracer_config.is_some() &&
        let Some(spec) = tracers.iter().find(|spec| spec.tracer.is_none())
    {
        bail!(
            "--tracer-config cannot be combined with `--tracer {}`; the struct logger is configured through --trace-options",
            spec.label
        );
    }

    let options =
        parse_options_object("--trace-options", args.trace_options.as_deref())?.unwrap_or_default();
    for (key, flag) in [("tracer", "--tracer"), ("tracerConfig", "--tracer-config")] {
        if options.contains_key(key) {
            bail!("--trace-options must not set `{key}`; use {flag}");
        }
    }

    Ok(CorpusConfig {
        methods: resolve_methods(format, &args.methods)?,
        top_gas: resolve_top_gas(format, args.top_gas)?,
        tracers,
        tracer_config: tracer_config.map(serde_json::Value::Object),
        options,
    })
}

/// Resolve the `--tracer` specs, defaulting to `callTracer`.
fn resolve_tracers(format: ExtractFormat, requested: &[String]) -> Result<Vec<TracerSpec>> {
    if !format.is_corpus() {
        return Ok(Vec::new());
    }
    if requested.is_empty() {
        return Ok(vec![TracerSpec::builtin(DEFAULT_TRACER)]);
    }
    requested.iter().map(|spec| TracerSpec::parse(spec)).collect()
}

/// Parse a flag that takes a JSON object.
fn parse_options_object(flag: &str, value: Option<&str>) -> Result<Option<TraceOptions>> {
    let Some(value) = value else {
        return Ok(None);
    };

    let parsed: serde_json::Value =
        serde_json::from_str(value).wrap_err_with(|| format!("{flag} is not valid JSON"))?;
    match parsed {
        serde_json::Value::Object(object) => Ok(Some(object)),
        _ => Err(eyre::eyre!("{flag} must be a JSON object")),
    }
}

/// Validate `--top-gas` against the format.
fn resolve_top_gas(format: ExtractFormat, top_gas: Option<usize>) -> Result<Option<usize>> {
    match top_gas {
        None => Ok(None),
        Some(_) if !format.is_corpus() => {
            Err(eyre::eyre!("--top-gas applies to --format calls and --format traces only"))
        }
        Some(0) => Err(eyre::eyre!("--top-gas must be greater than zero")),
        Some(limit) => Ok(Some(limit)),
    }
}

/// Resolve `--methods` against the methods the format can emit.
fn resolve_methods(format: ExtractFormat, requested: &[String]) -> Result<Vec<CallMethod>> {
    if !format.is_corpus() {
        if !requested.is_empty() {
            bail!("--methods only applies to --format calls and --format traces");
        }
        return Ok(Vec::new());
    }

    if requested.is_empty() {
        return Ok(format.default_methods().to_vec());
    }

    let available = format.available_methods();
    let mut selected = Vec::new();
    for name in requested {
        let method = available.iter().find(|method| method.as_str() == name).ok_or_else(|| {
            eyre::eyre!(
                "--format {:?} cannot emit `{name}`; available: {}",
                format,
                available.iter().map(CallMethod::as_str).collect::<Vec<_>>().join(", ")
            )
        })?;
        if !selected.contains(method) {
            selected.push(*method);
        }
    }

    // Emit in the format's own order so the corpus is deterministic regardless
    // of how the methods were listed on the command line.
    Ok(available.iter().copied().filter(|method| selected.contains(method)).collect())
}

pub(crate) async fn run_extract_big_blocks<N>(args: ExtractBigBlocksArgs) -> Result<()>
where
    N: Network,
    N::TxEnvelope: Decodable + Encodable2718 + Transaction + 'static,
    N::Header: Decodable + BlockHeader + Sealable + 'static,
{
    if args.count == 0 {
        bail!("--count must be greater than 0");
    }
    if args.target_gas == 0 {
        bail!("--target-gas must be greater than 0");
    }

    let provider = retrying_http_provider::<N>(&args.rpc)?;

    match args.output {
        Some(ref path) => {
            let file = std::fs::File::create(path)
                .wrap_err_with(|| format!("failed to create output file: {}", path.display()))?;
            let mut writer = std::io::BufWriter::new(file);
            write_big_blocks::<N, _, _>(&provider, &mut writer, &args).await?;
            eprintln!("wrote {} big blocks to {}", args.count, path.display());
        }
        None => {
            let mut writer = std::io::stdout();
            write_big_blocks::<N, _, _>(&provider, &mut writer, &args).await?;
        }
    }

    Ok(())
}

#[derive(serde::Serialize)]
struct BlockOutputLine<'a> {
    raw: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    bal: Option<&'a str>,
    key: &'a str,
    number: u64,
    timestamp: u64,
    gas_used: u64,
    gas_limit: u64,
    tx_count: usize,
}

#[derive(serde::Serialize)]
struct TransactionOutputLine<'a> {
    phase: &'static str,
    id: String,
    raw: String,
    sender: &'a Address,
    submission_keys: [&'a Address; 1],
    inclusion_keys: [Address; 0],
}

/// One line of a replay corpus, as read by `bench call`.
#[derive(serde::Serialize)]
struct CorpusOutputLine<'a> {
    method: &'a str,
    params: Vec<serde_json::Value>,
    meta: RecordMeta,
}

/// The opaque per-record metadata carried through to the replay outputs.
#[derive(serde::Serialize)]
struct RecordMeta {
    block: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hash: Option<B256>,
    /// Names the variant this record replays, so `bench call` reports it under
    /// `method:label` instead of pooling it with the method's other variants.
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<String>,
}

/// A call object built from a source transaction.
type CallObject = serde_json::Map<String, serde_json::Value>;

/// The tracing options object of a `debug_trace*` record.
type TraceOptions = serde_json::Map<String, serde_json::Value>;

/// Everything that decides which records a fetched block contributes.
#[derive(Debug, Default)]
struct CorpusConfig {
    /// Methods to emit, in the format's own order.
    methods: Vec<CallMethod>,
    /// Keep only this many transactions per block, by descending gas limit.
    top_gas: Option<usize>,
    /// One entry per `--tracer`, in the order given.
    tracers: Vec<TracerSpec>,
    /// `--tracer-config`, carried by every named-tracer record.
    tracer_config: Option<serde_json::Value>,
    /// `--trace-options`, merged into every tracing options object.
    options: TraceOptions,
}

impl CorpusConfig {
    /// The tracing options object one spec contributes.
    fn trace_options(&self, spec: &TracerSpec) -> serde_json::Value {
        let mut options = self.options.clone();
        if let Some(tracer) = &spec.tracer {
            options.insert("tracer".into(), serde_json::Value::String(tracer.clone()));
            if let Some(config) = &self.tracer_config {
                options.insert("tracerConfig".into(), config.clone());
            }
        }
        serde_json::Value::Object(options)
    }
}

/// One `--tracer` spec: what a record asks the node to run, and the label it
/// is reported under.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TracerSpec {
    /// The `tracer` field, absent for the node's own struct logger.
    tracer: Option<String>,
    /// The record's `meta.label`.
    label: String,
}

impl TracerSpec {
    /// A tracer named on the command line, passed to the node verbatim.
    ///
    /// Names are not checked against a list: to a node, an unknown tracer
    /// string is JavaScript source, and which builtins exist is the node's
    /// business, not this tool's.
    fn builtin(name: &str) -> Self {
        Self { tracer: Some(name.to_string()), label: name.to_string() }
    }

    /// Parse one `--tracer` value.
    fn parse(spec: &str) -> Result<Self> {
        let resolved = match spec.strip_prefix(JS_TRACER_PREFIX) {
            Some(path) => Self::javascript(Path::new(path))?,
            None if spec == STRUCTLOG_TRACER => {
                Self { tracer: None, label: STRUCTLOG_TRACER.to_string() }
            }
            None => Self::builtin(spec),
        };

        // A label the replay would reject turns a long extraction into a
        // corpus that cannot be loaded, so it fails here instead.
        let valid = (1..=MAX_LABEL_LEN).contains(&resolved.label.len()) &&
            resolved
                .label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._+:-".contains(&byte));
        if !valid {
            bail!(
                "--tracer {spec:?} yields the label `{}`, which must match {LABEL_PATTERN}",
                resolved.label
            );
        }
        Ok(resolved)
    }

    /// A JavaScript tracer read from a file; its source is the tracer string.
    fn javascript(path: &Path) -> Result<Self> {
        let source = std::fs::read_to_string(path)
            .wrap_err_with(|| format!("failed to read JavaScript tracer {}", path.display()))?;
        if source.trim().is_empty() {
            bail!("JavaScript tracer {} is empty", path.display());
        }

        let stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| eyre::eyre!("JavaScript tracer {} has no file name", path.display()))?;
        Ok(Self { tracer: Some(source), label: format!("{JS_TRACER_PREFIX}{stem}") })
    }
}

/// One emitted record: its parameters, and the variant label it carries.
struct RecordVariant {
    params: Vec<serde_json::Value>,
    label: Option<String>,
}

async fn write_extracted_blocks<W: Write>(
    rx: &mut mpsc::Receiver<Result<FetchedBlock>>,
    writer: &mut W,
    total: u64,
    format: ExtractFormat,
    corpus: &CorpusConfig,
) -> Result<u64> {
    let start = std::time::Instant::now();
    let mut last_log = start;
    let mut count = 0u64;
    let mut item_count = 0u64;

    while let Some(result) = rx.recv().await {
        let block = result?;

        match format {
            ExtractFormat::Blocks => {
                let raw_hex = format!("0x{}", hex::encode(&block.rlp_bytes));
                let bal_hex = block.bal_rlp.as_ref().map(|bal| format!("0x{}", hex::encode(bal)));
                let key_hex = format!("{}", block.hash);
                let line = BlockOutputLine {
                    raw: &raw_hex,
                    bal: bal_hex.as_deref(),
                    key: &key_hex,
                    number: block.number,
                    timestamp: block.timestamp,
                    gas_used: block.gas_used,
                    gas_limit: block.gas_limit,
                    tx_count: block.tx_count,
                };
                serde_json::to_writer(&mut *writer, &line)?;
                writer.write_all(b"\n")?;
                item_count += 1;
            }
            ExtractFormat::Transactions => {
                for (index, transaction) in block.transactions.iter().enumerate() {
                    let line = TransactionOutputLine {
                        phase: "workload",
                        id: format!("block:{}:tx:{index}", block.number),
                        raw: format!("0x{}", hex::encode(&transaction.raw)),
                        sender: &transaction.signer,
                        submission_keys: [&transaction.signer],
                        inclusion_keys: [],
                    };
                    serde_json::to_writer(&mut *writer, &line)?;
                    writer.write_all(b"\n")?;
                    item_count += 1;
                }
            }
            ExtractFormat::Calls | ExtractFormat::Traces => {
                item_count += write_corpus_records(&mut *writer, &block, corpus)?;
            }
        }
        count += 1;

        let now = std::time::Instant::now();
        if count.is_multiple_of(1000) || now.duration_since(last_log).as_secs() >= 5 {
            let elapsed = now.duration_since(start).as_secs_f64();
            let bps = count as f64 / elapsed;
            eprintln!(
                "extracted {}/{} blocks ({:.1}%) - {:.0} blocks/s",
                count,
                total,
                count as f64 / total as f64 * 100.0,
                bps
            );
            last_log = now;
        }
    }

    writer.flush()?;
    Ok(item_count)
}

/// A transaction decoded out of a fetched block.
///
/// Only the fields the selected format writes are populated: signer recovery
/// and transaction hashing are skipped where the output does not use them, and
/// the unused fields keep their zero value.
struct FetchedTransaction {
    raw: Bytes,
    signer: Address,
    hash: B256,
    gas_limit: u64,
    call: Option<CallObject>,
}

/// Write the corpus records one block contributes.
fn write_corpus_records<W: Write>(
    writer: &mut W,
    block: &FetchedBlock,
    corpus: &CorpusConfig,
) -> Result<u64> {
    let mut written = 0u64;

    for (index, transaction) in selected_transactions(block, corpus.top_gas) {
        for method in &corpus.methods {
            for variant in transaction_params(*method, transaction, corpus) {
                serde_json::to_writer(
                    &mut *writer,
                    &CorpusOutputLine {
                        method: method.as_str(),
                        params: variant.params,
                        meta: RecordMeta {
                            block: block.number,
                            index: Some(index),
                            hash: Some(transaction.hash),
                            label: variant.label,
                        },
                    },
                )?;
                writer.write_all(b"\n")?;
                written += 1;
            }
        }
    }

    for method in &corpus.methods {
        for variant in block_params(*method, block.number, corpus) {
            serde_json::to_writer(
                &mut *writer,
                &CorpusOutputLine {
                    method: method.as_str(),
                    params: variant.params,
                    meta: RecordMeta {
                        block: block.number,
                        index: None,
                        hash: None,
                        label: variant.label,
                    },
                },
            )?;
            writer.write_all(b"\n")?;
            written += 1;
        }
    }

    Ok(written)
}

/// The transactions a block contributes, in block order, optionally limited to
/// the `top_gas` with the highest gas limit; ties keep the earlier transaction.
fn selected_transactions(
    block: &FetchedBlock,
    top_gas: Option<usize>,
) -> Vec<(usize, &FetchedTransaction)> {
    let mut selected: Vec<(usize, &FetchedTransaction)> =
        block.transactions.iter().enumerate().collect();
    if let Some(limit) = top_gas &&
        limit < selected.len()
    {
        selected.sort_by(|(index_a, a), (index_b, b)| {
            b.gas_limit.cmp(&a.gas_limit).then(index_a.cmp(index_b))
        });
        selected.truncate(limit);
        selected.sort_by_key(|(index, _)| *index);
    }
    selected
}

/// The records one transaction contributes for one method.
///
/// A method may contribute several: a `debug_trace*` method emits one record
/// per `--tracer`, and the parity-style methods emit one per set of trace
/// types. Each carries the label that keeps it a separate reporting key.
fn transaction_params(
    method: CallMethod,
    transaction: &FetchedTransaction,
    corpus: &CorpusConfig,
) -> Vec<RecordVariant> {
    let hash = serde_json::json!(transaction.hash);
    let Some(call) = transaction.call.as_ref() else {
        return match method {
            CallMethod::DebugTraceTransaction => corpus
                .tracers
                .iter()
                .map(|spec| RecordVariant {
                    params: vec![hash.clone(), corpus.trace_options(spec)],
                    label: Some(spec.label.clone()),
                })
                .collect(),
            CallMethod::TraceTransaction => {
                vec![RecordVariant { params: vec![hash], label: None }]
            }
            CallMethod::TraceReplayTransaction => vec![RecordVariant {
                params: vec![hash, serde_json::json!(PARITY_TRACE_STATE_DIFF)],
                label: Some(parity_label(&PARITY_TRACE_STATE_DIFF)),
            }],
            _ => Vec::new(),
        };
    };

    let call = serde_json::Value::Object(call.clone());
    match method {
        CallMethod::EthCall | CallMethod::EthCreateAccessList => {
            vec![RecordVariant { params: vec![call, serde_json::json!("latest")], label: None }]
        }
        CallMethod::EthEstimateGas => {
            // Estimation with a pinned gas limit answers a different question,
            // so the source transaction's limit is dropped.
            let mut call = call;
            if let Some(call) = call.as_object_mut() {
                call.remove("gas");
            }
            vec![RecordVariant { params: vec![call, serde_json::json!("latest")], label: None }]
        }
        CallMethod::DebugTraceCall => corpus
            .tracers
            .iter()
            .map(|spec| RecordVariant {
                params: vec![call.clone(), serde_json::json!("latest"), corpus.trace_options(spec)],
                label: Some(spec.label.clone()),
            })
            .collect(),
        CallMethod::TraceCall => [PARITY_TRACE.as_slice(), PARITY_TRACE_STATE_DIFF.as_slice()]
            .into_iter()
            .map(|types| RecordVariant {
                params: vec![call.clone(), serde_json::json!(types), serde_json::json!("latest")],
                label: Some(parity_label(types)),
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// The records a block-addressed method contributes, if this is one.
fn block_params(method: CallMethod, number: u64, corpus: &CorpusConfig) -> Vec<RecordVariant> {
    let number = serde_json::json!(format!("0x{number:x}"));
    match method {
        CallMethod::DebugTraceBlockByNumber => corpus
            .tracers
            .iter()
            .map(|spec| RecordVariant {
                params: vec![number.clone(), corpus.trace_options(spec)],
                label: Some(spec.label.clone()),
            })
            .collect(),
        CallMethod::TraceBlock => vec![RecordVariant { params: vec![number], label: None }],
        CallMethod::TraceReplayBlockTransactions => vec![RecordVariant {
            params: vec![number, serde_json::json!(["trace", "stateDiff"])],
            label: Some(parity_label(&["trace", "stateDiff"])),
        }],
        _ => Vec::new(),
    }
}

/// The label a parity-style record carries: the trace types it asks for, so
/// the variants of one method stay separate reporting keys.
fn parity_label(types: &[&str]) -> String {
    types.join("+")
}

/// Build the call object a replayed transaction runs as.
///
/// Fee fields are omitted so the call runs at a zero gas price on any node,
/// and blob transactions keep only their call fields. The gas limit is pinned
/// to the source transaction's so the result does not depend on the node's
/// configured gas cap.
fn call_object<T: Transaction>(transaction: &T, signer: Address) -> CallObject {
    let mut call = CallObject::new();
    call.insert("from".into(), serde_json::json!(signer));
    if let Some(to) = transaction.to() {
        call.insert("to".into(), serde_json::json!(to));
    }
    call.insert("gas".into(), serde_json::json!(format!("0x{:x}", transaction.gas_limit())));
    call.insert("value".into(), serde_json::json!(transaction.value()));
    call.insert("input".into(), serde_json::json!(transaction.input()));

    if let Some(access_list) = transaction.access_list().filter(|list| !list.is_empty()) {
        call.insert("accessList".into(), serde_json::json!(access_list));
    }
    if let Some(authorizations) = transaction.authorization_list().filter(|list| !list.is_empty()) {
        call.insert("authorizationList".into(), serde_json::json!(authorizations));
    }

    call
}

struct FetchedBlock {
    rlp_bytes: Bytes,
    bal_rlp: Option<Bytes>,
    hash: alloy_primitives::B256,
    number: u64,
    timestamp: u64,
    gas_used: u64,
    gas_limit: u64,
    tx_count: usize,
    transactions: Vec<FetchedTransaction>,
}

async fn fetch_blocks<N, P>(
    provider: P,
    from: u64,
    to: u64,
    include_bal: bool,
    buffer_size: usize,
    format: ExtractFormat,
    tx: mpsc::Sender<Result<FetchedBlock>>,
) where
    N: Network,
    N::TxEnvelope: Decodable + Encodable2718 + SignerRecoverable + Transaction + 'static,
    N::Header: Decodable + 'static,
    P: Provider<N> + DebugApi<N> + Clone + 'static,
{
    let buffer_size = buffer_size.max(1);
    let mut block_stream = stream::iter(from..=to)
        .map(move |block_num| {
            let provider = provider.clone();
            async move {
                let rlp_bytes: Bytes = provider
                    .debug_get_raw_block(BlockNumberOrTag::Number(block_num).into())
                    .await
                    .wrap_err_with(|| format!("failed to fetch raw block {block_num}"))?;

                let bal_rlp = if include_bal {
                    Some(fetch_encoded_block_access_list(&provider, block_num).await?)
                } else {
                    None
                };

                let sealed: Sealed<alloy_consensus::Block<N::TxEnvelope, N::Header>> =
                    alloy_consensus::Block::decode_sealed(&mut rlp_bytes.as_ref())
                        .map_err(|e| eyre::eyre!("failed to decode block {block_num}: {e}"))?;

                let hash = sealed.hash();
                let block = sealed.inner();
                let transactions = match format {
                    ExtractFormat::Blocks => Vec::new(),
                    _ => block
                        .body
                        .transactions
                        .iter()
                        .enumerate()
                        .map(|(index, transaction)| {
                            let signer = if matches!(
                                format,
                                ExtractFormat::Transactions | ExtractFormat::Calls
                            ) {
                                transaction.recover_signer_unchecked().map_err(|error| {
                                    eyre::eyre!(
                                        "failed to recover signer for transaction {index} in block {block_num}: {error}"
                                    )
                                })?
                            } else {
                                Address::ZERO
                            };

                            Ok(FetchedTransaction {
                                raw: match format {
                                    ExtractFormat::Transactions => transaction.encoded_2718().into(),
                                    _ => Bytes::new(),
                                },
                                signer,
                                hash: match format {
                                    ExtractFormat::Calls | ExtractFormat::Traces => {
                                        transaction.trie_hash()
                                    }
                                    _ => B256::ZERO,
                                },
                                gas_limit: transaction.gas_limit(),
                                call: matches!(format, ExtractFormat::Calls)
                                    .then(|| call_object(transaction, signer)),
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                };

                Ok(FetchedBlock {
                    rlp_bytes,
                    bal_rlp,
                    hash,
                    number: block.header.number(),
                    timestamp: block.header.timestamp(),
                    gas_used: block.header.gas_used(),
                    gas_limit: block.header.gas_limit(),
                    tx_count: block.body.transactions.len(),
                    transactions,
                })
            }
        })
        .buffered(buffer_size);

    while let Some(result) = block_stream.next().await {
        let is_err = result.is_err();
        if tx.send(result).await.is_err() {
            break;
        }
        if is_err {
            break;
        }
    }
}

async fn write_big_blocks<N, P, W>(
    provider: &P,
    writer: &mut W,
    args: &ExtractBigBlocksArgs,
) -> Result<()>
where
    N: Network,
    N::TxEnvelope: Decodable + Encodable2718 + Transaction + 'static,
    N::Header: Decodable + BlockHeader + Sealable + 'static,
    P: Provider<N> + DebugApi<N> + Clone + 'static,
    W: Write,
{
    let start = std::time::Instant::now();
    let mut emitted = 0_u64;
    let mut accumulated_block_hashes = Vec::new();
    let mut first_source_block = None;

    // Buffered prefetch stream: keeps `buffer_size` block fetches in flight concurrently.
    let buffer_size = args.buffer_size.max(1);
    let include_bal = args.bal;
    let provider = provider.clone();
    let mut block_stream = stream::iter(args.from..)
        .map(move |block_num| {
            let provider = provider.clone();
            async move { fetch_execution_data::<N, _>(&provider, block_num, include_bal).await }
        })
        .buffered(buffer_size);

    while emitted < args.count {
        let mut blocks = Vec::new();
        let mut block_access_lists = Vec::new();
        let mut accumulated_gas = 0_u64;

        while accumulated_gas < args.target_gas {
            let fetched = block_stream
                .next()
                .await
                .ok_or_else(|| eyre::eyre!("block stream exhausted unexpectedly"))??;
            first_source_block.get_or_insert(fetched.execution_data.block_number());
            accumulated_gas =
                accumulated_gas.saturating_add(fetched.execution_data.payload.as_v1().gas_used);
            blocks.push(fetched.execution_data);
            block_access_lists.push(fetched.block_access_list);
        }

        let merged_block_access_list = merge_block_access_lists(&blocks, block_access_lists);
        let big_block = build_big_block(
            blocks,
            emitted,
            first_source_block.unwrap_or(args.from),
            accumulated_block_hashes.clone(),
            merged_block_access_list,
        )?;

        for switch_data in &big_block.env_switches {
            accumulated_block_hashes.push((switch_data.block_number(), switch_data.block_hash()));
        }
        if accumulated_block_hashes.len() > 256 {
            let excess = accumulated_block_hashes.len() - 256;
            accumulated_block_hashes.drain(..excess);
        }

        serde_json::to_writer(&mut *writer, &big_block)?;
        writer.write_all(b"\n")?;
        emitted += 1;

        let elapsed = start.elapsed().as_secs_f64();
        let rate = if elapsed > 0.0 { emitted as f64 / elapsed } else { 0.0 };
        eprintln!(
            "generated {}/{} big blocks ({:.1}%) - {:.2} big blocks/s",
            emitted,
            args.count,
            emitted as f64 / args.count as f64 * 100.0,
            rate,
        );
    }

    writer.flush()?;
    Ok(())
}

struct FetchedExecutionData {
    execution_data: ExecutionData,
    block_access_list: Option<BlockAccessList>,
}

async fn fetch_execution_data<N, P>(
    provider: &P,
    block_num: u64,
    include_bal: bool,
) -> Result<FetchedExecutionData>
where
    N: Network,
    N::TxEnvelope: Decodable + Encodable2718 + Transaction,
    N::Header: Decodable + BlockHeader + Sealable,
    P: Provider<N> + DebugApi<N>,
{
    let rlp_bytes: Bytes = provider
        .debug_get_raw_block(BlockNumberOrTag::Number(block_num).into())
        .await
        .wrap_err_with(|| format!("failed to fetch raw block {block_num}"))?;

    let block_access_list =
        if include_bal { Some(fetch_block_access_list(provider, block_num).await?) } else { None };

    let sealed: Sealed<alloy_consensus::Block<N::TxEnvelope, N::Header>> =
        alloy_consensus::Block::decode_sealed(&mut rlp_bytes.as_ref())
            .map_err(|e| eyre::eyre!("failed to decode block {block_num}: {e}"))?;
    let (block, _) = sealed.split();
    let (payload, sidecar) = ExecutionPayload::from_block_slow(&block);
    Ok(FetchedExecutionData {
        execution_data: ExecutionData { payload, sidecar },
        block_access_list,
    })
}

fn retrying_http_provider<N>(rpc: &str) -> Result<RootProvider<N>>
where
    N: Network,
{
    let retry_layer = RetryBackoffLayer::new(
        RAW_BLOCK_FETCH_ATTEMPTS,
        RAW_BLOCK_FETCH_INITIAL_BACKOFF_MS,
        u64::MAX,
    );
    let client =
        RpcClient::builder().layer(retry_layer).http(rpc.parse().wrap_err("invalid RPC URL")?);
    Ok(RootProvider::<N>::new(client))
}

fn build_big_block(
    blocks: Vec<ExecutionData>,
    big_block_idx: u64,
    first_source_block: u64,
    prior_block_hashes: Vec<(u64, B256)>,
    merged_block_access_list: Option<Bytes>,
) -> Result<BigBlockData<ExecutionData>> {
    if blocks.is_empty() {
        bail!("cannot build a big block with no source blocks");
    }

    Ok(BigBlockData {
        env_switches: blocks,
        prior_block_hashes,
        block_number: first_source_block + big_block_idx,
        merged_block_access_list,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{SignableTransaction, TxEip1559, TxEip4844, TxEip7702, TxLegacy};
    use alloy_eips::{
        eip2930::{AccessList, AccessListItem},
        eip7702::{Authorization, SignedAuthorization},
    };
    use alloy_primitives::{Signature, TxKind, U256};
    use serde_json::Value;

    fn signature() -> Signature {
        Signature::new(U256::from(1), U256::from(2), false)
    }

    fn legacy_create() -> alloy_consensus::TxEnvelope {
        TxLegacy {
            chain_id: Some(1),
            nonce: 7,
            gas_price: 1_000,
            gas_limit: 100_000,
            to: TxKind::Create,
            value: U256::from(5),
            input: Bytes::from_static(&[0x60, 0x01]),
        }
        .into_signed(signature())
        .into()
    }

    fn eip1559_with_access_list() -> alloy_consensus::TxEnvelope {
        TxEip1559 {
            chain_id: 1,
            nonce: 1,
            gas_limit: 21_000,
            max_fee_per_gas: 2_000,
            max_priority_fee_per_gas: 1_000,
            to: TxKind::Call(Address::repeat_byte(0xaa)),
            value: U256::from(1),
            access_list: AccessList(vec![AccessListItem {
                address: Address::repeat_byte(0xbb),
                storage_keys: vec![B256::repeat_byte(0x01)],
            }]),
            input: Bytes::new(),
        }
        .into_signed(signature())
        .into()
    }

    fn blob_transaction() -> alloy_consensus::TxEnvelope {
        let variant: alloy_consensus::TxEip4844Variant =
            alloy_consensus::TxEip4844Variant::TxEip4844(TxEip4844 {
                chain_id: 1,
                nonce: 2,
                gas_limit: 50_000,
                max_fee_per_gas: 2_000,
                max_priority_fee_per_gas: 1_000,
                to: Address::repeat_byte(0xcc),
                value: U256::from(3),
                access_list: AccessList::default(),
                blob_versioned_hashes: vec![B256::repeat_byte(0x02)],
                max_fee_per_blob_gas: 9_999,
                input: Bytes::from_static(&[0xde, 0xad]),
            });
        variant.into_signed(signature()).into()
    }

    fn eip7702_transaction() -> alloy_consensus::TxEnvelope {
        TxEip7702 {
            chain_id: 1,
            nonce: 3,
            gas_limit: 30_000,
            max_fee_per_gas: 2_000,
            max_priority_fee_per_gas: 1_000,
            to: Address::repeat_byte(0xdd),
            value: U256::ZERO,
            access_list: AccessList::default(),
            authorization_list: vec![SignedAuthorization::new_unchecked(
                Authorization {
                    chain_id: U256::from(1),
                    address: Address::repeat_byte(0xee),
                    nonce: 0,
                },
                0,
                U256::from(1),
                U256::from(2),
            )],
            input: Bytes::new(),
        }
        .into_signed(signature())
        .into()
    }

    fn fetched_block() -> FetchedBlock {
        FetchedBlock {
            rlp_bytes: Bytes::from_static(&[0xaa, 0xbb]),
            bal_rlp: None,
            hash: B256::repeat_byte(0x11),
            number: 42,
            timestamp: 1_700_000_000,
            gas_used: 21_000,
            gas_limit: 30_000_000,
            tx_count: 1,
            transactions: vec![FetchedTransaction {
                raw: Bytes::from_static(&[0x02, 0xca, 0xfe]),
                signer: Address::repeat_byte(0x22),
                hash: B256::ZERO,
                gas_limit: 21_000,
                call: None,
            }],
        }
    }

    fn corpus_block(format: ExtractFormat) -> FetchedBlock {
        let transaction = eip1559_with_access_list();
        let signer = Address::repeat_byte(0x22);
        let mut block = fetched_block();
        block.transactions = vec![FetchedTransaction {
            raw: Bytes::new(),
            signer,
            hash: B256::repeat_byte(0x33),
            gas_limit: 21_000,
            call: matches!(format, ExtractFormat::Calls).then(|| call_object(&transaction, signer)),
        }];
        block
    }

    fn traces_block_with_gas_limits(gas_limits: &[u64]) -> FetchedBlock {
        let mut block = fetched_block();
        block.transactions = gas_limits
            .iter()
            .enumerate()
            .map(|(index, gas_limit)| FetchedTransaction {
                raw: Bytes::new(),
                signer: Address::ZERO,
                hash: B256::repeat_byte(0x40 + index as u8),
                gas_limit: *gas_limit,
                call: None,
            })
            .collect();
        block.tx_count = gas_limits.len();
        block
    }

    /// A default corpus configuration emitting `methods` with `callTracer`.
    fn corpus_config(methods: &[CallMethod]) -> CorpusConfig {
        CorpusConfig {
            methods: methods.to_vec(),
            tracers: vec![TracerSpec::builtin(DEFAULT_TRACER)],
            ..Default::default()
        }
    }

    async fn write_block(
        block: FetchedBlock,
        format: ExtractFormat,
        methods: &[CallMethod],
    ) -> Vec<Value> {
        write_block_with(block, format, corpus_config(methods)).await
    }

    async fn write_block_with(
        block: FetchedBlock,
        format: ExtractFormat,
        corpus: CorpusConfig,
    ) -> Vec<Value> {
        let (sender, mut receiver) = mpsc::channel(1);
        sender.send(Ok(block)).await.unwrap();
        drop(sender);

        let mut output = Vec::new();
        write_extracted_blocks(&mut receiver, &mut output, 1, format, &corpus).await.unwrap();
        String::from_utf8(output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    /// Build an `ExtractArgs` carrying only the flags a test exercises.
    fn extract_args(format: ExtractFormat) -> ExtractArgs {
        ExtractArgs {
            rpc: "http://localhost:8545".to_string(),
            from: 1,
            to: 1,
            output: None,
            buffer_size: 1,
            bal: false,
            format,
            methods: Vec::new(),
            top_gas: None,
            tracer: Vec::new(),
            tracer_config: None,
            trace_options: None,
        }
    }

    #[tokio::test]
    async fn top_gas_keeps_the_heaviest_transactions_in_block_order() {
        let block = traces_block_with_gas_limits(&[21_000, 500_000, 100_000, 500_000]);
        let corpus = CorpusConfig {
            top_gas: Some(2),
            ..corpus_config(&[CallMethod::TraceTransaction, CallMethod::TraceBlock])
        };
        let lines = write_block_with(block, ExtractFormat::Traces, corpus).await;

        // The two 500k transactions win; the tie between them keeps the earlier one first,
        // and the block-level record is still emitted.
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0]["meta"]["index"], 1);
        assert_eq!(lines[0]["params"][0], format!("{}", B256::repeat_byte(0x41)));
        assert_eq!(lines[1]["meta"]["index"], 3);
        assert_eq!(lines[1]["params"][0], format!("{}", B256::repeat_byte(0x43)));
        assert_eq!(lines[2]["method"], "trace_block");
    }

    #[tokio::test]
    async fn top_gas_larger_than_the_block_keeps_every_transaction() {
        let block = traces_block_with_gas_limits(&[21_000, 100_000]);
        let corpus =
            CorpusConfig { top_gas: Some(5), ..corpus_config(&[CallMethod::TraceTransaction]) };
        let lines = write_block_with(block, ExtractFormat::Traces, corpus).await;

        assert_eq!(
            lines.iter().map(|line| line["meta"]["index"].as_u64().unwrap()).collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    #[test]
    fn top_gas_is_validated_against_the_format() {
        assert_eq!(resolve_top_gas(ExtractFormat::Traces, Some(3)).unwrap(), Some(3));
        assert_eq!(resolve_top_gas(ExtractFormat::Blocks, None).unwrap(), None);
        assert!(resolve_top_gas(ExtractFormat::Blocks, Some(3)).is_err());
        assert!(resolve_top_gas(ExtractFormat::Transactions, Some(3)).is_err());
        assert!(resolve_top_gas(ExtractFormat::Calls, Some(0)).is_err());
    }

    #[tokio::test]
    async fn writes_transaction_output_for_bench_send() {
        let lines = write_block(fetched_block(), ExtractFormat::Transactions, &[]).await;

        assert_eq!(lines.len(), 1);
        let line = &lines[0];
        assert_eq!(line["phase"], "workload");
        assert_eq!(line["id"], "block:42:tx:0");
        assert_eq!(line["raw"], "0x02cafe");
        assert_eq!(line["sender"], "0x2222222222222222222222222222222222222222");
        assert_eq!(line["submission_keys"][0], "0x2222222222222222222222222222222222222222");
        assert_eq!(line["inclusion_keys"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn block_output_keeps_original_transaction_count() {
        let lines = write_block(fetched_block(), ExtractFormat::Blocks, &[]).await;

        assert_eq!(lines[0]["tx_count"], 1);
        assert_eq!(lines[0]["raw"], "0xaabb");
    }

    #[test]
    fn call_object_carries_no_fee_fields() {
        let call = call_object(&eip1559_with_access_list(), Address::repeat_byte(0x22));

        assert_eq!(call["from"], "0x2222222222222222222222222222222222222222");
        assert_eq!(call["to"], "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(call["gas"], "0x5208");
        assert_eq!(call["value"], "0x1");
        assert_eq!(call["input"], "0x");
        for field in ["gasPrice", "maxFeePerGas", "maxPriorityFeePerGas", "maxFeePerBlobGas"] {
            assert!(!call.contains_key(field), "{field} should be omitted");
        }
    }

    #[test]
    fn call_object_keeps_a_non_empty_access_list() {
        let call = call_object(&eip1559_with_access_list(), Address::ZERO);
        assert_eq!(call["accessList"][0]["address"], "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        assert!(!call.contains_key("authorizationList"));

        let empty = call_object(&blob_transaction(), Address::ZERO);
        assert!(!empty.contains_key("accessList"));
    }

    #[test]
    fn call_object_keeps_an_authorization_list() {
        let call = call_object(&eip7702_transaction(), Address::ZERO);
        assert_eq!(
            call["authorizationList"][0]["address"],
            "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
        );
    }

    #[test]
    fn call_object_drops_blob_fields() {
        let call = call_object(&blob_transaction(), Address::ZERO);

        assert_eq!(call["to"], "0xcccccccccccccccccccccccccccccccccccccccc");
        assert_eq!(call["input"], "0xdead");
        assert!(!call.contains_key("blobVersionedHashes"));
        assert!(!call.contains_key("maxFeePerBlobGas"));
    }

    #[test]
    fn call_object_omits_to_for_contract_creation() {
        let call = call_object(&legacy_create(), Address::ZERO);
        assert!(!call.contains_key("to"));
        assert_eq!(call["gas"], "0x186a0");
    }

    #[tokio::test]
    async fn calls_format_emits_the_documented_parameter_layouts() {
        let methods = ExtractFormat::Calls.available_methods();
        let lines =
            write_block(corpus_block(ExtractFormat::Calls), ExtractFormat::Calls, methods).await;

        let emitted = lines.iter().map(|line| line["method"].as_str().unwrap()).collect::<Vec<_>>();
        assert_eq!(
            emitted,
            vec![
                "eth_call",
                "eth_estimateGas",
                "eth_createAccessList",
                "debug_traceCall",
                "trace_call",
                "trace_call",
            ]
        );

        assert_eq!(lines[0]["params"][1], "latest");
        assert_eq!(lines[0]["params"][0]["gas"], "0x5208");
        assert!(lines[1]["params"][0].get("gas").is_none(), "eth_estimateGas keeps no gas limit");
        assert_eq!(lines[2]["params"][1], "latest");
        assert_eq!(lines[3]["params"][2], serde_json::json!({"tracer": "callTracer"}));
        assert_eq!(lines[3]["meta"]["label"], "callTracer");
        assert_eq!(lines[4]["params"][1], serde_json::json!(["trace"]));
        assert_eq!(lines[4]["params"][2], "latest");
        assert_eq!(lines[4]["meta"]["label"], "trace");
        assert_eq!(lines[5]["params"][1], serde_json::json!(["trace", "stateDiff"]));
        assert_eq!(lines[5]["meta"]["label"], "trace+stateDiff");
        // The plain call methods have no variants, so they carry no label.
        assert!(lines[0]["meta"].get("label").is_none());

        for line in &lines {
            assert_eq!(line["meta"]["block"], 42);
            assert_eq!(line["meta"]["index"], 0);
            assert_eq!(
                line["meta"]["hash"],
                "0x3333333333333333333333333333333333333333333333333333333333333333"
            );
        }
    }

    #[tokio::test]
    async fn traces_format_emits_the_documented_parameter_layouts() {
        let methods = ExtractFormat::Traces.available_methods();
        let lines =
            write_block(corpus_block(ExtractFormat::Traces), ExtractFormat::Traces, methods).await;

        let emitted = lines.iter().map(|line| line["method"].as_str().unwrap()).collect::<Vec<_>>();
        assert_eq!(
            emitted,
            vec![
                "debug_traceTransaction",
                "trace_transaction",
                "trace_replayTransaction",
                "debug_traceBlockByNumber",
                "trace_block",
                "trace_replayBlockTransactions",
            ]
        );
        let replay_block =
            lines.iter().find(|line| line["method"] == "trace_replayBlockTransactions").unwrap();
        assert_eq!(replay_block["params"], serde_json::json!(["0x2a", ["trace", "stateDiff"]]));
        assert_eq!(replay_block["meta"]["label"], "trace+stateDiff");

        let hash = "0x3333333333333333333333333333333333333333333333333333333333333333";
        assert_eq!(lines[0]["params"], serde_json::json!([hash, {"tracer": "callTracer"}]));
        assert_eq!(lines[0]["meta"]["label"], "callTracer");
        assert_eq!(lines[1]["params"], serde_json::json!([hash]));
        assert!(lines[1]["meta"].get("label").is_none());
        assert_eq!(lines[2]["params"], serde_json::json!([hash, ["trace", "stateDiff"]]));
        assert_eq!(lines[2]["meta"]["label"], "trace+stateDiff");
        assert_eq!(lines[3]["params"], serde_json::json!(["0x2a", {"tracer": "callTracer"}]));

        // Block records address a block, not a transaction.
        assert_eq!(lines[4]["params"], serde_json::json!(["0x2a"]));
        assert_eq!(lines[4]["meta"], serde_json::json!({"block": 42}));
    }

    #[tokio::test]
    async fn the_default_tracer_emits_one_call_tracer_record_per_transaction() {
        let corpus = resolve_corpus_config(&extract_args(ExtractFormat::Traces)).unwrap();
        assert_eq!(corpus.tracers, vec![TracerSpec::builtin("callTracer")]);

        let mut with_debug = corpus;
        with_debug.methods = vec![CallMethod::DebugTraceTransaction];
        let lines = write_block_with(
            corpus_block(ExtractFormat::Traces),
            ExtractFormat::Traces,
            with_debug,
        )
        .await;

        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["params"][1], serde_json::json!({"tracer": "callTracer"}));
        assert_eq!(lines[0]["meta"]["label"], "callTracer");
    }

    #[tokio::test]
    async fn a_named_tracer_is_passed_through_verbatim() {
        let corpus = CorpusConfig {
            methods: vec![CallMethod::DebugTraceTransaction],
            tracers: resolve_tracers(ExtractFormat::Traces, &["flatCallTracer".to_string()])
                .unwrap(),
            ..Default::default()
        };
        let lines =
            write_block_with(corpus_block(ExtractFormat::Traces), ExtractFormat::Traces, corpus)
                .await;

        let hash = "0x3333333333333333333333333333333333333333333333333333333333333333";
        assert_eq!(lines[0]["params"], serde_json::json!([hash, {"tracer": "flatCallTracer"}]));
        assert_eq!(lines[0]["meta"]["label"], "flatCallTracer");
    }

    #[tokio::test]
    async fn structlog_leaves_the_tracer_field_out() {
        let corpus = CorpusConfig {
            methods: vec![CallMethod::DebugTraceTransaction, CallMethod::DebugTraceBlockByNumber],
            tracers: resolve_tracers(ExtractFormat::Traces, &["structlog".to_string()]).unwrap(),
            ..Default::default()
        };
        let lines =
            write_block_with(corpus_block(ExtractFormat::Traces), ExtractFormat::Traces, corpus)
                .await;

        let hash = "0x3333333333333333333333333333333333333333333333333333333333333333";
        assert_eq!(lines[0]["params"], serde_json::json!([hash, {}]));
        assert_eq!(lines[0]["meta"]["label"], "structlog");
        assert_eq!(lines[1]["params"], serde_json::json!(["0x2a", {}]));
        assert_eq!(lines[1]["meta"]["label"], "structlog");
    }

    #[tokio::test]
    async fn a_javascript_tracer_sends_the_file_contents() {
        let directory = tempfile::tempdir().unwrap();
        let script = directory.path().join("bigmap.js");
        let source = "{result: function() { return 1 }, fault: function() {}}";
        std::fs::write(&script, source).unwrap();

        let corpus = CorpusConfig {
            methods: vec![CallMethod::DebugTraceCall],
            tracers: resolve_tracers(ExtractFormat::Calls, &[format!("js:{}", script.display())])
                .unwrap(),
            ..Default::default()
        };
        let lines =
            write_block_with(corpus_block(ExtractFormat::Calls), ExtractFormat::Calls, corpus)
                .await;

        assert_eq!(lines[0]["params"][1], "latest");
        assert_eq!(lines[0]["params"][2], serde_json::json!({"tracer": source}));
        assert_eq!(lines[0]["meta"]["label"], "js:bigmap");
    }

    #[tokio::test]
    async fn tracer_config_and_trace_options_are_merged_into_every_debug_record() {
        let mut args = extract_args(ExtractFormat::Traces);
        args.tracer = vec!["prestateTracer".to_string()];
        args.tracer_config = Some(r#"{"diffMode":true}"#.to_string());
        args.trace_options = Some(r#"{"timeout":"60s","disableStorage":true}"#.to_string());

        let mut corpus = resolve_corpus_config(&args).unwrap();
        corpus.methods =
            vec![CallMethod::DebugTraceTransaction, CallMethod::DebugTraceBlockByNumber];
        let lines =
            write_block_with(corpus_block(ExtractFormat::Traces), ExtractFormat::Traces, corpus)
                .await;

        let options = serde_json::json!({
            "tracer": "prestateTracer",
            "tracerConfig": {"diffMode": true},
            "timeout": "60s",
            "disableStorage": true,
        });
        assert_eq!(lines[0]["params"][1], options);
        assert_eq!(lines[1]["params"][1], options);
        assert_eq!(lines[0]["meta"]["label"], "prestateTracer");
    }

    #[tokio::test]
    async fn trace_options_alone_configure_the_struct_logger() {
        let mut args = extract_args(ExtractFormat::Traces);
        args.tracer = vec!["structlog".to_string()];
        args.trace_options = Some(r#"{"disableStack":true,"enableMemory":false}"#.to_string());

        let mut corpus = resolve_corpus_config(&args).unwrap();
        corpus.methods = vec![CallMethod::DebugTraceTransaction];
        let lines =
            write_block_with(corpus_block(ExtractFormat::Traces), ExtractFormat::Traces, corpus)
                .await;

        assert_eq!(
            lines[0]["params"][1],
            serde_json::json!({"disableStack": true, "enableMemory": false})
        );
    }

    #[tokio::test]
    async fn every_tracer_spec_emits_its_own_record() {
        let mut args = extract_args(ExtractFormat::Traces);
        args.tracer = vec!["callTracer".to_string(), "structlog".to_string()];

        let mut corpus = resolve_corpus_config(&args).unwrap();
        corpus.methods = vec![CallMethod::DebugTraceTransaction, CallMethod::TraceTransaction];
        let lines =
            write_block_with(corpus_block(ExtractFormat::Traces), ExtractFormat::Traces, corpus)
                .await;

        let labels = lines
            .iter()
            .map(|line| (line["method"].as_str().unwrap(), line["meta"]["label"].as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            labels,
            vec![
                ("debug_traceTransaction", Some("callTracer")),
                ("debug_traceTransaction", Some("structlog")),
                ("trace_transaction", None),
            ]
        );
    }

    #[test]
    fn tracer_flags_are_rejected_for_the_non_corpus_formats() {
        for format in [ExtractFormat::Blocks, ExtractFormat::Transactions] {
            let mut tracer = extract_args(format);
            tracer.tracer = vec!["callTracer".to_string()];
            let mut config = extract_args(format);
            config.tracer_config = Some("{}".to_string());
            let mut options = extract_args(format);
            options.trace_options = Some("{}".to_string());

            for (args, flag) in
                [(tracer, "--tracer"), (config, "--tracer-config"), (options, "--trace-options")]
            {
                let error = resolve_corpus_config(&args).unwrap_err().to_string();
                assert!(error.contains(flag), "{format:?}: {error}");
            }
        }
    }

    #[test]
    fn tracer_config_is_rejected_for_the_struct_logger() {
        let mut args = extract_args(ExtractFormat::Traces);
        args.tracer = vec!["callTracer".to_string(), "structlog".to_string()];
        args.tracer_config = Some(r#"{"diffMode":true}"#.to_string());

        let error = resolve_corpus_config(&args).unwrap_err().to_string();
        assert!(error.contains("--tracer-config"), "{error}");
        assert!(error.contains("structlog"), "{error}");
    }

    #[test]
    fn the_json_flags_must_be_objects() {
        for value in ["[1,2]", "\"callTracer\"", "null", "{"] {
            let mut config = extract_args(ExtractFormat::Traces);
            config.tracer_config = Some(value.to_string());
            assert!(resolve_corpus_config(&config).is_err(), "--tracer-config {value}");

            let mut options = extract_args(ExtractFormat::Traces);
            options.trace_options = Some(value.to_string());
            assert!(resolve_corpus_config(&options).is_err(), "--trace-options {value}");
        }
    }

    #[test]
    fn trace_options_may_not_carry_the_keys_that_have_their_own_flag() {
        for (value, flag) in [
            (r#"{"tracer":"callTracer"}"#, "--tracer"),
            (r#"{"tracerConfig":{"diffMode":true}}"#, "--tracer-config"),
        ] {
            let mut args = extract_args(ExtractFormat::Traces);
            args.trace_options = Some(value.to_string());

            let error = resolve_corpus_config(&args).unwrap_err().to_string();
            assert!(error.contains(flag), "{value}: {error}");
        }
    }

    #[test]
    fn a_javascript_tracer_must_name_a_non_empty_file() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("absent.js");
        let empty = directory.path().join("empty.js");
        std::fs::write(&empty, "   \n").unwrap();
        // A file name the reporting key could not carry.
        let unlabellable = directory.path().join("my tracer.js");
        std::fs::write(&unlabellable, "{}").unwrap();

        for (path, expected) in
            [(missing, "failed to read"), (empty, "is empty"), (unlabellable, LABEL_PATTERN)]
        {
            let error = TracerSpec::parse(&format!("js:{}", path.display())).unwrap_err();
            assert!(format!("{error:#}").contains(expected), "{path:?}: {error:#}");
        }
    }

    #[test]
    fn resolve_methods_defaults_per_format() {
        assert_eq!(resolve_methods(ExtractFormat::Calls, &[]).unwrap(), vec![CallMethod::EthCall]);
        assert_eq!(
            resolve_methods(ExtractFormat::Traces, &[]).unwrap(),
            vec![CallMethod::DebugTraceTransaction, CallMethod::TraceTransaction]
        );
        assert!(resolve_methods(ExtractFormat::Blocks, &[]).unwrap().is_empty());
    }

    #[test]
    fn resolve_methods_normalizes_order_and_duplicates() {
        let requested =
            ["trace_call".to_string(), "eth_call".to_string(), "trace_call".to_string()];
        assert_eq!(
            resolve_methods(ExtractFormat::Calls, &requested).unwrap(),
            vec![CallMethod::EthCall, CallMethod::TraceCall]
        );
    }

    #[test]
    fn resolve_methods_rejects_methods_a_format_cannot_emit() {
        let error = resolve_methods(ExtractFormat::Calls, &["trace_block".to_string()])
            .unwrap_err()
            .to_string();
        assert!(error.contains("trace_block"), "{error}");
        assert!(error.contains("eth_call"), "{error}");

        assert!(resolve_methods(ExtractFormat::Blocks, &["eth_call".to_string()]).is_err());
    }
}
