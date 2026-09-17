//! CLI binary for the bench tool.
//!
//! Provides subcommands:
//! - `send` - Send from file/stdin
//! - `send-blocks` - Submit blocks via reth Engine API
//! - `call` - Replay an RPC corpus against a node
//! - `view` - Print an existing JSON report to console

use clap::{Args, Parser, Subcommand, ValueEnum};
use eyre::{bail, Context, Result};
use std::{collections::HashSet, num::NonZeroUsize, path::PathBuf, time::Duration};

use crate::{
    metrics_url::{parse_metrics_url, MetricsURL},
    wait_for_persistence::WaitForPersistence,
};

mod call;
mod metrics_forwarder;
mod metrics_url;
mod preparation;
mod send;
mod send_blocks;
mod view;
mod wait_for_persistence;

/// Arguments for the `send` subcommand.
#[derive(Args)]
pub struct SendArgs {
    /// Input file (NDJSON). If not specified, reads from stdin.
    #[arg(short, long)]
    pub input: Option<PathBuf>,

    /// Workload specification used to sign deferred transaction envelopes.
    ///
    /// Required only when the input contains `late_sign` records. Currently
    /// supports standard and sponsored Tempo deferred-signing envelopes.
    #[arg(long, value_name = "PATH")]
    pub late_signing_spec: Option<PathBuf>,

    /// RPC endpoint URLs (comma-separated or repeated)
    #[arg(long = "rpc-url", value_delimiter = ',', default_values_t = vec!["http://localhost:8545".to_string()])]
    pub rpc_urls: Vec<String>,

    /// Optional RPC endpoint for aggregate block, block-receipt, and txpool queries.
    ///
    /// Transaction submission continues to use --rpc-url. When omitted,
    /// aggregate queries use the first --rpc-url.
    #[arg(long)]
    pub query_rpc_url: Option<String>,

    /// HTTP header populated from --sender-header-map for sender-scoped requests.
    ///
    /// Must be supplied together with --sender-header-map.
    #[arg(long)]
    pub sender_header_name: Option<String>,

    /// JSON file mapping logical transaction sender addresses to secret header values.
    ///
    /// Must be supplied together with --sender-header-name. Values are loaded
    /// from this file so they do not appear in process arguments.
    #[arg(long)]
    pub sender_header_map: Option<PathBuf>,

    /// Interval between checks for an atomically replaced sender-header map.
    #[arg(long, default_value = "1s", value_parser = humantime::parse_duration)]
    pub sender_header_reload_interval: Duration,

    /// Send workload for this long before collecting benchmark results.
    /// Keep the input running for warmup plus the desired measurement duration.
    #[arg(long, default_value = "0s", value_parser = humantime::parse_duration, conflicts_with = "metrics_align")]
    pub warmup: Duration,

    /// Validator endpoint JSON: gate warmup on finalized self-proposals, then
    /// drain all pools and wait for their Finish checkpoints before measurement.
    /// Requires state masking to be disabled on validators.
    #[arg(long, conflicts_with_all = ["warmup", "metrics_align"], requires = "duration")]
    pub warmup_validators: Option<PathBuf>,

    /// Maximum time for all validators to propose during warmup.
    #[arg(long, default_value = "300s", value_parser = humantime::parse_duration)]
    pub warmup_timeout: Duration,

    /// Maximum time to flush submissions, empty pools and persist warmup blocks.
    #[arg(long, default_value = "300s", value_parser = humantime::parse_duration)]
    pub cooldown_timeout: Duration,

    /// Measured workload duration, starting after preparation. Input must last
    /// through this interval; outstanding submissions are flushed afterwards.
    #[arg(long, value_parser = humantime::parse_duration)]
    pub duration: Option<Duration>,

    /// Maximum transactions submitted per second (0 = unlimited).
    ///
    /// Controls throughput via a token bucket. Provides backpressure to the
    /// transaction source before enqueueing. If max-concurrent is too low,
    /// actual throughput may be lower than this target.
    #[arg(long, default_value = "0")]
    pub tps: u64,

    /// Maximum number of RPC requests in flight simultaneously.
    ///
    /// Controls parallelism independently of --tps. Limits how many
    /// connections are open at once to avoid overwhelming the RPC endpoint.
    #[arg(long, default_value = "100")]
    pub max_concurrent: usize,

    /// Maximum submitted transactions awaiting inclusion (defaults to --tps; 0 disables).
    ///
    /// Reserves capacity before dispatch and refills it from shared block hashes,
    /// including reverted transactions or Tempo transactions past their signed expiry.
    /// RPC concurrency and --tps still apply. The query endpoint must support
    /// eth_blockNumber and eth_getBlockByNumber. Receipt dependencies also require
    /// eth_getBlockReceipts. Unresolved transactions time out after five minutes.
    #[arg(long)]
    pub max_pending: Option<u64>,

    /// Number of times to retry failed transaction submissions.
    ///
    /// Set to 0 to never retry. If omitted, retries forever.
    #[arg(long, value_name = "N")]
    pub retries: Option<u32>,

    /// Request timeout
    #[arg(long, default_value = "30s", value_parser = humantime::parse_duration)]
    pub timeout: Duration,

    /// Report output destinations
    #[arg(long = "report", value_name = "FORMAT")]
    pub reports: Vec<String>,

    /// Metadata key=value pairs to include in the report.
    ///
    /// Can be specified multiple times. Example:
    ///   --metadata build-sha=abcdef --metadata build-profile=perf
    #[arg(short = 'm', long = "metadata", value_name = "KEY=VALUE")]
    pub metadata: Vec<String>,

    /// Prometheus metrics endpoint(s) to scrape during the benchmark.
    ///
    /// Use a single URL, comma-separated `node:URL` entries, or rich
    /// `key=value;key=value@URL` entries. Labels are added to scraped samples.
    #[arg(
        long,
        value_name = "URL|NODE:URL|LABELS@URL",
        value_delimiter = ',',
        value_parser = parse_metrics_url
    )]
    pub metrics_url: Vec<MetricsURL>,

    /// File containing metric names to publish to ClickHouse, one per line.
    #[arg(long, value_name = "PATH")]
    pub clickhouse_metrics_file: Option<PathBuf>,

    /// Scrape interval in milliseconds for the metrics scraper.
    #[arg(long, default_value = "500")]
    pub scrape_interval_ms: u64,

    /// Align exported metric timestamps to this benchmark-start Unix timestamp.
    ///
    /// Accepts Unix seconds or milliseconds. Exported samples keep their
    /// original offset within the run.
    #[arg(long = "metrics-align", value_name = "TIMESTAMP", value_parser = parse_unix_timestamp_ms)]
    pub metrics_align: Option<u64>,

    /// Forward scraped samples in real time via Prometheus remote write.
    ///
    /// Uses `/api/v1/write` and the same PROMETHEUS_* environment variables
    /// as `--report prometheus:<url>`. Requires `--metrics-url`.
    #[arg(long = "metrics-forward", value_name = "URL")]
    pub metrics_forward: Option<String>,

    /// Collect and report latency metrics.
    ///
    /// Disabled by default to avoid retaining one timestamped latency sample per
    /// successful transaction. When enabled, reports aggregate latency stats
    /// and individual samples under time_series.latencies.
    #[arg(long)]
    pub collect_latencies: bool,

    /// Collect receipt-derived gas metrics for non-system transactions in the benchmark block
    /// range.
    ///
    /// Receipts are fetched in batches after the workload completes.
    #[arg(long)]
    pub collect_receipt_metrics: bool,

    /// Skip setup-phase transactions in the input stream.
    #[arg(long)]
    pub skip_setup: bool,

    /// Wait for the transaction pool to drain after sending.
    ///
    /// Polls `txpool_status` and waits until the pending count reaches zero
    /// (3 consecutive readings) before collecting block stats and finalizing.
    /// Set to 0 to disable. Keeps the metrics scraper running during the wait.
    #[arg(long, default_value = "0")]
    pub drain_timeout: u64,
}

impl SendArgs {
    fn pending_limit(&self) -> Result<Option<NonZeroUsize>> {
        let limit = usize::try_from(self.max_pending.unwrap_or(self.tps))
            .context("pending limit exceeds this platform's capacity")?;
        Ok(NonZeroUsize::new(limit))
    }
}

/// Arguments for the `send-blocks` subcommand.
#[derive(Args)]
pub struct SendBlocksArgs {
    /// Engine API endpoint
    #[arg(long)]
    pub engine: String,

    /// Path to JWT secret file
    #[arg(long)]
    pub jwt_secret: PathBuf,

    /// Input file (NDJSON). If not specified, reads from stdin.
    #[arg(short, long)]
    pub input: Option<PathBuf>,

    /// Wait for persistence policy: always, never, or every:N
    ///
    /// Controls whether reth_newPayload blocks until the persistence
    /// threshold is crossed. Default is never.
    #[arg(long, default_value = "never", value_parser = parse_wait_for_persistence)]
    pub(crate) wait_for_persistence: WaitForPersistence,

    /// Minimum interval between block submissions.
    ///
    /// Measures from before reth_newPayload until after reth_forkchoiceUpdated.
    /// If processing takes longer than this, no extra sleep is added. Bare
    /// integers are treated as milliseconds.
    #[arg(long, value_name = "WAIT_TIME", value_parser = parse_duration_millis_fallback)]
    pub wait_time: Option<Duration>,

    /// Report output destinations
    #[arg(long = "report", value_name = "FORMAT")]
    pub reports: Vec<String>,

    /// Metadata key=value pairs to include in the report.
    #[arg(short = 'm', long = "metadata", value_name = "KEY=VALUE")]
    pub metadata: Vec<String>,

    /// Prometheus metrics endpoint(s) to scrape during the benchmark.
    ///
    /// Use a single URL, comma-separated `node:URL` entries, or rich
    /// `key=value;key=value@URL` entries. Labels are added to scraped samples.
    #[arg(
        long,
        value_name = "URL|NODE:URL|LABELS@URL",
        value_delimiter = ',',
        value_parser = parse_metrics_url
    )]
    pub metrics_url: Vec<MetricsURL>,

    /// File containing metric names to publish to ClickHouse, one per line.
    #[arg(long, value_name = "PATH")]
    pub clickhouse_metrics_file: Option<PathBuf>,

    /// Scrape interval in milliseconds for the metrics scraper.
    #[arg(long, default_value = "500")]
    pub scrape_interval_ms: u64,

    /// Align exported metric timestamps to this benchmark-start Unix timestamp.
    ///
    /// Accepts Unix seconds or milliseconds. Exported samples keep their
    /// original offset within the run.
    #[arg(long = "metrics-align", value_name = "TIMESTAMP", value_parser = parse_unix_timestamp_ms)]
    pub metrics_align: Option<u64>,

    /// Forward scraped samples in real time via Prometheus remote write.
    ///
    /// Uses `/api/v1/write` and the same PROMETHEUS_* environment variables
    /// as `--report prometheus:<url>`. Requires `--metrics-url`.
    #[arg(long = "metrics-forward", value_name = "URL")]
    pub metrics_forward: Option<String>,

    /// Build a synthetic side fork and alternate forkchoice updates.
    #[arg(
        long,
        value_name = "DEPTH",
        num_args = 0..=1,
        default_missing_value = "8",
        value_parser = parse_reorg_depth,
    )]
    pub reorg: Option<usize>,

    /// Additional canonical blocks between resolved synthetic side chains.
    #[arg(long, value_name = "BLOCKS", default_value_t = 0, requires = "reorg")]
    pub reorg_gap: usize,

    /// Regular HTTP RPC URL for testing_buildBlockV1.
    #[arg(
        long = "rpc",
        alias = "rpc-url",
        alias = "local-rpc-url",
        default_value = "http://localhost:8545"
    )]
    pub rpc: String,
}

/// Arguments for the `call` subcommand.
#[derive(Args)]
pub struct CallArgs {
    /// Corpus file: NDJSON, optionally gzip-compressed.
    #[arg(short, long)]
    pub input: PathBuf,

    /// RPC endpoint URL.
    #[arg(long, default_value = "http://localhost:8545")]
    pub rpc_url: String,

    /// Replay phase.
    ///
    /// `warmup` runs only the open-loop cell and writes no per-record output,
    /// so the measured phase never replays requests the node just answered
    /// cold. `measure` runs both phases and writes every output.
    #[arg(long, value_enum, default_value_t = CallPhase::Measure)]
    pub phase: CallPhase,

    /// Open-loop target rate in requests per second (0 = skip the open loop).
    #[arg(long, default_value_t = 100)]
    pub rps: u64,

    /// Open-loop wall clock.
    #[arg(long, default_value = "120s", value_parser = humantime::parse_duration)]
    pub duration: Duration,

    /// Fixed open-loop request count, used instead of --duration.
    #[arg(long, value_name = "N")]
    pub requests: Option<u64>,

    /// Maximum open-loop requests in flight.
    ///
    /// A request that would exceed this cap is counted as dropped rather than
    /// delayed, so the offered rate stays the configured one.
    #[arg(long, default_value_t = 256, value_parser = parse_positive_usize)]
    pub max_concurrent: usize,

    /// Closed-loop passes over the whole corpus (0 = skip the closed loop).
    #[arg(long, default_value_t = 20)]
    pub passes: u64,

    /// Closed-loop worker count.
    #[arg(long, default_value_t = 16, value_parser = parse_positive_usize)]
    pub concurrency: usize,

    /// Seed for the open-loop record sequence.
    #[arg(long, default_value_t = 1)]
    pub seed: u64,

    /// Replace the block parameter of every record that has a rewritable one.
    #[arg(long, value_name = "TAG")]
    pub block_tag: Option<String>,

    /// Drop fee fields from the call object of call-shaped records.
    #[arg(long)]
    pub strip_fees: bool,

    /// Replay only these methods; other records are skipped and counted.
    #[arg(long, value_delimiter = ',', value_name = "METHOD")]
    pub methods: Vec<String>,

    /// Per-request timeout.
    #[arg(long, default_value = "30s", value_parser = humantime::parse_duration)]
    pub timeout: Duration,

    /// Write per-record response digests as NDJSON.
    #[arg(long, value_name = "PATH")]
    pub responses: Option<PathBuf>,

    /// Write closed-loop per-record timings as CSV.
    #[arg(long, value_name = "PATH")]
    pub record_csv: Option<PathBuf>,

    /// Write open-loop per-request timings as CSV.
    #[arg(long, value_name = "PATH")]
    pub requests_csv: Option<PathBuf>,

    /// Exit non-zero above this HTTP and transport failure rate.
    #[arg(long, default_value_t = 1.0, value_name = "PERCENT")]
    pub max_fail_rate_pct: f64,

    /// Report output destinations
    #[arg(long = "report", value_name = "FORMAT")]
    pub reports: Vec<String>,

    /// Metadata key=value pairs to include in the report.
    #[arg(short = 'm', long = "metadata", value_name = "KEY=VALUE")]
    pub metadata: Vec<String>,

    /// Prometheus metrics endpoint(s) to scrape during the benchmark.
    ///
    /// Use a single URL, comma-separated `node:URL` entries, or rich
    /// `key=value;key=value@URL` entries. Labels are added to scraped samples.
    #[arg(
        long,
        value_name = "URL|NODE:URL|LABELS@URL",
        value_delimiter = ',',
        value_parser = parse_metrics_url
    )]
    pub metrics_url: Vec<MetricsURL>,

    /// File containing metric names to publish to ClickHouse, one per line.
    #[arg(long, value_name = "PATH")]
    pub clickhouse_metrics_file: Option<PathBuf>,

    /// Scrape interval in milliseconds for the metrics scraper.
    #[arg(long, default_value = "500")]
    pub scrape_interval_ms: u64,

    /// Align exported metric timestamps to this benchmark-start Unix timestamp.
    ///
    /// Accepts Unix seconds or milliseconds. Exported samples keep their
    /// original offset within the run.
    #[arg(long = "metrics-align", value_name = "TIMESTAMP", value_parser = parse_unix_timestamp_ms)]
    pub metrics_align: Option<u64>,

    /// Forward scraped samples in real time via Prometheus remote write.
    ///
    /// Uses `/api/v1/write` and the same PROMETHEUS_* environment variables
    /// as `--report prometheus:<url>`. Requires `--metrics-url`.
    #[arg(long = "metrics-forward", value_name = "URL")]
    pub metrics_forward: Option<String>,
}

/// Which phase of a corpus replay to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CallPhase {
    /// Offer load without recording anything per record.
    Warmup,
    /// Record the open-loop cell and the closed-loop passes.
    Measure,
}

impl CallPhase {
    /// Lowercase name written to the report.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Warmup => "warmup",
            Self::Measure => "measure",
        }
    }
}

/// Arguments for the `view` subcommand.
#[derive(Args)]
pub struct ViewArgs {
    /// Input JSON report file
    #[arg(default_value = "report.json")]
    pub input: PathBuf,
}

#[derive(Parser)]
#[command(name = "bench", about = "Transaction benchmarking tool")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Send transactions from file or stdin
    Send(SendArgs),
    /// Submit blocks or reth-bb big blocks via reth Engine API
    SendBlocks(SendBlocksArgs),
    /// Replay an RPC corpus (eth_call, debug_*, trace_*) against a node
    Call(CallArgs),
    /// Print an existing JSON report to the console
    View(ViewArgs),
}

fn parse_duration_millis_fallback(s: &str) -> Result<Duration, String> {
    humantime::parse_duration(s).or_else(|_| {
        s.trim()
            .parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|_| format!("invalid duration: {s:?}"))
    })
}

fn parse_unix_timestamp_ms(s: &str) -> Result<u64, String> {
    const UNIX_SECONDS_CUTOFF: u64 = 100_000_000_000;

    let timestamp =
        s.trim().parse::<u64>().map_err(|_| format!("invalid Unix timestamp: {s:?}"))?;

    if timestamp < UNIX_SECONDS_CUTOFF {
        timestamp
            .checked_mul(1000)
            .ok_or_else(|| format!("Unix timestamp overflows milliseconds: {s:?}"))
    } else {
        Ok(timestamp)
    }
}

fn parse_positive_usize(s: &str) -> Result<usize, String> {
    let value = s.trim().parse::<usize>().map_err(|e| format!("invalid value {s:?}: {e}"))?;
    if value == 0 {
        return Err(format!("{s:?} must be greater than 0"));
    }
    Ok(value)
}

fn parse_reorg_depth(s: &str) -> Result<usize, String> {
    let depth = s.trim().parse::<usize>().map_err(|e| format!("invalid reorg depth: {e}"))?;
    if depth == 0 {
        return Err("reorg depth requires DEPTH > 0".to_string());
    }
    Ok(depth)
}

fn parse_wait_for_persistence(s: &str) -> Result<WaitForPersistence, String> {
    match s {
        "always" => Ok(WaitForPersistence::Always),
        "never" => Ok(WaitForPersistence::Never),
        s if s.starts_with("every:") => {
            let n = s
                .strip_prefix("every:")
                .unwrap_or("0")
                .parse::<u64>()
                .map_err(|e| format!("invalid number in every:N: {e}"))?;
            if n == 0 {
                return Err("every:N requires N > 0".to_string());
            }
            Ok(WaitForPersistence::EveryN(n))
        }
        _ => Err(format!("invalid value '{s}': expected 'always', 'never', or 'every:N'")),
    }
}

fn load_metric_names(path: Option<&PathBuf>) -> Result<Option<HashSet<String>>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let contents = std::fs::read_to_string(path)
        .wrap_err_with(|| format!("failed to read metric allowlist from {}", path.display()))?;
    let names = contents
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(ToOwned::to_owned)
        .collect::<HashSet<_>>();
    if names.is_empty() {
        bail!("metric allowlist {} contains no metric names", path.display());
    }
    Ok(Some(names))
}

fn tracing_env_filter() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into())
}

fn allow_diagnostic_event(metadata: &tracing::Metadata<'_>) -> bool {
    if *metadata.level() != tracing::Level::TRACE {
        return true;
    }

    // These dependencies emit raw HTTP bodies or errors at TRACE. This filter
    // only suppresses those events; it never raises the configured log level.
    !matches!(
        metadata.target(),
        "alloy_transport_http::reqwest_transport" |
            "alloy_transport_http::hyper_transport" |
            "alloy_transport::layers::retry" |
            "alloy_json_rpc::result"
    )
}

fn init_tracing() {
    use tracing_subscriber::{
        filter::{filter_fn, FilterExt},
        layer::SubscriberExt,
        util::SubscriberInitExt,
        Layer,
    };

    let filter = tracing_env_filter().and(filter_fn(allow_diagnostic_event));
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(filter))
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();

    match cli.command {
        Command::Send(args) => send::execute(args).await,
        Command::SendBlocks(args) => send_blocks::execute(args).await,
        Command::Call(args) => call::execute(args).await,
        Command::View(args) => view::execute(args),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics_url::MetricsURL;
    use std::io::Write;

    #[test]
    fn test_parse_duration_millis_fallback_with_unit() {
        assert_eq!(parse_duration_millis_fallback("100ms"), Ok(Duration::from_millis(100)));
        assert_eq!(parse_duration_millis_fallback("2s"), Ok(Duration::from_secs(2)));
    }

    #[test]
    fn test_parse_duration_millis_fallback_bare_millis() {
        assert_eq!(parse_duration_millis_fallback("400"), Ok(Duration::from_millis(400)));
        assert_eq!(parse_duration_millis_fallback("0"), Ok(Duration::from_millis(0)));
    }

    #[test]
    fn test_parse_duration_millis_fallback_errors() {
        assert!(parse_duration_millis_fallback("abc").is_err());
        assert!(parse_duration_millis_fallback("").is_err());
    }

    #[test]
    fn test_parse_unix_timestamp_ms_seconds() {
        assert_eq!(parse_unix_timestamp_ms("1700000000"), Ok(1_700_000_000_000));
    }

    #[test]
    fn test_parse_unix_timestamp_ms_millis() {
        assert_eq!(parse_unix_timestamp_ms("1700000000123"), Ok(1_700_000_000_123));
    }

    #[test]
    fn test_parse_unix_timestamp_ms_errors() {
        assert!(parse_unix_timestamp_ms("abc").is_err());
        assert!(parse_unix_timestamp_ms("-1").is_err());
        assert!(parse_unix_timestamp_ms("").is_err());
    }

    #[test]
    fn test_parse_reorg_depth() {
        assert_eq!(parse_reorg_depth("1"), Ok(1));
        assert_eq!(parse_reorg_depth("8"), Ok(8));
        assert!(parse_reorg_depth("0").is_err());
        assert!(parse_reorg_depth("abc").is_err());
    }

    #[test]
    fn test_send_blocks_reorg_gap_requires_reorg() {
        assert!(Cli::try_parse_from([
            "bench",
            "send-blocks",
            "--engine=http://localhost:8551",
            "--jwt-secret=/tmp/jwt.hex",
            "--reorg-gap=0",
        ])
        .is_err());
    }

    #[test]
    fn test_send_pending_limit_defaults_and_overrides() {
        for (flags, expected) in [
            (vec![], None),
            (vec!["--tps", "0"], None),
            (vec!["--tps", "50000"], Some(50000)),
            (vec!["--tps", "50000", "--max-pending", "0"], None),
            (vec!["--tps", "50000", "--max-pending", "10000"], Some(10000)),
            (vec!["--max-pending", "10000"], Some(10000)),
        ] {
            let cli = Cli::try_parse_from(["bench", "send"].into_iter().chain(flags)).unwrap();
            let Command::Send(args) = cli.command else {
                panic!("expected send command");
            };
            assert_eq!(args.pending_limit().unwrap().map(NonZeroUsize::get), expected);
        }
    }

    #[test]
    fn test_send_retries_default_is_forever() {
        let cli = Cli::try_parse_from(["bench", "send"]).unwrap();

        let Command::Send(args) = cli.command else {
            panic!("expected send command");
        };

        assert_eq!(args.retries, None);
    }

    #[test]
    fn test_send_retries_zero_disables_retries() {
        let cli = Cli::try_parse_from(["bench", "send", "--retries", "0"]).unwrap();

        let Command::Send(args) = cli.command else {
            panic!("expected send command");
        };

        assert_eq!(args.retries, Some(0));
    }

    #[test]
    fn test_send_collect_latencies_default_disabled() {
        let cli = Cli::try_parse_from(["bench", "send"]).unwrap();

        let Command::Send(args) = cli.command else {
            panic!("expected send command");
        };

        assert!(!args.collect_latencies);
    }

    #[test]
    fn test_send_collect_latencies_enabled() {
        let cli = Cli::try_parse_from(["bench", "send", "--collect-latencies"]).unwrap();

        let Command::Send(args) = cli.command else {
            panic!("expected send command");
        };

        assert!(args.collect_latencies);
    }

    #[test]
    fn test_send_collect_receipt_metrics_default_disabled() {
        let cli = Cli::try_parse_from(["bench", "send"]).unwrap();

        let Command::Send(args) = cli.command else {
            panic!("expected send command");
        };

        assert!(!args.collect_receipt_metrics);
    }

    #[test]
    fn test_send_collect_receipt_metrics_enabled() {
        let cli = Cli::try_parse_from(["bench", "send", "--collect-receipt-metrics"]).unwrap();

        let Command::Send(args) = cli.command else {
            panic!("expected send command");
        };

        assert!(args.collect_receipt_metrics);
    }

    #[test]
    fn test_metrics_url_value_parser_single_url() {
        let cli = Cli::try_parse_from([
            "bench",
            "send",
            "--metrics-url",
            "http://127.0.0.1:9001/metrics",
        ])
        .unwrap();

        let Command::Send(args) = cli.command else {
            panic!("expected send command");
        };

        assert_eq!(
            args.metrics_url,
            vec![MetricsURL::Unlabeled("http://127.0.0.1:9001/metrics".to_string())]
        );
    }

    #[test]
    fn test_metrics_url_value_parser_rich_labels() {
        let cli = Cli::try_parse_from([
            "bench",
            "send",
            "--metrics-url",
            "validator=v0;region=us-east-1@http://127.0.0.1:9001/metrics",
        ])
        .unwrap();

        let Command::Send(args) = cli.command else {
            panic!("expected send command");
        };

        assert_eq!(args.metrics_url.len(), 1);
        assert_eq!(
            args.metrics_url[0],
            MetricsURL::Labeled {
                labels: std::collections::BTreeMap::from([
                    ("region".to_string(), "us-east-1".to_string()),
                    ("validator".to_string(), "v0".to_string()),
                ]),
                url: "http://127.0.0.1:9001/metrics".to_string(),
            }
        );
    }

    #[test]
    fn test_send_metrics_forward() {
        let cli = Cli::try_parse_from([
            "bench",
            "send",
            "--metrics-url",
            "http://127.0.0.1:9001/metrics",
            "--metrics-forward",
            "http://victoriametrics:8428",
        ])
        .unwrap();

        let Command::Send(args) = cli.command else {
            panic!("expected send command");
        };

        assert_eq!(args.metrics_forward, Some("http://victoriametrics:8428".to_string()));
    }

    #[test]
    fn test_send_blocks_metrics_forward() {
        let cli = Cli::try_parse_from([
            "bench",
            "send-blocks",
            "--engine",
            "http://localhost:8551",
            "--jwt-secret",
            "/tmp/jwt.hex",
            "--metrics-url",
            "http://127.0.0.1:9001/metrics",
            "--metrics-forward",
            "http://prometheus:9090",
        ])
        .unwrap();

        let Command::SendBlocks(args) = cli.command else {
            panic!("expected send-blocks command");
        };

        assert_eq!(args.metrics_forward, Some("http://prometheus:9090".to_string()));
    }

    #[test]
    fn test_metrics_url_value_parser_splits_comma_entries() {
        let cli = Cli::try_parse_from([
            "bench",
            "send-blocks",
            "--engine",
            "http://localhost:8551",
            "--jwt-secret",
            "/tmp/jwt.hex",
            "--metrics-url",
            "a:http://node-a:9001/metrics,b:http://node-b:9001/metrics",
        ])
        .unwrap();

        let Command::SendBlocks(args) = cli.command else {
            panic!("expected send-blocks command");
        };

        assert_eq!(
            args.metrics_url,
            vec![
                MetricsURL::Labeled {
                    labels: std::collections::BTreeMap::from([(
                        "node".to_string(),
                        "a".to_string(),
                    )]),
                    url: "http://node-a:9001/metrics".to_string(),
                },
                MetricsURL::Labeled {
                    labels: std::collections::BTreeMap::from([(
                        "node".to_string(),
                        "b".to_string(),
                    )]),
                    url: "http://node-b:9001/metrics".to_string(),
                },
            ]
        );
    }

    #[test]
    fn test_call_defaults() {
        let cli = Cli::try_parse_from(["bench", "call", "-i", "corpus.jsonl"]).unwrap();

        let Command::Call(args) = cli.command else {
            panic!("expected call command");
        };

        assert_eq!(args.phase, CallPhase::Measure);
        assert_eq!(args.rps, 100);
        assert_eq!(args.duration, Duration::from_secs(120));
        assert_eq!(args.requests, None);
        assert_eq!(args.max_concurrent, 256);
        assert_eq!(args.passes, 20);
        assert_eq!(args.concurrency, 16);
        assert_eq!(args.seed, 1);
        assert_eq!(args.timeout, Duration::from_secs(30));
        assert_eq!(args.max_fail_rate_pct, 1.0);
        assert!(args.methods.is_empty());
        assert_eq!(args.block_tag, None);
        assert!(!args.strip_fees);
    }

    #[test]
    fn test_call_accepts_a_comma_separated_method_filter() {
        let cli = Cli::try_parse_from([
            "bench",
            "call",
            "-i",
            "corpus.jsonl.gz",
            "--phase",
            "warmup",
            "--methods",
            "eth_call,debug_traceCall",
        ])
        .unwrap();

        let Command::Call(args) = cli.command else {
            panic!("expected call command");
        };

        assert_eq!(args.phase, CallPhase::Warmup);
        assert_eq!(args.methods, vec!["eth_call", "debug_traceCall"]);
    }

    #[test]
    fn test_call_rejects_a_zero_concurrency() {
        assert!(
            Cli::try_parse_from(["bench", "call", "-i", "c.jsonl", "--concurrency", "0"]).is_err()
        );
        assert!(Cli::try_parse_from(["bench", "call", "-i", "c.jsonl", "--max-concurrent", "0"])
            .is_err());
    }

    #[test]
    fn load_metric_names_ignores_comments_blanks_and_duplicates() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(file, "# dashboard metrics\nmetric_a\n\nmetric_b\nmetric_a").unwrap();

        let names = load_metric_names(Some(&file.path().to_path_buf())).unwrap().unwrap();
        assert_eq!(names, HashSet::from(["metric_a".to_string(), "metric_b".to_string()]));
    }
}
