//! CLI binary for the bench tool.
//!
//! Provides subcommands:
//! - `send` - Send from file/stdin
//! - `send-blocks` - Submit blocks via reth Engine API
//! - `view` - Print an existing JSON report to console

use clap::{Args, Parser, Subcommand};
use eyre::{bail, Context, Result};
use std::{collections::HashSet, path::PathBuf, time::Duration};

use crate::metrics_url::{parse_metrics_url, MetricsURL};

mod metrics_forwarder;
mod metrics_url;
mod send;
mod send_blocks;
mod view;

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

/// Arguments for the `send` subcommand.
#[derive(Args)]
pub struct SendArgs {
    /// Input file (NDJSON). If not specified, reads from stdin.
    #[arg(short, long)]
    pub input: Option<PathBuf>,

    /// RPC endpoint URLs (comma-separated or repeated)
    #[arg(long = "rpc-url", value_delimiter = ',', default_values_t = vec!["http://localhost:8545".to_string()])]
    pub rpc_urls: Vec<String>,

    /// Optional RPC endpoint for aggregate block and txpool queries.
    ///
    /// Transaction submission and sender-scoped receipt polling continue to
    /// use --rpc-url. When omitted, aggregate queries use the first --rpc-url.
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
    /// Use a single URL, or comma-separated `node:URL` entries for multiple
    /// endpoints. Labeled endpoints add `node=<label>` to scraped samples.
    #[arg(
        long,
        value_name = "URL|NODE:URL",
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

    /// Collect receipt-derived gas metrics for every accepted workload transaction.
    ///
    /// Disabled by default because receipt polling adds substantial RPC load.
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
    pub wait_for_persistence: bench_core::WaitForPersistence,

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
    /// Use a single URL, or comma-separated `node:URL` entries for multiple
    /// endpoints. Labeled endpoints add `node=<label>` to scraped samples.
    #[arg(
        long,
        value_name = "URL|NODE:URL",
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

fn parse_reorg_depth(s: &str) -> Result<usize, String> {
    let depth = s.trim().parse::<usize>().map_err(|e| format!("invalid reorg depth: {e}"))?;
    if depth == 0 {
        return Err("reorg depth requires DEPTH > 0".to_string());
    }
    Ok(depth)
}

fn parse_wait_for_persistence(s: &str) -> Result<bench_core::WaitForPersistence, String> {
    match s {
        "always" => Ok(bench_core::WaitForPersistence::Always),
        "never" => Ok(bench_core::WaitForPersistence::Never),
        s if s.starts_with("every:") => {
            let n = s
                .strip_prefix("every:")
                .unwrap_or("0")
                .parse::<u64>()
                .map_err(|e| format!("invalid number in every:N: {e}"))?;
            if n == 0 {
                return Err("every:N requires N > 0".to_string());
            }
            Ok(bench_core::WaitForPersistence::EveryN(n))
        }
        _ => Err(format!("invalid value '{s}': expected 'always', 'never', or 'every:N'")),
    }
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
                    node: "a".to_string(),
                    url: "http://node-a:9001/metrics".to_string(),
                },
                MetricsURL::Labeled {
                    node: "b".to_string(),
                    url: "http://node-b:9001/metrics".to_string(),
                },
            ]
        );
    }

    #[test]
    fn load_metric_names_ignores_comments_blanks_and_duplicates() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(file, "# dashboard metrics\nmetric_a\n\nmetric_b\nmetric_a").unwrap();

        let names = load_metric_names(Some(&file.path().to_path_buf())).unwrap().unwrap();
        assert_eq!(names, HashSet::from(["metric_a".to_string(), "metric_b".to_string()]));
    }
}
