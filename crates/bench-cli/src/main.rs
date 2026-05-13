//! CLI binary for the bench tool.
//!
//! Provides subcommands:
//! - `send` - Send from file/stdin
//! - `send-blocks` - Submit blocks via reth Engine API
//! - `view` - Print an existing JSON report to console

use clap::{Args, Parser, Subcommand};
use eyre::Result;
use std::{path::PathBuf, time::Duration};

mod metrics_url;
mod send;
mod send_blocks;
mod view;

/// Arguments for the `send` subcommand.
#[derive(Args)]
pub struct SendArgs {
    /// Input file (NDJSON). If not specified, reads from stdin.
    #[arg(short, long)]
    pub input: Option<PathBuf>,

    /// RPC endpoint URLs (comma-separated or repeated)
    #[arg(long = "rpc-url", value_delimiter = ',', default_values_t = vec!["http://localhost:8545".to_string()])]
    pub rpc_urls: Vec<String>,

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
        value_parser = metrics_url::parse_metrics_url
    )]
    pub metrics_url: Vec<metrics_url::MetricsURL>,

    /// Scrape interval in milliseconds for the metrics scraper.
    #[arg(long, default_value = "500")]
    pub scrape_interval_ms: u64,

    /// Align exported metric timestamps to this benchmark-start Unix timestamp.
    ///
    /// Accepts Unix seconds or milliseconds. Exported samples keep their
    /// original offset within the run.
    #[arg(long = "metrics-align", value_name = "TIMESTAMP", value_parser = parse_unix_timestamp_ms)]
    pub metrics_align: Option<u64>,

    /// Skip setup-phase transactions in the input stream.
    #[arg(long)]
    pub skip_setup: bool,

    /// Wait for the transaction pool to drain after sending.
    ///
    /// Polls `txpool_status` and waits until the pending count reaches zero
    /// (3 consecutive readings) before collecting block stats and finalizing.
    /// Set to 0 to disable. Keeps the metrics scraper running during the wait.
    #[arg(long, default_value = "300")]
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
    /// threshold is crossed. Default is every:2 (matching reth's
    /// DEFAULT_PERSISTENCE_THRESHOLD).
    #[arg(long, default_value = "every:2", value_parser = parse_wait_for_persistence)]
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
        value_parser = metrics_url::parse_metrics_url
    )]
    pub metrics_url: Vec<metrics_url::MetricsURL>,

    /// Scrape interval in milliseconds for the metrics scraper.
    #[arg(long, default_value = "500")]
    pub scrape_interval_ms: u64,

    /// Align exported metric timestamps to this benchmark-start Unix timestamp.
    ///
    /// Accepts Unix seconds or milliseconds. Exported samples keep their
    /// original offset within the run.
    #[arg(long = "metrics-align", value_name = "TIMESTAMP", value_parser = parse_unix_timestamp_ms)]
    pub metrics_align: Option<u64>,

    /// Build a synthetic side fork and alternate forkchoice updates.
    #[arg(
        long,
        value_name = "DEPTH",
        num_args = 0..=1,
        default_missing_value = "8",
        value_parser = parse_reorg_depth,
    )]
    pub reorg: Option<usize>,

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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

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
}
