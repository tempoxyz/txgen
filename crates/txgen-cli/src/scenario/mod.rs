//! Reusable multi-chain scenario schema, runtime, execution, and reporting.

use alloy_consensus::{SignableTransaction, Signed};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::Network;
use clap::{Args, Subcommand, ValueEnum};
use eyre::{bail, Result, WrapErr};
use rand::Rng;
use std::{collections::BTreeMap, io::Write, path::PathBuf, time::Duration};

use crate::NetworkAdapter;

mod clickhouse;
mod composition;
mod engine;
mod error;
mod report;
pub mod schema;
pub mod value;
mod wait;

pub use engine::{
    execute_scenario, validate_scenario_offline, FailurePolicy, ScenarioExecutionConfig,
};
pub use report::{
    ChainReportConfig, FailureReport, InstanceLifecycle, LatencyDistribution, LifecycleStep,
    ScenarioReport, ScenarioReportConfig, StepReport,
};
pub use schema::*;
pub use value::{
    coerce_event_filter, collect_variable_paths, eval_expression, event_value_matches,
    materialize_yaml, RuntimeContext, RuntimeValue,
};

/// Nested `scenario` command.
#[derive(Debug, Args)]
pub struct ScenarioArgs {
    #[command(subcommand)]
    command: ScenarioCommand,
}

#[derive(Debug, Subcommand)]
enum ScenarioCommand {
    /// Execute a versioned multi-chain scenario.
    Run(ScenarioRunArgs),
    /// Resolve and validate a scenario without contacting RPC endpoints.
    Validate(ScenarioValidateArgs),
    /// Resolve, validate, and emit a flattened scenario document.
    Render(ScenarioRenderArgs),
}

/// Controls for `scenario validate`.
#[derive(Debug, Args)]
pub struct ScenarioValidateArgs {
    /// Scenario YAML file.
    #[arg(long)]
    pub scenario: PathBuf,
}

/// Controls for `scenario render`.
#[derive(Debug, Args)]
pub struct ScenarioRenderArgs {
    /// Scenario YAML file.
    #[arg(long)]
    pub scenario: PathBuf,

    /// Write flattened YAML to this path instead of stdout.
    #[arg(long)]
    pub output: Option<PathBuf>,
}

/// Controls for `scenario run`.
#[derive(Debug, Args)]
pub struct ScenarioRunArgs {
    /// Scenario YAML file.
    #[arg(long)]
    pub scenario: PathBuf,

    /// Maximum number of scenario instances to start.
    #[arg(short = 'n', long)]
    pub count: Option<u64>,

    /// Window during which new scenario instances may start.
    #[arg(long, value_parser = humantime::parse_duration)]
    pub duration: Option<Duration>,

    /// Scenario instances started per second (0 = unlimited).
    #[arg(long, default_value_t = 0.0)]
    pub starts_per_second: f64,

    /// Maximum active scenario instances.
    #[arg(long, default_value_t = 1)]
    pub max_in_flight: usize,

    /// Default timeout for steps without an explicit `timeout`.
    #[arg(long, value_parser = humantime::parse_duration)]
    pub step_timeout: Option<Duration>,

    /// RNG seed for deterministic binding and template materialization.
    #[arg(long)]
    pub seed: Option<u64>,

    /// Behavior after an instance fails.
    #[arg(long, value_enum, default_value_t = FailurePolicyArg::Continue)]
    failure_policy: FailurePolicyArg,

    /// Maximum transaction submissions per second on each chain (0 = unlimited).
    #[arg(long, default_value_t = 0)]
    pub tx_rate: u64,

    /// Maximum RPC transaction submissions in flight on each chain.
    #[arg(long, default_value_t = 100)]
    pub max_rpc_in_flight: usize,

    /// Report destination: a JSON path, json:<path>, or clickhouse:<url>. Repeatable.
    #[arg(long = "report", value_name = "FORMAT")]
    pub reports: Vec<String>,

    /// Metadata key=value pair for ClickHouse reporting. Repeatable.
    #[arg(short = 'm', long = "metadata", value_name = "KEY=VALUE")]
    pub metadata: Vec<String>,

    /// Include the first N individual lifecycle records in the report.
    #[arg(long, default_value_t = 0)]
    pub sample_instances: usize,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum FailurePolicyArg {
    FailFast,
    Continue,
}

impl From<FailurePolicyArg> for FailurePolicy {
    fn from(value: FailurePolicyArg) -> Self {
        match value {
            FailurePolicyArg::FailFast => Self::FailFast,
            FailurePolicyArg::Continue => Self::Continue,
        }
    }
}

pub(crate) async fn run_scenario_command<A>(args: ScenarioArgs) -> Result<()>
where
    A: NetworkAdapter + Default + Send + Sync + 'static,
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
{
    match args.command {
        ScenarioCommand::Run(args) => run_scenario::<A>(args).await,
        ScenarioCommand::Validate(args) => validate_scenario::<A>(args),
        ScenarioCommand::Render(args) => render_scenario::<A>(args),
    }
}

fn validate_scenario<A: NetworkAdapter>(args: ScenarioValidateArgs) -> Result<()> {
    let resolved = composition::load_scenario(&args.scenario)?;
    validate_scenario_offline::<A>(&resolved.spec)?;
    println!(
        "scenario '{}' is valid ({} expanded steps)",
        resolved.spec.scenario.name,
        resolved.spec.scenario.steps.len()
    );
    Ok(())
}

fn render_scenario<A: NetworkAdapter>(args: ScenarioRenderArgs) -> Result<()> {
    let scenario_path = std::fs::canonicalize(&args.scenario).wrap_err_with(|| {
        format!("failed to resolve scenario file: {}", args.scenario.display())
    })?;
    let resolved = composition::load_scenario(&scenario_path)?;
    validate_scenario_offline::<A>(&resolved.spec)?;
    let mut rendered = serde_yaml::to_string(&resolved.rendered)?;
    if !rendered.ends_with('\n') {
        rendered.push('\n');
    }
    match args.output {
        Some(path) => {
            let file = std::fs::File::create(&path).wrap_err_with(|| {
                format!("failed to create rendered scenario: {}", path.display())
            })?;
            let mut writer = std::io::BufWriter::new(file);
            writer.write_all(rendered.as_bytes())?;
            writer.flush()?;
        }
        None => {
            let stdout = std::io::stdout();
            let mut writer = stdout.lock();
            writer.write_all(rendered.as_bytes())?;
            writer.flush()?;
        }
    }
    Ok(())
}

async fn run_scenario<A>(args: ScenarioRunArgs) -> Result<()>
where
    A: NetworkAdapter + Default + Send + Sync + 'static,
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
{
    if args.count == Some(0) {
        bail!("--count must be greater than zero");
    }
    let destinations = ScenarioReportDestinations::parse(&args.reports)?;
    let metadata = parse_metadata(&args.metadata)?;
    let spec = ScenarioSpec::load(&args.scenario)?;
    let clickhouse_reporters = destinations
        .clickhouse_urls
        .iter()
        .map(|url| {
            clickhouse::ScenarioClickHouseReporter::from_env(url, A::network_name(), &metadata)
        })
        .collect::<Result<Vec<_>>>()?;
    let count = args.count.or_else(|| args.duration.is_none().then_some(1));
    let seed = args.seed.unwrap_or_else(|| rand::rng().random());
    let report = execute_scenario::<A>(
        spec,
        ScenarioExecutionConfig {
            count,
            duration: args.duration,
            starts_per_second: args.starts_per_second,
            max_in_flight: args.max_in_flight,
            step_timeout: args.step_timeout,
            seed,
            failure_policy: args.failure_policy.into(),
            transaction_rate: args.tx_rate,
            max_rpc_in_flight: args.max_rpc_in_flight,
            sample_instances: args.sample_instances,
        },
    )
    .await?;

    finalize_scenario_reports(&report, &destinations, &clickhouse_reporters)
}

#[derive(Debug, Default, PartialEq, Eq)]
struct ScenarioReportDestinations {
    json_paths: Vec<PathBuf>,
    clickhouse_urls: Vec<String>,
    json_stdout: bool,
}

impl ScenarioReportDestinations {
    fn parse(specs: &[String]) -> Result<Self> {
        if specs.is_empty() {
            return Ok(Self { json_stdout: true, ..Self::default() });
        }

        let mut destinations = Self::default();
        for spec in specs {
            if let Some(path) = spec.strip_prefix("json:") {
                if path.is_empty() {
                    bail!("scenario JSON report path must not be empty");
                }
                destinations.json_paths.push(PathBuf::from(path));
            } else if let Some(url) = spec.strip_prefix("clickhouse:") {
                if url.is_empty() {
                    bail!("scenario ClickHouse report URL must not be empty");
                }
                let url = canonical_clickhouse_url(url)?;
                if destinations.clickhouse_urls.contains(&url) {
                    bail!("duplicate scenario ClickHouse report destination '{url}'");
                }
                destinations.clickhouse_urls.push(url);
            } else {
                if spec.is_empty() {
                    bail!("scenario JSON report path must not be empty");
                }
                destinations.json_paths.push(PathBuf::from(spec));
            }
        }
        Ok(destinations)
    }
}

fn canonical_clickhouse_url(value: &str) -> Result<String> {
    let mut url = url::Url::parse(value).wrap_err("invalid scenario ClickHouse report URL")?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        bail!("scenario ClickHouse report URL must use HTTP or HTTPS");
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!(
            "scenario ClickHouse report URL must not contain credentials; use CLICKHOUSE_USER and CLICKHOUSE_PASSWORD"
        );
    }
    if url.query().is_some() || url.fragment().is_some() {
        bail!("scenario ClickHouse report URL must not contain a query string or fragment");
    }
    let path = url.path().trim_end_matches('/').to_string();
    url.set_path(if path.is_empty() { "/" } else { &path });
    Ok(url.as_str().trim_end_matches('/').to_string())
}

fn parse_metadata(values: &[String]) -> Result<BTreeMap<String, String>> {
    let mut metadata = BTreeMap::new();
    for value in values {
        let (key, value) = value
            .split_once('=')
            .ok_or_else(|| eyre::eyre!("invalid metadata format '{value}'; expected key=value"))?;
        if key.is_empty() {
            bail!("metadata key cannot be empty");
        }
        metadata.insert(key.to_string(), value.to_string());
    }
    Ok(metadata)
}

fn finalize_scenario_reports(
    report: &ScenarioReport,
    destinations: &ScenarioReportDestinations,
    clickhouse_reporters: &[clickhouse::ScenarioClickHouseReporter],
) -> Result<()> {
    let mut failures = Vec::new();

    if destinations.json_stdout {
        let stdout = std::io::stdout();
        let mut writer = stdout.lock();
        if let Err(error) = write_json_report(&mut writer, report) {
            failures.push(format!("stdout JSON report: {error:#}"));
        }
    }
    for path in &destinations.json_paths {
        if let Err(error) = write_json_report_file(path, report) {
            failures.push(format!("JSON report {}: {error:#}", path.display()));
        }
    }

    for reporter in clickhouse_reporters {
        match reporter.publish(report) {
            Ok(()) => eprintln!(
                "published scenario report {} to ClickHouse at {}",
                report.run_id,
                reporter.endpoint()
            ),
            Err(error) => failures.push(format!(
                "ClickHouse report {} (run_id={}): {error:#}",
                reporter.endpoint(),
                report.run_id
            )),
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        bail!("scenario report destination failure(s):\n- {}", failures.join("\n- "));
    }
}

fn write_json_report_file(path: &std::path::Path, report: &ScenarioReport) -> Result<()> {
    let file = std::fs::File::create(path)
        .wrap_err_with(|| format!("failed to create scenario report: {}", path.display()))?;
    let mut writer = std::io::BufWriter::new(file);
    write_json_report(&mut writer, report)
}

fn write_json_report(writer: &mut impl Write, report: &ScenarioReport) -> Result<()> {
    serde_json::to_writer_pretty(&mut *writer, report)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::Read, net::TcpListener, thread};

    fn sample_report() -> ScenarioReport {
        ScenarioReport {
            version: 1,
            run_id: uuid::Uuid::new_v4(),
            scenario: "reporting-test".into(),
            configuration: ScenarioReportConfig {
                chains: vec![ChainReportConfig {
                    name: "primary".into(),
                    network: "tempo".into(),
                    chain_id: 1,
                    workload: "/tmp/workload.yml".into(),
                }],
                requested_instances: Some(2),
                run_duration_ms: None,
                starts_per_second: 1.0,
                maximum_in_flight: 2,
                default_step_timeout_ms: 1_000,
                transaction_rate_per_chain: 0,
                maximum_rpc_in_flight_per_chain: 10,
                seed: 9,
                failure_policy: "continue".into(),
            },
            started_at_unix_ms: 1_000,
            finished_at_unix_ms: 2_000,
            elapsed_ms: 1_000,
            started: 2,
            completed: 1,
            failed: 1,
            timed_out: 1,
            completed_scenarios_per_second: 1.0,
            maximum_in_flight: 2,
            steps: vec![StepReport {
                index: 0,
                name: "submit".into(),
                chain: "primary".into(),
                kind: "submit".into(),
                provenance: None,
                success: 1,
                failed: 1,
                latency: LatencyDistribution {
                    samples: 2,
                    min_ms: 1.0,
                    max_ms: 2.0,
                    mean_ms: 1.5,
                    p50_ms: 1.0,
                    p95_ms: 2.0,
                    p99_ms: 2.0,
                },
            }],
            total_scenario_latency: LatencyDistribution {
                samples: 1,
                min_ms: 10.0,
                max_ms: 10.0,
                mean_ms: 10.0,
                p50_ms: 10.0,
                p95_ms: 10.0,
                p99_ms: 10.0,
            },
            failures: Vec::new(),
            sampled_instances: Vec::new(),
        }
    }

    fn serve_clickhouse(statuses: &[&str]) -> (String, thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let statuses = statuses.iter().map(|status| status.to_string()).collect::<Vec<_>>();
        let server = thread::spawn(move || {
            let mut requests = Vec::new();
            for status in statuses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 4096];
                let mut expected_len = None;
                loop {
                    let read = stream.read(&mut buffer).unwrap();
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if let Some(header_end) =
                        request.windows(4).position(|part| part == b"\r\n\r\n")
                    {
                        let header_end = header_end + 4;
                        let headers = String::from_utf8_lossy(&request[..header_end]);
                        let content_length = headers.lines().find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        });
                        expected_len = Some(header_end + content_length.unwrap_or(0));
                    }
                    if expected_len.is_some_and(|length| request.len() >= length) {
                        break;
                    }
                }
                requests.push(String::from_utf8(request).unwrap());
                let body = if status.starts_with('2') { "" } else { "injected failure" };
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
            requests
        });
        (endpoint, server)
    }

    #[test]
    fn parses_legacy_and_named_report_destinations() {
        let defaults = ScenarioReportDestinations::parse(&[]).unwrap();
        assert!(defaults.json_stdout);

        let destinations = ScenarioReportDestinations::parse(&[
            "legacy.json".into(),
            "json:named.json".into(),
            "clickhouse:https://clickhouse.example".into(),
        ])
        .unwrap();
        assert_eq!(
            destinations.json_paths,
            [PathBuf::from("legacy.json"), PathBuf::from("named.json")]
        );
        assert_eq!(destinations.clickhouse_urls, ["https://clickhouse.example"]);
        assert!(!destinations.json_stdout);
        assert!(ScenarioReportDestinations::parse(&[
            "clickhouse:https://CLICKHOUSE.EXAMPLE:443".into(),
            "clickhouse:https://clickhouse.example/".into(),
        ])
        .is_err());
    }

    #[test]
    fn parses_metadata_and_rejects_malformed_values() {
        let metadata =
            parse_metadata(&["git-sha=abc".into(), "phase=nightly=zones".into()]).unwrap();
        assert_eq!(metadata["git-sha"], "abc");
        assert_eq!(metadata["phase"], "nightly=zones");
        assert!(parse_metadata(&["missing-separator".into()]).is_err());
        assert!(parse_metadata(&["=value".into()]).is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn clickhouse_publication_batches_steps_and_commits_run_last() {
        let (endpoint, server) = serve_clickhouse(&["200 OK", "200 OK", "200 OK"]);
        let metadata = BTreeMap::from([
            ("git-sha".into(), "abc123".into()),
            ("git-ref".into(), "main".into()),
        ]);
        let reporter =
            clickhouse::ScenarioClickHouseReporter::from_env(&endpoint, "tempo", &metadata)
                .unwrap();

        reporter.publish(&sample_report()).unwrap();
        let requests = server.join().unwrap();

        assert_eq!(requests.len(), 3);
        assert!(requests[0].contains("txgen_scenario_steps"));
        assert!(requests[1].contains("txgen_scenario_runs"));
        assert!(requests[2].contains("txgen_runs"));
        assert!(requests.iter().all(|request| !request.contains("txgen_blocks")));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn clickhouse_failure_keeps_json_and_does_not_publish_run_marker() {
        let (endpoint, server) = serve_clickhouse(&["200 OK", "500 Internal Server Error"]);
        let metadata = BTreeMap::from([
            ("git-sha".into(), "abc123".into()),
            ("git-ref".into(), "main".into()),
        ]);
        let reporter =
            clickhouse::ScenarioClickHouseReporter::from_env(&endpoint, "tempo", &metadata)
                .unwrap();
        let path = std::env::temp_dir()
            .join(format!("txgen-scenario-report-partial-{}.json", uuid::Uuid::new_v4()));
        let destinations = ScenarioReportDestinations {
            json_paths: vec![path.clone()],
            clickhouse_urls: vec![endpoint],
            json_stdout: false,
        };
        let report = sample_report();

        let error =
            finalize_scenario_reports(&report, &destinations, &[reporter]).unwrap_err().to_string();
        let written: serde_json::Value =
            serde_json::from_reader(std::fs::File::open(&path).unwrap()).unwrap();
        let requests = server.join().unwrap();
        let _ = std::fs::remove_file(path);

        assert_eq!(written["run_id"], report.run_id.to_string());
        assert!(error.contains("scenario aggregate row"));
        assert!(error.contains(&report.run_id.to_string()));
        assert_eq!(requests.len(), 2);
        assert!(requests[0].contains("txgen_scenario_steps"));
        assert!(requests[1].contains("txgen_scenario_runs"));
        assert!(requests.iter().all(|request| !request.contains("txgen_runs")));
    }
}
