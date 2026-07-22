use super::ScenarioReport;
use bench_core::{
    insert_receipt_gas_records, ClickHouseClient, DEFAULT_CLICKHOUSE_RECEIPT_BATCH_SIZE,
};
use eyre::{bail, Result, WrapErr};
use serde::Serialize;
use std::collections::BTreeMap;

/// Finalized scenario report publisher backed by ClickHouse JSONEachRow inserts.
pub(crate) struct ScenarioClickHouseReporter {
    client: ClickHouseClient,
    endpoint: String,
    platform: String,
    git_sha: String,
    git_ref: String,
    metadata: BTreeMap<String, String>,
}

impl ScenarioClickHouseReporter {
    pub(crate) fn from_env(
        endpoint: &str,
        platform: &str,
        metadata: &BTreeMap<String, String>,
    ) -> Result<Self> {
        let mut metadata = metadata.clone();
        let git_sha = take_required_metadata(&mut metadata, "git-sha")?;
        let git_ref = take_required_metadata(&mut metadata, "git-ref")?;

        for reserved in ["scenario", "platform", "mode", "run-id", "run_id"] {
            if metadata.contains_key(reserved) {
                bail!(
                    "scenario ClickHouse metadata key '{reserved}' is derived by txgen and cannot be overridden"
                );
            }
        }

        let client = ClickHouseClient::from_env(endpoint)
            .wrap_err("failed to create scenario ClickHouse client")?;
        let display_endpoint = client.endpoint_origin();
        Ok(Self {
            client,
            endpoint: display_endpoint,
            platform: platform.to_string(),
            git_sha,
            git_ref,
            metadata,
        })
    }

    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Publish a complete report using `txgen_runs` as the visibility marker.
    ///
    /// ClickHouse does not provide a transaction spanning these MergeTree
    /// tables. Detail rows are therefore written first and the common run row
    /// last. Consumers that begin from `txgen_runs` cannot observe a partially
    /// published scenario, though an interrupted write can leave orphan detail
    /// rows for that UUID.
    pub(crate) fn publish(&self, report: &ScenarioReport) -> Result<()> {
        let rows = self.build_rows(report);

        self.client
            .insert_rows_synchronous("txgen_scenario_steps", &rows.steps)
            .wrap_err("failed to insert scenario step rows before publication")?;
        insert_receipt_gas_records(
            &self.client,
            report.run_id,
            &report.receipt_records,
            DEFAULT_CLICKHOUSE_RECEIPT_BATCH_SIZE,
        )
        .wrap_err("failed to insert receipt gas rows before publication")?;
        self.client
            .insert_rows_synchronous("txgen_scenario_runs", &[rows.scenario])
            .wrap_err("failed to insert scenario aggregate row before publication")?;
        self.client
            .insert_rows_synchronous("txgen_runs", &[rows.run])
            .wrap_err("failed to publish scenario run marker")?;

        Ok(())
    }

    fn build_rows(&self, report: &ScenarioReport) -> ScenarioClickHouseRows {
        let run = ClickHouseRunRow {
            run_id: report.run_id,
            started_at: report.started_at_unix_ms,
            finished_at: report.finished_at_unix_ms,
            scenario_name: report.scenario.clone(),
            platform: self.platform.clone(),
            mode: "scenario",
            git_sha: self.git_sha.clone(),
            git_ref: self.git_ref.clone(),
            config: report_config(report),
            metadata: self.metadata.clone(),
        };
        let scenario = ScenarioRunRow {
            run_id: report.run_id,
            report_version: report.version,
            requested_journeys: report.configuration.requested_instances,
            started_journeys: report.started,
            completed_journeys: report.completed,
            failed_journeys: report.failed,
            timed_out_journeys: report.timed_out,
            elapsed_ms: report.elapsed_ms,
            completed_journeys_per_second: report.completed_scenarios_per_second,
            maximum_in_flight: usize_to_u64(report.maximum_in_flight),
            latency_samples: usize_to_u64(report.total_scenario_latency.samples),
            latency_min_ms: report.total_scenario_latency.min_ms,
            latency_mean_ms: report.total_scenario_latency.mean_ms,
            latency_p50_ms: report.total_scenario_latency.p50_ms,
            latency_p95_ms: report.total_scenario_latency.p95_ms,
            latency_p99_ms: report.total_scenario_latency.p99_ms,
            latency_max_ms: report.total_scenario_latency.max_ms,
        };
        let steps = report
            .steps
            .iter()
            .map(|step| {
                let provenance = step.provenance.as_ref();
                ScenarioStepRow {
                    run_id: report.run_id,
                    step_index: usize_to_u64(step.index),
                    step_name: step.name.clone(),
                    chain: step.chain.clone(),
                    kind: step.kind.clone(),
                    success: step.success,
                    failed: step.failed,
                    latency_samples: usize_to_u64(step.latency.samples),
                    latency_min_ms: step.latency.min_ms,
                    latency_mean_ms: step.latency.mean_ms,
                    latency_p50_ms: step.latency.p50_ms,
                    latency_p95_ms: step.latency.p95_ms,
                    latency_p99_ms: step.latency.p99_ms,
                    latency_max_ms: step.latency.max_ms,
                    source_file: provenance.map(|value| value.source_file.clone()),
                    fragment: provenance.map(|value| value.fragment.clone()),
                    instance_alias: provenance.map(|value| value.instance_alias.clone()),
                    local_step_name: provenance.map(|value| value.local_step_name.clone()),
                    local_step_index: provenance.map(|value| usize_to_u64(value.local_step_index)),
                }
            })
            .collect();

        ScenarioClickHouseRows { run, scenario, steps }
    }
}

fn take_required_metadata(metadata: &mut BTreeMap<String, String>, key: &str) -> Result<String> {
    let value = metadata.remove(key).unwrap_or_default();
    if value.trim().is_empty() {
        bail!(
            "scenario ClickHouse reporter requires metadata '{key}'; pass --metadata {key}=<value>"
        );
    }
    Ok(value)
}

fn report_config(report: &ScenarioReport) -> BTreeMap<String, String> {
    let configuration = &report.configuration;
    let mut config = BTreeMap::new();
    if let Some(requested) = configuration.requested_instances {
        config.insert("requested_instances".into(), requested.to_string());
    }
    if let Some(duration) = configuration.run_duration_ms {
        config.insert("run_duration_ms".into(), duration.to_string());
    }
    config.insert("starts_per_second".into(), configuration.starts_per_second.to_string());
    config.insert("maximum_in_flight".into(), configuration.maximum_in_flight.to_string());
    config.insert(
        "default_step_timeout_ms".into(),
        configuration.default_step_timeout_ms.to_string(),
    );
    config.insert(
        "transaction_rate_per_chain".into(),
        configuration.transaction_rate_per_chain.to_string(),
    );
    config.insert(
        "maximum_rpc_in_flight_per_chain".into(),
        configuration.maximum_rpc_in_flight_per_chain.to_string(),
    );
    config.insert("seed".into(), configuration.seed.to_string());
    config.insert("failure_policy".into(), configuration.failure_policy.clone());
    for chain in &configuration.chains {
        let prefix = format!("chain.{}", chain.name);
        config.insert(format!("{prefix}.network"), chain.network.clone());
        config.insert(format!("{prefix}.chain_id"), chain.chain_id.to_string());
        config.insert(format!("{prefix}.workload"), chain.workload.clone());
    }
    config
}

const fn usize_to_u64(value: usize) -> u64 {
    if value > u64::MAX as usize {
        u64::MAX
    } else {
        value as u64
    }
}

struct ScenarioClickHouseRows {
    run: ClickHouseRunRow,
    scenario: ScenarioRunRow,
    steps: Vec<ScenarioStepRow>,
}

#[derive(Debug, Serialize)]
struct ClickHouseRunRow {
    run_id: uuid::Uuid,
    started_at: u64,
    finished_at: u64,
    scenario_name: String,
    platform: String,
    mode: &'static str,
    git_sha: String,
    git_ref: String,
    config: BTreeMap<String, String>,
    metadata: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct ScenarioRunRow {
    run_id: uuid::Uuid,
    report_version: u32,
    requested_journeys: Option<u64>,
    started_journeys: u64,
    completed_journeys: u64,
    failed_journeys: u64,
    timed_out_journeys: u64,
    elapsed_ms: u64,
    completed_journeys_per_second: f64,
    maximum_in_flight: u64,
    latency_samples: u64,
    latency_min_ms: f64,
    latency_mean_ms: f64,
    latency_p50_ms: f64,
    latency_p95_ms: f64,
    latency_p99_ms: f64,
    latency_max_ms: f64,
}

#[derive(Debug, Serialize)]
struct ScenarioStepRow {
    run_id: uuid::Uuid,
    step_index: u64,
    step_name: String,
    chain: String,
    kind: String,
    success: u64,
    failed: u64,
    latency_samples: u64,
    latency_min_ms: f64,
    latency_mean_ms: f64,
    latency_p50_ms: f64,
    latency_p95_ms: f64,
    latency_p99_ms: f64,
    latency_max_ms: f64,
    source_file: Option<String>,
    fragment: Option<String>,
    instance_alias: Option<String>,
    local_step_name: Option<String>,
    local_step_index: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::{
        ChainReportConfig, LatencyDistribution, ScenarioReportConfig, StepProvenance, StepReport,
    };

    fn sample_report() -> ScenarioReport {
        ScenarioReport {
            version: 1,
            run_id: uuid::Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap(),
            scenario: "zones-roundtrip".into(),
            configuration: ScenarioReportConfig {
                chains: vec![ChainReportConfig {
                    name: "zone".into(),
                    network: "tempo".into(),
                    chain_id: 4242,
                    workload: "/workloads/zone.yml".into(),
                }],
                requested_instances: Some(100),
                run_duration_ms: None,
                starts_per_second: 10.0,
                maximum_in_flight: 20,
                default_step_timeout_ms: 5_000,
                transaction_rate_per_chain: 0,
                maximum_rpc_in_flight_per_chain: 100,
                seed: 7,
                failure_policy: "continue".into(),
            },
            started_at_unix_ms: 1_000,
            finished_at_unix_ms: 3_000,
            elapsed_ms: 2_000,
            started: 100,
            completed: 98,
            failed: 2,
            timed_out: 1,
            completed_scenarios_per_second: 49.0,
            maximum_in_flight: 18,
            steps: vec![StepReport {
                index: 0,
                name: "deposit.submit".into(),
                chain: "zone".into(),
                kind: "submit".into(),
                provenance: Some(StepProvenance {
                    source_file: "fragments/deposit.yml".into(),
                    fragment: "deposit".into(),
                    instance_alias: "deposit".into(),
                    local_step_name: "submit".into(),
                    local_step_index: 0,
                }),
                success: 98,
                failed: 2,
                latency: LatencyDistribution {
                    samples: 100,
                    min_ms: 1.0,
                    max_ms: 8.0,
                    mean_ms: 3.0,
                    p50_ms: 2.0,
                    p95_ms: 6.0,
                    p99_ms: 7.0,
                },
            }],
            total_scenario_latency: LatencyDistribution {
                samples: 98,
                min_ms: 10.0,
                max_ms: 80.0,
                mean_ms: 30.0,
                p50_ms: 20.0,
                p95_ms: 60.0,
                p99_ms: 70.0,
            },
            receipt_metrics: Vec::new(),
            receipt_records: Vec::new(),
            failures: Vec::new(),
            sampled_instances: Vec::new(),
        }
    }

    #[test]
    fn serializes_common_aggregate_and_step_rows_with_one_run_id() {
        let reporter = ScenarioClickHouseReporter {
            client: ClickHouseClient::new("http://localhost:8123", "benchmarks", None, None)
                .unwrap(),
            endpoint: "http://localhost:8123".into(),
            platform: "tempo".into(),
            git_sha: "abc123".into(),
            git_ref: "main".into(),
            metadata: BTreeMap::from([("phase".into(), "nightly".into())]),
        };
        let report = sample_report();
        let rows = reporter.build_rows(&report);
        let common = serde_json::to_value(&rows.run).unwrap();
        let aggregate = serde_json::to_value(&rows.scenario).unwrap();
        let step = serde_json::to_value(&rows.steps[0]).unwrap();

        for row in [&common, &aggregate, &step] {
            assert_eq!(row["run_id"], report.run_id.to_string());
        }
        assert_eq!(common["scenario_name"], "zones-roundtrip");
        assert_eq!(common["started_at"], 1_000);
        assert_eq!(common["finished_at"], 3_000);
        assert_eq!(common["platform"], "tempo");
        assert_eq!(common["mode"], "scenario");
        assert_eq!(common["git_sha"], "abc123");
        assert_eq!(common["git_ref"], "main");
        assert_eq!(common["config"]["chain.zone.chain_id"], "4242");
        assert_eq!(common["metadata"]["phase"], "nightly");
        assert_eq!(aggregate["report_version"], 1);
        assert_eq!(aggregate["requested_journeys"], 100);
        assert_eq!(aggregate["started_journeys"], 100);
        assert_eq!(aggregate["completed_journeys"], 98);
        assert_eq!(aggregate["failed_journeys"], 2);
        assert_eq!(aggregate["timed_out_journeys"], 1);
        assert_eq!(aggregate["elapsed_ms"], 2_000);
        assert_eq!(aggregate["completed_journeys_per_second"], 49.0);
        assert_eq!(aggregate["maximum_in_flight"], 18);
        assert_eq!(aggregate["latency_samples"], 98);
        assert_eq!(aggregate["latency_min_ms"], 10.0);
        assert_eq!(aggregate["latency_mean_ms"], 30.0);
        assert_eq!(aggregate["latency_p50_ms"], 20.0);
        assert_eq!(aggregate["latency_p95_ms"], 60.0);
        assert_eq!(aggregate["latency_p99_ms"], 70.0);
        assert_eq!(aggregate["latency_max_ms"], 80.0);
        assert_eq!(step["step_index"], 0);
        assert_eq!(step["step_name"], "deposit.submit");
        assert_eq!(step["chain"], "zone");
        assert_eq!(step["kind"], "submit");
        assert_eq!(step["success"], 98);
        assert_eq!(step["failed"], 2);
        assert_eq!(step["latency_samples"], 100);
        assert_eq!(step["latency_min_ms"], 1.0);
        assert_eq!(step["latency_mean_ms"], 3.0);
        assert_eq!(step["latency_p50_ms"], 2.0);
        assert_eq!(step["latency_p95_ms"], 6.0);
        assert_eq!(step["latency_p99_ms"], 7.0);
        assert_eq!(step["latency_max_ms"], 8.0);
        assert_eq!(step["fragment"], "deposit");
        assert_eq!(step["instance_alias"], "deposit");
        assert_eq!(step["source_file"], "fragments/deposit.yml");
        assert_eq!(step["local_step_name"], "submit");
        assert_eq!(step["local_step_index"], 0);

        let mut duration_report = report.clone();
        duration_report.configuration.requested_instances = None;
        duration_report.steps.push(StepReport {
            index: 1,
            name: "unreached".into(),
            chain: "zone".into(),
            kind: "wait_log".into(),
            provenance: None,
            success: 0,
            failed: 0,
            latency: LatencyDistribution::default(),
        });
        let duration_rows = reporter.build_rows(&duration_report);
        let aggregate = serde_json::to_value(&duration_rows.scenario).unwrap();
        let unreached = serde_json::to_value(&duration_rows.steps[1]).unwrap();
        assert!(aggregate["requested_journeys"].is_null());
        assert_eq!(unreached["latency_samples"], 0);
        assert!(unreached["fragment"].is_null());
        assert!(unreached["local_step_index"].is_null());
    }

    #[test]
    fn requires_git_metadata_and_rejects_derived_keys() {
        let missing = ScenarioClickHouseReporter::from_env(
            "http://localhost:8123",
            "tempo",
            &BTreeMap::new(),
        )
        .err()
        .unwrap()
        .to_string();
        assert!(missing.contains("git-sha"));

        let metadata = BTreeMap::from([
            ("git-sha".into(), "abc".into()),
            ("git-ref".into(), "main".into()),
            ("platform".into(), "other".into()),
        ]);
        let reserved =
            ScenarioClickHouseReporter::from_env("http://localhost:8123", "tempo", &metadata)
                .err()
                .unwrap()
                .to_string();
        assert!(reserved.contains("cannot be overridden"));
    }
}
