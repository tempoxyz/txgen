use bench_core::{compute_latency_stats, ReceiptMetricGroup};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioReport {
    pub version: u32,
    pub scenario: String,
    pub configuration: ScenarioReportConfig,
    pub started_at_unix_ms: u64,
    pub finished_at_unix_ms: u64,
    pub elapsed_ms: u64,
    pub started: u64,
    pub completed: u64,
    pub failed: u64,
    pub timed_out: u64,
    pub completed_scenarios_per_second: f64,
    pub maximum_in_flight: usize,
    pub steps: Vec<StepReport>,
    pub total_scenario_latency: LatencyDistribution,
    /// Receipt-derived gas metrics grouped by chain, workload input, and scenario step.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub receipt_metrics: Vec<ReceiptMetricGroup>,
    pub failures: Vec<FailureReport>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sampled_instances: Vec<InstanceLifecycle>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioReportConfig {
    pub chains: Vec<ChainReportConfig>,
    pub requested_instances: Option<u64>,
    pub run_duration_ms: Option<u64>,
    pub starts_per_second: f64,
    pub maximum_in_flight: usize,
    pub default_step_timeout_ms: u64,
    pub transaction_rate_per_chain: u64,
    pub maximum_rpc_in_flight_per_chain: usize,
    pub seed: u64,
    pub failure_policy: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChainReportConfig {
    pub name: String,
    pub network: String,
    pub chain_id: u64,
    pub workload: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StepReport {
    pub index: usize,
    pub name: String,
    pub kind: String,
    pub success: u64,
    pub failed: u64,
    pub latency: LatencyDistribution,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct LatencyDistribution {
    pub samples: usize,
    pub min_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

impl LatencyDistribution {
    pub fn from_samples(samples: &[Duration]) -> Self {
        if samples.is_empty() {
            return Self::default();
        }
        let stats = compute_latency_stats(samples);
        Self {
            samples: samples.len(),
            min_ms: duration_ms_f64(stats.min),
            max_ms: duration_ms_f64(stats.max),
            mean_ms: duration_ms_f64(stats.mean),
            p50_ms: duration_ms_f64(stats.p50),
            p95_ms: duration_ms_f64(stats.p95),
            p99_ms: duration_ms_f64(stats.p99),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FailureReport {
    pub step_index: usize,
    pub step_name: String,
    pub classification: String,
    pub count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_detail: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InstanceLifecycle {
    pub instance: u64,
    pub started_at_unix_ms: u64,
    pub finished_at_unix_ms: u64,
    pub elapsed_ms: u64,
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_step: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_classification: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_detail: Option<String>,
    pub steps: Vec<LifecycleStep>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LifecycleStep {
    pub index: usize,
    pub name: String,
    pub kind: String,
    pub success: bool,
    pub latency_ms: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct InstanceOutcome {
    pub instance: u64,
    pub started_at_unix_ms: u64,
    pub finished_at_unix_ms: u64,
    pub elapsed: Duration,
    pub steps: Vec<StepOutcome>,
    pub failure: Option<InstanceFailure>,
}

#[derive(Debug, Clone)]
pub(crate) struct StepOutcome {
    pub index: usize,
    pub name: String,
    pub kind: String,
    pub success: bool,
    pub latency: Duration,
}

#[derive(Debug, Clone)]
pub(crate) struct InstanceFailure {
    pub step_index: usize,
    pub step_name: String,
    pub classification: String,
    pub timed_out: bool,
    pub detail: Option<String>,
}

const MAX_LATENCY_SAMPLES: usize = 65_536;

#[derive(Default)]
struct LatencyAccumulator {
    observed: usize,
    minimum: Option<Duration>,
    maximum: Option<Duration>,
    total_ms: f64,
    reservoir: Vec<Duration>,
}

impl LatencyAccumulator {
    fn record(&mut self, value: Duration) {
        self.observed = self.observed.saturating_add(1);
        self.minimum = Some(self.minimum.map_or(value, |current| current.min(value)));
        self.maximum = Some(self.maximum.map_or(value, |current| current.max(value)));
        self.total_ms += duration_ms_f64(value);

        if self.reservoir.len() < MAX_LATENCY_SAMPLES {
            self.reservoir.push(value);
            return;
        }
        let slot = reservoir_hash(self.observed as u64) % self.observed as u64;
        if slot < MAX_LATENCY_SAMPLES as u64 {
            self.reservoir[slot as usize] = value;
        }
    }

    fn distribution(&self) -> LatencyDistribution {
        if self.observed == 0 {
            return LatencyDistribution::default();
        }
        let sampled = compute_latency_stats(&self.reservoir);
        LatencyDistribution {
            samples: self.observed,
            min_ms: duration_ms_f64(self.minimum.expect("observed latency has a minimum")),
            max_ms: duration_ms_f64(self.maximum.expect("observed latency has a maximum")),
            mean_ms: self.total_ms / self.observed as f64,
            p50_ms: duration_ms_f64(sampled.p50),
            p95_ms: duration_ms_f64(sampled.p95),
            p99_ms: duration_ms_f64(sampled.p99),
        }
    }
}

fn reservoir_hash(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// Bounded-memory aggregation of completed instance outcomes.
pub(crate) struct ScenarioAccumulator {
    completed: u64,
    failed: u64,
    timed_out: u64,
    step_samples: Vec<LatencyAccumulator>,
    step_success: Vec<u64>,
    step_failed: Vec<u64>,
    total_scenario_latency: LatencyAccumulator,
    failure_counts: BTreeMap<(usize, String, String), (u64, Option<String>)>,
    sampled_instances: BTreeMap<u64, InstanceLifecycle>,
    sample_limit: usize,
}

impl ScenarioAccumulator {
    pub(crate) fn new(step_count: usize, sample_limit: usize) -> Self {
        Self {
            completed: 0,
            failed: 0,
            timed_out: 0,
            step_samples: (0..step_count).map(|_| LatencyAccumulator::default()).collect(),
            step_success: vec![0; step_count],
            step_failed: vec![0; step_count],
            total_scenario_latency: LatencyAccumulator::default(),
            failure_counts: BTreeMap::new(),
            sampled_instances: BTreeMap::new(),
            sample_limit,
        }
    }

    pub(crate) fn record(&mut self, outcome: InstanceOutcome) {
        if outcome.failure.is_none() {
            self.completed = self.completed.saturating_add(1);
            self.total_scenario_latency.record(outcome.elapsed);
        } else {
            self.failed = self.failed.saturating_add(1);
        }
        if outcome.failure.as_ref().is_some_and(|failure| failure.timed_out) {
            self.timed_out = self.timed_out.saturating_add(1);
        }

        for step in &outcome.steps {
            if let Some(samples) = self.step_samples.get_mut(step.index) {
                samples.record(step.latency);
                if step.success {
                    self.step_success[step.index] = self.step_success[step.index].saturating_add(1);
                } else {
                    self.step_failed[step.index] = self.step_failed[step.index].saturating_add(1);
                }
            }
        }
        if let Some(failure) = &outcome.failure {
            let entry = self
                .failure_counts
                .entry((
                    failure.step_index,
                    failure.step_name.clone(),
                    failure.classification.clone(),
                ))
                .or_insert_with(|| (0, failure.detail.clone()));
            entry.0 = entry.0.saturating_add(1);
        }

        if self.sample_limit > 0 {
            let instance = outcome.instance;
            if self.sampled_instances.len() < self.sample_limit ||
                self.sampled_instances
                    .last_key_value()
                    .is_some_and(|(largest, _)| instance < *largest)
            {
                self.sampled_instances.insert(instance, InstanceLifecycle::from(&outcome));
                while self.sampled_instances.len() > self.sample_limit {
                    self.sampled_instances.pop_last();
                }
            }
        }
    }

    pub(crate) fn counts(&self) -> (u64, u64, u64) {
        (self.completed, self.failed, self.timed_out)
    }
}

impl ScenarioReport {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build(
        scenario: String,
        configuration: ScenarioReportConfig,
        started_at: SystemTime,
        finished_at: SystemTime,
        elapsed: Duration,
        started: u64,
        maximum_in_flight: usize,
        step_definitions: &[(String, String)],
        receipt_metrics: Vec<ReceiptMetricGroup>,
        accumulator: ScenarioAccumulator,
    ) -> Self {
        let ScenarioAccumulator {
            completed,
            failed,
            timed_out,
            step_samples,
            step_success,
            step_failed,
            total_scenario_latency,
            failure_counts,
            sampled_instances,
            sample_limit: _,
        } = accumulator;
        let steps = step_definitions
            .iter()
            .enumerate()
            .map(|(index, (name, kind))| StepReport {
                index,
                name: name.clone(),
                kind: kind.clone(),
                success: step_success[index],
                failed: step_failed[index],
                latency: step_samples[index].distribution(),
            })
            .collect();
        let failures = failure_counts
            .into_iter()
            .map(|((step_index, step_name, classification), (count, sample_detail))| {
                FailureReport { step_index, step_name, classification, count, sample_detail }
            })
            .collect();
        let sampled_instances = sampled_instances.into_values().collect();
        let elapsed_seconds = elapsed.as_secs_f64();

        Self {
            version: 1,
            scenario,
            configuration,
            started_at_unix_ms: unix_ms(started_at),
            finished_at_unix_ms: unix_ms(finished_at),
            elapsed_ms: duration_ms(elapsed),
            started,
            completed,
            failed,
            timed_out,
            completed_scenarios_per_second: if elapsed_seconds > 0.0 {
                completed as f64 / elapsed_seconds
            } else {
                0.0
            },
            maximum_in_flight,
            steps,
            total_scenario_latency: total_scenario_latency.distribution(),
            receipt_metrics,
            failures,
            sampled_instances,
        }
    }
}

impl From<&InstanceOutcome> for InstanceLifecycle {
    fn from(outcome: &InstanceOutcome) -> Self {
        let failure = outcome.failure.as_ref();
        Self {
            instance: outcome.instance,
            started_at_unix_ms: outcome.started_at_unix_ms,
            finished_at_unix_ms: outcome.finished_at_unix_ms,
            elapsed_ms: duration_ms(outcome.elapsed),
            outcome: if failure.is_some() { "failed" } else { "completed" }.to_string(),
            failure_step: failure.map(|failure| failure.step_index),
            failure_classification: failure.map(|failure| failure.classification.clone()),
            failure_detail: failure.and_then(|failure| failure.detail.clone()),
            steps: outcome
                .steps
                .iter()
                .map(|step| LifecycleStep {
                    index: step.index,
                    name: step.name.clone(),
                    kind: step.kind.clone(),
                    success: step.success,
                    latency_ms: duration_ms_f64(step.latency),
                })
                .collect(),
        }
    }
}

pub(crate) fn unix_ms(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn duration_ms_f64(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::U256;
    use bench_core::{ReceiptGasSample, ReceiptMetricsAccumulator};

    #[test]
    fn report_counts_journey_success_and_timeouts() {
        let now = SystemTime::now();
        let outcomes = vec![
            InstanceOutcome {
                instance: 0,
                started_at_unix_ms: 1,
                finished_at_unix_ms: 2,
                elapsed: Duration::from_millis(10),
                steps: vec![StepOutcome {
                    index: 0,
                    name: "send".into(),
                    kind: "submit".into(),
                    success: true,
                    latency: Duration::from_millis(2),
                }],
                failure: None,
            },
            InstanceOutcome {
                instance: 1,
                started_at_unix_ms: 1,
                finished_at_unix_ms: 3,
                elapsed: Duration::from_millis(20),
                steps: vec![StepOutcome {
                    index: 0,
                    name: "send".into(),
                    kind: "submit".into(),
                    success: false,
                    latency: Duration::from_millis(5),
                }],
                failure: Some(InstanceFailure {
                    step_index: 0,
                    step_name: "send".into(),
                    classification: "timeout".into(),
                    timed_out: true,
                    detail: Some("step timeout elapsed".into()),
                }),
            },
        ];
        let mut accumulator = ScenarioAccumulator::new(1, 1);
        for outcome in outcomes {
            accumulator.record(outcome);
        }
        let mut receipt_metrics = ReceiptMetricsAccumulator::default();
        receipt_metrics.record(
            BTreeMap::from([
                ("chain".to_string(), "zone".to_string()),
                ("input".to_string(), "transfer".to_string()),
                ("step".to_string(), "send".to_string()),
            ]),
            ReceiptGasSample {
                gas_used: U256::from(21_000),
                effective_gas_price: Some(U256::from(2)),
            },
        );
        let report = ScenarioReport::build(
            "roundtrip".into(),
            ScenarioReportConfig {
                chains: Vec::new(),
                requested_instances: Some(2),
                run_duration_ms: None,
                starts_per_second: 0.0,
                maximum_in_flight: 2,
                default_step_timeout_ms: 1_000,
                transaction_rate_per_chain: 0,
                maximum_rpc_in_flight_per_chain: 2,
                seed: 1,
                failure_policy: "continue".into(),
            },
            now,
            now,
            Duration::from_secs(1),
            2,
            2,
            &[("send".into(), "submit".into())],
            receipt_metrics.into_metrics(),
            accumulator,
        );
        assert_eq!(report.completed, 1);
        assert_eq!(report.failed, 1);
        assert_eq!(report.timed_out, 1);
        assert_eq!(report.steps[0].success, 1);
        assert_eq!(report.steps[0].failed, 1);
        assert_eq!(report.sampled_instances.len(), 1);
        assert_eq!(report.total_scenario_latency.samples, 1);
        let serialized = serde_json::to_value(&report).unwrap();
        assert_eq!(serialized["receipt_metrics"][0]["labels"]["step"], "send");
        assert_eq!(serialized["receipt_metrics"][0]["fee_paid"]["p99"], 42_000.0);
    }

    #[test]
    fn latency_aggregation_bounds_reservoir_memory() {
        let mut accumulator = LatencyAccumulator::default();
        for index in 0..(MAX_LATENCY_SAMPLES + 100) {
            accumulator.record(Duration::from_nanos(index as u64 + 1));
        }
        let distribution = accumulator.distribution();
        assert_eq!(distribution.samples, MAX_LATENCY_SAMPLES + 100);
        assert_eq!(accumulator.reservoir.len(), MAX_LATENCY_SAMPLES);
        assert!((distribution.min_ms - 0.000_001).abs() < f64::EPSILON);
    }
}
