use super::schema::StepProvenance;
use alloy_primitives::B256;
use bench_core::{compute_latency_stats, ReceiptGasRecord, ReceiptMetricGroup};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioReport {
    pub version: u32,
    /// Identifier shared by every output sink for this scenario run.
    pub run_id: uuid::Uuid,
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
    /// Backward-compatible client-observed end-to-end journey duration.
    pub total_scenario_latency: LatencyDistribution,
    /// Client wall time from instance start until every required branch completes.
    pub client_observed_e2e_latency: LatencyDistribution,
    /// Longest observed dependency path, calculated independently per instance.
    pub observed_critical_path_latency: LatencyDistribution,
    /// Protocol and dependency-edge timings derived from completed instance traces.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub causal_edges: Vec<CausalEdgeReport>,
    /// Receipt-derived gas metrics grouped by chain, workload input, and scenario step.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub receipt_metrics: Vec<ReceiptMetricGroup>,
    /// Per-transaction receipt gas records retained for detail reporters.
    #[serde(skip)]
    pub receipt_records: Vec<ReceiptGasRecord>,
    pub failures: Vec<FailureReport>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sampled_instances: Vec<InstanceLifecycle>,
}

type StepDefinition = (String, String, String, String, Vec<String>, Option<StepProvenance>);

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
    pub observation_mode: String,
    pub observation_poll_interval_ms: u64,
    pub subscription_configured: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct StepReport {
    pub index: usize,
    pub id: String,
    pub name: String,
    pub chain: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provenance: Option<StepProvenance>,
    pub success: u64,
    pub failed: u64,
    /// Backward-compatible alias for `command_latency`.
    pub latency: LatencyDistribution,
    /// Time spent executing the local step command, not protocol latency.
    pub command_latency: LatencyDistribution,
}

/// Aggregate timing for an actual scenario dependency or submit/observation relation.
#[derive(Debug, Clone, Serialize)]
pub struct CausalEdgeReport {
    pub relation: String,
    pub source_step_id: String,
    pub destination_step_id: String,
    pub source_milestone: String,
    pub destination_milestone: String,
    pub observed_latency: LatencyDistribution,
    pub chain_timestamp_delta: LatencyDistribution,
    pub destination_observation_lag: LatencyDistribution,
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

    pub fn from_millisecond_samples(samples: &[f64]) -> Self {
        if samples.is_empty() {
            return Self::default();
        }
        let mut sorted = samples.to_vec();
        sorted.sort_by(f64::total_cmp);
        let percentile = |percent: usize| {
            let index = (sorted.len() * percent / 100).min(sorted.len() - 1);
            sorted[index]
        };
        Self {
            samples: sorted.len(),
            min_ms: sorted[0],
            max_ms: sorted[sorted.len() - 1],
            mean_ms: sorted.iter().sum::<f64>() / sorted.len() as f64,
            p50_ms: percentile(50),
            p95_ms: percentile(95),
            p99_ms: percentile(99),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FailureReport {
    pub step_index: usize,
    pub step_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provenance: Option<StepProvenance>,
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
    pub client_observed_e2e_latency_ms: f64,
    pub observed_critical_path_latency_ms: f64,
    pub critical_path_step_ids: Vec<String>,
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_step: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_provenance: Option<StepProvenance>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_classification: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_detail: Option<String>,
    pub steps: Vec<LifecycleStep>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LifecycleStep {
    pub index: usize,
    pub id: String,
    pub name: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provenance: Option<StepProvenance>,
    pub success: bool,
    pub started_offset_ms: u64,
    pub finished_offset_ms: u64,
    /// Backward-compatible alias for `command_latency_ms`.
    pub latency_ms: f64,
    /// Local command duration. Protocol timings are in `milestones` and `causal_edges`.
    pub command_latency_ms: f64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub milestones: Vec<ProtocolMilestone>,
}

/// Secret-free, per-instance protocol observation retained for sampled traces.
#[derive(Debug, Clone, Serialize)]
pub struct ProtocolMilestone {
    pub kind: String,
    pub chain: String,
    pub run_offset_ms: u64,
    pub wall_time_unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub submitted_at_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accepted_at_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_observed_at_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_hash: Option<B256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_hash: Option<B256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_index: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_index: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canonical_block_timestamp_ms: Option<u64>,
    pub confirmation_depth: u64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub event_names: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub log_indices: Vec<u64>,
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
    pub id: String,
    pub name: String,
    pub kind: String,
    pub depends_on: Vec<String>,
    pub provenance: Option<StepProvenance>,
    pub success: bool,
    pub latency: Duration,
    pub started_offset_ms: u64,
    pub finished_offset_ms: u64,
    pub milestones: Vec<ProtocolMilestone>,
}

#[derive(Debug, Clone)]
pub(crate) struct InstanceFailure {
    pub step_index: usize,
    pub step_name: String,
    pub failure_provenance: Option<StepProvenance>,
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

#[derive(Default)]
struct MillisecondAccumulator {
    observed: usize,
    minimum: Option<f64>,
    maximum: Option<f64>,
    total: f64,
    reservoir: Vec<f64>,
}

impl MillisecondAccumulator {
    fn record(&mut self, value: f64) {
        if !value.is_finite() {
            return;
        }
        self.observed = self.observed.saturating_add(1);
        self.minimum = Some(self.minimum.map_or(value, |current| current.min(value)));
        self.maximum = Some(self.maximum.map_or(value, |current| current.max(value)));
        self.total += value;
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
        let sampled = LatencyDistribution::from_millisecond_samples(&self.reservoir);
        LatencyDistribution {
            samples: self.observed,
            min_ms: self.minimum.expect("observed value has a minimum"),
            max_ms: self.maximum.expect("observed value has a maximum"),
            mean_ms: self.total / self.observed as f64,
            p50_ms: sampled.p50_ms,
            p95_ms: sampled.p95_ms,
            p99_ms: sampled.p99_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CausalEdgeKey {
    relation: String,
    source_step_id: String,
    destination_step_id: String,
    source_milestone: String,
    destination_milestone: String,
}

#[derive(Default)]
struct CausalEdgeAccumulator {
    observed_latency: MillisecondAccumulator,
    chain_timestamp_delta: MillisecondAccumulator,
    destination_observation_lag: MillisecondAccumulator,
}

fn reservoir_hash(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

type FailureKey = (usize, String, String);
type FailureAggregate = (u64, Option<String>, Option<StepProvenance>);

/// Bounded-memory aggregation of completed instance outcomes.
pub(crate) struct ScenarioAccumulator {
    completed: u64,
    failed: u64,
    timed_out: u64,
    step_samples: Vec<LatencyAccumulator>,
    step_success: Vec<u64>,
    step_failed: Vec<u64>,
    total_scenario_latency: LatencyAccumulator,
    critical_path_latency: MillisecondAccumulator,
    causal_edges: BTreeMap<CausalEdgeKey, CausalEdgeAccumulator>,
    failure_counts: BTreeMap<FailureKey, FailureAggregate>,
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
            critical_path_latency: MillisecondAccumulator::default(),
            causal_edges: BTreeMap::new(),
            failure_counts: BTreeMap::new(),
            sampled_instances: BTreeMap::new(),
            sample_limit,
        }
    }

    pub(crate) fn record(&mut self, outcome: InstanceOutcome) {
        if outcome.failure.is_none() {
            self.completed = self.completed.saturating_add(1);
            self.total_scenario_latency.record(outcome.elapsed);
            let (_, critical_path_latency_ms) = observed_critical_path(&outcome.steps);
            self.critical_path_latency.record(critical_path_latency_ms);
            record_causal_edges(&mut self.causal_edges, &outcome.steps);
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
                .or_insert_with(|| (0, failure.detail.clone(), failure.failure_provenance.clone()));
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

struct SelectedMilestone<'a> {
    kind: &'a str,
    run_offset_ms: u64,
    wall_time_unix_ms: Option<u64>,
    chain_timestamp_ms: Option<u64>,
}

fn primary_milestone(step: &StepOutcome) -> SelectedMilestone<'_> {
    if let Some(milestone) = step.milestones.last() {
        SelectedMilestone {
            kind: &milestone.kind,
            run_offset_ms: milestone.run_offset_ms,
            wall_time_unix_ms: Some(
                milestone.first_observed_at_unix_ms.unwrap_or(milestone.wall_time_unix_ms),
            ),
            chain_timestamp_ms: milestone.canonical_block_timestamp_ms,
        }
    } else {
        SelectedMilestone {
            kind: "command_complete",
            run_offset_ms: step.finished_offset_ms,
            wall_time_unix_ms: None,
            chain_timestamp_ms: None,
        }
    }
}

#[derive(Clone)]
struct CriticalPathState {
    ids: Vec<String>,
    dag_started_offset_ms: u64,
    predecessor_finished_offset_ms: u64,
    finished_offset_ms: u64,
}

impl CriticalPathState {
    fn latency_ms(&self) -> f64 {
        self.finished_offset_ms.saturating_sub(self.dag_started_offset_ms) as f64
    }
}

fn observed_critical_path(steps: &[StepOutcome]) -> (Vec<String>, f64) {
    if steps.is_empty() {
        return (Vec::new(), 0.0);
    }
    let by_id = steps.iter().map(|step| (step.id.as_str(), step)).collect::<BTreeMap<_, _>>();
    let dag_started_offset_ms = steps
        .iter()
        .filter(|step| step.depends_on.is_empty())
        .map(|step| step.started_offset_ms)
        .min()
        .unwrap_or_else(|| {
            steps.iter().map(|step| step.started_offset_ms).min().unwrap_or_default()
        });
    let mut memo = BTreeMap::<String, CriticalPathState>::new();

    fn visit(
        id: &str,
        by_id: &BTreeMap<&str, &StepOutcome>,
        memo: &mut BTreeMap<String, CriticalPathState>,
        dag_started_offset_ms: u64,
    ) -> Option<CriticalPathState> {
        if let Some(state) = memo.get(id) {
            return Some(state.clone());
        }
        let step = *by_id.get(id)?;
        // Dependency readiness is controlled by command completion, which can
        // be later than a receipt or log's first-observed milestone when the
        // step waits for confirmations.
        let finish = step.finished_offset_ms;
        let mut state = if step.depends_on.is_empty() {
            CriticalPathState {
                ids: vec![step.id.clone()],
                dag_started_offset_ms,
                predecessor_finished_offset_ms: step.started_offset_ms,
                finished_offset_ms: finish,
            }
        } else {
            let mut candidates = step
                .depends_on
                .iter()
                .filter_map(|dependency| visit(dependency, by_id, memo, dag_started_offset_ms))
                .collect::<Vec<_>>();
            candidates.sort_by(|left, right| {
                left.finished_offset_ms
                    .cmp(&right.finished_offset_ms)
                    .then_with(|| right.ids.cmp(&left.ids))
            });
            let mut selected = candidates.pop()?;
            let predecessor_finished_offset_ms = selected.finished_offset_ms;
            selected.ids.push(step.id.clone());
            selected.predecessor_finished_offset_ms = predecessor_finished_offset_ms;
            selected.finished_offset_ms = finish;
            selected
        };
        state.finished_offset_ms =
            state.finished_offset_ms.max(state.predecessor_finished_offset_ms);
        memo.insert(id.to_string(), state.clone());
        Some(state)
    }

    let dependencies =
        steps.iter().flat_map(|step| step.depends_on.iter().cloned()).collect::<BTreeSet<_>>();
    let mut terminals = steps
        .iter()
        .filter(|step| !dependencies.contains(&step.id))
        .filter_map(|step| visit(&step.id, &by_id, &mut memo, dag_started_offset_ms))
        .collect::<Vec<_>>();
    terminals.sort_by(|left, right| {
        left.finished_offset_ms
            .cmp(&right.finished_offset_ms)
            .then_with(|| right.ids.cmp(&left.ids))
    });
    terminals
        .pop()
        .map(|state| {
            let latency_ms = state.latency_ms();
            (state.ids, latency_ms)
        })
        .unwrap_or_default()
}

fn record_causal_edges(
    aggregates: &mut BTreeMap<CausalEdgeKey, CausalEdgeAccumulator>,
    steps: &[StepOutcome],
) {
    let by_id = steps.iter().map(|step| (step.id.as_str(), step)).collect::<BTreeMap<_, _>>();
    for destination in steps {
        let destination_milestone = primary_milestone(destination);
        for source_id in &destination.depends_on {
            let Some(source) = by_id.get(source_id.as_str()) else { continue };
            let source_milestone = primary_milestone(source);
            record_causal_edge(
                aggregates,
                CausalEdgeKey {
                    relation: "dependency".to_string(),
                    source_step_id: source.id.clone(),
                    destination_step_id: destination.id.clone(),
                    source_milestone: source_milestone.kind.to_string(),
                    destination_milestone: destination_milestone.kind.to_string(),
                },
                &source_milestone,
                &destination_milestone,
            );
        }
    }

    let mut submissions = BTreeMap::<(String, B256), (&StepOutcome, &ProtocolMilestone)>::new();
    for step in steps {
        for milestone in &step.milestones {
            if milestone.kind != "submit" {
                continue;
            }
            let Some(hash) = milestone.transaction_hash else { continue };
            submissions.entry((milestone.chain.clone(), hash)).or_insert((step, milestone));
        }
    }

    for step in steps {
        for milestone in &step.milestones {
            if milestone.kind == "submit" {
                continue;
            }
            let Some(hash) = milestone.transaction_hash else { continue };
            let Some((source_step, source)) = submissions.get(&(milestone.chain.clone(), hash))
            else {
                continue;
            };
            let submitted_run_offset_ms = source
                .submitted_at_unix_ms
                .zip(source.accepted_at_unix_ms)
                .map(|(submitted, accepted)| {
                    source.run_offset_ms.saturating_sub(accepted.saturating_sub(submitted))
                })
                .unwrap_or(source.run_offset_ms);
            let source_selected = SelectedMilestone {
                kind: &source.kind,
                run_offset_ms: submitted_run_offset_ms,
                wall_time_unix_ms: source.submitted_at_unix_ms.or(Some(source.wall_time_unix_ms)),
                chain_timestamp_ms: source.canonical_block_timestamp_ms,
            };
            let destination_selected = SelectedMilestone {
                kind: &milestone.kind,
                run_offset_ms: milestone.run_offset_ms,
                wall_time_unix_ms: Some(
                    milestone.first_observed_at_unix_ms.unwrap_or(milestone.wall_time_unix_ms),
                ),
                chain_timestamp_ms: milestone.canonical_block_timestamp_ms,
            };
            record_causal_edge(
                aggregates,
                CausalEdgeKey {
                    relation: "submit_to_observation".to_string(),
                    source_step_id: source_step.id.clone(),
                    destination_step_id: step.id.clone(),
                    source_milestone: source.kind.clone(),
                    destination_milestone: milestone.kind.clone(),
                },
                &source_selected,
                &destination_selected,
            );
        }
    }
}

fn record_causal_edge(
    aggregates: &mut BTreeMap<CausalEdgeKey, CausalEdgeAccumulator>,
    key: CausalEdgeKey,
    source: &SelectedMilestone<'_>,
    destination: &SelectedMilestone<'_>,
) {
    let aggregate = aggregates.entry(key).or_default();
    aggregate
        .observed_latency
        .record(destination.run_offset_ms as f64 - source.run_offset_ms as f64);
    if let (Some(source), Some(destination)) =
        (source.chain_timestamp_ms, destination.chain_timestamp_ms)
    {
        aggregate.chain_timestamp_delta.record(destination as f64 - source as f64);
    }
    if let (Some(observed), Some(included)) =
        (destination.wall_time_unix_ms, destination.chain_timestamp_ms)
    {
        aggregate.destination_observation_lag.record(observed as f64 - included as f64);
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
        step_definitions: &[StepDefinition],
        receipt_metrics: Vec<ReceiptMetricGroup>,
        receipt_records: Vec<ReceiptGasRecord>,
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
            critical_path_latency,
            causal_edges,
            failure_counts,
            sampled_instances,
            sample_limit: _,
        } = accumulator;
        let steps = step_definitions
            .iter()
            .enumerate()
            .map(|(index, (id, name, chain, kind, depends_on, provenance))| StepReport {
                index,
                id: id.clone(),
                name: name.clone(),
                chain: chain.clone(),
                kind: kind.clone(),
                depends_on: depends_on.clone(),
                provenance: provenance.clone(),
                success: step_success[index],
                failed: step_failed[index],
                latency: step_samples[index].distribution(),
                command_latency: step_samples[index].distribution(),
            })
            .collect();
        let failures = failure_counts
            .into_iter()
            .map(|((step_index, step_name, classification), (count, sample_detail, provenance))| {
                FailureReport {
                    step_index,
                    step_name,
                    provenance,
                    classification,
                    count,
                    sample_detail,
                }
            })
            .collect();
        let sampled_instances = sampled_instances.into_values().collect();
        let client_observed_e2e_latency = total_scenario_latency.distribution();
        let observed_critical_path_latency = critical_path_latency.distribution();
        let causal_edges = causal_edges
            .into_iter()
            .map(|(key, aggregate)| CausalEdgeReport {
                relation: key.relation,
                source_step_id: key.source_step_id,
                destination_step_id: key.destination_step_id,
                source_milestone: key.source_milestone,
                destination_milestone: key.destination_milestone,
                observed_latency: aggregate.observed_latency.distribution(),
                chain_timestamp_delta: aggregate.chain_timestamp_delta.distribution(),
                destination_observation_lag: aggregate.destination_observation_lag.distribution(),
            })
            .collect();
        let elapsed_seconds = elapsed.as_secs_f64();

        Self {
            version: 2,
            run_id: uuid::Uuid::new_v4(),
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
            total_scenario_latency: client_observed_e2e_latency.clone(),
            client_observed_e2e_latency,
            observed_critical_path_latency,
            causal_edges,
            receipt_metrics,
            receipt_records,
            failures,
            sampled_instances,
        }
    }
}

impl From<&InstanceOutcome> for InstanceLifecycle {
    fn from(outcome: &InstanceOutcome) -> Self {
        let failure = outcome.failure.as_ref();
        let (critical_path_step_ids, observed_critical_path_latency_ms) =
            observed_critical_path(&outcome.steps);
        Self {
            instance: outcome.instance,
            started_at_unix_ms: outcome.started_at_unix_ms,
            finished_at_unix_ms: outcome.finished_at_unix_ms,
            elapsed_ms: duration_ms(outcome.elapsed),
            client_observed_e2e_latency_ms: duration_ms_f64(outcome.elapsed),
            observed_critical_path_latency_ms,
            critical_path_step_ids,
            outcome: if failure.is_some() { "failed" } else { "completed" }.to_string(),
            failure_step: failure.map(|failure| failure.step_index),
            failure_provenance: failure.and_then(|failure| failure.failure_provenance.clone()),
            failure_classification: failure.map(|failure| failure.classification.clone()),
            failure_detail: failure.and_then(|failure| failure.detail.clone()),
            steps: outcome
                .steps
                .iter()
                .map(|step| LifecycleStep {
                    index: step.index,
                    id: step.id.clone(),
                    name: step.name.clone(),
                    kind: step.kind.clone(),
                    depends_on: step.depends_on.clone(),
                    provenance: step.provenance.clone(),
                    success: step.success,
                    started_offset_ms: step.started_offset_ms,
                    finished_offset_ms: step.finished_offset_ms,
                    latency_ms: duration_ms_f64(step.latency),
                    command_latency_ms: duration_ms_f64(step.latency),
                    milestones: step.milestones.clone(),
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
                    id: "send".into(),
                    name: "send".into(),
                    kind: "submit".into(),
                    depends_on: Vec::new(),
                    provenance: None,
                    success: true,
                    latency: Duration::from_millis(2),
                    started_offset_ms: 0,
                    finished_offset_ms: 2,
                    milestones: Vec::new(),
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
                    id: "send".into(),
                    name: "send".into(),
                    kind: "submit".into(),
                    depends_on: Vec::new(),
                    provenance: None,
                    success: false,
                    latency: Duration::from_millis(5),
                    started_offset_ms: 0,
                    finished_offset_ms: 5,
                    milestones: Vec::new(),
                }],
                failure: Some(InstanceFailure {
                    step_index: 0,
                    step_name: "send".into(),
                    failure_provenance: None,
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
            &[("send".into(), "send".into(), "primary".into(), "submit".into(), Vec::new(), None)],
            receipt_metrics.into_metrics(),
            Vec::new(),
            accumulator,
        );
        assert_eq!(report.completed, 1);
        assert_eq!(report.failed, 1);
        assert_eq!(report.timed_out, 1);
        assert_eq!(report.steps[0].success, 1);
        assert_eq!(report.steps[0].failed, 1);
        assert_eq!(report.sampled_instances.len(), 1);
        assert_eq!(report.total_scenario_latency.samples, 1);
        assert!(!report.run_id.is_nil());
        assert_eq!(report.steps[0].chain, "primary");

        let serialized = serde_json::to_value(&report).unwrap();
        assert_eq!(serialized["run_id"], report.run_id.to_string());
        assert!(serialized.get("receipt_records").is_none());
        assert_eq!(serialized["steps"][0]["chain"], "primary");
        assert_eq!(serialized["receipt_metrics"][0]["labels"]["step"], "send");
        assert_eq!(serialized["receipt_metrics"][0]["fee_paid"]["p99"], 42_000.0);
        assert!(serialized["steps"][0].get("provenance").is_none());
        assert!(serialized["failures"][0].get("provenance").is_none());
        assert!(serialized["sampled_instances"][0].get("failure_provenance").is_none());
        assert!(serialized["sampled_instances"][0]["steps"][0].get("provenance").is_none());
        assert_eq!(
            serialized["sampled_instances"][0]["steps"][0]["command_latency_ms"],
            serialized["sampled_instances"][0]["steps"][0]["latency_ms"]
        );
    }

    #[test]
    fn report_retains_expanded_step_provenance() {
        let now = SystemTime::now();
        let spec = super::super::schema::ScenarioSpec::parse(
            r#"
version: 1
chains:
  primary: { network: tempo, rpc_url: http://primary.invalid, workload: ./workload.yml }
fragments:
  capture:
    outputs: { cursor: checkpoint }
    steps:
      - checkpoint: { chain: primary }
        save: cursor
scenario:
  name: composed
  steps:
    - { use: capture, as: first }
    - { use: capture, as: second }
"#,
        )
        .unwrap();
        let first_provenance = spec.scenario.steps[0].provenance.clone().unwrap();
        assert_eq!(first_provenance.instance_alias, "first");
        let provenance = spec.scenario.steps[1].provenance.clone().unwrap();
        let outcome = InstanceOutcome {
            instance: 0,
            started_at_unix_ms: 1,
            finished_at_unix_ms: 2,
            elapsed: Duration::from_millis(10),
            steps: vec![
                StepOutcome {
                    index: 0,
                    id: "first.cursor".into(),
                    name: "first.cursor".into(),
                    kind: "checkpoint".into(),
                    depends_on: Vec::new(),
                    provenance: Some(first_provenance.clone()),
                    success: true,
                    latency: Duration::from_millis(1),
                    started_offset_ms: 0,
                    finished_offset_ms: 1,
                    milestones: Vec::new(),
                },
                StepOutcome {
                    index: 1,
                    id: "second.cursor".into(),
                    name: "second.cursor".into(),
                    kind: "checkpoint".into(),
                    depends_on: vec!["first.cursor".into()],
                    provenance: Some(provenance.clone()),
                    success: false,
                    latency: Duration::from_millis(2),
                    started_offset_ms: 1,
                    finished_offset_ms: 3,
                    milestones: Vec::new(),
                },
            ],
            failure: Some(InstanceFailure {
                step_index: 1,
                step_name: "second.cursor".into(),
                failure_provenance: Some(provenance.clone()),
                classification: "timeout".into(),
                timed_out: true,
                detail: Some("step timeout elapsed".into()),
            }),
        };
        let mut accumulator = ScenarioAccumulator::new(2, 1);
        accumulator.record(outcome);
        let report = ScenarioReport::build(
            "composed".into(),
            ScenarioReportConfig {
                chains: Vec::new(),
                requested_instances: Some(1),
                run_duration_ms: None,
                starts_per_second: 0.0,
                maximum_in_flight: 1,
                default_step_timeout_ms: 1_000,
                transaction_rate_per_chain: 0,
                maximum_rpc_in_flight_per_chain: 1,
                seed: 1,
                failure_policy: "continue".into(),
            },
            now,
            now,
            Duration::from_secs(1),
            1,
            1,
            &[
                (
                    "first.cursor".into(),
                    "first.cursor".into(),
                    "primary".into(),
                    "checkpoint".into(),
                    Vec::new(),
                    Some(first_provenance),
                ),
                (
                    "second.cursor".into(),
                    "second.cursor".into(),
                    "primary".into(),
                    "checkpoint".into(),
                    vec!["first.cursor".into()],
                    Some(provenance),
                ),
            ],
            Vec::new(),
            Vec::new(),
            accumulator,
        );

        let serialized = serde_json::to_value(&report).unwrap();
        assert_eq!(serialized["run_id"], report.run_id.to_string());
        assert_eq!(serialized["steps"][0]["chain"], "primary");
        assert_eq!(serialized["steps"][1]["chain"], "primary");
        for value in [
            &serialized["steps"][1]["provenance"],
            &serialized["failures"][0]["provenance"],
            &serialized["sampled_instances"][0]["failure_provenance"],
            &serialized["sampled_instances"][0]["steps"][1]["provenance"],
        ] {
            assert_eq!(value["source_file"], "<inline>");
            assert_eq!(value["fragment"], "capture");
            assert_eq!(value["instance_alias"], "second");
            assert_eq!(value["local_step_name"], "cursor");
            assert_eq!(value["local_step_index"], 0);
        }
    }

    #[test]
    fn binding_failure_does_not_inherit_first_step_provenance() {
        let now = SystemTime::now();
        let provenance = StepProvenance {
            source_file: "fragments/transfers.yml".into(),
            fragment: "submit-and-confirm".into(),
            instance_alias: "first_transfer".into(),
            local_step_name: "submission".into(),
            local_step_index: 0,
        };
        let outcome = InstanceOutcome {
            instance: 0,
            started_at_unix_ms: 1,
            finished_at_unix_ms: 2,
            elapsed: Duration::from_millis(1),
            steps: Vec::new(),
            failure: Some(InstanceFailure {
                step_index: 0,
                step_name: "bindings".into(),
                failure_provenance: None,
                classification: "binding_error".into(),
                timed_out: false,
                detail: Some("account lease pool closed".into()),
            }),
        };
        let mut accumulator = ScenarioAccumulator::new(1, 1);
        accumulator.record(outcome);
        let report = ScenarioReport::build(
            "composed".into(),
            ScenarioReportConfig {
                chains: Vec::new(),
                requested_instances: Some(1),
                run_duration_ms: None,
                starts_per_second: 0.0,
                maximum_in_flight: 1,
                default_step_timeout_ms: 1_000,
                transaction_rate_per_chain: 0,
                maximum_rpc_in_flight_per_chain: 1,
                seed: 1,
                failure_policy: "continue".into(),
            },
            now,
            now,
            Duration::from_secs(1),
            1,
            1,
            &[(
                "first_transfer.submission".into(),
                "first_transfer.submission".into(),
                "primary".into(),
                "submit".into(),
                Vec::new(),
                Some(provenance),
            )],
            Vec::new(),
            Vec::new(),
            accumulator,
        );

        let serialized = serde_json::to_value(&report).unwrap();
        assert!(serialized["failures"][0].get("provenance").is_none());
        assert!(serialized["sampled_instances"][0].get("failure_provenance").is_none());
    }

    #[test]
    fn reports_longest_observed_branch_and_dependency_edges() {
        let now = SystemTime::now();
        let steps = vec![
            StepOutcome {
                index: 0,
                id: "root".into(),
                name: "root".into(),
                kind: "checkpoint".into(),
                depends_on: Vec::new(),
                provenance: None,
                success: true,
                latency: Duration::from_millis(10),
                started_offset_ms: 0,
                finished_offset_ms: 10,
                milestones: Vec::new(),
            },
            StepOutcome {
                index: 1,
                id: "fast".into(),
                name: "fast".into(),
                kind: "delay".into(),
                depends_on: vec!["root".into()],
                provenance: None,
                success: true,
                latency: Duration::from_millis(10),
                started_offset_ms: 10,
                finished_offset_ms: 20,
                milestones: Vec::new(),
            },
            StepOutcome {
                index: 2,
                id: "slow".into(),
                name: "slow".into(),
                kind: "delay".into(),
                depends_on: vec!["root".into()],
                provenance: None,
                success: true,
                latency: Duration::from_millis(30),
                started_offset_ms: 10,
                finished_offset_ms: 40,
                milestones: Vec::new(),
            },
            StepOutcome {
                index: 3,
                id: "join".into(),
                name: "join".into(),
                kind: "checkpoint".into(),
                depends_on: vec!["fast".into(), "slow".into()],
                provenance: None,
                success: true,
                latency: Duration::from_millis(10),
                started_offset_ms: 40,
                finished_offset_ms: 50,
                milestones: Vec::new(),
            },
        ];
        let mut accumulator = ScenarioAccumulator::new(steps.len(), 1);
        accumulator.record(InstanceOutcome {
            instance: 0,
            started_at_unix_ms: 1,
            finished_at_unix_ms: 51,
            elapsed: Duration::from_millis(50),
            steps,
            failure: None,
        });
        let report = ScenarioReport::build(
            "two-branch-join".into(),
            ScenarioReportConfig {
                chains: Vec::new(),
                requested_instances: Some(1),
                run_duration_ms: None,
                starts_per_second: 0.0,
                maximum_in_flight: 1,
                default_step_timeout_ms: 1_000,
                transaction_rate_per_chain: 0,
                maximum_rpc_in_flight_per_chain: 1,
                seed: 1,
                failure_policy: "continue".into(),
            },
            now,
            now,
            Duration::from_millis(50),
            1,
            1,
            &[
                (
                    "root".into(),
                    "root".into(),
                    "primary".into(),
                    "checkpoint".into(),
                    Vec::new(),
                    None,
                ),
                (
                    "fast".into(),
                    "fast".into(),
                    "primary".into(),
                    "delay".into(),
                    vec!["root".into()],
                    None,
                ),
                (
                    "slow".into(),
                    "slow".into(),
                    "primary".into(),
                    "delay".into(),
                    vec!["root".into()],
                    None,
                ),
                (
                    "join".into(),
                    "join".into(),
                    "primary".into(),
                    "checkpoint".into(),
                    vec!["fast".into(), "slow".into()],
                    None,
                ),
            ],
            Vec::new(),
            Vec::new(),
            accumulator,
        );

        assert_eq!(report.observed_critical_path_latency.samples, 1);
        assert_eq!(report.observed_critical_path_latency.p50_ms, 50.0);
        assert_eq!(report.sampled_instances[0].critical_path_step_ids, ["root", "slow", "join"]);
        assert_eq!(report.causal_edges.len(), 4);
        let slow_to_join = report
            .causal_edges
            .iter()
            .find(|edge| {
                edge.relation == "dependency" &&
                    edge.source_step_id == "slow" &&
                    edge.destination_step_id == "join"
            })
            .unwrap();
        assert_eq!(slow_to_join.observed_latency.p50_ms, 10.0);
    }

    #[test]
    fn critical_path_uses_step_completion_after_first_observation() {
        let milestone = |run_offset_ms| ProtocolMilestone {
            kind: "receipt".into(),
            chain: "primary".into(),
            run_offset_ms,
            wall_time_unix_ms: run_offset_ms,
            submitted_at_unix_ms: None,
            accepted_at_unix_ms: None,
            first_observed_at_unix_ms: Some(run_offset_ms),
            transaction_hash: None,
            block_number: Some(1),
            block_hash: None,
            transaction_index: None,
            log_index: None,
            canonical_block_timestamp_ms: None,
            confirmation_depth: 1,
            event_names: Vec::new(),
            log_indices: Vec::new(),
        };
        let steps = vec![
            StepOutcome {
                index: 0,
                id: "confirmed_slow".into(),
                name: "confirmed_slow".into(),
                kind: "wait_receipt".into(),
                depends_on: Vec::new(),
                provenance: None,
                success: true,
                latency: Duration::from_millis(100),
                started_offset_ms: 0,
                finished_offset_ms: 100,
                milestones: vec![milestone(10)],
            },
            StepOutcome {
                index: 1,
                id: "fast".into(),
                name: "fast".into(),
                kind: "wait_receipt".into(),
                depends_on: Vec::new(),
                provenance: None,
                success: true,
                latency: Duration::from_millis(60),
                started_offset_ms: 0,
                finished_offset_ms: 60,
                milestones: vec![milestone(50)],
            },
            StepOutcome {
                index: 2,
                id: "join".into(),
                name: "join".into(),
                kind: "wait_receipt".into(),
                depends_on: vec!["confirmed_slow".into(), "fast".into()],
                provenance: None,
                success: true,
                latency: Duration::from_millis(50),
                started_offset_ms: 100,
                finished_offset_ms: 150,
                milestones: vec![milestone(105)],
            },
        ];

        let (ids, latency_ms) = observed_critical_path(&steps);
        assert_eq!(ids, ["confirmed_slow", "join"]);
        assert_eq!(latency_ms, 150.0);
    }

    #[test]
    fn critical_path_uses_shared_origin_and_latest_terminal() {
        let steps = vec![
            StepOutcome {
                index: 0,
                id: "early_terminal".into(),
                name: "early_terminal".into(),
                kind: "checkpoint".into(),
                depends_on: Vec::new(),
                provenance: None,
                success: true,
                latency: Duration::from_millis(100),
                started_offset_ms: 0,
                finished_offset_ms: 100,
                milestones: Vec::new(),
            },
            StepOutcome {
                index: 1,
                id: "late_root".into(),
                name: "late_root".into(),
                kind: "checkpoint".into(),
                depends_on: Vec::new(),
                provenance: None,
                success: true,
                latency: Duration::from_millis(20),
                started_offset_ms: 90,
                finished_offset_ms: 110,
                milestones: Vec::new(),
            },
            StepOutcome {
                index: 2,
                id: "late_terminal".into(),
                name: "late_terminal".into(),
                kind: "checkpoint".into(),
                depends_on: vec!["late_root".into()],
                provenance: None,
                success: true,
                latency: Duration::from_millis(10),
                started_offset_ms: 110,
                finished_offset_ms: 120,
                milestones: Vec::new(),
            },
        ];

        let (ids, latency_ms) = observed_critical_path(&steps);
        assert_eq!(ids, ["late_root", "late_terminal"]);
        assert_eq!(latency_ms, 120.0);
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
