use std::{fmt::Debug, future::Future, path::PathBuf};

use eyre::{bail, Result, WrapErr};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{to_value, Value};

use crate::{
    AbiValueGenerator, ActionArtifact, FailureArtifact, GenerateContext, SwarmPolicy,
    VerificationTrigger,
};

/// Stateless action generation for one live property campaign.
///
/// A workload generator owns only randomized action construction. It does not
/// predict transaction outcomes or maintain a parallel copy of protocol state.
pub trait WorkloadGenerator: Sized {
    /// Stable campaign name used by registries and failure artifacts.
    const NAME: &'static str;
    /// Stable campaign semantics/serialization version.
    const VERSION: &'static str;

    /// Per-case swarm configuration.
    type Swarm: Clone + Debug + DeserializeOwned + Serialize;
    /// Selectable action family.
    type ActionKind: Clone + Debug;
    /// Concrete replayable action.
    type Action: Clone + Debug + DeserializeOwned + Serialize;

    /// Generate one swarm configuration.
    fn generate_swarm(
        &self,
        rng: &mut dyn rand::RngCore,
        policy: &SwarmPolicy,
    ) -> Result<Self::Swarm>;

    /// Return action families enabled by the generated swarm.
    fn enabled_actions(&self, swarm: &Self::Swarm) -> Vec<Self::ActionKind>;

    /// Generate one concrete action using ABI-shaped randomized values.
    fn generate_action(
        &self,
        swarm: &Self::Swarm,
        kind: &Self::ActionKind,
        context: &mut GenerateContext<'_>,
    ) -> Result<Self::Action>;
}

/// Live execution, lifecycle correlation, and independent verification boundary.
pub trait CampaignHarness<W: WorkloadGenerator> {
    /// Receipt or execution result returned for an action.
    type Trace: Clone + Debug + Serialize;
    /// Actual correlated events proving a terminal lifecycle state.
    type TerminalEvidence: Clone + Debug + Serialize;
    /// Complete independent invariant report.
    type Verification: Clone + Debug + Serialize;

    /// Prepare a new case. This may reset an isolated topology or attach to the
    /// current live topology, but it does not construct protocol model state.
    fn reset_case(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Submit one concrete action and return its actual execution evidence.
    fn execute<'a>(
        &'a mut self,
        action: &'a W::Action,
    ) -> impl Future<Output = Result<Self::Trace>> + Send + 'a;

    /// Correlate the action with its terminal cross-layer lifecycle events.
    /// Return `None` when the action has no terminal lifecycle transition.
    fn await_terminal<'a>(
        &'a mut self,
        action: &'a W::Action,
        trace: &'a Self::Trace,
    ) -> impl Future<Output = Result<Option<Self::TerminalEvidence>>> + Send + 'a;

    /// Run the independent invariant verifier against chain-derived state.
    fn verify(
        &mut self,
        trigger: VerificationTrigger,
    ) -> impl Future<Output = Result<Self::Verification>> + Send;

    /// Return an invariant violation contained in a completed report.
    fn violation(&self, verification: &Self::Verification) -> Option<String>;
}

/// Controls for one property run.
#[derive(Clone, Debug)]
pub struct RunConfig {
    /// Number of independent cases.
    pub cases: u64,
    /// Keep generating cases until the first failure or process shutdown.
    pub continuous: bool,
    /// Maximum generated actions per case.
    pub max_steps: usize,
    /// Run the independent verifier every N executed actions. Zero disables
    /// interval checks; terminal and final verification remain mandatory.
    pub verify_every_steps: usize,
    /// Swarm selection policy.
    pub swarm: SwarmPolicy,
    /// Run seed. Defaults to OS-seeded randomness through [`RunConfig::random`].
    pub seed: u64,
    /// Optional failure artifact directory.
    pub failure_directory: Option<PathBuf>,
}

impl RunConfig {
    /// Construct a run configuration with an OS-random seed.
    pub fn random(cases: u64, max_steps: usize) -> Self {
        Self {
            cases,
            continuous: false,
            max_steps,
            verify_every_steps: 25,
            swarm: SwarmPolicy::default(),
            seed: rand::rng().random(),
            failure_directory: None,
        }
    }

    /// Construct a deterministic configuration for debugging and tests.
    pub fn seeded(cases: u64, max_steps: usize, seed: u64) -> Self {
        Self {
            cases,
            continuous: false,
            max_steps,
            verify_every_steps: 25,
            swarm: SwarmPolicy::default(),
            seed,
            failure_directory: None,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.cases == 0 {
            bail!("property cases must be greater than zero");
        }
        if self.max_steps == 0 {
            bail!("property max_steps must be greater than zero");
        }
        self.swarm.validate()?;
        Ok(())
    }
}

/// Successful-run counters.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RunReport {
    /// Seed used by the run.
    pub seed: u64,
    /// Fully completed cases.
    pub completed_cases: u64,
    /// Executed actions that reached the verification loop.
    pub completed_steps: u64,
    /// Independent invariant checks completed successfully.
    pub completed_verifications: u64,
}

/// Result of a property run.
#[derive(Clone, Debug)]
pub struct RunResult {
    /// Run counters up to success or failure.
    pub report: RunReport,
    /// First invariant failure, if any.
    pub failure: Option<FailureArtifact>,
    /// Written failure path when a directory was configured.
    pub failure_path: Option<PathBuf>,
}

/// Run independent swarm-generated cases until completion or the first invariant failure.
pub async fn run<W, H>(workload: &W, harness: &mut H, config: RunConfig) -> Result<RunResult>
where
    W: WorkloadGenerator,
    H: CampaignHarness<W>,
{
    config.validate()?;
    let mut rng = StdRng::seed_from_u64(config.seed);
    let mut abi = AbiValueGenerator::default();
    let mut report = RunReport { seed: config.seed, ..RunReport::default() };

    let case_limit = if config.continuous { u64::MAX } else { config.cases };
    for case_index in 0..case_limit {
        harness
            .reset_case()
            .await
            .wrap_err_with(|| format!("failed to initialize property case {case_index}"))?;
        let swarm = executable_swarm(workload, &config.swarm, &mut rng)?;
        let target_steps = rng.random_range(1..=config.max_steps);
        let mut actions = Vec::with_capacity(target_steps);

        for step_index in 0..target_steps {
            let enabled = workload.enabled_actions(&swarm);
            if enabled.is_empty() {
                break;
            }
            let kind = enabled[rng.random_range(0..enabled.len())].clone();
            let action = workload.generate_action(
                &swarm,
                &kind,
                &mut GenerateContext { rng: &mut rng, abi: &mut abi, case_index, step_index },
            )?;
            let trace = harness.execute(&action).await?;
            let terminal = harness.await_terminal(&action, &trace).await?;
            actions.push(ActionArtifact {
                action: to_json(&action, "property action")?,
                trace: to_json(&trace, "execution trace")?,
                terminal_evidence: terminal
                    .as_ref()
                    .map(|value| to_json(value, "terminal lifecycle evidence"))
                    .transpose()?,
            });
            report.completed_steps += 1;

            if terminal.is_some() {
                if let Some(result) = verify(
                    workload,
                    harness,
                    &config,
                    &mut report,
                    case_index,
                    Some(step_index),
                    VerificationTrigger::TerminalTransition,
                    &swarm,
                    &actions,
                )
                .await?
                {
                    return Ok(result);
                }
            }

            let executed = step_index + 1;
            if config.verify_every_steps != 0 && executed % config.verify_every_steps == 0 {
                if let Some(result) = verify(
                    workload,
                    harness,
                    &config,
                    &mut report,
                    case_index,
                    Some(step_index),
                    VerificationTrigger::Periodic,
                    &swarm,
                    &actions,
                )
                .await?
                {
                    return Ok(result);
                }
            }
        }

        if let Some(result) = verify(
            workload,
            harness,
            &config,
            &mut report,
            case_index,
            None,
            VerificationTrigger::Final,
            &swarm,
            &actions,
        )
        .await?
        {
            return Ok(result);
        }
        report.completed_cases += 1;
    }

    Ok(RunResult { report, failure: None, failure_path: None })
}

#[allow(clippy::too_many_arguments)]
async fn verify<W, H>(
    _workload: &W,
    harness: &mut H,
    config: &RunConfig,
    report: &mut RunReport,
    case_index: u64,
    step_index: Option<usize>,
    trigger: VerificationTrigger,
    swarm: &W::Swarm,
    actions: &[ActionArtifact],
) -> Result<Option<RunResult>>
where
    W: WorkloadGenerator,
    H: CampaignHarness<W>,
{
    let verification = harness.verify(trigger).await?;
    if let Some(error) = harness.violation(&verification) {
        let failure = FailureArtifact {
            campaign: W::NAME.to_string(),
            campaign_version: W::VERSION.to_string(),
            seed: config.seed,
            case_index,
            step_index,
            trigger,
            error,
            swarm: to_json(swarm, "property swarm")?,
            actions: actions.to_vec(),
            verification: to_json(&verification, "independent verification report")?,
        };
        return finish_failure(report.clone(), failure, config.failure_directory.as_deref())
            .map(Some);
    }
    report.completed_verifications += 1;
    Ok(None)
}

fn executable_swarm<W: WorkloadGenerator>(
    workload: &W,
    policy: &SwarmPolicy,
    rng: &mut dyn rand::RngCore,
) -> Result<W::Swarm> {
    for _ in 0..policy.max_resamples {
        let swarm = workload.generate_swarm(rng, policy)?;
        if !workload.enabled_actions(&swarm).is_empty() {
            return Ok(swarm);
        }
    }
    bail!(
        "campaign '{}' did not generate an executable swarm in {} attempts",
        W::NAME,
        policy.max_resamples
    )
}

fn finish_failure(
    report: RunReport,
    failure: FailureArtifact,
    directory: Option<&std::path::Path>,
) -> Result<RunResult> {
    let failure_path = directory.map(|path| failure.write_yaml(path)).transpose()?;
    Ok(RunResult { report, failure: Some(failure), failure_path })
}

fn to_json(value: &impl Serialize, label: &str) -> Result<Value> {
    to_value(value).wrap_err_with(|| format!("failed to serialize {label}"))
}
