use std::{fmt::Debug, future::Future, path::PathBuf};

use eyre::{bail, Result, WrapErr};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{to_value, Value};

use crate::{AbiValueGenerator, FailureArtifact, FailureStage, GenerateContext, SwarmPolicy};

/// A model's predicted next state and expected execution outcome.
#[derive(Clone, Debug, Serialize)]
pub struct Prediction<S, E> {
    /// Candidate state to commit after successful verification.
    pub state: S,
    /// Model-defined expected execution outcome.
    pub expected: E,
}

/// Protocol model driven by the generic property runner.
pub trait PropertyModel: Sized {
    /// Stable model name used by registries and failure artifacts.
    const NAME: &'static str;
    /// Stable model semantics/serialization version.
    const VERSION: &'static str;

    /// Complete committed model state.
    type State: Clone + Debug + Serialize;
    /// Per-case swarm configuration.
    type Swarm: Clone + Debug + DeserializeOwned + Serialize;
    /// Selectable action family.
    type ActionKind: Clone + Debug;
    /// Concrete replayable action.
    type Action: Clone + Debug + DeserializeOwned + Serialize;
    /// Expected execution classification.
    type Expected: Clone + Debug + Serialize;
    /// Harness execution result.
    type Trace: Clone + Debug + Serialize;
    /// Model-defined observation request.
    type ObservationRequest: Clone + Debug;
    /// Observed protocol state.
    type Observation: Clone + Debug + Serialize;

    /// Return the last committed state.
    fn state(&self) -> &Self::State;

    /// Generate one swarm configuration.
    fn generate_swarm(
        &self,
        rng: &mut dyn rand::RngCore,
        policy: &SwarmPolicy,
    ) -> Result<Self::Swarm>;

    /// Return actions enabled by both the swarm and current model state.
    fn enabled_actions(&self, swarm: &Self::Swarm) -> Vec<Self::ActionKind>;

    /// Generate one concrete action.
    fn generate_action(
        &self,
        swarm: &Self::Swarm,
        kind: &Self::ActionKind,
        context: &mut GenerateContext<'_>,
    ) -> Result<Self::Action>;

    /// Predict the result without mutating committed state.
    fn predict(&self, action: &Self::Action) -> Result<Prediction<Self::State, Self::Expected>>;

    /// Describe observations required to verify one transition.
    fn transition_observation(&self, action: &Self::Action) -> Self::ObservationRequest;

    /// Verify execution and observations against a prediction, returning the
    /// reconciled state to commit. This permits RPC models to refresh fields
    /// affected by transaction fees or other observable execution metadata.
    fn verify_transition(
        &self,
        prediction: &Prediction<Self::State, Self::Expected>,
        action: &Self::Action,
        trace: &Self::Trace,
        observation: &Self::Observation,
    ) -> Result<Self::State>;

    /// Describe observations required for the final full-state check.
    fn final_observation(&self) -> Self::ObservationRequest;

    /// Verify all global invariants at the end of a case.
    fn verify_all(&self, observation: &Self::Observation) -> Result<()>;

    /// Commit a state that has passed verification.
    fn commit(&mut self, state: Self::State);
}

/// Topology/execution adapter for one model.
pub trait PropertyHarness<M: PropertyModel> {
    /// Restore a clean baseline and initialize a fresh model.
    fn reset_and_initialize(&mut self) -> impl Future<Output = Result<M>> + Send;

    /// Execute one concrete action.
    fn execute<'a>(
        &'a mut self,
        action: &'a M::Action,
    ) -> impl Future<Output = Result<M::Trace>> + Send + 'a;

    /// Resolve a model-defined observation request.
    fn observe<'a>(
        &'a mut self,
        request: &'a M::ObservationRequest,
    ) -> impl Future<Output = Result<M::Observation>> + Send + 'a;
}

/// Controls for one property run.
#[derive(Clone, Debug)]
pub struct RunConfig {
    /// Number of independent cases.
    pub cases: u64,
    /// Maximum generated actions per case.
    pub max_steps: usize,
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
            max_steps,
            swarm: SwarmPolicy::default(),
            seed: rand::rng().random(),
            failure_directory: None,
        }
    }

    /// Construct a deterministic configuration for debugging and tests.
    pub fn seeded(cases: u64, max_steps: usize, seed: u64) -> Self {
        Self { cases, max_steps, swarm: SwarmPolicy::default(), seed, failure_directory: None }
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
    /// Successfully verified transitions.
    pub completed_steps: u64,
}

/// Result of a property run.
#[derive(Clone, Debug)]
pub struct RunResult {
    /// Run counters up to success or failure.
    pub report: RunReport,
    /// First model failure, if any.
    pub failure: Option<FailureArtifact>,
    /// Written failure path when a directory was configured.
    pub failure_path: Option<PathBuf>,
}

/// Run independent swarm-generated cases until completion or the first model failure.
pub async fn run<M, H>(harness: &mut H, config: RunConfig) -> Result<RunResult>
where
    M: PropertyModel,
    H: PropertyHarness<M>,
{
    config.validate()?;
    let mut rng = StdRng::seed_from_u64(config.seed);
    let mut abi = AbiValueGenerator::default();
    let mut report = RunReport { seed: config.seed, ..RunReport::default() };

    for case_index in 0..config.cases {
        let mut model = harness
            .reset_and_initialize()
            .await
            .wrap_err_with(|| format!("failed to initialize property case {case_index}"))?;
        let swarm = executable_swarm(&model, &config.swarm, &mut rng)?;
        let target_steps = rng.random_range(1..=config.max_steps);
        let mut actions = Vec::with_capacity(target_steps);

        for step_index in 0..target_steps {
            let enabled = model.enabled_actions(&swarm);
            if enabled.is_empty() {
                break;
            }
            let kind = enabled[rng.random_range(0..enabled.len())].clone();
            let action = model.generate_action(
                &swarm,
                &kind,
                &mut GenerateContext { rng: &mut rng, abi: &mut abi, case_index, step_index },
            )?;
            let prediction = model.predict(&action)?;
            let committed_state = to_json(model.state(), "committed model state")?;
            actions.push(to_json(&action, "property action")?);
            let trace = harness.execute(&action).await?;
            let request = model.transition_observation(&action);
            let observation = harness.observe(&request).await?;

            let reconciled_state =
                match model.verify_transition(&prediction, &action, &trace, &observation) {
                    Ok(state) => state,
                    Err(error) => {
                        let failure = FailureArtifact {
                            model: M::NAME.to_string(),
                            model_version: M::VERSION.to_string(),
                            seed: config.seed,
                            case_index,
                            step_index: Some(step_index),
                            stage: FailureStage::Transition,
                            error: format!("{error:#}"),
                            swarm: to_json(&swarm, "property swarm")?,
                            actions,
                            committed_state,
                            predicted_state: Some(to_json(
                                &prediction.state,
                                "predicted model state",
                            )?),
                            expected: Some(to_json(&prediction.expected, "expected outcome")?),
                            trace: Some(to_json(&trace, "execution trace")?),
                            observation: to_json(&observation, "transition observation")?,
                        };
                        return finish_failure(
                            report,
                            failure,
                            config.failure_directory.as_deref(),
                        );
                    }
                };

            model.commit(reconciled_state);
            report.completed_steps += 1;
        }

        let observation = harness.observe(&model.final_observation()).await?;
        if let Err(error) = model.verify_all(&observation) {
            let failure = FailureArtifact {
                model: M::NAME.to_string(),
                model_version: M::VERSION.to_string(),
                seed: config.seed,
                case_index,
                step_index: None,
                stage: FailureStage::FinalVerification,
                error: format!("{error:#}"),
                swarm: to_json(&swarm, "property swarm")?,
                actions,
                committed_state: to_json(model.state(), "committed model state")?,
                predicted_state: None,
                expected: None,
                trace: None,
                observation: to_json(&observation, "final observation")?,
            };
            return finish_failure(report, failure, config.failure_directory.as_deref());
        }

        report.completed_cases += 1;
    }

    Ok(RunResult { report, failure: None, failure_path: None })
}

fn executable_swarm<M: PropertyModel>(
    model: &M,
    policy: &SwarmPolicy,
    rng: &mut dyn rand::RngCore,
) -> Result<M::Swarm> {
    for _ in 0..policy.max_resamples {
        let swarm = model.generate_swarm(rng, policy)?;
        if !model.enabled_actions(&swarm).is_empty() {
            return Ok(swarm);
        }
    }
    bail!(
        "model '{}' did not generate an initially executable swarm in {} attempts",
        M::NAME,
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
