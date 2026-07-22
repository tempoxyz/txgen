use super::{
    error::StepError,
    report::{
        unix_ms, ChainReportConfig, InstanceFailure, InstanceOutcome, ScenarioAccumulator,
        ScenarioReport, ScenarioReportConfig, StepOutcome,
    },
    schema::{
        AccountSelection, BindingDef, ChainDef, ChainId, ScenarioSpec, StepAction, StepDef,
        SubmitAwait, SubmitStep,
    },
    value::{
        collect_variable_paths, eval_expression, materialize_yaml, RuntimeContext, RuntimeValue,
    },
    wait::{self, DEFAULT_POLL_INTERVAL},
};
use crate::{
    materialize_and_sign_template, materialize_setup_online, MaterializedSetup, MaterializedTx,
    NetworkAdapter, ScenarioActionContext,
};
use alloy_consensus::{SignableTransaction, Signed};
use alloy_dyn_abi::{DynSolType, DynSolValue};
use alloy_eips::{eip2718::Encodable2718, BlockNumberOrTag};
use alloy_network::{
    primitives::{BlockResponse, HeaderResponse},
    AnyNetwork, Network,
};
use alloy_primitives::{keccak256, Address, TxHash, B256, U256};
use alloy_provider::{DynProvider, Provider, ProviderBuilder};
use bench_core::{
    ReceiptCollector, ReceiptCollectorHandle, ReceiptMetricGroup, ReceiptMetricLabels,
    RequestAuthProvider, RpcEndpoint, RpcSubmission, RpcSubmitFailureKind, RpcSubmitter,
    SenderConfig, SenderHeaderAuthProvider,
};
use eyre::{bail, Result, WrapErr};
use rand::{rngs::StdRng, Rng, RngCore, SeedableRng};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::{Duration, Instant, SystemTime},
};
use tokio::{
    sync::{Mutex, MutexGuard, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::Instant as TokioInstant,
};
use txgen_core::{
    merge_yaml, AccountManager, AddressPoolManager, ArtifactManager, BuildContext, NonceTracker,
    SignerExt, TxPhase, WorkloadSpec,
};

const FALLBACK_STEP_TIMEOUT: Duration = Duration::from_secs(300);

/// Failure behavior after one scenario instance fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailurePolicy {
    /// Stop starting new instances; allow already-started instances to finish.
    FailFast,
    /// Continue starting instances until the configured count or duration ends.
    Continue,
}

impl FailurePolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FailFast => "fail-fast",
            Self::Continue => "continue",
        }
    }
}

/// Programmatic controls for one scenario run.
#[derive(Debug, Clone)]
pub struct ScenarioExecutionConfig {
    /// Maximum number of journeys to start. `None` means duration-only.
    pub count: Option<u64>,
    /// Window during which new journeys may start.
    pub duration: Option<Duration>,
    /// New journeys started per second. Zero means unlimited.
    pub starts_per_second: f64,
    /// Maximum concurrently active journeys, including account-lease waiters.
    pub max_in_flight: usize,
    /// Default used only when a step has no explicit timeout.
    pub step_timeout: Option<Duration>,
    /// Deterministic run seed.
    pub seed: u64,
    /// Whether a failure stops future starts.
    pub failure_policy: FailurePolicy,
    /// Per-chain transaction submissions per second. Zero means unlimited.
    pub transaction_rate: u64,
    /// Maximum in-flight RPC submissions per chain.
    pub max_rpc_in_flight: usize,
    /// Number of individual lifecycle records included in the report.
    pub sample_instances: usize,
}

impl Default for ScenarioExecutionConfig {
    fn default() -> Self {
        Self {
            count: Some(1),
            duration: None,
            starts_per_second: 0.0,
            max_in_flight: 1,
            step_timeout: None,
            seed: 0,
            failure_policy: FailurePolicy::Continue,
            transaction_rate: 0,
            max_rpc_in_flight: 100,
            sample_instances: 0,
        }
    }
}

impl ScenarioExecutionConfig {
    fn validate(&self) -> Result<()> {
        if self.count == Some(0) {
            bail!("scenario count must be greater than zero");
        }
        if self.duration == Some(Duration::ZERO) {
            bail!("scenario duration must be greater than zero");
        }
        if self.count.is_none() && self.duration.is_none() {
            bail!("scenario execution requires a count or duration");
        }
        if !self.starts_per_second.is_finite() || self.starts_per_second < 0.0 {
            bail!("scenario starts per second must be finite and non-negative");
        }
        if self.max_in_flight == 0 {
            bail!("maximum in-flight scenario instances must be greater than zero");
        }
        if self.step_timeout == Some(Duration::ZERO) {
            bail!("default step timeout must be greater than zero");
        }
        if self.max_rpc_in_flight == 0 {
            bail!("maximum in-flight RPC submissions must be greater than zero");
        }
        Ok(())
    }
}

/// Execute a parsed scenario with a fresh adapter per named chain.
pub async fn execute_scenario<A>(
    spec: ScenarioSpec,
    configuration: ScenarioExecutionConfig,
) -> Result<ScenarioReport>
where
    A: NetworkAdapter + Default + Send + Sync + 'static,
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
{
    configuration.validate()?;
    spec.validate()?;
    let engine = Arc::new(ScenarioEngine::<A>::initialize(spec, configuration.clone()).await?);
    engine.run(configuration).await
}

/// Validate a resolved scenario and all local workload, template, ABI, event, and binding
/// references without contacting any RPC endpoint.
pub fn validate_scenario_offline<A: NetworkAdapter>(spec: &ScenarioSpec) -> Result<()> {
    spec.validate()?;
    load_validated_scenario_inputs::<A>(spec, 0)?;
    Ok(())
}

fn load_validated_scenario_inputs<A: NetworkAdapter>(
    spec: &ScenarioSpec,
    seed: u64,
) -> Result<(BTreeMap<String, ChainInput>, BTreeMap<String, BindingRuntime>)> {
    let chain_inputs = load_chain_inputs::<A>(spec)?;
    validate_workload_references::<A>(spec, &chain_inputs)?;
    let bindings = build_binding_runtimes(spec, &chain_inputs, seed)?;
    Ok((chain_inputs, bindings))
}

struct ScenarioEngine<A: NetworkAdapter> {
    spec: ScenarioSpec,
    chains: BTreeMap<String, Arc<ChainRuntime<A>>>,
    bindings: BTreeMap<String, BindingRuntime>,
    default_step_timeout: Duration,
}

impl<A> ScenarioEngine<A>
where
    A: NetworkAdapter + Default + Send + Sync + 'static,
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
{
    async fn initialize(spec: ScenarioSpec, config: ScenarioExecutionConfig) -> Result<Self> {
        let (mut chain_inputs, bindings) = load_validated_scenario_inputs::<A>(&spec, config.seed)?;

        let mut prepared_chains = BTreeMap::new();
        for (name, definition) in &spec.chains {
            let input = chain_inputs
                .remove(name)
                .expect("every scenario chain was loaded during preflight");
            let chain = ChainRuntime::<A>::prepare(
                name,
                definition,
                input,
                config.transaction_rate,
                config.max_rpc_in_flight,
            )
            .await
            .wrap_err_with(|| format!("failed to preflight scenario chain '{name}'"))?;
            prepared_chains.insert(name.clone(), chain);
        }

        for (name, prepared) in &prepared_chains {
            prepared
                .validate_setup_and_static_submissions(
                    &spec,
                    instance_seed(config.seed, stable_hash(name)),
                )
                .await
                .wrap_err_with(|| {
                    format!("failed to validate setup and templates for chain '{name}'")
                })?;
        }

        // All remote chain IDs and initial nonce state are checked before the
        // first setup transaction can mutate any chain, and every setup is
        // materialized once without submission to catch later-step errors.
        let mut chains = BTreeMap::new();
        for (name, prepared) in prepared_chains {
            let chain = prepared
                .initialize(
                    instance_seed(config.seed, stable_hash(&name)),
                    config.max_rpc_in_flight,
                )
                .await
                .wrap_err_with(|| format!("failed to initialize scenario chain '{name}'"))?;
            chains.insert(name, Arc::new(chain));
        }

        let default_step_timeout =
            config.step_timeout.or(spec.scenario.timeout).unwrap_or(FALLBACK_STEP_TIMEOUT);

        Ok(Self { spec, chains, bindings, default_step_timeout })
    }

    async fn run(self: Arc<Self>, config: ScenarioExecutionConfig) -> Result<ScenarioReport> {
        let started_at = SystemTime::now();
        let run_start = Instant::now();
        let mut tasks: JoinSet<InstanceOutcome> = JoinSet::new();
        let mut outcomes =
            ScenarioAccumulator::new(self.spec.scenario.steps.len(), config.sample_instances);
        let mut next_instance = 0u64;
        let mut last_instance_start = None;
        let mut in_flight = 0usize;
        let mut maximum_in_flight = 0usize;
        let mut stop_starting = false;

        loop {
            let count_available = config.count.is_none_or(|count| next_instance < count);
            let time_available =
                config.duration.is_none_or(|duration| run_start.elapsed() < duration);
            let may_start = !stop_starting && count_available && time_available;

            if may_start && in_flight < config.max_in_flight {
                if let Some(delay) = start_delay(
                    run_start,
                    last_instance_start,
                    config.starts_per_second,
                    config.duration,
                ) {
                    if in_flight == 0 {
                        tokio::time::sleep(delay).await;
                    } else {
                        tokio::select! {
                            _ = tokio::time::sleep(delay) => {}
                            joined = tasks.join_next() => {
                                if let Some(joined) = joined {
                                    let outcome = joined.wrap_err("scenario instance task failed")?;
                                    in_flight -= 1;
                                    if outcome.failure.is_some() && config.failure_policy == FailurePolicy::FailFast {
                                        stop_starting = true;
                                    }
                                    outcomes.record(outcome);
                                }
                            }
                        }
                    }
                    continue;
                }

                // Re-check the duration after a paced wait.
                if config.duration.is_some_and(|duration| run_start.elapsed() >= duration) {
                    continue;
                }
                let instance = next_instance;
                next_instance = next_instance
                    .checked_add(1)
                    .ok_or_else(|| eyre::eyre!("scenario instance counter overflowed u64"))?;
                let engine = self.clone();
                let seed = config.seed;
                tasks.spawn(async move { engine.run_instance(instance, seed).await });
                last_instance_start = Some(Instant::now());
                in_flight += 1;
                maximum_in_flight = maximum_in_flight.max(in_flight);
                continue;
            }

            if in_flight == 0 {
                break;
            }
            if let Some(joined) = tasks.join_next().await {
                let outcome = joined.wrap_err("scenario instance task failed")?;
                in_flight -= 1;
                if outcome.failure.is_some() && config.failure_policy == FailurePolicy::FailFast {
                    stop_starting = true;
                }
                outcomes.record(outcome);
            }
        }

        // Receipt draining is report finalization, not part of measured scenario execution.
        let finished_at = SystemTime::now();
        let elapsed = run_start.elapsed();
        let mut receipt_metrics = Vec::new();
        for chain in self.chains.values() {
            receipt_metrics.extend(chain.finish_receipt_metrics().await);
        }
        receipt_metrics.sort_by(|left, right| left.labels.cmp(&right.labels));
        let step_definitions = self
            .spec
            .scenario
            .steps
            .iter()
            .enumerate()
            .map(|(index, step)| {
                (
                    step_name(index, step),
                    step.action.chain().to_string(),
                    step.action.name().to_string(),
                    step.provenance.clone(),
                )
            })
            .collect::<Vec<_>>();
        let chain_configuration = self
            .chains
            .iter()
            .map(|(name, chain)| ChainReportConfig {
                name: name.clone(),
                network: A::network_name().to_string(),
                chain_id: chain.chain_id,
                workload: chain.workload_path.display().to_string(),
            })
            .collect();
        let report_configuration = ScenarioReportConfig {
            chains: chain_configuration,
            requested_instances: config.count,
            run_duration_ms: config
                .duration
                .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)),
            starts_per_second: config.starts_per_second,
            maximum_in_flight: config.max_in_flight,
            default_step_timeout_ms: u64::try_from(self.default_step_timeout.as_millis())
                .unwrap_or(u64::MAX),
            transaction_rate_per_chain: config.transaction_rate,
            maximum_rpc_in_flight_per_chain: config.max_rpc_in_flight,
            seed: config.seed,
            failure_policy: config.failure_policy.as_str().to_string(),
        };

        Ok(ScenarioReport::build(
            self.spec.scenario.name.clone(),
            report_configuration,
            started_at,
            finished_at,
            elapsed,
            next_instance,
            maximum_in_flight,
            &step_definitions,
            receipt_metrics,
            outcomes,
        ))
    }

    async fn run_instance(&self, instance: u64, run_seed: u64) -> InstanceOutcome {
        let started_at = SystemTime::now();
        let started = Instant::now();
        let mut rng = StdRng::seed_from_u64(instance_seed(run_seed, instance));
        let (mut context, _leases) = match self.bind_instance(instance, &mut rng).await {
            Ok(binding) => binding,
            Err(error) => {
                let detail = error.sanitized_detail();
                return InstanceOutcome {
                    instance,
                    started_at_unix_ms: unix_ms(started_at),
                    finished_at_unix_ms: unix_ms(SystemTime::now()),
                    elapsed: started.elapsed(),
                    steps: Vec::new(),
                    failure: Some(InstanceFailure {
                        step_index: 0,
                        step_name: "bindings".to_string(),
                        failure_provenance: None,
                        classification: error.classification.to_string(),
                        timed_out: false,
                        detail,
                    }),
                };
            }
        };
        let mut step_outcomes = Vec::new();

        for (index, step) in self.spec.scenario.steps.iter().enumerate() {
            let name = step_name(index, step);
            let kind = step.action.name().to_string();
            let step_started = Instant::now();
            let timeout = step.timeout.unwrap_or(self.default_step_timeout);
            let deadline = TokioInstant::now() + timeout;
            let result = if matches!(&step.action, StepAction::Submit(_)) {
                // Submit handles its own deadline so a timed-out RPC cannot outlive
                // the instance and release its account lease while still running.
                self.execute_step(step, &name, &context, &mut rng, deadline).await
            } else {
                match tokio::time::timeout_at(
                    deadline,
                    self.execute_step(step, &name, &context, &mut rng, deadline),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(StepError::timeout()),
                }
            };
            let latency = step_started.elapsed();

            match result {
                Ok(value) => {
                    step_outcomes.push(StepOutcome {
                        index,
                        name: name.clone(),
                        kind,
                        provenance: step.provenance.clone(),
                        success: true,
                        latency,
                    });
                    if let Some(save) = &step.save {
                        match context.with_path(save.clone(), value) {
                            Ok(next) => context = next,
                            Err(error) => {
                                return failed_outcome(
                                    instance,
                                    started_at,
                                    started,
                                    step_outcomes,
                                    index,
                                    step,
                                    StepError::new("context_error", error.to_string()),
                                );
                            }
                        }
                    }
                }
                Err(error) => {
                    step_outcomes.push(StepOutcome {
                        index,
                        name: name.clone(),
                        kind,
                        provenance: step.provenance.clone(),
                        success: false,
                        latency,
                    });
                    return failed_outcome(
                        instance,
                        started_at,
                        started,
                        step_outcomes,
                        index,
                        step,
                        error,
                    );
                }
            }
        }

        InstanceOutcome {
            instance,
            started_at_unix_ms: unix_ms(started_at),
            finished_at_unix_ms: unix_ms(SystemTime::now()),
            elapsed: started.elapsed(),
            steps: step_outcomes,
            failure: None,
        }
    }

    async fn bind_instance(
        &self,
        instance: u64,
        rng: &mut StdRng,
    ) -> Result<(RuntimeContext, Vec<OwnedSemaphorePermit>), StepError> {
        let mut selections = Vec::with_capacity(self.bindings.len());
        for (name, binding) in &self.bindings {
            let BindingRuntime::Account { selection, addresses, lease_pool, lease_slot, .. } =
                binding
            else {
                continue;
            };
            let index = match selection {
                AccountSelection::Lease => {
                    let pool = lease_pool.as_ref().expect("lease binding has pool");
                    pool.index(instance, *lease_slot)
                }
                AccountSelection::Random => rng.random_range(0..addresses.len()),
                AccountSelection::Index(index) => *index,
            };
            selections.push((name, binding, index, addresses[index]));
        }

        let mut lease_requests = selections
            .iter()
            .filter_map(|(name, binding, index, address)| {
                let BindingRuntime::Account { selection, lease_pool, .. } = binding else {
                    return None;
                };
                (*selection == AccountSelection::Lease).then_some((
                    **address,
                    *name,
                    lease_pool.as_ref().expect("lease binding has pool"),
                    *index,
                ))
            })
            .collect::<Vec<_>>();
        lease_requests.sort_by_key(|(address, _, _, _)| *address);
        if lease_requests.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(StepError::new(
                "binding_error",
                "two lease bindings selected the same account address",
            ));
        }
        let mut leases = Vec::with_capacity(lease_requests.len());
        for (_, _, pool, index) in lease_requests {
            leases.push(pool.acquire_index(index).await?);
        }

        let mut roots = BTreeMap::new();
        for (name, binding, index, address) in selections {
            let BindingRuntime::Account { pool, .. } = binding else { unreachable!() };
            roots.insert(name.clone(), account_binding_value(pool, index, address));
        }
        for (name, binding) in &self.bindings {
            if matches!(binding, BindingRuntime::Bytes32) {
                let mut bytes = [0u8; 32];
                rng.fill_bytes(&mut bytes);
                roots.insert(name.clone(), RuntimeValue::Bytes32(B256::from(bytes)));
            }
        }
        let context = RuntimeContext::new(roots)
            .map_err(|error| StepError::new("binding_error", error.to_string()))?;
        Ok((context, leases))
    }

    async fn execute_step(
        &self,
        step: &StepDef,
        step_name: &str,
        context: &RuntimeContext,
        rng: &mut StdRng,
        deadline: TokioInstant,
    ) -> Result<RuntimeValue, StepError> {
        let chain = self
            .chains
            .get(step.action.chain())
            .ok_or_else(|| StepError::new("configuration_error", "unknown chain"))?;
        match &step.action {
            StepAction::Checkpoint(_) => chain.checkpoint().await,
            StepAction::Invoke(invoke) => {
                let arguments =
                    serde_yaml::to_value(&invoke.with_value).map_err(StepError::expression)?;
                let arguments =
                    materialize_yaml(&arguments, context).map_err(StepError::expression)?;
                let output = chain
                    .adapter
                    .invoke_scenario_action(
                        &invoke.action,
                        &arguments,
                        ScenarioActionContext {
                            chain: &chain.name,
                            chain_id: chain.chain_id,
                            query_provider: &chain.query_provider,
                        },
                    )
                    .await
                    .map_err(|error| StepError::new("invoke_error", error.to_string()))?;
                let RuntimeValue::Object(mut output) = RuntimeValue::from_yaml(&output)
                    .map_err(|error| StepError::new("invoke_error", error.to_string()))?
                else {
                    return Err(StepError::new(
                        "invoke_error",
                        "scenario action returned a non-object value",
                    ));
                };
                output.insert("chain".to_string(), RuntimeValue::String(chain.name.clone()));
                output.insert("action".to_string(), RuntimeValue::String(invoke.action.clone()));
                Ok(RuntimeValue::Object(output))
            }
            StepAction::Submit(submit) => {
                let submit_rng = rng.clone();
                let (value, next_rng) = chain
                    .execute_submit(
                        step_name,
                        submit.clone(),
                        context.clone(),
                        submit_rng,
                        deadline,
                    )
                    .await?;
                *rng = next_rng;
                Ok(value)
            }
            StepAction::WaitReceipt(wait_receipt) => {
                let hash = expression_hash(&wait_receipt.transaction_hash, context)
                    .map_err(StepError::expression)?;
                let sender = wait_receipt
                    .sender
                    .as_ref()
                    .map(|value| wait::expression_address(value, context))
                    .transpose()
                    .map_err(StepError::expression)?;
                let receipt = wait::wait_for_receipt(
                    &chain.query_provider,
                    &chain.submitter,
                    &chain.name,
                    sender,
                    hash,
                    wait_receipt.poll_interval.unwrap_or(DEFAULT_POLL_INTERVAL),
                    wait_receipt.confirmations.unwrap_or(0),
                )
                .await?;
                if !receipt.status && !wait_receipt.allow_revert {
                    return Err(StepError::new("reverted_receipt", "transaction receipt reverted"));
                }
                Ok(receipt.value)
            }
            StepAction::WaitLog(wait_log) => {
                let abi = chain
                    .artifacts
                    .get(&wait_log.abi)
                    .map_err(|error| StepError::abi(error.to_string()))?;
                wait::wait_for_log(
                    &chain.query_provider,
                    &chain.submitter,
                    &chain.name,
                    abi,
                    wait_log,
                    context,
                )
                .await
            }
        }
    }
}

fn failed_outcome(
    instance: u64,
    started_at: SystemTime,
    started: Instant,
    steps: Vec<StepOutcome>,
    step_index: usize,
    step: &StepDef,
    error: StepError,
) -> InstanceOutcome {
    let detail = error.sanitized_detail();
    InstanceOutcome {
        instance,
        started_at_unix_ms: unix_ms(started_at),
        finished_at_unix_ms: unix_ms(SystemTime::now()),
        elapsed: started.elapsed(),
        steps,
        failure: Some(InstanceFailure {
            step_index,
            step_name: step_name(step_index, step),
            failure_provenance: step.provenance.clone(),
            classification: error.classification.to_string(),
            timed_out: error.classification == "timeout",
            detail,
        }),
    }
}

struct ChainRuntime<A: NetworkAdapter> {
    name: String,
    chain_id: u64,
    workload_path: PathBuf,
    spec: WorkloadSpec,
    accounts: AccountManager,
    address_pools: AddressPoolManager,
    artifacts: ArtifactManager,
    adapter: A,
    setup: MaterializedSetup,
    nonces: Mutex<NonceTracker>,
    submit_prepare_gate: Mutex<()>,
    submission_lanes: Arc<SubmissionLanes>,
    submission_ambiguous: AtomicBool,
    query_provider: DynProvider<AnyNetwork>,
    submitter: RpcSubmitter,
    receipt_collector: Mutex<Option<ReceiptCollector>>,
    receipt_collector_handle: StdMutex<Option<ReceiptCollectorHandle>>,
}

struct ChainInput {
    workload_path: PathBuf,
    submission_rpc_url: url::Url,
    query_rpc_url: url::Url,
    spec: WorkloadSpec,
    accounts: AccountManager,
    address_pools: AddressPoolManager,
    artifacts: ArtifactManager,
}

struct PreparedChain<A: NetworkAdapter> {
    name: String,
    chain_id: u64,
    query_rpc_url: String,
    workload_path: PathBuf,
    spec: WorkloadSpec,
    accounts: AccountManager,
    address_pools: AddressPoolManager,
    artifacts: ArtifactManager,
    adapter: A,
    nonces: NonceTracker,
    query_provider: DynProvider<AnyNetwork>,
    submitter: RpcSubmitter,
}

#[derive(Default)]
struct SubmissionLanes {
    active: StdMutex<BTreeSet<[u8; 20]>>,
    notify: tokio::sync::Notify,
}

impl SubmissionLanes {
    fn try_acquire(self: &Arc<Self>, keys: BTreeSet<[u8; 20]>) -> Option<SubmissionLaneGuard> {
        let mut active = self.active.lock().expect("submission lane mutex poisoned");
        if keys.iter().any(|key| active.contains(key)) {
            return None;
        }
        active.extend(keys.iter().copied());
        Some(SubmissionLaneGuard { lanes: self.clone(), keys })
    }
}

struct SubmissionLaneGuard {
    lanes: Arc<SubmissionLanes>,
    keys: BTreeSet<[u8; 20]>,
}

impl Drop for SubmissionLaneGuard {
    fn drop(&mut self) {
        let mut active = self.lanes.active.lock().expect("submission lane mutex poisoned");
        for key in &self.keys {
            active.remove(key);
        }
        drop(active);
        self.lanes.notify.notify_waiters();
    }
}

fn load_chain_inputs<A: NetworkAdapter>(
    spec: &ScenarioSpec,
) -> Result<BTreeMap<String, ChainInput>> {
    let mut inputs = BTreeMap::new();
    let mut submission_endpoints = BTreeMap::<String, String>::new();
    for (name, definition) in &spec.chains {
        if definition.network != A::network_name() {
            bail!(
                "chain '{name}' selects network '{}', but this binary provides the '{}' adapter",
                definition.network,
                A::network_name()
            );
        }
        let submission_rpc_url = parse_rpc_url(name, "rpc_url", &definition.rpc_url)?;
        let query_rpc_url = parse_rpc_url(
            name,
            "query_rpc_url",
            definition.query_rpc_url.as_deref().unwrap_or(&definition.rpc_url),
        )?;
        if let Some(existing) =
            submission_endpoints.insert(submission_rpc_url.to_string(), name.clone())
        {
            bail!(
                "chains '{existing}' and '{name}' resolve to the same submission RPC endpoint; aliases would maintain conflicting nonce state"
            );
        }
        let workload = WorkloadSpec::load(&definition.workload).wrap_err_with(|| {
            format!("failed to load workload for chain '{name}': {}", definition.workload.display())
        })?;
        let workload_base =
            definition.workload.parent().unwrap_or_else(|| std::path::Path::new("."));
        let accounts = AccountManager::from_spec(&workload.accounts)
            .wrap_err_with(|| format!("failed to load accounts for chain '{name}'"))?;
        let address_pools = AddressPoolManager::from_spec(&workload.address_pools)
            .wrap_err_with(|| format!("failed to load address pools for chain '{name}'"))?;
        let artifacts = ArtifactManager::load(&workload.artifacts, workload_base)
            .wrap_err_with(|| format!("failed to load artifacts for chain '{name}'"))?;
        inputs.insert(
            name.clone(),
            ChainInput {
                workload_path: definition.workload.clone(),
                submission_rpc_url,
                query_rpc_url,
                spec: workload,
                accounts,
                address_pools,
                artifacts,
            },
        );
    }
    Ok(inputs)
}

fn parse_rpc_url(chain: &str, field: &str, value: &str) -> Result<url::Url> {
    let mut url = url::Url::parse(value)
        .map_err(|_| eyre::eyre!("chain '{chain}' has an invalid {field}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        bail!("chain '{chain}' {field} must use http or https");
    }
    if url.fragment().is_some() {
        bail!("chain '{chain}' {field} must not include a URL fragment");
    }
    let default_port = if url.scheme() == "http" { 80 } else { 443 };
    if url.port_or_known_default() == Some(default_port) {
        url.set_port(None).map_err(|_| eyre::eyre!("chain '{chain}' has an invalid {field}"))?;
    }
    Ok(url)
}

impl<A> PreparedChain<A>
where
    A: NetworkAdapter + Default,
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
{
    async fn validate_setup_and_static_submissions(
        &self,
        scenario: &ScenarioSpec,
        setup_seed: u64,
    ) -> Result<()> {
        let mut adapter = A::default();
        let mut nonces = NonceTracker::new();
        tokio::time::timeout(
            FALLBACK_STEP_TIMEOUT,
            adapter.prepare_nonces(&self.spec, &self.accounts, &mut nonces, &self.query_rpc_url),
        )
        .await
        .map_err(|_| eyre::eyre!("setup validation nonce preparation timed out"))??;
        let mut rng = StdRng::seed_from_u64(setup_seed);
        let mut context = BuildContext::new_with_address_pools(
            self.chain_id,
            &self.spec.gas,
            &self.accounts,
            &self.address_pools,
            &self.artifacts,
            &mut nonces,
            &mut rng,
        );
        let submitter = self.submitter.clone();
        let setup = materialize_setup_online(
            &mut adapter,
            &self.spec,
            &mut context,
            FALLBACK_STEP_TIMEOUT,
            move |transaction| {
                let submitter = submitter.clone();
                async move {
                    submitter.validate_submission_auth(transaction.sender)?;
                    Ok(transaction)
                }
            },
        )
        .await?;

        for (index, submit) in statically_replayable_submissions(scenario, &self.name)? {
            let label = scenario.scenario.steps[index].diagnostic_label(index);
            let mut value = self
                .spec
                .templates
                .get(&submit.template)
                .cloned()
                .expect("template references were validated before dry-run materialization");
            let overlay = materialize_yaml(&submit.with_value, &RuntimeContext::empty())?;
            merge_yaml(&mut value, overlay);
            let value = setup.resolve_template(value)?;
            tokio::time::timeout(
                FALLBACK_STEP_TIMEOUT,
                adapter.prepare_request(&value, &mut context),
            )
            .await
            .map_err(|_| {
                eyre::eyre!("{label} template '{}' preparation timed out", submit.template)
            })??;
            let transaction = materialize_and_sign_template(
                &adapter,
                &submit.template,
                value,
                TxPhase::Workload,
                &[],
                &mut context,
            )
            .wrap_err_with(|| {
                format!("{label} has an invalid static overlay for template '{}'", submit.template)
            })?;
            self.submitter.validate_submission_auth(Some(transaction.sender)).wrap_err_with(
                || {
                    format!(
                        "{label} has no submission authentication for template '{}'",
                        submit.template
                    )
                },
            )?;
        }
        Ok(())
    }

    async fn initialize(self, setup_seed: u64, receipt_workers: usize) -> Result<ChainRuntime<A>> {
        let Self {
            name,
            chain_id,
            query_rpc_url: _,
            workload_path,
            spec,
            accounts,
            address_pools,
            artifacts,
            mut adapter,
            mut nonces,
            query_provider,
            submitter,
        } = self;
        let setup = {
            let mut rng = StdRng::seed_from_u64(setup_seed);
            let mut context = BuildContext::new_with_address_pools(
                chain_id,
                &spec.gas,
                &accounts,
                &address_pools,
                &artifacts,
                &mut nonces,
                &mut rng,
            );
            materialize_setup_online(
                &mut adapter,
                &spec,
                &mut context,
                FALLBACK_STEP_TIMEOUT,
                |transaction| {
                    submit_setup_transaction(
                        submitter.clone(),
                        query_provider.clone(),
                        name.clone(),
                        transaction,
                    )
                },
            )
            .await
            .wrap_err_with(|| format!("failed to materialize setup for chain '{name}'"))?
        };
        let receipt_collector = ReceiptCollector::start(submitter.clone(), receipt_workers);
        let receipt_collector_handle = receipt_collector.handle();

        Ok(ChainRuntime {
            name,
            chain_id,
            workload_path,
            spec,
            accounts,
            address_pools,
            artifacts,
            adapter,
            setup,
            nonces: Mutex::new(nonces),
            submit_prepare_gate: Mutex::new(()),
            submission_lanes: Arc::new(SubmissionLanes::default()),
            submission_ambiguous: AtomicBool::new(false),
            query_provider,
            submitter,
            receipt_collector: Mutex::new(Some(receipt_collector)),
            receipt_collector_handle: StdMutex::new(Some(receipt_collector_handle)),
        })
    }
}

impl<A> ChainRuntime<A>
where
    A: NetworkAdapter + Default,
    <A::Network as Network>::UnsignedTx: SignableTransaction<alloy_primitives::Signature>,
    <A::Network as Network>::TxEnvelope:
        From<Signed<<A::Network as Network>::UnsignedTx>> + Encodable2718,
{
    async fn prepare(
        name: &str,
        definition: &ChainDef,
        input: ChainInput,
        transaction_rate: u64,
        max_rpc_in_flight: usize,
    ) -> Result<PreparedChain<A>> {
        let ChainInput {
            workload_path,
            submission_rpc_url,
            query_rpc_url,
            spec,
            accounts,
            address_pools,
            artifacts,
        } = input;
        let submission_provider = ProviderBuilder::new_with_network::<AnyNetwork>()
            .connect_http(submission_rpc_url)
            .erased();
        let query_provider = ProviderBuilder::new_with_network::<AnyNetwork>()
            .connect_http(query_rpc_url.clone())
            .erased();
        let rpc_chain_id =
            tokio::time::timeout(FALLBACK_STEP_TIMEOUT, query_provider.get_chain_id())
                .await
                .map_err(|_| eyre::eyre!("chain ID query timed out for chain '{name}'"))?
                .map_err(|_| eyre::eyre!("failed to query chain ID for chain '{name}'"))?;
        let chain_id = match definition.chain_id {
            ChainId::Auto => rpc_chain_id,
            ChainId::Explicit(expected) if expected == rpc_chain_id => expected,
            ChainId::Explicit(expected) => {
                bail!(
                    "chain '{name}' expected chain ID {expected}, but its RPC reported {rpc_chain_id}"
                );
            }
        };

        let adapter = A::default();
        let mut nonces = NonceTracker::new();
        tokio::time::timeout(
            FALLBACK_STEP_TIMEOUT,
            adapter.prepare_nonces(&spec, &accounts, &mut nonces, query_rpc_url.as_str()),
        )
        .await
        .map_err(|_| eyre::eyre!("nonce preparation timed out for chain '{name}'"))?
        .map_err(|_| eyre::eyre!("failed to prepare nonce state for chain '{name}'"))?;
        let request_auth: Option<Arc<dyn RequestAuthProvider>> = definition
            .request_auth
            .as_ref()
            .map(|auth| {
                SenderHeaderAuthProvider::from_file(
                    &auth.sender_header.name,
                    &auth.sender_header.map,
                    auth.sender_header.reload_interval.unwrap_or(Duration::from_secs(1)),
                )
                .map(|provider| Arc::new(provider) as Arc<dyn RequestAuthProvider>)
            })
            .transpose()
            .wrap_err_with(|| {
                format!("failed to configure request authentication for chain '{name}'")
            })?;
        let submitter = RpcSubmitter::new_with_request_auth(
            vec![RpcEndpoint::new(format!("{name}-submission"), submission_provider)],
            SenderConfig { rate_limit: transaction_rate, max_concurrent: max_rpc_in_flight },
            request_auth,
        )?;
        Ok(PreparedChain {
            name: name.to_string(),
            chain_id,
            query_rpc_url: query_rpc_url.to_string(),
            workload_path,
            spec,
            accounts,
            address_pools,
            artifacts,
            adapter,
            nonces,
            query_provider,
            submitter,
        })
    }

    async fn execute_submit(
        &self,
        step_name: &str,
        submit: SubmitStep,
        context: RuntimeContext,
        mut rng: StdRng,
        deadline: TokioInstant,
    ) -> Result<(RuntimeValue, StdRng), StepError> {
        let (materialized, submission_lanes) = self
            .prepare_submission(&submit.template, &submit.with_value, &context, &mut rng, deadline)
            .await?;
        let attempt_started_at = SystemTime::now();
        let attempt_started = Instant::now();
        let submission = match self
            .submitter
            .submit_classified_until(&materialized.generated, deadline)
            .await
        {
            Ok(submission) => submission,
            Err(error) => {
                if error.kind() == RpcSubmitFailureKind::Ambiguous {
                    self.track_receipt_metrics(
                        step_name,
                        &submit.template,
                        materialized.sender,
                        materialized.tx_hash,
                    );
                }
                let lookup = match error.kind() {
                    RpcSubmitFailureKind::BeforeSend => None,
                    RpcSubmitFailureKind::Rejected | RpcSubmitFailureKind::Ambiguous => {
                        let lookup = tokio::time::timeout_at(
                            deadline,
                            self.submitter.transaction_exists(
                                Some(materialized.sender),
                                materialized.tx_hash,
                            ),
                        )
                        .await;
                        match lookup {
                            Ok(result) => Some(result),
                            Err(_) => {
                                self.submission_ambiguous.store(true, Ordering::Release);
                                return Err(StepError::new(
                                    "timeout",
                                    "step timed out while checking an uncertain transaction submission; further submissions on this chain are disabled",
                                ));
                            }
                        }
                    }
                };
                if lookup
                    .as_ref()
                    .is_some_and(|result| result.as_ref().is_ok_and(|transaction| *transaction))
                {
                    RpcSubmission {
                        tx_hash: materialized.tx_hash,
                        acceptance_latency: attempt_started.elapsed(),
                        submitted_at: attempt_started_at,
                    }
                } else if error.kind() == RpcSubmitFailureKind::BeforeSend {
                    match self.rollback_submitted_nonces(&materialized, deadline).await {
                        Some(true) => {}
                        Some(false) => {
                            self.submission_ambiguous.store(true, Ordering::Release);
                            return Err(StepError::new(
                                "nonce_recovery_error",
                                "failed to restore nonce state after an RPC rejection",
                            ));
                        }
                        None => {
                            self.submission_ambiguous.store(true, Ordering::Release);
                            return Err(StepError::new(
                                "timeout",
                                "step timed out before nonce recovery could be proven safe; further submissions on this chain are disabled",
                            ));
                        }
                    }
                    if error.is_timeout() {
                        return Err(StepError::new(
                            "timeout",
                            "step timed out before transaction dispatch",
                        ));
                    }
                    return Err(StepError::new("submission_rejected", error.to_string()));
                } else {
                    // A JSON-RPC rejection is not proof that its nonce is reusable:
                    // `already known`, `nonce too low`, and replacement errors can
                    // all describe state the local tracker cannot safely reconstruct.
                    self.submission_ambiguous.store(true, Ordering::Release);
                    let classification = if error.kind() == RpcSubmitFailureKind::Rejected &&
                        lookup.as_ref().is_some_and(|result| {
                            result.as_ref().is_ok_and(|transaction| !*transaction)
                        }) {
                        "submission_rejected"
                    } else {
                        "submission_ambiguous"
                    };
                    return Err(StepError::new(classification, error.to_string()));
                }
            }
        };
        self.track_receipt_metrics(
            step_name,
            &submit.template,
            materialized.sender,
            materialized.tx_hash,
        );
        if submission.tx_hash != materialized.tx_hash {
            self.submission_ambiguous.store(true, Ordering::Release);
            return Err(StepError::new(
                "rpc_hash_mismatch",
                "RPC returned a transaction hash different from the signed payload",
            ));
        }
        drop(submission_lanes);

        let receipt = if submit.await_mode == Some(SubmitAwait::Receipt) {
            let receipt = tokio::time::timeout_at(
                deadline,
                wait::wait_for_receipt(
                    &self.query_provider,
                    &self.submitter,
                    &self.name,
                    Some(materialized.sender),
                    submission.tx_hash,
                    DEFAULT_POLL_INTERVAL,
                    0,
                ),
            )
            .await
            .map_err(|_| StepError::timeout())??;
            if !receipt.status {
                return Err(StepError::new("reverted_receipt", "submitted transaction reverted"));
            }
            receipt.value
        } else {
            RuntimeValue::Null
        };

        Ok((
            object([
                ("chain", RuntimeValue::String(self.name.clone())),
                ("template", RuntimeValue::String(submit.template.clone())),
                ("id", RuntimeValue::String(submit.template)),
                ("sender", RuntimeValue::Address(materialized.sender)),
                ("tx_hash", RuntimeValue::Bytes32(submission.tx_hash)),
                ("submitted_at", RuntimeValue::Uint(U256::from(unix_ms(submission.submitted_at)))),
                (
                    "acceptance_latency",
                    RuntimeValue::Uint(U256::from(
                        u64::try_from(submission.acceptance_latency.as_millis())
                            .unwrap_or(u64::MAX),
                    )),
                ),
                ("receipt", receipt),
            ]),
            rng,
        ))
    }

    async fn finish_receipt_metrics(&self) -> Vec<ReceiptMetricGroup> {
        drop(
            self.receipt_collector_handle
                .lock()
                .expect("receipt collector handle mutex poisoned")
                .take(),
        );
        let collector = self.receipt_collector.lock().await.take();
        match collector {
            Some(collector) => collector.finish().await,
            None => Vec::new(),
        }
    }

    fn track_receipt_metrics(
        &self,
        step_name: &str,
        input: &str,
        sender: Address,
        tx_hash: TxHash,
    ) {
        let handle =
            self.receipt_collector_handle.lock().expect("receipt collector handle mutex poisoned");
        let Some(handle) = handle.as_ref() else { return };
        handle.track(
            Some(sender),
            tx_hash,
            ReceiptMetricLabels::from([
                ("chain".to_string(), self.name.clone()),
                ("input".to_string(), input.to_string()),
                ("step".to_string(), step_name.to_string()),
            ]),
        );
    }

    async fn prepare_submission(
        &self,
        template: &str,
        overlay: &serde_yaml::Value,
        context: &RuntimeContext,
        rng: &mut StdRng,
        deadline: TokioInstant,
    ) -> Result<(MaterializedTx, SubmissionLaneGuard), StepError> {
        loop {
            let prepare_gate = lock_before_deadline(&self.submit_prepare_gate, deadline)
                .await
                .ok_or_else(StepError::timeout)?;
            if self.submission_ambiguous.load(Ordering::Acquire) {
                return Err(StepError::new(
                    "nonce_state_ambiguous",
                    "an earlier submission on this chain had an unknown acceptance outcome",
                ));
            }

            let mut attempt_rng = rng.clone();
            let materialized = tokio::time::timeout_at(
                deadline,
                self.materialize(template, overlay, context, &mut attempt_rng),
            )
            .await
            .map_err(|_| StepError::timeout())??;
            let keys = materialized
                .nonce_reservations
                .iter()
                .map(|reservation| reservation.key)
                .chain(
                    materialized
                        .generated
                        .submission_keys
                        .iter()
                        .chain(&materialized.generated.inclusion_keys)
                        .map(|key| key.into_inner()),
                )
                .collect::<BTreeSet<_>>();
            if keys.is_empty() {
                if !self.rollback_reserved_nonces(&materialized).await {
                    self.submission_ambiguous.store(true, Ordering::Release);
                }
                return Err(StepError::new(
                    "materialization_error",
                    "materialized transaction has no scheduling key",
                ));
            }

            let notified = self.submission_lanes.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(lanes) = self.submission_lanes.try_acquire(keys) {
                *rng = attempt_rng;
                drop(prepare_gate);
                return Ok((materialized, lanes));
            }

            if !self.rollback_reserved_nonces(&materialized).await {
                self.submission_ambiguous.store(true, Ordering::Release);
                return Err(StepError::new(
                    "nonce_recovery_error",
                    "failed to restore nonce state while waiting for an active submission lane",
                ));
            }
            drop(prepare_gate);
            tokio::time::timeout_at(deadline, notified).await.map_err(|_| StepError::timeout())?;
        }
    }

    async fn materialize(
        &self,
        template: &str,
        overlay: &serde_yaml::Value,
        context: &RuntimeContext,
        rng: &mut StdRng,
    ) -> Result<MaterializedTx, StepError> {
        let mut value = self
            .spec
            .templates
            .get(template)
            .cloned()
            .ok_or_else(|| StepError::new("template_error", "template not found"))?;
        let overlay = materialize_yaml(overlay, context).map_err(StepError::expression)?;
        merge_yaml(&mut value, overlay);
        let value = self
            .setup
            .resolve_template(value)
            .map_err(|error| StepError::new("materialization_error", error.to_string()))?;
        let mut nonces = self.nonces.lock().await;
        let mut build_context = BuildContext::new_with_address_pools(
            self.chain_id,
            &self.spec.gas,
            &self.accounts,
            &self.address_pools,
            &self.artifacts,
            &mut nonces,
            rng,
        );
        self.adapter
            .prepare_request(&value, &mut build_context)
            .await
            .map_err(|error| StepError::new("materialization_error", error.to_string()))?;
        materialize_and_sign_template(
            &self.adapter,
            template,
            value,
            TxPhase::Workload,
            &[],
            &mut build_context,
        )
        .map_err(|error| StepError::new("materialization_error", error.to_string()))
    }

    async fn rollback_reserved_nonces(&self, transaction: &MaterializedTx) -> bool {
        let mut nonces = self.nonces.lock().await;
        let mut restored = true;
        for reservation in transaction.nonce_reservations.iter().rev() {
            restored &= nonces.rewind(reservation.key, reservation.nonce);
        }
        restored
    }

    async fn rollback_submitted_nonces(
        &self,
        transaction: &MaterializedTx,
        deadline: TokioInstant,
    ) -> Option<bool> {
        // Exclude speculative materialization while proving that this accepted
        // reservation is still the newest value on every affected lane.
        let _prepare_gate = lock_before_deadline(&self.submit_prepare_gate, deadline).await?;
        Some(self.rollback_reserved_nonces(transaction).await)
    }

    async fn checkpoint(&self) -> Result<RuntimeValue, StepError> {
        let block_number = self.query_provider.get_block_number().await.map_err(StepError::rpc)?;
        let block = self
            .query_provider
            .get_block_by_number(BlockNumberOrTag::Number(block_number))
            .await
            .map_err(StepError::rpc)?;
        let block_hash = block.map(|block| block.header().hash());
        Ok(object([
            ("chain", RuntimeValue::String(self.name.clone())),
            ("block_number", RuntimeValue::Uint(U256::from(block_number))),
            ("block_hash", block_hash.map(RuntimeValue::Bytes32).unwrap_or(RuntimeValue::Null)),
            ("captured_at", RuntimeValue::Uint(U256::from(unix_ms(SystemTime::now())))),
        ]))
    }
}

async fn lock_before_deadline<T>(
    mutex: &Mutex<T>,
    deadline: TokioInstant,
) -> Option<MutexGuard<'_, T>> {
    tokio::time::timeout_at(deadline, mutex.lock()).await.ok()
}

async fn submit_setup_transaction(
    submitter: RpcSubmitter,
    query_provider: DynProvider<AnyNetwork>,
    chain_name: String,
    transaction: txgen_core::GeneratedTx,
) -> Result<txgen_core::GeneratedTx> {
    let deadline = TokioInstant::now() + FALLBACK_STEP_TIMEOUT;
    let expected_hash = keccak256(&transaction.raw);
    let sender = transaction.sender;
    let submission = match submitter.submit_classified_until(&transaction, deadline).await {
        Ok(submission) => submission,
        Err(error) if error.kind() == RpcSubmitFailureKind::BeforeSend => {
            return Err(eyre::eyre!(
                "setup transaction was not dispatched on chain '{chain_name}': {error}"
            ));
        }
        Err(error) => {
            let found = tokio::time::timeout_at(
                deadline,
                submitter.transaction_exists(sender, expected_hash),
            )
            .await
            .map_err(|_| {
                eyre::eyre!(
                    "setup submission outcome is unknown on chain '{chain_name}' after lookup timed out"
                )
            })?
            .map_err(|_| {
                eyre::eyre!(
                    "setup submission outcome is unknown on chain '{chain_name}' because transaction lookup failed"
                )
            })?;
            if !found {
                return Err(eyre::eyre!(
                    "setup transaction was rejected or has an unknown acceptance outcome on chain '{chain_name}': {error}"
                ));
            }
            RpcSubmission {
                tx_hash: expected_hash,
                acceptance_latency: Duration::ZERO,
                submitted_at: SystemTime::now(),
            }
        }
    };
    if submission.tx_hash != expected_hash {
        bail!("setup RPC returned a mismatched transaction hash on chain '{chain_name}'");
    }
    let receipt = tokio::time::timeout_at(
        deadline,
        wait::wait_for_receipt(
            &query_provider,
            &submitter,
            &chain_name,
            sender,
            submission.tx_hash,
            DEFAULT_POLL_INTERVAL,
            0,
        ),
    )
    .await
    .map_err(|_| eyre::eyre!("setup receipt timed out on chain '{chain_name}'"))?
    .map_err(|error| eyre::eyre!("setup receipt failed on chain '{chain_name}': {error}"))?;
    if !receipt.status {
        bail!("setup transaction reverted on chain '{chain_name}'");
    }
    Ok(transaction)
}

enum BindingRuntime {
    Account {
        pool: String,
        selection: AccountSelection,
        addresses: Vec<Address>,
        lease_pool: Option<Arc<LeasePool>>,
        lease_slot: usize,
    },
    Bytes32,
}

struct LeasePool {
    permits: Vec<Arc<Semaphore>>,
    slots_per_instance: usize,
    offset: usize,
}

impl LeasePool {
    fn index(&self, instance: u64, slot: usize) -> usize {
        let len = self.permits.len();
        let base = usize::try_from(instance).unwrap_or_else(|_| {
            let reduced = instance % u64::try_from(len).unwrap_or(u64::MAX);
            usize::try_from(reduced).unwrap_or(0)
        });
        base.wrapping_mul(self.slots_per_instance).wrapping_add(slot).wrapping_add(self.offset) %
            len
    }

    async fn acquire_index(&self, index: usize) -> Result<OwnedSemaphorePermit, StepError> {
        self.permits[index]
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| StepError::new("binding_error", "account lease pool closed"))
    }
}

fn build_binding_runtimes(
    spec: &ScenarioSpec,
    chains: &BTreeMap<String, ChainInput>,
    seed: u64,
) -> Result<BTreeMap<String, BindingRuntime>> {
    let mut pool_addresses = BTreeMap::<String, Vec<Address>>::new();
    let mut lease_counts = BTreeMap::<String, usize>::new();
    let mut pool_bindings = BTreeMap::<String, Vec<String>>::new();
    let mut required_chains = BTreeMap::<String, BTreeSet<String>>::new();

    for (binding_name, binding) in &spec.scenario.bindings {
        let BindingDef::Account(account) = binding else { continue };
        pool_bindings.entry(account.pool.clone()).or_default().push(binding_name.clone());
        required_chains
            .entry(account.pool.clone())
            .or_default()
            .extend(binding_submit_chains(spec, binding_name)?);
        if account.select == AccountSelection::Lease {
            *lease_counts.entry(account.pool.clone()).or_default() += 1;
        }
    }

    for (pool, binding_names) in &pool_bindings {
        let consuming_chains = required_chains.entry(pool.clone()).or_default();
        if consuming_chains.is_empty() {
            consuming_chains.extend(chains.iter().filter_map(|(name, chain)| {
                chain.accounts.get_pool(pool).is_ok().then_some(name.clone())
            }));
        }
        if consuming_chains.is_empty() {
            bail!(
                "account pool '{pool}' required by binding(s) {} was not found",
                binding_names.join(", ")
            );
        }

        let mut canonical: Option<Vec<Address>> = None;
        for chain_name in consuming_chains.iter() {
            let chain = &chains[chain_name];
            let signers = chain.accounts.get_pool(pool).wrap_err_with(|| {
                format!(
                    "account pool '{pool}' required by binding(s) {} is missing on chain '{chain_name}'",
                    binding_names.join(", ")
                )
            })?;
            let addresses = signers.iter().map(SignerExt::address).collect::<Vec<_>>();
            if addresses.is_empty() {
                bail!("account pool '{pool}' is empty on chain '{chain_name}'");
            }
            if let Some(expected) = &canonical {
                if expected != &addresses {
                    bail!(
                        "account pool '{pool}' must derive the same accounts on each consuming chain"
                    );
                }
            } else {
                canonical = Some(addresses);
            }
        }
        pool_addresses.insert(pool.clone(), canonical.expect("at least one consuming chain"));
    }

    let mut permits_by_address = BTreeMap::<Address, Arc<Semaphore>>::new();
    let mut lease_pools = BTreeMap::new();
    for (pool, count) in &lease_counts {
        let addresses = &pool_addresses[pool];
        if *count > addresses.len() {
            bail!(
                "scenario leases {count} accounts from pool '{pool}' per instance, but the pool has only {}",
                addresses.len()
            );
        }
        let offset =
            usize::try_from(instance_seed(seed, stable_hash(pool))).unwrap_or(0) % addresses.len();
        lease_pools.insert(
            pool.clone(),
            Arc::new(LeasePool {
                permits: addresses
                    .iter()
                    .map(|address| {
                        permits_by_address
                            .entry(*address)
                            .or_insert_with(|| Arc::new(Semaphore::new(1)))
                            .clone()
                    })
                    .collect(),
                slots_per_instance: *count,
                offset,
            }),
        );
    }

    let mut next_slot = BTreeMap::<String, usize>::new();
    let mut runtimes = BTreeMap::new();
    for (name, binding) in &spec.scenario.bindings {
        let BindingDef::Account(account) = binding else {
            runtimes.insert(name.clone(), BindingRuntime::Bytes32);
            continue;
        };
        let addresses = pool_addresses[&account.pool].clone();
        if let AccountSelection::Index(index) = account.select &&
            index >= addresses.len()
        {
            bail!(
                "account binding '{name}' selects index {index}, but pool '{}' has {} accounts",
                account.pool,
                addresses.len()
            );
        }
        let lease_slot = if account.select == AccountSelection::Lease {
            let slot = next_slot.entry(account.pool.clone()).or_default();
            let current = *slot;
            *slot += 1;
            current
        } else {
            0
        };
        runtimes.insert(
            name.clone(),
            BindingRuntime::Account {
                pool: account.pool.clone(),
                selection: account.select,
                addresses,
                lease_pool: lease_pools.get(&account.pool).cloned(),
                lease_slot,
            },
        );
    }
    Ok(runtimes)
}

fn binding_submit_chains(spec: &ScenarioSpec, binding: &str) -> Result<Vec<String>> {
    let mut chains = Vec::new();
    for step in &spec.scenario.steps {
        let StepAction::Submit(submit) = &step.action else { continue };
        let referenced = collect_variable_paths(&submit.with_value)?
            .into_iter()
            .any(|path| path == binding || path.starts_with(&format!("{binding}.")));
        if referenced && !chains.contains(&submit.chain) {
            chains.push(submit.chain.clone());
        }
    }
    Ok(chains)
}

fn validate_workload_references<A: NetworkAdapter>(
    spec: &ScenarioSpec,
    chains: &BTreeMap<String, ChainInput>,
) -> Result<()> {
    for (index, step) in spec.scenario.steps.iter().enumerate() {
        let label = step.diagnostic_label(index);
        let chain = &chains[step.action.chain()];
        match &step.action {
            StepAction::Invoke(invoke)
                if !A::scenario_actions().contains(&invoke.action.as_str()) =>
            {
                let supported = if A::scenario_actions().is_empty() {
                    "none".to_string()
                } else {
                    A::scenario_actions().join(", ")
                };
                bail!(
                    "{label} references unsupported '{}' action '{}' (supported actions: {supported})",
                    A::network_name(),
                    invoke.action
                );
            }
            StepAction::Submit(submit) if !chain.spec.templates.contains_key(&submit.template) => {
                bail!(
                    "{label} references missing template '{}' on chain '{}'",
                    submit.template,
                    submit.chain
                );
            }
            StepAction::WaitLog(wait_log) => {
                let abi = chain.artifacts.get(&wait_log.abi).wrap_err_with(|| {
                    format!(
                        "{label} references missing ABI artifact '{}' on chain '{}'",
                        wait_log.abi, wait_log.chain
                    )
                })?;
                let filter_types =
                    wait::resolve_event_filter_types(abi, &wait_log.event, &wait_log.where_value)
                        .wrap_err_with(|| {
                        format!(
                            "{label} has an invalid event '{}' for ABI '{}' on chain '{}'",
                            wait_log.event, wait_log.abi, wait_log.chain
                        )
                    })?;
                for (name, expression) in &wait_log.where_value {
                    let filter_type = &filter_types[name];
                    spec.validate_abi_filter_expression_type(
                        index,
                        expression,
                        &filter_type.sol_type,
                        filter_type.accepts_precomputed_hash,
                        name,
                    )?;
                    if collect_variable_paths(expression)?.is_empty() {
                        wait::validate_constant_event_filter(expression, filter_type)
                            .wrap_err_with(|| {
                                format!(
                                    "{label} event filter '{}' is invalid for ABI type '{}'",
                                    name, filter_type.sol_type
                                )
                            })?;
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn statically_replayable_submissions<'a>(
    scenario: &'a ScenarioSpec,
    chain: &str,
) -> Result<Vec<(usize, &'a SubmitStep)>> {
    let mut submissions = Vec::new();
    for (index, step) in scenario.scenario.steps.iter().enumerate() {
        let StepAction::Submit(submit) = &step.action else { continue };
        if submit.chain != chain {
            continue;
        }
        // Materialization is stateful (notably for explicit Tempo nonces), so
        // later submits cannot be faithfully replayed after a dynamic gap.
        if !collect_variable_paths(&submit.with_value)?.is_empty() {
            break;
        }
        submissions.push((index, submit));
    }
    Ok(submissions)
}

fn account_binding_value(pool: &str, index: usize, address: Address) -> RuntimeValue {
    object([
        ("pool", RuntimeValue::String(pool.to_string())),
        ("index", RuntimeValue::Uint(U256::from(index))),
        ("address", RuntimeValue::Address(address)),
        (
            "ref",
            object([
                ("pool", RuntimeValue::String(pool.to_string())),
                ("select", object([("index", RuntimeValue::Uint(U256::from(index)))])),
            ]),
        ),
    ])
}

fn expression_hash(value: &serde_yaml::Value, context: &RuntimeContext) -> Result<TxHash> {
    match eval_expression(value, context)?.coerce_dyn_sol(&DynSolType::FixedBytes(32))? {
        DynSolValue::FixedBytes(value, 32) => Ok(value),
        _ => unreachable!("bytes32 coercion returned another type"),
    }
}

fn object<const N: usize>(values: [(&str, RuntimeValue); N]) -> RuntimeValue {
    RuntimeValue::Object(values.into_iter().map(|(key, value)| (key.to_string(), value)).collect())
}

fn step_name(index: usize, step: &StepDef) -> String {
    step.save.clone().unwrap_or_else(|| format!("step_{}_{}", index + 1, step.action.name()))
}

fn start_delay(
    run_started: Instant,
    last_start: Option<Instant>,
    starts_per_second: f64,
    run_duration: Option<Duration>,
) -> Option<Duration> {
    if starts_per_second == 0.0 {
        return None;
    }
    let last_start = last_start?;
    let target = last_start + Duration::from_secs_f64(1.0 / starts_per_second);
    let now = Instant::now();
    let mut delay = target.checked_duration_since(now)?;
    if let Some(duration) = run_duration {
        let remaining = (run_started + duration).saturating_duration_since(now);
        delay = delay.min(remaining);
    }
    (!delay.is_zero()).then_some(delay)
}

fn instance_seed(seed: u64, instance: u64) -> u64 {
    let mut value = seed ^ instance.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn stable_hash(value: &str) -> u64 {
    value.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_seeds_are_stable_and_distinct() {
        assert_eq!(instance_seed(42, 7), instance_seed(42, 7));
        assert_ne!(instance_seed(42, 7), instance_seed(42, 8));
        assert_ne!(instance_seed(42, 7), instance_seed(43, 7));
    }

    #[test]
    fn start_pacing_is_based_on_journeys_not_transactions() {
        let now = Instant::now();
        assert!(start_delay(now, None, 10.0, None).is_none());
        let delay = start_delay(now, Some(now), 10.0, None).unwrap();
        assert!(delay <= Duration::from_millis(100));
        assert!(delay > Duration::from_millis(50));
        assert!(start_delay(now, Some(now), 0.0, None).is_none());

        let overdue = now.checked_sub(Duration::from_secs(1)).unwrap();
        assert!(start_delay(now, Some(overdue), 10.0, None).is_none());
    }

    #[test]
    fn execution_config_rejects_invalid_limits() {
        let config = ScenarioExecutionConfig { max_in_flight: 0, ..Default::default() };
        assert!(config.validate().is_err());
        let config = ScenarioExecutionConfig { starts_per_second: f64::NAN, ..Default::default() };
        assert!(config.validate().is_err());
    }

    #[test]
    fn rpc_urls_reject_fragments_and_normalize_default_ports() {
        let explicit = parse_rpc_url("x", "rpc_url", "http://EXAMPLE.com:80/rpc").unwrap();
        let implicit = parse_rpc_url("x", "rpc_url", "http://example.com/rpc").unwrap();
        assert_eq!(explicit, implicit);
        assert!(parse_rpc_url("x", "rpc_url", "https://example.com/rpc#alias").is_err());
    }

    #[test]
    fn static_submission_preflight_stops_after_a_dynamic_nonce_gap() {
        let scenario = ScenarioSpec::parse(
            r#"version: 1
chains:
  x:
    network: tempo
    rpc_url: http://x.invalid
    workload: ./x.yaml
scenario:
  name: preflight-prefix
  bindings:
    user:
      account: { pool: users, select: lease }
  steps:
    - submit: { chain: x, template: before }
    - submit:
        chain: x
        template: dynamic
        with: { from: { var: user.ref } }
    - submit:
        chain: x
        template: after
        with: { nonce: 2 }
"#,
        )
        .unwrap();

        let submissions = statically_replayable_submissions(&scenario, "x").unwrap();
        assert_eq!(submissions.len(), 1);
        assert_eq!(submissions[0].0, 0);
        assert_eq!(submissions[0].1.template, "before");
    }

    #[tokio::test]
    async fn deterministic_lease_indices_are_exclusive_and_release() {
        let pool = Arc::new(LeasePool {
            permits: (0..2).map(|_| Arc::new(Semaphore::new(1))).collect(),
            slots_per_instance: 1,
            offset: 0,
        });
        let first_index = pool.index(0, 0);
        let first = pool.acquire_index(first_index).await.unwrap();
        let second_index = pool.index(1, 0);
        let second = pool.acquire_index(second_index).await.unwrap();
        assert_ne!(first_index, second_index);
        assert_eq!(pool.permits[first_index].available_permits(), 0);
        drop(first);
        assert_eq!(pool.permits[first_index].available_permits(), 1);
        drop(second);
    }

    #[tokio::test]
    async fn cancelling_an_instance_releases_its_lease() {
        let pool = Arc::new(LeasePool {
            permits: vec![Arc::new(Semaphore::new(1))],
            slots_per_instance: 1,
            offset: 0,
        });
        let acquired = Arc::new(tokio::sync::Notify::new());
        let task = {
            let pool = pool.clone();
            let acquired = acquired.clone();
            tokio::spawn(async move {
                let _lease = pool.acquire_index(0).await.unwrap();
                acquired.notify_one();
                std::future::pending::<()>().await;
            })
        };
        acquired.notified().await;
        assert_eq!(pool.permits[0].available_permits(), 0);
        task.abort();
        let _ = task.await;
        assert_eq!(pool.permits[0].available_permits(), 1);
    }

    #[tokio::test]
    async fn nonce_recovery_gate_respects_the_step_deadline() {
        let gate = Mutex::new(());
        let held = gate.lock().await;
        let started = Instant::now();
        let acquired =
            lock_before_deadline(&gate, TokioInstant::now() + Duration::from_millis(5)).await;
        assert!(acquired.is_none());
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(held);
    }

    #[test]
    fn submission_lanes_allow_disjoint_keys_and_exclude_collisions() {
        let lanes = Arc::new(SubmissionLanes::default());
        let first = lanes.try_acquire(BTreeSet::from([[1; 20]])).unwrap();
        let second = lanes.try_acquire(BTreeSet::from([[2; 20]])).unwrap();
        assert!(lanes.try_acquire(BTreeSet::from([[1; 20]])).is_none());
        drop(first);
        assert!(lanes.try_acquire(BTreeSet::from([[1; 20]])).is_some());
        drop(second);
    }

    #[test]
    fn account_binding_has_indexed_reference() {
        let value = account_binding_value("users", 3, Address::repeat_byte(1));
        let RuntimeValue::Object(value) = value else { panic!("object") };
        let RuntimeValue::Object(reference) = &value["ref"] else { panic!("ref") };
        let RuntimeValue::Object(select) = &reference["select"] else { panic!("select") };
        assert_eq!(select["index"], RuntimeValue::Uint(U256::from(3)));
    }
}
