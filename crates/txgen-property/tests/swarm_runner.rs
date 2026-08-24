use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use alloy_dyn_abi::{DynSolType, DynSolValue};
use eyre::{bail, ensure, Result};
use serde::{Deserialize, Serialize};
use tempfile::tempdir;
use txgen_property::{
    run, AbiStrategy, GenerateContext, ModelRegistry, Prediction, PropertyHarness, PropertyModel,
    RunConfig, SwarmPolicy,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
enum ActionKind {
    Deposit,
    Withdraw,
    ReturnToZone,
    SettleWithdrawal,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "action")]
enum Action {
    Deposit { amount: u8 },
    Withdraw { amount: u8 },
    ReturnToZone { amount: u8 },
    SettleWithdrawal,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Expected {
    Success,
    Revert,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct State {
    l1_user: u16,
    portal: u16,
    zone_user: u16,
    supply: u16,
    pending_withdrawal: u16,
}

impl Default for State {
    fn default() -> Self {
        Self { l1_user: u8::MAX.into(), portal: 0, zone_user: 0, supply: 0, pending_withdrawal: 0 }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Swarm {
    actions: BTreeSet<ActionKind>,
    abi_strategy: AbiStrategy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct Trace {
    outcome: Expected,
}

#[derive(Debug)]
struct SolvencyModel {
    state: State,
}

impl SolvencyModel {
    fn new(state: State) -> Self {
        Self { state }
    }

    fn generated_amount(context: &mut GenerateContext<'_>, strategy: AbiStrategy) -> Result<u8> {
        match context.abi_value(strategy, &DynSolType::Uint(8), None) {
            DynSolValue::Uint(value, 8) => Ok(value.to::<u8>()),
            value => bail!("ABI generator returned unexpected value {value:?}"),
        }
    }

    fn outcome_and_state(&self, action: &Action) -> (Expected, State) {
        let mut next = self.state.clone();
        let outcome = match *action {
            Action::Deposit { amount } if u16::from(amount) <= next.l1_user => {
                let amount = u16::from(amount);
                next.l1_user -= amount;
                next.portal += amount;
                next.zone_user += amount;
                next.supply += amount;
                Expected::Success
            }
            Action::Withdraw { amount } if u16::from(amount) <= next.zone_user => {
                let amount = u16::from(amount);
                next.zone_user -= amount;
                next.supply -= amount;
                next.pending_withdrawal += amount;
                Expected::Success
            }
            Action::ReturnToZone { amount } if u16::from(amount) <= next.pending_withdrawal => {
                let amount = u16::from(amount);
                next.pending_withdrawal -= amount;
                next.zone_user += amount;
                next.supply += amount;
                Expected::Success
            }
            Action::SettleWithdrawal if next.pending_withdrawal <= next.portal => {
                let amount = next.pending_withdrawal;
                next.pending_withdrawal = 0;
                next.portal -= amount;
                next.l1_user += amount;
                Expected::Success
            }
            _ => Expected::Revert,
        };
        (outcome, next)
    }
}

impl PropertyModel for SolvencyModel {
    const NAME: &'static str = "toy-solvency";
    const VERSION: &'static str = "1";

    type State = State;
    type Swarm = Swarm;
    type ActionKind = ActionKind;
    type Action = Action;
    type Expected = Expected;
    type Trace = Trace;
    type ObservationRequest = ();
    type Observation = State;

    fn state(&self) -> &Self::State {
        &self.state
    }

    fn generate_swarm(
        &self,
        rng: &mut dyn rand::RngCore,
        policy: &SwarmPolicy,
    ) -> Result<Self::Swarm> {
        let actions = policy
            .subset(&[ActionKind::Deposit, ActionKind::Withdraw, ActionKind::ReturnToZone], rng)
            .into_iter()
            .collect();
        let abi_strategy = *policy
            .choose(&[AbiStrategy::Random, AbiStrategy::Echidna], rng)
            .expect("non-empty ABI strategy set");
        Ok(Swarm { actions, abi_strategy })
    }

    fn enabled_actions(&self, swarm: &Self::Swarm) -> Vec<Self::ActionKind> {
        let mut enabled = Vec::new();
        if self.state.l1_user > 0 && swarm.actions.contains(&ActionKind::Deposit) {
            enabled.push(ActionKind::Deposit);
        }
        if self.state.zone_user > 0 && swarm.actions.contains(&ActionKind::Withdraw) {
            enabled.push(ActionKind::Withdraw);
        }
        if self.state.pending_withdrawal > 0 {
            if swarm.actions.contains(&ActionKind::ReturnToZone) {
                enabled.push(ActionKind::ReturnToZone);
            }
            // Lifecycle actions are state-driven, not optional swarm capabilities.
            enabled.push(ActionKind::SettleWithdrawal);
        }
        enabled
    }

    fn generate_action(
        &self,
        swarm: &Self::Swarm,
        kind: &Self::ActionKind,
        context: &mut GenerateContext<'_>,
    ) -> Result<Self::Action> {
        let amount = Self::generated_amount(context, swarm.abi_strategy)?;
        Ok(match kind {
            ActionKind::Deposit => Action::Deposit { amount },
            ActionKind::Withdraw => Action::Withdraw { amount },
            ActionKind::ReturnToZone => Action::ReturnToZone { amount },
            ActionKind::SettleWithdrawal => Action::SettleWithdrawal,
        })
    }

    fn predict(&self, action: &Self::Action) -> Result<Prediction<Self::State, Self::Expected>> {
        let (expected, state) = self.outcome_and_state(action);
        Ok(Prediction { state, expected })
    }

    fn transition_observation(&self, _action: &Self::Action) -> Self::ObservationRequest {}

    fn verify_transition(
        &self,
        prediction: &Prediction<Self::State, Self::Expected>,
        _action: &Self::Action,
        trace: &Self::Trace,
        observation: &Self::Observation,
    ) -> Result<Self::State> {
        ensure!(trace.outcome == prediction.expected, "execution outcome disagreed with model");
        ensure!(observation == &prediction.state, "observed state disagreed with model");
        verify_invariants(observation)?;
        Ok(observation.clone())
    }

    fn final_observation(&self) -> Self::ObservationRequest {}

    fn verify_all(&self, observation: &Self::Observation) -> Result<()> {
        ensure!(observation == &self.state, "final observation disagreed with committed model");
        verify_invariants(observation)
    }

    fn commit(&mut self, state: Self::State) {
        self.state = state;
    }
}

fn verify_invariants(state: &State) -> Result<()> {
    ensure!(
        state.portal == state.supply + state.pending_withdrawal,
        "solvency accounting identity violated"
    );
    ensure!(
        state.l1_user + state.portal + state.zone_user == u16::from(u8::MAX) + state.zone_user,
        "test fixture accounting overflowed"
    );
    Ok(())
}

#[derive(Debug)]
struct MemoryHarness {
    state: State,
    executed: Arc<Mutex<Vec<Action>>>,
    corrupt_after: Option<usize>,
}

impl MemoryHarness {
    fn healthy(executed: Arc<Mutex<Vec<Action>>>) -> Self {
        Self { state: State::default(), executed, corrupt_after: None }
    }
}

impl PropertyHarness<SolvencyModel> for MemoryHarness {
    async fn reset_and_initialize(&mut self) -> Result<SolvencyModel> {
        self.state = State::default();
        Ok(SolvencyModel::new(self.state.clone()))
    }

    async fn execute(&mut self, action: &Action) -> Result<Trace> {
        let model = SolvencyModel::new(self.state.clone());
        let (outcome, next) = model.outcome_and_state(action);
        self.state = next;
        let mut executed = self.executed.lock().expect("execution log mutex poisoned");
        executed.push(action.clone());
        if self.corrupt_after == Some(executed.len()) {
            self.state.portal += 1;
        }
        Ok(Trace { outcome })
    }

    async fn observe(&mut self, _request: &()) -> Result<State> {
        Ok(self.state.clone())
    }
}

#[tokio::test]
async fn runs_swarm_cases_with_abi_fuzz_and_checks_invariants() {
    let executed = Arc::new(Mutex::new(Vec::new()));
    let mut harness = MemoryHarness::healthy(executed.clone());
    let result = run::<SolvencyModel, _>(&mut harness, RunConfig::seeded(20, 30, 0x5eed))
        .await
        .expect("property run should complete");
    let actions = executed.lock().expect("execution log mutex poisoned");

    eprintln!(
        "[property] model={} seed={} cases={} verified_steps={}",
        SolvencyModel::NAME,
        result.report.seed,
        result.report.completed_cases,
        result.report.completed_steps
    );
    let deposits = actions.iter().filter(|action| matches!(action, Action::Deposit { .. })).count();
    let withdrawals =
        actions.iter().filter(|action| matches!(action, Action::Withdraw { .. })).count();
    let returns =
        actions.iter().filter(|action| matches!(action, Action::ReturnToZone { .. })).count();
    let settlements =
        actions.iter().filter(|action| matches!(action, Action::SettleWithdrawal)).count();
    eprintln!(
        "[property] distribution deposit={deposits} withdraw={withdrawals} \
         return_to_zone={returns} settle={settlements}"
    );
    for (index, action) in actions.iter().take(12).enumerate() {
        eprintln!("[property] action[{index}]={action:?}");
    }
    if actions.len() > 12 {
        eprintln!("[property] ... {} additional actions", actions.len() - 12);
    }
    eprintln!("[property] result=pass invariants=[solvency,closed_loop]");

    assert!(result.failure.is_none());
    assert_eq!(result.report.completed_cases, 20);
    assert!(result.report.completed_steps > 0);
    assert!(!actions.is_empty());
}

#[tokio::test]
async fn explicit_seed_reproduces_the_generated_actions() {
    async fn actions(seed: u64) -> Vec<Action> {
        let executed = Arc::new(Mutex::new(Vec::new()));
        let mut harness = MemoryHarness::healthy(executed.clone());
        run::<SolvencyModel, _>(&mut harness, RunConfig::seeded(8, 16, seed))
            .await
            .expect("property run should complete");
        executed.lock().expect("execution log mutex poisoned").clone()
    }

    assert_eq!(actions(42).await, actions(42).await);
    assert_ne!(actions(42).await, actions(43).await);
}

#[tokio::test]
async fn writes_first_failure_without_committing_the_bad_transition() {
    let directory = tempdir().expect("temporary failure directory");
    let executed = Arc::new(Mutex::new(Vec::new()));
    let mut harness = MemoryHarness { state: State::default(), executed, corrupt_after: Some(1) };
    let mut config = RunConfig::seeded(1, 8, 7);
    config.failure_directory = Some(directory.path().to_path_buf());

    let result = run::<SolvencyModel, _>(&mut harness, config)
        .await
        .expect("model mismatches are structured run results");
    let failure = result.failure.expect("corruption must produce a failure");
    let path = result.failure_path.expect("failure artifact must be written");

    assert_eq!(failure.step_index, Some(0));
    assert_eq!(failure.committed_state, serde_json::to_value(State::default()).unwrap());
    assert!(failure.error.contains("observed state disagreed with model"));
    assert!(path.exists());
    let yaml = std::fs::read_to_string(path).expect("failure YAML");
    eprintln!("[property] intentional mismatch artifact:\n{yaml}");
    assert!(yaml.contains("model: toy-solvency"));
    assert!(yaml.contains("seed: 7"));
}

#[tokio::test]
async fn registers_and_runs_rust_models_by_stable_name() {
    let executed = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ModelRegistry::new();
    registry
        .register::<SolvencyModel, _>(MemoryHarness::healthy(executed))
        .expect("first registration");
    assert_eq!(registry.get("toy-solvency").unwrap().version, "1");
    let result = registry
        .run("toy-solvency", RunConfig::seeded(2, 4, 99))
        .await
        .expect("registered model run");
    assert_eq!(result.report.completed_cases, 2);
    assert!(registry.register::<SolvencyModel, _>(MemoryHarness::healthy(Arc::default())).is_err());
    assert!(registry.run("missing", RunConfig::seeded(1, 1, 1)).await.is_err());
}

#[test]
fn prediction_is_pure_and_the_withdraw_return_loop_restores_state() {
    let mut model = SolvencyModel::new(State::default());

    let deposit = model.predict(&Action::Deposit { amount: 100 }).expect("deposit prediction");
    assert_eq!(model.state(), &State::default(), "predict must not mutate committed state");
    assert_eq!(deposit.expected, Expected::Success);
    model.commit(deposit.state);
    let before_loop = model.state().clone();

    let withdraw = model.predict(&Action::Withdraw { amount: 100 }).expect("withdraw prediction");
    assert_eq!(withdraw.expected, Expected::Success);
    eprintln!("[closed-loop] before={before_loop:?}");
    eprintln!("[closed-loop] after_withdraw={:?}", withdraw.state);
    model.commit(withdraw.state);

    let returned = model.predict(&Action::ReturnToZone { amount: 100 }).expect("return prediction");
    assert_eq!(returned.expected, Expected::Success);
    eprintln!("[closed-loop] after_return={:?}", returned.state);
    assert_eq!(returned.state, before_loop, "closed withdrawal loop must restore state");
    model.commit(returned.state);
    verify_invariants(model.state()).expect("closed-loop state must remain solvent");

    let impossible = model.predict(&Action::Withdraw { amount: 101 }).expect("revert prediction");
    assert_eq!(impossible.expected, Expected::Revert);
    assert_eq!(impossible.state, before_loop, "expected revert must preserve state");
}
