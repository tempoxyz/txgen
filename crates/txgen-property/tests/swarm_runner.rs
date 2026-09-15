use std::{collections::BTreeSet, fs};

use eyre::Result;
use serde::{Deserialize, Serialize};
use txgen_property::{
    run, AbiStrategy, CampaignHarness, CampaignRegistry, GenerateContext, RunConfig, SwarmPolicy,
    VerificationTrigger, WorkloadGenerator,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
enum Kind {
    Add,
    Subtract,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Swarm {
    actions: BTreeSet<Kind>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Action {
    kind: Kind,
    amount: u8,
}

#[derive(Clone, Debug, Serialize)]
struct Trace {
    accepted: bool,
    resulting_value: u64,
}

#[derive(Clone, Debug, Serialize)]
struct TerminalEvidence {
    resulting_value: u64,
}

#[derive(Clone, Debug, Serialize)]
struct Verification {
    l1_snapshot_block: u64,
    zone_snapshot_block: u64,
    observed_value: u64,
    allowed_maximum: u64,
}

#[derive(Clone, Debug, Default)]
struct Workload;

impl WorkloadGenerator for Workload {
    const NAME: &'static str = "mock-chain-invariant";
    const VERSION: &'static str = "2";

    type Swarm = Swarm;
    type ActionKind = Kind;
    type Action = Action;

    fn generate_swarm(
        &self,
        rng: &mut dyn rand::RngCore,
        policy: &SwarmPolicy,
    ) -> Result<Self::Swarm> {
        Ok(Swarm {
            actions: policy.subset(&[Kind::Add, Kind::Subtract], rng).into_iter().collect(),
        })
    }

    fn enabled_actions(&self, swarm: &Self::Swarm) -> Vec<Self::ActionKind> {
        swarm.actions.iter().copied().collect()
    }

    fn generate_action(
        &self,
        _swarm: &Self::Swarm,
        kind: &Self::ActionKind,
        context: &mut GenerateContext<'_>,
    ) -> Result<Self::Action> {
        let amount =
            match context.abi_value(AbiStrategy::Random, &alloy_dyn_abi::DynSolType::Uint(8), None)
            {
                alloy_dyn_abi::DynSolValue::Uint(value, 8) => value.to::<u8>(),
                value => panic!("unexpected generated value {value:?}"),
            };
        Ok(Action { kind: *kind, amount })
    }
}

#[derive(Debug)]
struct Harness {
    value: u64,
    allowed_maximum: u64,
    block: u64,
    triggers: Vec<VerificationTrigger>,
    executed: Vec<Action>,
}

impl Harness {
    fn new(allowed_maximum: u64) -> Self {
        Self { value: 0, allowed_maximum, block: 0, triggers: Vec::new(), executed: Vec::new() }
    }
}

impl CampaignHarness<Workload> for Harness {
    type Trace = Trace;
    type TerminalEvidence = TerminalEvidence;
    type Verification = Verification;

    async fn reset_case(&mut self) -> Result<()> {
        self.value = 0;
        Ok(())
    }

    async fn execute(&mut self, action: &Action) -> Result<Trace> {
        self.block += 1;
        self.executed.push(action.clone());
        match action.kind {
            Kind::Add => self.value += u64::from(action.amount),
            Kind::Subtract => self.value = self.value.saturating_sub(u64::from(action.amount)),
        }
        Ok(Trace { accepted: true, resulting_value: self.value })
    }

    async fn await_terminal(
        &mut self,
        _action: &Action,
        trace: &Trace,
    ) -> Result<Option<TerminalEvidence>> {
        Ok(Some(TerminalEvidence { resulting_value: trace.resulting_value }))
    }

    async fn verify(&mut self, trigger: VerificationTrigger) -> Result<Verification> {
        self.triggers.push(trigger);
        Ok(Verification {
            l1_snapshot_block: self.block,
            zone_snapshot_block: self.block,
            observed_value: self.value,
            allowed_maximum: self.allowed_maximum,
        })
    }

    fn violation(&self, verification: &Verification) -> Option<String> {
        (verification.observed_value > verification.allowed_maximum).then(|| {
            format!(
                "observed {} exceeds {}",
                verification.observed_value, verification.allowed_maximum
            )
        })
    }
}

#[tokio::test]
async fn runs_terminal_periodic_and_final_verification_without_predictions() -> Result<()> {
    let workload = Workload;
    let mut harness = Harness::new(u64::MAX);
    let mut config = RunConfig::seeded(1, 4, 7);
    config.verify_every_steps = 1;
    config.swarm.density = 1.0;
    let result = run(&workload, &mut harness, config).await?;

    assert!(result.failure.is_none());
    assert!(result.report.completed_steps > 0);
    assert!(harness.triggers.contains(&VerificationTrigger::TerminalTransition));
    assert!(harness.triggers.contains(&VerificationTrigger::Periodic));
    assert!(harness.triggers.contains(&VerificationTrigger::Final));
    Ok(())
}

#[tokio::test]
async fn failure_artifact_contains_complete_verification_and_chain_evidence() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let workload = Workload;
    let mut harness = Harness::new(0);
    let mut config = RunConfig::seeded(1, 8, 11);
    config.failure_directory = Some(temporary.path().to_path_buf());
    config.swarm.density = 1.0;
    let result = run(&workload, &mut harness, config).await?;
    let failure = result.failure.expect("must find invariant violation");

    assert_eq!(failure.campaign, Workload::NAME);
    assert!(!failure.actions.is_empty());
    assert!(failure.actions.last().unwrap().terminal_evidence.is_some());
    assert!(failure.verification.get("l1_snapshot_block").is_some());
    assert!(failure.verification.get("zone_snapshot_block").is_some());
    let path = result.failure_path.expect("artifact path");
    assert!(fs::read_to_string(path)?.contains("verification:"));
    Ok(())
}

#[tokio::test]
async fn registers_and_runs_campaigns_by_stable_name() -> Result<()> {
    let mut campaigns = CampaignRegistry::new();
    campaigns.register(Workload, Harness::new(u64::MAX))?;
    assert_eq!(campaigns.get(Workload::NAME).unwrap().version, "2");
    let result = campaigns.run(Workload::NAME, RunConfig::seeded(1, 2, 19)).await?;
    assert_eq!(result.report.completed_cases, 1);
    Ok(())
}

#[tokio::test]
async fn generated_seed_replays_actions_without_embedding_behavior() -> Result<()> {
    async fn actions(seed: u64) -> Result<Vec<Action>> {
        let workload = Workload;
        let mut harness = Harness::new(u64::MAX);
        run(&workload, &mut harness, RunConfig::seeded(1, 8, seed)).await?;
        Ok(harness.executed)
    }

    assert_eq!(actions(97).await?, actions(97).await?);
    Ok(())
}
