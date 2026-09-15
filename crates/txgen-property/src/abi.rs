use abi_fuzz::{
    generators::{EchidnaGenerator, RandomGenerator},
    Constraints, Generator,
};
use alloy_dyn_abi::{DynSolType, DynSolValue};
use rand::RngCore;
use serde::{Deserialize, Serialize};

/// An ABI-fuzz generation strategy selected by a model's swarm.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AbiStrategy {
    /// Uniform type-driven generation.
    Random,
    /// Echidna-style integer buckets with random generation for other types.
    Echidna,
}

/// Reusable ABI-fuzz generators for one property run.
#[derive(Debug, Default)]
pub struct AbiValueGenerator {
    random: RandomGenerator,
    echidna: EchidnaGenerator,
}

impl AbiValueGenerator {
    /// Generate one value matching `ty`.
    pub fn generate(
        &mut self,
        strategy: AbiStrategy,
        ty: &DynSolType,
        constraints: Option<&Constraints>,
        rng: &mut dyn RngCore,
    ) -> DynSolValue {
        match strategy {
            AbiStrategy::Random => self.random.generate_constrained(ty, constraints, rng),
            // EchidnaGenerator intentionally has no constrained override. A
            // model that needs bounds should select Random with constraints or
            // provide a concrete model-derived value.
            AbiStrategy::Echidna => self.echidna.generate(ty, rng),
        }
    }
}

/// Generation services available while a model creates one action.
pub struct GenerateContext<'a> {
    /// RNG stream owned by the property runner.
    pub rng: &'a mut dyn RngCore,
    /// ABI-fuzz generator facade.
    pub abi: &'a mut AbiValueGenerator,
    /// Zero-based case index.
    pub case_index: u64,
    /// Zero-based step index inside the case.
    pub step_index: usize,
}

impl GenerateContext<'_> {
    /// Generate one ABI value through the selected strategy.
    pub fn abi_value(
        &mut self,
        strategy: AbiStrategy,
        ty: &DynSolType,
        constraints: Option<&Constraints>,
    ) -> DynSolValue {
        self.abi.generate(strategy, ty, constraints, self.rng)
    }
}
