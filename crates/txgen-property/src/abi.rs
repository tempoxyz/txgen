use alloy_dyn_abi::{DynSolType, DynSolValue};
use alloy_primitives::U256;
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
pub struct AbiValueGenerator;

impl AbiValueGenerator {
    /// Generate one value matching `ty`.
    pub fn generate(
        &mut self,
        strategy: AbiStrategy,
        ty: &DynSolType,
        rng: &mut dyn RngCore,
    ) -> DynSolValue {
        let DynSolType::Uint(bits) = ty else {
            panic!("built-in ABI generator currently supports unsigned integers, got {ty}")
        };
        let max = if *bits == 256 { U256::MAX } else { (U256::from(1) << bits) - U256::from(1) };
        let value = match strategy {
            AbiStrategy::Random => {
                let mut bytes = [0_u8; 32];
                rng.fill_bytes(&mut bytes);
                U256::from_be_bytes(bytes) & max
            }
            AbiStrategy::Echidna => match rng.next_u32() % 5 {
                0 => U256::ZERO,
                1 => U256::from(1),
                2 => max,
                3 => max.saturating_sub(U256::from(1)),
                _ => max >> 1,
            },
        };
        DynSolValue::Uint(value, *bits)
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
    pub fn abi_value(&mut self, strategy: AbiStrategy, ty: &DynSolType) -> DynSolValue {
        self.abi.generate(strategy, ty, self.rng)
    }
}
