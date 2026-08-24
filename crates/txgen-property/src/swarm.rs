use eyre::{bail, Result};
use rand::{Rng, RngCore};
use serde::{Deserialize, Serialize};

/// Generic controls used while a model creates one case's swarm.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct SwarmPolicy {
    /// Independent inclusion probability for optional swarm capabilities.
    pub density: f64,
    /// Maximum attempts to produce an initially executable swarm.
    pub max_resamples: usize,
}

impl Default for SwarmPolicy {
    fn default() -> Self {
        Self { density: 0.5, max_resamples: 256 }
    }
}

impl SwarmPolicy {
    /// Validate policy bounds.
    pub fn validate(self) -> Result<Self> {
        if !self.density.is_finite() || !(0.0..=1.0).contains(&self.density) {
            bail!("swarm density must be a finite value in [0, 1]");
        }
        if self.max_resamples == 0 {
            bail!("swarm max_resamples must be greater than zero");
        }
        Ok(self)
    }

    /// Decide whether one optional capability is enabled.
    pub fn include(&self, rng: &mut dyn RngCore) -> bool {
        rng.random_bool(self.density)
    }

    /// Select an optional subset using the configured density.
    pub fn subset<T: Clone>(&self, values: &[T], rng: &mut dyn RngCore) -> Vec<T> {
        values.iter().filter(|_| self.include(rng)).cloned().collect()
    }

    /// Choose one value uniformly.
    pub fn choose<'a, T>(&self, values: &'a [T], rng: &mut dyn RngCore) -> Option<&'a T> {
        (!values.is_empty()).then(|| &values[rng.random_range(0..values.len())])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};

    #[test]
    fn validates_density_and_resample_limit() {
        assert!(SwarmPolicy::default().validate().is_ok());
        assert!(SwarmPolicy { density: -0.1, ..SwarmPolicy::default() }.validate().is_err());
        assert!(SwarmPolicy { density: 1.1, ..SwarmPolicy::default() }.validate().is_err());
        assert!(SwarmPolicy { max_resamples: 0, ..SwarmPolicy::default() }.validate().is_err());
    }

    #[test]
    fn zero_and_full_density_select_expected_subsets() {
        let mut rng = StdRng::seed_from_u64(7);
        let values = [1, 2, 3];
        assert!(SwarmPolicy { density: 0.0, ..SwarmPolicy::default() }
            .subset(&values, &mut rng)
            .is_empty());
        assert_eq!(
            SwarmPolicy { density: 1.0, ..SwarmPolicy::default() }.subset(&values, &mut rng),
            values
        );
    }
}
