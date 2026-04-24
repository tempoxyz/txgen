use alloy_primitives::{Address, Bytes, U256};
use eyre::{bail, Result};
use rand::Rng;
use serde::{de::DeserializeOwned, Deserialize};

use crate::{AccountManager, SelectMode};

/// A value that can be either a literal or a generator expression.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum GenValue<T> {
    /// Literal value.
    Literal(T),
    /// Generator expression.
    Generator(Generator),
}

impl<T: Default> Default for GenValue<T> {
    fn default() -> Self {
        Self::Literal(T::default())
    }
}

/// Generator expressions for dynamic value generation.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Generator {
    /// Uniform random integer in range `[min, max]`.
    Uniform([u64; 2]),
    /// Random choice from a list of values.
    Choice(Vec<serde_yaml::Value>),
    /// Account address from a pool.
    Pool { pool: String, select: SelectMode },
    /// Random bytes of given length.
    RandomBytes(usize),
    /// Explicit constant value.
    Const(serde_yaml::Value),
}

/// Resolver for generator expressions.
pub struct ValueResolver<'a> {
    pub accounts: &'a AccountManager,
    pub rng: &'a mut dyn rand::RngCore,
}

impl ValueResolver<'_> {
    /// Resolve a YAML value to a concrete type, handling generator expressions.
    pub fn resolve<T: DeserializeOwned + FromGenerator>(
        &mut self,
        value: &serde_yaml::Value,
    ) -> Result<T> {
        // Try to deserialize as GenValue first
        if let Ok(gen_value) = serde_yaml::from_value::<GenValue<T>>(value.clone()) {
            match gen_value {
                GenValue::Literal(v) => return Ok(v),
                GenValue::Generator(generator) => return T::from_generator(&generator, self),
            }
        }

        // Fall back to direct deserialization
        Ok(serde_yaml::from_value(value.clone())?)
    }

    /// Resolve a GenValue to a concrete value.
    pub fn resolve_gen<T: DeserializeOwned + FromGenerator + Clone>(
        &mut self,
        gen_value: &GenValue<T>,
    ) -> Result<T> {
        match gen_value {
            GenValue::Literal(v) => Ok(v.clone()),
            GenValue::Generator(generator) => T::from_generator(generator, self),
        }
    }
}

/// Trait for types that can be generated from a Generator.
pub trait FromGenerator: Sized {
    fn from_generator(generator: &Generator, resolver: &mut ValueResolver<'_>) -> Result<Self>;
}

impl FromGenerator for u64 {
    fn from_generator(generator: &Generator, resolver: &mut ValueResolver<'_>) -> Result<Self> {
        match generator {
            Generator::Uniform([min, max]) => Ok(resolver.rng.random_range(*min..=*max)),
            Generator::Const(v) => Ok(serde_yaml::from_value(v.clone())?),
            Generator::Choice(choices) => {
                let idx = resolver.rng.random_range(0..choices.len());
                Ok(serde_yaml::from_value(choices[idx].clone())?)
            }
            _ => bail!("cannot generate u64 from {:?}", generator),
        }
    }
}

impl FromGenerator for u128 {
    fn from_generator(generator: &Generator, resolver: &mut ValueResolver<'_>) -> Result<Self> {
        match generator {
            Generator::Uniform([min, max]) => {
                Ok(resolver.rng.random_range(*min as u128..=*max as u128))
            }
            Generator::Const(v) => Ok(serde_yaml::from_value(v.clone())?),
            Generator::Choice(choices) => {
                let idx = resolver.rng.random_range(0..choices.len());
                Ok(serde_yaml::from_value(choices[idx].clone())?)
            }
            _ => bail!("cannot generate u128 from {:?}", generator),
        }
    }
}

impl FromGenerator for U256 {
    fn from_generator(generator: &Generator, resolver: &mut ValueResolver<'_>) -> Result<Self> {
        match generator {
            Generator::Uniform([min, max]) => {
                let val = resolver.rng.random_range(*min..=*max);
                Ok(U256::from(val))
            }
            Generator::Const(v) => {
                // Handle both numeric and string representations
                if let Some(n) = v.as_u64() {
                    Ok(U256::from(n))
                } else if let Some(s) = v.as_str() {
                    Ok(s.parse()?)
                } else {
                    bail!("cannot parse U256 from {:?}", v)
                }
            }
            Generator::Choice(choices) => {
                let idx = resolver.rng.random_range(0..choices.len());
                let v = &choices[idx];
                if let Some(n) = v.as_u64() {
                    Ok(U256::from(n))
                } else if let Some(s) = v.as_str() {
                    Ok(s.parse()?)
                } else {
                    bail!("cannot parse U256 from {:?}", v)
                }
            }
            _ => bail!("cannot generate U256 from {:?}", generator),
        }
    }
}

impl FromGenerator for Address {
    fn from_generator(generator: &Generator, resolver: &mut ValueResolver<'_>) -> Result<Self> {
        match generator {
            Generator::Pool { pool, select } => {
                let signer = match select {
                    SelectMode::Random => resolver.accounts.get_random(pool, resolver.rng)?,
                    SelectMode::Index(idx) => resolver.accounts.get_by_index(pool, *idx)?,
                };
                Ok(signer.address())
            }
            Generator::Const(v) => {
                let s: String = serde_yaml::from_value(v.clone())?;
                Ok(s.parse()?)
            }
            Generator::Choice(choices) => {
                let idx = resolver.rng.random_range(0..choices.len());
                let s: String = serde_yaml::from_value(choices[idx].clone())?;
                Ok(s.parse()?)
            }
            _ => bail!("cannot generate Address from {:?}", generator),
        }
    }
}

impl FromGenerator for Bytes {
    fn from_generator(generator: &Generator, resolver: &mut ValueResolver<'_>) -> Result<Self> {
        match generator {
            Generator::RandomBytes(len) => {
                let mut bytes = vec![0u8; *len];
                resolver.rng.fill(&mut bytes[..]);
                Ok(Bytes::from(bytes))
            }
            Generator::Const(v) => {
                let s: String = serde_yaml::from_value(v.clone())?;
                Ok(s.parse()?)
            }
            _ => bail!("cannot generate Bytes from {:?}", generator),
        }
    }
}

impl FromGenerator for String {
    fn from_generator(generator: &Generator, resolver: &mut ValueResolver<'_>) -> Result<Self> {
        match generator {
            Generator::Const(v) => Ok(serde_yaml::from_value(v.clone())?),
            Generator::Choice(choices) => {
                let idx = resolver.rng.random_range(0..choices.len());
                Ok(serde_yaml::from_value(choices[idx].clone())?)
            }
            _ => bail!("cannot generate String from {:?}", generator),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};

    #[test]
    fn test_uniform_u64() {
        let accounts = AccountManager::empty();
        let mut rng = StdRng::seed_from_u64(42);
        let mut resolver = ValueResolver { accounts: &accounts, rng: &mut rng };

        let generator = Generator::Uniform([1, 100]);
        let val: u64 = u64::from_generator(&generator, &mut resolver).unwrap();
        assert!((1..=100).contains(&val));
    }

    #[test]
    fn test_const_address() {
        let accounts = AccountManager::empty();
        let mut rng = StdRng::seed_from_u64(42);
        let mut resolver = ValueResolver { accounts: &accounts, rng: &mut rng };

        let generator = Generator::Const(serde_yaml::Value::String(
            "0x0000000000000000000000000000000000000001".to_string(),
        ));
        let addr: Address = Address::from_generator(&generator, &mut resolver).unwrap();
        assert_eq!(
            addr,
            Address::from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
        );
    }
}
