use alloy_primitives::{Address, Bytes, B256, U256};
use eyre::{bail, Result};
use rand::Rng;
use serde::{de::DeserializeOwned, Deserialize};

use crate::{AccountManager, AddressPoolManager, SelectMode};

/// A value that can be either a literal or a generator expression.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
#[expect(clippy::large_enum_variant)]
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
    Uniform(UniformRange),
    /// Random choice from a list of values.
    Choice(Vec<serde_yaml::Value>),
    /// Account address from a signer pool.
    Pool { pool: String, select: SelectMode },
    /// Destination address from an address-only pool.
    AddressPool { pool: String, select: SelectMode },
    /// Random bytes of given length.
    RandomBytes(usize),
    /// Random value.
    Random,
    /// Explicit constant value.
    Const(serde_yaml::Value),
}

/// Uniform random integer range.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum UniformRange {
    /// Inclusive `[min, max]` range.
    Range([serde_yaml::Value; 2]),
    /// Inclusive range with optional step.
    Options {
        min: serde_yaml::Value,
        max: serde_yaml::Value,
        #[serde(default)]
        step: Option<serde_yaml::Value>,
    },
}

/// Resolver for generator expressions.
pub struct ValueResolver<'a> {
    pub accounts: &'a AccountManager,
    pub address_pools: &'a AddressPoolManager,
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

    /// Resolve a YAML generator expression to a YAML value.
    pub fn resolve_yaml(&mut self, value: &serde_yaml::Value) -> Result<serde_yaml::Value> {
        if let Some(generator) = parse_generator(value) {
            return serde_yaml::Value::from_generator(&generator, self);
        }

        Ok(value.clone())
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

pub(crate) fn parse_generator(value: &serde_yaml::Value) -> Option<Generator> {
    match serde_yaml::from_value::<GenValue<()>>(value.clone()).ok()? {
        GenValue::Generator(generator) => Some(generator),
        GenValue::Literal(_) => None,
    }
}

fn choose_yaml_value<'a>(
    choices: &'a [serde_yaml::Value],
    resolver: &mut ValueResolver<'_>,
) -> Result<&'a serde_yaml::Value> {
    if choices.is_empty() {
        bail!("choice generator must contain at least one value");
    }

    let idx = resolver.rng.random_range(0..choices.len());
    Ok(&choices[idx])
}

fn yaml_i128(value: &serde_yaml::Value, context: &str) -> Result<i128> {
    serde_yaml::from_value(value.clone())
        .map_err(|err| eyre::eyre!("{context} must be an integer: {err}"))
}

fn sample_uniform_i128(range: &UniformRange, resolver: &mut ValueResolver<'_>) -> Result<i128> {
    let (min, max, step) = match range {
        UniformRange::Range([min, max]) => {
            (yaml_i128(min, "uniform min")?, yaml_i128(max, "uniform max")?, 1)
        }
        UniformRange::Options { min, max, step } => (
            yaml_i128(min, "uniform min")?,
            yaml_i128(max, "uniform max")?,
            match step {
                Some(step) => yaml_i128(step, "uniform step")?,
                None => 1,
            },
        ),
    };

    if step <= 0 {
        bail!("uniform step must be greater than zero");
    }
    if min > max {
        bail!("uniform min must be less than or equal to max");
    }

    let span = max.checked_sub(min).ok_or_else(|| eyre::eyre!("uniform range overflowed"))?;
    let slots = span / step;
    let offset = resolver.rng.random_range(0..=slots);
    let distance =
        offset.checked_mul(step).ok_or_else(|| eyre::eyre!("uniform selection overflowed"))?;
    min.checked_add(distance).ok_or_else(|| eyre::eyre!("uniform selection overflowed"))
}

impl FromGenerator for u64 {
    fn from_generator(generator: &Generator, resolver: &mut ValueResolver<'_>) -> Result<Self> {
        match generator {
            Generator::Uniform(range) => Ok(sample_uniform_i128(range, resolver)?.try_into()?),
            Generator::Const(v) => Ok(serde_yaml::from_value(v.clone())?),
            Generator::Choice(choices) => {
                let idx = resolver.rng.random_range(0..choices.len());
                Ok(serde_yaml::from_value(choices[idx].clone())?)
            }
            Generator::Random => Ok(resolver.rng.random()),
            _ => bail!("cannot generate u64 from {:?}", generator),
        }
    }
}

impl FromGenerator for u128 {
    fn from_generator(generator: &Generator, resolver: &mut ValueResolver<'_>) -> Result<Self> {
        match generator {
            Generator::Uniform(range) => Ok(sample_uniform_i128(range, resolver)?.try_into()?),
            Generator::Const(v) => Ok(serde_yaml::from_value(v.clone())?),
            Generator::Choice(choices) => {
                let idx = resolver.rng.random_range(0..choices.len());
                Ok(serde_yaml::from_value(choices[idx].clone())?)
            }
            Generator::Random => Ok(resolver.rng.random()),
            _ => bail!("cannot generate u128 from {:?}", generator),
        }
    }
}

impl FromGenerator for U256 {
    fn from_generator(generator: &Generator, resolver: &mut ValueResolver<'_>) -> Result<Self> {
        match generator {
            Generator::Uniform(range) => {
                let val: u128 = sample_uniform_i128(range, resolver)?.try_into()?;
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
            Generator::Random => Ok(resolver.rng.random()),
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
            Generator::AddressPool { pool, select } => match select {
                SelectMode::Random => resolver.address_pools.get_random(pool, resolver.rng),
                SelectMode::Index(idx) => resolver.address_pools.get_by_index(pool, *idx),
            },
            Generator::Const(v) => {
                let s: String = serde_yaml::from_value(v.clone())?;
                Ok(s.parse()?)
            }
            Generator::Choice(choices) => {
                let idx = resolver.rng.random_range(0..choices.len());
                let s: String = serde_yaml::from_value(choices[idx].clone())?;
                Ok(s.parse()?)
            }
            Generator::Random => Ok(resolver.rng.random()),
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

impl FromGenerator for B256 {
    fn from_generator(generator: &Generator, resolver: &mut ValueResolver<'_>) -> Result<Self> {
        match generator {
            Generator::RandomBytes(len) => {
                if *len != 32 {
                    bail!("bytes32 random_bytes must be 32 bytes, got {len}");
                }
                let mut bytes = [0u8; 32];
                resolver.rng.fill(&mut bytes[..]);
                Ok(B256::from(bytes))
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
            Generator::Random => Ok(resolver.rng.random()),
            _ => bail!("cannot generate B256 from {:?}", generator),
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

impl FromGenerator for serde_yaml::Value {
    fn from_generator(generator: &Generator, resolver: &mut ValueResolver<'_>) -> Result<Self> {
        match generator {
            Generator::Uniform(range) => {
                Ok(serde_yaml::to_value(sample_uniform_i128(range, resolver)?)?)
            }
            Generator::Choice(choices) => {
                let value = choose_yaml_value(choices, resolver)?.clone();
                resolver.resolve_yaml(&value)
            }
            Generator::Pool { pool, select } => {
                let signer = match select {
                    SelectMode::Random => resolver.accounts.get_random(pool, resolver.rng)?,
                    SelectMode::Index(idx) => resolver.accounts.get_by_index(pool, *idx)?,
                };
                Ok(serde_yaml::Value::String(signer.address().to_string()))
            }
            Generator::AddressPool { pool, select } => {
                let address = match select {
                    SelectMode::Random => resolver.address_pools.get_random(pool, resolver.rng)?,
                    SelectMode::Index(idx) => resolver.address_pools.get_by_index(pool, *idx)?,
                };
                Ok(serde_yaml::Value::String(address.to_string()))
            }
            Generator::RandomBytes(len) => {
                let mut bytes = vec![0u8; *len];
                resolver.rng.fill(&mut bytes[..]);
                Ok(serde_yaml::Value::String(Bytes::from(bytes).to_string()))
            }
            Generator::Const(value) => Ok(value.clone()),
            Generator::Random => {
                bail!("cannot resolve untyped random generator to a YAML value")
            }
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
        let address_pools = AddressPoolManager::empty();
        let mut rng = StdRng::seed_from_u64(42);
        let mut resolver =
            ValueResolver { accounts: &accounts, address_pools: &address_pools, rng: &mut rng };

        let generator = Generator::Uniform(UniformRange::Range([
            serde_yaml::to_value(1).unwrap(),
            serde_yaml::to_value(100).unwrap(),
        ]));
        let val: u64 = u64::from_generator(&generator, &mut resolver).unwrap();
        assert!((1..=100).contains(&val));
    }

    #[test]
    fn test_uniform_i64_with_step() {
        let accounts = AccountManager::empty();
        let address_pools = AddressPoolManager::empty();
        let mut rng = StdRng::seed_from_u64(42);
        let mut resolver =
            ValueResolver { accounts: &accounts, address_pools: &address_pools, rng: &mut rng };
        let value = serde_yaml::from_str::<serde_yaml::Value>(
            r#"
uniform:
  min: -30
  max: 30
  step: 10
"#,
        )
        .unwrap();

        for _ in 0..32 {
            let val: i64 = serde_yaml::from_value(resolver.resolve_yaml(&value).unwrap()).unwrap();
            assert!((-30..=30).contains(&val));
            assert_eq!(val % 10, 0);
        }
    }

    #[test]
    fn test_const_address() {
        let accounts = AccountManager::empty();
        let address_pools = AddressPoolManager::empty();
        let mut rng = StdRng::seed_from_u64(42);
        let mut resolver =
            ValueResolver { accounts: &accounts, address_pools: &address_pools, rng: &mut rng };

        let generator = Generator::Const(serde_yaml::Value::String(
            "0x0000000000000000000000000000000000000001".to_string(),
        ));
        let addr: Address = Address::from_generator(&generator, &mut resolver).unwrap();
        assert_eq!(
            addr,
            Address::from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
        );
    }

    #[test]
    fn test_random() {
        let accounts = AccountManager::empty();
        let address_pools = AddressPoolManager::empty();
        let mut rng = StdRng::seed_from_u64(42);
        let mut resolver =
            ValueResolver { accounts: &accounts, address_pools: &address_pools, rng: &mut rng };

        let generator = Generator::Random;

        // Supported types: should succeed.
        assert!(u64::from_generator(&generator, &mut resolver).is_ok());
        assert!(u128::from_generator(&generator, &mut resolver).is_ok());
        assert!(U256::from_generator(&generator, &mut resolver).is_ok());
        assert!(Address::from_generator(&generator, &mut resolver).is_ok());
        assert!(B256::from_generator(&generator, &mut resolver).is_ok());

        // Unsupported types: should fail.
        assert!(Bytes::from_generator(&generator, &mut resolver).is_err());
        assert!(String::from_generator(&generator, &mut resolver).is_err());
    }

    #[test]
    fn test_address_pool_generator() -> Result<()> {
        let accounts = AccountManager::empty();
        let expected = Address::from([7u8; 20]);
        let address_pools = AddressPoolManager::from_spec(&std::collections::HashMap::from([(
            "recipients".to_string(),
            crate::AddressPoolDef {
                addresses: vec![expected],
                mnemonic: None,
                index: None,
                range: None,
                fast: None,
            },
        )]))?;
        let mut rng = StdRng::seed_from_u64(42);
        let mut resolver =
            ValueResolver { accounts: &accounts, address_pools: &address_pools, rng: &mut rng };

        let generator =
            Generator::AddressPool { pool: "recipients".to_string(), select: SelectMode::Index(0) };
        let address = Address::from_generator(&generator, &mut resolver)?;

        assert_eq!(address, expected);
        Ok(())
    }
}
