use alloy_primitives::{Address, Bytes, B256, U256};
use eyre::{bail, Result, WrapErr};
use rand::Rng;
use serde::{de::DeserializeOwned, Deserialize};

use crate::{AccountManager, SelectMode};

const ADDRESS_LEN: usize = 20;

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
    /// Random address, optionally with a fixed byte prefix.
    RandomAddress(RandomAddressDef),
    /// Random bytes of given length.
    RandomBytes(usize),
    /// Random value.
    Random,
    /// Explicit constant value.
    Const(serde_yaml::Value),
}

/// Configuration for random address generation.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RandomAddressDef {
    /// Optional hex byte prefix copied into the start of the address.
    #[serde(default)]
    pub prefix: Option<String>,
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
            Generator::Random => Ok(resolver.rng.random()),
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
            Generator::Random => Ok(resolver.rng.random()),
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
            Generator::RandomAddress(def) => random_address(def.prefix.as_deref(), resolver.rng),
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

fn random_address(prefix: Option<&str>, rng: &mut dyn rand::RngCore) -> Result<Address> {
    let prefix = prefix.map(parse_address_prefix).transpose()?.unwrap_or_default();
    let mut bytes = [0u8; ADDRESS_LEN];
    bytes[..prefix.len()].copy_from_slice(&prefix);
    rng.fill(&mut bytes[prefix.len()..]);
    Ok(Address::from(bytes))
}

fn parse_address_prefix(prefix: &str) -> Result<Bytes> {
    let bytes: Bytes = prefix.parse().wrap_err("invalid random_address prefix hex")?;
    if bytes.len() > ADDRESS_LEN {
        bail!("random_address prefix is too long: {} bytes (max {ADDRESS_LEN})", bytes.len());
    }
    Ok(bytes)
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

    #[test]
    fn test_random_address() {
        let accounts = AccountManager::empty();
        let mut rng = StdRng::seed_from_u64(42);
        let mut resolver = ValueResolver { accounts: &accounts, rng: &mut rng };
        let generator = Generator::RandomAddress(RandomAddressDef::default());

        let addr = Address::from_generator(&generator, &mut resolver)
            .expect("random_address should generate an address");

        assert_ne!(addr, Address::ZERO);
    }

    #[test]
    fn test_random_address_deterministic() {
        let accounts = AccountManager::empty();
        let generator = Generator::RandomAddress(RandomAddressDef::default());

        let mut rng_a = StdRng::seed_from_u64(42);
        let mut resolver_a = ValueResolver { accounts: &accounts, rng: &mut rng_a };
        let addr_a = Address::from_generator(&generator, &mut resolver_a)
            .expect("random_address should generate an address");

        let mut rng_b = StdRng::seed_from_u64(42);
        let mut resolver_b = ValueResolver { accounts: &accounts, rng: &mut rng_b };
        let addr_b = Address::from_generator(&generator, &mut resolver_b)
            .expect("random_address should generate an address");

        assert_eq!(addr_a, addr_b);
    }

    #[test]
    fn test_random_address_prefix() {
        let accounts = AccountManager::empty();
        let mut rng = StdRng::seed_from_u64(42);
        let mut resolver = ValueResolver { accounts: &accounts, rng: &mut rng };
        let generator = Generator::RandomAddress(RandomAddressDef {
            prefix: Some("0x00000000000000000000000000000000dead".to_string()),
        });

        let addr = Address::from_generator(&generator, &mut resolver)
            .expect("random_address with prefix should generate an address");

        let prefix = hex::decode("00000000000000000000000000000000dead")
            .expect("test prefix should be valid hex");
        assert_eq!(&addr.as_slice()[..prefix.len()], prefix.as_slice());
        assert_ne!(&addr.as_slice()[prefix.len()..], &[0u8; 2]);
    }

    #[test]
    fn test_random_address_from_yaml() {
        let accounts = AccountManager::empty();
        let mut rng = StdRng::seed_from_u64(42);
        let mut resolver = ValueResolver { accounts: &accounts, rng: &mut rng };
        let value: GenValue<Address> = serde_yaml::from_str("random_address: {}")
            .expect("random_address generator should parse from YAML");

        let addr =
            resolver.resolve_gen(&value).expect("random_address YAML should resolve to an address");

        assert_ne!(addr, Address::ZERO);
    }

    #[test]
    fn test_random() {
        let accounts = AccountManager::empty();
        let mut rng = StdRng::seed_from_u64(42);
        let mut resolver = ValueResolver { accounts: &accounts, rng: &mut rng };

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
}
