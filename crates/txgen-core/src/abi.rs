use alloy_dyn_abi::DynSolValue;
use alloy_json_abi::JsonAbi;
use alloy_primitives::{Address, Bytes, U256};
use eyre::{Result, WrapErr, bail};
use serde::Deserialize;
use std::{collections::HashMap, path::PathBuf};

use crate::{GenValue, ValueResolver};

/// Manages loaded ABI artifacts.
#[derive(Debug, Default)]
pub struct ArtifactManager {
    abis: HashMap<String, JsonAbi>,
}

impl ArtifactManager {
    /// Load artifacts from path mappings.
    pub fn load(artifacts: &HashMap<String, PathBuf>, base_path: &std::path::Path) -> Result<Self> {
        let mut abis = HashMap::new();

        for (name, path) in artifacts {
            let full_path = if path.is_absolute() {
                path.clone()
            } else {
                base_path.join(path)
            };

            let content = std::fs::read_to_string(&full_path)
                .wrap_err_with(|| format!("failed to read artifact: {}", full_path.display()))?;

            let abi: JsonAbi = serde_json::from_str(&content)
                .wrap_err_with(|| format!("failed to parse ABI: {}", full_path.display()))?;

            abis.insert(name.clone(), abi);
        }

        Ok(Self { abis })
    }

    /// Create an empty artifact manager (for testing).
    pub fn empty() -> Self {
        Self::default()
    }

    /// Get an ABI by name.
    pub fn get(&self, name: &str) -> Result<&JsonAbi> {
        self.abis
            .get(name)
            .ok_or_else(|| eyre::eyre!("artifact '{}' not found", name))
    }
}

/// Definition of a contract call in the workload spec.
#[derive(Debug, Clone, Deserialize)]
pub struct CallDef {
    /// Target contract address.
    pub to: GenValue<Address>,

    /// ABI artifact name (optional for raw calls).
    pub abi: Option<String>,

    /// Function name or signature.
    pub function: String,

    /// Function arguments.
    #[serde(default)]
    pub args: Vec<serde_yaml::Value>,

    /// Value to send with the call.
    #[serde(default)]
    pub value: GenValue<U256>,
}

/// Result of encoding a call.
#[derive(Debug, Clone)]
pub struct EncodedCall {
    /// Target address.
    pub to: Address,
    /// Call value.
    pub value: U256,
    /// Encoded calldata.
    pub input: Bytes,
}

impl CallDef {
    /// Encode this call definition into calldata.
    pub fn encode(
        &self,
        artifacts: &ArtifactManager,
        resolver: &mut ValueResolver<'_>,
    ) -> Result<EncodedCall> {
        let to = resolver.resolve_gen(&self.to)?;
        let value = resolver.resolve_gen(&self.value)?;

        let input = if let Some(abi_name) = &self.abi {
            let abi = artifacts.get(abi_name)?;
            encode_function_call(abi, &self.function, &self.args, resolver)?
        } else {
            // Raw function selector (4 bytes) - assume function is the hex selector
            self.function.parse()?
        };

        Ok(EncodedCall { to, value, input })
    }
}

/// Encode a function call using the ABI.
fn encode_function_call(
    abi: &JsonAbi,
    function: &str,
    args: &[serde_yaml::Value],
    resolver: &mut ValueResolver<'_>,
) -> Result<Bytes> {
    // Find the function - try exact match first, then by name only
    let func = if function.contains('(') {
        // Exact signature match
        abi.functions()
            .find(|f| f.signature() == function)
            .ok_or_else(|| eyre::eyre!("function '{}' not found in ABI", function))?
    } else {
        // Name-only match (take first overload)
        abi.function(function)
            .and_then(|funcs| funcs.first())
            .ok_or_else(|| eyre::eyre!("function '{}' not found in ABI", function))?
    };

    if func.inputs.len() != args.len() {
        bail!(
            "function '{}' expects {} arguments, got {}",
            function,
            func.inputs.len(),
            args.len()
        );
    }

    // Convert arguments to DynSolValue based on ABI types
    let mut encoded_args = Vec::with_capacity(args.len());
    for (arg, param) in args.iter().zip(&func.inputs) {
        let sol_value = yaml_to_sol_value(arg, &param.ty.to_string(), resolver)?;
        encoded_args.push(sol_value);
    }

    // Encode the call
    let selector = func.selector();
    let encoded_params = DynSolValue::Tuple(encoded_args).abi_encode_params();

    let mut calldata = Vec::with_capacity(4 + encoded_params.len());
    calldata.extend_from_slice(&selector[..]);
    calldata.extend_from_slice(&encoded_params);

    Ok(Bytes::from(calldata))
}

/// Convert a YAML value to a DynSolValue based on the expected Solidity type.
fn yaml_to_sol_value(
    value: &serde_yaml::Value,
    sol_type: &str,
    resolver: &mut ValueResolver<'_>,
) -> Result<DynSolValue> {
    // Handle generator expressions
    if value.is_mapping() {
        // Check if it's a generator
        if value.get("uniform").is_some()
            || value.get("choice").is_some()
            || value.get("pool").is_some()
            || value.get("random_bytes").is_some()
            || value.get("const").is_some()
        {
            return resolve_generator_to_sol(value, sol_type, resolver);
        }
    }

    // Direct value conversion
    match sol_type {
        "address" => {
            let s: String = serde_yaml::from_value(value.clone())?;
            let addr: Address = s.parse()?;
            Ok(DynSolValue::Address(addr))
        }
        "bool" => {
            let b: bool = serde_yaml::from_value(value.clone())?;
            Ok(DynSolValue::Bool(b))
        }
        "string" => {
            let s: String = serde_yaml::from_value(value.clone())?;
            Ok(DynSolValue::String(s))
        }
        "bytes" => {
            let s: String = serde_yaml::from_value(value.clone())?;
            let bytes: Bytes = s.parse()?;
            Ok(DynSolValue::Bytes(bytes.to_vec()))
        }
        t if t.starts_with("uint") => {
            let val: U256 = if value.is_number() {
                U256::from(value.as_u64().unwrap_or(0))
            } else {
                let s: String = serde_yaml::from_value(value.clone())?;
                s.parse()?
            };
            Ok(DynSolValue::Uint(val, parse_uint_bits(t)?))
        }
        t if t.starts_with("int") => {
            let val: i64 = serde_yaml::from_value(value.clone())?;
            let bits = parse_int_bits(t)?;
            Ok(DynSolValue::Int(
                alloy_primitives::I256::try_from(val)?,
                bits,
            ))
        }
        t if t.starts_with("bytes") && t.len() > 5 => {
            // Fixed bytes (bytes1, bytes32, etc.)
            let s: String = serde_yaml::from_value(value.clone())?;
            let bytes: Bytes = s.parse()?;
            let size: usize = t[5..].parse()?;
            let mut fixed = vec![0u8; size];
            let len = bytes.len().min(size);
            fixed[..len].copy_from_slice(&bytes[..len]);
            Ok(DynSolValue::FixedBytes(
                alloy_primitives::FixedBytes::from_slice(&fixed),
                size,
            ))
        }
        t if t.ends_with("[]") => {
            // Dynamic array
            let inner_type = &t[..t.len() - 2];
            let arr: Vec<serde_yaml::Value> = serde_yaml::from_value(value.clone())?;
            let values: Result<Vec<_>> = arr
                .iter()
                .map(|v| yaml_to_sol_value(v, inner_type, resolver))
                .collect();
            Ok(DynSolValue::Array(values?))
        }
        _ => bail!("unsupported Solidity type: {}", sol_type),
    }
}

fn resolve_generator_to_sol(
    value: &serde_yaml::Value,
    sol_type: &str,
    resolver: &mut ValueResolver<'_>,
) -> Result<DynSolValue> {
    match sol_type {
        "address" => {
            let addr: Address = resolver.resolve(value)?;
            Ok(DynSolValue::Address(addr))
        }
        t if t.starts_with("uint") => {
            let val: U256 = resolver.resolve(value)?;
            Ok(DynSolValue::Uint(val, parse_uint_bits(t)?))
        }
        "bytes" => {
            let bytes: Bytes = resolver.resolve(value)?;
            Ok(DynSolValue::Bytes(bytes.to_vec()))
        }
        _ => bail!("generator not supported for type: {}", sol_type),
    }
}

fn parse_uint_bits(t: &str) -> Result<usize> {
    if t == "uint" {
        return Ok(256);
    }
    let bits: usize = t[4..].parse()?;
    Ok(bits)
}

fn parse_int_bits(t: &str) -> Result<usize> {
    if t == "int" {
        return Ok(256);
    }
    let bits: usize = t[3..].parse()?;
    Ok(bits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_artifact_manager_empty() {
        let manager = ArtifactManager::empty();
        assert!(manager.get("nonexistent").is_err());
    }
}
