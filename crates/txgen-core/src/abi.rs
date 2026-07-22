use alloy_dyn_abi::DynSolValue;
use alloy_json_abi::{JsonAbi, Param};
use alloy_primitives::{Address, Bytes, B256, U256};
use eyre::{bail, ensure, Result, WrapErr};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::PathBuf,
};

use crate::{value::parse_generator, GenValue, Generator, ValueResolver};

/// Artifact definition in the workload spec.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ArtifactDef {
    /// Path to an ABI JSON file or a compiler artifact containing an `abi` field.
    Path(PathBuf),
    /// Separate ABI and bytecode paths. If `abi` is omitted, an empty ABI is used. `bytecode`
    /// may point at a raw hex file or compiler artifact.
    Object { abi: Option<PathBuf>, bytecode: Option<PathBuf> },
}

#[derive(Debug)]
struct Artifact {
    abi: JsonAbi,
    bytecode: Option<Bytes>,
}

/// Manages loaded ABI/deployment artifacts.
#[derive(Debug, Default)]
pub struct ArtifactManager {
    artifacts: HashMap<String, Artifact>,
}

impl ArtifactManager {
    /// Load artifacts from path mappings.
    pub fn load(
        artifacts: &HashMap<String, ArtifactDef>,
        base_path: &std::path::Path,
    ) -> Result<Self> {
        let mut loaded = HashMap::new();

        for (name, def) in artifacts {
            let artifact = match def {
                ArtifactDef::Path(path) => load_artifact(Some(path), None, base_path)?,
                ArtifactDef::Object { abi, bytecode } => {
                    load_artifact(abi.as_ref(), bytecode.as_ref(), base_path)?
                }
            };
            loaded.insert(name.clone(), artifact);
        }

        Ok(Self { artifacts: loaded })
    }

    /// Create an empty artifact manager (for testing).
    pub fn empty() -> Self {
        Self::default()
    }

    /// Get an ABI by name.
    pub fn get(&self, name: &str) -> Result<&JsonAbi> {
        Ok(&self
            .artifacts
            .get(name)
            .ok_or_else(|| eyre::eyre!("artifact '{}' not found", name))?
            .abi)
    }

    /// Build EVM initcode by appending ABI-encoded constructor arguments to bytecode.
    pub fn encode_constructor(
        &self,
        name: &str,
        args: &[serde_yaml::Value],
        resolver: &mut ValueResolver<'_>,
    ) -> Result<Bytes> {
        let artifact =
            self.artifacts.get(name).ok_or_else(|| eyre::eyre!("artifact '{}' not found", name))?;
        let bytecode = artifact
            .bytecode
            .as_ref()
            .ok_or_else(|| eyre::eyre!("artifact '{}' has no bytecode", name))?;
        let mut initcode = bytecode.to_vec();

        let inputs = artifact
            .abi
            .constructor()
            .map(|constructor| constructor.inputs.as_slice())
            .unwrap_or(&[]);
        if inputs.len() != args.len() {
            bail!(
                "constructor for artifact '{}' expects {} arguments, got {}",
                name,
                inputs.len(),
                args.len()
            );
        }

        let mut encoded_args = Vec::with_capacity(args.len());
        for (arg, param) in args.iter().zip(inputs) {
            encoded_args.push(yaml_to_sol_value(arg, &param.ty.to_string(), resolver)?);
        }
        initcode.extend_from_slice(&DynSolValue::Tuple(encoded_args).abi_encode_params());

        Ok(Bytes::from(initcode))
    }
}

fn load_artifact(
    abi_path: Option<&PathBuf>,
    bytecode_path: Option<&PathBuf>,
    base_path: &std::path::Path,
) -> Result<Artifact> {
    let json = abi_path
        .map(|path| {
            let path = resolve_path(path, base_path);
            let content = std::fs::read_to_string(&path)
                .wrap_err_with(|| format!("failed to read artifact: {}", path.display()))?;
            serde_json::from_str(&content)
                .wrap_err_with(|| format!("failed to parse artifact JSON: {}", path.display()))
        })
        .transpose()?;

    let abi = match (&json, abi_path) {
        (Some(json), Some(path)) => parse_abi_json(json).wrap_err_with(|| {
            format!("failed to parse ABI: {}", resolve_path(path, base_path).display())
        })?,
        _ => JsonAbi::default(),
    };
    let bytecode = if let Some(path) = bytecode_path {
        Some(load_bytecode(path, base_path)?)
    } else {
        json.as_ref().and_then(parse_bytecode_json).transpose()?
    };

    Ok(Artifact { abi, bytecode })
}

fn resolve_path(path: &PathBuf, base_path: &std::path::Path) -> PathBuf {
    if path.is_absolute() {
        path.clone()
    } else {
        base_path.join(path)
    }
}

fn parse_abi_json(json: &serde_json::Value) -> Result<JsonAbi> {
    if let Some(abi) = json.get("abi") {
        Ok(serde_json::from_value(abi.clone())?)
    } else {
        Ok(serde_json::from_value(json.clone())?)
    }
}

fn load_bytecode(path: &PathBuf, base_path: &std::path::Path) -> Result<Bytes> {
    let path = resolve_path(path, base_path);
    let content = std::fs::read_to_string(&path)
        .wrap_err_with(|| format!("failed to read bytecode: {}", path.display()))?;
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&content) &&
        let Some(bytecode) = parse_bytecode_json(&json).transpose()?
    {
        return Ok(bytecode);
    }
    parse_bytecode_str(content.trim())
        .wrap_err_with(|| format!("failed to parse bytecode: {}", path.display()))
}

fn parse_bytecode_json(json: &serde_json::Value) -> Option<Result<Bytes>> {
    let bytecode = json.get("bytecode")?;
    if let Some(s) = bytecode.as_str() {
        return Some(parse_bytecode_str(s));
    }
    if let Some(s) = bytecode.get("object").and_then(|value| value.as_str()) {
        return Some(parse_bytecode_str(s));
    }
    Some(Err(eyre::eyre!("unsupported bytecode JSON shape")))
}

fn parse_bytecode_str(value: &str) -> Result<Bytes> {
    let hex = value.strip_prefix("0x").unwrap_or(value);
    Ok(Bytes::from(hex::decode(hex)?))
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
    pub args: CallArgs,

    /// Value to send with the call.
    #[serde(default)]
    pub value: GenValue<U256>,
}

/// Function arguments for a contract call.
#[derive(Debug, Clone)]
pub enum CallArgs {
    /// A fixed argument list.
    List(Vec<serde_yaml::Value>),
    /// Evaluate local variables and use them to materialize an argument list.
    Vars(CallArgsVars),
}

/// Local variable definitions and values for a contract call argument list.
#[derive(Debug, Clone, Deserialize)]
pub struct CallArgsVars {
    /// Variables resolved once per call before encoding `values`.
    ///
    /// The map keys form the local `{ var: ... }` namespace for this argument block.
    #[serde(default)]
    vars: BTreeMap<String, serde_yaml::Value>,
    /// Argument expressions to encode after variables are resolved.
    values: Vec<serde_yaml::Value>,
}

impl Default for CallArgs {
    fn default() -> Self {
        Self::List(Vec::new())
    }
}

impl CallArgs {
    fn resolve(&self, resolver: &mut ValueResolver<'_>) -> Result<Vec<serde_yaml::Value>> {
        match self {
            Self::List(args) => Ok(args.clone()),
            Self::Vars(args) => {
                let mut resolved = HashMap::new();
                let mut resolving = HashSet::new();

                for name in args.vars.keys() {
                    resolve_call_arg_var(
                        name,
                        &args.vars,
                        &mut resolved,
                        &mut resolving,
                        resolver,
                    )?;
                }

                args.values
                    .iter()
                    .map(|value| {
                        eval_call_arg_expr(
                            value,
                            &args.vars,
                            &mut resolved,
                            &mut resolving,
                            resolver,
                        )
                    })
                    .collect()
            }
        }
    }
}

impl<'de> Deserialize<'de> for CallArgs {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_yaml::Value::deserialize(deserializer)?;
        match value {
            serde_yaml::Value::Null => Ok(Self::default()),
            serde_yaml::Value::Sequence(args) => Ok(Self::List(args)),
            serde_yaml::Value::Mapping(mapping) => {
                let vars_key = serde_yaml::Value::String("vars".to_string());
                let values_key = serde_yaml::Value::String("values".to_string());
                if mapping.contains_key(&vars_key) || mapping.contains_key(&values_key) {
                    let args = CallArgsVars::deserialize(serde_yaml::Value::Mapping(mapping))
                        .map_err(serde::de::Error::custom)?;
                    return Ok(Self::Vars(args));
                }

                Err(serde::de::Error::custom(
                    "call args must be a list or `{ vars: {...}, values: [...] }`",
                ))
            }
            _ => Err(serde::de::Error::custom(
                "call args must be a list or `{ vars: {...}, values: [...] }`",
            )),
        }
    }
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
            let args = self.args.resolve(resolver)?;
            encode_function_call(abi, &self.function, &args, resolver)?
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
        let sol_value = yaml_to_param_value(arg, param, resolver)?;
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

fn yaml_to_param_value(
    value: &serde_yaml::Value,
    param: &Param,
    resolver: &mut ValueResolver<'_>,
) -> Result<DynSolValue> {
    if param.components.is_empty() {
        return yaml_to_sol_value(value, &param.ty, resolver);
    }

    if param.ty == "tuple" {
        return yaml_to_tuple_value(value, &param.components, resolver);
    }

    let Some(length) = param.ty.strip_prefix("tuple[").and_then(|ty| ty.strip_suffix(']')) else {
        bail!("unsupported compound Solidity type: {}", param.ty);
    };
    let values = value.as_sequence().ok_or_else(|| eyre::eyre!("{} must be a list", param.ty))?;
    let values = values
        .iter()
        .map(|value| yaml_to_tuple_value(value, &param.components, resolver))
        .collect::<Result<Vec<_>>>()?;

    if length.is_empty() {
        Ok(DynSolValue::Array(values))
    } else {
        let expected: usize = length.parse()?;
        if values.len() != expected {
            bail!("{} expects {expected} values, got {}", param.ty, values.len());
        }
        Ok(DynSolValue::FixedArray(values))
    }
}

fn yaml_to_tuple_value(
    value: &serde_yaml::Value,
    components: &[Param],
    resolver: &mut ValueResolver<'_>,
) -> Result<DynSolValue> {
    let values = if let Some(values) = value.as_sequence() {
        if values.len() != components.len() {
            bail!("tuple expects {} values, got {}", components.len(), values.len());
        }
        values
            .iter()
            .zip(components)
            .map(|(value, component)| yaml_to_param_value(value, component, resolver))
            .collect::<Result<Vec<_>>>()?
    } else if let Some(mapping) = value.as_mapping() {
        if mapping.len() != components.len() {
            bail!("tuple expects {} fields, got {}", components.len(), mapping.len());
        }
        let mut values = Vec::with_capacity(components.len());
        for component in components {
            if component.name.is_empty() {
                bail!("unnamed tuple components must be supplied as a list");
            }
            let key = serde_yaml::Value::String(component.name.clone());
            let value = mapping
                .get(&key)
                .ok_or_else(|| eyre::eyre!("tuple is missing field '{}'", component.name))?;
            values.push(yaml_to_param_value(value, component, resolver)?);
        }
        values
    } else {
        bail!("tuple must be a list or mapping");
    };

    Ok(DynSolValue::Tuple(values))
}

fn resolve_call_arg_var(
    name: &str,
    vars: &BTreeMap<String, serde_yaml::Value>,
    resolved: &mut HashMap<String, serde_yaml::Value>,
    resolving: &mut HashSet<String>,
    resolver: &mut ValueResolver<'_>,
) -> Result<serde_yaml::Value> {
    if let Some(value) = resolved.get(name) {
        return Ok(value.clone());
    }

    let expr = vars.get(name).ok_or_else(|| eyre::eyre!("unknown call args variable '{name}'"))?;
    if !resolving.insert(name.to_string()) {
        bail!("circular call args variable dependency involving '{name}'");
    }

    let value = eval_call_arg_expr(expr, vars, resolved, resolving, resolver)
        .wrap_err_with(|| format!("failed to resolve call args variable '{name}'"))?;
    resolving.remove(name);
    resolved.insert(name.to_string(), value.clone());
    Ok(value)
}

fn eval_call_arg_expr(
    expr: &serde_yaml::Value,
    vars: &BTreeMap<String, serde_yaml::Value>,
    resolved: &mut HashMap<String, serde_yaml::Value>,
    resolving: &mut HashSet<String>,
    resolver: &mut ValueResolver<'_>,
) -> Result<serde_yaml::Value> {
    match expr {
        serde_yaml::Value::Mapping(mapping) => {
            if let Some(value) = expression_value(mapping, "var")? {
                let name: String = serde_yaml::from_value(value.clone())?;
                return resolve_call_arg_var(&name, vars, resolved, resolving, resolver);
            }

            if let Some(value) = expression_value(mapping, "if")? {
                return eval_call_arg_if(value, vars, resolved, resolving, resolver);
            }

            let mut evaluated = serde_yaml::Mapping::new();
            for (key, value) in mapping {
                evaluated.insert(
                    key.clone(),
                    eval_call_arg_expr(value, vars, resolved, resolving, resolver)?,
                );
            }
            resolver.resolve_yaml(&serde_yaml::Value::Mapping(evaluated))
        }
        serde_yaml::Value::Sequence(values) => values
            .iter()
            .map(|value| eval_call_arg_expr(value, vars, resolved, resolving, resolver))
            .collect::<Result<Vec<_>>>()
            .map(serde_yaml::Value::Sequence),
        _ => Ok(expr.clone()),
    }
}

fn expression_value<'a>(
    mapping: &'a serde_yaml::Mapping,
    key: &str,
) -> Result<Option<&'a serde_yaml::Value>> {
    let key_value = serde_yaml::Value::String(key.to_string());
    let Some(value) = mapping.get(&key_value) else {
        return Ok(None);
    };

    if mapping.len() != 1 {
        bail!("call args expression '{key}' cannot be combined with other keys");
    }
    Ok(Some(value))
}

fn eval_call_arg_if(
    value: &serde_yaml::Value,
    vars: &BTreeMap<String, serde_yaml::Value>,
    resolved: &mut HashMap<String, serde_yaml::Value>,
    resolving: &mut HashSet<String>,
    resolver: &mut ValueResolver<'_>,
) -> Result<serde_yaml::Value> {
    let mapping = value
        .as_mapping()
        .ok_or_else(|| eyre::eyre!("call args if expression must be a mapping"))?;
    let cond = required_mapping_value(mapping, "cond")?;
    let then_value = required_mapping_value(mapping, "then")?;
    let else_value = required_mapping_value(mapping, "else")?;

    let cond = eval_call_arg_bool(cond, vars, resolved, resolving, resolver, "if cond")?;
    if cond {
        eval_call_arg_expr(then_value, vars, resolved, resolving, resolver)
    } else {
        eval_call_arg_expr(else_value, vars, resolved, resolving, resolver)
    }
}

fn required_mapping_value<'a>(
    mapping: &'a serde_yaml::Mapping,
    key: &str,
) -> Result<&'a serde_yaml::Value> {
    let key_value = serde_yaml::Value::String(key.to_string());
    mapping.get(&key_value).ok_or_else(|| eyre::eyre!("missing call args expression key '{key}'"))
}

fn eval_call_arg_bool(
    value: &serde_yaml::Value,
    vars: &BTreeMap<String, serde_yaml::Value>,
    resolved: &mut HashMap<String, serde_yaml::Value>,
    resolving: &mut HashSet<String>,
    resolver: &mut ValueResolver<'_>,
    context: &str,
) -> Result<bool> {
    let value = eval_call_arg_expr(value, vars, resolved, resolving, resolver)?;
    serde_yaml::from_value(value).wrap_err_with(|| format!("call args {context} must be a bool"))
}

/// Convert a YAML value to a DynSolValue based on the expected Solidity type.
fn yaml_to_sol_value(
    value: &serde_yaml::Value,
    sol_type: &str,
    resolver: &mut ValueResolver<'_>,
) -> Result<DynSolValue> {
    // Handle generator expressions
    if parse_generator(value).is_some() {
        return resolve_generator_to_sol(value, sol_type, resolver);
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
            let val: U256 = serde_yaml::from_value(value.clone())
                .wrap_err_with(|| format!("invalid {t} literal"))?;
            Ok(DynSolValue::Uint(val, parse_uint_bits(t)?))
        }
        t if t.starts_with("int") => {
            let val: i64 = serde_yaml::from_value(value.clone())?;
            let bits = parse_int_bits(t)?;
            Ok(DynSolValue::Int(alloy_primitives::I256::try_from(val)?, bits))
        }
        t if t.starts_with("bytes") && t.len() > 5 => {
            // Fixed bytes (bytes1, bytes32, etc.)
            let s: String = serde_yaml::from_value(value.clone())?;
            let bytes: Bytes = s.parse()?;
            let size: usize = t[5..].parse()?;
            ensure!(size <= 32, "invalid fixed bytes size {size}");
            ensure!(bytes.len() == size, "{t} expects {size} bytes, got {}", bytes.len());
            let mut fixed = [0u8; 32];
            fixed[..size].copy_from_slice(&bytes);
            Ok(DynSolValue::FixedBytes(B256::from(fixed), size))
        }
        t if t.ends_with("[]") => {
            // Dynamic array
            let inner_type = &t[..t.len() - 2];
            let arr: Vec<serde_yaml::Value> = serde_yaml::from_value(value.clone())?;
            let values: Result<Vec<_>> =
                arr.iter().map(|v| yaml_to_sol_value(v, inner_type, resolver)).collect();
            Ok(DynSolValue::Array(values?))
        }
        _ => {
            bail!("unsupported Solidity type: {}", sol_type);
        }
    }
}

fn resolve_generator_to_sol(
    value: &serde_yaml::Value,
    sol_type: &str,
    resolver: &mut ValueResolver<'_>,
) -> Result<DynSolValue> {
    if matches!(parse_generator(value), Some(Generator::Random)) {
        let value = match sol_type {
            "address" => serde_yaml::Value::String(resolver.resolve::<Address>(value)?.to_string()),
            t if t.starts_with("uint") => {
                serde_yaml::Value::String(resolver.resolve::<U256>(value)?.to_string())
            }
            _ => {
                bail!("generator not supported for type: {}", sol_type);
            }
        };
        return yaml_to_sol_value(&value, sol_type, resolver);
    }

    let value = resolver.resolve_yaml(value)?;
    yaml_to_sol_value(&value, sol_type, resolver)
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
    use crate::{AccountManager, AddressPoolManager, ValueResolver};
    use rand::SeedableRng;

    #[test]
    fn test_artifact_manager_empty() {
        let manager = ArtifactManager::empty();
        assert!(manager.get("nonexistent").is_err());
    }

    #[test]
    fn test_bytecode_only_artifact_defaults_to_empty_abi() -> Result<()> {
        let dir = std::env::temp_dir().join(format!(
            "txgen-bytecode-only-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join("contract.bin"), "0x6000")?;

        let definitions = HashMap::from([(
            "contract".to_string(),
            ArtifactDef::Object { abi: None, bytecode: Some("contract.bin".into()) },
        )]);
        let manager = ArtifactManager::load(&definitions, &dir)?;
        let accounts = AccountManager::empty();
        let address_pools = AddressPoolManager::empty();
        let mut rng = rand::rng();
        let mut resolver =
            ValueResolver { accounts: &accounts, address_pools: &address_pools, rng: &mut rng };

        assert_eq!(manager.get("contract")?, &JsonAbi::default());
        assert_eq!(
            manager.encode_constructor("contract", &[], &mut resolver)?,
            Bytes::from_static(&[0x60, 0x00])
        );
        std::fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[test]
    fn test_negative_uint_literal_fails() {
        let accounts = AccountManager::empty();
        let address_pools = AddressPoolManager::empty();
        let mut rng = rand::rng();
        let mut resolver =
            ValueResolver { accounts: &accounts, address_pools: &address_pools, rng: &mut rng };
        let value = serde_yaml::from_str::<serde_yaml::Value>("-1").expect("valid YAML");

        let err = yaml_to_sol_value(&value, "uint256", &mut resolver)
            .expect_err("negative uint literals should fail");

        assert!(err.to_string().contains("invalid uint256 literal"));
    }

    #[test]
    fn test_hex_uint_literal() {
        let accounts = AccountManager::empty();
        let address_pools = AddressPoolManager::empty();
        let mut rng = rand::rng();
        let mut resolver =
            ValueResolver { accounts: &accounts, address_pools: &address_pools, rng: &mut rng };
        let value = serde_yaml::from_str::<serde_yaml::Value>("\"0x10\"").expect("valid YAML");

        let sol_value = yaml_to_sol_value(&value, "uint256", &mut resolver).unwrap();

        assert_eq!(sol_value, DynSolValue::Uint(U256::from(16), 256));
    }

    #[test]
    fn test_named_tuple_literal() {
        let param: Param = serde_json::from_value(serde_json::json!({
            "name": "encrypted",
            "type": "tuple",
            "internalType": "struct EncryptedDepositPayload",
            "components": [
                { "name": "ephemeralPubkeyX", "type": "bytes32" },
                { "name": "ephemeralPubkeyYParity", "type": "uint8" },
                { "name": "ciphertext", "type": "bytes" },
                { "name": "nonce", "type": "bytes12" },
                { "name": "tag", "type": "bytes16" }
            ]
        }))
        .unwrap();
        let value = serde_yaml::from_str::<serde_yaml::Value>(
            r#"
ephemeralPubkeyX: "0x1111111111111111111111111111111111111111111111111111111111111111"
ephemeralPubkeyYParity: 3
ciphertext: "0x1234"
nonce: "0x222222222222222222222222"
tag: "0x33333333333333333333333333333333"
"#,
        )
        .unwrap();
        let accounts = AccountManager::empty();
        let address_pools = AddressPoolManager::empty();
        let mut rng = rand::rng();
        let mut resolver =
            ValueResolver { accounts: &accounts, address_pools: &address_pools, rng: &mut rng };

        let actual = yaml_to_param_value(&value, &param, &mut resolver).unwrap();
        let mut nonce = [0u8; 32];
        nonce[..12].fill(0x22);
        let mut tag = [0u8; 32];
        tag[..16].fill(0x33);

        assert_eq!(
            actual,
            DynSolValue::Tuple(vec![
                DynSolValue::FixedBytes(B256::repeat_byte(0x11), 32,),
                DynSolValue::Uint(U256::from(3), 8),
                DynSolValue::Bytes(vec![0x12, 0x34]),
                DynSolValue::FixedBytes(B256::from(nonce), 12),
                DynSolValue::FixedBytes(B256::from(tag), 16),
            ])
        );
    }

    #[test]
    fn test_fixed_bytes_literal_rejects_wrong_length() {
        let accounts = AccountManager::empty();
        let address_pools = AddressPoolManager::empty();
        let mut rng = rand::rng();
        let mut resolver =
            ValueResolver { accounts: &accounts, address_pools: &address_pools, rng: &mut rng };
        let value = serde_yaml::Value::String("0x22".to_string());

        let error = yaml_to_sol_value(&value, "bytes12", &mut resolver).unwrap_err();

        assert!(error.to_string().contains("bytes12 expects 12 bytes, got 1"));
    }

    #[test]
    fn test_fractional_uint_literal_fails() {
        let accounts = AccountManager::empty();
        let address_pools = AddressPoolManager::empty();
        let mut rng = rand::rng();
        let mut resolver =
            ValueResolver { accounts: &accounts, address_pools: &address_pools, rng: &mut rng };
        let value = serde_yaml::from_str::<serde_yaml::Value>("1.5").expect("valid YAML");

        let err = yaml_to_sol_value(&value, "uint256", &mut resolver)
            .expect_err("fractional uint literals should fail");

        assert!(err.to_string().contains("invalid uint256 literal"));
    }

    #[test]
    fn test_address_pool_generator_in_abi_arg() -> Result<()> {
        let accounts = AccountManager::empty();
        let expected = Address::from([9u8; 20]);
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
        let mut rng = rand::rng();
        let mut resolver =
            ValueResolver { accounts: &accounts, address_pools: &address_pools, rng: &mut rng };
        let value = serde_yaml::from_str::<serde_yaml::Value>(
            r#"
address_pool:
  pool: recipients
  select: { index: 0 }
"#,
        )?;

        let sol_value = yaml_to_sol_value(&value, "address", &mut resolver)?;

        assert_eq!(sol_value, DynSolValue::Address(expected));
        Ok(())
    }

    #[test]
    fn test_random_generator_in_abi_arg() -> Result<()> {
        let accounts = AccountManager::empty();
        let address_pools = AddressPoolManager::empty();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut resolver =
            ValueResolver { accounts: &accounts, address_pools: &address_pools, rng: &mut rng };
        let value = serde_yaml::Value::String("random".to_string());

        assert!(matches!(
            yaml_to_sol_value(&value, "address", &mut resolver)?,
            DynSolValue::Address(_)
        ));
        assert!(matches!(
            yaml_to_sol_value(&value, "uint256", &mut resolver)?,
            DynSolValue::Uint(_, 256)
        ));
        Ok(())
    }

    #[test]
    fn test_call_args_vars_resolve_dependent_dex_ticks() -> Result<()> {
        let args: CallArgs = serde_yaml::from_str(
            r#"
vars:
  is_bid:
    choice: [true, false]
  tick:
    if:
      cond: { var: is_bid }
      then: { uniform: { min: -30, max: 0, step: 10 } }
      else: { uniform: { min: 0, max: 30, step: 10 } }
values:
  - "0x20c0000000000000000000000000000000000001"
  - { uniform: { min: 100000000, max: 500000000, step: 100000000 } }
  - { var: is_bid }
  - { var: tick }
  - if:
      cond: { var: is_bid }
      then: { uniform: { min: { var: tick }, max: 30, step: 10 } }
      else: { uniform: { min: -30, max: { var: tick }, step: 10 } }
"#,
        )?;
        let accounts = AccountManager::empty();
        let address_pools = AddressPoolManager::empty();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut resolver =
            ValueResolver { accounts: &accounts, address_pools: &address_pools, rng: &mut rng };

        for _ in 0..128 {
            let values = args.resolve(&mut resolver)?;
            assert_eq!(values.len(), 5);

            let amount: i64 = serde_yaml::from_value(values[1].clone())?;
            let is_bid: bool = serde_yaml::from_value(values[2].clone())?;
            let tick: i64 = serde_yaml::from_value(values[3].clone())?;
            let flip_tick: i64 = serde_yaml::from_value(values[4].clone())?;

            assert!((100000000..=500000000).contains(&amount));
            assert_eq!(amount % 100000000, 0);
            assert_eq!(tick % 10, 0);
            assert_eq!(flip_tick % 10, 0);

            if is_bid {
                assert!((-30..=0).contains(&tick));
                assert!((tick..=30).contains(&flip_tick));
            } else {
                assert!((0..=30).contains(&tick));
                assert!((-30..=tick).contains(&flip_tick));
            }

            assert!(matches!(
                yaml_to_sol_value(&values[1], "uint128", &mut resolver)?,
                DynSolValue::Uint(_, 128)
            ));
            assert_eq!(
                yaml_to_sol_value(&values[2], "bool", &mut resolver)?,
                DynSolValue::Bool(is_bid)
            );
            assert!(matches!(
                yaml_to_sol_value(&values[3], "int16", &mut resolver)?,
                DynSolValue::Int(_, 16)
            ));
            assert!(matches!(
                yaml_to_sol_value(&values[4], "int16", &mut resolver)?,
                DynSolValue::Int(_, 16)
            ));
        }

        Ok(())
    }

    #[test]
    fn test_call_args_vars_reject_circular_dependencies() {
        let args: CallArgs = serde_yaml::from_str(
            r#"
vars:
  a: { var: b }
  b: { var: a }
values:
  - { var: a }
"#,
        )
        .expect("valid call args");
        let accounts = AccountManager::empty();
        let address_pools = AddressPoolManager::empty();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut resolver =
            ValueResolver { accounts: &accounts, address_pools: &address_pools, rng: &mut rng };

        let err = args.resolve(&mut resolver).expect_err("cycle should fail");
        let err = format!("{err:?}");

        assert!(err.contains("circular call args variable dependency"));
    }

    #[test]
    fn test_call_args_vars_reject_empty_choice() {
        let args: CallArgs = serde_yaml::from_str(
            r#"
vars:
  side: { choice: [] }
values:
  - { var: side }
"#,
        )
        .expect("valid call args");
        let accounts = AccountManager::empty();
        let address_pools = AddressPoolManager::empty();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut resolver =
            ValueResolver { accounts: &accounts, address_pools: &address_pools, rng: &mut rng };

        let err = args.resolve(&mut resolver).expect_err("empty choice should fail");
        let err = format!("{err:?}");

        assert!(err.contains("choice generator must contain at least one value"));
    }
}
