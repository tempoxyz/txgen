use alloy_dyn_abi::{DynSolType, DynSolValue};
use alloy_primitives::{keccak256, Address, Bytes, B256, I256, U256};
use eyre::{bail, Result, WrapErr};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Typed, secret-free value stored in one scenario instance's runtime context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeValue {
    Null,
    Bool(bool),
    Int(I256),
    Uint(U256),
    Address(Address),
    Bytes(Bytes),
    Bytes32(B256),
    String(String),
    Array(Vec<RuntimeValue>),
    Object(BTreeMap<String, RuntimeValue>),
}

/// Immutable roots visible to runtime expressions for one scenario instance.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeContext {
    roots: Arc<BTreeMap<String, RuntimeValue>>,
}

impl RuntimeContext {
    /// Construct an empty runtime context.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Construct a context from a complete set of immutable roots.
    pub fn new(roots: BTreeMap<String, RuntimeValue>) -> Result<Self> {
        for name in roots.keys() {
            validate_root_name(name)?;
        }
        Ok(Self { roots: Arc::new(roots) })
    }

    /// Return the context roots without allowing mutation.
    pub fn roots(&self) -> &BTreeMap<String, RuntimeValue> {
        &self.roots
    }

    /// Return a new context containing one additional root.
    ///
    /// Existing contexts and values remain immutable. Duplicate roots are rejected.
    pub fn with_root(&self, name: impl Into<String>, value: RuntimeValue) -> Result<Self> {
        let name = name.into();
        validate_root_name(&name)?;
        if self.roots.contains_key(&name) {
            bail!("runtime root '{name}' already exists");
        }
        let mut roots = (*self.roots).clone();
        roots.insert(name, value);
        Ok(Self { roots: Arc::new(roots) })
    }

    /// Resolve a dotted object path. Decimal path components index arrays.
    pub fn get(&self, path: &str) -> Result<&RuntimeValue> {
        validate_variable_path(path)?;
        let mut parts = path.split('.');
        let root = parts.next().expect("validated non-empty path");
        let mut current = self
            .roots
            .get(root)
            .ok_or_else(|| eyre::eyre!("unknown runtime root '{root}' in variable '{path}'"))?;

        for part in parts {
            current = match current {
                RuntimeValue::Object(values) => values.get(part).ok_or_else(|| {
                    eyre::eyre!("runtime variable '{path}' has no field '{part}'")
                })?,
                RuntimeValue::Array(values) => {
                    let index: usize = part.parse().map_err(|_| {
                        eyre::eyre!(
                            "runtime variable '{path}' uses non-numeric array index '{part}'"
                        )
                    })?;
                    values.get(index).ok_or_else(|| {
                        eyre::eyre!("runtime variable '{path}' array index {index} is out of range")
                    })?
                }
                _ => {
                    bail!("runtime variable '{path}' traverses through scalar field '{part}'");
                }
            };
        }

        Ok(current)
    }
}

impl RuntimeValue {
    /// Convert a plain YAML value to a typed runtime value.
    ///
    /// Strings intentionally remain strings; address and byte coercion requires an
    /// expected ABI type or an already typed saved value.
    pub fn from_yaml(value: &serde_yaml::Value) -> Result<Self> {
        match value {
            serde_yaml::Value::Null => Ok(Self::Null),
            serde_yaml::Value::Bool(value) => Ok(Self::Bool(*value)),
            serde_yaml::Value::Number(value) => {
                if let Some(value) = value.as_i64() &&
                    value < 0
                {
                    return Ok(Self::Int(I256::try_from(value)?));
                }
                if let Some(value) = value.as_u64() {
                    return Ok(Self::Uint(U256::from(value)));
                }
                bail!("runtime numeric literals must be integers");
            }
            serde_yaml::Value::String(value) => Ok(Self::String(value.clone())),
            serde_yaml::Value::Sequence(values) => {
                values.iter().map(Self::from_yaml).collect::<Result<Vec<_>>>().map(Self::Array)
            }
            serde_yaml::Value::Mapping(values) => {
                let mut object = BTreeMap::new();
                for (key, value) in values {
                    let key = key
                        .as_str()
                        .ok_or_else(|| eyre::eyre!("runtime object keys must be strings"))?;
                    object.insert(key.to_string(), Self::from_yaml(value)?);
                }
                Ok(Self::Object(object))
            }
            serde_yaml::Value::Tagged(value) => Self::from_yaml(&value.value),
        }
    }

    /// Convert this value back to YAML for network-template deserialization.
    pub fn to_yaml(&self) -> Result<serde_yaml::Value> {
        match self {
            Self::Null => Ok(serde_yaml::Value::Null),
            Self::Bool(value) => Ok(serde_yaml::Value::Bool(*value)),
            Self::Int(value) => match i64::try_from(*value) {
                Ok(value) => Ok(serde_yaml::to_value(value)?),
                Err(_) => Ok(serde_yaml::Value::String(value.to_string())),
            },
            Self::Uint(value) if *value <= U256::from(u64::MAX) => {
                Ok(serde_yaml::to_value(value.to::<u64>())?)
            }
            Self::Uint(value) => Ok(serde_yaml::Value::String(value.to_string())),
            Self::Address(value) => Ok(serde_yaml::Value::String(value.to_string())),
            Self::Bytes(value) => Ok(serde_yaml::Value::String(value.to_string())),
            Self::Bytes32(value) => Ok(serde_yaml::Value::String(value.to_string())),
            Self::String(value) => Ok(serde_yaml::Value::String(value.clone())),
            Self::Array(values) => values
                .iter()
                .map(Self::to_yaml)
                .collect::<Result<Vec<_>>>()
                .map(serde_yaml::Value::Sequence),
            Self::Object(values) => {
                let mut mapping = serde_yaml::Mapping::new();
                for (key, value) in values {
                    mapping.insert(serde_yaml::Value::String(key.clone()), value.to_yaml()?);
                }
                Ok(serde_yaml::Value::Mapping(mapping))
            }
        }
    }

    /// Convert a decoded ABI value into a runtime value suitable for immutable saves.
    pub fn from_dyn_sol(value: &DynSolValue) -> Result<Self> {
        match value {
            DynSolValue::Bool(value) => Ok(Self::Bool(*value)),
            DynSolValue::Int(value, _) => Ok(Self::Int(*value)),
            DynSolValue::Uint(value, _) => Ok(Self::Uint(*value)),
            DynSolValue::FixedBytes(value, 32) => Ok(Self::Bytes32(*value)),
            DynSolValue::FixedBytes(value, size) => {
                if *size > 32 {
                    bail!("decoded fixed-bytes width {size} exceeds 32 bytes");
                }
                Ok(Self::Bytes(Bytes::copy_from_slice(&value[..*size])))
            }
            DynSolValue::Address(value) => Ok(Self::Address(*value)),
            DynSolValue::Function(value) => {
                Ok(Self::Bytes(Bytes::copy_from_slice(value.as_slice())))
            }
            DynSolValue::Bytes(value) => Ok(Self::Bytes(Bytes::copy_from_slice(value))),
            DynSolValue::String(value) => Ok(Self::String(value.clone())),
            DynSolValue::Array(values) |
            DynSolValue::FixedArray(values) |
            DynSolValue::Tuple(values) => {
                values.iter().map(Self::from_dyn_sol).collect::<Result<Vec<_>>>().map(Self::Array)
            }
            DynSolValue::CustomStruct { prop_names, tuple, .. } => {
                if prop_names.len() != tuple.len() {
                    bail!("decoded ABI struct has mismatched field names and values");
                }
                let mut object = BTreeMap::new();
                for (name, value) in prop_names.iter().zip(tuple) {
                    object.insert(name.clone(), Self::from_dyn_sol(value)?);
                }
                Ok(Self::Object(object))
            }
        }
    }

    /// Infer a Solidity value for deterministic packed encoding.
    pub fn infer_dyn_sol(&self) -> Result<DynSolValue> {
        match self {
            Self::Null => {
                bail!("cannot infer a Solidity type for null");
            }
            Self::Bool(value) => Ok(DynSolValue::Bool(*value)),
            Self::Int(value) => Ok(DynSolValue::Int(*value, 256)),
            Self::Uint(value) => Ok(DynSolValue::Uint(*value, 256)),
            Self::Address(value) => Ok(DynSolValue::Address(*value)),
            Self::Bytes(value) => Ok(DynSolValue::Bytes(value.to_vec())),
            Self::Bytes32(value) => Ok(DynSolValue::FixedBytes(*value, 32)),
            Self::String(value) => Ok(DynSolValue::String(value.clone())),
            Self::Array(values) => values
                .iter()
                .map(Self::infer_dyn_sol)
                .collect::<Result<Vec<_>>>()
                .map(DynSolValue::Array),
            Self::Object(_) => {
                bail!("cannot infer a Solidity type for an object");
            }
        }
    }

    /// Strictly coerce this value to an expected event or ABI parameter type.
    pub fn coerce_dyn_sol(&self, expected: &DynSolType) -> Result<DynSolValue> {
        match (self, expected) {
            (Self::Bytes(value), DynSolType::FixedBytes(size)) if value.len() != *size => {
                bail!("fixed bytes value has length {}, expected {}", value.len(), size);
            }
            (Self::Bytes32(_), DynSolType::FixedBytes(size)) if *size != 32 => {
                bail!("bytes32 value cannot be coerced to bytes{size}");
            }
            (Self::Array(values), DynSolType::FixedArray(_, size)) if values.len() != *size => {
                bail!("fixed array value has length {}, expected {}", values.len(), size);
            }
            _ => {}
        }

        let json = self.to_json();
        expected.coerce_json(&json).wrap_err_with(|| {
            format!("failed to coerce runtime value to Solidity type '{expected}'")
        })
    }

    fn to_json(&self) -> serde_json::Value {
        match self {
            Self::Null => serde_json::Value::Null,
            Self::Bool(value) => serde_json::Value::Bool(*value),
            Self::Int(value) => serde_json::Value::String(value.to_string()),
            Self::Uint(value) => serde_json::Value::String(value.to_string()),
            Self::Address(value) => serde_json::Value::String(value.to_string()),
            Self::Bytes(value) => serde_json::Value::String(value.to_string()),
            Self::Bytes32(value) => serde_json::Value::String(value.to_string()),
            Self::String(value) => serde_json::Value::String(value.clone()),
            Self::Array(values) => {
                serde_json::Value::Array(values.iter().map(Self::to_json).collect())
            }
            Self::Object(values) => serde_json::Value::Object(
                values.iter().map(|(key, value)| (key.clone(), value.to_json())).collect(),
            ),
        }
    }

    fn raw_hash_bytes(&self) -> Result<Vec<u8>> {
        match self {
            Self::String(value) => Ok(value.as_bytes().to_vec()),
            Self::Bytes(value) => Ok(value.to_vec()),
            Self::Bytes32(value) => Ok(value.as_slice().to_vec()),
            Self::Address(value) => Ok(value.as_slice().to_vec()),
            Self::Array(values) => values
                .iter()
                .map(|value| match value {
                    Self::Uint(value) if *value <= U256::from(u8::MAX) => Ok(value.to::<u8>()),
                    _ => {
                        bail!("keccak256 byte arrays must contain uint8 values");
                    }
                })
                .collect(),
            _ => {
                bail!(
                    "keccak256 requires bytes, bytes32, address, string, or a uint8 array; use ABI encoding for typed values"
                );
            }
        }
    }
}

/// Evaluate a runtime expression or recursively type a literal YAML value.
pub fn eval_expression(
    expression: &serde_yaml::Value,
    context: &RuntimeContext,
) -> Result<RuntimeValue> {
    if let serde_yaml::Value::Mapping(mapping) = expression &&
        let Some((operator, operand)) = expression_entry(mapping)?
    {
        return match operator {
            "var" => {
                let path = operand
                    .as_str()
                    .ok_or_else(|| eyre::eyre!("`var` expression path must be a string"))?;
                Ok(context.get(path)?.clone())
            }
            "keccak256" => {
                let value = eval_expression(operand, context)?;
                Ok(RuntimeValue::Bytes32(keccak256(value.raw_hash_bytes()?)))
            }
            "abi_encode" => {
                Ok(RuntimeValue::Bytes(Bytes::from(encode_typed_abi(operand, context, false)?)))
            }
            "abi_encode_packed" => {
                Ok(RuntimeValue::Bytes(Bytes::from(encode_packed(operand, context)?)))
            }
            "keccak256_packed" => {
                Ok(RuntimeValue::Bytes32(keccak256(encode_packed(operand, context)?)))
            }
            _ => unreachable!("expression_entry only returns known operators"),
        };
    }

    match expression {
        serde_yaml::Value::Sequence(values) => values
            .iter()
            .map(|value| eval_expression(value, context))
            .collect::<Result<Vec<_>>>()
            .map(RuntimeValue::Array),
        serde_yaml::Value::Mapping(values) => {
            let mut object = BTreeMap::new();
            for (key, value) in values {
                let key = key
                    .as_str()
                    .ok_or_else(|| eyre::eyre!("runtime object keys must be strings"))?;
                object.insert(key.to_string(), eval_expression(value, context)?);
            }
            Ok(RuntimeValue::Object(object))
        }
        serde_yaml::Value::Tagged(value) => eval_expression(&value.value, context),
        _ => RuntimeValue::from_yaml(expression),
    }
}

/// Recursively replace runtime expressions with YAML values suitable for template parsing.
pub fn materialize_yaml(
    value: &serde_yaml::Value,
    context: &RuntimeContext,
) -> Result<serde_yaml::Value> {
    match value {
        serde_yaml::Value::Mapping(mapping) if expression_entry(mapping)?.is_some() => {
            eval_expression(value, context)?.to_yaml()
        }
        serde_yaml::Value::Mapping(mapping) => {
            let mut materialized = serde_yaml::Mapping::new();
            for (key, value) in mapping {
                materialized.insert(key.clone(), materialize_yaml(value, context)?);
            }
            Ok(serde_yaml::Value::Mapping(materialized))
        }
        serde_yaml::Value::Sequence(values) => values
            .iter()
            .map(|value| materialize_yaml(value, context))
            .collect::<Result<Vec<_>>>()
            .map(serde_yaml::Value::Sequence),
        serde_yaml::Value::Tagged(value) => {
            let mut tagged = (**value).clone();
            tagged.value = materialize_yaml(&tagged.value, context)?;
            Ok(serde_yaml::Value::Tagged(Box::new(tagged)))
        }
        _ => Ok(value.clone()),
    }
}

/// Evaluate and coerce an event-filter expression to its ABI input type.
pub fn coerce_event_filter(
    expression: &serde_yaml::Value,
    expected: &DynSolType,
    context: &RuntimeContext,
) -> Result<DynSolValue> {
    eval_expression(expression, context)?.coerce_dyn_sol(expected)
}

/// Compare an expected runtime filter to a decoded event value using the event ABI type.
pub fn event_value_matches(
    expected: &RuntimeValue,
    actual: &DynSolValue,
    sol_type: &DynSolType,
) -> Result<bool> {
    Ok(expected.coerce_dyn_sol(sol_type)? == *actual)
}

/// Collect all `{ var: path }` references nested within a YAML value.
pub fn collect_variable_paths(value: &serde_yaml::Value) -> Result<BTreeSet<String>> {
    let mut paths = BTreeSet::new();
    collect_variable_paths_inner(value, &mut paths)?;
    Ok(paths)
}

/// Validate dotted runtime-variable path syntax without consulting a context.
pub fn validate_variable_path(path: &str) -> Result<()> {
    if path.is_empty() {
        bail!("runtime variable path must not be empty");
    }
    if path.split('.').any(str::is_empty) {
        bail!("runtime variable path '{path}' contains an empty component");
    }
    Ok(())
}

fn collect_variable_paths_inner(
    value: &serde_yaml::Value,
    paths: &mut BTreeSet<String>,
) -> Result<()> {
    match value {
        serde_yaml::Value::Mapping(mapping) => {
            if let Some((operator, operand)) = expression_entry(mapping)? {
                if operator == "var" {
                    let path = operand
                        .as_str()
                        .ok_or_else(|| eyre::eyre!("`var` expression path must be a string"))?;
                    validate_variable_path(path)?;
                    paths.insert(path.to_string());
                } else {
                    collect_variable_paths_inner(operand, paths)?;
                }
            } else {
                for value in mapping.values() {
                    collect_variable_paths_inner(value, paths)?;
                }
            }
        }
        serde_yaml::Value::Sequence(values) => {
            for value in values {
                collect_variable_paths_inner(value, paths)?;
            }
        }
        serde_yaml::Value::Tagged(value) => {
            collect_variable_paths_inner(&value.value, paths)?;
        }
        _ => {}
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TypedAbiExpression {
    types: Vec<String>,
    values: Vec<serde_yaml::Value>,
}

fn encode_typed_abi(
    operand: &serde_yaml::Value,
    context: &RuntimeContext,
    packed: bool,
) -> Result<Vec<u8>> {
    let definition: TypedAbiExpression = serde_yaml::from_value(operand.clone())
        .wrap_err("ABI expression must be `{ types: [...], values: [...] }`")?;
    if definition.types.len() != definition.values.len() {
        bail!(
            "ABI expression has {} types but {} values",
            definition.types.len(),
            definition.values.len()
        );
    }

    let mut values = Vec::with_capacity(definition.values.len());
    for (index, (type_name, expression)) in
        definition.types.iter().zip(&definition.values).enumerate()
    {
        let sol_type = parse_abi_type(type_name)
            .wrap_err_with(|| format!("invalid Solidity type {index} ('{type_name}')"))?;
        let value = eval_expression(expression, context)
            .wrap_err_with(|| format!("failed to evaluate ABI value {index}"))?;
        values.push(
            value
                .coerce_dyn_sol(&sol_type)
                // Do not retain the coercion source: alloy includes the JSON value in
                // type-mismatch diagnostics, and ABI expression values can be secret.
                .map_err(|_| eyre::eyre!("failed to coerce ABI value {index} as '{type_name}'"))?,
        );
    }

    let tuple = DynSolValue::Tuple(values);
    Ok(if packed { tuple.abi_encode_packed() } else { tuple.abi_encode_params() })
}

/// Parse a Solidity ABI type, preserving names on tuple members.
///
/// `DynSolType::parse` accepts canonical ABI type strings but intentionally
/// rejects Solidity declarations such as `(uint256 amount,address recipient)`.
/// Runtime expressions use those declarations to map YAML objects onto tuple
/// fields, so named tuples are represented as `CustomStruct` values while
/// unnamed tuples retain the usual positional representation.
fn parse_abi_type(type_name: &str) -> Result<DynSolType> {
    let mut parser = AbiTypeParser { input: type_name, offset: 0 };
    let ty = parser.parse_type()?;
    parser.skip_whitespace();
    if !parser.is_eof() {
        bail!("unexpected token at byte {}", parser.offset);
    }
    Ok(ty)
}

struct AbiTypeParser<'a> {
    input: &'a str,
    offset: usize,
}

impl AbiTypeParser<'_> {
    fn parse_type(&mut self) -> Result<DynSolType> {
        self.skip_whitespace();
        let mut ty = if self.consume_byte(b'(') {
            self.parse_tuple()?
        } else {
            let name = self.parse_identifier()?;
            if name == "tuple" {
                self.skip_whitespace();
                if !self.consume_byte(b'(') {
                    bail!("expected '(' after tuple at byte {}", self.offset);
                }
                self.parse_tuple()?
            } else {
                DynSolType::parse(name).wrap_err("invalid Solidity ABI type")?
            }
        };

        loop {
            self.skip_whitespace();
            if !self.consume_byte(b'[') {
                break;
            }
            self.skip_whitespace();
            let start = self.offset;
            while self.peek_byte().is_some_and(|byte| byte.is_ascii_digit()) {
                self.offset += 1;
            }
            let length = if start == self.offset {
                None
            } else {
                Some(
                    self.input[start..self.offset]
                        .parse::<usize>()
                        .wrap_err("invalid fixed array length")?,
                )
            };
            self.skip_whitespace();
            if !self.consume_byte(b']') {
                bail!("expected ']' at byte {}", self.offset);
            }
            ty = match length {
                Some(0) => bail!("fixed array length must be greater than zero"),
                Some(length) => DynSolType::FixedArray(Box::new(ty), length),
                None => DynSolType::Array(Box::new(ty)),
            };
        }
        Ok(ty)
    }

    fn parse_tuple(&mut self) -> Result<DynSolType> {
        let mut members = Vec::new();
        let mut names = Vec::new();
        self.skip_whitespace();
        if self.consume_byte(b')') {
            return Ok(DynSolType::Tuple(members));
        }

        loop {
            let member = self.parse_type()?;
            self.skip_whitespace();
            let name = self.try_parse_identifier().unwrap_or_default().to_string();
            members.push(member);
            names.push(name);
            self.skip_whitespace();
            if self.consume_byte(b')') {
                break;
            }
            if !self.consume_byte(b',') {
                bail!("expected ',' or ')' at byte {}", self.offset);
            }
            self.skip_whitespace();
        }

        let named = names.iter().any(|name| !name.is_empty());
        if named && names.iter().any(String::is_empty) {
            bail!("tuple members must either all be named or all be unnamed");
        }
        if named {
            Ok(DynSolType::CustomStruct {
                name: "tuple".to_string(),
                prop_names: names,
                tuple: members,
            })
        } else {
            Ok(DynSolType::Tuple(members))
        }
    }

    fn parse_identifier(&mut self) -> Result<&str> {
        let offset = self.offset;
        match self.try_parse_identifier() {
            Some(identifier) => Ok(identifier),
            None => bail!("expected Solidity type at byte {offset}"),
        }
    }

    fn try_parse_identifier(&mut self) -> Option<&str> {
        self.skip_whitespace();
        let start = self.offset;
        let first = self.peek_byte()?;
        if !(first.is_ascii_alphabetic() || first == b'_') {
            return None;
        }
        self.offset += 1;
        while self.peek_byte().is_some_and(|byte| byte.is_ascii_alphanumeric() || byte == b'_') {
            self.offset += 1;
        }
        Some(&self.input[start..self.offset])
    }

    fn skip_whitespace(&mut self) {
        while self.peek_byte().is_some_and(|byte| byte.is_ascii_whitespace()) {
            self.offset += 1;
        }
    }

    fn consume_byte(&mut self, expected: u8) -> bool {
        if self.peek_byte() == Some(expected) {
            self.offset += 1;
            true
        } else {
            false
        }
    }

    fn peek_byte(&self) -> Option<u8> {
        self.input.as_bytes().get(self.offset).copied()
    }

    fn is_eof(&self) -> bool {
        self.offset == self.input.len()
    }
}

fn encode_packed(operand: &serde_yaml::Value, context: &RuntimeContext) -> Result<Vec<u8>> {
    match operand {
        serde_yaml::Value::Mapping(_) => encode_typed_abi(operand, context, true),
        serde_yaml::Value::Sequence(expressions) => {
            let values = expressions
                .iter()
                .enumerate()
                .map(|(index, expression)| {
                    eval_expression(expression, context)
                        .wrap_err_with(|| format!("failed to evaluate packed value {index}"))?
                        .infer_dyn_sol()
                        .wrap_err_with(|| format!("failed to infer packed value {index}"))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(DynSolValue::Tuple(values).abi_encode_packed())
        }
        _ => {
            bail!(
                "packed ABI expression must be `{{ types: [...], values: [...] }}` or an inferred value list"
            );
        }
    }
}

fn expression_entry(
    mapping: &serde_yaml::Mapping,
) -> Result<Option<(&'static str, &serde_yaml::Value)>> {
    let mut found = None;
    for (key, value) in mapping {
        let Some(operator) = key.as_str().and_then(recognized_operator) else { continue };
        if found.is_some() || mapping.len() != 1 {
            bail!("runtime expression '{operator}' cannot be combined with other keys");
        }
        found = Some((operator, value));
    }
    Ok(found)
}

fn recognized_operator(value: &str) -> Option<&'static str> {
    match value {
        "var" => Some("var"),
        "keccak256" => Some("keccak256"),
        "keccak256_packed" => Some("keccak256_packed"),
        "abi_encode" => Some("abi_encode"),
        "abi_encode_packed" => Some("abi_encode_packed"),
        _ => None,
    }
}

fn validate_root_name(name: &str) -> Result<()> {
    if name.is_empty() {
        bail!("runtime root name must not be empty");
    }
    if name.contains('.') {
        bail!("runtime root name '{name}' must not contain '.'");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(values: impl IntoIterator<Item = (&'static str, RuntimeValue)>) -> RuntimeValue {
        RuntimeValue::Object(
            values.into_iter().map(|(key, value)| (key.to_string(), value)).collect(),
        )
    }

    fn context() -> RuntimeContext {
        let address = Address::repeat_byte(0x11);
        RuntimeContext::new(BTreeMap::from([
            (
                "user".to_string(),
                object([
                    ("address", RuntimeValue::Address(address)),
                    (
                        "ref",
                        object([
                            ("pool", RuntimeValue::String("users".to_string())),
                            ("select", object([("index", RuntimeValue::Uint(U256::from(3)))])),
                        ]),
                    ),
                ]),
            ),
            (
                "deposit".to_string(),
                object([
                    ("tx_hash", RuntimeValue::Bytes32(B256::repeat_byte(0x22))),
                    ("args", object([("amount", RuntimeValue::Uint(U256::from(42)))])),
                ]),
            ),
            (
                "items".to_string(),
                RuntimeValue::Array(vec![
                    RuntimeValue::String("first".to_string()),
                    RuntimeValue::String("second".to_string()),
                ]),
            ),
        ]))
        .unwrap()
    }

    #[test]
    fn dotted_lookup_supports_objects_and_arrays() {
        let context = context();
        assert_eq!(
            context.get("deposit.args.amount").unwrap(),
            &RuntimeValue::Uint(U256::from(42))
        );
        assert_eq!(context.get("items.1").unwrap(), &RuntimeValue::String("second".to_string()));
        assert!(context.get("items.last").is_err());
        assert!(context.get("deposit.missing").is_err());
    }

    #[test]
    fn contexts_are_immutable_and_reject_duplicate_roots() {
        let original = RuntimeContext::empty();
        let extended = original.with_root("saved", RuntimeValue::Bool(true)).unwrap();
        assert!(original.get("saved").is_err());
        assert_eq!(extended.get("saved").unwrap(), &RuntimeValue::Bool(true));
        assert!(extended.with_root("saved", RuntimeValue::Bool(false)).is_err());
        assert!(extended.with_root("bad.root", RuntimeValue::Null).is_err());
    }

    #[tokio::test]
    async fn concurrent_instance_contexts_remain_isolated() {
        let base = RuntimeContext::empty();
        let first_base = base.clone();
        let second_base = base.clone();
        let (first, second) = tokio::join!(
            async move {
                tokio::task::yield_now().await;
                first_base.with_root("result", RuntimeValue::Uint(U256::from(1)))
            },
            async move { second_base.with_root("result", RuntimeValue::Uint(U256::from(2))) },
        );
        let first = first.unwrap();
        let second = second.unwrap();

        assert!(base.get("result").is_err());
        assert_eq!(first.get("result").unwrap(), &RuntimeValue::Uint(U256::from(1)));
        assert_eq!(second.get("result").unwrap(), &RuntimeValue::Uint(U256::from(2)));
    }

    #[test]
    fn var_evaluation_preserves_runtime_type() {
        let expression: serde_yaml::Value = serde_yaml::from_str("{ var: user.address }").unwrap();
        assert_eq!(
            eval_expression(&expression, &context()).unwrap(),
            RuntimeValue::Address(Address::repeat_byte(0x11))
        );
    }

    #[test]
    fn rejects_malformed_or_unknown_variables() {
        let malformed: serde_yaml::Value =
            serde_yaml::from_str("{ var: user.address, fallback: zero }").unwrap();
        assert!(eval_expression(&malformed, &context()).is_err());

        let unknown: serde_yaml::Value = serde_yaml::from_str("{ var: future.value }").unwrap();
        assert!(eval_expression(&unknown, &context()).is_err());
        assert!(validate_variable_path("a..b").is_err());
    }

    #[test]
    fn recursively_materializes_template_yaml() {
        let value: serde_yaml::Value = serde_yaml::from_str(
            r#"
from: { var: user.ref }
call:
  args:
    - { var: user.address }
    - { var: deposit.args.amount }
"#,
        )
        .unwrap();
        let materialized = materialize_yaml(&value, &context()).unwrap();
        let from = &materialized["from"];
        assert_eq!(from["pool"].as_str(), Some("users"));
        assert_eq!(from["select"]["index"].as_u64(), Some(3));
        assert_eq!(materialized["call"]["args"][1].as_u64(), Some(42));
    }

    #[test]
    fn computes_raw_keccak256() {
        let expression: serde_yaml::Value = serde_yaml::from_str("{ keccak256: hello }").unwrap();
        assert_eq!(
            eval_expression(&expression, &RuntimeContext::empty()).unwrap(),
            RuntimeValue::Bytes32(keccak256(b"hello"))
        );
    }

    #[test]
    fn performs_typed_standard_abi_encoding() {
        let expression: serde_yaml::Value = serde_yaml::from_str(
            r#"
abi_encode:
  types: [address, uint256]
  values:
    - { var: user.address }
    - { var: deposit.args.amount }
"#,
        )
        .unwrap();
        let actual = eval_expression(&expression, &context()).unwrap();
        let expected = DynSolValue::Tuple(vec![
            DynSolValue::Address(Address::repeat_byte(0x11)),
            DynSolValue::Uint(U256::from(42), 256),
        ])
        .abi_encode_params();
        assert_eq!(actual, RuntimeValue::Bytes(Bytes::from(expected)));
    }

    #[test]
    fn abi_encode_materializes_named_nested_callback_tuple() -> Result<()> {
        let encrypted = object([
            ("ephemeralPubkeyX", RuntimeValue::Bytes32(B256::repeat_byte(0x11))),
            ("ephemeralPubkeyYParity", RuntimeValue::Uint(U256::from(1))),
            ("ciphertext", RuntimeValue::Bytes(Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]))),
            ("nonce", RuntimeValue::Bytes(Bytes::from_static(&[0x22; 12]))),
            ("tag", RuntimeValue::Bytes(Bytes::from_static(&[0x33; 16]))),
        ]);
        let context = RuntimeContext::new(BTreeMap::from([
            (
                "encrypted_return".to_string(),
                object([
                    ("keyIndex", RuntimeValue::Uint(U256::from(7))),
                    ("encrypted", encrypted.clone()),
                ]),
            ),
            ("action_id".to_string(), RuntimeValue::Bytes32(B256::repeat_byte(0x55))),
            (
                "account".to_string(),
                object([("address", RuntimeValue::Address(Address::repeat_byte(0x66)))]),
            ),
        ]))?;
        let request: serde_yaml::Value = serde_yaml::from_str(
            r#"
call:
  function: requestWithdrawal
  args:
    - vault-id
    - data:
        abi_encode:
          types:
            - "tuple(uint8 flow,address outputToken,uint256 keyIndex,(bytes32 ephemeralPubkeyX,uint8 ephemeralPubkeyYParity,bytes ciphertext,bytes12 nonce,bytes16 tag) encrypted,uint128 minVaultAssets,uint128 minVaultShares,uint128 minOutputAmount,bytes32 actionId,address refundRecipient)"
          values:
            - flow: 0
              outputToken: "0x4444444444444444444444444444444444444444"
              keyIndex: { var: encrypted_return.keyIndex }
              encrypted: { var: encrypted_return.encrypted }
              minVaultAssets: 5
              minVaultShares: 6
              minOutputAmount: 0
              actionId: { var: action_id }
              refundRecipient: { var: account.address }
    - "0x"
"#,
        )?;

        // This is the same runtime materialization path used by a submit
        // step: the resulting `data` string is directly usable as a bytes
        // argument to requestWithdrawal.
        let materialized = materialize_yaml(&request, &context)?;
        let data: Bytes = materialized["call"]["args"][1]["data"]
            .as_str()
            .ok_or_else(|| eyre::eyre!("callback data was not materialized as bytes"))?
            .parse()?;
        // Solidity fixture: abi.encode(callback). Keep this golden vector
        // independent of the dynamic ABI implementation under test.
        let solidity_abi_encode = Bytes::from(hex::decode(concat!(
            "0000000000000000000000000000000000000000000000000000000000000020",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "0000000000000000000000004444444444444444444444444444444444444444",
            "0000000000000000000000000000000000000000000000000000000000000007",
            "0000000000000000000000000000000000000000000000000000000000000120",
            "0000000000000000000000000000000000000000000000000000000000000005",
            "0000000000000000000000000000000000000000000000000000000000000006",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "5555555555555555555555555555555555555555555555555555555555555555",
            "0000000000000000000000006666666666666666666666666666666666666666",
            "1111111111111111111111111111111111111111111111111111111111111111",
            "0000000000000000000000000000000000000000000000000000000000000001",
            "00000000000000000000000000000000000000000000000000000000000000a0",
            "2222222222222222222222220000000000000000000000000000000000000000",
            "3333333333333333333333333333333300000000000000000000000000000000",
            "0000000000000000000000000000000000000000000000000000000000000004",
            "deadbeef00000000000000000000000000000000000000000000000000000000",
        ))?);
        assert!(data == solidity_abi_encode, "callback ABI encoding diverged from Solidity");

        let callback_type = parse_abi_type(
            "tuple(uint8 flow,address outputToken,uint256 keyIndex,(bytes32 ephemeralPubkeyX,uint8 ephemeralPubkeyYParity,bytes ciphertext,bytes12 nonce,bytes16 tag) encrypted,uint128 minVaultAssets,uint128 minVaultShares,uint128 minOutputAmount,bytes32 actionId,address refundRecipient)",
        )?;
        let decoded = callback_type.abi_decode_params(&data)?;
        assert_eq!(
            RuntimeValue::from_dyn_sol(&decoded)?,
            object([
                ("flow", RuntimeValue::Uint(U256::ZERO)),
                ("outputToken", RuntimeValue::Address(Address::repeat_byte(0x44))),
                ("keyIndex", RuntimeValue::Uint(U256::from(7))),
                ("encrypted", encrypted),
                ("minVaultAssets", RuntimeValue::Uint(U256::from(5))),
                ("minVaultShares", RuntimeValue::Uint(U256::from(6))),
                ("minOutputAmount", RuntimeValue::Uint(U256::ZERO)),
                ("actionId", RuntimeValue::Bytes32(B256::repeat_byte(0x55))),
                ("refundRecipient", RuntimeValue::Address(Address::repeat_byte(0x66))),
            ])
        );
        Ok(())
    }

    #[test]
    fn abi_encode_supports_named_tuple_arrays_and_booleans() -> Result<()> {
        let expression: serde_yaml::Value = serde_yaml::from_str(
            r#"
abi_encode:
  types: ["tuple(uint16[] ids,bool approved,bytes4 tag)"]
  values:
    - ids: [1, 513]
      approved: true
      tag: "0xdeadbeef"
"#,
        )?;
        let RuntimeValue::Bytes(encoded) = eval_expression(&expression, &RuntimeContext::empty())?
        else {
            panic!("abi_encode must produce bytes");
        };
        let decoded = parse_abi_type("tuple(uint16[] ids,bool approved,bytes4 tag)")?
            .abi_decode_params(&encoded)?;
        assert_eq!(
            RuntimeValue::from_dyn_sol(&decoded)?,
            object([
                (
                    "ids",
                    RuntimeValue::Array(vec![
                        RuntimeValue::Uint(U256::from(1)),
                        RuntimeValue::Uint(U256::from(513)),
                    ]),
                ),
                ("approved", RuntimeValue::Bool(true)),
                ("tag", RuntimeValue::Bytes(Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]))),
            ])
        );
        Ok(())
    }

    #[test]
    fn abi_encode_coercion_errors_do_not_include_values() {
        let expression: serde_yaml::Value = serde_yaml::from_str(
            r#"
abi_encode:
  types: ["tuple(bytes4 tag)"]
  values: [{ tag: "not-secret-hex" }]
"#,
        )
        .unwrap();
        let error = eval_expression(&expression, &RuntimeContext::empty()).unwrap_err();
        assert!(!error.to_string().contains("not-secret-hex"));
    }

    #[test]
    fn performs_typed_packed_encoding() {
        let expression: serde_yaml::Value = serde_yaml::from_str(
            r#"
abi_encode_packed:
  types: [address, bytes32]
  values:
    - { var: user.address }
    - { var: deposit.tx_hash }
"#,
        )
        .unwrap();
        let actual = eval_expression(&expression, &context()).unwrap();
        let expected = DynSolValue::Tuple(vec![
            DynSolValue::Address(Address::repeat_byte(0x11)),
            DynSolValue::FixedBytes(B256::repeat_byte(0x22), 32),
        ])
        .abi_encode_packed();
        assert_eq!(actual, RuntimeValue::Bytes(Bytes::from(expected)));
    }

    #[test]
    fn hashes_inferred_packed_common_path() {
        let expression: serde_yaml::Value = serde_yaml::from_str(
            r#"
keccak256_packed:
  - { var: user.address }
  - { var: deposit.tx_hash }
"#,
        )
        .unwrap();
        let packed = DynSolValue::Tuple(vec![
            DynSolValue::Address(Address::repeat_byte(0x11)),
            DynSolValue::FixedBytes(B256::repeat_byte(0x22), 32),
        ])
        .abi_encode_packed();
        assert_eq!(
            eval_expression(&expression, &context()).unwrap(),
            RuntimeValue::Bytes32(keccak256(packed))
        );
    }

    #[test]
    fn rejects_mismatched_typed_abi_values() {
        let expression: serde_yaml::Value = serde_yaml::from_str(
            r#"
abi_encode:
  types: [address, uint256]
  values: [{ var: user.address }]
"#,
        )
        .unwrap();
        let error = eval_expression(&expression, &context()).unwrap_err().to_string();
        assert!(error.contains("2 types but 1 values"));
    }

    #[test]
    fn collects_nested_variable_paths() {
        let expression: serde_yaml::Value = serde_yaml::from_str(
            r#"
outer:
  - { var: user.address }
  - keccak256_packed:
      types: [bytes32, uint256]
      values:
        - { var: deposit.tx_hash }
        - { var: deposit.args.amount }
"#,
        )
        .unwrap();
        assert_eq!(
            collect_variable_paths(&expression).unwrap(),
            BTreeSet::from([
                "deposit.args.amount".to_string(),
                "deposit.tx_hash".to_string(),
                "user.address".to_string(),
            ])
        );
    }

    #[test]
    fn coerces_event_filter_values_by_abi_type() {
        let address_expr: serde_yaml::Value =
            serde_yaml::from_str("{ var: user.address }").unwrap();
        assert_eq!(
            coerce_event_filter(&address_expr, &DynSolType::Address, &context()).unwrap(),
            DynSolValue::Address(Address::repeat_byte(0x11))
        );

        let amount_expr: serde_yaml::Value =
            serde_yaml::from_str("{ var: deposit.args.amount }").unwrap();
        assert_eq!(
            coerce_event_filter(&amount_expr, &DynSolType::Uint(64), &context()).unwrap(),
            DynSolValue::Uint(U256::from(42), 64)
        );
        assert!(coerce_event_filter(&amount_expr, &DynSolType::Address, &context()).is_err());
    }

    #[test]
    fn fixed_bytes_coercion_rejects_length_mismatch() {
        let value = RuntimeValue::Bytes(Bytes::from(vec![1, 2, 3]));
        assert!(value.coerce_dyn_sol(&DynSolType::FixedBytes(4)).is_err());
    }

    #[test]
    fn decoded_values_round_trip_to_runtime_types() {
        let values = [
            DynSolValue::Bool(true),
            DynSolValue::Uint(U256::from(9), 32),
            DynSolValue::Address(Address::repeat_byte(0x33)),
            DynSolValue::FixedBytes(B256::repeat_byte(0x44), 32),
            DynSolValue::Array(vec![DynSolValue::String("value".to_string())]),
        ];
        let runtime =
            values.iter().map(RuntimeValue::from_dyn_sol).collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(runtime[0], RuntimeValue::Bool(true));
        assert_eq!(runtime[1], RuntimeValue::Uint(U256::from(9)));
        assert_eq!(runtime[2], RuntimeValue::Address(Address::repeat_byte(0x33)));
        assert_eq!(runtime[3], RuntimeValue::Bytes32(B256::repeat_byte(0x44)));
        assert_eq!(
            runtime[4],
            RuntimeValue::Array(vec![RuntimeValue::String("value".to_string())])
        );
    }

    #[test]
    fn event_value_equality_uses_expected_type() {
        let expected = RuntimeValue::Uint(U256::from(7));
        let actual = DynSolValue::Uint(U256::from(7), 128);
        assert!(event_value_matches(&expected, &actual, &DynSolType::Uint(128)).unwrap());
        assert!(!event_value_matches(
            &RuntimeValue::Uint(U256::from(8)),
            &actual,
            &DynSolType::Uint(128)
        )
        .unwrap());
    }
}
