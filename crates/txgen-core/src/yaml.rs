use eyre::{bail, Result, WrapErr};
use std::path::{Path, PathBuf};

const INCLUDE_KEY: &str = "include";
const INCLUDES_KEY: &str = "includes";
const MERGE_KEY: &str = "merge";
const APPEND_KEY: &str = "append";

/// Load a YAML document, resolving txgen composition directives.
///
/// Files can compose specs with `include`/`includes`, `merge`, and `append`.
/// Included paths are resolved relative to the file that names them.
pub fn load_yaml(path: &Path) -> Result<serde_yaml::Value> {
    let mut resolver = YamlResolver::default();
    let mut output = serde_yaml::Value::Null;
    resolver.apply_file(path, &mut output)?;
    Ok(output)
}

/// Parse a YAML document string, resolving non-file composition directives.
pub fn parse_yaml(yaml: &str) -> Result<serde_yaml::Value> {
    let value = parse_document(yaml, "<inline>")?;
    let mut resolver = YamlResolver::default();
    let mut output = serde_yaml::Value::Null;
    resolver.apply_document(value, None, "<inline>", &mut output)?;
    Ok(output)
}

/// Deep-merge a YAML overlay into a base value.
///
/// Mapping values are merged recursively, except variable references of the
/// form `{ var: ... }`, which replace the base value atomically. All other
/// overlay values replace the base value. A `null` overlay is treated as no-op
/// so omitted `with` blocks do not erase the referenced template.
pub fn merge_yaml(base: &mut serde_yaml::Value, overlay: serde_yaml::Value) {
    if matches!(overlay, serde_yaml::Value::Null) {
        return;
    }

    // Variable references are scalar leaves even though their YAML encoding is
    // a mapping. Replacing a mapping field with `{ var: ... }` must discard the
    // original mapping instead of retaining its keys alongside `var`.
    if matches!(
        &overlay,
        serde_yaml::Value::Mapping(mapping)
            if mapping.len() == 1 && mapping.contains_key("var")
    ) {
        *base = overlay;
        return;
    }

    match (base, overlay) {
        (serde_yaml::Value::Mapping(base_map), serde_yaml::Value::Mapping(overlay_map)) => {
            for (key, value) in overlay_map {
                match base_map.get_mut(&key) {
                    Some(base_value) => merge_yaml(base_value, value),
                    None => {
                        base_map.insert(key, value);
                    }
                }
            }
        }
        (base_value, overlay_value) => {
            *base_value = overlay_value;
        }
    }
}

/// Append YAML sequence leaves into a base value.
///
/// Mapping values are traversed recursively. Sequence leaves are appended to
/// existing sequences or inserted when absent. All other leaves are rejected.
pub fn append_yaml(base: &mut serde_yaml::Value, overlay: serde_yaml::Value) -> Result<()> {
    match overlay {
        serde_yaml::Value::Null => Ok(()),
        serde_yaml::Value::Mapping(overlay_map) => {
            if matches!(base, serde_yaml::Value::Null) {
                *base = serde_yaml::Value::Mapping(serde_yaml::Mapping::new());
            }

            let serde_yaml::Value::Mapping(base_map) = base else {
                bail!("cannot append nested fields into a non-mapping value");
            };

            for (key, value) in overlay_map {
                match base_map.get_mut(&key) {
                    Some(base_value) => append_yaml(base_value, value)?,
                    None => {
                        let mut base_value = serde_yaml::Value::Null;
                        append_yaml(&mut base_value, value)?;
                        base_map.insert(key, base_value);
                    }
                }
            }

            Ok(())
        }
        serde_yaml::Value::Sequence(mut overlay_values) => {
            if matches!(base, serde_yaml::Value::Null) {
                *base = serde_yaml::Value::Sequence(Vec::new());
            }

            let serde_yaml::Value::Sequence(base_values) = base else {
                bail!("cannot append a sequence into a non-sequence value");
            };

            base_values.append(&mut overlay_values);
            Ok(())
        }
        _ => {
            bail!("append section leaves must be sequences");
        }
    }
}

/// Expand `${VAR}` patterns and parse the result as a YAML value.
fn parse_document(content: &str, label: &str) -> Result<serde_yaml::Value> {
    let expanded = expand_env_vars(content).wrap_err("failed to expand environment variables")?;
    serde_yaml::from_str(&expanded).wrap_err_with(|| format!("failed to parse YAML: {label}"))
}

/// Expand `${VAR}` patterns with environment variable values.
fn expand_env_vars(input: &str) -> Result<String> {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let mut var_name = String::new();
            let mut found_end = false;
            for c in chars.by_ref() {
                if c == '}' {
                    found_end = true;
                    break;
                }
                var_name.push(c);
            }

            if !found_end {
                bail!("unterminated environment variable expansion: ${{{var_name}");
            }

            match std::env::var(&var_name) {
                Ok(value) => result.push_str(&value),
                Err(std::env::VarError::NotPresent) => {
                    bail!("environment variable `{var_name}` referenced in spec is not set");
                }
                Err(std::env::VarError::NotUnicode(_)) => {
                    bail!(
                        "environment variable `{var_name}` referenced in spec is not valid Unicode"
                    );
                }
            }
        } else {
            result.push(c);
        }
    }

    Ok(result)
}

#[derive(Default)]
struct YamlResolver {
    stack: Vec<PathBuf>,
}

impl YamlResolver {
    fn apply_file(&mut self, path: &Path, output: &mut serde_yaml::Value) -> Result<()> {
        let path = std::fs::canonicalize(path)
            .wrap_err_with(|| format!("failed to resolve spec file: {}", path.display()))?;
        if let Some(existing) = self.stack.iter().position(|stack_path| stack_path == &path) {
            let mut cycle: Vec<String> =
                self.stack[existing..].iter().map(|path| path.display().to_string()).collect();
            cycle.push(path.display().to_string());
            bail!("cyclic spec include: {}", cycle.join(" -> "));
        }

        self.stack.push(path.clone());
        let result = self.apply_file_inner(&path, output);
        self.stack.pop();
        result
    }

    fn apply_file_inner(&mut self, path: &Path, output: &mut serde_yaml::Value) -> Result<()> {
        let content = std::fs::read_to_string(path)
            .wrap_err_with(|| format!("failed to read spec file: {}", path.display()))?;
        let value = parse_document(&content, &path.display().to_string())?;
        let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
        self.apply_document(value, Some(base_dir), &path.display().to_string(), output)
    }

    fn apply_document(
        &mut self,
        value: serde_yaml::Value,
        base_dir: Option<&Path>,
        label: &str,
        output: &mut serde_yaml::Value,
    ) -> Result<()> {
        let serde_yaml::Value::Mapping(mut mapping) = value else {
            merge_yaml(output, value);
            return Ok(());
        };

        let include = mapping.remove(INCLUDE_KEY);
        let includes = mapping.remove(INCLUDES_KEY);
        let merge = mapping.remove(MERGE_KEY);
        let append = mapping.remove(APPEND_KEY);
        let has_directives =
            include.is_some() || includes.is_some() || merge.is_some() || append.is_some();

        if !has_directives {
            let mut value = serde_yaml::Value::Mapping(mapping);
            normalize_artifact_paths(&mut value, base_dir);
            merge_yaml(output, value);
            return Ok(());
        }

        if include.is_some() && includes.is_some() {
            bail!("{label} uses both `include` and `includes`; use only one");
        }

        if let Some(include_value) = include.or(includes) {
            for include_path in parse_include_paths(include_value, label)? {
                let include_path = resolve_include_path(base_dir, &include_path, label)?;
                self.apply_file(&include_path, output).wrap_err_with(|| {
                    format!("failed to apply include: {}", include_path.display())
                })?;
            }
        }

        if !mapping.is_empty() {
            let mut value = serde_yaml::Value::Mapping(mapping);
            normalize_artifact_paths(&mut value, base_dir);
            merge_yaml(output, value);
        }

        if let Some(mut merge_value) = merge {
            ensure_mapping(&merge_value, label, MERGE_KEY)?;
            normalize_artifact_paths(&mut merge_value, base_dir);
            merge_yaml(output, merge_value);
        }

        if let Some(append_value) = append {
            ensure_mapping(&append_value, label, APPEND_KEY)?;
            append_yaml(output, append_value)
                .wrap_err_with(|| format!("failed to apply append section in {label}"))?;
        }

        Ok(())
    }
}

fn parse_include_paths(value: serde_yaml::Value, label: &str) -> Result<Vec<String>> {
    match value {
        serde_yaml::Value::Null => Ok(Vec::new()),
        serde_yaml::Value::String(path) => Ok(vec![path]),
        serde_yaml::Value::Sequence(paths) => paths
            .into_iter()
            .map(|value| match value {
                serde_yaml::Value::String(path) => Ok(path),
                _ => {
                    bail!("include entries in {label} must be strings");
                }
            })
            .collect(),
        _ => {
            bail!("include section in {label} must be a string or list of strings");
        }
    }
}

fn resolve_include_path(
    base_dir: Option<&Path>,
    include_path: &str,
    label: &str,
) -> Result<PathBuf> {
    let include_path = Path::new(include_path);
    if include_path.is_absolute() {
        return Ok(include_path.to_path_buf());
    }

    let Some(base_dir) = base_dir else {
        bail!("relative includes in {label} require loading the spec from a file");
    };

    Ok(base_dir.join(include_path))
}

fn ensure_mapping(value: &serde_yaml::Value, label: &str, section: &str) -> Result<()> {
    match value {
        serde_yaml::Value::Null | serde_yaml::Value::Mapping(_) => Ok(()),
        _ => {
            bail!("{section} section in {label} must be a mapping");
        }
    }
}

fn normalize_artifact_paths(value: &mut serde_yaml::Value, base_dir: Option<&Path>) {
    let Some(base_dir) = base_dir else {
        return;
    };

    let serde_yaml::Value::Mapping(mapping) = value else {
        return;
    };

    let Some(serde_yaml::Value::Mapping(artifacts)) = mapping.get_mut("artifacts") else {
        return;
    };

    for artifact in artifacts.values_mut() {
        normalize_artifact_def_paths(artifact, base_dir);
    }
}

fn normalize_artifact_def_paths(value: &mut serde_yaml::Value, base_dir: &Path) {
    match value {
        serde_yaml::Value::String(path) => {
            *path = normalize_path_string(path, base_dir);
        }
        serde_yaml::Value::Mapping(mapping) => {
            normalize_artifact_object_path(mapping, "abi", base_dir);
            normalize_artifact_object_path(mapping, "bytecode", base_dir);
        }
        _ => {}
    }
}

fn normalize_artifact_object_path(mapping: &mut serde_yaml::Mapping, key: &str, base_dir: &Path) {
    if let Some(serde_yaml::Value::String(path)) = mapping.get_mut(key) {
        *path = normalize_path_string(path, base_dir);
    }
}

fn normalize_path_string(path: &str, base_dir: &Path) -> String {
    let path = Path::new(path);
    if path.is_absolute() {
        path.display().to_string()
    } else {
        base_dir.join(path).display().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::merge_yaml;

    #[test]
    fn variable_reference_replaces_mapping_atomically() {
        let mut base = serde_yaml::from_str(
            r#"
from:
  pool: users
  select: { index: 0 }
"#,
        )
        .unwrap();
        let overlay = serde_yaml::from_str(
            r#"
from: { var: claim.from }
"#,
        )
        .unwrap();

        merge_yaml(&mut base, overlay);

        let expected: serde_yaml::Value = serde_yaml::from_str(
            r#"
from: { var: claim.from }
"#,
        )
        .unwrap();
        assert_eq!(base, expected);
    }

    #[test]
    fn regular_mappings_still_merge_recursively() {
        let mut base = serde_yaml::from_str(
            r#"
call:
  to: "0x0000000000000000000000000000000000000001"
  args:
    amount: 1
    recipient: alice
"#,
        )
        .unwrap();
        let overlay = serde_yaml::from_str(
            r#"
call:
  args:
    amount: 2
"#,
        )
        .unwrap();

        merge_yaml(&mut base, overlay);

        let expected: serde_yaml::Value = serde_yaml::from_str(
            r#"
call:
  to: "0x0000000000000000000000000000000000000001"
  args:
    amount: 2
    recipient: alice
"#,
        )
        .unwrap();
        assert_eq!(base, expected);
    }
}
