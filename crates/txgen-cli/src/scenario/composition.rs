use super::schema::{ScenarioSpec, StepProvenance};
use eyre::{bail, Result, WrapErr};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};
use txgen_core::expand_env_vars;

const INCLUDE_KEY: &str = "include";
const FRAGMENTS_KEY: &str = "fragments";
const SCENARIO_KEY: &str = "scenario";
const STEPS_KEY: &str = "steps";

/// A fully expanded scenario together with the deterministic YAML used to parse it.
#[derive(Debug)]
pub(crate) struct ResolvedScenario {
    pub spec: ScenarioSpec,
    pub rendered: serde_yaml::Value,
}

/// Load, compose, and validate a scenario document from a file.
pub(crate) fn load_scenario(path: &Path) -> Result<ResolvedScenario> {
    let mut resolver = ScenarioResolver::default();
    let root = resolver
        .collect_file(path, DocumentRole::Root)?
        .expect("root scenario collection returns its document");
    resolver.resolve(root)
}

/// Parse, compose, and validate an inline scenario document.
///
/// Inline documents may declare and instantiate fragments, but cannot include
/// files because they have no declaring directory for relative path resolution.
pub(crate) fn parse_scenario(yaml: &str) -> Result<ResolvedScenario> {
    let mut resolver = ScenarioResolver::default();
    let root = parse_document(yaml, "<inline>", None)?;
    if !root.document.include.paths().is_empty() {
        bail!("inline scenario documents cannot use `include`; load the scenario from a file");
    }
    resolver.register_fragments(&root)?;
    resolver.resolve(root)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DocumentRole {
    Root,
    Included,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum IncludeDef {
    One(PathBuf),
    Many(Vec<PathBuf>),
}

#[derive(Debug, Clone, Default)]
struct Includes(Option<IncludeDef>);

impl Includes {
    fn paths(&self) -> Vec<PathBuf> {
        match &self.0 {
            None => Vec::new(),
            Some(IncludeDef::One(path)) => vec![path.clone()],
            Some(IncludeDef::Many(paths)) => paths.clone(),
        }
    }
}

impl<'de> Deserialize<'de> for Includes {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Option::<IncludeDef>::deserialize(deserializer).map(Self)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDocument {
    #[serde(default)]
    version: Option<u64>,
    #[serde(default)]
    include: Includes,
    #[serde(default)]
    fragments: BTreeMap<String, RawFragment>,
    #[serde(default)]
    #[serde(rename = "chains")]
    _chains: Option<serde_yaml::Value>,
    #[serde(default, rename = "scenario")]
    _scenario: Option<serde_yaml::Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFragment {
    #[serde(default)]
    parameters: BTreeMap<String, String>,
    #[serde(default)]
    outputs: BTreeMap<String, String>,
    steps: Vec<serde_yaml::Value>,
}

#[derive(Debug, Clone)]
struct LocatedDocument {
    raw: serde_yaml::Value,
    document: RawDocument,
    source_file: String,
    base_dir: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct LocatedFragment {
    name: String,
    definition: RawFragment,
    source_file: String,
}

#[derive(Debug, Clone)]
struct UseDef {
    fragment: String,
    alias: String,
    arguments: BTreeMap<String, serde_yaml::Value>,
}

#[derive(Debug, Clone, Default)]
struct ExpansionScope {
    alias_prefix: Option<String>,
    parameters: BTreeMap<String, serde_yaml::Value>,
    local_roots: BTreeSet<String>,
    local_aliases: BTreeSet<String>,
    fragment: Option<String>,
}

#[derive(Debug, Clone)]
struct ExpandedStep {
    value: serde_yaml::Value,
    provenance: Option<StepProvenance>,
    diagnostic: String,
}

#[derive(Debug, Clone)]
struct ParameterCheck {
    step_index: usize,
    expression: serde_yaml::Value,
    expected: String,
    label: String,
}

#[derive(Debug, Clone)]
struct SaveOrigin {
    step_index: usize,
    label: String,
}

#[derive(Debug, Clone, Default)]
struct FragmentScope {
    roots: BTreeSet<String>,
    aliases: BTreeSet<String>,
}

#[derive(Debug, Clone)]
struct OutputReferenceCheck {
    boundary_alias: String,
    path: String,
    label: String,
}

#[derive(Default)]
struct ScenarioResolver {
    stack: Vec<PathBuf>,
    fragments: BTreeMap<String, LocatedFragment>,
    aliases: BTreeMap<String, String>,
    alias_outputs: BTreeMap<String, BTreeSet<String>>,
    saves: BTreeMap<String, SaveOrigin>,
    fragment_stack: Vec<String>,
    used_fragments: BTreeSet<String>,
    expanded_steps: Vec<ExpandedStep>,
    parameter_checks: Vec<ParameterCheck>,
    output_reference_checks: Vec<OutputReferenceCheck>,
}

impl ScenarioResolver {
    fn validate_fragment_library(&self) -> Result<()> {
        for (index, fragment) in self.fragments.values().enumerate() {
            if self.used_fragments.contains(&fragment.name) {
                continue;
            }
            let alias = format!("__fragment_definition_{index}");
            let label =
                format!("fragment '{}' declared in '{}'", fragment.name, fragment.source_file);
            let parameters = fragment
                .definition
                .parameters
                .keys()
                .map(|name| (name.clone(), serde_yaml::Value::Null))
                .collect();
            let mut validator = Self {
                fragments: self.fragments.clone(),
                fragment_stack: vec![fragment.name.clone()],
                ..Self::default()
            };
            validator.aliases.insert(alias.clone(), label.clone());
            let outputs = validator.expand_fragment(fragment, &label, alias.clone(), parameters)?;
            validator.alias_outputs.insert(alias, outputs.keys().cloned().collect::<BTreeSet<_>>());
            validator.validate_output_references()?;
        }
        Ok(())
    }

    fn collect_file(&mut self, path: &Path, role: DocumentRole) -> Result<Option<LocatedDocument>> {
        let canonical_path = std::fs::canonicalize(path)
            .wrap_err_with(|| format!("failed to resolve scenario file: {}", path.display()))?;
        if let Some(position) = self.stack.iter().position(|candidate| candidate == &canonical_path)
        {
            let mut cycle = self.stack[position..]
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>();
            cycle.push(canonical_path.display().to_string());
            bail!("cyclic scenario include: {}", cycle.join(" -> "));
        }

        self.stack.push(canonical_path);
        let result = self.collect_file_inner(path, role);
        self.stack.pop();
        result
    }

    fn collect_file_inner(
        &mut self,
        path: &Path,
        role: DocumentRole,
    ) -> Result<Option<LocatedDocument>> {
        let content = std::fs::read_to_string(path)
            .wrap_err_with(|| format!("failed to read scenario file: {}", path.display()))?;
        let base_dir = path.parent().map(Path::to_path_buf);
        let document = parse_document(&content, &path.display().to_string(), base_dir.clone())?;

        if role == DocumentRole::Included {
            if mapping_contains(&document.raw, "chains") ||
                mapping_contains(&document.raw, SCENARIO_KEY)
            {
                bail!(
                    "included scenario file '{}' may contribute only `version`, `include`, and `fragments`; `chains` and `scenario` are root-only",
                    document.source_file
                );
            }
            if let Some(version) = document.document.version &&
                version != 1
            {
                bail!(
                    "included scenario file '{}' uses unsupported version {version}; expected version 1",
                    document.source_file
                );
            }
        }

        for include in document.document.include.paths() {
            let include_path = if include.is_absolute() {
                include
            } else {
                document
                    .base_dir
                    .as_deref()
                    .expect("file documents have a base directory")
                    .join(include)
            };
            self.collect_file(&include_path, DocumentRole::Included).wrap_err_with(|| {
                format!(
                    "failed to apply scenario include '{}' declared by '{}'",
                    include_path.display(),
                    document.source_file
                )
            })?;
        }

        self.register_fragments(&document)?;
        Ok((role == DocumentRole::Root).then_some(document))
    }

    fn register_fragments(&mut self, document: &LocatedDocument) -> Result<()> {
        for (name, definition) in &document.document.fragments {
            validate_nonempty_name(name, "fragment").wrap_err_with(|| {
                format!("invalid fragment declaration in '{}'", document.source_file)
            })?;
            validate_fragment_definition(name, definition, &document.source_file)?;
            if let Some(previous) = self.fragments.get(name) {
                bail!(
                    "duplicate scenario fragment '{name}' declared in '{}' and '{}'",
                    previous.source_file,
                    document.source_file
                );
            }
            self.fragments.insert(
                name.clone(),
                LocatedFragment {
                    name: name.clone(),
                    definition: definition.clone(),
                    source_file: document.source_file.clone(),
                },
            );
        }
        Ok(())
    }

    fn resolve(mut self, mut root: LocatedDocument) -> Result<ResolvedScenario> {
        let root_steps = scenario_steps(&root.raw, &root.source_file)?;
        validate_root_scope(&root_steps, &root.source_file)?;
        let root_scope = ExpansionScope::default();
        for (index, step) in root_steps.iter().enumerate() {
            let label = expanded_step_label(
                &root.source_file,
                index,
                self.expanded_steps.len(),
                &root_scope,
            );
            if let Some(use_def) = parse_use(step, &label)? {
                self.expand_use(&use_def, &root_scope, &root.source_file, index)?;
            } else {
                self.emit_concrete(step, &root_scope, &root.source_file, index)?;
            }
        }
        self.validate_fragment_library()?;
        self.validate_output_references()?;

        remove_mapping_key(&mut root.raw, INCLUDE_KEY)?;
        remove_mapping_key(&mut root.raw, FRAGMENTS_KEY)?;
        replace_scenario_steps(
            &mut root.raw,
            self.expanded_steps.iter().map(|step| step.value.clone()).collect(),
            &root.source_file,
        )?;
        if let Some(base_dir) = &root.base_dir {
            resolve_root_chain_paths(&mut root.raw, base_dir)?;
        }

        for expanded in &self.expanded_steps {
            serde_yaml::from_value::<super::schema::StepDef>(expanded.value.clone())
                .wrap_err_with(|| format!("failed to parse {}", expanded.diagnostic))?;
        }

        let mut spec: ScenarioSpec =
            serde_yaml::from_value(root.raw.clone()).wrap_err_with(|| {
                format!("failed to parse fully expanded scenario '{}'", root.source_file)
            })?;
        if spec.scenario.steps.len() != self.expanded_steps.len() {
            bail!("internal error: expanded scenario step metadata is out of sync");
        }
        for (step, expanded) in spec.scenario.steps.iter_mut().zip(&self.expanded_steps) {
            step.provenance = expanded.provenance.clone();
        }
        for (alias, origin) in self.aliases.iter().filter(|(alias, _)| !alias.contains('.')) {
            if spec.scenario.bindings.contains_key(alias) {
                bail!(
                    "scenario fragment alias '{alias}' at {origin} conflicts with a scenario binding"
                );
            }
        }
        spec.validate()?;
        for check in &self.parameter_checks {
            spec.validate_parameter_argument(
                check.step_index,
                &check.expression,
                &check.expected,
                &check.label,
            )?;
        }

        Ok(ResolvedScenario { spec, rendered: root.raw })
    }

    fn expand_use(
        &mut self,
        use_def: &UseDef,
        caller_scope: &ExpansionScope,
        declaring_source: &str,
        local_step_index: usize,
    ) -> Result<BTreeMap<String, String>> {
        let insertion_index = self.expanded_steps.len();
        let full_alias = join_path(caller_scope.alias_prefix.as_deref(), &use_def.alias);
        let fragment = self.fragments.get(&use_def.fragment).cloned().ok_or_else(|| {
            let caller = match (&caller_scope.fragment, &caller_scope.alias_prefix) {
                (Some(fragment), Some(alias)) => {
                    format!(" while expanding fragment '{fragment}' instance '{alias}'")
                }
                _ => String::new(),
            };
            eyre::eyre!(
                "expanded step {} at {} uses unknown scenario fragment '{}' as instance '{}'{}",
                insertion_index + 1,
                step_label(declaring_source, local_step_index),
                use_def.fragment,
                full_alias,
                caller
            )
        })?;
        self.used_fragments.insert(fragment.name.clone());
        let use_label = format!(
            "expanded step {} at {} uses fragment '{}' declared in '{}' as instance '{}'",
            insertion_index + 1,
            step_label(declaring_source, local_step_index),
            fragment.name,
            fragment.source_file,
            full_alias
        );
        if let Some(previous) = self.aliases.get(&full_alias) {
            bail!(
                "duplicate scenario fragment alias '{full_alias}' at {use_label}; first declared at {previous}"
            );
        }
        self.aliases.insert(full_alias.clone(), use_label.clone());

        let expected = fragment.definition.parameters.keys().cloned().collect::<BTreeSet<_>>();
        let supplied = use_def.arguments.keys().cloned().collect::<BTreeSet<_>>();
        let missing = expected.difference(&supplied).cloned().collect::<Vec<_>>();
        let unknown = supplied.difference(&expected).cloned().collect::<Vec<_>>();
        if !missing.is_empty() {
            bail!("{use_label} is missing required parameter(s): {}", missing.join(", "));
        }
        if !unknown.is_empty() {
            bail!("{use_label} supplies unknown parameter(s): {}", unknown.join(", "));
        }

        let mut arguments = BTreeMap::new();
        for (name, expected_type) in &fragment.definition.parameters {
            let transformed = transform_value(
                &use_def.arguments[name],
                caller_scope,
                &format!("{use_label} parameter '{name}'"),
                &mut self.output_reference_checks,
            )?;
            self.parameter_checks.push(ParameterCheck {
                step_index: insertion_index,
                expression: transformed.clone(),
                expected: expected_type.clone(),
                label: format!("{use_label} parameter '{name}'"),
            });
            arguments.insert(name.clone(), transformed);
        }

        if let Some(position) =
            self.fragment_stack.iter().position(|candidate| candidate == &fragment.name)
        {
            let mut cycle = self.fragment_stack[position..].to_vec();
            cycle.push(fragment.name.clone());
            bail!("recursive scenario fragment at {use_label}: {}", cycle.join(" -> "));
        }

        self.fragment_stack.push(fragment.name.clone());
        let result = self.expand_fragment(&fragment, &use_label, full_alias.clone(), arguments);
        self.fragment_stack.pop();
        let outputs = result?;
        self.alias_outputs.insert(full_alias, outputs.keys().cloned().collect::<BTreeSet<_>>());
        Ok(outputs)
    }

    fn expand_fragment(
        &mut self,
        fragment: &LocatedFragment,
        use_label: &str,
        full_alias: String,
        parameters: BTreeMap<String, serde_yaml::Value>,
    ) -> Result<BTreeMap<String, String>> {
        let local_scope = validate_fragment_scope(fragment)?;
        let scope = ExpansionScope {
            alias_prefix: Some(full_alias.clone()),
            parameters,
            local_roots: local_scope.roots,
            local_aliases: local_scope.aliases,
            fragment: Some(fragment.name.clone()),
        };
        let mut accessible_saves = BTreeMap::new();

        for (index, step) in fragment.definition.steps.iter().enumerate() {
            let label = expanded_step_label(
                &fragment.source_file,
                index,
                self.expanded_steps.len(),
                &scope,
            );
            if let Some(use_def) = parse_use(step, &label)? {
                let nested_alias = use_def.alias.clone();
                let nested_outputs =
                    self.expand_use(&use_def, &scope, &fragment.source_file, index)?;
                for (name, kind) in nested_outputs {
                    accessible_saves.insert(format!("{nested_alias}.{name}"), kind);
                }
            } else if let Some((name, kind)) =
                self.emit_concrete(step, &scope, &fragment.source_file, index)?
            {
                accessible_saves.insert(name, kind);
            }
        }

        let mut outputs = BTreeMap::new();
        for (name, expected_kind) in &fragment.definition.outputs {
            let actual_kind = accessible_saves.get(name).ok_or_else(|| {
                eyre::eyre!(
                    "{use_label} declares output '{name}' but produces no accessible save with that name",
                )
            })?;
            if actual_kind != expected_kind {
                bail!(
                    "{use_label} declares output '{}' as '{}', but the save has type '{}'",
                    name,
                    expected_kind,
                    actual_kind
                );
            }
            outputs.insert(name.clone(), actual_kind.clone());
        }
        Ok(outputs)
    }

    fn emit_concrete(
        &mut self,
        raw_step: &serde_yaml::Value,
        scope: &ExpansionScope,
        source_file: &str,
        local_step_index: usize,
    ) -> Result<Option<(String, String)>> {
        let label =
            expanded_step_label(source_file, local_step_index, self.expanded_steps.len(), scope);
        let mut value =
            transform_value(raw_step, scope, &label, &mut self.output_reference_checks)?;
        let action = concrete_action(&value, &label)?.to_string();
        let output_kind = output_kind_for_action(&action).to_string();
        let local_save = step_save(&value, &label)?.map(str::to_string);

        let provenance =
            if let (Some(fragment), Some(alias)) = (&scope.fragment, &scope.alias_prefix) {
                let local_step_name = local_save
                    .clone()
                    .unwrap_or_else(|| format!("step_{}_{}", local_step_index + 1, action));
                Some(StepProvenance {
                    source_file: source_file.to_string(),
                    fragment: fragment.clone(),
                    instance_alias: alias.clone(),
                    local_step_name,
                    local_step_index,
                })
            } else {
                None
            };

        if let Some(save) = &local_save {
            let full_save = if let Some(alias) = &scope.alias_prefix {
                validate_component(save, "fragment-local save")
                    .wrap_err_with(|| format!("invalid {label}"))?;
                let full = format!("{alias}.{save}");
                set_step_save(&mut value, full.clone(), &label)?;
                full
            } else {
                validate_path(save, "scenario save")
                    .wrap_err_with(|| format!("invalid {label}"))?;
                save.clone()
            };
            self.register_save(&full_save, &label)?;
        }

        self.expanded_steps.push(ExpandedStep { value, provenance, diagnostic: label });
        Ok(local_save.map(|save| (save, output_kind)))
    }

    fn register_save(&mut self, save: &str, label: &str) -> Result<()> {
        let step_index = self.expanded_steps.len();
        if let Some(previous) = self.saves.get(save) {
            bail!(
                "duplicate save name '{save}' at expanded steps {} ({}) and {} ({label})",
                previous.step_index + 1,
                previous.label,
                step_index + 1
            );
        }
        if let Some((other, origin)) = self.saves.iter().find(|(other, _)| {
            save.starts_with(&format!("{other}.")) || other.starts_with(&format!("{save}."))
        }) {
            bail!(
                "save path '{save}' at expanded step {} ({label}) conflicts with save path '{}' at expanded step {} ({})",
                step_index + 1,
                other,
                origin.step_index + 1,
                origin.label
            );
        }
        self.saves.insert(save.to_string(), SaveOrigin { step_index, label: label.to_string() });
        Ok(())
    }

    fn validate_output_references(&self) -> Result<()> {
        for check in &self.output_reference_checks {
            let Some(outputs) = self.alias_outputs.get(&check.boundary_alias) else { continue };
            let tail = check
                .path
                .strip_prefix(&check.boundary_alias)
                .and_then(|tail| tail.strip_prefix('.'));
            let visible = tail.is_some_and(|tail| {
                outputs.iter().any(|output| {
                    tail == output ||
                        tail.strip_prefix(output).is_some_and(|tail| tail.starts_with('.'))
                })
            });
            if !visible {
                let available = if outputs.is_empty() {
                    "none".to_string()
                } else {
                    outputs.iter().cloned().collect::<Vec<_>>().join(", ")
                };
                bail!(
                    "{} references private or unknown output '{}' of fragment instance '{}'; declared outputs: {available}",
                    check.label,
                    check.path,
                    check.boundary_alias
                );
            }
        }
        Ok(())
    }
}

fn parse_document(
    yaml: &str,
    source_file: &str,
    base_dir: Option<PathBuf>,
) -> Result<LocatedDocument> {
    let expanded = expand_env_vars(yaml).wrap_err_with(|| {
        format!("failed to expand scenario environment variables in '{source_file}'")
    })?;
    let raw: serde_yaml::Value = serde_yaml::from_str(&expanded)
        .wrap_err_with(|| format!("failed to parse scenario YAML: {source_file}"))?;
    let document: RawDocument = serde_yaml::from_value(raw.clone())
        .wrap_err_with(|| format!("invalid scenario document: {source_file}"))?;
    Ok(LocatedDocument { raw, document, source_file: source_file.to_string(), base_dir })
}

fn validate_fragment_definition(
    name: &str,
    fragment: &RawFragment,
    source_file: &str,
) -> Result<()> {
    for (parameter, parameter_type) in &fragment.parameters {
        validate_component(parameter, "fragment parameter")
            .wrap_err_with(|| format!("invalid fragment '{name}' declared in '{source_file}'"))?;
        if !matches!(
            parameter_type.as_str(),
            "string" | "account_ref" | "address" | "u256" | "bool" | "bytes" | "bytes32" | "value"
        ) {
            bail!(
                "fragment '{name}' in '{source_file}' parameter '{parameter}' has unknown type '{parameter_type}'"
            );
        }
    }
    for (output, output_kind) in &fragment.outputs {
        validate_path(output, "fragment output")
            .wrap_err_with(|| format!("invalid fragment '{name}' declared in '{source_file}'"))?;
        if !matches!(output_kind.as_str(), "checkpoint" | "submit" | "receipt" | "log") {
            bail!(
                "fragment '{name}' in '{source_file}' output '{output}' has unknown type '{output_kind}'"
            );
        }
    }
    validate_fragment_scope(&LocatedFragment {
        name: name.to_string(),
        definition: fragment.clone(),
        source_file: source_file.to_string(),
    })?;
    Ok(())
}

fn validate_fragment_scope(fragment: &LocatedFragment) -> Result<FragmentScope> {
    let mut roots = BTreeMap::<String, String>::new();
    let mut aliases = BTreeSet::new();
    for (index, step) in fragment.definition.steps.iter().enumerate() {
        let label = step_label(&fragment.source_file, index);
        let root = if let Some(use_def) = parse_use(step, &label)? {
            validate_component(&use_def.alias, "fragment alias")
                .wrap_err_with(|| format!("invalid {label} in fragment '{}'", fragment.name))?;
            aliases.insert(use_def.alias.clone());
            Some((use_def.alias, "alias"))
        } else {
            concrete_action(step, &label)?;
            step_save(step, &label)?
                .map(|save| {
                    validate_component(save, "fragment-local save").wrap_err_with(|| {
                        format!("invalid {label} in fragment '{}'", fragment.name)
                    })?;
                    Ok::<_, eyre::Report>((save.to_string(), "save"))
                })
                .transpose()?
        };
        if let Some((root, kind)) = root &&
            let Some(previous) = roots.insert(root.clone(), format!("{kind} at {label}"))
        {
            bail!(
                "fragment '{}' in '{}' reuses local name '{}' for {} and {kind} at {label}",
                fragment.name,
                fragment.source_file,
                root,
                previous
            );
        }
    }
    Ok(FragmentScope { roots: roots.into_keys().collect(), aliases })
}

fn validate_root_scope(steps: &[serde_yaml::Value], source_file: &str) -> Result<()> {
    let mut aliases = BTreeMap::<String, String>::new();
    let mut save_roots = BTreeMap::<String, String>::new();
    for (index, step) in steps.iter().enumerate() {
        let label = step_label(source_file, index);
        if let Some(use_def) = parse_use(step, &label)? {
            validate_component(&use_def.alias, "fragment alias")
                .wrap_err_with(|| format!("invalid {label}"))?;
            if let Some(previous) = aliases.insert(use_def.alias.clone(), label.clone()) {
                bail!(
                    "duplicate scenario fragment alias '{}' at {label}; first declared at {previous}",
                    use_def.alias
                );
            }
        } else {
            concrete_action(step, &label)?;
            if let Some(save) = step_save(step, &label)? {
                validate_path(save, "scenario save")
                    .wrap_err_with(|| format!("invalid {label}"))?;
                let root = save.split('.').next().expect("validated nonempty save");
                save_roots.entry(root.to_string()).or_insert(label);
            }
        }
    }
    for (alias, alias_label) in aliases {
        if let Some(save_label) = save_roots.get(&alias) {
            bail!(
                "scenario fragment alias '{alias}' at {alias_label} conflicts with a save namespace at {save_label}"
            );
        }
    }
    Ok(())
}

fn parse_use(value: &serde_yaml::Value, label: &str) -> Result<Option<UseDef>> {
    let serde_yaml::Value::Mapping(mapping) = value else { return Ok(None) };
    if !mapping.contains_key(string_key("use")) {
        return Ok(None);
    }

    let mut unknown = Vec::new();
    for key in mapping.keys() {
        let key =
            key.as_str().ok_or_else(|| eyre::eyre!("{label} composition keys must be strings"))?;
        if !matches!(key, "use" | "as" | "with") {
            unknown.push(key.to_string());
        }
    }
    if !unknown.is_empty() {
        unknown.sort();
        bail!("{label} fragment use contains unknown field(s): {}", unknown.join(", "));
    }

    let fragment = mapping
        .get(string_key("use"))
        .and_then(serde_yaml::Value::as_str)
        .ok_or_else(|| eyre::eyre!("{label} `use` must name a fragment with a string"))?;
    validate_nonempty_name(fragment, "fragment").wrap_err_with(|| format!("invalid {label}"))?;
    let alias = mapping
        .get(string_key("as"))
        .and_then(serde_yaml::Value::as_str)
        .ok_or_else(|| eyre::eyre!("{label} fragment use requires string field `as`"))?;
    validate_component(alias, "fragment alias").wrap_err_with(|| format!("invalid {label}"))?;

    let arguments = match mapping.get(string_key("with")) {
        None => BTreeMap::new(),
        Some(serde_yaml::Value::Mapping(arguments)) => {
            let mut parsed = BTreeMap::new();
            for (key, value) in arguments {
                let key = key.as_str().ok_or_else(|| {
                    eyre::eyre!("{label} fragment parameter names must be strings")
                })?;
                parsed.insert(key.to_string(), value.clone());
            }
            parsed
        }
        Some(_) => {
            bail!("{label} fragment `with` must be a parameter mapping");
        }
    };

    Ok(Some(UseDef { fragment: fragment.to_string(), alias: alias.to_string(), arguments }))
}

fn transform_value(
    value: &serde_yaml::Value,
    scope: &ExpansionScope,
    label: &str,
    output_reference_checks: &mut Vec<OutputReferenceCheck>,
) -> Result<serde_yaml::Value> {
    match value {
        serde_yaml::Value::Mapping(mapping) => {
            if scope.fragment.is_some() &&
                mapping.len() == 1 &&
                mapping.contains_key(string_key("param"))
            {
                let parameter = mapping
                    .get(string_key("param"))
                    .and_then(serde_yaml::Value::as_str)
                    .ok_or_else(|| eyre::eyre!("{label} `param` expression must name a string"))?;
                return scope.parameters.get(parameter).cloned().ok_or_else(|| {
                    let context = scope
                        .fragment
                        .as_deref()
                        .map_or("outside a fragment".to_string(), |fragment| {
                            format!("in fragment '{fragment}'")
                        });
                    eyre::eyre!("{label} references unknown parameter '{parameter}' {context}")
                });
            }

            if mapping.len() == 1 &&
                let Some(path) =
                    mapping.get(string_key("var")).and_then(serde_yaml::Value::as_str)
            {
                let root = path.split('.').next().unwrap_or(path);
                if scope.local_roots.contains(root) {
                    let alias = scope
                        .alias_prefix
                        .as_deref()
                        .expect("local roots only exist in fragment scopes");
                    let rewritten_path = format!("{alias}.{path}");
                    if scope.local_aliases.contains(root) {
                        output_reference_checks.push(OutputReferenceCheck {
                            boundary_alias: format!("{alias}.{root}"),
                            path: rewritten_path.clone(),
                            label: label.to_string(),
                        });
                    }
                    let mut rewritten = serde_yaml::Mapping::new();
                    rewritten.insert(string_key("var"), serde_yaml::Value::String(rewritten_path));
                    return Ok(serde_yaml::Value::Mapping(rewritten));
                }
                output_reference_checks.push(OutputReferenceCheck {
                    boundary_alias: root.to_string(),
                    path: path.to_string(),
                    label: label.to_string(),
                });
                return Ok(value.clone());
            }

            let mut transformed = serde_yaml::Mapping::new();
            for (key, value) in mapping {
                transformed.insert(
                    key.clone(),
                    transform_value(value, scope, label, output_reference_checks)?,
                );
            }
            Ok(serde_yaml::Value::Mapping(transformed))
        }
        serde_yaml::Value::Sequence(values) => values
            .iter()
            .map(|value| transform_value(value, scope, label, output_reference_checks))
            .collect::<Result<Vec<_>>>()
            .map(serde_yaml::Value::Sequence),
        serde_yaml::Value::Tagged(tagged) => {
            let mut transformed = (**tagged).clone();
            transformed.value =
                transform_value(&transformed.value, scope, label, output_reference_checks)?;
            Ok(serde_yaml::Value::Tagged(Box::new(transformed)))
        }
        _ => Ok(value.clone()),
    }
}

fn concrete_action<'a>(value: &'a serde_yaml::Value, label: &str) -> Result<&'a str> {
    let serde_yaml::Value::Mapping(mapping) = value else {
        bail!("{label} scenario step must be a mapping");
    };
    let actions = mapping
        .keys()
        .filter_map(serde_yaml::Value::as_str)
        .filter(|key| !matches!(*key, "save" | "timeout"))
        .collect::<Vec<_>>();
    if actions.len() != 1 {
        bail!(
            "{label} scenario step must contain exactly one of `checkpoint`, `submit`, `wait_receipt`, or `wait_log`"
        );
    }
    let action = actions[0];
    if !matches!(action, "checkpoint" | "submit" | "wait_receipt" | "wait_log") {
        bail!("{label} has unknown scenario step action '{action}'");
    }
    Ok(action)
}

fn output_kind_for_action(action: &str) -> &str {
    match action {
        "checkpoint" => "checkpoint",
        "submit" => "submit",
        "wait_receipt" => "receipt",
        "wait_log" => "log",
        _ => unreachable!("concrete_action returned a known action"),
    }
}

fn step_save<'a>(value: &'a serde_yaml::Value, label: &str) -> Result<Option<&'a str>> {
    let serde_yaml::Value::Mapping(mapping) = value else {
        bail!("{label} scenario step must be a mapping");
    };
    mapping
        .get(string_key("save"))
        .map(|save| save.as_str().ok_or_else(|| eyre::eyre!("{label} `save` must be a string")))
        .transpose()
}

fn set_step_save(value: &mut serde_yaml::Value, save: String, label: &str) -> Result<()> {
    let serde_yaml::Value::Mapping(mapping) = value else {
        bail!("{label} scenario step must be a mapping");
    };
    mapping.insert(string_key("save"), serde_yaml::Value::String(save));
    Ok(())
}

fn scenario_steps(value: &serde_yaml::Value, source_file: &str) -> Result<Vec<serde_yaml::Value>> {
    let root = value
        .as_mapping()
        .ok_or_else(|| eyre::eyre!("scenario document '{source_file}' must be a mapping"))?;
    let scenario = root
        .get(string_key(SCENARIO_KEY))
        .and_then(serde_yaml::Value::as_mapping)
        .ok_or_else(|| eyre::eyre!("root scenario file '{source_file}' requires `scenario`"))?;
    scenario
        .get(string_key(STEPS_KEY))
        .and_then(serde_yaml::Value::as_sequence)
        .cloned()
        .ok_or_else(|| eyre::eyre!("root scenario file '{source_file}' requires `scenario.steps`"))
}

fn replace_scenario_steps(
    value: &mut serde_yaml::Value,
    steps: Vec<serde_yaml::Value>,
    source_file: &str,
) -> Result<()> {
    let root = value
        .as_mapping_mut()
        .ok_or_else(|| eyre::eyre!("scenario document '{source_file}' must be a mapping"))?;
    let scenario = root
        .get_mut(string_key(SCENARIO_KEY))
        .and_then(serde_yaml::Value::as_mapping_mut)
        .ok_or_else(|| eyre::eyre!("root scenario file '{source_file}' requires `scenario`"))?;
    scenario.insert(string_key(STEPS_KEY), serde_yaml::Value::Sequence(steps));
    Ok(())
}

fn resolve_root_chain_paths(value: &mut serde_yaml::Value, base_dir: &Path) -> Result<()> {
    let Some(root) = value.as_mapping_mut() else { return Ok(()) };
    let Some(chains) = root.get_mut(string_key("chains")) else { return Ok(()) };
    let Some(chains) = chains.as_mapping_mut() else { return Ok(()) };
    for chain in chains.values_mut() {
        let Some(chain) = chain.as_mapping_mut() else { continue };
        if let Some(workload) = chain.get_mut(string_key("workload")) &&
            let Some(path) = workload.as_str()
        {
            let path = Path::new(path);
            if !path.as_os_str().is_empty() && path.is_relative() {
                *workload = serde_yaml::Value::String(base_dir.join(path).display().to_string());
            }
        }
        if let Some(auth_map) = chain
            .get_mut(string_key("request_auth"))
            .and_then(serde_yaml::Value::as_mapping_mut)
            .and_then(|auth| auth.get_mut(string_key("sender_header")))
            .and_then(serde_yaml::Value::as_mapping_mut)
            .and_then(|header| header.get_mut(string_key("map"))) &&
            let Some(path) = auth_map.as_str()
        {
            let path = Path::new(path);
            if !path.as_os_str().is_empty() && path.is_relative() {
                *auth_map = serde_yaml::Value::String(base_dir.join(path).display().to_string());
            }
        }
    }
    Ok(())
}

fn remove_mapping_key(value: &mut serde_yaml::Value, name: &str) -> Result<()> {
    let mapping =
        value.as_mapping_mut().ok_or_else(|| eyre::eyre!("scenario document must be a mapping"))?;
    mapping.remove(string_key(name));
    Ok(())
}

fn mapping_contains(value: &serde_yaml::Value, name: &str) -> bool {
    value.as_mapping().is_some_and(|mapping| mapping.contains_key(string_key(name)))
}

fn string_key(name: &str) -> serde_yaml::Value {
    serde_yaml::Value::String(name.to_string())
}

fn join_path(prefix: Option<&str>, component: &str) -> String {
    prefix.map_or_else(|| component.to_string(), |prefix| format!("{prefix}.{component}"))
}

fn validate_nonempty_name(name: &str, context: &str) -> Result<()> {
    if name.trim().is_empty() {
        bail!("{context} name must not be empty");
    }
    Ok(())
}

fn validate_component(name: &str, context: &str) -> Result<()> {
    validate_nonempty_name(name, context)?;
    if name.contains('.') {
        bail!("{context} '{name}' must be one name component and cannot contain '.'");
    }
    Ok(())
}

fn validate_path(path: &str, context: &str) -> Result<()> {
    validate_nonempty_name(path, context)?;
    if path.split('.').any(str::is_empty) {
        bail!("{context} '{path}' contains an empty path component");
    }
    Ok(())
}

fn step_label(source_file: &str, local_step_index: usize) -> String {
    format!("step {} in '{source_file}'", local_step_index + 1)
}

fn expanded_step_label(
    source_file: &str,
    local_step_index: usize,
    expanded_step_index: usize,
    scope: &ExpansionScope,
) -> String {
    match (&scope.fragment, &scope.alias_prefix) {
        (Some(fragment), Some(alias)) => format!(
            "expanded step {}, source '{}', fragment '{}', instance '{}', local step {}",
            expanded_step_index + 1,
            source_file,
            fragment,
            alias,
            local_step_index + 1
        ),
        _ => format!(
            "expanded step {} from step {} in '{}'",
            expanded_step_index + 1,
            local_step_index + 1,
            source_file
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    struct TestDir(PathBuf);

    impl TestDir {
        fn new(label: &str) -> Self {
            let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "txgen-scenario-composition-{label}-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }

        fn write(&self, relative: &str, content: &str) {
            let path = self.0.join(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(path, content).unwrap();
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn error_chain(error: eyre::Report) -> String {
        error.chain().map(ToString::to_string).collect::<Vec<_>>().join(": ")
    }

    const LEGACY: &str = r#"
version: 1
chains:
  primary:
    network: tempo
    rpc_url: http://primary.invalid
    workload: ./workload.yml
scenario:
  name: legacy
  steps:
    - checkpoint:
        chain: primary
      save: before
"#;

    #[test]
    fn preserves_legacy_scenario_documents() {
        let resolved = parse_scenario(LEGACY).unwrap();
        assert_eq!(resolved.spec.scenario.steps.len(), 1);
        assert_eq!(resolved.spec.scenario.steps[0].save.as_deref(), Some("before"));
        assert!(resolved.spec.scenario.steps[0].provenance.is_none());
        let rendered = serde_yaml::to_string(&resolved.rendered).unwrap();
        assert!(!rendered.contains("fragments:"));
        assert!(!rendered.contains("include:"));
    }

    #[test]
    fn preserves_literal_param_keys_outside_exact_fragment_expressions() {
        let yaml = LEGACY.replace(
            "    - checkpoint:\n        chain: primary\n      save: before\n",
            "    - submit:\n        chain: primary\n        template: literal-data\n        with:\n          exact: { param: literal }\n          mixed: { param: literal, other: true }\n",
        );
        let resolved = parse_scenario(&yaml).unwrap();
        let super::super::schema::StepAction::Submit(submit) =
            &resolved.spec.scenario.steps[0].action
        else {
            panic!("expected submit step");
        };
        assert_eq!(submit.with_value["exact"]["param"].as_str(), Some("literal"));
        assert_eq!(submit.with_value["mixed"]["param"].as_str(), Some("literal"));
    }

    #[test]
    fn expands_repeated_inline_fragment_instances() {
        let yaml = format!(
            "{LEGACY}\nfragments:\n  mark:\n    parameters:\n      chain: string\n    outputs:\n      point: checkpoint\n    steps:\n      - checkpoint:\n          chain: {{ param: chain }}\n        save: point\n",
        )
        .replace(
            "    - checkpoint:\n        chain: primary\n      save: before\n",
            "    - use: mark\n      as: first\n      with: { chain: primary }\n    - use: mark\n      as: second\n      with: { chain: primary }\n",
        );
        let resolved = parse_scenario(&yaml).unwrap();
        let saves = resolved
            .spec
            .scenario
            .steps
            .iter()
            .map(|step| step.save.as_deref().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(saves, ["first.point", "second.point"]);
        let first = resolved.spec.scenario.steps[0].provenance.as_ref().unwrap();
        assert_eq!(first.fragment, "mark");
        assert_eq!(first.instance_alias, "first");
        assert_eq!(first.local_step_name, "point");
    }

    #[test]
    fn exposes_only_declared_namespaced_outputs_to_callers() {
        let yaml = r#"
version: 1
chains:
  primary: { network: tempo, rpc_url: http://primary.invalid, workload: ./workload.yml }
fragments:
  capture:
    outputs: { cursor: checkpoint }
    steps:
      - checkpoint: { chain: primary }
        save: hidden
      - checkpoint: { chain: primary }
        save: cursor
scenario:
  name: outputs
  steps:
    - use: capture
      as: item
    - wait_receipt:
        chain: primary
        transaction_hash: { var: item.cursor.block_hash }
"#;
        let resolved = parse_scenario(yaml).unwrap();
        assert_eq!(resolved.spec.scenario.steps.len(), 3);

        let error =
            parse_scenario(&yaml.replace("item.cursor.block_hash", "item.hidden.block_hash"))
                .unwrap_err()
                .to_string();
        assert!(error.contains("private or unknown output"), "{error}");
        assert!(error.contains("item.hidden.block_hash"), "{error}");
        assert!(error.contains("declared outputs: cursor"), "{error}");
    }

    #[test]
    fn nested_outputs_must_be_reexported_by_the_parent() {
        let yaml = r#"
version: 1
chains:
  primary: { network: tempo, rpc_url: http://primary.invalid, workload: ./workload.yml }
fragments:
  inner:
    outputs: { done: checkpoint }
    steps:
      - checkpoint: { chain: primary }
        save: done
  outer:
    steps:
      - use: inner
        as: child
scenario:
  name: nested-outputs
  steps:
    - use: outer
      as: top
    - wait_receipt:
        chain: primary
        transaction_hash: { var: top.child.done.block_hash }
"#;
        let private = parse_scenario(yaml).unwrap_err().to_string();
        assert!(private.contains("fragment instance 'top'"), "{private}");

        let exported = yaml.replace(
            "  outer:\n    steps:",
            "  outer:\n    outputs: { child.done: checkpoint }\n    steps:",
        );
        parse_scenario(&exported).unwrap();
    }

    #[test]
    fn caller_parameter_values_are_not_captured_by_local_saves() {
        let yaml = r#"
version: 1
chains:
  primary: { network: tempo, rpc_url: http://primary.invalid, workload: ./workload.yml }
fragments:
  observe:
    parameters: { start: u256 }
    outputs: { observed: log }
    steps:
      - checkpoint: { chain: primary }
        save: submission
      - wait_log:
          chain: primary
          from_block: { param: start }
          abi: Events
          event: Seen
        save: observed
scenario:
  name: capture
  steps:
    - checkpoint: { chain: primary }
      save: submission
    - use: observe
      as: nested
      with:
        start: { var: submission.block_number }
"#;
        let resolved = parse_scenario(yaml).unwrap();
        let rendered = serde_yaml::to_string(&resolved.rendered).unwrap();
        assert!(rendered.contains("var: submission.block_number"));
        assert!(!rendered.contains("var: nested.submission.block_number"));
    }

    #[test]
    fn loads_nested_relative_includes_and_nested_fragments() {
        let dir = TestDir::new("relative");
        dir.write(
            "library/nested/leaf.yml",
            r#"
version: 1
fragments:
  leaf:
    outputs: { done: checkpoint }
    steps:
      - checkpoint: { chain: primary }
        save: done
"#,
        );
        dir.write(
            "library/outer.yml",
            r#"
include: nested/leaf.yml
fragments:
  outer:
    outputs: { child.done: checkpoint }
    steps:
      - use: leaf
        as: child
"#,
        );
        dir.write(
            "scenario.yml",
            r#"
version: 1
include: library/outer.yml
chains:
  primary:
    network: tempo
    rpc_url: http://primary.invalid
    request_auth:
      sender_header:
        name: X-Authorization-Token
        map: ./sender-auth.json
    workload: ./workload.yml
scenario:
  name: nested
  steps:
    - use: outer
      as: top
"#,
        );

        let resolved = load_scenario(&dir.path().join("scenario.yml")).unwrap();
        assert_eq!(resolved.spec.scenario.steps[0].save.as_deref(), Some("top.child.done"));
        let provenance = resolved.spec.scenario.steps[0].provenance.as_ref().unwrap();
        assert_eq!(provenance.fragment, "leaf");
        assert_eq!(provenance.instance_alias, "top.child");
        assert_eq!(resolved.spec.chains["primary"].workload, dir.path().join("workload.yml"));
        assert_eq!(
            resolved.spec.chains["primary"].request_auth.as_ref().unwrap().sender_header.map,
            dir.path().join("sender-auth.json")
        );
    }

    #[cfg(unix)]
    #[test]
    fn preserves_declaring_path_when_root_is_loaded_through_a_symlink() {
        use std::os::unix::fs::symlink;

        let dir = TestDir::new("symlink-root");
        dir.write("actual/scenario.yml", LEGACY);
        fs::create_dir_all(dir.path().join("entry")).unwrap();
        symlink("../actual/scenario.yml", dir.path().join("entry/scenario.yml")).unwrap();

        let resolved = load_scenario(&dir.path().join("entry/scenario.yml")).unwrap();
        assert_eq!(resolved.spec.chains["primary"].workload, dir.path().join("entry/workload.yml"));
    }

    #[test]
    fn rendering_is_deterministic_and_removes_composition_operators() {
        let yaml = r#"
version: 1
fragments:
  mark:
    parameters: { chain: string }
    outputs: { point: checkpoint }
    steps:
      - checkpoint: { chain: { param: chain } }
        save: point
chains:
  primary: { network: tempo, rpc_url: http://primary.invalid, workload: ./workload.yml }
scenario:
  name: deterministic
  steps:
    - use: mark
      as: here
      with: { chain: primary }
"#;
        let first = serde_yaml::to_string(&parse_scenario(yaml).unwrap().rendered).unwrap();
        let second = serde_yaml::to_string(&parse_scenario(yaml).unwrap().rendered).unwrap();
        assert_eq!(first, second);
        assert!(!first.contains("fragments:"));
        assert!(!first.contains("use:"));
        assert!(!first.contains("param:"));
        assert!(first.contains("save: here.point"));
    }

    #[test]
    fn rejects_include_cycles_and_root_fields_in_includes() {
        let dir = TestDir::new("include-errors");
        dir.write("missing.yml", &format!("{LEGACY}\ninclude: absent.yml\n"));
        let missing = error_chain(load_scenario(&dir.path().join("missing.yml")).unwrap_err());
        assert!(missing.contains("absent.yml"), "{missing}");
        assert!(missing.contains("declared by"), "{missing}");

        dir.write("a.yml", "include: b.yml\nfragments: {}\n");
        dir.write("b.yml", "include: a.yml\nfragments: {}\n");
        let cycle = error_chain(load_scenario(&dir.path().join("a.yml")).unwrap_err());
        assert!(cycle.contains("cyclic scenario include"), "{cycle}");

        dir.write("bad.yml", "chains: {}\nscenario: { name: bad, steps: [] }\nfragments: {}\n");
        dir.write("root.yml", &format!("{LEGACY}\ninclude: bad.yml\n"));
        let root_fields = error_chain(load_scenario(&dir.path().join("root.yml")).unwrap_err());
        assert!(root_fields.contains("root-only"), "{root_fields}");
    }

    #[test]
    fn rejects_duplicate_fragment_contributions() {
        let dir = TestDir::new("duplicate-fragments");
        dir.write(
            "one.yml",
            "fragments: { shared: { steps: [{ checkpoint: { chain: primary } }] } }\n",
        );
        dir.write(
            "two.yml",
            "fragments: { shared: { steps: [{ checkpoint: { chain: primary } }] } }\n",
        );
        dir.write("root.yml", &format!("{LEGACY}\ninclude: [one.yml, two.yml]\n"));
        let error = error_chain(load_scenario(&dir.path().join("root.yml")).unwrap_err());
        assert!(error.contains("duplicate scenario fragment 'shared'"), "{error}");
        assert!(error.contains("one.yml"), "{error}");
        assert!(error.contains("two.yml"), "{error}");
    }

    #[test]
    fn rejects_fragment_recursion() {
        let yaml = r#"
version: 1
chains:
  primary: { network: tempo, rpc_url: http://primary.invalid, workload: ./workload.yml }
fragments:
  a:
    steps: [{ use: b, as: b }]
  b:
    steps: [{ use: a, as: a }]
scenario:
  name: recursive
  steps: [{ use: a, as: root }]
"#;
        let error = parse_scenario(yaml).unwrap_err().to_string();
        assert!(error.contains("recursive scenario fragment"), "{error}");
        assert!(error.contains("a -> b -> a"), "{error}");
    }

    #[test]
    fn validates_unreachable_fragment_definitions() {
        let base = r#"
version: 1
chains:
  primary: { network: tempo, rpc_url: http://primary.invalid, workload: ./workload.yml }
fragments:
  DEFINITIONS
scenario:
  name: unused-fragments
  steps: [{ checkpoint: { chain: primary } }]
"#;
        let missing_output = base.replace(
            "DEFINITIONS",
            "broken:\n    outputs: { missing: checkpoint }\n    steps: [{ checkpoint: { chain: primary } }]",
        );
        let error = parse_scenario(&missing_output).unwrap_err().to_string();
        assert!(error.contains("produces no accessible save"), "{error}");

        let unknown_nested =
            base.replace("DEFINITIONS", "broken:\n    steps: [{ use: absent, as: nested }]");
        let error = parse_scenario(&unknown_nested).unwrap_err().to_string();
        assert!(error.contains("unknown scenario fragment 'absent'"), "{error}");

        let recursive = base.replace(
            "DEFINITIONS",
            "a:\n    steps: [{ use: b, as: b }]\n  b:\n    steps: [{ use: a, as: a }]",
        );
        let error = parse_scenario(&recursive).unwrap_err().to_string();
        assert!(error.contains("recursive scenario fragment"), "{error}");
    }

    #[test]
    fn rejects_missing_and_unknown_parameters() {
        let base = r#"
version: 1
chains:
  primary: { network: tempo, rpc_url: http://primary.invalid, workload: ./workload.yml }
fragments:
  mark:
    parameters: { chain: string }
    steps: [{ checkpoint: { chain: { param: chain } } }]
scenario:
  name: parameters
  steps:
    - use: mark
      as: item
      WITH
"#;
        let missing = parse_scenario(&base.replace("      WITH\n", "")).unwrap_err().to_string();
        assert!(missing.contains("missing required parameter(s): chain"), "{missing}");

        let unknown =
            parse_scenario(&base.replace("WITH", "with: { chain: primary, extra: true }"))
                .unwrap_err()
                .to_string();
        assert!(unknown.contains("unknown parameter(s): extra"), "{unknown}");
    }

    #[test]
    fn rejects_parameter_type_mismatches() {
        let yaml = r#"
version: 1
chains:
  primary: { network: tempo, rpc_url: http://primary.invalid, workload: ./workload.yml }
fragments:
  submit:
    parameters: { amount: u256 }
    steps:
      - submit:
          chain: primary
          template: transfer
          with: { amount: { param: amount } }
scenario:
  name: types
  steps:
    - use: submit
      as: invalid
      with: { amount: -1 }
"#;
        let error = parse_scenario(yaml).unwrap_err().to_string();
        assert!(error.contains("parameter 'amount'"), "{error}");
        assert!(error.contains("expects parameter type 'u256'"), "{error}");
        assert!(error.contains("expanded step 1"), "{error}");
    }

    #[test]
    fn rejects_duplicate_aliases_and_local_saves() {
        let duplicate_alias = r#"
version: 1
chains:
  primary: { network: tempo, rpc_url: http://primary.invalid, workload: ./workload.yml }
fragments:
  mark:
    steps: [{ checkpoint: { chain: primary } }]
scenario:
  name: aliases
  steps:
    - { use: mark, as: same }
    - { use: mark, as: same }
"#;
        let error = parse_scenario(duplicate_alias).unwrap_err().to_string();
        assert!(error.contains("duplicate scenario fragment alias 'same'"), "{error}");

        let duplicate_save = duplicate_alias.replace(
            "steps: [{ checkpoint: { chain: primary } }]",
            "steps:\n      - { checkpoint: { chain: primary }, save: point }\n      - { checkpoint: { chain: primary }, save: point }",
        );
        let error = parse_scenario(&duplicate_save).unwrap_err().to_string();
        assert!(error.contains("reuses local name 'point'"), "{error}");
    }

    #[test]
    fn validates_declared_outputs() {
        let base = r#"
version: 1
chains:
  primary: { network: tempo, rpc_url: http://primary.invalid, workload: ./workload.yml }
fragments:
  mark:
    outputs: { point: EXPECTED }
    steps:
      - checkpoint: { chain: primary }
        save: ACTUAL
scenario:
  name: outputs
  steps: [{ use: mark, as: item }]
"#;
        let missing =
            parse_scenario(&base.replace("EXPECTED", "checkpoint").replace("ACTUAL", "other"))
                .unwrap_err()
                .to_string();
        assert!(missing.contains("produces no accessible save"), "{missing}");

        let wrong_kind =
            parse_scenario(&base.replace("EXPECTED", "receipt").replace("ACTUAL", "point"))
                .unwrap_err()
                .to_string();
        assert!(wrong_kind.contains("save has type 'checkpoint'"), "{wrong_kind}");
        for context in ["expanded step 1", "<inline>", "fragment 'mark'", "instance 'item'"] {
            assert!(missing.contains(context), "missing {context:?} in: {missing}");
        }
    }

    #[test]
    fn typed_fragment_step_errors_include_expansion_provenance() {
        let yaml = r#"
version: 1
chains:
  primary: { network: tempo, rpc_url: http://primary.invalid, workload: ./workload.yml }
fragments:
  malformed:
    steps:
      - checkpoint: { chain: 7 }
scenario:
  name: diagnostics
  steps: [{ use: malformed, as: broken }]
"#;
        let error = error_chain(parse_scenario(yaml).unwrap_err());
        for context in [
            "expanded step 1",
            "source '<inline>'",
            "fragment 'malformed'",
            "instance 'broken'",
            "local step 1",
        ] {
            assert!(error.contains(context), "missing {context:?} in: {error}");
        }
    }

    #[test]
    fn validates_forward_references_after_expansion() {
        let yaml = r#"
version: 1
chains:
  primary: { network: tempo, rpc_url: http://primary.invalid, workload: ./workload.yml }
fragments:
  invalid:
    outputs: { receipt: receipt, later: submit }
    steps:
      - wait_receipt:
          chain: primary
          transaction_hash: { var: later.tx_hash }
        save: receipt
      - submit:
          chain: primary
          template: transfer
        save: later
scenario:
  name: forward
  steps: [{ use: invalid, as: item }]
"#;
        let error = parse_scenario(yaml).unwrap_err().to_string();
        assert!(error.contains("forward reference"), "{error}");
        assert!(error.contains("item.later"), "{error}");
    }
}
