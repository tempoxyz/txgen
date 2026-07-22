use serde_yaml::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static NEXT_TEMP_DIRECTORY: AtomicU64 = AtomicU64::new(0);

#[test]
fn validates_and_deterministically_renders_composed_scenario() {
    let directory = TestDirectory::new("render");
    let scenario = write_valid_fixture(directory.path());

    let validation = scenario_command("validate", &scenario, None);
    assert_success("scenario validate", &validation);
    assert_eq!(
        String::from_utf8(validation.stdout).unwrap(),
        "scenario 'composed-checkpoints' is valid (3 expanded steps)\n"
    );

    let first = scenario_command("render", &scenario, None);
    assert_success("first scenario render", &first);
    let second = scenario_command("render", &scenario, None);
    assert_success("second scenario render", &second);
    assert_eq!(first.stdout, second.stdout, "rendered YAML must be deterministic");

    let rendered: Value = serde_yaml::from_slice(&first.stdout).expect("rendered scenario is YAML");
    assert_no_composition_structure(&rendered);
    let steps = rendered
        .get("scenario")
        .and_then(|scenario| scenario.get("steps"))
        .and_then(Value::as_sequence)
        .expect("rendered scenario steps");
    assert_eq!(steps.len(), 3);
    assert_eq!(steps[0].get("save").and_then(Value::as_str), Some("first.cursor"));
    assert_eq!(steps[1].get("save").and_then(Value::as_str), Some("inline_cursor"));
    assert_eq!(steps[2].get("save").and_then(Value::as_str), Some("second.cursor"));
    for step in steps {
        assert_eq!(
            step.get("checkpoint")
                .and_then(|checkpoint| checkpoint.get("chain"))
                .and_then(Value::as_str),
            Some("primary")
        );
    }

    let output_path = directory.path().join("rendered.yaml");
    let file_render = scenario_command("render", &scenario, Some(&output_path));
    assert_success("scenario render --output", &file_render);
    assert!(file_render.stdout.is_empty(), "--output must leave stdout empty");
    assert_eq!(fs::read(output_path).unwrap(), first.stdout);

    let rendered_validation =
        scenario_command("validate", &directory.path().join("rendered.yaml"), None);
    assert_success("validate rendered scenario", &rendered_validation);
}

#[test]
fn validate_reports_fragment_use_context() {
    let directory = TestDirectory::new("invalid");
    fs::write(directory.path().join("workload.yaml"), "chain_id: 1\n").unwrap();
    let scenario = directory.path().join("malformed.yaml");
    fs::write(
        &scenario,
        r#"version: 1
fragments:
  capture:
    parameters:
      chain: string
    outputs:
      cursor: checkpoint
    steps:
      - checkpoint:
          chain: { param: chain }
        save: cursor
chains:
  primary:
    network: tempo
    rpc_url: http://127.0.0.1:1
    chain_id: 1
    workload: ./workload.yaml
scenario:
  name: malformed-composition
  steps:
    - use: capture
      as: broken
      with: {}
"#,
    )
    .unwrap();

    let output = scenario_command("validate", &scenario, None);
    assert!(!output.status.success(), "malformed fragment use unexpectedly validated");
    let stderr = String::from_utf8_lossy(&output.stderr);
    for expected in ["malformed.yaml", "capture", "broken", "chain"] {
        assert!(
            stderr.contains(expected),
            "validation error did not contain {expected:?}\nstderr:\n{stderr}"
        );
    }
}

#[test]
fn validate_rejects_static_errors_after_fragment_expansion() {
    let event_abi = r#"[
  {
    "type": "event",
    "name": "Seen",
    "anonymous": false,
    "inputs": [{ "name": "amount", "type": "uint256", "indexed": false }]
  }
]"#;
    let cases = [
        (
            "chain",
            "chain_id: 1\n",
            None,
            "      - checkpoint: { chain: missing }\n",
            "references unknown chain 'missing'",
        ),
        (
            "template",
            "chain_id: 1\n",
            None,
            "      - submit: { chain: primary, template: missing }\n",
            "references missing template 'missing'",
        ),
        (
            "abi",
            "chain_id: 1\n",
            None,
            "      - wait_log: { chain: primary, from_block: 0, abi: Missing, event: Seen }\n",
            "references missing ABI artifact 'Missing'",
        ),
        (
            "event",
            "chain_id: 1\nartifacts:\n  Events: ./events.json\n",
            Some(event_abi),
            "      - wait_log: { chain: primary, from_block: 0, abi: Events, event: Missing }\n",
            "has an invalid event 'Missing'",
        ),
        (
            "filter",
            "chain_id: 1\nartifacts:\n  Events: ./events.json\n",
            Some(event_abi),
            "      - wait_log:\n          chain: primary\n          from_block: 0\n          abi: Events\n          event: Seen\n          where: { amount: false }\n",
            "event filter 'amount' expects ABI type 'uint256'",
        ),
    ];

    for (name, workload, artifact, fragment_step, expected) in cases {
        let directory = TestDirectory::new(name);
        let scenario =
            write_static_error_fixture(directory.path(), workload, artifact, fragment_step);
        let output = scenario_command("validate", &scenario, None);
        assert!(!output.status.success(), "{name} fixture unexpectedly validated");
        let stderr = String::from_utf8_lossy(&output.stderr);
        for context in [expected, "expanded step 1", "fragment 'broken'", "instance 'case'"] {
            assert!(
                stderr.contains(context),
                "{name} error did not contain {context:?}\nstderr:\n{stderr}"
            );
        }
        assert!(stderr.contains("scenario.yaml"), "{name} error omitted source file: {stderr}");
    }
}

fn write_valid_fixture(directory: &Path) -> PathBuf {
    fs::write(directory.join("workload.yaml"), "chain_id: 1\n").unwrap();
    let scenario = directory.join("scenario.yaml");
    fs::write(
        &scenario,
        r#"version: 1
fragments:
  capture:
    parameters:
      chain: string
    outputs:
      cursor: checkpoint
    steps:
      - checkpoint:
          chain: { param: chain }
        save: cursor
chains:
  primary:
    network: tempo
    rpc_url: http://127.0.0.1:1
    chain_id: 1
    workload: ./workload.yaml
scenario:
  name: composed-checkpoints
  steps:
    - use: capture
      as: first
      with:
        chain: primary
    - checkpoint:
        chain: primary
      save: inline_cursor
    - use: capture
      as: second
      with:
        chain: primary
"#,
    )
    .unwrap();
    scenario
}

fn write_static_error_fixture(
    directory: &Path,
    workload: &str,
    artifact: Option<&str>,
    fragment_step: &str,
) -> PathBuf {
    fs::write(directory.join("workload.yaml"), workload).unwrap();
    if let Some(artifact) = artifact {
        fs::write(directory.join("events.json"), artifact).unwrap();
    }
    let scenario = directory.join("scenario.yaml");
    fs::write(
        &scenario,
        format!(
            r#"version: 1
fragments:
  broken:
    steps:
{fragment_step}chains:
  primary:
    network: tempo
    rpc_url: http://127.0.0.1:1
    chain_id: 1
    workload: ./workload.yaml
scenario:
  name: static-error
  steps:
    - use: broken
      as: case
"#
        ),
    )
    .unwrap();
    scenario
}

fn scenario_command(command: &str, scenario: &Path, output: Option<&Path>) -> Output {
    let mut process = Command::new(env!("CARGO_BIN_EXE_txgen-tempo"));
    process.arg("scenario").arg(command).arg("--scenario").arg(scenario);
    if let Some(output) = output {
        process.arg("--output").arg(output);
    }
    process.output().expect("run txgen-tempo scenario command")
}

fn assert_success(command: &str, output: &Output) {
    assert!(
        output.status.success(),
        "{command} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_no_composition_structure(value: &Value) {
    let root = value.as_mapping().expect("rendered scenario root is a mapping");
    assert!(!root.contains_key("include"), "rendered scenario retained root include");
    assert!(!root.contains_key("fragments"), "rendered scenario retained fragment declarations");
    let steps =
        value["scenario"]["steps"].as_sequence().expect("rendered scenario steps are a sequence");
    for step in steps {
        assert!(
            !step.as_mapping().is_some_and(|step| step.contains_key("use")),
            "rendered scenario retained a fragment use"
        );
    }
}

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(name: &str) -> Self {
        let unique = NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!(
            "txgen-scenario-composition-{name}-{}-{timestamp}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create scenario composition test directory");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
