#![cfg(unix)]

use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use txgen_cli::scenario::{execute_scenario, ScenarioExecutionConfig, ScenarioSpec};
use txgen_ethereum::EthereumAdapter;

const SAVED_VALUE: &str = "saved-value-fed-to-next-command";

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let suffix =
            SystemTime::now().duration_since(UNIX_EPOCH).expect("clock after epoch").as_nanos();
        let path = std::env::temp_dir()
            .join(format!("txgen-command-scenario-{}-{suffix}", std::process::id()));
        fs::create_dir_all(&path).expect("create temporary test directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn command_only_scenario_saves_json_for_later_command() {
    let scenario = ScenarioSpec::parse(&format!(
        r#"version: 1
scenario:
  name: command-values
  steps:
    - command:
        program: /bin/sh
        args:
          - -c
          - |
            test "$TEST_COMMAND_ENV" = "expected-environment" || exit 20
            test "$1" = "expected-argument" || exit 21
            printf '%s' '{{"payload":"{SAVED_VALUE}"}}'
          - txgen-command-test
          - expected-argument
        env:
          TEST_COMMAND_ENV: expected-environment
        stdout: json
      save: produced
    - command:
        program: /bin/sh
        args:
          - -c
          - |
            test "$1" = "{SAVED_VALUE}" || exit 22
            printf '%s' '{{"accepted":true}}'
          - txgen-command-test
          - {{ var: produced.payload }}
        stdout: json
      save: consumed
"#
    ))
    .expect("parse command-only scenario");

    let report = tokio::time::timeout(
        Duration::from_secs(3),
        execute_scenario::<EthereumAdapter>(
            scenario,
            ScenarioExecutionConfig {
                allow_commands: true,
                max_command_in_flight: 1,
                sample_instances: 1,
                ..Default::default()
            },
        ),
    )
    .await
    .expect("command-only scenario timed out")
    .expect("command-only scenario execution failed");

    assert_eq!(report.configuration.chains.len(), 0);
    assert!(report.configuration.commands_enabled);
    assert_eq!(report.configuration.maximum_commands_in_flight, 1);
    assert_eq!(report.started, 1);
    assert_eq!(report.completed, 1);
    assert_eq!(report.failed, 0);
    assert_eq!(report.steps.len(), 2);
    assert!(report
        .steps
        .iter()
        .all(|step| { step.kind == "command" && step.success == 1 && step.failed == 0 }));
}

#[tokio::test]
async fn commands_are_rejected_by_default_before_execution() {
    let directory = TempDir::new();
    let marker = directory.path().join("command-ran");
    let scenario = ScenarioSpec::parse(&format!(
        r#"version: 1
chains:
  must_not_initialize:
    network: ethereum
    rpc_url: http://127.0.0.1:1
    chain_id: auto
    workload: /definitely/not/a/txgen-workload.yaml
scenario:
  name: command-disabled
  steps:
    - command:
        program: /bin/sh
        args:
          - -c
          - |
            touch '{}'
            printf '%s' '{{"ran":true}}'
        stdout: json
"#,
        marker.display()
    ))
    .expect("parse disabled-command scenario");

    let error = execute_scenario::<EthereumAdapter>(scenario, ScenarioExecutionConfig::default())
        .await
        .expect_err("commands should require explicit opt-in");

    assert!(error.to_string().contains("--allow-commands"), "unexpected opt-in error: {error}");
    assert!(!marker.exists(), "disabled command unexpectedly executed");
}

#[tokio::test]
async fn command_paths_are_preflighted_before_chain_initialization() {
    let scenario = ScenarioSpec::parse(
        r#"version: 1
chains:
  must_not_initialize:
    network: ethereum
    rpc_url: http://127.0.0.1:1
    chain_id: auto
    workload: /definitely/not/a/txgen-workload.yaml
scenario:
  name: command-preflight
  steps:
    - command:
        program: /definitely/not/a/txgen-command
        stdout: json
"#,
    )
    .expect("parse command-preflight scenario");

    let error = execute_scenario::<EthereumAdapter>(
        scenario,
        ScenarioExecutionConfig { allow_commands: true, ..Default::default() },
    )
    .await
    .expect_err("missing command program should fail preflight");

    assert!(
        error.to_string().contains("command program does not exist"),
        "unexpected preflight error: {error}"
    );
}

#[tokio::test]
async fn reports_do_not_serialize_command_inputs_or_captured_output() {
    const ARG_SECRET: &str = "argv-secret-5e7e";
    const ENV_SECRET: &str = "environment-secret-3c91";
    const STDOUT_SECRET: &str = "stdout-secret-c841";
    const STDERR_SECRET: &str = "stderr-secret-a23b";

    let scenario = ScenarioSpec::parse(&format!(
        r#"version: 1
scenario:
  name: command-redaction
  steps:
    - command:
        program: /bin/sh
        args:
          - -c
          - |
            printf '%s' '{{"secret":"{STDOUT_SECRET}"}}'
            printf '%s' '{STDERR_SECRET}' >&2
            exit 23
          - txgen-command-test
          - {ARG_SECRET}
        env:
          TEST_COMMAND_SECRET: {ENV_SECRET}
        stdout: json
"#
    ))
    .expect("parse command-redaction scenario");

    let report = execute_scenario::<EthereumAdapter>(
        scenario,
        ScenarioExecutionConfig {
            allow_commands: true,
            max_command_in_flight: 1,
            sample_instances: 1,
            ..Default::default()
        },
    )
    .await
    .expect("execute command-redaction scenario");

    assert_eq!(report.failed, 1);
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].classification, "command_exit_nonzero");
    let serialized = serde_json::to_string(&report).expect("serialize scenario report");
    for secret in [ARG_SECRET, ENV_SECRET, STDOUT_SECRET, STDERR_SECRET] {
        assert!(!serialized.contains(secret), "scenario report leaked {secret}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn command_timeout_is_classified_and_finishes_promptly() {
    let scenario = ScenarioSpec::parse(
        r#"version: 1
scenario:
  name: command-timeout
  steps:
    - command:
        program: /bin/sh
        args:
          - -c
          - sleep 30
        stdout: json
      timeout: 50ms
"#,
    )
    .expect("parse command-timeout scenario");

    let report = tokio::time::timeout(
        Duration::from_secs(3),
        execute_scenario::<EthereumAdapter>(
            scenario,
            ScenarioExecutionConfig {
                allow_commands: true,
                max_command_in_flight: 1,
                ..Default::default()
            },
        ),
    )
    .await
    .expect("timed-out command was not killed and reaped")
    .expect("execute command-timeout scenario");

    assert_eq!(report.started, 1);
    assert_eq!(report.completed, 0);
    assert_eq!(report.failed, 1);
    assert_eq!(report.timed_out, 1);
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].classification, "timeout");
}
