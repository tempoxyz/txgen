use serde_json::Value;
use std::{
    fs,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

const TEST_MNEMONIC: &str = "test test test test test test test test test test test junk";

fn generate(spec: &str) -> (std::process::Output, Vec<Value>) {
    let test_dir = std::env::temp_dir().join(format!(
        "txgen-setup-order-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    ));
    fs::create_dir_all(&test_dir).unwrap();
    let spec_path = test_dir.join("spec.yaml");
    let output_path = test_dir.join("generated.ndjson");
    fs::write(&spec_path, spec).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_txgen-tempo"))
        .args(["generate", "--spec"])
        .arg(&spec_path)
        .args(["--count", "0", "--seed", "1", "--output"])
        .arg(&output_path)
        .output()
        .unwrap();
    let txs = fs::read_to_string(&output_path)
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    fs::remove_dir_all(test_dir).unwrap();
    (output, txs)
}

fn generated(spec: &str) -> Vec<Value> {
    let (output, txs) = generate(spec);
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    txs
}

#[test]
fn setup_preserves_nonce_lanes_without_global_inclusion_keys() {
    let txs = generated(&setup_scheduling_spec());
    assert_eq!(txs.len(), 4);
    assert_eq!(txs[0]["id"], "setup.warmup_one");
    assert_eq!(txs[1]["id"], "setup.warmup_two");
    assert_eq!(txs[2]["id"], "setup.authorize_users[0]");
    assert_eq!(txs[3]["id"], "setup.authorize_users[1]");
    for tx in &txs {
        assert_eq!(tx["phase"], "setup");
        assert_eq!(tx["inclusion_keys"], serde_json::json!([]));
        assert!(tx.get("depends_on").is_none());
        assert_eq!(tx["submission_keys"].as_array().unwrap().len(), 1);
    }
    assert_eq!(txs[0]["sender"], txs[2]["sender"]);
    assert_eq!(txs[0]["submission_keys"], txs[2]["submission_keys"]);
    assert_ne!(txs[0]["sender"], txs[1]["sender"]);
    assert_ne!(txs[0]["submission_keys"], txs[1]["submission_keys"]);
}

#[test]
fn setup_exposes_ordered_and_expiring_nonce_lanes_to_sender() {
    for (first_extra, second_extra, same_lane) in [
        ("", "", true),
        ("nonce_key: 7", "nonce_key: 7", true),
        ("nonce_key: 7", "nonce_key: 8", false),
        (
            "expiring_nonce: true\nvalid_for_secs: 25",
            "expiring_nonce: true\nvalid_for_secs: 25",
            false,
        ),
    ] {
        let mut spec: serde_yaml::Value = serde_yaml::from_str(&setup_scheduling_spec()).unwrap();
        let steps = spec["setup"]["steps"].as_sequence_mut().unwrap();
        steps.truncate(2);
        steps[1]["tx"]["from"]["select"]["index"] = 0.into();
        for (step, extra) in steps.iter_mut().zip([first_extra, second_extra]) {
            if !extra.is_empty() {
                let extra: serde_yaml::Value = serde_yaml::from_str(extra).unwrap();
                step["tx"].as_mapping_mut().unwrap().extend(extra.as_mapping().unwrap().clone());
            }
        }
        let txs = generated(&serde_yaml::to_string(&spec).unwrap());
        assert_eq!(txs[0]["sender"], txs[1]["sender"]);
        assert_eq!(txs[0]["submission_keys"] == txs[1]["submission_keys"], same_lane);
        assert!(txs.iter().all(|tx| tx["inclusion_keys"] == serde_json::json!([])));
    }
}

fn setup_scheduling_spec() -> String {
    format!(
        r#"
chain_id: 1337

accounts:
  users:
    mnemonic: "{TEST_MNEMONIC}"
    range: [0, 2]

setup:
  steps:
    - id: warmup_one
      tx:
        type: tempo
        from:
          pool: users
          select: {{ index: 0 }}
        gas_limit: 21000
        max_fee_per_gas: 1000000000
        max_priority_fee_per_gas: 1000000000
        to: "0x0000000000000000000000000000000000000000"
        value: 1
    - id: warmup_two
      tx:
        type: tempo
        from:
          pool: users
          select: {{ index: 1 }}
        gas_limit: 21000
        max_fee_per_gas: 1000000000
        max_priority_fee_per_gas: 1000000000
        to: "0x0000000000000000000000000000000000000000"
        value: 1
    - id: authorize_users
      keychain_authorize_pool:
        accounts:
          pool: users
        access_keys:
          mnemonic: "{TEST_MNEMONIC}"
          range: [100, 102]
        key_type: secp256k1
        gas_limit: 400000
        max_fee_per_gas: 1000000000
        max_priority_fee_per_gas: 1000000000

templates:
  noop:
    type: tempo
    from:
      pool: users
      select: {{ index: 0 }}
    gas_limit: 21000
    max_fee_per_gas: 1000000000
    max_priority_fee_per_gas: 1000000000
    to: "0x0000000000000000000000000000000000000000"
    value: 1

mix:
  - template: noop
    weight: 1
"#
    )
}

#[test]
fn explicit_dependencies_expand_to_all_transactions_in_a_step() {
    let mut spec: serde_yaml::Value = serde_yaml::from_str(&setup_scheduling_spec()).unwrap();
    let steps = spec["setup"]["steps"].as_sequence_mut().unwrap();
    let mut consumer = steps[0].clone();
    consumer["id"] = "consumer".into();
    consumer["depends_on"] = serde_yaml::to_value(["authorize_users"]).unwrap();
    steps.push(consumer);
    let txs = generated(&serde_yaml::to_string(&spec).unwrap());
    assert_eq!(
        txs[4]["depends_on"],
        serde_json::json!(["setup.authorize_users[0]", "setup.authorize_users[1]"])
    );
}

#[test]
fn setup_rejects_nonce_dependency_cycles_before_emitting_any_transaction() {
    let mut spec: serde_yaml::Value = serde_yaml::from_str(&setup_scheduling_spec()).unwrap();
    let steps = spec["setup"]["steps"].as_sequence_mut().unwrap();
    steps.truncate(2);
    steps[1]["tx"]["from"]["select"]["index"] = 0.into();
    steps[0]["depends_on"] = serde_yaml::to_value(["warmup_two"]).unwrap();
    let (output, txs) = generate(&serde_yaml::to_string(&spec).unwrap());
    assert!(!output.status.success());
    assert!(txs.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("setup dependency cycle"));
}

#[test]
fn setup_accepts_forward_dependencies_between_independent_lanes() {
    let mut spec: serde_yaml::Value = serde_yaml::from_str(&setup_scheduling_spec()).unwrap();
    let steps = spec["setup"]["steps"].as_sequence_mut().unwrap();
    steps.truncate(2);
    steps[0]["depends_on"] = serde_yaml::to_value(["warmup_two"]).unwrap();
    let txs = generated(&serde_yaml::to_string(&spec).unwrap());
    assert_eq!(txs[0]["depends_on"], serde_json::json!(["setup.warmup_two"]));
}
