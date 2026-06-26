use serde_json::Value;
use std::{
    fs,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

const TEST_MNEMONIC: &str = "test test test test test test test test test test test junk";

#[test]
fn keychain_authorize_pool_setup_uses_independent_inclusion_keys() {
    let test_dir = std::env::temp_dir().join(format!(
        "txgen-tempo-setup-scheduling-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    ));
    fs::create_dir_all(&test_dir).unwrap();

    let spec_path = test_dir.join("spec.yaml");
    let output_path = test_dir.join("generated.ndjson");
    fs::write(&spec_path, setup_scheduling_spec()).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_txgen-tempo"))
        .arg("generate")
        .arg("--spec")
        .arg(&spec_path)
        .arg("--count")
        .arg("0")
        .arg("--seed")
        .arg("1")
        .arg("--output")
        .arg(&output_path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "txgen-tempo generate failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let generated = fs::read_to_string(&output_path).unwrap();
    let txs = generated
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();

    assert_eq!(txs.len(), 4);
    assert_eq!(txs[0]["id"], "setup.warmup_one");
    assert_eq!(txs[1]["id"], "setup.warmup_two");
    assert_eq!(txs[2]["id"], "setup.authorize_users[0]");
    assert_eq!(txs[3]["id"], "setup.authorize_users[1]");
    assert!(txs.iter().all(|tx| tx["phase"] == "setup"));

    let warmup_one_key = inclusion_key(&txs[0]);
    let warmup_two_key = inclusion_key(&txs[1]);
    let keychain_zero_key = inclusion_key(&txs[2]);
    let keychain_one_key = inclusion_key(&txs[3]);

    assert_eq!(warmup_one_key, warmup_two_key);
    assert_ne!(keychain_zero_key, warmup_one_key);
    assert_ne!(keychain_one_key, warmup_one_key);
    assert_ne!(keychain_zero_key, keychain_one_key);

    let _ = fs::remove_dir_all(test_dir);
}

fn inclusion_key(tx: &Value) -> &str {
    tx["inclusion_keys"]
        .as_array()
        .and_then(|keys| keys.first())
        .and_then(Value::as_str)
        .expect("generated tx should have one inclusion key")
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
