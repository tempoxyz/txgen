use alloy_consensus::transaction::SignerRecoverable;
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::{
    address, aliases::U96, keccak256, Address, Bytes, Signature, TxKind, B256, U256,
};
use alloy_sol_types::{eip712_domain, sol, SolCall, SolStruct, SolValue};
use serde_json::{json, Value};
use std::{
    fs,
    process::Command,
    sync::atomic::{AtomicUsize, Ordering},
};
use tempo_primitives::TempoTxEnvelope;
use txgen_core::derive_mnemonic_signer;

const MNEMONIC: &str = "test test test test test test test test test test test junk";
const RESERVE: Address = address!("4d50500000000000000000000000000000000000");
const TOKEN: Address = address!("20c0000000000000000000000000000000000001");

sol! {
    struct ChannelDescriptor {
        address payer;
        address payee;
        address operator;
        address token;
        bytes32 salt;
        address authorizedSigner;
        bytes32 expiringNonceHash;
    }
    struct Voucher { bytes32 channelId; uint96 cumulativeAmount; }
    function open(address payee, address operator, address token, uint96 deposit,
                  bytes32 salt, address authorizedSigner) returns (bytes32 channelId);
    function settle(ChannelDescriptor descriptor, uint96 cumulativeAmount, bytes signature);
}

fn account(index: u32) -> Address {
    static ACCOUNTS: std::sync::OnceLock<[Address; 3]> = std::sync::OnceLock::new();
    ACCOUNTS.get_or_init(|| {
        std::array::from_fn(|i| derive_mnemonic_signer(MNEMONIC, i as u32).unwrap().address())
    })[index as usize]
}

fn spec(authorized_signer: bool) -> Value {
    let input = openCall {
        payee: account(1),
        operator: Address::ZERO,
        token: TOKEN,
        deposit: U96::from(10),
        salt: B256::repeat_byte(1),
        authorizedSigner: if authorized_signer { account(2) } else { Address::ZERO },
    }
    .abi_encode();
    json!({
        "chain_id": 1337,
        "gas": {"max_fee_per_gas": 1000000000, "max_priority_fee_per_gas": 1000000000},
        "accounts": {"users": {"mnemonic": MNEMONIC, "range": [0, 3]}},
        "templates": {
            "open": {
                "type": "tempo", "from": {"pool": "users", "select": {"index": 0}},
                "gas_limit": 1000000, "expiring_nonce": true, "valid_before": 2000000000,
                "to": RESERVE, "input": Bytes::from(input)
            },
            "settle": {
                "type": "tempo", "from": {"pool": "users", "select": {"index": 1}},
                "gas_limit": 1000000, "expiring_nonce": true, "valid_before": 2000000000,
                "mpp_settle": {
                    "open_transaction": {"var": "opened.raw"},
                    "voucher_signer": {"pool": "users", "select": {"index": if authorized_signer {2} else {0}}},
                    "cumulative_amount": 10
                }
            }
        },
        "sequences": {"channel": {"steps": [
            {"template": "open", "save": "opened"}, {"template": "settle"}
        ]}},
        "mix": [{"sequence": "channel", "weight": 1}]
    })
}

fn generate(spec: &Value, count: usize, workers: usize) -> Result<Vec<Value>, String> {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "txgen-mpp-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("spec.yaml");
    let output_path = dir.join("txs.ndjson");
    fs::write(&path, serde_yaml::to_string(spec).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_txgen-tempo"))
        .args([
            "generate",
            "--seed",
            "42",
            "--count",
            &count.to_string(),
            "--signing-workers",
            &workers.to_string(),
        ])
        .arg("--spec")
        .arg(path)
        .arg("--output")
        .arg(&output_path)
        .output()
        .unwrap();
    let result = if output.status.success() {
        Ok(fs::read_to_string(output_path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).into_owned())
    };
    fs::remove_dir_all(dir).unwrap();
    result
}

fn decode(row: &Value) -> TempoTxEnvelope {
    let raw: Bytes = serde_json::from_value(row["raw"].clone()).unwrap();
    TempoTxEnvelope::decode_2718(&mut raw.as_ref()).unwrap()
}

#[test]
fn saved_opens_produce_ordered_deterministic_settlements() {
    // Exceeds the single worker's bounded signing queue. An odd budget must not
    // emit an opening transaction without its settlement.
    let spec = spec(false);
    let rows = generate(&spec, 257, 1).unwrap();
    assert_eq!(rows, generate(&spec, 257, 4).unwrap());
    assert_eq!(rows.len(), 256);
    let mut channels = std::collections::HashSet::new();
    for pair in rows.as_chunks::<2>().0 {
        assert_eq!(pair[0]["inclusion_keys"], pair[1]["inclusion_keys"]);
        assert!(!pair[0]["inclusion_keys"].as_array().unwrap().is_empty());
        let channel = assert_settlement(&pair[0], &pair[1], account(0));
        assert!(channels.insert(channel));
    }
}

#[test]
fn authorized_voucher_signer_is_used_instead_of_payer() {
    let rows = generate(&spec(true), 2, 2).unwrap();
    assert_settlement(&rows[0], &rows[1], account(2));
}

#[test]
fn sponsored_open_uses_its_actual_signed_context() {
    let mut spec = spec(false);
    spec["templates"]["open"]["sponsor"] = json!({"pool": "users", "select": {"index": 2}});
    let rows = generate(&spec, 2, 2).unwrap();
    assert_settlement(&rows[0], &rows[1], account(0));
}

#[test]
fn wrong_chain_and_malformed_open_are_rejected() {
    let original = generate(&spec(false), 2, 1).unwrap();
    let raw = original[0]["raw"].as_str().unwrap();
    let mut wrong_chain = spec(false);
    wrong_chain["chain_id"] = json!(1);
    wrong_chain["templates"]["settle"]["mpp_settle"]["open_transaction"] = json!(raw);
    assert!(generate(&wrong_chain, 2, 1).unwrap_err().contains("belongs to another chain"));

    let mut trailing = spec(false);
    trailing["templates"]["settle"]["mpp_settle"]["open_transaction"] = json!(format!("{raw}00"));
    assert!(generate(&trailing, 2, 1).unwrap_err().contains("trailing bytes"));

    let mut wrong_target = spec(false);
    wrong_target["templates"]["open"]["to"] = json!(Address::ZERO);
    assert!(generate(&wrong_target, 2, 1)
        .unwrap_err()
        .contains("must target the native Channel Reserve"));

    let mut wrong_call = spec(false);
    wrong_call["templates"]["open"]["input"] = json!("0xdeadbeef");
    assert!(generate(&wrong_call, 2, 1).unwrap_err().contains("not a valid native open"));

    let mut duplicate = spec(false);
    duplicate["sequences"]["channel"]["steps"][1]["save"] = json!("opened");
    assert!(generate(&duplicate, 2, 1).unwrap_err().contains("invalid or duplicate save"));
}

fn assert_settlement(open: &Value, settled: &Value, signer: Address) -> B256 {
    let opening = decode(open);
    let opening = opening.as_aa().unwrap();
    let settlement = decode(settled);
    let settlement = settlement.as_aa().unwrap();
    assert_eq!(settlement.recover_signer().unwrap(), account(1));
    let call = &settlement.tx().calls[0];
    assert_eq!(call.to, TxKind::Call(RESERVE));
    let decoded = settleCall::abi_decode_validate(&call.input).unwrap();
    let d = &decoded.descriptor;
    assert_eq!(d.payer, account(0));
    assert_eq!(d.payee, account(1));
    assert_eq!(d.operator, Address::ZERO);
    assert_eq!(d.token, TOKEN);
    assert_eq!(d.salt, B256::repeat_byte(1));
    assert_eq!(d.expiringNonceHash, opening.expiring_nonce_hash(d.payer));
    assert_ne!(d.expiringNonceHash, B256::ZERO);
    let id = keccak256(
        (
            d.payer,
            d.payee,
            d.operator,
            d.token,
            d.salt,
            d.authorizedSigner,
            d.expiringNonceHash,
            RESERVE,
            U256::from(1337),
        )
            .abi_encode(),
    );
    assert_eq!(decoded.cumulativeAmount, U96::from(10));
    // Use Alloy's typed EIP-712 implementation independently of the generator's
    // manual domain/struct encoding.
    let digest = Voucher { channelId: id, cumulativeAmount: decoded.cumulativeAmount }
        .eip712_signing_hash(&eip712_domain! {
            name: "TIP20 Channel Reserve", version: "1", chain_id: 1337,
            verifying_contract: RESERVE,
        });
    assert_eq!(
        Signature::try_from(decoded.signature.as_ref())
            .unwrap()
            .recover_address_from_prehash(&digest)
            .unwrap(),
        signer
    );
    id
}

#[test]
fn invalid_settlements_and_save_names_are_rejected() {
    for (pointer, value, expected) in [
        (
            "/templates/settle/mpp_settle/voucher_signer/select/index",
            json!(2),
            "voucher_signer does not match",
        ),
        (
            "/templates/settle/from/select/index",
            json!(2),
            "sender must be the channel payee or operator",
        ),
        (
            "/templates/settle/mpp_settle/cumulative_amount",
            json!(0),
            "cumulative_amount must be positive",
        ),
        (
            "/templates/settle/mpp_settle/cumulative_amount",
            json!(11),
            "no greater than the opening deposit",
        ),
        ("/templates/settle/mpp_settle/call_index", json!(1), "opening call index out of bounds"),
        ("/sequences/channel/steps/0/save", json!("bad.name"), "invalid or duplicate save"),
        ("/sequences/channel/steps/0/save", json!(""), "invalid or duplicate save"),
    ] {
        let mut spec = spec(false);
        // call_index is optional in the successful fixture.
        if pointer.ends_with("/call_index") {
            spec["templates"]["settle"]["mpp_settle"]["call_index"] = value;
        } else {
            *spec.pointer_mut(pointer).unwrap() = value;
        }
        let error = generate(&spec, 2, 1).unwrap_err();
        assert!(error.contains(expected), "expected {expected}, got {error}");
    }
}
