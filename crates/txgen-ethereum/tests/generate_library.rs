use std::path::PathBuf;
use txgen_cli::{generate_transactions, generate_transactions_into, GenerateArgs};
use txgen_core::{GeneratedTx, NdjsonWriter};
use txgen_ethereum::EthereumAdapter;

fn args(seed: u64) -> GenerateArgs {
    GenerateArgs {
        spec: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/simple.yaml"),
        count: Some(8),
        duration: None,
        output: None,
        rpc: None,
        seed: Some(seed),
        signing_workers: 2,
    }
}

#[tokio::test]
async fn library_generation_is_deterministic_and_materialized() {
    let first = generate_transactions(EthereumAdapter, args(7)).await.unwrap();
    let second = generate_transactions(EthereumAdapter, args(7)).await.unwrap();

    assert_eq!(first, second);
    assert_eq!(first.len(), 8);
    assert!(first.iter().all(|transaction| {
        !transaction.raw.is_empty() &&
            transaction.sender.is_some() &&
            !transaction.submission_keys.is_empty()
    }));
}

#[tokio::test]
async fn typed_and_ndjson_sinks_emit_identical_transactions() {
    let expected = generate_transactions(EthereumAdapter, args(9)).await.unwrap();
    let mut writer = NdjsonWriter::new(Vec::new());
    generate_transactions_into(EthereumAdapter, args(9), &mut writer).await.unwrap();

    let encoded = String::from_utf8(writer.into_inner()).unwrap();
    let decoded = encoded
        .lines()
        .map(|line| serde_json::from_str::<GeneratedTx>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(decoded, expected);
}
