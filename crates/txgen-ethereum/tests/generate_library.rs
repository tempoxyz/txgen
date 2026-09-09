use std::path::PathBuf;
use txgen_cli::{generate_transactions, GenerateArgs};
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
