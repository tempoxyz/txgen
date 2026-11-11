//! Fund a workload spec's accounts through the Tempo faucet (`tempo_fundAddress`) and verify
//! that every account holds the funded tokens before returning.
//!
//! The faucet answers each request as soon as its mint transaction enters the pool, so an
//! answered request does not mean a funded account: a mint can still be dropped (for example when
//! its expiring nonce lapses in a congested pool) and the pool then reports empty. This command
//! therefore waits for the pool to drain, reads every account's token balances, and requests
//! funding again for the accounts still short, for a bounded number of rounds.

use alloy_primitives::{address, Address, Bytes, U256};
use alloy_provider::{Provider, RootProvider};
use clap::Args;
use eyre::{bail, Result, WrapErr};
use futures::{stream, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    path::PathBuf,
    time::{Duration, Instant},
};
use txgen_core::{AccountManager, WorkloadSpec};

/// pathUSD, which the Tempo faucet mints.
const DEFAULT_TOKEN: Address = address!("0x20c0000000000000000000000000000000000000");
/// `balanceOf(address)` selector.
const BALANCE_OF_SELECTOR: [u8; 4] = [0x70, 0xa0, 0x82, 0x31];
/// Consecutive empty txpool readings that count as drained.
const DRAIN_EMPTY_READINGS: u32 = 3;
const POLL_INTERVAL: Duration = Duration::from_secs(1);
const MAX_REQUEST_BACKOFF: Duration = Duration::from_secs(5);
/// Default smallest balance that counts as funded: 1,000 pathUSD (6 decimals). Far below a faucet
/// mint (1e12 in the bench node) and above the dust state-bloat fixtures give existing accounts
/// (1 pathUSD), so dust is not mistaken for funding.
const DEFAULT_MIN_BALANCE: u64 = 1_000_000_000;
/// Unfunded addresses listed in the final error.
const REPORTED_ADDRESSES: usize = 10;

/// Fund a workload spec's accounts with the Tempo faucet and verify their balances.
#[derive(Args)]
pub struct FundArgs {
    /// Workload spec file (YAML)
    #[arg(short, long)]
    pub spec: PathBuf,

    /// RPC endpoint with the faucet (`tempo_fundAddress`) enabled
    #[arg(long)]
    pub rpc: String,

    /// Token whose balance shows an account is funded (repeatable)
    #[arg(long = "token", default_values_t = [DEFAULT_TOKEN])]
    pub tokens: Vec<Address>,

    /// Smallest balance of every --token that counts as funded
    #[arg(long, default_value_t = U256::from(DEFAULT_MIN_BALANCE))]
    pub min_balance: U256,

    /// Funding rounds: each funds the accounts still short, waits for the txpool to drain and
    /// reads their balances again
    #[arg(long, default_value_t = 5)]
    pub rounds: u32,

    /// Concurrent faucet requests
    #[arg(long, default_value_t = 16)]
    pub concurrency: usize,

    /// Concurrent balance reads
    #[arg(long, default_value_t = 64)]
    pub check_concurrency: usize,

    /// Attempts per faucet request before leaving the account to the next round
    #[arg(long, default_value_t = 5)]
    pub request_attempts: u32,

    /// Seconds to wait each round for the txpool to drain
    #[arg(long, default_value_t = 120)]
    pub drain_timeout_secs: u64,
}

/// Fund every signer account of the spec, then verify balances and refund the accounts still
/// short until all are funded or the rounds run out.
pub async fn run_fund(args: FundArgs) -> Result<()> {
    if args.rounds == 0 || args.concurrency == 0 || args.check_concurrency == 0 {
        bail!("--rounds, --concurrency and --check-concurrency must be at least 1");
    }
    if args.request_attempts == 0 {
        bail!("--request-attempts must be at least 1");
    }
    if args.tokens.is_empty() {
        bail!("at least one --token is required");
    }
    let spec = WorkloadSpec::load(&args.spec)
        .wrap_err_with(|| format!("failed to load spec: {}", args.spec.display()))?;
    let accounts = AccountManager::from_spec(&spec.accounts)?;
    // Pools may share accounts; fund each address once.
    let addresses: Vec<Address> = accounts
        .all_addresses()
        .flat_map(|(_, addrs)| addrs)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if addresses.is_empty() {
        bail!("spec has no accounts to fund: {}", args.spec.display());
    }
    let url = args.rpc.parse().wrap_err_with(|| format!("invalid --rpc URL: {}", args.rpc))?;
    let provider = RootProvider::new_http(url);

    let total = addresses.len();
    let mut short = unfunded(&provider, &args, addresses).await?;
    eprintln!("fund: {} of {total} account(s) need funding", short.len());
    for round in 1..=args.rounds {
        if short.is_empty() {
            break;
        }
        let requested = short.len();
        let failed = request_funding(&provider, &args, &short).await;
        eprintln!(
            "fund: round {round}/{}: requested {requested} account(s), {failed} request(s) failed",
            args.rounds
        );
        wait_for_settlement(&provider, Duration::from_secs(args.drain_timeout_secs)).await?;
        short = unfunded(&provider, &args, short).await?;
        eprintln!("fund: round {round}/{}: {} account(s) still short", args.rounds, short.len());
    }
    if !short.is_empty() {
        let listed: Vec<String> =
            short.iter().take(REPORTED_ADDRESSES).map(ToString::to_string).collect();
        bail!(
            "{} of {total} account(s) still hold less than {} of a checked token after {} funding round(s): {}{}",
            short.len(),
            args.min_balance,
            args.rounds,
            listed.join(", "),
            if short.len() > REPORTED_ADDRESSES { ", ..." } else { "" }
        );
    }
    eprintln!("fund: all {total} account(s) funded");
    Ok(())
}

/// Addresses whose balance of any checked token is below the minimum, in input order.
async fn unfunded(
    provider: &RootProvider,
    args: &FundArgs,
    addresses: Vec<Address>,
) -> Result<Vec<Address>> {
    let results: Vec<Result<Option<Address>>> = stream::iter(addresses)
        .map(|address| async move {
            for token in &args.tokens {
                if balance_of(provider, *token, address).await? < args.min_balance {
                    return Ok(Some(address));
                }
            }
            Ok(None)
        })
        .buffered(args.check_concurrency)
        .collect()
        .await;
    results.into_iter().filter_map(Result::transpose).collect()
}

async fn balance_of(provider: &RootProvider, token: Address, account: Address) -> Result<U256> {
    let mut input = Vec::with_capacity(36);
    input.extend_from_slice(&BALANCE_OF_SELECTOR);
    input.extend_from_slice(account.into_word().as_slice());
    let call = json!({"to": token, "data": Bytes::from(input)});
    let output: Bytes = provider
        .raw_request("eth_call".into(), (call, "latest"))
        .await
        .wrap_err_with(|| format!("failed to read {token} balance of {account}"))?;
    if output.len() < 32 {
        bail!("{token} balanceOf({account}) returned {} byte(s), expected 32", output.len());
    }
    Ok(U256::from_be_slice(&output[..32]))
}

/// Request funding for every address, retrying failed requests with backoff. Returns how many
/// requests still failed; the balance check after the round decides what is funded.
async fn request_funding(provider: &RootProvider, args: &FundArgs, addresses: &[Address]) -> usize {
    stream::iter(addresses.iter().copied())
        .map(|address| async move {
            let mut backoff = Duration::from_millis(200);
            for attempt in 1..=args.request_attempts {
                let result: Result<Value, _> =
                    provider.raw_request("tempo_fundAddress".into(), (address,)).await;
                match result {
                    Ok(_) => return true,
                    Err(error) if attempt == args.request_attempts => {
                        eprintln!("fund: tempo_fundAddress({address}) failed: {error}");
                    }
                    Err(_) => {
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_REQUEST_BACKOFF);
                    }
                }
            }
            false
        })
        .buffer_unordered(args.concurrency)
        .filter(|funded| std::future::ready(!funded))
        .count()
        .await
}

#[derive(Debug, Deserialize)]
struct TxpoolStatus {
    pending: U256,
    queued: U256,
}

/// Wait until the txpool stays empty (pending and queued) for a few readings, then for one more
/// block so the last mints are in the state the balance check reads. A timeout is not an error:
/// the balance check that follows finds the mints that did not land.
async fn wait_for_settlement(provider: &RootProvider, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let mut empty = 0;
    while Instant::now() < deadline {
        let status: TxpoolStatus = provider
            .raw_request("txpool_status".into(), Vec::<Value>::new())
            .await
            .wrap_err("failed to read txpool_status")?;
        if status.pending.is_zero() && status.queued.is_zero() {
            empty += 1;
            if empty >= DRAIN_EMPTY_READINGS {
                break;
            }
        } else {
            empty = 0;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    if empty < DRAIN_EMPTY_READINGS {
        eprintln!("fund: txpool did not drain within {}s; checking balances", timeout.as_secs());
        return Ok(());
    }
    let block = provider.get_block_number().await.wrap_err("failed to read block number")?;
    while Instant::now() < deadline &&
        provider.get_block_number().await.wrap_err("failed to read block number")? <= block
    {
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    Ok(())
}
