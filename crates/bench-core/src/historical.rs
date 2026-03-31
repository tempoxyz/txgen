//! Historical transaction source.
//!
//! Fetches blocks and receipts from an archive RPC node, extracts candidate
//! transactions with per-tx gas usage, and streams them via an async channel.
//!
//! Blob transactions (type 3) are skipped since they cannot be replayed without
//! sidecars.

use alloy_primitives::{B256, Bytes};
use alloy_provider::{Provider, ext::DebugApi};
use eyre::{Context, Result};
use tokio::sync::mpsc;

/// Number of blocks to prefetch ahead of consumption.
const DEFAULT_PREFETCH_SIZE: usize = 20;

/// Default number of retries for transient RPC errors.
const DEFAULT_RPC_RETRIES: u32 = 3;

/// Base delay for exponential backoff on RPC retries.
const RETRY_BASE_DELAY: std::time::Duration = std::time::Duration::from_millis(500);

/// EIP-2718 transaction type for blob transactions.
const BLOB_TX_TYPE: u8 = 3;

/// A candidate transaction extracted from a historical block.
#[derive(Debug, Clone)]
pub struct HistoricalTx {
    /// Block number this transaction was included in.
    pub block_number: u64,
    /// Raw signed transaction bytes (EIP-2718 encoded).
    pub raw: Bytes,
    /// EIP-2718 transaction type.
    pub tx_type: u8,
    /// Gas used by this transaction (from receipt cumulative_gas_used deltas).
    pub gas_used: u64,
}

/// Configuration for the historical transaction fetcher.
#[derive(Debug, Clone)]
pub struct HistoricalFetcherConfig {
    /// First block to fetch (inclusive).
    pub from: u64,
    /// Last block to fetch (inclusive).
    pub to: u64,
    /// Number of blocks to prefetch ahead.
    pub prefetch_size: usize,
    /// Number of retries for transient RPC errors.
    pub rpc_retries: u32,
}

impl HistoricalFetcherConfig {
    /// Create a new config with defaults for prefetch size and retries.
    pub fn new(from: u64, to: u64) -> Self {
        Self {
            from,
            to,
            prefetch_size: DEFAULT_PREFETCH_SIZE,
            rpc_retries: DEFAULT_RPC_RETRIES,
        }
    }

    /// Set the number of blocks to prefetch ahead.
    pub fn with_prefetch_size(mut self, size: usize) -> Self {
        self.prefetch_size = size;
        self
    }

    /// Set the number of retries for transient RPC errors.
    pub fn with_rpc_retries(mut self, retries: u32) -> Self {
        self.rpc_retries = retries;
        self
    }
}

/// Historical transaction fetcher.
///
/// Fetches blocks and receipts from an archive RPC node using buffered async
/// prefetch, extracts candidate transactions, and streams them to a consumer.
pub struct HistoricalFetcher<P> {
    provider: P,
    config: HistoricalFetcherConfig,
}

impl<P> HistoricalFetcher<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    /// Create a new historical fetcher.
    pub fn new(provider: P, config: HistoricalFetcherConfig) -> Self {
        Self { provider, config }
    }

    /// Start fetching and return a receiver for extracted transactions.
    ///
    /// Spawns a background task that fetches blocks + receipts concurrently
    /// and sends extracted transactions through the channel.
    pub fn start(self) -> mpsc::Receiver<Result<Vec<HistoricalTx>>> {
        let (tx, rx) = mpsc::channel(self.config.prefetch_size.max(1));

        tokio::spawn(async move {
            fetch_and_extract(
                self.provider,
                self.config.from,
                self.config.to,
                self.config.rpc_retries,
                tx,
            )
            .await;
        });

        rx
    }
}

/// Fetch blocks + receipts and extract transactions, sending per-block batches.
async fn fetch_and_extract<P: Provider>(
    provider: P,
    from: u64,
    to: u64,
    rpc_retries: u32,
    tx: mpsc::Sender<Result<Vec<HistoricalTx>>>,
) {
    for block_num in from..=to {
        let result = fetch_block_txs(&provider, block_num, rpc_retries).await;

        let is_err = result.is_err();
        if tx.send(result).await.is_err() {
            tracing::debug!("channel closed, stopping historical fetcher");
            break;
        }
        if is_err {
            break;
        }
    }
}

/// Fetch a single block and its receipts, extract candidate transactions.
///
/// Uses `debug_getRawTransaction` to fetch raw EIP-2718 bytes directly,
/// avoiding decode/re-encode roundtrips.
async fn fetch_block_txs<P: Provider>(
    provider: &P,
    block_num: u64,
    max_retries: u32,
) -> Result<Vec<HistoricalTx>> {
    let (block, receipts) = tokio::try_join!(
        fetch_with_retry(
            || async {
                provider
                    .get_block_by_number(block_num.into())
                    .hashes()
                    .await
                    .wrap_err_with(|| format!("failed to fetch block {block_num}"))?
                    .ok_or_else(|| eyre::eyre!("block {block_num} not found"))
            },
            max_retries,
        ),
        fetch_with_retry(
            || async {
                provider
                    .get_block_receipts(block_num.into())
                    .await
                    .wrap_err_with(|| format!("failed to fetch receipts for block {block_num}"))?
                    .ok_or_else(|| eyre::eyre!("receipts for block {block_num} not found"))
            },
            max_retries,
        ),
    )?;

    let tx_hashes: Vec<B256> = block.transactions.hashes().collect();
    let block_number = block.header.number;

    eyre::ensure!(
        tx_hashes.len() == receipts.len(),
        "block {block_number} has {} transactions but {} receipts",
        tx_hashes.len(),
        receipts.len()
    );

    let mut result = Vec::new();
    let mut prev_cumulative_gas = 0u64;

    for (hash, receipt) in tx_hashes.iter().zip(receipts.iter()) {
        let tx_type = receipt.transaction_type() as u8;

        // Skip blob transactions (type 3)
        if tx_type == BLOB_TX_TYPE {
            prev_cumulative_gas = receipt.inner.cumulative_gas_used();
            continue;
        }

        let gas_used = receipt
            .inner
            .cumulative_gas_used()
            .saturating_sub(prev_cumulative_gas);
        prev_cumulative_gas = receipt.inner.cumulative_gas_used();

        let raw = fetch_with_retry(
            || async {
                provider
                    .debug_get_raw_transaction(*hash)
                    .await
                    .map_err(|e| eyre::eyre!(e))
                    .wrap_err_with(|| format!("failed to fetch raw tx {hash}"))
            },
            max_retries,
        )
        .await?;

        result.push(HistoricalTx {
            block_number,
            raw,
            tx_type,
            gas_used,
        });
    }

    Ok(result)
}

/// Retry a fallible async operation with exponential backoff.
async fn fetch_with_retry<F, Fut, T>(mut f: F, max_retries: u32) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut attempt = 0;
    loop {
        match f().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                if attempt >= max_retries {
                    return Err(e);
                }
                let delay = RETRY_BASE_DELAY * 2u32.saturating_pow(attempt);
                tracing::warn!(
                    attempt = attempt + 1,
                    max_retries,
                    delay_ms = delay.as_millis() as u64,
                    error = %e,
                    "RPC request failed, retrying"
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = HistoricalFetcherConfig::new(100, 200);
        assert_eq!(config.from, 100);
        assert_eq!(config.to, 200);
        assert_eq!(config.prefetch_size, DEFAULT_PREFETCH_SIZE);
        assert_eq!(config.rpc_retries, DEFAULT_RPC_RETRIES);
    }

    #[test]
    fn test_config_builder() {
        let config = HistoricalFetcherConfig::new(100, 200)
            .with_prefetch_size(50)
            .with_rpc_retries(5);
        assert_eq!(config.prefetch_size, 50);
        assert_eq!(config.rpc_retries, 5);
    }
}
