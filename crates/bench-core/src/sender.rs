//! Transaction sender with scheduling key ordering and rate limiting.
//!
//! Handles sending transactions to an RPC endpoint while:
//! - Respecting scheduling key ordering (same key = sequential, different key = parallel)
//! - Applying rate limiting

use crate::metrics::MetricsCollector;
use alloy_primitives::Bytes;
use eyre::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use txgen_core::GeneratedTx;

/// Configuration for the sender.
#[derive(Debug, Clone)]
pub struct SenderConfig {
    /// RPC endpoint URL.
    pub rpc_url: String,
    /// Maximum transactions per second (0 = unlimited).
    pub rate_limit: u64,
    /// Maximum concurrent requests.
    pub max_concurrent: usize,
    /// Request timeout.
    pub timeout: Duration,
}

impl Default for SenderConfig {
    fn default() -> Self {
        Self {
            rpc_url: "http://localhost:8545".to_string(),
            rate_limit: 0,
            max_concurrent: 100,
            timeout: Duration::from_secs(30),
        }
    }
}

/// A transaction to be sent.
struct PendingTx {
    raw: Bytes,
    key: [u8; 20],
    queued_at: Instant,
}

/// JSON-RPC request for eth_sendRawTransaction.
#[derive(serde::Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'a str,
    method: &'a str,
    params: [&'a str; 1],
    id: u64,
}

/// JSON-RPC response.
#[derive(serde::Deserialize)]
struct RpcResponse {
    #[allow(dead_code)]
    result: Option<serde_json::Value>,
    error: Option<RpcError>,
}

/// JSON-RPC error.
#[derive(serde::Deserialize)]
struct RpcError {
    #[allow(dead_code)]
    code: i64,
    #[allow(dead_code)]
    message: String,
}

/// Transaction sender.
pub struct Sender {
    config: SenderConfig,
    client: reqwest::Client,
    metrics: Arc<MetricsCollector>,
    semaphore: Arc<Semaphore>,
    /// Per-key queues to ensure ordering.
    key_queues: HashMap<[u8; 20], mpsc::Sender<PendingTx>>,
    /// Worker task handles for awaiting completion.
    worker_handles: Vec<JoinHandle<()>>,
    /// Rate limiter tokens.
    rate_limiter: Option<Arc<RateLimiter>>,
}

impl Sender {
    /// Create a new sender.
    pub fn new(config: SenderConfig, metrics: Arc<MetricsCollector>) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .context("failed to create HTTP client")?;

        let semaphore = Arc::new(Semaphore::new(config.max_concurrent));

        let rate_limiter = if config.rate_limit > 0 {
            Some(Arc::new(RateLimiter::new(config.rate_limit)))
        } else {
            None
        };

        Ok(Self {
            config,
            client,
            metrics,
            semaphore,
            key_queues: HashMap::new(),
            worker_handles: Vec::new(),
            rate_limiter,
        })
    }

    /// Send a transaction.
    ///
    /// This respects scheduling key ordering: transactions with the same key
    /// are sent sequentially, while transactions with different keys can be
    /// sent in parallel.
    pub async fn send(&mut self, tx: GeneratedTx) -> Result<()> {
        // Apply rate limiting before enqueueing to provide backpressure
        // to the source reader. This makes the rate limit global rather
        // than per-key.
        if let Some(ref limiter) = self.rate_limiter {
            limiter.acquire().await;
        }

        let pending = PendingTx {
            raw: tx.raw,
            key: tx.key,
            queued_at: Instant::now(),
        };

        self.metrics.record_sent().await;

        // Get or create the queue for this key.
        let queue = match self.key_queues.entry(pending.key) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                let (sender, receiver) = mpsc::channel(1024);

                // Spawn a worker for this key.
                let client = self.client.clone();
                let rpc_url = self.config.rpc_url.clone();
                let metrics = self.metrics.clone();
                let semaphore = self.semaphore.clone();

                let handle = tokio::spawn(async move {
                    key_worker(receiver, client, rpc_url, metrics, semaphore).await;
                });
                self.worker_handles.push(handle);

                e.insert(sender)
            }
        };

        if queue.send(pending).await.is_err() {
            tracing::warn!("Failed to enqueue transaction, worker channel closed");
            self.metrics.record_failure().await;
        }

        Ok(())
    }

    /// Wait for all pending transactions to complete.
    pub async fn flush(&mut self) {
        // Drop all senders to signal workers to stop.
        self.key_queues.clear();

        // Wait for all workers to finish processing.
        for handle in self.worker_handles.drain(..) {
            let _ = handle.await;
        }
    }
}

/// Maximum number of retries per transaction.
const MAX_RETRIES: u32 = 10;

/// Retry backoff delays (microseconds): 100µs, 500µs, 1ms, 5ms, 10ms, ...
const RETRY_BACKOFFS_US: [u64; 6] = [100, 500, 1_000, 5_000, 10_000, 50_000];

/// Worker that processes transactions for a single scheduling key.
async fn key_worker(
    mut receiver: mpsc::Receiver<PendingTx>,
    client: reqwest::Client,
    rpc_url: String,
    metrics: Arc<MetricsCollector>,
    semaphore: Arc<Semaphore>,
) {
    let mut request_id = 0u64;

    while let Some(pending) = receiver.recv().await {
        // Acquire semaphore permit.
        let _permit = semaphore.acquire().await;

        request_id += 1;
        let raw_hex = format!("0x{}", hex::encode(&pending.raw));

        let mut attempt = 0u32;
        loop {
            let start = Instant::now();
            let request = RpcRequest {
                jsonrpc: "2.0",
                method: "eth_sendRawTransaction",
                params: [&raw_hex],
                id: request_id,
            };

            let result = client
                .post(&rpc_url)
                .json(&request)
                .send()
                .await
                .and_then(|r| r.error_for_status());

            match result {
                Ok(response) => {
                    let latency = start.elapsed();
                    match response.json::<RpcResponse>().await {
                        Ok(rpc_response) => {
                            if let Some(ref err) = rpc_response.error {
                                // Retry on transient RPC errors (txpool full, etc.)
                                attempt += 1;
                                if attempt <= MAX_RETRIES {
                                    let backoff_idx =
                                        (attempt as usize - 1).min(RETRY_BACKOFFS_US.len() - 1);
                                    let backoff =
                                        Duration::from_micros(RETRY_BACKOFFS_US[backoff_idx]);
                                    tracing::debug!(
                                        attempt,
                                        code = err.code,
                                        message = %err.message,
                                        backoff_us = backoff.as_micros() as u64,
                                        "Retrying RPC error"
                                    );
                                    tokio::time::sleep(backoff).await;
                                    continue;
                                }
                                tracing::warn!(
                                    code = err.code,
                                    message = %err.message,
                                    attempts = attempt,
                                    "RPC error (exhausted retries)"
                                );
                                metrics.record_failure().await;
                            } else {
                                if attempt > 0 {
                                    tracing::debug!(attempts = attempt + 1, "Succeeded after retry");
                                }
                                metrics.record_success(latency).await;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "Failed to parse RPC response");
                            metrics.record_failure().await;
                        }
                    }
                }
                Err(e) => {
                    // Retry on HTTP errors (429, connection reset, etc.)
                    attempt += 1;
                    if attempt <= MAX_RETRIES {
                        let backoff_idx =
                            (attempt as usize - 1).min(RETRY_BACKOFFS_US.len() - 1);
                        let backoff = Duration::from_micros(RETRY_BACKOFFS_US[backoff_idx]);
                        tracing::debug!(
                            attempt,
                            error = %e,
                            backoff_us = backoff.as_micros() as u64,
                            "Retrying HTTP error"
                        );
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    tracing::warn!(error = %e, attempts = attempt, "HTTP request failed (exhausted retries)");
                    metrics.record_failure().await;
                }
            }
            break;
        }
    }
}

/// Token bucket rate limiter using scheduled times.
///
/// Tracks the *scheduled* next-token time rather than the last-wake time.
/// This eliminates throughput loss from sleep overshoot: if a sleep
/// overshoots by 500µs, subsequent tokens are issued immediately until
/// the schedule catches up.
struct RateLimiter {
    interval: Duration,
    next_token: tokio::sync::Mutex<Instant>,
}

impl RateLimiter {
    fn new(tokens_per_sec: u64) -> Self {
        Self {
            interval: Duration::from_secs_f64(1.0 / tokens_per_sec as f64),
            next_token: tokio::sync::Mutex::new(Instant::now()),
        }
    }

    async fn acquire(&self) {
        let mut next = self.next_token.lock().await;
        let now = Instant::now();

        if *next > now {
            tokio::time::sleep(*next - now).await;
        }

        // Advance from the scheduled time, not wall-clock, so we can
        // burst to catch up after sleep overshoot.
        *next = (*next).max(now) + self.interval;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sender_config_default() {
        let config = SenderConfig::default();
        assert_eq!(config.rpc_url, "http://localhost:8545");
        assert_eq!(config.rate_limit, 0);
        assert_eq!(config.max_concurrent, 100);
    }
}
