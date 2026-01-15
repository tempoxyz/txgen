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
                let rate_limiter = self.rate_limiter.clone();

                let handle = tokio::spawn(async move {
                    key_worker(receiver, client, rpc_url, metrics, semaphore, rate_limiter).await;
                });
                self.worker_handles.push(handle);

                e.insert(sender)
            }
        };

        queue.send(pending).await.ok();

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

/// Worker that processes transactions for a single scheduling key.
async fn key_worker(
    mut receiver: mpsc::Receiver<PendingTx>,
    client: reqwest::Client,
    rpc_url: String,
    metrics: Arc<MetricsCollector>,
    semaphore: Arc<Semaphore>,
    rate_limiter: Option<Arc<RateLimiter>>,
) {
    let mut request_id = 0u64;

    while let Some(pending) = receiver.recv().await {
        // Apply rate limiting.
        if let Some(ref limiter) = rate_limiter {
            limiter.acquire().await;
        }

        // Acquire semaphore permit.
        let _permit = semaphore.acquire().await;

        // Send the transaction.
        request_id += 1;
        let start = Instant::now();

        let raw_hex = format!("0x{}", hex::encode(&pending.raw));
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
                            tracing::warn!(
                                code = err.code,
                                message = %err.message,
                                "RPC error"
                            );
                            metrics.record_failure().await;
                        } else {
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
                tracing::warn!(error = %e, "HTTP request failed");
                metrics.record_failure().await;
            }
        }

        // Track queue latency (time from queued to sent).
        let _queue_latency = pending.queued_at.elapsed();
    }
}

/// Simple token bucket rate limiter.
struct RateLimiter {
    interval: Duration,
    last_token: tokio::sync::Mutex<Instant>,
}

impl RateLimiter {
    fn new(tokens_per_sec: u64) -> Self {
        Self {
            interval: Duration::from_secs_f64(1.0 / tokens_per_sec as f64),
            last_token: tokio::sync::Mutex::new(Instant::now()),
        }
    }

    async fn acquire(&self) {
        let mut last = self.last_token.lock().await;
        let now = Instant::now();
        let elapsed = now.duration_since(*last);

        if elapsed < self.interval {
            tokio::time::sleep(self.interval - elapsed).await;
        }

        *last = Instant::now();
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
