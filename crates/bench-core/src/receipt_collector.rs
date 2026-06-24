//! Background transaction receipt collection.

use crate::metrics::MetricsCollector;
use alloy_network::{primitives::ReceiptResponse, AnyNetwork};
use alloy_primitives::B256;
use alloy_provider::{DynProvider, Provider};
use rand::seq::IndexedRandom;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, Semaphore},
    task::JoinSet,
};

const RECEIPT_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Handle for feeding transaction hashes into a background receipt collector.
pub struct ReceiptCollector {
    tx: mpsc::UnboundedSender<B256>,
    handle: tokio::task::JoinHandle<()>,
}

impl ReceiptCollector {
    /// Start a collector with bounded concurrent receipt polling.
    pub fn start(
        providers: Vec<DynProvider<AnyNetwork>>,
        metrics: Arc<MetricsCollector>,
        workers: usize,
        drain_timeout: Duration,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let workers = workers.max(1);
        let handle = tokio::spawn(run_collector(rx, providers, metrics, workers, drain_timeout));

        Self { tx, handle }
    }

    /// Sender used by transaction submission workers.
    pub fn sender(&self) -> mpsc::UnboundedSender<B256> {
        self.tx.clone()
    }

    /// Stop accepting new hashes and wait for pending receipt polling to drain.
    pub async fn finish(self) {
        drop(self.tx);
        if let Err(err) = self.handle.await {
            tracing::warn!(%err, "receipt collector task failed");
        }
    }
}

async fn run_collector(
    mut rx: mpsc::UnboundedReceiver<B256>,
    providers: Vec<DynProvider<AnyNetwork>>,
    metrics: Arc<MetricsCollector>,
    workers: usize,
    drain_timeout: Duration,
) {
    let semaphore = Arc::new(Semaphore::new(workers));
    let mut tasks = JoinSet::new();
    let mut accepted = 0u64;
    let mut completed = 0u64;

    while let Some(tx_hash) = rx.recv().await {
        accepted += 1;
        let Ok(permit) = semaphore.clone().acquire_owned().await else {
            break;
        };
        let providers = providers.clone();
        let metrics = metrics.clone();
        tasks.spawn(async move {
            let _permit = permit;
            poll_receipt(providers, metrics, tx_hash).await;
        });

        while let Some(result) = tasks.try_join_next() {
            completed += 1;
            if let Err(err) = result {
                tracing::warn!(%err, "receipt polling task failed");
            }
        }
    }

    let drain = async {
        let mut completed = completed;
        while let Some(result) = tasks.join_next().await {
            completed += 1;
            if let Err(err) = result {
                tracing::warn!(%err, "receipt polling task failed");
            }
        }
        completed
    };

    let completed = if drain_timeout.is_zero() {
        completed
    } else {
        match tokio::time::timeout(drain_timeout, drain).await {
            Ok(completed) => completed,
            Err(_) => completed,
        }
    };

    metrics.set_receipt_pending(accepted.saturating_sub(completed));
}

async fn poll_receipt(
    providers: Vec<DynProvider<AnyNetwork>>,
    metrics: Arc<MetricsCollector>,
    tx_hash: B256,
) {
    loop {
        let Some(provider) = providers.choose(&mut rand::rng()) else {
            metrics.record_receipt_error();
            return;
        };

        match provider.get_transaction_receipt(tx_hash).await {
            Ok(Some(receipt)) => {
                metrics.record_receipt(receipt.status());
                return;
            }
            Ok(None) => tokio::time::sleep(RECEIPT_POLL_INTERVAL).await,
            Err(err) => {
                metrics.record_receipt_error();
                tracing::warn!(%err, %tx_hash, "failed polling transaction receipt");
                tokio::time::sleep(RECEIPT_POLL_INTERVAL).await;
            }
        }
    }
}
