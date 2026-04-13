//! Background block head-poller for send mode.
//!
//! Polls the provider for new block numbers and records [`BlockMarker`]s
//! with monotonic timestamps from [`RunClock`].

use crate::clock::RunClock;
use crate::timeline::BlockMarker;
use alloy_eips::BlockNumberOrTag;
use alloy_provider::Provider;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, watch};

/// Handle returned by [`start_block_poller`]. Stops the poller on drop.
pub struct BlockPollerHandle {
    stop_tx: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<()>,
    markers: Arc<RwLock<Vec<BlockMarker>>>,
}

impl BlockPollerHandle {
    /// Stop the poller and wait for it to finish.
    pub async fn stop(self) {
        let _ = self.stop_tx.send(true);
        let _ = self.handle.await;
    }

    /// Drain all collected block markers.
    pub async fn drain(self) -> Vec<BlockMarker> {
        let _ = self.stop_tx.send(true);
        let _ = self.handle.await;
        std::mem::take(&mut *self.markers.write().await)
    }
}

/// Start a background block poller task.
///
/// Polls the provider for new blocks at the given interval and records
/// [`BlockMarker`]s. Returns a handle to stop the poller and drain markers.
pub fn start_block_poller<P: Provider + Clone + 'static>(
    provider: P,
    clock: RunClock,
    interval: Duration,
    start_block: u64,
) -> BlockPollerHandle {
    let (stop_tx, stop_rx) = watch::channel(false);
    let markers = Arc::new(RwLock::new(Vec::new()));

    let handle = tokio::spawn(poller_loop(
        provider,
        clock,
        interval,
        start_block,
        stop_rx,
        markers.clone(),
    ));

    BlockPollerHandle {
        stop_tx,
        handle,
        markers,
    }
}

async fn poller_loop<P: Provider>(
    provider: P,
    clock: RunClock,
    interval: Duration,
    start_block: u64,
    mut stop_rx: watch::Receiver<bool>,
    markers: Arc<RwLock<Vec<BlockMarker>>>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_seen = start_block;

    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            result = stop_rx.changed() => {
                if result.is_err() || *stop_rx.borrow() {
                    break;
                }
            }
        }

        match provider.get_block_number().await {
            Ok(current) => {
                // Record a marker for each new block since last poll.
                if current > last_seen {
                    let offset_ms = clock.offset_ms();
                    let mut new_markers = Vec::new();

                    for num in (last_seen + 1)..=current {
                        // Try to get the block timestamp from the chain.
                        let chain_timestamp = match provider
                            .get_block_by_number(BlockNumberOrTag::Number(num))
                            .await
                        {
                            Ok(Some(block)) => Some(block.header.timestamp),
                            _ => None,
                        };

                        new_markers.push(BlockMarker {
                            number: num,
                            chain_timestamp,
                            offset_ms,
                            new_payload_done_offset_ms: None,
                            fcu_done_offset_ms: None,
                        });
                    }

                    markers.write().await.extend(new_markers);
                    last_seen = current;
                }
            }
            Err(e) => {
                tracing::debug!(error = %e, "Failed to poll block number");
            }
        }
    }
}
