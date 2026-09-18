//! Shared block observation for pending limits and receipt dependencies.
//!
//! Register before dispatching a transaction. Registration waits for a shared
//! starting head, so even inclusion before the submission response is covered.
//! HTTP endpoints use one head poller; block RPC traffic scales with blocks,
//! never with the number of pending transactions. This observes inclusion only,
//! not finality or additional confirmations.

use alloy_consensus::BlockHeader;
use alloy_eips::BlockId;
use alloy_network::{primitives::BlockResponse, AnyNetwork, AnyTransactionReceipt};
use alloy_primitives::TxHash;
use alloy_provider::{DynProvider, Provider};
use eyre::{eyre, Result};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{oneshot, watch};

const POLL_INTERVAL: Duration = Duration::from_millis(100);
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(300);
const INCLUSION_TIMEOUT: Duration = Duration::from_secs(3000);

type Receipt = Arc<AnyTransactionReceipt>;
type ReceiptResult = Result<Inclusion, String>;

#[derive(Debug)]
pub(crate) enum Inclusion {
    Included(Option<Receipt>),
    Expired,
}

struct Interest {
    sender: oneshot::Sender<ReceiptResult>,
    receipt: bool,
    expires_at: Option<u64>,
}

/// Cloneable, lazy receipt service for one chain's aggregate query provider.
#[derive(Clone)]
pub struct ReceiptTracker(Arc<Inner>);

struct Inner {
    provider: DynProvider<AnyNetwork>,
    state: Mutex<State>,
}

struct State {
    running: bool,
    preparing: usize,
    next_id: u64,
    ready: watch::Sender<Option<Result<(), String>>>,
    pending: HashMap<TxHash, HashMap<u64, Interest>>,
}

impl ReceiptTracker {
    /// Use an endpoint authorized for `eth_blockNumber`, `eth_getBlockByNumber`,
    /// and (for receipt dependencies) `eth_getBlockReceipts`.
    /// Sender-specific submission credentials are not used for aggregate queries.
    pub fn new(provider: DynProvider<AnyNetwork>) -> Self {
        let state = State {
            running: false,
            preparing: 0,
            next_id: 0,
            ready: watch::channel(None).0,
            pending: HashMap::new(),
        };

        let inner = Inner { provider, state: Mutex::new(state) };

        Self(Arc::new(inner))
    }

    /// Wait for the starting head before signing an expiring transaction.
    pub(crate) async fn prepare(&self) -> Result<ReceiptRegistration> {
        let (mut ready, start) = {
            let mut state = self.0.state.lock().expect("receipt tracker state");
            let start = !state.running;

            if start {
                state.running = true;
                state.ready = watch::channel(None).0;
            }

            state.preparing += 1;

            (state.ready.subscribe(), start)
        };

        let registration = ReceiptRegistration { tracker: self.clone() };

        if start {
            tokio::spawn(self.0.clone().run());
        }

        tokio::time::timeout(RECEIPT_TIMEOUT, async {
            loop {
                if let Some(result) = ready.borrow_and_update().clone() {
                    return result.map_err(|error| eyre!(error));
                }

                ready.changed().await.map_err(|_| eyre!("receipt tracker stopped"))?;
            }
        })
        .await
        .map_err(|_| eyre!("timed out starting receipt observation"))??;

        Ok(registration)
    }
}

/// Keeps the scanner alive between establishing the head and registering the
/// final signed hash, without an await between signing and dispatch.
pub(crate) struct ReceiptRegistration {
    tracker: ReceiptTracker,
}

impl ReceiptRegistration {
    pub(crate) fn register(
        self,
        hash: TxHash,
        needs_receipt: bool,
        expires_at: Option<u64>,
    ) -> Result<ReceiptWaiter> {
        let (sender, receiver) = oneshot::channel();

        let id = {
            let mut state = self.tracker.0.state.lock().expect("receipt tracker state");

            if !state.running || !matches!(*state.ready.borrow(), Some(Ok(()))) {
                return Err(eyre!("receipt tracker stopped before transaction registration"));
            }

            let id = state.next_id;
            state.next_id += 1;

            state
                .pending
                .entry(hash)
                .or_default()
                .insert(id, Interest { sender, receipt: needs_receipt, expires_at });

            id
        };

        Ok(ReceiptWaiter { tracker: self.tracker.clone(), hash, id, receiver })
    }
}

impl Drop for ReceiptRegistration {
    fn drop(&mut self) {
        let mut state = self.tracker.0.state.lock().expect("receipt tracker state");
        state.preparing -= 1;
    }
}

/// Dropping a waiter, including during registration, removes its interest.
pub(crate) struct ReceiptWaiter {
    tracker: ReceiptTracker,
    hash: TxHash,
    id: u64,
    receiver: oneshot::Receiver<ReceiptResult>,
}

impl ReceiptWaiter {
    #[cfg(test)]
    pub(crate) async fn wait(self) -> Result<Receipt> {
        match self.observe().await? {
            Inclusion::Included(Some(receipt)) => Ok(receipt),
            _ => Err(eyre!("transaction expired before receipt observation")),
        }
    }

    pub(crate) async fn observe(mut self) -> Result<Inclusion> {
        tokio::time::timeout(INCLUSION_TIMEOUT, &mut self.receiver)
            .await
            .map_err(|_| eyre!("timed out waiting for transaction inclusion"))?
            .map_err(|_| eyre!("receipt tracker stopped"))?
            .map_err(|error| eyre!(error))
    }
}

impl Drop for ReceiptWaiter {
    fn drop(&mut self) {
        let mut state = self.tracker.0.state.lock().expect("receipt tracker state");

        if let Some(waiters) = state.pending.get_mut(&self.hash) {
            waiters.remove(&self.id);

            if waiters.is_empty() {
                state.pending.remove(&self.hash);
            }
        }
    }
}

impl Inner {
    /// Stop under the registration lock so a concurrent registration either
    /// joins this scanner or starts a new one, without losing its wakeup.
    fn stop_if_idle(&self) -> bool {
        let mut state = self.state.lock().expect("receipt tracker state");

        if state.pending.is_empty() && state.preparing == 0 {
            state.running = false;
            true
        } else {
            false
        }
    }

    fn dispatch(&self, receipts: Vec<AnyTransactionReceipt>) {
        let mut state = self.state.lock().expect("receipt tracker state");

        for receipt in receipts {
            if let Some(waiters) = state.pending.remove(&receipt.transaction_hash()) {
                let receipt = Arc::new(receipt);

                for interest in waiters.into_values() {
                    let _ = interest.sender.send(Ok(Inclusion::Included(Some(receipt.clone()))));
                }
            }
        }
    }

    /// Resolve hash-only observations and report whether included hashes need receipts.
    fn dispatch_hashes(&self, hashes: impl Iterator<Item = TxHash>) -> bool {
        let mut state = self.state.lock().expect("receipt tracker state");
        let mut needs_receipts = false;
        for hash in hashes {
            if let Some(waiters) = state.pending.get_mut(&hash) {
                needs_receipts |= waiters.values().any(|interest| interest.receipt);
                let included: Vec<_> = waiters
                    .iter()
                    .filter_map(|(&id, interest)| (!interest.receipt).then_some(id))
                    .collect();
                for id in included {
                    let interest = waiters.remove(&id).expect("included interest exists");
                    let _ = interest.sender.send(Ok(Inclusion::Included(None)));
                }
                if waiters.is_empty() {
                    state.pending.remove(&hash);
                }
            }
        }
        needs_receipts
    }

    fn expire(&self, timestamp: u64) {
        let mut state = self.state.lock().expect("receipt tracker state");
        state.pending.retain(|_, waiters| {
            let expired: Vec<_> = waiters
                .iter()
                .filter_map(|(&id, interest)| {
                    interest.expires_at.filter(|&expiry| expiry <= timestamp).map(|_| id)
                })
                .collect();
            for id in expired {
                let interest = waiters.remove(&id).expect("expired interest exists");
                let _ = interest.sender.send(Ok(Inclusion::Expired));
            }
            !waiters.is_empty()
        });
    }

    async fn run(self: Arc<Self>) {
        let mut next_block = None;

        loop {
            if self.stop_if_idle() {
                return;
            }

            match tokio::time::timeout(RPC_TIMEOUT, self.scan(&mut next_block)).await {
                Ok(Ok(())) => {}
                Ok(Err(error))
                    if matches!(
                        error.downcast_ref::<alloy_transport::TransportError>(),
                        Some(alloy_transport::RpcError::ErrorResp(payload)) if payload.code == -32601
                    ) =>
                {
                    let message = "inclusion observation requires eth_blockNumber and eth_getBlockByNumber (eth_getBlockReceipts for receipt dependencies) on the query endpoint";

                    let mut state = self.state.lock().expect("receipt tracker state");
                    state.ready.send_replace(Some(Err(message.to_string())));

                    for (_, waiters) in state.pending.drain() {
                        for interest in waiters.into_values() {
                            let _ = interest.sender.send(Err(message.to_string()));
                        }
                    }

                    state.running = false;

                    return;
                }
                // Diagnostics deliberately omit endpoint/transport details,
                // which can contain credentials. Retry centrally, not per tx.
                _ => tracing::debug!("block receipt observation failed; retrying"),
            }

            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn scan(&self, next_block: &mut Option<u64>) -> Result<()> {
        let head = self.provider.get_block_number().await?;

        let Some(mut number) = *next_block else {
            // No caller can submit before this baseline has been established.
            *next_block = Some(head.saturating_add(1));

            let state = self.state.lock().expect("receipt tracker state");
            state.ready.send_replace(Some(Ok(())));

            return Ok(());
        };

        if head.saturating_add(1) < number {
            // A rolled-back head must not leave the cursor ahead indefinitely.
            number = head.saturating_add(1);
            *next_block = Some(number);
        }

        // Catch up gaps, including blocks missed during transient RPC failures.
        // Limit each scan so cancellation and newly registered interests remain
        // responsive even when the endpoint is far behind.
        for _ in 0..64 {
            if number > head {
                break;
            }

            let block = self
                .provider
                .get_block_by_number(number.into())
                .await?
                .ok_or_else(|| eyre!("block is not available yet"))?;

            if self.dispatch_hashes(block.transactions().hashes()) {
                let receipts = self
                    .provider
                    .get_block_receipts(BlockId::number(number))
                    .await?
                    .ok_or_else(|| eyre!("block receipts are not available yet"))?;

                self.dispatch(receipts);
            }

            // Only expire after checking inclusion in every block through this
            // timestamp. Wall-clock expiry alone can race a lagging RPC node.
            self.expire(block.header().timestamp());

            number = number.saturating_add(1);
            *next_block = Some(number);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256};
    use alloy_provider::ProviderBuilder;
    use alloy_transport::mock::Asserter;
    use serde_json::{json, Value};

    fn tracker(asserter: &Asserter) -> ReceiptTracker {
        ReceiptTracker::new(
            ProviderBuilder::new_with_network::<AnyNetwork>()
                .connect_mocked_client(asserter.clone())
                .erased(),
        )
    }

    fn block(number: u64, hashes: &[TxHash]) -> Value {
        json!({
            "number": format!("0x{number:x}"), "hash": B256::repeat_byte(number as u8),
            "parentHash": B256::ZERO, "sha3Uncles": B256::ZERO,
            "logsBloom": format!("0x{}", "00".repeat(256)),
            "transactionsRoot": B256::ZERO, "stateRoot": B256::ZERO,
            "receiptsRoot": B256::ZERO, "miner": Address::ZERO,
            "difficulty": "0x0", "extraData": "0x", "gasLimit": "0xffffff",
            "gasUsed": "0x0", "timestamp": format!("0x{number:x}"),
            "uncles": [], "transactions": hashes,
            "mixHash": B256::ZERO, "nonce": "0x0000000000000000"
        })
    }

    fn receipt(hash: TxHash, number: u64) -> Value {
        json!({
            "transactionHash": hash,
            "transactionIndex": "0x0",
            "blockHash": B256::repeat_byte(number as u8),
            "blockNumber": format!("0x{number:x}"),
            "from": Address::ZERO,
            "to": Address::ZERO,
            "cumulativeGasUsed": "0x5208",
            "gasUsed": "0x5208",
            "contractAddress": null,
            "logs": [],
            "logsBloom": format!("0x{}", "00".repeat(256)),
            "status": "0x1",
            "effectiveGasPrice": "0x1",
            "type": "0x2"
        })
    }

    #[tokio::test(start_paused = true)]
    async fn fifty_thousand_interests_share_a_block_response_and_duplicate_waiters() {
        let asserter = Asserter::new();
        asserter.push_success(&"0x0");

        let tracker = tracker(&asserter);
        let mut waiters = Vec::new();
        let mut receipts = Vec::new();

        for i in 0u64..50_000 {
            let hash = alloy_primitives::keccak256(i.to_be_bytes());
            waiters.push(tracker.prepare().await.unwrap().register(hash, true, None).unwrap());
            receipts.push(receipt(hash, 1));
        }

        let duplicate_hash = alloy_primitives::keccak256(0u64.to_be_bytes());
        let duplicate =
            tracker.prepare().await.unwrap().register(duplicate_hash, true, None).unwrap();

        // Unrelated receipts do not create or resolve an interest.
        receipts.push(receipt(B256::repeat_byte(0xff), 1));
        asserter.push_success(&"0x1");
        let hashes: Vec<_> = receipts
            .iter()
            .map(|receipt| serde_json::from_value(receipt["transactionHash"].clone()).unwrap())
            .collect();
        asserter.push_success(&block(1, &hashes));
        asserter.push_success(&receipts);

        for waiter in waiters {
            waiter.wait().await.unwrap();
        }

        assert_eq!(duplicate.wait().await.unwrap().transaction_hash(), duplicate_hash);
        assert!(asserter.read_q().is_empty());
        assert!(tracker.0.state.lock().unwrap().pending.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn retries_missing_receipts_and_catches_up_every_skipped_block() {
        let asserter = Asserter::new();
        asserter.push_success(&"0x5");

        let tracker = tracker(&asserter);
        let hash = B256::repeat_byte(1);
        let waiter = tracker.prepare().await.unwrap().register(hash, true, None).unwrap();

        // The head jumps three blocks; indexing of the middle block is late.
        asserter.push_success(&"0x8");
        asserter.push_success(&block(6, &[]));
        asserter.push_success(&block(7, &[hash]));
        asserter.push_success(&Value::Null);
        asserter.push_success(&"0x8");
        asserter.push_success(&block(7, &[hash]));
        asserter.push_success(&vec![receipt(hash, 7)]);
        // No interests remain after block 7, so block 8 needs no receipt request.
        asserter.push_success(&block(8, &[]));

        assert_eq!(waiter.wait().await.unwrap().block_number(), Some(7));
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn only_included_receipt_waiters_trigger_receipt_fetches() {
        let asserter = Asserter::new();
        asserter.push_success(&"0x0");

        let tracker = tracker(&asserter);
        let receipt_hash = B256::repeat_byte(1);
        let included_hash = B256::repeat_byte(2);
        let expired_hash = B256::repeat_byte(3);
        let receipt_waiter =
            tracker.prepare().await.unwrap().register(receipt_hash, true, Some(2)).unwrap();
        let included =
            tracker.prepare().await.unwrap().register(included_hash, false, Some(1)).unwrap();
        let expired =
            tracker.prepare().await.unwrap().register(expired_hash, false, Some(1)).unwrap();

        asserter.push_success(&"0x2");
        // A receipt waiter exists, but its hash is absent: no receipt RPC for block 1.
        asserter.push_success(&block(1, &[included_hash]));
        asserter.push_success(&block(2, &[receipt_hash]));
        asserter.push_success(&Value::Null);
        // Retry this block before expiry can resolve the included receipt waiter.
        asserter.push_success(&"0x2");
        asserter.push_success(&block(2, &[receipt_hash]));
        asserter.push_success(&vec![receipt(receipt_hash, 2)]);

        assert!(matches!(included.observe().await.unwrap(), Inclusion::Included(None)));
        assert!(matches!(expired.observe().await.unwrap(), Inclusion::Expired));
        assert_eq!(receipt_waiter.wait().await.unwrap().transaction_hash(), receipt_hash);
        assert!(asserter.read_q().is_empty());
        assert!(tracker.0.state.lock().unwrap().pending.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_stops_polling_and_new_interest_restarts_at_current_head() {
        let asserter = Asserter::new();
        asserter.push_success(&"0x1");

        let tracker = tracker(&asserter);
        let hash = B256::repeat_byte(1);
        let first = tracker.prepare().await.unwrap().register(hash, true, None).unwrap();
        let second = tracker.prepare().await.unwrap().register(hash, true, None).unwrap();

        drop(first);
        assert_eq!(tracker.0.state.lock().unwrap().pending[&hash].len(), 1);

        drop(second);
        tokio::time::sleep(POLL_INTERVAL * 2).await;
        assert!(!tracker.0.state.lock().unwrap().running);

        asserter.push_success(&"0x64");
        let restarted = tracker.prepare().await.unwrap().register(hash, true, None).unwrap();

        asserter.push_success(&"0x65");
        asserter.push_success(&block(101, &[hash]));
        asserter.push_success(&vec![receipt(hash, 101)]);

        restarted.wait().await.unwrap();

        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn transient_receipt_failure_does_not_advance_cursor() {
        let asserter = Asserter::new();
        asserter.push_success(&"0x0");

        let tracker = tracker(&asserter);
        let hash = B256::repeat_byte(1);
        let waiter = tracker.prepare().await.unwrap().register(hash, true, None).unwrap();

        asserter.push_success(&"0x1");
        asserter.push_success(&block(1, &[hash]));
        asserter.push_failure_msg("temporary receipt failure");
        asserter.push_success(&"0x1");
        asserter.push_success(&block(1, &[hash]));
        asserter.push_success(&vec![receipt(hash, 1)]);

        assert_eq!(waiter.wait().await.unwrap().block_number(), Some(1));
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn unsupported_block_receipts_fail_waiters_without_per_transaction_fallback() {
        let asserter = Asserter::new();
        asserter.push_success(&"0x0");

        let tracker = tracker(&asserter);
        let first =
            tracker.prepare().await.unwrap().register(B256::repeat_byte(1), true, None).unwrap();
        let second =
            tracker.prepare().await.unwrap().register(B256::repeat_byte(2), true, None).unwrap();

        asserter.push_success(&"0x1");
        asserter.push_success(&block(1, &[B256::repeat_byte(1)]));
        asserter.push_failure(
            serde_json::from_value(json!({
                "code": -32601, "message": "method not found"
            }))
            .unwrap(),
        );

        assert!(first.wait().await.unwrap_err().to_string().contains("eth_getBlockReceipts"));
        assert!(second.wait().await.is_err());
        assert!(asserter.read_q().is_empty());
        assert!(!tracker.0.state.lock().unwrap().running);
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_removes_interest_when_chain_stalls() {
        let asserter = Asserter::new();
        asserter.push_success(&"0x0");

        let tracker = tracker(&asserter);
        let waiter =
            tracker.prepare().await.unwrap().register(B256::repeat_byte(1), true, None).unwrap();

        assert!(waiter.wait().await.unwrap_err().to_string().contains("timed out"));
        assert!(tracker.0.state.lock().unwrap().pending.is_empty());
    }
}
