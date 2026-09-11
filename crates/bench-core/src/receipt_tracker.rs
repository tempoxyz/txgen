//! Shared block receipt observation for transaction inclusion dependencies.
//!
//! Register before dispatching a transaction. Registration waits for a shared
//! starting head, so even inclusion before the submission response is covered.
//! HTTP endpoints use one head poller; receipt RPC traffic scales with blocks,
//! never with the number of pending transactions. This observes inclusion only,
//! not finality or additional confirmations.

use alloy_eips::BlockId;
use alloy_network::{AnyNetwork, AnyTransactionReceipt};
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

type Receipt = Arc<AnyTransactionReceipt>;
type ReceiptResult = Result<Receipt, String>;

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
    pending: HashMap<TxHash, HashMap<u64, oneshot::Sender<ReceiptResult>>>,
}

impl ReceiptTracker {
    /// Use an endpoint authorized for `eth_blockNumber` and `eth_getBlockReceipts`.
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

    /// Establish interest before the caller sends the signed transaction.
    #[cfg(test)]
    pub(crate) async fn register(&self, hash: TxHash) -> Result<ReceiptWaiter> {
        self.prepare().await?.register(hash)
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
    pub(crate) fn register(self, hash: TxHash) -> Result<ReceiptWaiter> {
        let (sender, receiver) = oneshot::channel();

        let id = {
            let mut state = self.tracker.0.state.lock().expect("receipt tracker state");

            if !state.running || !matches!(*state.ready.borrow(), Some(Ok(()))) {
                return Err(eyre!("receipt tracker stopped before transaction registration"));
            }

            let id = state.next_id;
            state.next_id += 1;

            state.pending.entry(hash).or_default().insert(id, sender);

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
    pub(crate) async fn wait(mut self) -> Result<Receipt> {
        tokio::time::timeout(RECEIPT_TIMEOUT, &mut self.receiver)
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

                for sender in waiters.into_values() {
                    let _ = sender.send(Ok(receipt.clone()));
                }
            }
        }
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
                    let message = "receipt observation requires eth_blockNumber and eth_getBlockReceipts on the query endpoint";

                    let mut state = self.state.lock().expect("receipt tracker state");
                    state.ready.send_replace(Some(Err(message.to_string())));

                    for (_, waiters) in state.pending.drain() {
                        for sender in waiters.into_values() {
                            let _ = sender.send(Err(message.to_string()));
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

            let receipts = self
                .provider
                .get_block_receipts(BlockId::number(number))
                .await?
                .ok_or_else(|| eyre!("block receipts are not available yet"))?;

            if receipts.iter().any(|r| r.block_number() != Some(number) || r.block_hash().is_none())
            {
                return Err(eyre!("block receipt response has inconsistent inclusion fields"));
            }

            self.dispatch(receipts);

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
    async fn ten_thousand_interests_share_a_block_response_and_duplicate_waiters() {
        let asserter = Asserter::new();
        asserter.push_success(&"0x0");

        let tracker = tracker(&asserter);
        let mut waiters = Vec::new();
        let mut receipts = Vec::new();

        for i in 0u64..10_000 {
            let hash = alloy_primitives::keccak256(i.to_be_bytes());
            waiters.push(tracker.register(hash).await.unwrap());
            receipts.push(receipt(hash, 1));
        }

        let duplicate_hash = alloy_primitives::keccak256(0u64.to_be_bytes());
        let duplicate = tracker.register(duplicate_hash).await.unwrap();

        // Unrelated receipts do not create or resolve an interest.
        receipts.push(receipt(B256::repeat_byte(0xff), 1));
        asserter.push_success(&"0x1");
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
        let waiter = tracker.register(hash).await.unwrap();

        // The head jumps three blocks; indexing of the middle block is late.
        asserter.push_success(&"0x8");
        asserter.push_success(&Vec::<Value>::new());
        asserter.push_success(&Value::Null);
        asserter.push_success(&"0x8");
        asserter.push_success(&vec![receipt(hash, 7)]);
        asserter.push_success(&Vec::<Value>::new());

        assert_eq!(waiter.wait().await.unwrap().block_number(), Some(7));
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_stops_polling_and_new_interest_restarts_at_current_head() {
        let asserter = Asserter::new();
        asserter.push_success(&"0x1");

        let tracker = tracker(&asserter);
        let hash = B256::repeat_byte(1);
        let first = tracker.register(hash).await.unwrap();
        let second = tracker.register(hash).await.unwrap();

        drop(first);
        assert_eq!(tracker.0.state.lock().unwrap().pending[&hash].len(), 1);

        drop(second);
        tokio::time::sleep(POLL_INTERVAL * 2).await;
        assert!(!tracker.0.state.lock().unwrap().running);

        asserter.push_success(&"0x64");
        let restarted = tracker.register(hash).await.unwrap();

        asserter.push_success(&"0x65");
        asserter.push_success(&vec![receipt(hash, 101)]);

        restarted.wait().await.unwrap();

        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn transient_failure_and_invalid_receipts_do_not_advance_cursor() {
        let asserter = Asserter::new();
        asserter.push_success(&"0x0");

        let tracker = tracker(&asserter);
        let hash = B256::repeat_byte(1);
        let waiter = tracker.register(hash).await.unwrap();

        asserter.push_success(&"0x1");
        asserter.push_failure_msg("temporary receipt failure");
        asserter.push_success(&"0x1");
        asserter.push_success(&vec![receipt(hash, 2)]);
        asserter.push_success(&"0x1");
        asserter.push_success(&vec![receipt(hash, 1)]);

        assert_eq!(waiter.wait().await.unwrap().block_number(), Some(1));
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn unsupported_block_receipts_fail_waiters_without_per_transaction_fallback() {
        let asserter = Asserter::new();
        asserter.push_success(&"0x0");

        let tracker = tracker(&asserter);
        let first = tracker.register(B256::repeat_byte(1)).await.unwrap();
        let second = tracker.register(B256::repeat_byte(2)).await.unwrap();

        asserter.push_success(&"0x1");
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
        let waiter = tracker.register(B256::repeat_byte(1)).await.unwrap();

        assert!(waiter.wait().await.unwrap_err().to_string().contains("timed out"));
        assert!(tracker.0.state.lock().unwrap().pending.is_empty());
    }
}
