use std::collections::HashMap;

/// One nonce value consumed from a tracker while materializing a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NonceReservation {
    /// Scheduling lane whose counter was advanced.
    pub key: [u8; 20],
    /// Reserved nonce value.
    pub nonce: u64,
    /// Whether the reservation orders transactions or only guarantees uniqueness.
    pub kind: NonceReservationKind,
}

/// How a locally reserved nonce participates in submission ordering and rollback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NonceReservationKind {
    /// A chain nonce that orders transactions sharing the same scheduling lane.
    Ordered,
    /// A consume-once local counter used to make independently valid payloads unique.
    ///
    /// Unique reservations are allocated atomically, but they neither serialize
    /// transaction submission nor get reused after a later materialization or
    /// submission failure.
    Unique,
}

/// Tracks nonces per scheduling key.
///
/// Each scheduling key maps to a monotonically increasing nonce counter.
/// This ensures transactions with the same key are ordered correctly.
#[derive(Debug, Default)]
pub struct NonceTracker {
    nonces: HashMap<[u8; 20], u64>,
}

impl NonceTracker {
    /// Create a new nonce tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the next nonce for a scheduling key, incrementing the counter.
    pub fn next(&mut self, key: [u8; 20]) -> u64 {
        let nonce = self.nonces.entry(key).or_insert(0);
        let current = *nonce;
        *nonce += 1;
        current
    }

    /// Get the current nonce for a key without incrementing.
    pub fn current(&self, key: &[u8; 20]) -> u64 {
        self.nonces.get(key).copied().unwrap_or(0)
    }

    /// Peek the next nonce without consuming it.
    pub fn peek(&self, key: &[u8; 20]) -> u64 {
        self.current(key)
    }

    /// Reset the nonce for a key to a specific value.
    pub fn reset(&mut self, key: [u8; 20], nonce: u64) {
        self.nonces.insert(key, nonce);
    }

    /// Rewind a just-reserved nonce when no later reservation used the same key.
    ///
    /// Returns `true` only when the current counter is exactly `nonce + 1`.
    /// Online callers use this after an RPC rejects a signed transaction, while
    /// avoiding a rollback over a concurrently materialized transaction.
    pub fn rewind(&mut self, key: [u8; 20], nonce: u64) -> bool {
        let Some(expected) = nonce.checked_add(1) else {
            return false;
        };
        if self.current(&key) != expected {
            return false;
        }
        self.reset(key, nonce);
        true
    }

    /// Clear all tracked nonces.
    pub fn clear(&mut self) {
        self.nonces.clear();
    }

    /// Get the number of tracked keys.
    pub fn len(&self) -> usize {
        self.nonces.len()
    }

    /// Check if no keys are tracked.
    pub fn is_empty(&self) -> bool {
        self.nonces.is_empty()
    }

    /// Check if a key has been initialized.
    pub fn contains(&self, key: &[u8; 20]) -> bool {
        self.nonces.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nonce_tracking() {
        let mut tracker = NonceTracker::new();
        let key = [0u8; 20];

        assert_eq!(tracker.current(&key), 0);
        assert_eq!(tracker.next(key), 0);
        assert_eq!(tracker.next(key), 1);
        assert_eq!(tracker.next(key), 2);
        assert_eq!(tracker.current(&key), 3);
    }

    #[test]
    fn test_independent_keys() {
        let mut tracker = NonceTracker::new();
        let key1 = [1u8; 20];
        let key2 = [2u8; 20];

        assert_eq!(tracker.next(key1), 0);
        assert_eq!(tracker.next(key2), 0);
        assert_eq!(tracker.next(key1), 1);
        assert_eq!(tracker.next(key2), 1);
    }

    #[test]
    fn test_reset() {
        let mut tracker = NonceTracker::new();
        let key = [0u8; 20];

        tracker.next(key);
        tracker.next(key);
        tracker.reset(key, 100);

        assert_eq!(tracker.next(key), 100);
    }

    #[test]
    fn test_rewind_only_latest_reservation() {
        let mut tracker = NonceTracker::new();
        let key = [3u8; 20];
        assert_eq!(tracker.next(key), 0);
        assert!(tracker.rewind(key, 0));
        assert_eq!(tracker.next(key), 0);

        assert_eq!(tracker.next(key), 1);
        assert!(!tracker.rewind(key, 0));
        assert_eq!(tracker.current(&key), 2);
    }
}
