use std::collections::HashMap;

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
}
