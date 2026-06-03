use alloy_primitives::FixedBytes;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A 20-byte transaction scheduling key.
///
/// Scheduling keys are opaque ordering constraints. They are often derived from
/// addresses, but can also be hashes of protocol lanes or sequence instances,
/// so this type intentionally does not use an address newtype.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SchedulingKey([u8; 20]);

impl SchedulingKey {
    /// Create a scheduling key from raw bytes.
    pub const fn new(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }

    /// Return the raw key bytes.
    pub const fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    /// Consume the key and return the raw bytes.
    pub const fn into_inner(self) -> [u8; 20] {
        self.0
    }
}

impl From<[u8; 20]> for SchedulingKey {
    fn from(bytes: [u8; 20]) -> Self {
        Self::new(bytes)
    }
}

impl From<SchedulingKey> for [u8; 20] {
    fn from(key: SchedulingKey) -> Self {
        key.into_inner()
    }
}

impl AsRef<[u8; 20]> for SchedulingKey {
    fn as_ref(&self) -> &[u8; 20] {
        self.as_bytes()
    }
}

/// Deduplicate scheduling keys while preserving their first-seen order.
pub fn dedup_scheduling_keys(keys: impl IntoIterator<Item = SchedulingKey>) -> Vec<SchedulingKey> {
    let mut deduped = Vec::new();
    for key in keys {
        if !deduped.contains(&key) {
            deduped.push(key);
        }
    }
    deduped
}

impl Serialize for SchedulingKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        FixedBytes::<20>::from(self.0).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SchedulingKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        FixedBytes::<20>::deserialize(deserializer).map(|bytes| Self::new(bytes.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_roundtrip() {
        let key = SchedulingKey::from([0xab; 20]);
        let json = serde_json::to_string(&key).unwrap();
        assert_eq!(json, "\"0xabababababababababababababababababababab\"");

        let parsed: SchedulingKey = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, key);
    }
}
