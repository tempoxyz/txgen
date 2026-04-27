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
        let mut out = String::with_capacity(42);
        out.push_str("0x");
        for byte in self.0 {
            use std::fmt::Write;
            write!(out, "{byte:02x}").map_err(serde::ser::Error::custom)?;
        }
        serializer.serialize_str(&out)
    }
}

impl<'de> Deserialize<'de> for SchedulingKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        parse_hex_key(&value).map(Self::new).map_err(serde::de::Error::custom)
    }
}

fn parse_hex_key(value: &str) -> Result<[u8; 20], String> {
    let hex = value.strip_prefix("0x").unwrap_or(value);
    if hex.len() != 40 {
        return Err(format!("scheduling key must be 20 bytes, got {} hex chars", hex.len()));
    }

    let mut bytes = [0u8; 20];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let hi = hex_nibble(hex.as_bytes()[index * 2])?;
        let lo = hex_nibble(hex.as_bytes()[index * 2 + 1])?;
        *byte = (hi << 4) | lo;
    }
    Ok(bytes)
}

fn hex_nibble(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(format!("invalid hex character '{}'", byte as char)),
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
