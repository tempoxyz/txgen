//! Request-scoped RPC authentication.
//!
//! Authentication providers receive non-secret request context and return the
//! HTTP headers for one JSON-RPC request. Callers must attach the returned map
//! to that request rather than configuring client-wide default headers.

use alloy_primitives::{Address, TxHash};
use eyre::{bail, Context, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{
    de::{MapAccess, Visitor},
    Deserialize, Deserializer,
};
use std::{
    collections::HashMap,
    fmt,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant, SystemTime},
};

/// Non-secret information about one JSON-RPC request.
#[derive(Debug, Clone, Copy)]
pub struct RpcRequestContext<'a> {
    /// Identity of the RPC endpoint selected for the request.
    pub endpoint: &'a str,
    /// JSON-RPC method name.
    pub method: &'a str,
    /// Logical on-chain transaction sender, when the request is sender-scoped.
    pub sender: Option<Address>,
    /// Transaction hash, once submission has succeeded.
    pub tx_hash: Option<TxHash>,
}

/// Supplies request-specific HTTP authentication headers.
pub trait RequestAuthProvider: Send + Sync {
    /// Return the headers for exactly one RPC request.
    ///
    /// Implementations must not include credential values in errors, tracing,
    /// or debug output. Callers additionally mark every returned value as
    /// sensitive before attaching the headers to a request.
    fn headers_for(&self, context: &RpcRequestContext<'_>) -> Result<HeaderMap>;
}

/// Sender-to-header map loaded from a JSON file.
///
/// The file is checked at most once per `reload_interval`. A replacement is
/// parsed and validated in full before it replaces the active map. Failed
/// reloads leave the last valid map active.
pub struct SenderHeaderAuthProvider {
    header_name: HeaderName,
    path: PathBuf,
    reload_interval: Duration,
    active: RwLock<Arc<SenderHeaderMap>>,
    reload: Mutex<ReloadState>,
}

type SenderHeaderMap = HashMap<Address, HeaderValue>;

#[derive(Debug)]
struct ReloadState {
    next_check: Instant,
    active_stamp: FileStamp,
    permission_checked_stamp: FileStamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileStamp {
    modified: Option<SystemTime>,
    created: Option<SystemTime>,
    len: u64,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    mode: u32,
}

impl SenderHeaderAuthProvider {
    /// Load and validate a sender map from `path`.
    pub fn from_file(
        header_name: &str,
        path: impl Into<PathBuf>,
        reload_interval: Duration,
    ) -> Result<Self> {
        let header_name = HeaderName::from_str(header_name)
            .map_err(|_| eyre::eyre!("invalid sender authentication header name"))?;
        let path = path.into();
        let opened = open_sender_map(&path).wrap_err("failed to load sender header map")?;
        warn_if_broadly_readable(&path, &opened.stamp);
        let stamp = opened.stamp;
        let map = read_sender_map(opened).wrap_err("failed to load sender header map")?;

        Ok(Self {
            header_name,
            path,
            reload_interval,
            active: RwLock::new(Arc::new(map)),
            reload: Mutex::new(ReloadState {
                next_check: Instant::now() + reload_interval,
                active_stamp: stamp,
                permission_checked_stamp: stamp,
            }),
        })
    }

    fn maybe_reload(&self) {
        let now = Instant::now();
        let Ok(mut reload) = self.reload.try_lock() else {
            return;
        };
        if now < reload.next_check {
            return;
        }
        reload.next_check = now + self.reload_interval;

        let opened = match open_sender_map(&self.path) {
            Ok(opened) => opened,
            Err(_) => {
                // Deliberately omit the underlying parse error: malformed file
                // contents may themselves contain authentication values.
                tracing::warn!(
                    path = %self.path.display(),
                    "Failed to reload sender header map; keeping the last valid map"
                );
                return;
            }
        };

        if opened.stamp != reload.permission_checked_stamp {
            warn_if_broadly_readable(&self.path, &opened.stamp);
            reload.permission_checked_stamp = opened.stamp;
        }
        // Unix identity includes device/inode and mode, so an equal stamp is a
        // strong unchanged-file signal. On other platforms, metadata alone may
        // not distinguish an equal-length atomic replacement on a coarse-time
        // filesystem; read and validate on each bounded check there.
        #[cfg(unix)]
        if opened.stamp == reload.active_stamp {
            return;
        }

        let stamp = opened.stamp;
        match read_sender_map(opened) {
            Ok(map) => {
                let entries = map.len();
                *self.active.write().expect("sender header map lock poisoned") = Arc::new(map);
                let stamp_changed = reload.active_stamp != stamp;
                reload.active_stamp = stamp;
                if stamp_changed {
                    tracing::info!(path = %self.path.display(), entries, "Reloaded sender header map");
                }
            }
            Err(_) => {
                // Deliberately omit the underlying parse error: malformed file
                // contents may themselves contain authentication values.
                tracing::warn!(
                    path = %self.path.display(),
                    "Failed to reload sender header map; keeping the last valid map"
                );
            }
        }
    }
}

impl RequestAuthProvider for SenderHeaderAuthProvider {
    fn headers_for(&self, context: &RpcRequestContext<'_>) -> Result<HeaderMap> {
        self.maybe_reload();

        let sender = context.sender.ok_or_else(|| {
            eyre::eyre!(
                "sender metadata is required for authenticated RPC method {}",
                context.method
            )
        })?;
        let active = self.active.read().expect("sender header map lock poisoned").clone();
        let value = active.get(&sender).ok_or_else(|| {
            eyre::eyre!(
                "no sender authentication mapping for {sender} (RPC method {})",
                context.method
            )
        })?;

        let mut headers = HeaderMap::new();
        headers.insert(self.header_name.clone(), value.clone());
        Ok(headers)
    }
}

impl fmt::Debug for SenderHeaderAuthProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SenderHeaderAuthProvider")
            .field("header_name", &self.header_name)
            .field("path", &self.path)
            .field("reload_interval", &self.reload_interval)
            .field("active", &"<redacted>")
            .finish()
    }
}

struct OpenedSenderMap {
    file: File,
    stamp: FileStamp,
}

fn open_sender_map(path: &Path) -> Result<OpenedSenderMap> {
    let file = File::open(path).wrap_err("failed to open map file")?;
    let metadata = file.metadata().wrap_err("failed to inspect map file")?;
    let stamp = FileStamp::from_metadata(&metadata);
    Ok(OpenedSenderMap { file, stamp })
}

fn read_sender_map(mut opened: OpenedSenderMap) -> Result<SenderHeaderMap> {
    let mut bytes = Vec::new();
    opened.file.read_to_end(&mut bytes).wrap_err("failed to read map file")?;
    parse_sender_map(&bytes)
}

fn parse_sender_map(bytes: &[u8]) -> Result<SenderHeaderMap> {
    let raw: RawSenderMap = serde_json::from_slice(bytes).map_err(|error| {
        eyre::eyre!(
            "invalid sender header map JSON ({:?} at line {}, column {})",
            error.classify(),
            error.line(),
            error.column()
        )
    })?;
    if raw.0.is_empty() {
        bail!("sender header map must contain at least one entry");
    }

    let mut parsed = HashMap::with_capacity(raw.0.len());
    for (index, (address, token)) in raw.0.into_iter().enumerate() {
        let sender = Address::from_str(&address)
            .map_err(|_| eyre::eyre!("invalid sender address at map entry {index}"))?;
        if token.is_empty() {
            bail!("empty authentication value for sender {sender}");
        }
        let mut value = HeaderValue::from_str(&token)
            .map_err(|_| eyre::eyre!("invalid HTTP header value for sender {sender}"))?;
        value.set_sensitive(true);

        if parsed.insert(sender, value).is_some() {
            bail!("duplicate normalized sender address in sender header map: {sender}");
        }
    }
    Ok(parsed)
}

/// Raw entries are retained in order so duplicate JSON keys cannot be silently
/// overwritten before normalized-address validation runs.
struct RawSenderMap(Vec<(String, String)>);

impl<'de> Deserialize<'de> for RawSenderMap {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SenderMapVisitor;

        impl<'de> Visitor<'de> for SenderMapVisitor {
            type Value = RawSenderMap;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON object mapping sender addresses to header values")
            }

            fn visit_map<M>(self, mut map: M) -> std::result::Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut entries = Vec::with_capacity(map.size_hint().unwrap_or(0));
                while let Some(entry) = map.next_entry()? {
                    entries.push(entry);
                }
                Ok(RawSenderMap(entries))
            }
        }

        deserializer.deserialize_map(SenderMapVisitor)
    }
}

impl FileStamp {
    fn from_metadata(metadata: &std::fs::Metadata) -> Self {
        Self {
            modified: metadata.modified().ok(),
            created: metadata.created().ok(),
            len: metadata.len(),
            #[cfg(unix)]
            device: std::os::unix::fs::MetadataExt::dev(metadata),
            #[cfg(unix)]
            inode: std::os::unix::fs::MetadataExt::ino(metadata),
            #[cfg(unix)]
            mode: std::os::unix::fs::MetadataExt::mode(metadata),
        }
    }
}

#[cfg(unix)]
fn warn_if_broadly_readable(path: &Path, stamp: &FileStamp) {
    if stamp.mode & 0o044 != 0 {
        tracing::warn!(
            path = %path.display(),
            permissions = format_args!("{:#05o}", stamp.mode & 0o777),
            "Sender header map is readable by group or other users"
        );
    }
}

#[cfg(not(unix))]
fn warn_if_broadly_readable(_path: &Path, _stamp: &FileStamp) {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    const HEADER: &str = "x-test-sender-auth";
    const SENDER: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn context(sender: Option<Address>) -> RpcRequestContext<'static> {
        RpcRequestContext {
            endpoint: "submission-0",
            method: "eth_sendRawTransaction",
            sender,
            tx_hash: None,
        }
    }

    fn write_map(path: &Path, value: &str) {
        fs::write(path, format!(r#"{{"{SENDER}":"{value}"}}"#)).unwrap();
    }

    fn replace(path: &Path, contents: &str) {
        let replacement = path.with_extension("replacement");
        fs::write(&replacement, contents).unwrap();
        fs::rename(replacement, path).unwrap();
    }

    fn value(provider: &SenderHeaderAuthProvider) -> String {
        let sender = SENDER.parse().unwrap();
        provider
            .headers_for(&context(Some(sender)))
            .unwrap()
            .get(HEADER)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn normalizes_addresses_and_rejects_normalized_duplicates() {
        let parsed = parse_sender_map(
            br#"{"0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA":"not-a-real-token"}"#,
        )
        .unwrap();
        assert!(parsed.contains_key(&SENDER.parse::<Address>().unwrap()));

        let duplicate = br#"{
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa":"value-one",
            "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA":"value-two"
        }"#;
        assert!(parse_sender_map(duplicate)
            .unwrap_err()
            .to_string()
            .contains("duplicate normalized sender"));

        let exact_duplicate = br#"{
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa":"value-one",
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa":"value-two"
        }"#;
        assert!(parse_sender_map(exact_duplicate)
            .unwrap_err()
            .to_string()
            .contains("duplicate normalized sender"));
    }

    #[test]
    fn validates_map_entries_without_exposing_values() {
        assert!(parse_sender_map(br#"{}"#).is_err());
        let invalid_address =
            parse_sender_map(br#"{"secret-fixture-key":"secret-fixture-value"}"#).unwrap_err();
        let invalid_address_debug = format!("{invalid_address:?}");
        assert!(!invalid_address_debug.contains("secret-fixture-key"));
        assert!(!invalid_address_debug.contains("secret-fixture-value"));

        let top_level_secret = parse_sender_map(br#""secret-fixture-value""#).unwrap_err();
        assert!(!format!("{top_level_secret:?}").contains("secret-fixture-value"));

        let err = parse_sender_map(
            format!(r#"{{"{SENDER}":"invalid\nsecret-fixture-value"}}"#).as_bytes(),
        )
        .unwrap_err();
        assert!(!format!("{err:?}").contains("secret-fixture-value"));
    }

    #[test]
    fn initial_load_errors_do_not_expose_file_contents() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("map.json");
        fs::write(&path, r#""secret-fixture-value""#).unwrap();

        let error = SenderHeaderAuthProvider::from_file(HEADER, &path, Duration::ZERO).unwrap_err();
        assert!(!format!("{error:?}").contains("secret-fixture-value"));
    }

    #[cfg(unix)]
    #[test]
    fn file_stamp_tracks_permission_changes() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().unwrap();
        let path = temp.path().join("map.json");
        write_map(&path, "fixture-value-one");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let private = open_sender_map(&path).unwrap().stamp;

        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        let broad = open_sender_map(&path).unwrap().stamp;

        assert_ne!(private, broad);
        assert_eq!(broad.mode & 0o777, 0o644);
    }

    #[test]
    fn requires_sender_and_mapping() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("map.json");
        write_map(&path, "fixture-value-one");
        let provider = SenderHeaderAuthProvider::from_file(HEADER, &path, Duration::ZERO).unwrap();

        assert!(provider.headers_for(&context(None)).unwrap_err().to_string().contains("sender"));
        assert!(provider
            .headers_for(&context(Some(Address::ZERO)))
            .unwrap_err()
            .to_string()
            .contains("no sender authentication mapping"));
    }

    #[test]
    fn atomically_reloads_a_valid_replacement() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("map.json");
        write_map(&path, "fixture-value-one");
        let provider = SenderHeaderAuthProvider::from_file(HEADER, &path, Duration::ZERO).unwrap();
        assert_eq!(value(&provider), "fixture-value-one");

        replace(&path, &format!(r#"{{"{SENDER}":"fixture-value-two"}}"#));
        assert_eq!(value(&provider), "fixture-value-two");
    }

    #[test]
    fn malformed_replacement_keeps_last_valid_map() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("map.json");
        write_map(&path, "fixture-value-one");
        let provider = SenderHeaderAuthProvider::from_file(HEADER, &path, Duration::ZERO).unwrap();

        replace(&path, "{ malformed");
        assert_eq!(value(&provider), "fixture-value-one");
    }

    #[test]
    fn header_values_and_provider_debug_are_redacted() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("map.json");
        write_map(&path, "fixture-value-one");
        let provider = SenderHeaderAuthProvider::from_file(HEADER, &path, Duration::ZERO).unwrap();
        let headers = provider.headers_for(&context(Some(SENDER.parse().unwrap()))).unwrap();

        assert!(!format!("{headers:?}").contains("fixture-value-one"));
        assert!(!format!("{provider:?}").contains("fixture-value-one"));
    }
}
