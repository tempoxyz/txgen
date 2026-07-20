use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Duration, Instant},
};
use txgen_tempo::zone_auth::{
    parse_token_fields, recover_signer, SIGNATURE_LEN, TOKEN_HEX_LEN, TOKEN_LEN, TOKEN_VERSION,
};

const TEST_MNEMONIC: &str = "test test test test test test test test test test test junk";
const MNEMONIC_ENV: &str = "TXGEN_AUTH_TOKEN_MAP_TEST_MNEMONIC";
const ZONE_ID: u32 = 71;
const CHAIN_ID: u64 = 421_700_071;
static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let sequence = NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir()
            .join(format!("txgen-auth-token-map-cli-{}-{sequence}", std::process::id()));
        fs::create_dir(&path).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        }

        Self(path)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self(Some(child))
    }

    fn id(&self) -> u32 {
        self.0.as_ref().unwrap().id()
    }

    fn wait_with_output(mut self) -> Output {
        self.0.take().unwrap().wait_with_output().unwrap()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn selected_thousand_account_pool_matches_the_flat_token_contract() {
    const START: u32 = 11;
    const END: u32 = 1_011;

    let directory = TestDir::new();
    let spec = directory.join("spec.yaml");
    let output_path = directory.join("tokens.json");
    write_spec(&spec, "users", START, END);

    let output = auth_command(&spec, "users", &output_path, ZONE_ID, 600, 30).output().unwrap();
    assert_success(&output);

    let raw = fs::read(&output_path).unwrap();
    let tokens: BTreeMap<String, String> = serde_json::from_slice(&raw).unwrap();
    assert_eq!(tokens.len(), usize::try_from(END - START).unwrap());
    assert_eq!(raw, serde_json::to_vec(&tokens).unwrap());
    assert!(!raw.iter().any(u8::is_ascii_whitespace));

    let addresses_output = Command::new(env!("CARGO_BIN_EXE_txgen-tempo"))
        .arg("addresses")
        .arg("--spec")
        .arg(&spec)
        .env(MNEMONIC_ENV, TEST_MNEMONIC)
        .output()
        .unwrap();
    assert_success(&addresses_output);
    let mut expected_addresses = String::from_utf8(addresses_output.stdout)
        .unwrap()
        .lines()
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();
    expected_addresses.sort_unstable();
    assert_eq!(tokens.keys().cloned().collect::<Vec<_>>(), expected_addresses);

    let mut shared_fields = None;
    for (address, token_hex) in &tokens {
        assert_eq!(address.len(), 42);
        assert!(address.starts_with("0x"));
        assert!(is_lower_hex(&address[2..]));
        assert_eq!(token_hex.len(), TOKEN_HEX_LEN);
        assert!(is_lower_hex(token_hex));

        let token = hex::decode(token_hex).unwrap();
        assert_eq!(token.len(), TOKEN_LEN);
        let fields = parse_token_fields(&token[SIGNATURE_LEN..]).unwrap();
        let field_tuple =
            (fields.version, fields.zone_id, fields.chain_id, fields.issued_at, fields.expires_at);
        assert_eq!(fields.version, TOKEN_VERSION);
        assert_eq!(fields.zone_id, ZONE_ID);
        assert_eq!(fields.chain_id, CHAIN_ID);
        assert_eq!(fields.expires_at - fields.issued_at, 600);
        assert_eq!(shared_fields.get_or_insert(field_tuple), &field_tuple);

        let recovered = recover_signer(&token).unwrap();
        assert_eq!(format!("0x{}", hex::encode(recovered.as_slice())), *address);
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(fs::metadata(&output_path).unwrap().permissions().mode() & 0o777, 0o600);
    }

    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stdout.contains("wrote 1000 Zone authorization tokens"));
    assert!(stdout.contains(&format!("zone {ZONE_ID} chain {CHAIN_ID}")));
    assert!(stdout.contains(&output_path.display().to_string()));
    assert!(!stdout.contains(TEST_MNEMONIC));
    assert!(!stderr.contains(TEST_MNEMONIC));
    assert!(!stdout.contains('{'));
    for (address, token) in &tokens {
        assert!(!stdout.contains(address));
        assert!(!stdout.contains(token));
        assert!(!stderr.contains(token));
    }
}

#[test]
fn rejects_unknown_and_empty_account_pools() {
    let directory = TestDir::new();
    let spec = directory.join("spec.yaml");
    let output_path = directory.join("tokens.json");
    write_spec(&spec, "users", 0, 1);

    let unknown = auth_command(&spec, "missing", &output_path, ZONE_ID, 600, 30).output().unwrap();
    assert_failure(&unknown, "account pool 'missing' not found");
    assert!(!output_path.exists());

    write_spec(&spec, "users", 8, 8);
    let empty = auth_command(&spec, "users", &output_path, ZONE_ID, 600, 30).output().unwrap();
    assert_failure(&empty, "account pool 'users' is empty");
    assert!(!output_path.exists());
}

#[test]
fn rejects_invalid_zone_ttl_and_refresh_windows() {
    let directory = TestDir::new();
    let spec = directory.join("spec.yaml");
    write_spec(&spec, "users", 0, 1);

    let cases = [
        (0, 600, 30, "--zone-id must be nonzero"),
        (ZONE_ID, 0, 0, "--ttl-secs must be between 1 and 2592000"),
        (ZONE_ID, 2_592_001, 30, "--ttl-secs must be between 1 and 2592000"),
        (ZONE_ID, 30, 30, "--refresh-before-secs must be less than --ttl-secs"),
        (ZONE_ID, 30, 31, "--refresh-before-secs must be less than --ttl-secs"),
    ];

    for (index, (zone_id, ttl_secs, refresh_before_secs, expected_error)) in
        cases.into_iter().enumerate()
    {
        let output_path = directory.join(&format!("invalid-{index}.json"));
        let output =
            auth_command(&spec, "users", &output_path, zone_id, ttl_secs, refresh_before_secs)
                .output()
                .unwrap();
        assert_failure(&output, expected_error);
        assert!(!output_path.exists());
    }
}

#[test]
fn existing_output_requires_force_and_force_replaces_it_securely() {
    let directory = TestDir::new();
    let spec = directory.join("spec.yaml");
    let output_path = directory.join("tokens.json");
    write_spec(&spec, "users", 0, 1);
    fs::write(&output_path, b"last-valid-map").unwrap();

    let refused = auth_command(&spec, "users", &output_path, ZONE_ID, 600, 30).output().unwrap();
    assert_failure(&refused, "output already exists; pass --force to replace it");
    assert_eq!(fs::read(&output_path).unwrap(), b"last-valid-map");

    let replaced = auth_command(&spec, "users", &output_path, ZONE_ID, 600, 30)
        .arg("--force")
        .output()
        .unwrap();
    assert_success(&replaced);
    let tokens: BTreeMap<String, String> =
        serde_json::from_slice(&fs::read(&output_path).unwrap()).unwrap();
    assert_eq!(tokens.len(), 1);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(fs::metadata(&output_path).unwrap().permissions().mode() & 0o777, 0o600);
    }
}

#[cfg(unix)]
#[test]
fn watch_writes_the_initial_map_and_exits_cleanly_on_sigterm() {
    let directory = TestDir::new();
    let spec = directory.join("spec.yaml");
    let output_path = directory.join("tokens.json");
    write_spec(&spec, "users", 3, 5);

    let child = auth_command(&spec, "users", &output_path, ZONE_ID, 600, 30)
        .arg("--watch")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let child = ChildGuard::new(child);

    let tokens = wait_for_map(&output_path, Duration::from_secs(5));
    let fields = map_fields(&tokens);
    assert_eq!(tokens.len(), 2);
    assert_eq!(fields.4, fields.3 + 600);

    let termination =
        Command::new("kill").arg("-TERM").arg(child.id().to_string()).status().unwrap();
    assert!(termination.success());
    let output = child.wait_with_output();
    assert_success(&output);

    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stdout.contains("wrote 2 Zone authorization tokens"));
    assert!(stdout.contains("(watching)"));
    assert!(!stdout.contains(TEST_MNEMONIC));
    assert!(!stderr.contains(TEST_MNEMONIC));
    for token in tokens.values() {
        assert!(!stdout.contains(token));
        assert!(!stderr.contains(token));
    }
}

fn auth_command(
    spec: &Path,
    pool: &str,
    output: &Path,
    zone_id: u32,
    ttl_secs: u64,
    refresh_before_secs: u64,
) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_txgen-tempo"));
    command
        .arg("auth-token-map")
        .arg("--spec")
        .arg(spec)
        .arg("--pool")
        .arg(pool)
        .arg("--zone-id")
        .arg(zone_id.to_string())
        .arg("--chain-id")
        .arg(CHAIN_ID.to_string())
        .arg("--ttl-secs")
        .arg(ttl_secs.to_string())
        .arg("--refresh-before-secs")
        .arg(refresh_before_secs.to_string())
        .arg("--output")
        .arg(output)
        .env(MNEMONIC_ENV, TEST_MNEMONIC);
    command
}

fn write_spec(path: &Path, pool: &str, start: u32, end: u32) {
    fs::write(
        path,
        format!(
            "chain_id: 1\naccounts:\n  {pool}:\n    mnemonic: \"${{{MNEMONIC_ENV}}}\"\n    range: [{start}, {end}]\n"
        ),
    )
    .unwrap();
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn assert_failure(output: &Output, expected_error: &str) {
    assert!(
        !output.status.success(),
        "command unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(expected_error),
        "expected stderr to contain {expected_error:?}\nactual stderr:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );
}

fn is_lower_hex(value: &str) -> bool {
    value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn wait_for_map(path: &Path, timeout: Duration) -> BTreeMap<String, String> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(bytes) = fs::read(path) &&
            let Ok(map) = serde_json::from_slice::<BTreeMap<String, String>>(&bytes) &&
            !map.is_empty()
        {
            return map;
        }
        assert!(Instant::now() < deadline, "timed out waiting for token map");
        thread::sleep(Duration::from_millis(25));
    }
}

fn map_fields(map: &BTreeMap<String, String>) -> (u8, u32, u64, u64, u64) {
    let token = hex::decode(map.values().next().unwrap()).unwrap();
    let fields = parse_token_fields(&token[SIGNATURE_LEN..]).unwrap();
    (fields.version, fields.zone_id, fields.chain_id, fields.issued_at, fields.expires_at)
}
