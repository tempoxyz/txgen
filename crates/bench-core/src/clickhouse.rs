//! Shared ClickHouse HTTP client.

use eyre::{bail, Context, Result};
use serde::Serialize;
use std::{fmt, time::Duration};

/// Synchronous JSONEachRow client for ClickHouse's HTTP interface.
///
/// Inserts must be invoked from a multi-threaded Tokio runtime so the HTTP
/// future can run without blocking a runtime worker.
#[derive(Clone)]
pub struct ClickHouseClient {
    url: reqwest::Url,
    database: String,
    user: Option<String>,
    password: Option<String>,
    client: reqwest::Client,
}

impl fmt::Debug for ClickHouseClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClickHouseClient")
            .field("url", &self.url.origin().ascii_serialization())
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "[REDACTED]"))
            .finish_non_exhaustive()
    }
}

impl ClickHouseClient {
    /// Create a client using ClickHouse connection settings from the environment.
    ///
    /// `CLICKHOUSE_DATABASE` defaults to `default`. Authentication is read from
    /// `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD` when present.
    pub fn from_env(url: &str) -> Result<Self> {
        let database =
            std::env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".to_string());
        let user = std::env::var("CLICKHOUSE_USER").ok();
        let password = std::env::var("CLICKHOUSE_PASSWORD").ok();
        Self::new(url, database, user, password)
    }

    /// Create a client with explicit ClickHouse connection settings.
    pub fn new(
        url: impl Into<String>,
        database: impl Into<String>,
        user: Option<String>,
        password: Option<String>,
    ) -> Result<Self> {
        let url = url.into();
        let trimmed_url = url.trim();
        if trimmed_url.is_empty() {
            bail!("ClickHouse endpoint must not be empty");
        }

        let mut parsed = reqwest::Url::parse(trimmed_url).context("invalid ClickHouse endpoint")?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            bail!("ClickHouse endpoint must be an HTTP or HTTPS URL");
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            bail!(
                "ClickHouse endpoint must not contain credentials; use CLICKHOUSE_USER and CLICKHOUSE_PASSWORD"
            );
        }
        if parsed.query().is_some() || parsed.fragment().is_some() {
            bail!("ClickHouse endpoint must not contain a query string or fragment");
        }
        let path = parsed.path().trim_end_matches('/').to_string();
        parsed.set_path(if path.is_empty() { "/" } else { &path });

        let database = database.into();
        validate_identifier("database", &database)?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .context("failed to create HTTP client")?;

        Ok(Self { url: parsed, database, user, password, client })
    }

    /// Return the ClickHouse database used by this client.
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Return a log-safe endpoint containing only the URL origin.
    pub fn endpoint_origin(&self) -> String {
        self.url.origin().ascii_serialization()
    }

    /// Insert rows into a table using ClickHouse's `FORMAT JSONEachRow` protocol.
    pub fn insert_rows<T: Serialize>(&self, table: &str, rows: &[T]) -> Result<()> {
        self.insert_rows_with_mode(table, rows, false)
    }

    /// Insert rows and wait until ClickHouse has committed them.
    ///
    /// Use this when a later insert acts as a visibility marker for this batch.
    pub fn insert_rows_synchronous<T: Serialize>(&self, table: &str, rows: &[T]) -> Result<()> {
        self.insert_rows_with_mode(table, rows, true)
    }

    fn insert_rows_with_mode<T: Serialize>(
        &self,
        table: &str,
        rows: &[T],
        synchronous: bool,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        validate_identifier("table", table)?;

        let mut body = String::new();
        for row in rows {
            body.push_str(
                &serde_json::to_string(row)
                    .wrap_err_with(|| format!("failed to serialize row for {table}"))?,
            );
            body.push('\n');
        }

        let query = format!("INSERT INTO {}.{} FORMAT JSONEachRow", self.database, table);
        let mut url = self.url.clone();
        url.query_pairs_mut().append_pair("query", &query);
        if synchronous {
            // A synchronous acknowledgement is required when callers use a
            // later insert as a visibility marker for earlier table writes.
            url.query_pairs_mut()
                .append_pair("async_insert", "0")
                .append_pair("wait_for_async_insert", "1");
        }

        let rt = tokio::runtime::Handle::try_current()
            .context("ClickHouse inserts require a Tokio runtime")?;
        if !matches!(rt.runtime_flavor(), tokio::runtime::RuntimeFlavor::MultiThread) {
            bail!("ClickHouse inserts require a multi-threaded Tokio runtime");
        }
        let mut req = self.client.post(url).header("Content-Type", "application/json");
        if let Some(ref user) = self.user {
            req = req.header("X-ClickHouse-User", user);
        }
        if let Some(ref password) = self.password {
            req = req.header("X-ClickHouse-Key", password);
        }
        let resp = tokio::task::block_in_place(|| rt.block_on(req.body(body).send()))
            .wrap_err_with(|| format!("failed to insert into {table}"))?;

        let status = resp.status();
        if !status.is_success() {
            let body = tokio::task::block_in_place(|| rt.block_on(resp.text()))
                .unwrap_or_else(|_| "<no body>".to_string());
            bail!("ClickHouse insert into {table} failed (HTTP {status}): {body}");
        }

        tracing::info!(table, rows = rows.len(), "Inserted rows into ClickHouse");
        Ok(())
    }
}

fn validate_identifier(kind: &str, value: &str) -> Result<()> {
    let mut characters = value.chars();
    if !characters
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic()) ||
        !characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        bail!(
            "ClickHouse {kind} must be an unquoted identifier containing only ASCII letters, digits, and underscores"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::mpsc,
        thread,
    };

    #[derive(Serialize)]
    struct TestRow<'a> {
        id: u64,
        name: &'a str,
    }

    fn serve_once(status: &str, response_body: &str) -> (String, mpsc::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let status = status.to_string();
        let response_body = response_body.to_string();
        let (sender, receiver) = mpsc::channel();

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4096];
            let mut expected_len = None;

            loop {
                let read = stream.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);

                if let Some(header_end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    let header_end = header_end + 4;
                    let headers = String::from_utf8_lossy(&request[..header_end]);
                    let content_length = headers.lines().find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    });
                    expected_len = Some(header_end + content_length.unwrap_or(0));
                }

                if expected_len.is_some_and(|len| request.len() >= len) {
                    break;
                }
            }

            sender.send(String::from_utf8(request).unwrap()).unwrap();
            write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
                response_body.len()
            )
            .unwrap();
        });

        (format!("http://{address}"), receiver)
    }

    #[test]
    fn rejects_invalid_endpoints() {
        assert!(ClickHouseClient::new("", "default", None, None).is_err());
        assert!(ClickHouseClient::new("ftp://example.com", "default", None, None).is_err());
        assert!(ClickHouseClient::new("not a URL", "default", None, None).is_err());
        assert!(ClickHouseClient::new("https://example.com", "txgen-runs", None, None).is_err());
        assert!(ClickHouseClient::new("https://user:secret@example.com", "default", None, None)
            .is_err());
        assert!(ClickHouseClient::new("https://example.com?token=secret", "default", None, None)
            .is_err());
        assert!(ClickHouseClient::new("https://example.com#secret", "default", None, None).is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn inserts_json_each_row_with_auth_headers() {
        let (url, request) = serve_once("200 OK", "");
        let client = ClickHouseClient::new(
            url.clone(),
            "analytics",
            Some("txgen".to_string()),
            Some("secret".to_string()),
        )
        .unwrap();
        assert_eq!(client.database(), "analytics");
        assert_eq!(client.endpoint_origin(), url);
        let debug = format!("{client:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("secret"));

        client
            .insert_rows_synchronous(
                "scenario_steps",
                &[TestRow { id: 1, name: "first" }, TestRow { id: 2, name: "second" }],
            )
            .unwrap();

        let request = request.recv().unwrap();
        let request_lower = request.to_ascii_lowercase();
        assert!(request.contains("analytics.scenario_steps"));
        assert!(request.contains("async_insert=0"));
        assert!(request.contains("wait_for_async_insert=1"));
        assert!(request_lower.contains("x-clickhouse-user: txgen"));
        assert!(request_lower.contains("x-clickhouse-key: secret"));
        assert!(
            request.ends_with("{\"id\":1,\"name\":\"first\"}\n{\"id\":2,\"name\":\"second\"}\n")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn includes_clickhouse_response_in_insert_errors() {
        let (url, request) = serve_once("400 Bad Request", "unknown table");
        let client = ClickHouseClient::new(url, "default", None, None).unwrap();

        let error = client
            .insert_rows("missing", &[TestRow { id: 1, name: "first" }])
            .unwrap_err()
            .to_string();
        let request = request.recv().unwrap();

        assert!(error.contains("ClickHouse insert into missing failed (HTTP 400 Bad Request)"));
        assert!(error.contains("unknown table"));
        assert!(!request.contains("async_insert"));
    }

    #[tokio::test]
    async fn current_thread_runtime_returns_an_error_instead_of_panicking() {
        let client = ClickHouseClient::new("http://127.0.0.1:1", "default", None, None).unwrap();
        let error = client
            .insert_rows("rows", &[TestRow { id: 1, name: "first" }])
            .unwrap_err()
            .to_string();
        assert!(error.contains("multi-threaded Tokio runtime"));
    }
}
