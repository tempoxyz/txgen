//! Transaction sources for bench.
//!
//! Sources produce [`GeneratedTx`] items from various inputs:
//! - txgen subprocess (spawns txgen and reads NDJSON from stdout)
//! - file (reads NDJSON from a file)
//! - stdin (reads NDJSON from stdin)

use alloy_primitives::Bytes;
use eyre::{Context, Result};
use std::{io::BufRead, path::Path, process::Stdio};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
};
use txgen_core::GeneratedTx;

/// A transaction read from a source.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SourceTx {
    /// Raw transaction bytes (hex-encoded with 0x prefix).
    pub raw: String,
    /// Scheduling keys (hex-encoded with 0x prefix).
    pub scheduling_keys: Vec<String>,
}

impl SourceTx {
    /// Parse into a [`GeneratedTx`].
    pub fn into_generated_tx(self) -> Result<GeneratedTx> {
        let raw = self
            .raw
            .strip_prefix("0x")
            .unwrap_or(&self.raw)
            .parse::<Bytes>()
            .context("invalid raw tx hex")?;

        if self.scheduling_keys.is_empty() {
            eyre::bail!("scheduling_keys must not be empty");
        }

        let mut scheduling_keys = Vec::with_capacity(self.scheduling_keys.len());
        for key in self.scheduling_keys {
            let key_bytes =
                hex::decode(key.strip_prefix("0x").unwrap_or(&key)).context("invalid key hex")?;

            if key_bytes.len() != 20 {
                eyre::bail!("scheduling key must be 20 bytes, got {}", key_bytes.len());
            }

            let mut parsed = [0u8; 20];
            parsed.copy_from_slice(&key_bytes);
            if !scheduling_keys.contains(&parsed) {
                scheduling_keys.push(parsed);
            }
        }

        Ok(GeneratedTx { raw, scheduling_keys })
    }
}

/// Transaction source trait.
pub trait TxSource {
    /// Get the next transaction from this source.
    ///
    /// Returns `None` when the source is exhausted.
    fn next_tx(&mut self) -> impl std::future::Future<Output = Result<Option<GeneratedTx>>> + Send;
}

/// Source that reads transactions from a file.
pub struct FileSource {
    lines: std::io::Lines<std::io::BufReader<std::fs::File>>,
}

impl FileSource {
    /// Create a new file source.
    pub fn new(path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path).context("failed to open file")?;
        let reader = std::io::BufReader::new(file);
        Ok(Self { lines: reader.lines() })
    }
}

impl TxSource for FileSource {
    async fn next_tx(&mut self) -> Result<Option<GeneratedTx>> {
        match self.lines.next() {
            Some(Ok(line)) => {
                let source_tx: SourceTx =
                    serde_json::from_str(&line).context("failed to parse NDJSON line")?;
                Ok(Some(source_tx.into_generated_tx()?))
            }
            Some(Err(e)) => Err(e).context("failed to read line"),
            None => Ok(None),
        }
    }
}

/// Source that reads transactions from stdin.
pub struct StdinSource {
    reader: BufReader<tokio::io::Stdin>,
    line_buf: String,
}

impl StdinSource {
    /// Create a new stdin source.
    pub fn new() -> Self {
        Self { reader: BufReader::new(tokio::io::stdin()), line_buf: String::new() }
    }
}

impl Default for StdinSource {
    fn default() -> Self {
        Self::new()
    }
}

impl TxSource for StdinSource {
    async fn next_tx(&mut self) -> Result<Option<GeneratedTx>> {
        self.line_buf.clear();
        let bytes_read = self
            .reader
            .read_line(&mut self.line_buf)
            .await
            .context("failed to read line from stdin")?;

        if bytes_read == 0 {
            return Ok(None);
        }

        let source_tx: SourceTx =
            serde_json::from_str(&self.line_buf).context("failed to parse NDJSON line")?;
        Ok(Some(source_tx.into_generated_tx()?))
    }
}

/// Source that spawns txgen as a subprocess and reads from its stdout.
pub struct TxgenSource {
    child: Child,
    reader: BufReader<tokio::process::ChildStdout>,
    line_buf: String,
}

impl TxgenSource {
    /// Spawn txgen with the given arguments.
    pub async fn spawn(txgen_bin: &str, args: &[String]) -> Result<Self> {
        let mut child = Command::new(txgen_bin)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .context("failed to spawn txgen")?;

        let stdout = child.stdout.take().ok_or_else(|| eyre::eyre!("txgen stdout not captured"))?;

        let reader = BufReader::new(stdout);

        Ok(Self { child, reader, line_buf: String::new() })
    }

    /// Wait for the txgen process to exit.
    pub async fn wait(&mut self) -> Result<std::process::ExitStatus> {
        self.child.wait().await.context("failed to wait for txgen")
    }
}

impl TxSource for TxgenSource {
    async fn next_tx(&mut self) -> Result<Option<GeneratedTx>> {
        self.line_buf.clear();
        let bytes_read = self
            .reader
            .read_line(&mut self.line_buf)
            .await
            .context("failed to read from txgen stdout")?;

        if bytes_read == 0 {
            return Ok(None);
        }

        let source_tx: SourceTx =
            serde_json::from_str(&self.line_buf).context("failed to parse NDJSON from txgen")?;
        Ok(Some(source_tx.into_generated_tx()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_multiple_scheduling_keys() {
        let source_tx: SourceTx = serde_json::from_str(
            r#"{
                "raw": "0x02f870",
                "scheduling_keys": [
                    "0x1111111111111111111111111111111111111111",
                    "0x2222222222222222222222222222222222222222",
                    "0x1111111111111111111111111111111111111111"
                ]
            }"#,
        )
        .unwrap();

        let generated = source_tx.into_generated_tx().unwrap();
        assert_eq!(generated.scheduling_keys.len(), 2);
        assert_eq!(generated.scheduling_keys[0], [0x11; 20]);
        assert_eq!(generated.scheduling_keys[1], [0x22; 20]);
    }
}
