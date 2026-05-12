//! Transaction sources for bench.
//!
//! Sources produce [`GeneratedTx`] items from various inputs:
//! - file (reads NDJSON from a file)
//! - stdin (reads NDJSON from stdin)

use alloy_primitives::Bytes;
use eyre::{Context, Result};
use std::{io::BufRead, path::Path};
use tokio::io::{AsyncBufReadExt, BufReader};
use txgen_core::{dedup_scheduling_keys, GeneratedTx, SchedulingKey, TxPhase};

/// A transaction read from a source.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SourceTx {
    /// Stream phase for this transaction.
    #[serde(default)]
    pub phase: TxPhase,
    /// Optional human-readable transaction identifier for diagnostics.
    #[serde(default)]
    pub id: Option<String>,
    /// Raw transaction bytes (hex-encoded with 0x prefix).
    pub raw: String,
    /// Scheduling keys released once RPC submission succeeds (hex-encoded with 0x prefix).
    pub submission_keys: Vec<SchedulingKey>,
    /// Scheduling keys released once a transaction receipt is observed (hex-encoded with 0x
    /// prefix).
    #[serde(default)]
    pub inclusion_keys: Vec<SchedulingKey>,
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

        let submission_keys = dedup_scheduling_keys(self.submission_keys);
        let inclusion_keys = dedup_scheduling_keys(self.inclusion_keys);

        if submission_keys.is_empty() && inclusion_keys.is_empty() {
            eyre::bail!("transactions must have at least one submission or inclusion key");
        }

        Ok(GeneratedTx { phase: self.phase, id: self.id, raw, submission_keys, inclusion_keys })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_submission_and_inclusion_keys() {
        let source_tx: SourceTx = serde_json::from_str(
            r#"{
                "raw": "0x02f870",
                "submission_keys": [
                    "0x1111111111111111111111111111111111111111",
                    "0x1111111111111111111111111111111111111111"
                ],
                "inclusion_keys": [
                    "0x2222222222222222222222222222222222222222"
                ]
            }"#,
        )
        .unwrap();

        let generated = source_tx.into_generated_tx().unwrap();
        assert_eq!(generated.phase, TxPhase::Workload);
        assert_eq!(generated.submission_keys, vec![SchedulingKey::from([0x11; 20])]);
        assert_eq!(generated.inclusion_keys, vec![SchedulingKey::from([0x22; 20])]);
    }
}
