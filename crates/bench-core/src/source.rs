//! Transaction sources for bench.
//!
//! Sources produce [`GeneratedTx`] items from various inputs:
//! - file (reads NDJSON from a file)
//! - stdin (reads NDJSON from stdin)

use eyre::{Context, Result};
use std::{io::BufRead, path::Path};
use tokio::io::{AsyncBufReadExt, BufReader};
use txgen_core::{dedup_scheduling_keys, GeneratedTx};

fn parse_transaction(line: &str) -> Result<GeneratedTx> {
    let mut transaction: GeneratedTx =
        serde_json::from_str(line).context("failed to parse NDJSON line")?;
    transaction.submission_keys = dedup_scheduling_keys(transaction.submission_keys);
    transaction.inclusion_keys = dedup_scheduling_keys(transaction.inclusion_keys);
    if transaction.submission_keys.is_empty() && transaction.inclusion_keys.is_empty() {
        eyre::bail!("transactions must have at least one submission or inclusion key");
    }
    Ok(transaction)
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
            Some(Ok(line)) => Ok(Some(parse_transaction(&line)?)),
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

        Ok(Some(parse_transaction(&self.line_buf)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use txgen_core::{SchedulingKey, TxPhase};

    #[test]
    fn parses_submission_and_inclusion_keys() {
        let generated = parse_transaction(
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

        assert_eq!(generated.phase, TxPhase::Workload);
        assert_eq!(generated.sender, None);
        assert_eq!(generated.submission_keys, vec![SchedulingKey::from([0x11; 20])]);
        assert_eq!(generated.inclusion_keys, vec![SchedulingKey::from([0x22; 20])]);
    }

    #[test]
    fn parses_sender_metadata() {
        let generated = parse_transaction(
            r#"{
                "raw": "0x02f870",
                "sender": "0x3333333333333333333333333333333333333333",
                "submission_keys": [
                    "0x1111111111111111111111111111111111111111"
                ]
            }"#,
        )
        .unwrap();

        assert_eq!(generated.sender, Some(alloy_primitives::Address::repeat_byte(0x33)));
    }
}
