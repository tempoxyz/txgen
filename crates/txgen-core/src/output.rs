use alloy_primitives::Bytes;
use eyre::Result;
use serde::Serialize;
use std::io::Write;

/// A generated transaction ready for output.
#[derive(Debug, Clone)]
pub struct GeneratedTx {
    /// RLP-encoded signed transaction (EIP-2718 envelope).
    pub raw: Bytes,
    /// Scheduling keys (20 bytes each).
    ///
    /// A transaction may carry multiple independent ordering constraints. Bench
    /// must only submit it when none of its keys are active. Transactions that
    /// share any key must be sent sequentially; transactions with disjoint key
    /// sets may be sent in parallel.
    pub scheduling_keys: Vec<[u8; 20]>,
}

/// JSON output format for NDJSON stream.
#[derive(Serialize)]
struct OutputTx<'a> {
    raw: &'a str,
    scheduling_keys: &'a [String],
}

/// Writes generated transactions as newline-delimited JSON.
pub struct NdjsonWriter<W: Write> {
    writer: W,
    count: u64,
    raw_hex: String,
    key_hex: Vec<String>,
}

impl<W: Write> NdjsonWriter<W> {
    /// Create a new NDJSON writer.
    pub fn new(writer: W) -> Self {
        Self { writer, count: 0, raw_hex: String::new(), key_hex: Vec::new() }
    }

    /// Write a generated transaction.
    pub fn write(&mut self, tx: &GeneratedTx) -> Result<()> {
        // Reuse string buffers to avoid allocations
        self.raw_hex.clear();
        self.raw_hex.push_str("0x");
        for byte in tx.raw.iter() {
            use std::fmt::Write;
            write!(self.raw_hex, "{:02x}", byte)?;
        }

        self.key_hex.clear();
        self.key_hex.reserve(tx.scheduling_keys.len());
        for key in &tx.scheduling_keys {
            let mut key_hex = String::with_capacity(42);
            key_hex.push_str("0x");
            for byte in key.iter() {
                use std::fmt::Write;
                write!(key_hex, "{:02x}", byte)?;
            }
            self.key_hex.push(key_hex);
        }

        let out = OutputTx { raw: &self.raw_hex, scheduling_keys: &self.key_hex };

        serde_json::to_writer(&mut self.writer, &out)?;
        self.writer.write_all(b"\n")?;
        self.count += 1;

        Ok(())
    }

    /// Flush the writer.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    /// Get the number of transactions written.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Consume the writer and return the inner writer.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

/// Create a writer for stdout.
pub fn stdout_writer() -> NdjsonWriter<std::io::Stdout> {
    NdjsonWriter::new(std::io::stdout())
}

/// Create a writer for a file.
pub fn file_writer(
    path: &std::path::Path,
) -> Result<NdjsonWriter<std::io::BufWriter<std::fs::File>>> {
    let file = std::fs::File::create(path)?;
    let buf = std::io::BufWriter::new(file);
    Ok(NdjsonWriter::new(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ndjson_output() {
        let mut buf = Vec::new();
        let mut writer = NdjsonWriter::new(&mut buf);

        let tx = GeneratedTx {
            raw: Bytes::from(vec![0x02, 0xf8, 0x70]),
            scheduling_keys: vec![[0xab; 20]],
        };

        writer.write(&tx).unwrap();
        writer.flush().unwrap();

        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("\"raw\":\"0x02f870\""));
        assert!(
            output.contains("\"scheduling_keys\":[\"0xabababababababababababababababababababab\"]")
        );
        assert!(output.ends_with('\n'));
    }

    #[test]
    fn test_count() {
        let mut buf = Vec::new();
        let mut writer = NdjsonWriter::new(&mut buf);

        let tx = GeneratedTx { raw: Bytes::from(vec![0x00]), scheduling_keys: vec![[0x00; 20]] };

        assert_eq!(writer.count(), 0);
        writer.write(&tx).unwrap();
        assert_eq!(writer.count(), 1);
        writer.write(&tx).unwrap();
        assert_eq!(writer.count(), 2);
    }
}
