use alloy_eips::BlockNumberOrTag;
use alloy_network::AnyNetwork;
use alloy_provider::{DynProvider, Provider};
use eyre::{Context, Result};
use flate2::{write::GzEncoder, Compression};
use serde_json::{Map, Value};
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
    time::Duration,
};
use tokio::{sync::oneshot, task::JoinHandle};

pub(crate) struct BlockArtifactRecorder {
    kind: BlockArtifactKind,
    stop_tx: Option<oneshot::Sender<u64>>,
    handle: Option<JoinHandle<Result<BlockArtifactRecorderStats>>>,
}

#[derive(Debug)]
pub(crate) struct BlockArtifactRecorderStats {
    pub(crate) blocks_written: u64,
    pub(crate) first_block: Option<u64>,
    pub(crate) last_block: Option<u64>,
}

#[derive(Clone, Copy)]
pub(crate) enum BlockArtifactKind {
    BlockAccessList,
    TrieWitness,
}

impl BlockArtifactKind {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::BlockAccessList => "block_access_list",
            Self::TrieWitness => "trie_witness",
        }
    }

    const fn field_name(self) -> &'static str {
        match self {
            Self::BlockAccessList => "block_access_list",
            Self::TrieWitness => "trie_witness",
        }
    }
}

type BlockArtifactWriter = GzEncoder<BufWriter<File>>;

impl BlockArtifactRecorder {
    pub(crate) async fn start(
        provider: DynProvider<AnyNetwork>,
        start_block: u64,
        path: PathBuf,
        kind: BlockArtifactKind,
    ) -> Result<Self> {
        let file =
            OpenOptions::new().create(true).append(true).open(&path).wrap_err_with(|| {
                format!("failed to open {} output file {}", kind.label(), path.display())
            })?;
        let writer = GzEncoder::new(BufWriter::new(file), Compression::default());
        let (stop_tx, stop_rx) = oneshot::channel();
        let handle =
            tokio::spawn(record_block_artifacts(provider, writer, start_block, stop_rx, kind));

        tracing::info!(
            artifact = kind.label(),
            path = %path.display(),
            start_block,
            "Started block artifact recorder"
        );

        Ok(Self { kind, stop_tx: Some(stop_tx), handle: Some(handle) })
    }

    pub(crate) async fn stop_at(mut self, end_block: u64) -> Result<BlockArtifactRecorderStats> {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(end_block);
        }

        let handle = self
            .handle
            .take()
            .ok_or_else(|| eyre::eyre!("{} recorder task missing", self.kind.label()))?;

        handle
            .await
            .wrap_err_with(|| format!("{} recorder task failed to join", self.kind.label()))?
            .wrap_err_with(|| format!("{} recorder failed", self.kind.label()))
    }
}

impl Drop for BlockArtifactRecorder {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

async fn record_block_artifacts(
    provider: DynProvider<AnyNetwork>,
    mut writer: BlockArtifactWriter,
    start_block: u64,
    mut stop_rx: oneshot::Receiver<u64>,
    kind: BlockArtifactKind,
) -> Result<BlockArtifactRecorderStats> {
    let mut next_block = start_block.saturating_add(1);
    let mut stop_at = None;
    let mut stats =
        BlockArtifactRecorderStats { blocks_written: 0, first_block: None, last_block: None };

    loop {
        if let Some(target_block) = stop_at &&
            next_block > target_block
        {
            finish_block_artifact_output(&mut writer, kind)?;
            return Ok(stats);
        }

        let latest_block =
            provider.get_block_number().await.wrap_err("failed to get latest block number")?;
        let target_block = stop_at.unwrap_or(latest_block);
        let fetch_through = latest_block.min(target_block);

        while next_block <= fetch_through {
            let artifact = fetch_block_artifact(&provider, next_block, kind).await?;
            write_block_artifact(&mut writer, next_block, kind.field_name(), artifact)?;
            stats.blocks_written += 1;
            stats.first_block.get_or_insert(next_block);
            stats.last_block = Some(next_block);
            tracing::debug!(artifact = kind.label(), block = next_block, "Recorded block artifact");
            next_block += 1;
        }

        if let Some(target_block) = stop_at &&
            next_block > target_block
        {
            finish_block_artifact_output(&mut writer, kind)?;
            return Ok(stats);
        }

        tokio::select! {
            result = &mut stop_rx, if stop_at.is_none() => {
                stop_at = Some(result.unwrap_or_else(|_| next_block.saturating_sub(1)));
            }
            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
        }
    }
}

fn finish_block_artifact_output(
    writer: &mut BlockArtifactWriter,
    kind: BlockArtifactKind,
) -> Result<()> {
    writer
        .try_finish()
        .wrap_err_with(|| format!("failed to finish {} gzip output", kind.label()))?;
    writer
        .get_mut()
        .flush()
        .wrap_err_with(|| format!("failed to flush {} output", kind.label()))?;
    Ok(())
}

async fn fetch_block_artifact(
    provider: &DynProvider<AnyNetwork>,
    block_number: u64,
    kind: BlockArtifactKind,
) -> Result<Value> {
    match kind {
        BlockArtifactKind::BlockAccessList => {
            let block_access_list = provider
                .get_block_access_list_by_number(BlockNumberOrTag::Number(block_number))
                .await
                .wrap_err_with(|| format!("failed to fetch block access list {block_number}"))?
                .ok_or_else(|| {
                    eyre::eyre!("block access list not found for block {block_number}")
                })?;
            serde_json::to_value(block_access_list)
                .wrap_err_with(|| format!("failed to serialize block access list {block_number}"))
        }
        BlockArtifactKind::TrieWitness => provider
            .client()
            .request("debug_executionWitness", (BlockNumberOrTag::Number(block_number),))
            .await
            .wrap_err_with(|| format!("failed to fetch trie witness {block_number}")),
    }
}

fn write_block_artifact(
    writer: &mut BlockArtifactWriter,
    block_number: u64,
    field_name: &str,
    artifact: Value,
) -> Result<()> {
    let mut line = Map::with_capacity(2);
    line.insert("number".to_string(), Value::from(block_number));
    line.insert(field_name.to_string(), artifact);

    let mut buffer = Vec::new();
    serde_json::to_writer(&mut buffer, &line)
        .wrap_err_with(|| format!("failed to serialize {field_name} {block_number}"))?;
    buffer.push(b'\n');
    writer
        .write_all(&buffer)
        .wrap_err_with(|| format!("failed to write {field_name} {block_number} to gzip output"))?;
    writer
        .flush()
        .wrap_err_with(|| format!("failed to flush {field_name} {block_number} to gzip output"))?;
    Ok(())
}
