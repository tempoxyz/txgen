//! `bench send-blocks` - Submit blocks via reth Engine API
//!
//! Reads NDJSON `{raw, key, number, timestamp, gas_used, gas_limit, tx_count}`
//! lines from stdin or file (produced by `txgen extract`). Lines may include
//! optional RLP-encoded `bal` bytes. Submits each block via `reth_newPayload`
//! (as `BlockRlp`) and `reth_forkchoiceUpdated`,
//! collecting per-block timing and engine status from [`RethPayloadStatus`].

use crate::{
    metrics_forwarder::{build_metrics_forwarder, finish_metrics_forwarder, push_samples},
    metrics_url::metrics_scraper_configs,
    send::parse_metadata,
    SendBlocksArgs,
};
use alloy_consensus::Header as ConsensusHeader;
use alloy_network::Ethereum;
use alloy_primitives::{Address, Bytes, B256};
use alloy_provider::{ext::TestingApi, Provider, RootProvider};
use alloy_rlp::{Decodable, Header};
use alloy_rpc_types_engine::{
    ExecutionData, ForkchoiceState, JwtSecret, PayloadAttributes, TestingBuildBlockRequestV1,
};
use alloy_transport_http::{AuthLayer, Http, HyperClient};
use bench_core::{
    parse_reporters, start_scrapers, BigBlockData, BlockStats, ConsoleReporter, FinalReport,
    ProgressState, Reporter, RethApi, RethNewPayloadInput, RunClock, RunStats, Sample, SampleStore,
    WaitForPersistence,
};
use eyre::{Context, Result};
use std::{
    collections::BTreeMap,
    io::BufRead,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::io::{AsyncBufReadExt, BufReader};

/// NDJSON line from the block source (`txgen extract` output).
#[derive(serde::Deserialize)]
pub(crate) struct BlockLine {
    /// RLP-encoded block bytes (hex with 0x prefix).
    raw: Bytes,
    /// Optional RLP-encoded block access list bytes (hex with 0x prefix).
    #[serde(default)]
    bal: Option<Bytes>,
    /// Block hash.
    key: B256,
    /// Block number.
    number: u64,
    /// Block timestamp in seconds.
    timestamp: u64,
    /// Gas used by the block.
    gas_used: u64,
    /// Gas limit of the block.
    gas_limit: u64,
    /// Number of transactions in the block.
    tx_count: usize,
}

/// NDJSON line accepted by `bench send-blocks`.
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum InputLine {
    /// Raw RLP block line produced by `txgen extract`.
    RawBlock(BlockLine),
    /// Big-block payload line produced by `txgen extract-big-blocks`.
    BigBlock(Box<BigBlockData<ExecutionData>>),
}

struct ReorgState {
    depth: usize,
    gap: usize,
    gap_remaining: usize,
    pending: Vec<BlockLine>,
}

enum ReorgAction {
    Pending,
    Canonical(Vec<BlockLine>),
    Batch(Vec<BlockLine>),
}

impl ReorgState {
    const fn new(depth: usize, gap: usize) -> Self {
        Self { depth, gap, gap_remaining: 0, pending: Vec::new() }
    }

    fn push(&mut self, block: BlockLine) -> ReorgAction {
        if self.gap_remaining > 0 {
            self.gap_remaining -= 1;
            return ReorgAction::Canonical(vec![block]);
        }

        self.pending.push(block);
        if self.pending.len() < self.depth {
            return ReorgAction::Pending;
        }

        self.gap_remaining = self.gap;
        ReorgAction::Batch(std::mem::take(&mut self.pending))
    }

    fn finish(&mut self) -> ReorgAction {
        ReorgAction::Canonical(std::mem::take(&mut self.pending))
    }
}

struct ProcessingState<'a> {
    collector: &'a mut MetricsCollector,
    reorg_state: Option<&'a mut ReorgState>,
    persistence_policy: &'a WaitForPersistence,
}

const REORG_NON_BLOB_TX_DROP_INTERVAL: usize = 10;

pub async fn execute(args: SendBlocksArgs) -> Result<()> {
    let jwt_secret_hex =
        tokio::fs::read_to_string(&args.jwt_secret).await.wrap_err("failed to read JWT secret")?;
    let jwt_secret =
        JwtSecret::from_hex(jwt_secret_hex.trim()).wrap_err("invalid JWT secret hex")?;

    let metadata = parse_metadata(&args.metadata)?;
    let scraper_configs =
        metrics_scraper_configs(&args.metrics_url, Duration::from_millis(args.scrape_interval_ms))?;
    let persistence_policy = args.wait_for_persistence;
    tracing::info!(
        engine = %args.engine,
        input = args.input.as_ref().map_or("<stdin>", |p| p.to_str().unwrap_or("?")),
        wait_for_persistence = ?persistence_policy,
        wait_time_ms = args.wait_time.map(|d| d.as_millis()),
        reorg_depth = args.reorg,
        reorg_gap = args.reorg.map(|_| args.reorg_gap),
        rpc = %args.rpc,
        "Starting block submission"
    );

    let hyper_client = HyperClient::new().layer(AuthLayer::new(jwt_secret));
    let transport = Http::with_client(hyper_client, args.engine.parse()?);
    let provider = RootProvider::<Ethereum>::new(alloy_rpc_client::RpcClient::new(transport, true));
    let testing_provider =
        RootProvider::<Ethereum>::new_http(args.rpc.parse().wrap_err("invalid RPC URL")?);

    let mut reporters = parse_reporters(&args.reports, "send-blocks", &metadata)?;
    if reporters.is_empty() {
        reporters.push(Box::new(ConsoleReporter::stderr(false)));
    }

    let clock = RunClock::new();
    let store = SampleStore::with_labels(metadata.clone())?;
    let counters = Arc::new(BlockCounters::default());
    let metrics_forwarder =
        build_metrics_forwarder(args.metrics_forward.as_deref(), &metadata, &scraper_configs)?;

    // Start background scraper if metrics URL is configured.
    let scraper_handles = if !scraper_configs.is_empty() {
        let snap_counters = counters.clone();
        let snap_clock = clock.clone();
        let callback: bench_core::SampleCallback =
            Arc::new(move || snap_counters.snapshot_samples(&snap_clock));
        let forwarder_handle = metrics_forwarder.as_ref().map(|f| f.handle());

        start_scrapers(&scraper_configs, clock.clone(), store.clone(), callback, forwarder_handle)
    } else {
        Vec::new()
    };

    let mut collector = MetricsCollector::new(counters);
    let mut reorg_state = args.reorg.map(|depth| ReorgState::new(depth, args.reorg_gap));
    let start = Instant::now();

    if let Some(ref path) = args.input {
        let file = std::fs::File::open(path).wrap_err("failed to open input file")?;
        let reader = std::io::BufReader::new(file);

        for line in reader.lines() {
            let line = line.wrap_err("failed to read line")?;
            let input = parse_input_line(&line)?;

            process_input_and_wait(
                &provider,
                &testing_provider,
                input,
                ProcessingState {
                    collector: &mut collector,
                    reorg_state: reorg_state.as_mut(),
                    persistence_policy: &persistence_policy,
                },
                args.wait_time,
                start,
                &mut reporters,
            )
            .await?;
        }
    } else {
        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin);
        let mut line_buf = String::new();

        loop {
            line_buf.clear();
            let bytes_read =
                reader.read_line(&mut line_buf).await.wrap_err("failed to read from stdin")?;
            if bytes_read == 0 {
                break;
            }

            let input = parse_input_line(&line_buf)?;

            process_input_and_wait(
                &provider,
                &testing_provider,
                input,
                ProcessingState {
                    collector: &mut collector,
                    reorg_state: reorg_state.as_mut(),
                    persistence_policy: &persistence_policy,
                },
                args.wait_time,
                start,
                &mut reporters,
            )
            .await?;
        }
    }

    if let Some(reorg_state) = reorg_state.as_mut() {
        process_reorg_action(
            &provider,
            &testing_provider,
            reorg_state,
            &mut collector,
            &persistence_policy,
            args.wait_time,
            start,
            &mut reporters,
            None,
        )
        .await?;
    }

    // Stop the scraper before finalizing.
    if !scraper_handles.is_empty() {
        tracing::info!(
            scrapers = scraper_handles.len(),
            scrapes = scraper_handles.iter().map(|h| h.scrape_count()).sum::<u64>(),
            errors = scraper_handles.iter().map(|h| h.error_count()).sum::<u64>(),
            "Stopping metrics scrapers"
        );
        for handle in scraper_handles {
            handle.stop().await;
        }
    }

    // Push a final snapshot so counter totals are captured even if the
    // last scraper tick fired before the final block was submitted.
    let final_samples = collector.final_snapshot(&clock);
    let forwarder_handle = metrics_forwarder.as_ref().map(|f| f.handle());
    push_samples(&store, forwarder_handle.as_ref(), final_samples).await?;

    let sample_archive = store.finish().await?;

    let blocks = std::mem::take(&mut collector.blocks);
    let run_stats = RunStats::from_blocks_wall_time(&blocks, start.elapsed());

    for block in &blocks {
        for reporter in reporters.iter_mut() {
            reporter.on_block(block)?;
        }
    }

    let report = FinalReport {
        metadata: metadata.clone(),
        sample_archive: Some(sample_archive),
        blocks,
        run_stats: Some(run_stats),
        ..Default::default()
    };

    let mut finalize_result = Ok(());
    for reporter in reporters.iter_mut() {
        if let Err(err) = reporter.finalize(&report) {
            finalize_result = Err(err);
            break;
        }
    }

    let forwarder_result = finish_metrics_forwarder(metrics_forwarder).await;

    finalize_result?;
    forwarder_result?;
    Ok(())
}

fn parse_input_line(line: &str) -> Result<InputLine> {
    serde_json::from_str(line.trim()).wrap_err("failed to parse NDJSON line")
}

fn report_progress(
    collector: &MetricsCollector,
    start: Instant,
    reporters: &mut [Box<dyn Reporter>],
) -> Result<()> {
    let state = ProgressState {
        sent: collector.blocks_submitted(),
        success: collector.blocks_success(),
        failed: collector.blocks_failed(),
        elapsed: start.elapsed(),
        max_concurrent: 0,
        target_tps: None,
        unit: "block",
    };
    for reporter in reporters.iter_mut() {
        reporter.on_progress(&state)?;
    }
    Ok(())
}

async fn process_input_and_wait(
    provider: &(impl Provider + RethApi<Ethereum>),
    testing_provider: &(impl Provider + TestingApi<Ethereum>),
    input: InputLine,
    state: ProcessingState<'_>,
    wait_time: Option<Duration>,
    start: Instant,
    reporters: &mut [Box<dyn Reporter>],
) -> Result<()> {
    let ProcessingState { collector, reorg_state, persistence_policy } = state;
    if let Some(reorg_state) = reorg_state {
        let InputLine::RawBlock(block) = input else {
            eyre::bail!("--reorg is only supported for raw RLP block input");
        };
        return process_reorg_action(
            provider,
            testing_provider,
            reorg_state,
            collector,
            persistence_policy,
            wait_time,
            start,
            reporters,
            Some(block),
        )
        .await;
    }

    let block_start = Instant::now();

    match &input {
        InputLine::RawBlock(block) => {
            process_block(provider, block, collector, None, persistence_policy).await?
        }
        InputLine::BigBlock(big_block) => {
            process_big_block(provider, big_block, collector, persistence_policy).await?
        }
    }
    report_progress(collector, start, reporters)?;
    wait_for_next_block(block_start, wait_time).await;
    Ok(())
}

async fn wait_for_next_block(block_start: Instant, wait_time: Option<Duration>) {
    if let Some(wait_time) = wait_time {
        let remaining = wait_time.saturating_sub(block_start.elapsed());
        if !remaining.is_zero() {
            tokio::time::sleep(remaining).await;
        }
    }
}

async fn process_block(
    provider: &(impl Provider + RethApi<Ethereum>),
    block: &BlockLine,
    collector: &mut MetricsCollector,
    forkchoice_anchor: Option<B256>,
    persistence_policy: &WaitForPersistence,
) -> Result<()> {
    let input = RethNewPayloadInput::BlockRlp { block: block.raw.clone(), bal: block.bal.clone() };
    let wait = persistence_policy.should_wait(collector.blocks_submitted());

    let new_payload_start = Instant::now();
    let payload_status =
        provider.reth_new_payload(input, wait).await.wrap_err("reth_newPayload failed")?;
    let new_payload_latency = new_payload_start.elapsed();

    if !payload_status.status.is_valid() {
        collector.record_failure();
        eyre::bail!(
            "reth_newPayload returned non-VALID status for block {}: {:?}",
            block.number,
            payload_status.status,
        );
    }

    let (safe_block_hash, finalized_block_hash) = if let Some(anchor) = forkchoice_anchor {
        (anchor, anchor)
    } else {
        let safe_hash = collector.prev_block_hash.unwrap_or(block.key);
        (safe_hash, *collector.finalized_hash.get_or_insert(safe_hash))
    };

    let forkchoice_state =
        ForkchoiceState { head_block_hash: block.key, safe_block_hash, finalized_block_hash };
    let fcu_start = Instant::now();
    let fcu_result = provider
        .reth_forkchoice_updated(forkchoice_state)
        .await
        .wrap_err("reth_forkchoiceUpdated failed")?;
    let fcu_latency = fcu_start.elapsed();

    if !fcu_result.is_valid() {
        collector.record_failure();
        eyre::bail!(
            "reth_forkchoiceUpdated returned non-VALID status for block {}: {:?}",
            block.number,
            fcu_result.payload_status,
        );
    }

    let total_latency = new_payload_latency + fcu_latency;
    let payload_status_str = payload_status.status.status.to_string();

    let timestamp_ms = block.timestamp * 1000;
    let block_time_ms = collector.prev_timestamp_ms.map(|prev| timestamp_ms.saturating_sub(prev));
    collector.prev_timestamp_ms = Some(timestamp_ms);

    let block_stats = BlockStats {
        number: block.number,
        timestamp_ms,
        tx_count: block.tx_count,
        gas_used: block.gas_used,
        gas_limit: block.gas_limit,
        block_time_ms,
        new_payload_ms: Some(new_payload_latency.as_millis() as u64),
        forkchoice_updated_ms: Some(fcu_latency.as_millis() as u64),
        new_payload_server_latency_us: Some(payload_status.latency_us),
        persistence_wait_us: payload_status.persistence_wait_us,
        execution_cache_wait_us: payload_status.execution_cache_wait_us,
        sparse_trie_wait_us: payload_status.sparse_trie_wait_us,
    };

    collector.record_success(block_stats);

    tracing::info!(
        block = block.number,
        txs = block.tx_count,
        gas = block.gas_used,
        new_payload_ms = new_payload_latency.as_millis(),
        forkchoice_updated_ms = fcu_latency.as_millis(),
        total_ms = total_latency.as_millis(),
        new_payload_server_latency_us = payload_status.latency_us,
        status = %payload_status_str,
        "Submitted block"
    );

    collector.prev_block_hash = Some(block.key);

    if block.number.is_multiple_of(32) {
        collector.finalized_hash = Some(block.key);
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_reorg_action(
    provider: &(impl Provider + RethApi<Ethereum>),
    testing_provider: &(impl Provider + TestingApi<Ethereum>),
    state: &mut ReorgState,
    collector: &mut MetricsCollector,
    persistence_policy: &WaitForPersistence,
    wait_time: Option<Duration>,
    start: Instant,
    reporters: &mut [Box<dyn Reporter>],
    block: Option<BlockLine>,
) -> Result<()> {
    let action = match block {
        Some(block) => state.push(block),
        None => state.finish(),
    };
    let (blocks, forkchoice_anchor) = match action {
        ReorgAction::Pending => return Ok(()),
        ReorgAction::Canonical(blocks) => (blocks, None),
        ReorgAction::Batch(blocks) => {
            let first = blocks.first().expect("reorg batch must not be empty");
            let branch_point_hash = extract_block_header_from_block_rlp(first.raw.as_ref())
                .wrap_err_with(|| {
                    format!("failed to extract parent hash from block {}", first.number)
                })?
                .parent_hash;

            tracing::info!(
                reorg_depth = state.depth,
                reorg_gap = state.gap,
                branch_point = %branch_point_hash,
                "Starting reorg batch"
            );

            let mut parent_block_hash = branch_point_hash;
            for (index, block) in blocks.iter().enumerate() {
                let block_start = Instant::now();
                let source_offset = u64::try_from(index).unwrap_or(u64::MAX);
                let wait = persistence_policy
                    .should_wait(collector.blocks_submitted().saturating_add(source_offset));
                parent_block_hash = process_synthetic_block(
                    provider,
                    testing_provider,
                    block,
                    parent_block_hash,
                    branch_point_hash,
                    wait,
                )
                .await?;
                tracing::info!(
                    block = block.number,
                    fork_length = index + 1,
                    reorg_depth = state.depth,
                    branch_point = %branch_point_hash,
                    synthetic_head = %parent_block_hash,
                    "Submitted synthetic fork block"
                );
                wait_for_next_block(block_start, wait_time).await;
            }

            (blocks, Some(branch_point_hash))
        }
    };

    for block in &blocks {
        let block_start = Instant::now();
        process_block(provider, block, collector, forkchoice_anchor, persistence_policy).await?;
        report_progress(collector, start, reporters)?;
        wait_for_next_block(block_start, wait_time).await;
    }

    Ok(())
}

async fn process_synthetic_block(
    provider: &(impl Provider + RethApi<Ethereum>),
    testing_provider: &(impl Provider + TestingApi<Ethereum>),
    block: &BlockLine,
    parent_block_hash: B256,
    branch_point_hash: B256,
    wait: Option<bool>,
) -> Result<B256> {
    let transactions = extract_tx_bytes_from_block_rlp(block.raw.as_ref()).wrap_err_with(|| {
        format!("failed to extract raw transaction bytes from block {}", block.number)
    })?;
    let parent_beacon_block_root = extract_block_header_from_block_rlp(block.raw.as_ref())
        .wrap_err_with(|| {
            format!("failed to extract parent beacon block root from block {}", block.number)
        })?
        .parent_beacon_block_root;
    let payload_attributes = PayloadAttributes {
        timestamp: block.timestamp,
        prev_randao: B256::ZERO,
        suggested_fee_recipient: Address::ZERO,
        withdrawals: None,
        parent_beacon_block_root,
        slot_number: None,
        target_gas_limit: None,
    };
    let parent_beacon_block_root = parent_beacon_block_root.unwrap_or_default();
    let request = TestingBuildBlockRequestV1 {
        parent_block_hash,
        payload_attributes,
        transactions,
        extra_data: Some(Bytes::new()),
    };

    let envelope = testing_provider.build_block_v1(request).await.wrap_err_with(|| {
        format!(
            "testing_buildBlockV1 failed for block {}. Ensure the target exposes the hidden \
             testing RPC, for example: reth node --http --http.api eth,testing ...",
            block.number
        )
    })?;
    let synthetic_block_hash = envelope.execution_payload.payload_inner.payload_inner.block_hash;
    let (payload, sidecar) = envelope.into_payload_and_sidecar(parent_beacon_block_root);
    let execution_data = ExecutionData::new(payload, sidecar);

    let payload_status = provider
        .reth_new_payload(RethNewPayloadInput::ExecutionData(Box::new(execution_data)), wait)
        .await
        .wrap_err_with(|| {
            format!("reth_newPayload failed for synthetic fork block after block {}", block.number)
        })?;

    if !payload_status.status.is_valid() {
        eyre::bail!(
            "reth_newPayload returned non-VALID status for synthetic fork block after block {}: {:?}",
            block.number,
            payload_status.status,
        );
    }

    let fcu_result = provider
        .reth_forkchoice_updated(ForkchoiceState {
            head_block_hash: synthetic_block_hash,
            safe_block_hash: branch_point_hash,
            finalized_block_hash: branch_point_hash,
        })
        .await
        .wrap_err_with(|| {
            format!(
                "reth_forkchoiceUpdated failed for synthetic fork block after block {}",
                block.number
            )
        })?;

    if !fcu_result.is_valid() {
        eyre::bail!(
            "reth_forkchoiceUpdated returned non-VALID status for synthetic fork block after block {}: {:?}",
            block.number,
            fcu_result.payload_status,
        );
    }

    Ok(synthetic_block_hash)
}

fn extract_block_header_from_block_rlp(raw: &[u8]) -> Result<ConsensusHeader> {
    let mut block_buf = raw;
    let mut block_payload = Header::decode_bytes(&mut block_buf, true)
        .wrap_err("failed to decode outer block RLP list")?;
    if !block_buf.is_empty() {
        eyre::bail!("block RLP has trailing bytes after outer list");
    }

    ConsensusHeader::decode(&mut block_payload).wrap_err("failed to decode block header")
}

fn extract_tx_bytes_from_block_rlp(raw: &[u8]) -> Result<Vec<Bytes>> {
    let mut block_buf = raw;
    let mut block_payload = Header::decode_bytes(&mut block_buf, true)
        .wrap_err("failed to decode outer block RLP list")?;
    if !block_buf.is_empty() {
        eyre::bail!("block RLP has trailing bytes after outer list");
    }

    skip_rlp_item(&mut block_payload).wrap_err("failed to skip block header")?;
    let mut txs_payload = Header::decode_bytes(&mut block_payload, true)
        .wrap_err("failed to decode transactions RLP list")?;

    let mut transactions = Vec::new();
    let mut skipped_blob_txs = 0usize;
    let mut non_blob_txs = 0usize;
    let mut dropped_non_blob_txs = 0usize;
    while !txs_payload.is_empty() {
        let item_start = txs_payload;
        let header =
            Header::decode(&mut txs_payload).wrap_err("failed to decode transaction RLP item")?;
        let header_len = item_start.len() - txs_payload.len();
        let payload = &txs_payload[..header.payload_length];
        let encoded = &item_start[..header_len + header.payload_length];

        if header.list {
            non_blob_txs += 1;
            if should_keep_reorg_transaction(non_blob_txs) {
                transactions.push(Bytes::copy_from_slice(encoded));
            } else {
                dropped_non_blob_txs += 1;
            }
        } else if payload.first() == Some(&0x03) {
            skipped_blob_txs += 1;
        } else {
            non_blob_txs += 1;
            if should_keep_reorg_transaction(non_blob_txs) {
                transactions.push(Bytes::copy_from_slice(payload));
            } else {
                dropped_non_blob_txs += 1;
            }
        }

        txs_payload = &txs_payload[header.payload_length..];
    }

    if skipped_blob_txs > 0 {
        tracing::warn!(skipped_blob_txs, "Skipped blob transactions while building synthetic fork");
    }
    if dropped_non_blob_txs > 0 {
        tracing::info!(
            kept_non_blob_txs = transactions.len(),
            dropped_non_blob_txs,
            "Kept 90% of non-blob transactions while building synthetic fork"
        );
    }

    Ok(transactions)
}

fn should_keep_reorg_transaction(non_blob_tx_index: usize) -> bool {
    !non_blob_tx_index.is_multiple_of(REORG_NON_BLOB_TX_DROP_INTERVAL)
}

fn skip_rlp_item(buf: &mut &[u8]) -> Result<()> {
    let header = Header::decode(buf).wrap_err("failed to decode RLP item header")?;
    *buf = &buf[header.payload_length..];
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_rlp::Encodable;

    fn rlp_bytes(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        payload.encode(&mut out);
        out
    }

    fn rlp_list_from_encoded(items: &[Vec<u8>]) -> Vec<u8> {
        let payload_length = items.iter().map(Vec::len).sum();
        let mut out = Vec::new();
        Header { list: true, payload_length }.encode(&mut out);
        for item in items {
            out.extend_from_slice(item);
        }
        out
    }

    fn legacy_tx(id: u8) -> Vec<u8> {
        rlp_list_from_encoded(&[rlp_bytes(&[id])])
    }

    fn typed_tx(tx_type: u8, id: u8) -> Vec<u8> {
        rlp_bytes(&[tx_type, id])
    }

    fn block_with_transactions(transactions: &[Vec<u8>]) -> Vec<u8> {
        let header = rlp_list_from_encoded(&[]);
        let transactions = rlp_list_from_encoded(transactions);
        let ommers = rlp_list_from_encoded(&[]);
        rlp_list_from_encoded(&[header, transactions, ommers])
    }

    fn scheduler_block(number: u64) -> BlockLine {
        BlockLine {
            raw: Bytes::new(),
            bal: None,
            key: B256::ZERO,
            number,
            timestamp: 0,
            gas_used: 0,
            gas_limit: 0,
            tx_count: 0,
        }
    }

    fn record_action(action: ReorgAction, observed: &mut String) {
        match action {
            ReorgAction::Pending => {}
            ReorgAction::Canonical(blocks) => observed.push_str(&"C".repeat(blocks.len())),
            ReorgAction::Batch(blocks) => {
                observed.push_str(&"S".repeat(blocks.len()));
                observed.push_str(&"C".repeat(blocks.len()));
            }
        }
    }

    fn schedule(depth: usize, gap: usize, count: u64) -> String {
        let mut state = ReorgState::new(depth, gap);
        let mut observed = String::new();
        for number in 1..=count {
            record_action(state.push(scheduler_block(number)), &mut observed);
        }
        record_action(state.finish(), &mut observed);
        observed
    }

    #[test]
    fn schedules_non_overlapping_reorgs_with_canonical_gaps_and_eof_tails() {
        assert_eq!(schedule(2, 0, 4), "SSCCSSCC");
        assert_eq!(schedule(2, 1, 7), "SSCCCSSCCCC");
    }

    #[test]
    fn extracts_nine_of_ten_non_blob_transactions_for_reorg_payloads() {
        let source_transactions = (0..10).map(legacy_tx).collect::<Vec<_>>();
        let block = block_with_transactions(&source_transactions);

        let extracted = extract_tx_bytes_from_block_rlp(&block)
            .expect("valid block RLP should extract transactions");

        let expected = source_transactions
            .into_iter()
            .take(REORG_NON_BLOB_TX_DROP_INTERVAL - 1)
            .map(Bytes::from)
            .collect::<Vec<_>>();
        assert_eq!(extracted, expected);
    }

    #[test]
    fn blob_transactions_do_not_count_toward_reorg_payload_keep_ratio() {
        let mut source_transactions = (0..9).map(legacy_tx).collect::<Vec<_>>();
        source_transactions.push(typed_tx(0x03, 0));
        source_transactions.push(legacy_tx(9));
        let block = block_with_transactions(&source_transactions);

        let extracted = extract_tx_bytes_from_block_rlp(&block)
            .expect("valid block RLP should extract transactions");

        let expected = source_transactions
            .into_iter()
            .take(REORG_NON_BLOB_TX_DROP_INTERVAL - 1)
            .map(Bytes::from)
            .collect::<Vec<_>>();
        assert_eq!(extracted, expected);
    }
}

async fn process_big_block(
    provider: &(impl Provider + RethApi<Ethereum>),
    big_block: &BigBlockData<ExecutionData>,
    collector: &mut MetricsCollector,
    persistence_policy: &WaitForPersistence,
) -> Result<()> {
    let first_payload = big_block
        .env_switches
        .first()
        .ok_or_else(|| eyre::eyre!("big-block payload contains no execution payloads"))?;
    let last_payload = big_block
        .env_switches
        .last()
        .ok_or_else(|| eyre::eyre!("big-block payload contains no execution payloads"))?;

    let block_hash = last_payload.block_hash();
    let block_number = big_block.block_number;
    let tx_count = big_block.env_switches.iter().map(|data| data.transaction_count()).sum();
    let gas_used = big_block.env_switches.iter().map(|data| data.payload.as_v1().gas_used).sum();
    let gas_limit = big_block.env_switches.iter().map(|data| data.payload.gas_limit()).sum();
    let wait = persistence_policy.should_wait(collector.blocks_submitted());

    let input = RethNewPayloadInput::BigBlockData(Box::new(big_block.clone()));

    let new_payload_start = Instant::now();
    let payload_status =
        provider.reth_new_payload(input, wait).await.wrap_err("reth_newPayload failed")?;
    let new_payload_latency = new_payload_start.elapsed();

    if !payload_status.status.is_valid() {
        collector.record_failure();
        eyre::bail!(
            "reth_newPayload returned non-VALID status for big block {}: {:?}",
            block_number,
            payload_status.status,
        );
    }

    let forkchoice_state = ForkchoiceState {
        head_block_hash: block_hash,
        safe_block_hash: block_hash,
        finalized_block_hash: block_hash,
    };

    let fcu_start = Instant::now();
    let fcu_result = provider
        .reth_forkchoice_updated(forkchoice_state)
        .await
        .wrap_err("reth_forkchoiceUpdated failed")?;
    let fcu_latency = fcu_start.elapsed();

    if !fcu_result.is_valid() {
        collector.record_failure();
        eyre::bail!(
            "reth_forkchoiceUpdated returned non-VALID status for big block {}: {:?}",
            block_number,
            fcu_result.payload_status,
        );
    }

    let timestamp_ms = first_payload.payload.timestamp() * 1000;
    let block_time_ms = collector.prev_timestamp_ms.map(|prev| timestamp_ms.saturating_sub(prev));
    collector.prev_timestamp_ms = Some(timestamp_ms);

    let block_stats = BlockStats {
        number: block_number,
        timestamp_ms,
        tx_count,
        gas_used,
        gas_limit,
        block_time_ms,
        new_payload_ms: Some(new_payload_latency.as_millis() as u64),
        forkchoice_updated_ms: Some(fcu_latency.as_millis() as u64),
        new_payload_server_latency_us: Some(payload_status.latency_us),
        persistence_wait_us: payload_status.persistence_wait_us,
        execution_cache_wait_us: payload_status.execution_cache_wait_us,
        sparse_trie_wait_us: payload_status.sparse_trie_wait_us,
    };

    collector.record_success(block_stats);

    tracing::info!(
        block = block_number,
        txs = tx_count,
        gas = gas_used,
        new_payload_ms = new_payload_latency.as_millis(),
        forkchoice_updated_ms = fcu_latency.as_millis(),
        total_ms = (new_payload_latency + fcu_latency).as_millis(),
        new_payload_server_latency_us = payload_status.latency_us,
        status = %payload_status.status.status,
        "Submitted big block"
    );

    collector.prev_block_hash = Some(block_hash);
    if block_number.is_multiple_of(32) {
        collector.finalized_hash = Some(block_hash);
    }

    Ok(())
}

/// Shared atomic counters for the scraper snapshot callback.
#[derive(Debug, Default)]
struct BlockCounters {
    submitted: AtomicU64,
    success: AtomicU64,
    failed: AtomicU64,
}

impl BlockCounters {
    fn snapshot_samples(&self, clock: &RunClock) -> Vec<Sample> {
        let submitted = self.submitted.load(Ordering::Relaxed);
        let success = self.success.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        let offset_ms = clock.offset_ms();
        let unix_ms = clock.unix_ms();
        let labels = BTreeMap::new();

        vec![
            Sample {
                name: "txgen_blocks_sent_total".to_string(),
                labels: labels.clone(),
                value: submitted as f64,
                offset_ms,
                unix_ms,
            },
            Sample {
                name: "txgen_blocks_success_total".to_string(),
                labels: labels.clone(),
                value: success as f64,
                offset_ms,
                unix_ms,
            },
            Sample {
                name: "txgen_blocks_failed_total".to_string(),
                labels,
                value: failed as f64,
                offset_ms,
                unix_ms,
            },
        ]
    }
}

/// Block submission state and metrics collector.
struct MetricsCollector {
    counters: Arc<BlockCounters>,
    prev_block_hash: Option<B256>,
    finalized_hash: Option<B256>,
    prev_timestamp_ms: Option<u64>,
    blocks: Vec<BlockStats>,
}

impl MetricsCollector {
    fn new(counters: Arc<BlockCounters>) -> Self {
        Self {
            counters,
            prev_block_hash: None,
            finalized_hash: None,
            prev_timestamp_ms: None,
            blocks: Vec::new(),
        }
    }

    fn record_success(&mut self, stats: BlockStats) {
        self.counters.submitted.fetch_add(1, Ordering::Relaxed);
        self.counters.success.fetch_add(1, Ordering::Relaxed);
        self.blocks.push(stats);
    }

    fn record_failure(&self) {
        self.counters.submitted.fetch_add(1, Ordering::Relaxed);
        self.counters.failed.fetch_add(1, Ordering::Relaxed);
    }

    fn blocks_submitted(&self) -> u64 {
        self.counters.submitted.load(Ordering::Relaxed)
    }

    fn blocks_success(&self) -> u64 {
        self.counters.success.load(Ordering::Relaxed)
    }

    fn blocks_failed(&self) -> u64 {
        self.counters.failed.load(Ordering::Relaxed)
    }

    fn final_snapshot(&self, clock: &RunClock) -> Vec<Sample> {
        self.counters.snapshot_samples(clock)
    }
}
