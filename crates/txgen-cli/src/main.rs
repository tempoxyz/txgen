use alloy_consensus::{Block as ConsensusBlock, TxEnvelope};
use alloy_eips::BlockNumberOrTag;
use alloy_primitives::{Address, B256, Bytes};
use alloy_provider::{
    Provider, ProviderBuilder, ext::DebugApi, ext::TestingApi, network::Ethereum,
};
use alloy_rlp::{Decodable, Encodable};
use alloy_rpc_types_engine::{
    ExecutionPayload, ExecutionPayloadSidecar, PayloadAttributes, TestingBuildBlockRequestV1,
};
use clap::{Args, Parser, Subcommand};
use eyre::{Result, WrapErr, bail};
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::io::Write;
use std::path::PathBuf;
use tokio::sync::mpsc;
use txgen_core::{
    AccountManager, ArtifactManager, BlockTxEntry, BuildContext, ChainPlugin, GasConfig,
    GeneratedTx, NdjsonWriter, NonceTracker, WorkloadMode, WorkloadSpec,
};
use txgen_ethereum::{EthereumPlugin, EthereumTemplate};
use txgen_tempo::{TempoNonceProvider, TempoPlugin, TempoTemplate};

#[derive(Parser)]
#[command(name = "txgen", about = "Chain-agnostic transaction generator")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Generate transactions from a workload spec
    Generate(GenerateArgs),
    /// List all addresses from a workload spec (for funding)
    Addresses(AddressesArgs),
    /// Extract raw RLP blocks from an archive RPC as NDJSON
    Extract(ExtractArgs),
}

#[derive(Args)]
struct AddressesArgs {
    /// Workload spec file (YAML)
    #[arg(short, long)]
    spec: PathBuf,

    /// Output format: plain (one per line), json, or shell (for xargs)
    #[arg(short, long, default_value = "plain")]
    format: String,
}

#[derive(Args)]
struct GenerateArgs {
    /// Workload spec file (YAML)
    #[arg(short, long)]
    spec: PathBuf,

    /// Chain plugin: ethereum, tempo
    #[arg(short, long)]
    chain: String,

    /// Number of transactions to generate
    #[arg(short = 'n', long)]
    count: u64,

    /// Output file (default: stdout)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// RPC endpoint URL (to fetch current nonces from chain)
    #[arg(long)]
    rpc: Option<String>,

    /// Rate limit for RPC requests per second (0 = unbounded)
    #[arg(long, default_value = "0")]
    rpc_rps: u64,

    /// RNG seed for reproducibility
    #[arg(long)]
    seed: Option<u64>,
}

#[derive(Args)]
struct ExtractArgs {
    /// RPC endpoint URL (archive node with debug_getRawBlock)
    #[arg(long)]
    rpc: String,

    /// First block number to fetch (inclusive)
    #[arg(long)]
    from: u64,

    /// Last block number to fetch (inclusive)
    #[arg(long)]
    to: u64,

    /// Output file (default: stdout)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Number of blocks to prefetch ahead
    #[arg(long, default_value = "20")]
    buffer_size: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Generate(args) => run_generate(args).await,
        Command::Addresses(args) => run_addresses(args),
        Command::Extract(args) => run_extract(args).await,
    }
}

fn run_addresses(args: AddressesArgs) -> Result<()> {
    let spec = WorkloadSpec::load(&args.spec)
        .wrap_err_with(|| format!("failed to load spec: {}", args.spec.display()))?;

    let accounts = AccountManager::from_spec(&spec.accounts)?;

    let all_addresses: Vec<_> = accounts
        .all_addresses()
        .flat_map(|(_, addrs)| addrs)
        .collect();

    match args.format.as_str() {
        "plain" => {
            for addr in &all_addresses {
                println!("{addr}");
            }
        }
        "json" => {
            let json = serde_json::to_string_pretty(&all_addresses)?;
            println!("{json}");
        }
        "shell" => {
            // Space-separated for use with xargs
            let line: Vec<_> = all_addresses.iter().map(|a| a.to_string()).collect();
            println!("{}", line.join(" "));
        }
        other => bail!("unknown format: {}", other),
    }

    Ok(())
}

async fn run_generate(args: GenerateArgs) -> Result<()> {
    let spec = WorkloadSpec::load(&args.spec)
        .wrap_err_with(|| format!("failed to load spec: {}", args.spec.display()))?;

    let base_path = args
        .spec
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));

    let accounts = AccountManager::from_spec(&spec.accounts)?;
    let artifacts = ArtifactManager::load(&spec.artifacts, base_path)?;
    let gas = spec.gas.clone();
    let mut nonces = NonceTracker::new();

    // Fetch protocol nonces from RPC if provided
    if let Some(ref rpc_url) = args.rpc {
        fetch_protocol_nonces(&accounts, &mut nonces, rpc_url).await?;
    }

    let mut rng = match args.seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::from_os_rng(),
    };

    // Block generation mode: uses testing_buildBlockV1 RPC to build valid blocks
    if spec.mode == WorkloadMode::Blocks {
        let rpc_url = args
            .rpc
            .as_deref()
            .ok_or_else(|| eyre::eyre!("--rpc is required for block generation mode"))?;
        return run_generate_blocks(
            &spec,
            args.count,
            args.output,
            &accounts,
            &artifacts,
            &gas,
            &mut nonces,
            &mut rng,
            rpc_url,
            &args.chain,
        )
        .await;
    }

    match args.chain.as_str() {
        "ethereum" => generate_with_plugin::<EthereumPlugin, EthereumTemplate>(
            EthereumPlugin,
            &spec,
            args.count,
            args.output,
            &accounts,
            &artifacts,
            &gas,
            &mut nonces,
            &mut rng,
        ),
        "tempo" => {
            // For Tempo, use async generation with nonce provider for parallel lanes
            let nonce_provider = args.rpc.as_ref().map(|rpc_url| {
                let provider = ProviderBuilder::<_, _, Ethereum>::new()
                    .connect_http(rpc_url.parse().expect("invalid RPC URL"));
                if args.rpc_rps > 0 {
                    TempoNonceProvider::with_rate_limit(provider, args.rpc_rps)
                } else {
                    TempoNonceProvider::new(provider)
                }
            });

            generate_tempo(
                &spec,
                args.count,
                args.output,
                &accounts,
                &artifacts,
                &gas,
                &mut nonces,
                &mut rng,
                nonce_provider.as_ref(),
            )
            .await
        }
        other => bail!("unsupported chain plugin: {}", other),
    }
}

/// Generate Tempo transactions with async nonce fetching for parallel lanes.
#[allow(clippy::too_many_arguments)]
async fn generate_tempo<P: txgen_core::NonceProvider>(
    spec: &WorkloadSpec,
    count: u64,
    output: Option<PathBuf>,
    accounts: &AccountManager,
    artifacts: &ArtifactManager,
    gas: &GasConfig,
    nonces: &mut NonceTracker,
    rng: &mut StdRng,
    nonce_provider: Option<&P>,
) -> Result<()> {
    let plugin = TempoPlugin::default();
    let total_weight = spec.total_weight();
    if total_weight == 0 {
        bail!("no templates in mix (total weight is 0)");
    }

    let mut ctx = BuildContext::new(spec.chain_id, gas, accounts, artifacts, nonces, rng);

    match output {
        Some(path) => {
            let mut writer = txgen_core::output::file_writer(&path)?;
            generate_tempo_txs(
                &plugin,
                spec,
                count,
                total_weight,
                &mut ctx,
                &mut writer,
                nonce_provider,
            )
            .await?;
            eprintln!("wrote {} transactions to {}", count, path.display());
        }
        None => {
            let mut writer = txgen_core::output::stdout_writer();
            generate_tempo_txs(
                &plugin,
                spec,
                count,
                total_weight,
                &mut ctx,
                &mut writer,
                nonce_provider,
            )
            .await?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn generate_tempo_txs<W: Write, P: txgen_core::NonceProvider>(
    plugin: &TempoPlugin,
    spec: &WorkloadSpec,
    count: u64,
    total_weight: u64,
    ctx: &mut BuildContext<'_>,
    writer: &mut NdjsonWriter<W>,
    nonce_provider: Option<&P>,
) -> Result<()> {
    let start = std::time::Instant::now();
    let mut last_log = start;

    for i in 0..count {
        let template_name = pick_template(spec, ctx.rng, total_weight)?;
        let template_value = spec
            .templates
            .get(&template_name)
            .ok_or_else(|| eyre::eyre!("template '{}' not found", template_name))?;

        let template: TempoTemplate = serde_yaml::from_value(template_value.clone())
            .wrap_err_with(|| format!("failed to parse template '{}'", template_name))?;

        let tx = plugin
            .build_with_nonce_provider(template, ctx, nonce_provider)
            .await
            .wrap_err_with(|| format!("failed to build tx from template '{}'", template_name))?;

        writer.write(&tx)?;

        // Log progress every 10k txs or every 5 seconds
        let now = std::time::Instant::now();
        if (i + 1) % 10000 == 0 || now.duration_since(last_log).as_secs() >= 5 {
            let elapsed = now.duration_since(start).as_secs_f64();
            let tps = (i + 1) as f64 / elapsed;
            eprintln!(
                "generated {}/{} txs ({:.1}%) - {:.0} tx/s",
                i + 1,
                count,
                (i + 1) as f64 / count as f64 * 100.0,
                tps
            );
            last_log = now;
        }
    }
    writer.flush()?;
    Ok(())
}

/// Generate blocks using `testing_buildBlockV1`.
///
/// For each block: generate signed transactions using the chain plugin, send them
/// to the node via `testing_buildBlockV1` (which executes the txs against real state
/// and computes valid roots), then output the resulting block as NDJSON `{raw, key}`.
///
/// NOTE: reth does NOT trust any header fields from submitted blocks — it re-executes
/// every transaction and validates all computed roots (`state_root`, `receipts_root`,
/// `transactions_root`, `gas_used`, etc.) against the header. See:
/// - State root check: payload_validator.rs#L783-L804
/// - Post-execution: payload_validator.rs#L1346-L1412
///
/// This is why we use `testing_buildBlockV1` to let the node build valid blocks
/// rather than assembling them client-side.
#[allow(clippy::too_many_arguments)]
async fn run_generate_blocks(
    spec: &WorkloadSpec,
    count: u64,
    output: Option<PathBuf>,
    accounts: &AccountManager,
    artifacts: &ArtifactManager,
    gas: &GasConfig,
    nonces: &mut NonceTracker,
    rng: &mut StdRng,
    rpc_url: &str,
    chain: &str,
) -> Result<()> {
    let block_total_weight = spec.block_total_weight();
    if block_total_weight == 0 {
        bail!("no block templates in block_mix (total weight is 0)");
    }
    let tx_total_weight = spec.total_weight();

    let provider = ProviderBuilder::<_, _, Ethereum>::new()
        .connect_http(rpc_url.parse().wrap_err("invalid RPC URL")?);

    // Get the latest block to use as parent
    let latest = provider
        .get_block_by_number(BlockNumberOrTag::Latest)
        .await
        .wrap_err("failed to fetch latest block")?
        .ok_or_else(|| eyre::eyre!("no latest block found"))?;
    let mut parent_hash = latest.header.hash;
    let mut timestamp = latest.header.timestamp;

    let mut ctx = BuildContext::new(spec.chain_id, gas, accounts, artifacts, nonces, rng);

    match output {
        Some(path) => {
            let file = std::fs::File::create(&path)
                .wrap_err_with(|| format!("failed to create output file: {}", path.display()))?;
            let mut writer = std::io::BufWriter::new(file);
            generate_blocks(
                spec,
                count,
                block_total_weight,
                tx_total_weight,
                &mut ctx,
                &provider,
                &mut parent_hash,
                &mut timestamp,
                &mut writer,
                chain,
            )
            .await?;
            eprintln!("wrote {} blocks to {}", count, path.display());
        }
        None => {
            let mut writer = std::io::stdout();
            generate_blocks(
                spec,
                count,
                block_total_weight,
                tx_total_weight,
                &mut ctx,
                &provider,
                &mut parent_hash,
                &mut timestamp,
                &mut writer,
                chain,
            )
            .await?;
        }
    }

    Ok(())
}

/// Generate blocks and write as NDJSON.
#[allow(clippy::too_many_arguments)]
async fn generate_blocks<W: Write>(
    spec: &WorkloadSpec,
    count: u64,
    block_total_weight: u64,
    tx_total_weight: u64,
    ctx: &mut BuildContext<'_>,
    provider: &impl TestingApi<Ethereum>,
    parent_hash: &mut B256,
    timestamp: &mut u64,
    writer: &mut W,
    chain: &str,
) -> Result<()> {
    let start = std::time::Instant::now();

    for i in 0..count {
        let block_template_name = pick_block_template(spec, ctx.rng, block_total_weight)?;
        let block_template = spec
            .block_templates
            .get(&block_template_name)
            .ok_or_else(|| eyre::eyre!("block template '{}' not found", block_template_name))?;

        // Generate all transactions for this block
        let mut raw_txs: Vec<Bytes> = Vec::new();
        for tx_entry in &block_template.txs {
            let entry_txs = generate_block_tx_entry(tx_entry, spec, ctx, tx_total_weight, chain)?;
            raw_txs.extend(entry_txs);
        }

        // Advance timestamp (12s per block for increment strategy)
        *timestamp += 12;

        let fee_recipient = block_template.engine.fee_recipient.unwrap_or(Address::ZERO);

        let request = TestingBuildBlockRequestV1 {
            parent_block_hash: *parent_hash,
            payload_attributes: PayloadAttributes {
                timestamp: *timestamp,
                prev_randao: B256::ZERO,
                suggested_fee_recipient: fee_recipient,
                withdrawals: Some(vec![]),
                parent_beacon_block_root: Some(B256::ZERO),
            },
            transactions: raw_txs,
            extra_data: None,
        };

        let envelope = provider.build_block_v1(request).await.wrap_err_with(|| {
            format!(
                "testing_buildBlockV1 failed for block {} (template '{}')",
                i + 1,
                block_template_name
            )
        })?;

        // Convert payload to consensus block for RLP encoding
        let payload = ExecutionPayload::V3(envelope.execution_payload);
        let block_hash = payload.block_hash();

        let sidecar = ExecutionPayloadSidecar::v4(
            alloy_rpc_types_engine::CancunPayloadFields {
                parent_beacon_block_root: B256::ZERO,
                versioned_hashes: envelope.blobs_bundle.versioned_hashes(),
            },
            alloy_rpc_types_engine::PraguePayloadFields::new(envelope.execution_requests),
        );
        let block = payload
            .into_block_with_sidecar_raw(&sidecar)
            .wrap_err("failed to convert payload to block")?;

        let mut rlp_buf = Vec::new();
        block.encode(&mut rlp_buf);

        // Write NDJSON {raw, key}
        let raw_hex = format!("0x{}", hex::encode(&rlp_buf));
        let key_hex = format!("{block_hash}");
        let line = BlockOutputLine {
            raw: &raw_hex,
            key: &key_hex,
        };
        serde_json::to_writer(&mut *writer, &line)?;
        writer.write_all(b"\n")?;

        *parent_hash = block_hash;

        let elapsed = start.elapsed().as_secs_f64();
        let bps = (i + 1) as f64 / elapsed;
        eprintln!(
            "built block {}/{} (template '{}', {} txs) - {:.1} blocks/s",
            i + 1,
            count,
            block_template_name,
            block.body.transactions.len(),
            bps
        );
    }

    writer.flush()?;
    Ok(())
}

/// Generate transactions for a single block tx entry.
fn generate_block_tx_entry(
    entry: &BlockTxEntry,
    spec: &WorkloadSpec,
    ctx: &mut BuildContext<'_>,
    tx_total_weight: u64,
    chain: &str,
) -> Result<Vec<Bytes>> {
    let mut raw_txs = Vec::with_capacity(entry.count as usize);

    for _ in 0..entry.count {
        // Determine which template to use
        let template_name = if entry.mix.unwrap_or(false) {
            if tx_total_weight == 0 {
                bail!("block tx entry uses mix but no tx templates in mix");
            }
            pick_template(spec, ctx.rng, tx_total_weight)?
        } else if let Some(ref name) = entry.template {
            name.clone()
        } else {
            bail!("block tx entry must specify either 'template' or 'mix: true'");
        };

        let template_value = spec
            .templates
            .get(&template_name)
            .ok_or_else(|| eyre::eyre!("template '{}' not found", template_name))?;

        // Build the transaction using the appropriate chain plugin
        let tx: GeneratedTx = match chain {
            "ethereum" => {
                let template: EthereumTemplate = serde_yaml::from_value(template_value.clone())
                    .wrap_err_with(|| format!("failed to parse template '{}'", template_name))?;
                EthereumPlugin.build(template, ctx).wrap_err_with(|| {
                    format!("failed to build tx from template '{}'", template_name)
                })?
            }
            "tempo" => {
                let template: TempoTemplate = serde_yaml::from_value(template_value.clone())
                    .wrap_err_with(|| format!("failed to parse template '{}'", template_name))?;
                TempoPlugin::default()
                    .build(template, ctx)
                    .wrap_err_with(|| {
                        format!("failed to build tx from template '{}'", template_name)
                    })?
            }
            other => bail!("unsupported chain plugin: {}", other),
        };

        raw_txs.push(tx.raw);
    }

    Ok(raw_txs)
}

/// Pick a block template by weighted random selection.
fn pick_block_template(spec: &WorkloadSpec, rng: &mut StdRng, total_weight: u64) -> Result<String> {
    let roll = rng.random_range(0..total_weight);
    let mut cumulative = 0;
    for entry in &spec.block_mix {
        cumulative += entry.weight;
        if roll < cumulative {
            return Ok(entry.template.clone());
        }
    }
    // SAFETY: Should not reach here if total_weight > 0 and block_mix is non-empty
    unreachable!(
        "block template selection failed with roll={} total_weight={}",
        roll, total_weight
    )
}

/// Fetch protocol nonces (nonce_key=0) for all accounts from the RPC.
async fn fetch_protocol_nonces(
    accounts: &AccountManager,
    nonces: &mut NonceTracker,
    rpc_url: &str,
) -> Result<()> {
    let provider = ProviderBuilder::<_, _, Ethereum>::new()
        .connect_http(rpc_url.parse().wrap_err("invalid RPC URL")?);

    for (pool_name, addresses) in accounts.all_addresses() {
        let total = addresses.len();
        for (idx, address) in addresses.iter().enumerate() {
            eprintln!(
                "fetching nonce for {}[{}/{}] ({})...",
                pool_name,
                idx + 1,
                total,
                address
            );

            let nonce = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                provider.get_transaction_count(*address),
            )
            .await
            .wrap_err_with(|| format!("timeout fetching nonce for {}[{}]", pool_name, idx))?
            .wrap_err_with(|| {
                format!(
                    "failed to fetch nonce for {}[{}] ({})",
                    pool_name, idx, address
                )
            })?;

            // Protocol nonce scheduling key = sender address
            let scheduling_key = address.0.0;
            nonces.reset(scheduling_key, nonce);

            eprintln!(
                "fetched nonce for {}[{}/{}] ({}): {}",
                pool_name,
                idx + 1,
                total,
                address,
                nonce
            );
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn generate_with_plugin<P, T>(
    plugin: P,
    spec: &WorkloadSpec,
    count: u64,
    output: Option<PathBuf>,
    accounts: &AccountManager,
    artifacts: &ArtifactManager,
    gas: &GasConfig,
    nonces: &mut NonceTracker,
    rng: &mut StdRng,
) -> Result<()>
where
    P: ChainPlugin<Template = T>,
    T: serde::de::DeserializeOwned,
{
    let total_weight = spec.total_weight();
    if total_weight == 0 {
        bail!("no templates in mix (total weight is 0)");
    }

    let mut ctx = BuildContext::new(spec.chain_id, gas, accounts, artifacts, nonces, rng);

    match output {
        Some(path) => {
            let mut writer = txgen_core::output::file_writer(&path)?;
            generate_txs(&plugin, spec, count, total_weight, &mut ctx, &mut writer)?;
            eprintln!("wrote {} transactions to {}", count, path.display());
        }
        None => {
            let mut writer = txgen_core::output::stdout_writer();
            generate_txs(&plugin, spec, count, total_weight, &mut ctx, &mut writer)?;
        }
    }

    Ok(())
}

fn generate_txs<P, T, W>(
    plugin: &P,
    spec: &WorkloadSpec,
    count: u64,
    total_weight: u64,
    ctx: &mut BuildContext<'_>,
    writer: &mut NdjsonWriter<W>,
) -> Result<()>
where
    P: ChainPlugin<Template = T>,
    T: serde::de::DeserializeOwned,
    W: Write,
{
    for _ in 0..count {
        let template_name = pick_template(spec, ctx.rng, total_weight)?;
        let template_value = spec
            .templates
            .get(&template_name)
            .ok_or_else(|| eyre::eyre!("template '{}' not found", template_name))?;

        let template: T = serde_yaml::from_value(template_value.clone())
            .wrap_err_with(|| format!("failed to parse template '{}'", template_name))?;

        let tx: GeneratedTx = plugin
            .build(template, ctx)
            .wrap_err_with(|| format!("failed to build tx from template '{}'", template_name))?;

        writer.write(&tx)?;
    }
    writer.flush()?;
    Ok(())
}

async fn run_extract(args: ExtractArgs) -> Result<()> {
    if args.from > args.to {
        bail!("--from must be <= --to");
    }

    let provider = ProviderBuilder::<_, _, Ethereum>::new()
        .connect_http(args.rpc.parse().wrap_err("invalid RPC URL")?);

    let (tx, mut rx) = mpsc::channel::<Result<FetchedBlock>>(args.buffer_size);

    let from = args.from;
    let to = args.to;
    let fetch_handle = tokio::spawn(async move { fetch_blocks(provider, from, to, tx).await });

    let total = to - from + 1;
    let write_result = match args.output {
        Some(ref path) => {
            let file = std::fs::File::create(path)
                .wrap_err_with(|| format!("failed to create output file: {}", path.display()))?;
            let mut writer = std::io::BufWriter::new(file);
            let result = write_extracted_blocks(&mut rx, &mut writer, total).await;
            if result.is_ok() {
                eprintln!("wrote {} blocks to {}", total, path.display());
            }
            result
        }
        None => {
            let mut writer = std::io::stdout();
            write_extracted_blocks(&mut rx, &mut writer, total).await
        }
    };

    fetch_handle.await?;
    write_result
}

/// NDJSON output line for extracted blocks.
#[derive(serde::Serialize)]
struct BlockOutputLine<'a> {
    raw: &'a str,
    key: &'a str,
}

async fn write_extracted_blocks<W: Write>(
    rx: &mut mpsc::Receiver<Result<FetchedBlock>>,
    writer: &mut W,
    total: u64,
) -> Result<()> {
    let start = std::time::Instant::now();
    let mut last_log = start;
    let mut count = 0u64;

    while let Some(result) = rx.recv().await {
        let block = result?;

        let raw_hex = format!("0x{}", hex::encode(&block.rlp_bytes));
        let key_hex = format!("{}", block.hash);
        let line = BlockOutputLine {
            raw: &raw_hex,
            key: &key_hex,
        };
        serde_json::to_writer(&mut *writer, &line)?;
        writer.write_all(b"\n")?;
        count += 1;

        let now = std::time::Instant::now();
        if count % 1000 == 0 || now.duration_since(last_log).as_secs() >= 5 {
            let elapsed = now.duration_since(start).as_secs_f64();
            let bps = count as f64 / elapsed;
            eprintln!(
                "extracted {}/{} blocks ({:.1}%) - {:.0} blocks/s",
                count,
                total,
                count as f64 / total as f64 * 100.0,
                bps
            );
            last_log = now;
        }
    }

    writer.flush()?;
    Ok(())
}

/// A fetched block ready for output.
struct FetchedBlock {
    rlp_bytes: Bytes,
    hash: alloy_primitives::B256,
}

/// Fetch raw RLP-encoded blocks from source provider and send to channel.
async fn fetch_blocks<P: Provider + DebugApi>(
    provider: P,
    from: u64,
    to: u64,
    tx: mpsc::Sender<Result<FetchedBlock>>,
) {
    for block_num in from..=to {
        let result = async {
            let rlp_bytes: Bytes = provider
                .debug_get_raw_block(BlockNumberOrTag::Number(block_num).into())
                .await
                .wrap_err_with(|| format!("failed to fetch raw block {block_num}"))?;

            let mut buf = rlp_bytes.as_ref();
            let block = ConsensusBlock::<TxEnvelope>::decode(&mut buf)
                .wrap_err_with(|| format!("failed to RLP-decode block {block_num}"))?;
            let hash = block.header.hash_slow();

            Ok(FetchedBlock { rlp_bytes, hash })
        }
        .await;

        let is_err = result.is_err();
        if tx.send(result).await.is_err() {
            break;
        }
        if is_err {
            break;
        }
    }
}

fn pick_template(spec: &WorkloadSpec, rng: &mut StdRng, total_weight: u64) -> Result<String> {
    let roll = rng.random_range(0..total_weight);
    let mut cumulative = 0;
    for entry in &spec.mix {
        cumulative += entry.weight;
        if roll < cumulative {
            return Ok(entry.template.clone());
        }
    }
    // SAFETY: Should not reach here if total_weight > 0 and mix is non-empty
    unreachable!(
        "template selection failed with roll={} total_weight={}",
        roll, total_weight
    )
}
