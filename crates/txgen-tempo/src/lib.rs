mod nonce;
mod template;

pub use nonce::{prefetch_parallel_nonces, NONCE_PRECOMPILE};
pub use txgen_cli::fetch_protocol_nonces;

use alloy_eips::eip2718::Encodable2718;
use alloy_network::TransactionBuilder;
use alloy_primitives::{keccak256, Address, Bytes, TxKind, U256};
use alloy_provider::{network::Ethereum, DynProvider};
use alloy_signer::SignerSync;
use eyre::{bail, Result};
use rand::RngCore;
use std::{io::Write, num::NonZeroU64, sync::OnceLock};
use tempo_alloy::{rpc::TempoTransactionRequest, TempoNetwork};
use tempo_primitives::{
    transaction::{
        multisig_digest, Call, InitMultisig, MultisigOwner, MultisigSignature, PrimitiveSignature,
        TempoSignature, TEMPO_EXPIRING_NONCE_KEY, TEMPO_EXPIRING_NONCE_MAX_EXPIRY_SECS,
    },
    TempoTxEnvelope,
};
use txgen_cli::{sign_request_default, GenerateContext, NetworkAdapter, TxRequest};
use txgen_core::{
    BuildContext, GeneratedTx, NativeMultisig1Of1Account, NdjsonWriter, SchedulingKey, TxPhase,
};

pub use template::{TempoTemplate, TempoTxType};

/// Internal nonce-tracker slot used to derive deterministic uniqueness bumps for
/// expiring nonce transactions.
const EXPIRING_UNIQUENESS_COUNTER_KEY: [u8; 20] = *b"tempo-expiring-seq!!";

/// Tempo network adapter for transaction generation.
///
/// Supports all Ethereum transaction types (legacy, EIP-2930, EIP-1559)
/// plus Tempo native 0x76 transactions.
///
/// Holds the RPC [`DynProvider`] populated by [`Self::prefetch_nonces`] so
/// [`Self::build_request`] can lazy-fetch nonces for `(account, nonce_key)`
/// pairs that [`prefetch_parallel_nonces`] cannot enumerate up front (any
/// non-literal `nonce_key`, such as `uniform` or `choice`).
#[derive(Default)]
pub struct TempoAdapter {
    /// Set exactly once by [`Self::prefetch_nonces`], then read lock-free
    /// on the hot path by [`Self::next_nonce_lazy`].
    nonce_rpc: OnceLock<DynProvider<Ethereum>>,
}

impl TempoAdapter {
    /// Create a new adapter. The nonce RPC is populated by
    /// [`Self::prefetch_nonces`] when `--rpc` is supplied.
    pub const fn new() -> Self {
        Self { nonce_rpc: OnceLock::new() }
    }

    /// Return the next nonce for `scheduling_key`, fetching the on-chain
    /// value once if the [`txgen_core::NonceTracker`] hasn't seen this lane
    /// before.
    ///
    /// Bridges from the sync `BuildContext` to the async provider via
    /// `tokio::task::block_in_place`; the `txgen-tempo` binary uses
    /// `#[tokio::main]` (multi-threaded), where this is permitted.
    fn next_nonce_lazy(
        &self,
        ctx: &mut BuildContext<'_>,
        scheduling_key: [u8; 20],
        address: Address,
        nonce_key: U256,
    ) -> Result<u64> {
        if !ctx.nonces.contains(&scheduling_key)
            && let Some(provider) = self.nonce_rpc.get()
        {
            let n = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(nonce::fetch_lane_nonce(provider, address, nonce_key))
            })?;
            ctx.nonces.reset(scheduling_key, n);
        }
        Ok(ctx.next_nonce(scheduling_key))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TempoNonceMode {
    Protocol,
    Parallel(U256),
    Expiring,
}

impl NetworkAdapter for TempoAdapter {
    type Template = TempoTemplate;
    type Network = TempoNetwork;

    fn build_request(
        &self,
        template: Self::Template,
        ctx: &mut BuildContext<'_>,
    ) -> Result<TxRequest<TempoTransactionRequest>> {
        let selected = ctx.select_signer(&template.from)?;
        let native_multisig =
            ctx.accounts.native_multisig_1_of_1(&selected.pool, selected.index)?;
        let is_tempo = template.tx_type == TempoTxType::Tempo;
        if native_multisig.is_some() && !is_tempo {
            bail!(
                "native multisig account pools can only be used with Tempo transaction templates"
            );
        }
        let nonce_mode = resolve_nonce_mode(&template, is_tempo, ctx)?;
        if !matches!(nonce_mode, TempoNonceMode::Expiring)
            && let Some(valid_for_secs) = template.valid_for_secs
        {
            bail!(
                "`valid_for_secs` is only supported for expiring Tempo transactions (got {valid_for_secs}s on {:?})",
                template.tx_type
            );
        }
        let scheduling_key = compute_scheduling_key(selected.address, nonce_mode, ctx);
        let nonce = match nonce_mode {
            TempoNonceMode::Expiring => 0,
            TempoNonceMode::Protocol => {
                self.next_nonce_lazy(ctx, scheduling_key, selected.address, U256::ZERO)?
            }
            TempoNonceMode::Parallel(nonce_key) => {
                self.next_nonce_lazy(ctx, scheduling_key, selected.address, nonce_key)?
            }
        };

        let (to, value, input, calls) = resolve_call_data(&template, is_tempo, ctx)?;

        let mut req = TempoTransactionRequest::default();
        req.set_chain_id(ctx.chain_id);
        req.set_nonce(nonce);
        req.set_gas_limit(template.gas_limit);
        req.set_from(selected.address);

        match template.tx_type {
            TempoTxType::Tempo => {
                req.set_max_fee_per_gas(
                    template.max_fee_per_gas.unwrap_or(ctx.gas.max_fee_per_gas),
                );
                req.set_max_priority_fee_per_gas(
                    template.max_priority_fee_per_gas.unwrap_or(ctx.gas.max_priority_fee_per_gas),
                );

                req.calls = calls;

                let is_expiring = matches!(nonce_mode, TempoNonceMode::Expiring);
                let valid_before = match nonce_mode {
                    TempoNonceMode::Protocol => template.valid_before,
                    TempoNonceMode::Parallel(nonce_key) => {
                        req.set_nonce_key(nonce_key);
                        template.valid_before
                    }
                    TempoNonceMode::Expiring => {
                        req.set_nonce_key(TEMPO_EXPIRING_NONCE_KEY);
                        Some(resolve_expiring_valid_before(&template)?)
                    }
                };

                if let Some(fee_token) = template.fee_token {
                    req.set_fee_token(fee_token);
                }
                if let Some(native_multisig) = &native_multisig {
                    req.set_multisig_config_id(native_multisig.config_id);
                }
                if let Some(valid_after) = template.valid_after {
                    let valid_after = NonZeroU64::new(valid_after).ok_or_else(|| {
                        eyre::eyre!("Tempo transactions require `valid_after` to be greater than 0")
                    })?;
                    req.set_valid_after(valid_after);
                }
                if let Some(valid_before) = valid_before {
                    let valid_before = NonZeroU64::new(valid_before).ok_or_else(|| {
                        eyre::eyre!(
                            "Tempo transactions require `valid_before` to be greater than 0"
                        )
                    })?;
                    req.set_valid_before(valid_before);
                }
                if is_expiring {
                    apply_expiring_uniqueness_bump(&mut req, ctx)?;
                }

                // Handle sponsor signing: build a temporary TempoTransaction to
                // compute the fee_payer_signature_hash, sign it, then set on the request.
                if let Some(ref sponsor_ref) = template.sponsor {
                    let temp_tx = req
                        .clone()
                        .build_aa()
                        .map_err(|e| eyre::eyre!("failed to build AA tx for sponsor: {e}"))?;

                    let sponsor = ctx.select_signer(sponsor_ref)?;
                    let sponsor_signer = ctx.accounts.get_by_index(&sponsor.pool, sponsor.index)?;
                    let fee_payer_hash = temp_tx.fee_payer_signature_hash(selected.address);
                    let fee_payer_sig = sponsor_signer.sign_hash_sync(&fee_payer_hash)?;
                    req.set_fee_payer_signature(fee_payer_sig);
                }
            }
            TempoTxType::Legacy => {
                req.set_gas_price(template.gas_price.unwrap_or(ctx.gas.max_fee_per_gas));
                req.set_kind(to);
                req.set_value(value);
                if !input.is_empty() {
                    req.set_input(input);
                }
            }
            TempoTxType::Eip2930 => {
                req.set_gas_price(template.gas_price.unwrap_or(ctx.gas.max_fee_per_gas));
                req.set_access_list(Default::default());
                req.set_kind(to);
                req.set_value(value);
                if !input.is_empty() {
                    req.set_input(input);
                }
            }
            TempoTxType::Eip1559 => {
                req.set_max_fee_per_gas(
                    template.max_fee_per_gas.unwrap_or(ctx.gas.max_fee_per_gas),
                );
                req.set_max_priority_fee_per_gas(
                    template.max_priority_fee_per_gas.unwrap_or(ctx.gas.max_priority_fee_per_gas),
                );
                req.set_kind(to);
                req.set_value(value);
                if !input.is_empty() {
                    req.set_input(input);
                }
            }
        }

        Ok(TxRequest {
            request: req,
            signer_pool: selected.pool,
            signer_index: selected.index,
            key: scheduling_key,
        })
    }

    fn sign_request(
        name: &str,
        request: TempoTransactionRequest,
        signer: &txgen_core::EcdsaSigner,
    ) -> Result<Bytes> {
        sign_tempo_request(name, request, signer)
    }

    fn emit_auto_setup<W: Write>(
        &self,
        ctx: &mut BuildContext<'_>,
        writer: &mut NdjsonWriter<W>,
    ) -> Result<()> {
        for account in ctx.accounts.native_multisig_1_of_1_accounts() {
            if !account.setup.auto_setup {
                continue;
            }
            emit_native_multisig_setup(self, ctx, writer, &account)?;
        }
        Ok(())
    }

    async fn prefetch_nonces(&self, ctx: &mut GenerateContext, rpc: &str) -> Result<()> {
        use alloy_provider::{Provider, ProviderBuilder};
        use eyre::WrapErr;

        let provider = ProviderBuilder::<_, _, Ethereum>::new()
            .connect_http(rpc.parse().wrap_err("invalid RPC URL")?)
            .erased();

        let (accounts, nonces) = ctx.accounts_and_nonces();
        txgen_cli::fetch_protocol_nonces(accounts, nonces, rpc).await?;

        let (spec, accounts, nonces) = ctx.prefetch_state();
        prefetch_parallel_nonces(&provider, accounts, spec, nonces).await?;

        // Keep the provider so build_request can lazy-fetch nonces for any
        // (account, nonce_key) pair not enumerated by prefetch_parallel_nonces
        // (any non-literal `nonce_key` such as `uniform` or `choice`). `set`
        // only ever fails if called twice; the second call is a no-op we
        // accept silently because prefetch is only invoked once per run.
        let _ = self.nonce_rpc.set(provider);

        Ok(())
    }
}

fn emit_native_multisig_setup<W: Write>(
    adapter: &TempoAdapter,
    ctx: &mut BuildContext<'_>,
    writer: &mut NdjsonWriter<W>,
    account: &NativeMultisig1Of1Account,
) -> Result<()> {
    let scheduling_key = account.account.0 .0;
    let nonce = adapter.next_nonce_lazy(ctx, scheduling_key, account.account, U256::ZERO)?;
    let mut req = TempoTransactionRequest::default();
    req.set_chain_id(ctx.chain_id);
    req.set_nonce(nonce);
    req.set_gas_limit(account.setup.setup_gas_limit);
    req.set_max_fee_per_gas(ctx.gas.max_fee_per_gas);
    req.set_max_priority_fee_per_gas(ctx.gas.max_priority_fee_per_gas);
    req.set_from(account.account);
    req.set_kind(TxKind::Call(account.account));
    req.set_value(U256::ZERO);
    if let Some(fee_token) = account.setup.setup_fee_token {
        req.set_fee_token(fee_token);
    }
    req.set_multisig_init(native_multisig_init(account));

    let signer = ctx.accounts.get_by_index(&account.pool, account.index)?;
    let id = format!("native_multisig_1_of_1_setup.{}.{}", account.pool, account.index);
    let raw = sign_tempo_request(&id, req, signer)?;
    writer.write(&GeneratedTx {
        phase: TxPhase::Setup,
        id: Some(id),
        raw,
        submission_keys: vec![SchedulingKey::from(scheduling_key)],
        inclusion_keys: Vec::new(),
    })?;
    Ok(())
}

fn sign_tempo_request(
    name: &str,
    request: TempoTransactionRequest,
    signer: &txgen_core::EcdsaSigner,
) -> Result<Bytes> {
    if request.multisig_init.is_none() && request.multisig_config_id.is_none() {
        return sign_request_default::<TempoAdapter>(name, request, signer);
    }

    let init = request.multisig_init.clone();
    let (account, config_id) = if let Some(init) = init.as_ref() {
        let config_id = init
            .config_id()
            .map_err(|reason| eyre::eyre!("invalid native multisig init: {reason}"))?;
        let account = init
            .account()
            .map_err(|reason| eyre::eyre!("invalid native multisig account: {reason}"))?;
        (account, config_id)
    } else {
        let config_id = request
            .multisig_config_id
            .ok_or_else(|| eyre::eyre!("missing native multisig config id"))?;
        let account = request
            .from()
            .ok_or_else(|| eyre::eyre!("native multisig request must set from account"))?;
        (account, config_id)
    };

    if request.from().is_some_and(|from| from != account) {
        bail!("native multisig request from address does not match derived account");
    }

    let tx = request
        .build_aa()
        .map_err(|e| eyre::eyre!("failed to build AA tx from template '{name}': {e}"))?;
    let digest = multisig_digest(tx.signature_hash(), account, config_id);
    let owner_signature = PrimitiveSignature::Secp256k1(
        signer
            .sign_hash_sync(&digest)
            .map_err(|e| eyre::eyre!("failed to sign native multisig owner digest: {e}"))?,
    )
    .to_bytes();
    let signature = TempoSignature::Multisig(MultisigSignature {
        account,
        config_id,
        signatures: vec![owner_signature],
        init,
    });
    let envelope = TempoTxEnvelope::from(tx.into_signed(signature));
    Ok(Bytes::from(envelope.encoded_2718()))
}

fn native_multisig_init(account: &NativeMultisig1Of1Account) -> InitMultisig {
    InitMultisig {
        salt: account.salt,
        threshold: 1,
        owners: vec![MultisigOwner { owner: account.owner, weight: 1 }],
    }
}

fn resolve_nonce_mode(
    template: &TempoTemplate,
    is_tempo: bool,
    ctx: &mut BuildContext<'_>,
) -> Result<TempoNonceMode> {
    let resolved_nonce_key =
        template.nonce_key.as_ref().map(|nonce_key| ctx.resolve_value(nonce_key)).transpose()?;

    if !is_tempo {
        if template.expiring_nonce || resolved_nonce_key == Some(TEMPO_EXPIRING_NONCE_KEY) {
            bail!("expiring nonce mode is only supported for Tempo transactions");
        }
        return Ok(TempoNonceMode::Protocol);
    }

    if template.expiring_nonce {
        if resolved_nonce_key.is_some() {
            bail!(
                "expiring nonce templates must not set `nonce_key`; txgen sets the reserved key automatically"
            );
        }
        return Ok(TempoNonceMode::Expiring);
    }

    match resolved_nonce_key.unwrap_or(U256::ZERO) {
        key if key == TEMPO_EXPIRING_NONCE_KEY => Ok(TempoNonceMode::Expiring),
        key if key.is_zero() => Ok(TempoNonceMode::Protocol),
        key => Ok(TempoNonceMode::Parallel(key)),
    }
}

fn resolve_expiring_valid_before(template: &TempoTemplate) -> Result<u64> {
    match (template.valid_before, template.valid_for_secs) {
        (Some(_), Some(_)) => {
            bail!(
                "expiring nonce templates must set either `valid_before` or `valid_for_secs`, not both"
            )
        }
        (Some(valid_before), None) => {
            if valid_before == 0 {
                bail!("expiring nonce templates require `valid_before` to be greater than 0");
            }
            Ok(valid_before)
        }
        (None, Some(valid_for_secs)) => {
            if valid_for_secs == 0 {
                bail!("expiring nonce templates require `valid_for_secs` to be greater than 0");
            }
            if valid_for_secs > TEMPO_EXPIRING_NONCE_MAX_EXPIRY_SECS {
                bail!(
                    "expiring nonce templates require `valid_for_secs` <= {} seconds",
                    TEMPO_EXPIRING_NONCE_MAX_EXPIRY_SECS
                );
            }
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_secs())
                .unwrap_or(0)
                .checked_add(valid_for_secs)
                .ok_or_else(|| {
                    eyre::eyre!("expiring nonce `valid_for_secs` overflowed unix timestamp")
                })
        }
        (None, None) => {
            bail!("expiring nonce templates require either `valid_before` or `valid_for_secs`")
        }
    }
}

/// Deterministically perturb fee fields so expiring nonce transactions never
/// produce identical signed payloads within one generation run.
///
/// Tempo expiring nonce replay protection is hash-based, so two otherwise
/// identical transactions from the same sender can collide if their signed
/// payload is identical. txgen uses a local monotonic counter to bump both fee
/// fields before any signatures are produced. Adding the same bump to both
/// fields preserves `max_priority_fee_per_gas <= max_fee_per_gas`.
fn apply_expiring_uniqueness_bump(
    req: &mut TempoTransactionRequest,
    ctx: &mut BuildContext<'_>,
) -> Result<()> {
    let bump = u128::from(ctx.next_nonce(EXPIRING_UNIQUENESS_COUNTER_KEY)) + 1;

    let max_priority_fee_per_gas = req
        .max_priority_fee_per_gas()
        .ok_or_else(|| eyre::eyre!("Tempo expiring transactions require max_priority_fee_per_gas"))?
        .checked_add(bump)
        .ok_or_else(|| {
            eyre::eyre!("expiring nonce max_priority_fee_per_gas overflowed uniqueness bump")
        })?;
    let max_fee_per_gas = req
        .max_fee_per_gas()
        .ok_or_else(|| eyre::eyre!("Tempo expiring transactions require max_fee_per_gas"))?
        .checked_add(bump)
        .ok_or_else(|| eyre::eyre!("expiring nonce max_fee_per_gas overflowed uniqueness bump"))?;

    req.set_max_priority_fee_per_gas(max_priority_fee_per_gas);
    req.set_max_fee_per_gas(max_fee_per_gas);

    Ok(())
}

/// Compute the sender scheduling key used by txgen/bench submission ordering.
///
/// `bench send` serializes transactions that share the same scheduling key and
/// allows different keys to be sent in parallel.
///
/// - Protocol nonces use the sender address directly.
/// - 2D nonce lanes use a hash of `(sender, nonce_key)` so each lane is ordered independently.
/// - Expiring nonces use a fresh random key per transaction so txgen does not artificially
///   serialize them like a 2D lane.
pub(crate) fn compute_scheduling_key(
    sender: Address,
    nonce_mode: TempoNonceMode,
    ctx: &mut BuildContext<'_>,
) -> [u8; 20] {
    match nonce_mode {
        TempoNonceMode::Protocol => sender.0 .0,
        TempoNonceMode::Parallel(nonce_key) => compute_parallel_scheduling_key(sender, nonce_key),
        TempoNonceMode::Expiring => {
            let mut key = [0u8; 20];
            ctx.rng.fill_bytes(&mut key);
            key
        }
    }
}

/// Compute the stable scheduling key for a Tempo 2D nonce lane.
///
/// Transactions from the same sender and `nonce_key` share this key and must be
/// sent sequentially because the lane uses ordered nonces `0, 1, 2, ...`.
pub(crate) fn compute_parallel_scheduling_key(sender: Address, nonce_key: U256) -> [u8; 20] {
    let mut data = [0u8; 52];
    data[..20].copy_from_slice(sender.as_slice());
    data[20..52].copy_from_slice(&nonce_key.to_be_bytes::<32>());
    let hash = keccak256(data);
    let mut key = [0u8; 20];
    key.copy_from_slice(&hash[..20]);
    key
}

/// Resolve call data from the template into (to, value, input, calls).
///
/// For tempo transactions, the result is returned as `calls`; for EVM types
/// it is returned as `(to, value, input)`.
fn resolve_call_data(
    template: &TempoTemplate,
    is_tempo: bool,
    ctx: &mut BuildContext<'_>,
) -> Result<(TxKind, U256, Bytes, Vec<Call>)> {
    if let Some(ref call_defs) = template.calls {
        let mut calls = Vec::with_capacity(call_defs.len());
        for call_def in call_defs {
            let encoded = ctx.encode_call(call_def)?;
            calls.push(Call {
                to: TxKind::Call(encoded.to),
                value: encoded.value,
                input: encoded.input,
            });
        }
        Ok((TxKind::Create, U256::ZERO, Bytes::new(), calls))
    } else if let Some(ref call_def) = template.call {
        let encoded = ctx.encode_call(call_def)?;
        if is_tempo {
            Ok((
                TxKind::Create,
                U256::ZERO,
                Bytes::new(),
                vec![Call {
                    to: TxKind::Call(encoded.to),
                    value: encoded.value,
                    input: encoded.input,
                }],
            ))
        } else {
            Ok((TxKind::Call(encoded.to), encoded.value, encoded.input, Vec::new()))
        }
    } else {
        let to = ctx.resolve_to(&template.to)?;
        let value: U256 = ctx.resolve_value(&template.value)?;
        let input = template
            .input
            .as_ref()
            .map(|input| ctx.resolve_value(input))
            .transpose()?
            .unwrap_or_default();
        if is_tempo {
            Ok((TxKind::Create, U256::ZERO, Bytes::new(), vec![Call { to, value, input }]))
        } else {
            Ok((to, value, input, Vec::new()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::SignableTransaction;
    use alloy_eips::eip2718::Encodable2718;
    use alloy_primitives::Address;
    use rand::{rngs::StdRng, SeedableRng};
    use std::{
        collections::HashMap,
        time::{SystemTime, UNIX_EPOCH},
    };
    use tempo_primitives::TEMPO_TX_TYPE_ID;
    use txgen_core::{
        AccountAddressKind, AccountManager, AccountPoolDef, AccountRef, ArtifactManager, GasConfig,
        GenValue, NativeMultisig1Of1Def, NonceTracker, SelectMode,
    };

    const TEST_MNEMONIC: &str = "test test test test test test test test test test test junk";

    fn sign_and_encode<A: NetworkAdapter>(
        adapter: &A,
        template: A::Template,
        ctx: &mut BuildContext<'_>,
    ) -> Bytes
    where
        <A::Network as alloy_network::Network>::UnsignedTx:
            SignableTransaction<alloy_primitives::Signature>,
        <A::Network as alloy_network::Network>::TxEnvelope: From<alloy_consensus::Signed<<A::Network as alloy_network::Network>::UnsignedTx>>
            + Encodable2718,
    {
        let tx_req = adapter.build_request(template, ctx).unwrap();
        let signer = ctx.accounts.get_by_index(&tx_req.signer_pool, tx_req.signer_index).unwrap();
        A::sign_request("test", tx_req.request, signer).unwrap()
    }

    fn test_accounts() -> AccountManager {
        let mut accounts_map = HashMap::new();
        accounts_map.insert(
            "users".to_string(),
            AccountPoolDef {
                mnemonic: TEST_MNEMONIC.to_string(),
                index: None,
                range: Some([0, 10]),
                address_kind: AccountAddressKind::Signer,
                native_multisig_1_of_1: None,
            },
        );
        AccountManager::from_spec(&accounts_map).unwrap()
    }

    fn test_native_multisig_accounts() -> AccountManager {
        let mut accounts_map = HashMap::new();
        accounts_map.insert(
            "multisigs".to_string(),
            AccountPoolDef {
                mnemonic: TEST_MNEMONIC.to_string(),
                index: None,
                range: Some([0, 2]),
                address_kind: AccountAddressKind::Signer,
                native_multisig_1_of_1: Some(NativeMultisig1Of1Def {
                    auto_setup: true,
                    setup_gas_limit: 300_000,
                    setup_fee_token: None,
                }),
            },
        );
        AccountManager::from_spec(&accounts_map).unwrap()
    }

    fn base_template(tx_type: TempoTxType) -> TempoTemplate {
        TempoTemplate {
            tx_type,
            from: AccountRef { pool: "users".to_string(), select: SelectMode::Index(0) },
            gas_limit: 21000,
            value: GenValue::Literal(U256::from(1000)),
            to: Some(GenValue::Literal(Address::ZERO)),
            input: None,
            call: None,
            gas_price: None,
            max_fee_per_gas: Some(1_000_000_000),
            max_priority_fee_per_gas: Some(1_000_000_000),
            nonce_key: None,
            expiring_nonce: false,
            fee_token: None,
            sponsor: None,
            valid_after: None,
            valid_before: None,
            valid_for_secs: None,
            calls: None,
        }
    }

    fn sponsored_expiring_template() -> TempoTemplate {
        let mut template = base_template(TempoTxType::Tempo);
        template.expiring_nonce = true;
        template.valid_before = Some(1_700_000_000);
        template.sponsor =
            Some(AccountRef { pool: "users".to_string(), select: SelectMode::Index(1) });
        template
    }

    #[test]
    fn test_build_tempo_transfer() {
        let accounts = test_accounts();
        let artifacts = ArtifactManager::empty();
        let gas = GasConfig::default();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);

        let raw =
            sign_and_encode(&TempoAdapter::new(), base_template(TempoTxType::Tempo), &mut ctx);

        assert!(!raw.is_empty());
        assert_eq!(raw[0], TEMPO_TX_TYPE_ID);
    }

    #[test]
    fn test_build_native_multisig_1_of_1_transfer() {
        let accounts = test_native_multisig_accounts();
        let artifacts = ArtifactManager::empty();
        let gas = GasConfig::default();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);
        let multisig = accounts.native_multisig_1_of_1("multisigs", 0).unwrap().unwrap();

        let mut template = base_template(TempoTxType::Tempo);
        template.from = AccountRef { pool: "multisigs".to_string(), select: SelectMode::Index(0) };

        let tx_req = TempoAdapter::new().build_request(template, &mut ctx).unwrap();
        assert_eq!(tx_req.request.from(), Some(multisig.account));
        assert_eq!(tx_req.request.multisig_config_id, Some(multisig.config_id));
        assert_eq!(tx_req.key, multisig.account.0 .0);

        let signer = accounts.get_by_index(&tx_req.signer_pool, tx_req.signer_index).unwrap();
        let raw = TempoAdapter::sign_request("test", tx_req.request, signer).unwrap();

        assert!(!raw.is_empty());
        assert_eq!(raw[0], TEMPO_TX_TYPE_ID);
    }

    #[test]
    fn test_native_multisig_1_of_1_rejects_non_tempo_templates() {
        let accounts = test_native_multisig_accounts();
        let artifacts = ArtifactManager::empty();
        let gas = GasConfig::default();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);
        let mut template = base_template(TempoTxType::Eip1559);
        template.from = AccountRef { pool: "multisigs".to_string(), select: SelectMode::Index(0) };

        let err = match TempoAdapter::new().build_request(template, &mut ctx) {
            Ok(_) => panic!("native multisig EIP-1559 template should fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("can only be used with Tempo transaction templates"));
    }

    #[test]
    fn test_emit_native_multisig_1_of_1_auto_setup() {
        let accounts = test_native_multisig_accounts();
        let artifacts = ArtifactManager::empty();
        let gas = GasConfig::default();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);
        let mut buf = Vec::new();
        let count = {
            let mut writer = NdjsonWriter::new(&mut buf);
            TempoAdapter::new().emit_auto_setup(&mut ctx, &mut writer).unwrap();
            writer.count()
        };
        let output = String::from_utf8(buf).unwrap();
        let rows: Vec<&str> = output.lines().collect();

        assert_eq!(count, 2);
        assert_eq!(rows.len(), 2);
        assert!(rows[0].contains("\"phase\":\"setup\""));
        assert!(rows[0].contains("\"id\":\"native_multisig_1_of_1_setup."));
        assert!(rows[0].contains("\"raw\":\"0x76"));
    }

    #[test]
    fn test_build_tempo_with_parallel_nonce() {
        let accounts = test_accounts();
        let artifacts = ArtifactManager::empty();
        let gas = GasConfig::default();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);

        let mut template = base_template(TempoTxType::Tempo);
        template.nonce_key = Some(GenValue::Literal(U256::from(42)));

        let raw = sign_and_encode(&TempoAdapter::new(), template, &mut ctx);

        assert!(!raw.is_empty());
        assert_eq!(raw[0], TEMPO_TX_TYPE_ID);
    }

    #[test]
    fn test_lazy_fetch_no_provider_falls_back_to_tracker_default() {
        // Without a provider configured, build_request must keep the legacy
        // behaviour: assume `nonce=0` for any unseen scheduling key. This
        // protects callers that build txs without `--rpc` (e.g. unit tests).
        let accounts = test_accounts();
        let artifacts = ArtifactManager::empty();
        let gas = GasConfig::default();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);

        let mut template = base_template(TempoTxType::Tempo);
        template.nonce_key = Some(GenValue::Literal(U256::from(42)));

        let tx_req = TempoAdapter::new().build_request(template, &mut ctx).unwrap();

        assert_eq!(tx_req.request.nonce(), Some(0));
    }

    #[test]
    fn test_delegates_ethereum_types() {
        let accounts = test_accounts();
        let artifacts = ArtifactManager::empty();
        let gas = GasConfig::default();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);

        let raw =
            sign_and_encode(&TempoAdapter::new(), base_template(TempoTxType::Eip1559), &mut ctx);

        assert!(!raw.is_empty());
        assert_eq!(raw[0], 0x02);
    }

    #[test]
    fn test_build_tempo_with_expiring_nonce() {
        let accounts = test_accounts();
        let artifacts = ArtifactManager::empty();
        let gas = GasConfig::default();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let sender = accounts.get_by_index("users", 0).unwrap().address();
        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);

        let mut template = base_template(TempoTxType::Tempo);
        template.expiring_nonce = true;
        template.valid_before = Some(1_700_000_000);

        let tx_req = TempoAdapter::new().build_request(template, &mut ctx).unwrap();

        assert_eq!(tx_req.request.nonce(), Some(0));
        assert_eq!(tx_req.request.nonce_key, Some(TEMPO_EXPIRING_NONCE_KEY));
        assert_eq!(tx_req.request.valid_before.map(NonZeroU64::get), Some(1_700_000_000));
        assert_ne!(tx_req.key, sender.0 .0);
    }

    #[test]
    fn test_expiring_nonce_requests_do_not_share_lane_state() {
        let accounts = test_accounts();
        let artifacts = ArtifactManager::empty();
        let gas = GasConfig::default();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);

        let mut template = base_template(TempoTxType::Tempo);
        template.expiring_nonce = true;
        template.valid_before = Some(1_700_000_000);

        let first = TempoAdapter::new().build_request(template.clone(), &mut ctx).unwrap();
        let second = TempoAdapter::new().build_request(template, &mut ctx).unwrap();

        assert_eq!(first.request.nonce(), Some(0));
        assert_eq!(second.request.nonce(), Some(0));
        assert_ne!(first.key, second.key);
    }

    #[test]
    fn test_expiring_nonce_transactions_get_unique_signed_payloads() {
        let accounts = test_accounts();
        let artifacts = ArtifactManager::empty();
        let gas = GasConfig::default();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);

        let mut template = base_template(TempoTxType::Tempo);
        template.expiring_nonce = true;
        template.valid_before = Some(1_700_000_000);

        let first = sign_and_encode(&TempoAdapter::new(), template.clone(), &mut ctx);
        let second = sign_and_encode(&TempoAdapter::new(), template, &mut ctx);

        assert_ne!(first, second);
    }

    #[test]
    fn test_expiring_nonce_fee_bumps_are_monotonic() {
        let accounts = test_accounts();
        let artifacts = ArtifactManager::empty();
        let gas = GasConfig::default();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);

        let mut template = base_template(TempoTxType::Tempo);
        template.expiring_nonce = true;
        template.valid_before = Some(1_700_000_000);

        let first = TempoAdapter::new().build_request(template.clone(), &mut ctx).unwrap().request;
        let second = TempoAdapter::new().build_request(template, &mut ctx).unwrap().request;

        assert_eq!(first.max_priority_fee_per_gas(), Some(1_000_000_001));
        assert_eq!(first.max_fee_per_gas(), Some(1_000_000_001));
        assert_eq!(second.max_priority_fee_per_gas(), Some(1_000_000_002));
        assert_eq!(second.max_fee_per_gas(), Some(1_000_000_002));
    }

    #[test]
    fn test_expiring_nonce_valid_for_secs_is_resolved_at_build_time() {
        let accounts = test_accounts();
        let artifacts = ArtifactManager::empty();
        let gas = GasConfig::default();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);

        let mut template = base_template(TempoTxType::Tempo);
        template.expiring_nonce = true;
        template.valid_for_secs = Some(25);

        let before = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let tx_req = TempoAdapter::new().build_request(template, &mut ctx).unwrap();
        let after = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        let valid_before = tx_req.request.valid_before.unwrap();
        assert!(valid_before.get() >= before + 25);
        assert!(valid_before.get() <= after + 25);
    }

    #[test]
    fn test_sponsored_expiring_nonce_uniqueness_happens_before_fee_payer_signing() {
        let accounts = test_accounts();
        let artifacts = ArtifactManager::empty();
        let gas = GasConfig::default();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);

        let first = TempoAdapter::new()
            .build_request(sponsored_expiring_template(), &mut ctx)
            .unwrap()
            .request;
        let second = TempoAdapter::new()
            .build_request(sponsored_expiring_template(), &mut ctx)
            .unwrap()
            .request;

        assert_ne!(first.max_fee_per_gas(), second.max_fee_per_gas());
        assert_ne!(
            first.fee_payer_signature, second.fee_payer_signature,
            "fee-payer signature must reflect the per-tx expiring uniqueness bump"
        );
    }

    #[test]
    fn test_expiring_nonce_requires_expiry_field() {
        let accounts = test_accounts();
        let artifacts = ArtifactManager::empty();
        let gas = GasConfig::default();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);

        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);

        let mut template = base_template(TempoTxType::Tempo);
        template.expiring_nonce = true;

        let err = TempoAdapter::new()
            .build_request(template, &mut ctx)
            .err()
            .expect("expiring nonce without expiry should fail");
        assert!(err.to_string().contains(
            "expiring nonce templates require either `valid_before` or `valid_for_secs`"
        ));
    }

    #[test]
    fn test_scheduling_key_parallel_nonce() {
        let sender = Address::repeat_byte(0xab);
        let key1 = compute_parallel_scheduling_key(sender, U256::from(1));
        let key2 = compute_parallel_scheduling_key(sender, U256::from(2));

        assert_ne!(key1, sender.0 .0);
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_scheduling_key_protocol_nonce() {
        let accounts = test_accounts();
        let artifacts = ArtifactManager::empty();
        let gas = GasConfig::default();
        let mut nonces = NonceTracker::new();
        let mut rng = StdRng::seed_from_u64(42);
        let sender = Address::repeat_byte(0xab);
        let mut ctx = BuildContext::new(1, &gas, &accounts, &artifacts, &mut nonces, &mut rng);
        let key = compute_scheduling_key(sender, TempoNonceMode::Protocol, &mut ctx);
        assert_eq!(key, sender.0 .0);
    }
}
