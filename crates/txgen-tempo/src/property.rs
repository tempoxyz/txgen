//! Model-free Tempo/Zone workload generator and live backing-verifier harness.

use std::{
    collections::BTreeSet,
    future::Future,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use aes_gcm::{
    aead::{Aead, OsRng},
    Aes256Gcm, KeyInit, Nonce,
};
use alloy_dyn_abi::{DynSolType, DynSolValue};
use alloy_network::Ethereum;
use alloy_primitives::{keccak256, Address, Bytes, TxKind, B256, U256};
use alloy_rpc_types_eth::{Log, TransactionInput, TransactionRequest};
use alloy_sol_types::{sol, SolCall, SolEvent};
use eyre::{bail, ensure, Result, WrapErr};
use hkdf::Hkdf;
use k256::{
    elliptic_curve::sec1::{FromEncodedPoint, ToEncodedPoint},
    AffinePoint, ProjectivePoint,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::Sha256;
use tokio::time::Instant;
use txgen_cli::sign_standard_request;
use txgen_core::{EcdsaSigner, TxPhase};
use txgen_property::{
    AbiStrategy, CampaignHarness, GenerateContext, SwarmPolicy, VerificationTrigger,
    WorkloadGenerator,
};
use zone_portal_backing::{audit_portal_backing_rpc, PortalBackingReport, PortalBackingRequest};

use crate::zone_auth::{build_token_fields, encode_token_hex, sign_token};

mod u128_decimal {
    use serde::{de::Error, Deserialize, Deserializer, Serializer};
    use serde_json::Value;

    pub fn serialize<S: Serializer>(value: &u128, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u128, D::Error> {
        match Value::deserialize(deserializer)? {
            Value::String(value) => value.parse().map_err(D::Error::custom),
            Value::Number(value) => value
                .as_u64()
                .map(u128::from)
                .ok_or_else(|| D::Error::custom("u128 must be a non-negative integer or string")),
            _ => Err(D::Error::custom("u128 must be an integer or decimal string")),
        }
    }
}

mod optional_u128_decimal {
    use serde::{de::Error, Deserialize, Deserializer, Serializer};
    use serde_json::Value;

    pub fn serialize<S: Serializer>(
        value: &Option<u128>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(value) => serializer.serialize_some(&value.to_string()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<u128>, D::Error> {
        match Option::<Value>::deserialize(deserializer)? {
            None => Ok(None),
            Some(Value::String(value)) => value.parse().map(Some).map_err(D::Error::custom),
            Some(Value::Number(value)) => {
                value.as_u64().map(u128::from).map(Some).ok_or_else(|| {
                    D::Error::custom("u128 must be a non-negative integer or string")
                })
            }
            Some(_) => Err(D::Error::custom("u128 must be an integer or decimal string")),
        }
    }
}

sol! {
    interface PropertyTip20 {
        function balanceOf(address account) external view returns (uint256);
        function allowance(address owner, address spender) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
    }

    interface PropertyZonePortal {
        struct DepositPayload {
            bytes32 ephemeralPubkeyX;
            uint8 ephemeralPubkeyYParity;
            bytes ciphertext;
            bytes12 nonce;
            bytes16 tag;
        }

        function deposit(
            address token,
            uint128 amount,
            uint256 keyIndex,
            DepositPayload encrypted,
            address tempoRefundRecipient
        ) external returns (bytes32);

        function encryptionKeyAtBlock(uint64 tempoBlockNumber)
            external view returns (bytes32 x, uint8 yParity, uint256 keyIndex);

        event DepositMade(
            bytes32 indexed newCurrentDepositQueueHash,
            address indexed sender,
            address token,
            uint128 netAmount,
            uint128 fee,
            uint256 keyIndex,
            bytes32 ephemeralPubkeyX,
            uint8 ephemeralPubkeyYParity,
            bytes ciphertext,
            bytes12 nonce,
            bytes16 tag,
            address tempoRefundRecipient,
            uint64 depositNumber
        );

        event WithdrawalProcessed(
            address indexed to,
            bytes32 indexed senderTag,
            address token,
            uint128 amount,
            bool callbackSuccess
        );

        event WithdrawalBounceBack(
            bytes32 indexed newCurrentDepositQueueHash,
            uint64 indexed fallbackNonce,
            address token,
            uint128 amount,
            uint64 depositNumber
        );
    }

    interface PropertyZoneInbox {
        event TempoAdvanced(
            bytes32 indexed tempoBlockHash,
            uint64 indexed tempoBlockNumber,
            uint256 depositsProcessed,
            bytes32 newProcessedDepositQueueHash,
            uint64 lastProcessedDepositNumber
        );

        event DepositProcessed(
            bytes32 indexed depositHash,
            address indexed sender,
            address indexed to,
            address token,
            uint128 amount,
            bytes32 memo
        );

        event DepositFailed(
            bytes32 indexed depositHash,
            address indexed sender,
            address token,
            uint128 amount
        );

        event DepositRejected(
            bytes32 indexed depositHash,
            address indexed sender,
            uint8 depositType,
            address token,
            uint128 amount,
            address tempoRefundRecipient
        );
    }

    interface PropertyZoneOutbox {
        function requestWithdrawal(
            address token,
            address to,
            uint128 amount,
            bytes32 memo,
            uint64 gasLimit,
            address zoneFallbackRecipient,
            bytes data,
            bytes revealTo
        ) external;

        event WithdrawalRequested(
            uint64 indexed withdrawalIndex,
            address indexed sender,
            address token,
            address to,
            uint128 amount,
            uint128 fee,
            bytes32 memo,
            uint64 gasLimit,
            uint64 fallbackNonce,
            bytes data,
            bytes revealTo
        );
    }
}

/// Canonical ZoneOutbox predeploy.
pub const ZONE_OUTBOX: Address =
    alloy_primitives::address!("0x1c00000000000000000000000000000000000002");

/// Canonical ZoneInbox predeploy.
pub const ZONE_INBOX: Address =
    alloy_primitives::address!("0x1c00000000000000000000000000000000000001");

/// Live RPC endpoint and protocol configuration.
#[derive(Clone, Debug)]
pub struct ZoneLiveConfig {
    /// Tempo L1 HTTP RPC endpoint.
    pub l1_rpc_url: String,
    /// Full operator Zone HTTP RPC used for global verification.
    pub zone_rpc_url: String,
    /// Authenticated redacted Zone RPC used for user-scoped operations.
    pub zone_private_rpc_url: String,
    /// Zone identifier used in private-RPC authorization tokens.
    pub zone_id: u32,
    /// Zone chain ID used for authorization and transaction signing.
    pub zone_chain_id: u64,
    /// L1 ZonePortal address.
    pub portal: Address,
    /// TIP-20 address shared by Tempo and the Zone.
    pub token: Address,
    /// ZoneOutbox address.
    pub outbox: Address,
    /// Fixed transaction gas limit. Raw ABI actions still reach execution.
    pub transaction_gas_limit: u64,
    /// Maximum wait for a cross-layer liability to reach a terminal state.
    pub settlement_timeout: Duration,
    /// Poll interval while waiting for terminal lifecycle evidence.
    pub settlement_poll_interval: Duration,
    /// First L1 block covering the Portal's complete event history.
    pub l1_from_block: u64,
    /// First Zone block covering the Inbox/Outbox complete event history.
    pub zone_from_block: u64,
}

impl ZoneLiveConfig {
    /// Construct configuration with protocol defaults.
    pub fn new(
        l1_rpc_url: String,
        zone_rpc_url: String,
        zone_private_rpc_url: String,
        zone_id: u32,
        zone_chain_id: u64,
        portal: Address,
        token: Address,
    ) -> Self {
        Self {
            l1_rpc_url,
            zone_rpc_url,
            zone_private_rpc_url,
            zone_id,
            zone_chain_id,
            portal,
            token,
            outbox: ZONE_OUTBOX,
            transaction_gas_limit: 5_000_000,
            settlement_timeout: Duration::from_secs(120),
            settlement_poll_interval: Duration::from_millis(500),
            l1_from_block: 0,
            zone_from_block: 0,
        }
    }
}

/// Optional behavior families selected independently for one case.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ZoneActionKind {
    /// L1 Portal deposit.
    Deposit,
    /// Zone withdrawal request.
    Withdraw,
}

/// Swarm-selected interpretation of ABI-fuzz amount entropy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ZoneAmountMode {
    /// Submit the generated uint128 unchanged, including invalid values.
    Raw,
    /// Normalize the generated value into the currently observed spendable balance.
    Spendable,
}

/// One concrete replayable protocol action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum ZoneAction {
    /// Deposit the raw ABI-generated gross amount.
    Deposit {
        /// Raw ABI-fuzz entropy used to derive the submitted amount.
        #[serde(with = "u128_decimal")]
        raw_amount: u128,
        /// Whether execution submits the raw value or normalizes it to observed balance.
        amount_mode: ZoneAmountMode,
    },
    /// Withdraw the raw ABI-generated principal to the same account.
    Withdraw {
        /// Raw ABI-fuzz entropy used to derive the submitted amount.
        #[serde(with = "u128_decimal")]
        raw_amount: u128,
        /// Whether execution submits the raw value or normalizes it to observed balance.
        amount_mode: ZoneAmountMode,
    },
}

/// Per-case randomized swarm configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ZoneSwarm {
    /// Optional independently selected action families.
    pub actions: BTreeSet<ZoneActionKind>,
    /// ABI-fuzz integer generator selected for this case.
    pub abi_strategy: AbiStrategy,
    /// Amount interpretation selected for this case.
    pub amount_mode: ZoneAmountMode,
}

/// Stateless ABI-fuzz/swarm workload generator.
#[derive(Clone, Copy, Debug, Default)]
pub struct ZoneWorkload;

impl WorkloadGenerator for ZoneWorkload {
    const NAME: &'static str = "zone-backing";
    const VERSION: &'static str = "2";

    type Swarm = ZoneSwarm;
    type ActionKind = ZoneActionKind;
    type Action = ZoneAction;

    fn generate_swarm(
        &self,
        rng: &mut dyn rand::RngCore,
        policy: &SwarmPolicy,
    ) -> Result<Self::Swarm> {
        let actions = policy
            .subset(&[ZoneActionKind::Deposit, ZoneActionKind::Withdraw], rng)
            .into_iter()
            .collect();
        let abi_strategy = *policy
            .choose(&[AbiStrategy::Random, AbiStrategy::Echidna], rng)
            .expect("ABI strategy set is non-empty");
        let amount_mode = *policy
            .choose(&[ZoneAmountMode::Raw, ZoneAmountMode::Spendable], rng)
            .expect("amount mode set is non-empty");
        Ok(ZoneSwarm { actions, abi_strategy, amount_mode })
    }

    fn enabled_actions(&self, swarm: &Self::Swarm) -> Vec<Self::ActionKind> {
        swarm.actions.iter().copied().collect()
    }

    fn generate_action(
        &self,
        swarm: &Self::Swarm,
        kind: &Self::ActionKind,
        context: &mut GenerateContext<'_>,
    ) -> Result<Self::Action> {
        let raw_amount = match context.abi_value(swarm.abi_strategy, &DynSolType::Uint(128), None) {
            DynSolValue::Uint(value, 128) => value.to::<u128>(),
            value => bail!("ABI generator returned unexpected uint128 value {value:?}"),
        };
        Ok(match kind {
            ZoneActionKind::Deposit => {
                ZoneAction::Deposit { raw_amount, amount_mode: swarm.amount_mode }
            }
            ZoneActionKind::Withdraw => {
                ZoneAction::Withdraw { raw_amount, amount_mode: swarm.amount_mode }
            }
        })
    }
}

/// Layer on which an action executes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ZoneLayer {
    /// Tempo L1.
    Tempo,
    /// Zone L2.
    Zone,
}

/// Observed transaction outcome; this is receipt evidence, not a prediction.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ZoneOutcome {
    /// Transaction was included successfully.
    Success,
    /// Submission was rejected or the included transaction reverted.
    Revert,
}

/// Origin event fields used to correlate one submitted action across layers.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ZoneLifecycleOrigin {
    /// Portal deposit queue position.
    Deposit { deposit_number: u64, deposit_hash: B256 },
    /// Zone outbox position and the public correlation values carried to Tempo.
    Withdrawal { withdrawal_index: u64, fallback_nonce: u64, sender_tag: B256 },
}

/// Secret-free transaction execution result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ZoneExecutionTrace {
    /// Execution layer.
    pub layer: ZoneLayer,
    /// Submitted transaction hash, absent when rejected before acceptance.
    pub transaction_hash: Option<B256>,
    /// Actual execution classification.
    pub outcome: ZoneOutcome,
    /// Exact protocol amount submitted after applying the swarm's amount mode.
    #[serde(with = "optional_u128_decimal")]
    pub submitted_amount: Option<u128>,
    /// Correlation fields decoded from the successful origin event.
    pub lifecycle: Option<ZoneLifecycleOrigin>,
    /// Actual receipt JSON, absent when submission was rejected.
    pub receipt: Option<Value>,
}

/// Chain-derived evidence that an action no longer has an in-flight liability.
#[derive(Clone, Debug, Serialize)]
pub struct ZoneTerminalEvidence {
    /// Action receipt used to correlate the lifecycle wait.
    pub transaction_hash: Option<B256>,
    /// Correlation fields decoded from the origin event.
    pub lifecycle: Option<ZoneLifecycleOrigin>,
    /// Whether the action reverted, or its liability class drained.
    pub terminal_reason: String,
    /// Exact cross-layer events proving this action reached the stated transition.
    pub terminal_events: Vec<Value>,
    /// Complete backing report at the terminal observation.
    pub backing: PortalBackingReport,
}

/// Execution and verification boundary used by the campaign harness.
pub trait ZonePropertyBackend {
    /// Ensure the L1 Portal has sufficient token allowance.
    fn ensure_approvals(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Execute one raw workload action.
    fn execute<'a>(
        &'a mut self,
        action: &'a ZoneAction,
    ) -> impl Future<Output = Result<ZoneExecutionTrace>> + Send + 'a;

    /// Wait for an actual terminal liability observation.
    fn await_terminal<'a>(
        &'a mut self,
        action: &'a ZoneAction,
        trace: &'a ZoneExecutionTrace,
    ) -> impl Future<Output = Result<Option<ZoneTerminalEvidence>>> + Send + 'a;

    /// Run the shared chain-derived Portal backing verifier.
    fn verify_backing(&mut self) -> impl Future<Output = Result<PortalBackingReport>> + Send;
}

/// Adapter from a Zone backend to the generic model-free campaign harness.
#[derive(Debug)]
pub struct ZonePropertyHarness<B> {
    backend: B,
    approvals_ready: bool,
}

impl<B> ZonePropertyHarness<B> {
    /// Wrap a live or test backend.
    pub fn new(backend: B) -> Self {
        Self { backend, approvals_ready: false }
    }
}

impl<B> CampaignHarness<ZoneWorkload> for ZonePropertyHarness<B>
where
    B: ZonePropertyBackend + Send,
{
    type Trace = ZoneExecutionTrace;
    type TerminalEvidence = ZoneTerminalEvidence;
    type Verification = PortalBackingReport;

    async fn reset_case(&mut self) -> Result<()> {
        if !self.approvals_ready {
            self.backend.ensure_approvals().await?;
            self.approvals_ready = true;
        }
        Ok(())
    }

    async fn execute(&mut self, action: &ZoneAction) -> Result<ZoneExecutionTrace> {
        self.backend.execute(action).await
    }

    async fn await_terminal(
        &mut self,
        action: &ZoneAction,
        trace: &ZoneExecutionTrace,
    ) -> Result<Option<ZoneTerminalEvidence>> {
        self.backend.await_terminal(action, trace).await
    }

    async fn verify(&mut self, _trigger: VerificationTrigger) -> Result<PortalBackingReport> {
        self.backend.verify_backing().await
    }

    fn violation(&self, verification: &PortalBackingReport) -> Option<String> {
        (!verification.is_solvent()).then(|| {
            format!("Portal is underbacked by {} base units", verification.backing_deficit)
        })
    }
}

#[derive(Clone, Debug)]
struct ZoneAuth {
    signer: EcdsaSigner,
    zone_id: u32,
    chain_id: u64,
}

#[derive(Clone, Debug)]
struct JsonRpcClient {
    http: reqwest::Client,
    url: String,
    auth: Option<ZoneAuth>,
}

impl JsonRpcClient {
    fn new(url: String) -> Self {
        Self { http: reqwest::Client::new(), url, auth: None }
    }

    fn with_zone_auth(url: String, signer: EcdsaSigner, zone_id: u32, chain_id: u64) -> Self {
        Self {
            http: reqwest::Client::new(),
            url,
            auth: Some(ZoneAuth { signer, zone_id, chain_id }),
        }
    }

    async fn request<T: DeserializeOwned>(&self, method: &str, params: Value) -> Result<T> {
        let mut request = self.http.post(&self.url).json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }));
        if let Some(auth) = &self.auth {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .wrap_err("system clock is before the Unix epoch")?
                .as_secs();
            let fields = build_token_fields(
                auth.zone_id,
                auth.chain_id,
                now.saturating_sub(5),
                now.saturating_add(600),
            );
            let token = encode_token_hex(&sign_token(&auth.signer, &fields)?);
            request = request.header("X-Authorization-Token", token);
        }
        let response =
            request.send().await.wrap_err_with(|| format!("failed to call RPC method {method}"))?;
        let status = response.status();
        ensure!(status.is_success(), "RPC method {method} returned HTTP {status}");
        let envelope: RpcEnvelope = response
            .json()
            .await
            .wrap_err_with(|| format!("invalid JSON-RPC response for {method}"))?;
        if let Some(error) = envelope.error {
            bail!("RPC method {method} failed ({}): {}", error.code, error.message);
        }
        serde_json::from_value(envelope.result)
            .wrap_err_with(|| format!("invalid JSON-RPC result for {method}"))
    }

    async fn word(&self, from: Address, to: Address, data: Bytes) -> Result<U256> {
        let output = self.call(from, to, data).await?;
        ensure!(output.len() >= 32, "eth_call returned less than one ABI word");
        Ok(U256::from_be_slice(&output[output.len() - 32..]))
    }

    async fn call(&self, from: Address, to: Address, data: Bytes) -> Result<Bytes> {
        self.request("eth_call", json!([{"from": from, "to": to, "data": data}, "latest"])).await
    }

    async fn event_logs(
        &self,
        address: Address,
        from_block: u64,
        topic0: B256,
    ) -> Result<Vec<Log>> {
        self.request(
            "eth_getLogs",
            json!([{
                "address": address,
                "fromBlock": format!("0x{from_block:x}"),
                "toBlock": "latest",
                "topics": [topic0],
            }]),
        )
        .await
    }
}

#[derive(Debug, Deserialize)]
struct RpcEnvelope {
    #[serde(default)]
    result: Value,
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

/// Native live implementation using signed transactions and the shared verifier.
#[derive(Clone, Debug)]
pub struct LiveZoneBackend {
    config: ZoneLiveConfig,
    signer: EcdsaSigner,
    account: Address,
    l1: JsonRpcClient,
    operator_zone: JsonRpcClient,
    user_zone: JsonRpcClient,
    l1_chain_id: u64,
}

impl LiveZoneBackend {
    /// Connect to endpoints, validate chain IDs, and prepare a live backend.
    pub async fn connect(config: ZoneLiveConfig, signer: EcdsaSigner) -> Result<Self> {
        let account = signer.address();
        let l1 = JsonRpcClient::new(config.l1_rpc_url.clone());
        let operator_zone = JsonRpcClient::new(config.zone_rpc_url.clone());
        let user_zone = JsonRpcClient::with_zone_auth(
            config.zone_private_rpc_url.clone(),
            signer.clone(),
            config.zone_id,
            config.zone_chain_id,
        );
        let l1_chain_id = rpc_u64(&l1, "eth_chainId", json!([])).await?;
        let zone_chain_id = rpc_u64(&user_zone, "eth_chainId", json!([])).await?;
        ensure!(
            zone_chain_id == config.zone_chain_id,
            "redacted Zone RPC chain ID {zone_chain_id} does not match configured {}",
            config.zone_chain_id
        );
        eprintln!(
            "[zone-property] connected account={account} l1_chain_id={l1_chain_id} \
             zone_id={} zone_chain_id={zone_chain_id}",
            config.zone_id
        );
        Ok(Self { config, signer, account, l1, operator_zone, user_zone, l1_chain_id })
    }

    fn backing_request(&self) -> PortalBackingRequest {
        PortalBackingRequest {
            portal: self.config.portal,
            token: self.config.token,
            l1_from_block: self.config.l1_from_block,
            zone_from_block: self.config.zone_from_block,
        }
    }

    async fn audit(&self) -> Result<PortalBackingReport> {
        audit_portal_backing_rpc(
            &self.config.l1_rpc_url,
            &self.config.zone_rpc_url,
            self.backing_request(),
        )
        .await
    }

    async fn matching_event<E>(
        &self,
        client: &JsonRpcClient,
        address: Address,
        from_block: u64,
        mut matches: impl FnMut(&E) -> bool,
    ) -> Result<Option<Value>>
    where
        E: SolEvent,
    {
        for log in client.event_logs(address, from_block, E::SIGNATURE_HASH).await? {
            if let Ok(decoded) = E::decode_log(&log.inner) {
                if matches(&decoded.data) {
                    return serde_json::to_value(log)
                        .wrap_err("failed to serialize correlated lifecycle event")
                        .map(Some);
                }
            }
        }
        Ok(None)
    }

    async fn deposit_terminal_event(&self, deposit_hash: B256) -> Result<Option<(String, Value)>> {
        if let Some(event) = self
            .matching_event::<PropertyZoneInbox::DepositProcessed>(
                &self.operator_zone,
                ZONE_INBOX,
                self.config.zone_from_block,
                |event| event.depositHash == deposit_hash,
            )
            .await?
        {
            return Ok(Some(("deposit_processed".to_string(), event)));
        }
        if let Some(event) = self
            .matching_event::<PropertyZoneInbox::DepositFailed>(
                &self.operator_zone,
                ZONE_INBOX,
                self.config.zone_from_block,
                |event| event.depositHash == deposit_hash,
            )
            .await?
        {
            return Ok(Some(("deposit_failed".to_string(), event)));
        }
        if let Some(event) = self
            .matching_event::<PropertyZoneInbox::DepositRejected>(
                &self.operator_zone,
                ZONE_INBOX,
                self.config.zone_from_block,
                |event| event.depositHash == deposit_hash,
            )
            .await?
        {
            return Ok(Some(("deposit_rejected".to_string(), event)));
        }
        Ok(None)
    }

    async fn withdrawal_terminal_events(
        &self,
        sender_tag: B256,
        fallback_nonce: u64,
    ) -> Result<Option<(String, Vec<Value>)>> {
        let processed = self
            .matching_event::<PropertyZonePortal::WithdrawalProcessed>(
                &self.l1,
                self.config.portal,
                self.config.l1_from_block,
                |event| event.senderTag == sender_tag,
            )
            .await?;
        let Some(processed_log) = processed else {
            return Ok(None);
        };

        let decoded: Log = serde_json::from_value(processed_log.clone())
            .wrap_err("correlated WithdrawalProcessed event is invalid")?;
        let processed_transaction = decoded.transaction_hash.ok_or_else(|| {
            eyre::eyre!("correlated WithdrawalProcessed event is missing its transaction hash")
        })?;
        let processed_event = PropertyZonePortal::WithdrawalProcessed::decode_log(&decoded.inner)
            .wrap_err("failed to decode correlated WithdrawalProcessed event")?;
        if processed_event.data.callbackSuccess {
            return Ok(Some(("withdrawal_paid".to_string(), vec![processed_log])));
        }

        let mut bounceback_deposit = None;
        let bounceback_logs = self
            .l1
            .event_logs(
                self.config.portal,
                self.config.l1_from_block,
                PropertyZonePortal::WithdrawalBounceBack::SIGNATURE_HASH,
            )
            .await?;
        for log in bounceback_logs {
            let Ok(event) = PropertyZonePortal::WithdrawalBounceBack::decode_log(&log.inner) else {
                continue;
            };
            if event.data.fallbackNonce == fallback_nonce
                && log.transaction_hash == Some(processed_transaction)
            {
                bounceback_deposit = Some((event.data.depositNumber, serde_json::to_value(log)?));
                break;
            }
        }
        let Some((deposit_number, bounceback_log)) = bounceback_deposit else {
            return Ok(None);
        };

        let advanced = self
            .matching_event::<PropertyZoneInbox::TempoAdvanced>(
                &self.operator_zone,
                ZONE_INBOX,
                self.config.zone_from_block,
                |event| event.lastProcessedDepositNumber >= deposit_number,
            )
            .await?;
        let Some(advanced_log) = advanced else {
            return Ok(None);
        };
        Ok(Some((
            "withdrawal_bounceback_processed".to_string(),
            vec![processed_log, bounceback_log, advanced_log],
        )))
    }

    async fn ensure_allowance(
        &self,
        client: &JsonRpcClient,
        chain_id: u64,
        spender: Address,
        layer: ZoneLayer,
    ) -> Result<()> {
        let allowance = client
            .word(
                self.account,
                self.config.token,
                Bytes::from(
                    PropertyTip20::allowanceCall { owner: self.account, spender }.abi_encode(),
                ),
            )
            .await?;
        if allowance >= U256::from(u128::MAX) {
            return Ok(());
        }
        let data =
            Bytes::from(PropertyTip20::approveCall { spender, amount: U256::MAX }.abi_encode());
        let receipt_client = match layer {
            ZoneLayer::Tempo => &self.l1,
            ZoneLayer::Zone => &self.operator_zone,
        };
        let trace = self
            .send_transaction(
                client,
                receipt_client,
                chain_id,
                self.config.token,
                data,
                layer,
                None,
            )
            .await?;
        ensure!(trace.outcome == ZoneOutcome::Success, "{layer:?} approval reverted");
        Ok(())
    }

    async fn resolve_amount(
        &self,
        client: &JsonRpcClient,
        raw_amount: u128,
        mode: ZoneAmountMode,
    ) -> Result<u128> {
        if mode == ZoneAmountMode::Raw {
            return Ok(raw_amount);
        }
        let balance = client
            .word(
                self.account,
                self.config.token,
                Bytes::from(PropertyTip20::balanceOfCall { account: self.account }.abi_encode()),
            )
            .await?;
        if balance.is_zero() {
            return Ok(raw_amount);
        }
        let spendable = balance.min(U256::from(u128::MAX));
        Ok(((U256::from(raw_amount) % spendable) + U256::from(1_u8)).to::<u128>())
    }

    async fn encrypted_deposit_payload(
        &self,
    ) -> Result<(U256, PropertyZonePortal::DepositPayload)> {
        let block_number = rpc_u64(&self.l1, "eth_blockNumber", json!([])).await?;
        let call = PropertyZonePortal::encryptionKeyAtBlockCall { tempoBlockNumber: block_number };
        let output =
            self.l1.call(self.account, self.config.portal, Bytes::from(call.abi_encode())).await?;
        let key = PropertyZonePortal::encryptionKeyAtBlockCall::abi_decode_returns(&output)
            .wrap_err("failed to decode active Portal encryption key")?;

        let mut encoded_key = [0_u8; 33];
        encoded_key[0] = key.yParity;
        encoded_key[1..].copy_from_slice(key.x.as_slice());
        let encoded_point = k256::EncodedPoint::from_bytes(encoded_key)
            .wrap_err("Portal returned an invalid encryption key encoding")?;
        let sequencer_key =
            Option::<AffinePoint>::from(AffinePoint::from_encoded_point(&encoded_point))
                .ok_or_else(|| {
                    eyre::eyre!("Portal returned an encryption key off the secp256k1 curve")
                })?;

        let ephemeral_key = k256::SecretKey::random(&mut OsRng);
        let ephemeral_scalar = *ephemeral_key.to_nonzero_scalar();
        let ephemeral_public = AffinePoint::from(ProjectivePoint::GENERATOR * ephemeral_scalar);
        let ephemeral_encoded = ephemeral_public.to_encoded_point(true);
        let ephemeral_x = B256::from_slice(
            ephemeral_encoded
                .x()
                .ok_or_else(|| eyre::eyre!("ephemeral key has no x coordinate"))?,
        );
        let ephemeral_y_parity = ephemeral_encoded.as_bytes()[0];

        let shared = AffinePoint::from(ProjectivePoint::from(sequencer_key) * ephemeral_scalar);
        let shared_encoded = shared.to_encoded_point(true);
        let shared_x_source = shared_encoded
            .x()
            .ok_or_else(|| eyre::eyre!("ECDH shared point has no x coordinate"))?;
        let mut shared_x = [0_u8; 32];
        shared_x.copy_from_slice(shared_x_source);

        let mut info = [0_u8; 104];
        info[..20].copy_from_slice(self.config.portal.as_slice());
        info[20..52].copy_from_slice(&key.keyIndex.to_be_bytes::<32>());
        info[52..84].copy_from_slice(ephemeral_x.as_slice());
        info[84..].copy_from_slice(self.account.as_slice());
        let hkdf = Hkdf::<Sha256>::new(Some(b"ecies-aes-key"), &shared_x);
        let mut aes_key = [0_u8; 32];
        hkdf.expand(&info, &mut aes_key)
            .map_err(|_| eyre::eyre!("failed to derive deposit encryption key"))?;

        let mut plaintext = [0_u8; 64];
        plaintext[..20].copy_from_slice(self.account.as_slice());
        let nonce_bytes: [u8; 12] = rand::random();
        let nonce = Nonce::from(nonce_bytes);
        let encrypted = Aes256Gcm::new((&aes_key).into())
            .encrypt(&nonce, plaintext.as_ref())
            .map_err(|_| eyre::eyre!("failed to encrypt deposit payload"))?;
        ensure!(encrypted.len() == 80, "unexpected encrypted deposit length");
        let ciphertext = Bytes::copy_from_slice(&encrypted[..64]);
        let tag: [u8; 16] = encrypted[64..]
            .try_into()
            .map_err(|_| eyre::eyre!("deposit authentication tag is not 16 bytes"))?;

        Ok((
            key.keyIndex,
            PropertyZonePortal::DepositPayload {
                ephemeralPubkeyX: ephemeral_x,
                ephemeralPubkeyYParity: ephemeral_y_parity,
                ciphertext,
                nonce: nonce_bytes.into(),
                tag: tag.into(),
            },
        ))
    }

    async fn send_transaction(
        &self,
        submission_client: &JsonRpcClient,
        receipt_client: &JsonRpcClient,
        chain_id: u64,
        to: Address,
        input: Bytes,
        layer: ZoneLayer,
        submitted_amount: Option<u128>,
    ) -> Result<ZoneExecutionTrace> {
        let nonce =
            rpc_u64(submission_client, "eth_getTransactionCount", json!([self.account, "pending"]))
                .await?;
        let gas_price = submission_client.request::<U256>("eth_gasPrice", json!([])).await?;
        let gas_price = u256_to_u128(gas_price, "gas price")?;
        let request = TransactionRequest {
            from: Some(self.account),
            to: Some(TxKind::Call(to)),
            gas_price: Some(gas_price),
            gas: Some(self.config.transaction_gas_limit),
            input: TransactionInput::new(input),
            nonce: Some(nonce),
            chain_id: Some(chain_id),
            ..TransactionRequest::default()
        };
        let signed = sign_standard_request::<Ethereum>(
            "zone-property".to_string(),
            TxPhase::Workload,
            request,
            self.signer.clone(),
            self.account.0 .0,
            Vec::new(),
        )?;
        let transaction_hash = match submission_client
            .request::<B256>("eth_sendRawTransaction", json!([signed.raw]))
            .await
        {
            Ok(hash) => hash,
            Err(error) if is_execution_rejection(&error) => {
                return Ok(ZoneExecutionTrace {
                    layer,
                    transaction_hash: None,
                    outcome: ZoneOutcome::Revert,
                    submitted_amount,
                    lifecycle: None,
                    receipt: None,
                });
            }
            Err(error) => return Err(error),
        };
        eprintln!("[zone-property] submitted layer={layer:?} tx={transaction_hash}");

        let deadline = Instant::now() + self.config.settlement_timeout;
        loop {
            let receipt: Value = receipt_client
                .request("eth_getTransactionReceipt", json!([transaction_hash]))
                .await?;
            if !receipt.is_null() {
                let status = receipt
                    .get("status")
                    .and_then(Value::as_str)
                    .ok_or_else(|| eyre::eyre!("transaction receipt is missing status"))?;
                let outcome = if parse_quantity(status)? == 1 {
                    ZoneOutcome::Success
                } else {
                    ZoneOutcome::Revert
                };
                return Ok(ZoneExecutionTrace {
                    layer,
                    transaction_hash: Some(transaction_hash),
                    outcome,
                    submitted_amount,
                    lifecycle: None,
                    receipt: Some(receipt),
                });
            }
            ensure!(
                Instant::now() < deadline,
                "timed out waiting for transaction receipt {transaction_hash}"
            );
            tokio::time::sleep(self.config.settlement_poll_interval).await;
        }
    }
}

impl ZonePropertyBackend for LiveZoneBackend {
    async fn ensure_approvals(&mut self) -> Result<()> {
        self.ensure_allowance(&self.l1, self.l1_chain_id, self.config.portal, ZoneLayer::Tempo)
            .await?;
        self.ensure_allowance(
            &self.user_zone,
            self.config.zone_chain_id,
            self.config.outbox,
            ZoneLayer::Zone,
        )
        .await
    }

    async fn execute(&mut self, action: &ZoneAction) -> Result<ZoneExecutionTrace> {
        match *action {
            ZoneAction::Deposit { raw_amount, amount_mode } => {
                let amount = self.resolve_amount(&self.l1, raw_amount, amount_mode).await?;
                let (key_index, encrypted) = self.encrypted_deposit_payload().await?;
                let data = Bytes::from(
                    PropertyZonePortal::depositCall {
                        token: self.config.token,
                        amount,
                        keyIndex: key_index,
                        encrypted,
                        tempoRefundRecipient: self.account,
                    }
                    .abi_encode(),
                );
                let mut trace = self
                    .send_transaction(
                        &self.l1,
                        &self.l1,
                        self.l1_chain_id,
                        self.config.portal,
                        data,
                        ZoneLayer::Tempo,
                        Some(amount),
                    )
                    .await?;
                trace.lifecycle = origin_lifecycle(
                    action,
                    self.account,
                    trace.transaction_hash,
                    trace.receipt.as_ref(),
                )?;
                Ok(trace)
            }
            ZoneAction::Withdraw { raw_amount, amount_mode } => {
                let amount = self.resolve_amount(&self.user_zone, raw_amount, amount_mode).await?;
                let data = Bytes::from(
                    PropertyZoneOutbox::requestWithdrawalCall {
                        token: self.config.token,
                        to: self.account,
                        amount,
                        memo: B256::ZERO,
                        gasLimit: 0,
                        zoneFallbackRecipient: self.account,
                        data: Bytes::new(),
                        revealTo: Bytes::new(),
                    }
                    .abi_encode(),
                );
                let mut trace = self
                    .send_transaction(
                        &self.user_zone,
                        &self.operator_zone,
                        self.config.zone_chain_id,
                        self.config.outbox,
                        data,
                        ZoneLayer::Zone,
                        Some(amount),
                    )
                    .await?;
                trace.lifecycle = origin_lifecycle(
                    action,
                    self.account,
                    trace.transaction_hash,
                    trace.receipt.as_ref(),
                )?;
                Ok(trace)
            }
        }
    }

    async fn await_terminal(
        &mut self,
        _action: &ZoneAction,
        trace: &ZoneExecutionTrace,
    ) -> Result<Option<ZoneTerminalEvidence>> {
        if trace.outcome == ZoneOutcome::Revert {
            return Ok(Some(ZoneTerminalEvidence {
                transaction_hash: trace.transaction_hash,
                lifecycle: trace.lifecycle.clone(),
                terminal_reason: "transaction_reverted".to_string(),
                terminal_events: Vec::new(),
                backing: self.audit().await?,
            }));
        }
        let lifecycle = trace.lifecycle.as_ref().ok_or_else(|| {
            eyre::eyre!("successful transaction receipt is missing its lifecycle origin event")
        })?;
        let deadline = Instant::now() + self.config.settlement_timeout;
        loop {
            let terminal = match lifecycle {
                ZoneLifecycleOrigin::Deposit { deposit_hash, .. } => self
                    .deposit_terminal_event(*deposit_hash)
                    .await?
                    .map(|(reason, event)| (reason, vec![event])),
                ZoneLifecycleOrigin::Withdrawal { fallback_nonce, sender_tag, .. } => {
                    self.withdrawal_terminal_events(*sender_tag, *fallback_nonce).await?
                }
            };
            if let Some((terminal_reason, terminal_events)) = terminal {
                eprintln!(
                    "[zone-property] terminal reason={terminal_reason} lifecycle={lifecycle:?} events={}",
                    terminal_events.len()
                );
                return Ok(Some(ZoneTerminalEvidence {
                    transaction_hash: trace.transaction_hash,
                    lifecycle: trace.lifecycle.clone(),
                    terminal_reason,
                    terminal_events,
                    backing: self.audit().await?,
                }));
            }
            if Instant::now() >= deadline {
                let backing = self.audit().await?;
                bail!(
                    "timed out waiting for lifecycle {lifecycle:?}; pending deposits={}, pending withdrawals={}",
                    backing.pending_deposit_liability,
                    backing.pending_withdrawal_liability
                );
            }
            tokio::time::sleep(self.config.settlement_poll_interval).await;
        }
    }

    async fn verify_backing(&mut self) -> Result<PortalBackingReport> {
        let report = self.audit().await?;
        eprintln!(
            "[zone-property] verify l1={}@{} zone={}@{} portal_balance={} required_backing={} deficit={}",
            report.l1_snapshot_block,
            report.l1_snapshot_hash,
            report.zone_snapshot_block,
            report.zone_snapshot_hash,
            report.portal_balance,
            report.required_backing,
            report.backing_deficit,
        );
        Ok(report)
    }
}

fn origin_lifecycle(
    action: &ZoneAction,
    account: Address,
    transaction_hash: Option<B256>,
    receipt: Option<&Value>,
) -> Result<Option<ZoneLifecycleOrigin>> {
    let Some(logs) = receipt.and_then(|value| value.get("logs")).and_then(Value::as_array) else {
        return Ok(None);
    };
    for value in logs {
        let log: Log = serde_json::from_value(value.clone())
            .wrap_err("transaction receipt contains an invalid log")?;
        match action {
            ZoneAction::Deposit { .. } => {
                if let Ok(event) = PropertyZonePortal::DepositMade::decode_log(&log.inner) {
                    return Ok(Some(ZoneLifecycleOrigin::Deposit {
                        deposit_number: event.data.depositNumber,
                        deposit_hash: event.data.newCurrentDepositQueueHash,
                    }));
                }
            }
            ZoneAction::Withdraw { .. } => {
                if let Ok(event) = PropertyZoneOutbox::WithdrawalRequested::decode_log(&log.inner) {
                    let transaction_hash = transaction_hash.ok_or_else(|| {
                        eyre::eyre!("successful withdrawal receipt is missing its transaction hash")
                    })?;
                    return Ok(Some(ZoneLifecycleOrigin::Withdrawal {
                        withdrawal_index: event.data.withdrawalIndex,
                        fallback_nonce: event.data.fallbackNonce,
                        sender_tag: withdrawal_sender_tag(
                            account,
                            transaction_hash,
                            event.data.fallbackNonce,
                        ),
                    }));
                }
            }
        }
    }
    Ok(None)
}

fn withdrawal_sender_tag(account: Address, transaction_hash: B256, fallback_nonce: u64) -> B256 {
    let mut preimage = [0_u8; 60];
    preimage[..20].copy_from_slice(account.as_slice());
    preimage[20..52].copy_from_slice(transaction_hash.as_slice());
    preimage[52..].copy_from_slice(&fallback_nonce.to_be_bytes());
    keccak256(preimage)
}

fn is_execution_rejection(error: &eyre::Report) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    [
        "execution reverted",
        "revert",
        "insufficient funds",
        "insufficient allowance",
        "transfer amount exceeds",
        "policy forbids",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

async fn rpc_u64(client: &JsonRpcClient, method: &str, params: Value) -> Result<u64> {
    let value = client.request::<U256>(method, params).await?;
    ensure!(value <= U256::from(u64::MAX), "RPC method {method} result exceeds u64");
    Ok(value.to::<u64>())
}

fn u256_to_u128(value: U256, label: &str) -> Result<u128> {
    ensure!(value <= U256::from(u128::MAX), "{label} exceeds u128");
    Ok(value.to::<u128>())
}

fn parse_quantity(value: &str) -> Result<u64> {
    let digits = value.strip_prefix("0x").unwrap_or(value);
    u64::from_str_radix(digits, 16).wrap_err_with(|| format!("invalid RPC quantity {value}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::Log as PrimitiveLog;
    use txgen_property::{run, RunConfig};

    fn receipt_with_event<E: SolEvent>(address: Address, event: E, tx_hash: B256) -> Value {
        let log = Log {
            inner: PrimitiveLog { address, data: event.encode_log_data() },
            transaction_hash: Some(tx_hash),
            ..Log::default()
        };
        json!({"logs": [log]})
    }

    #[test]
    fn decodes_exact_deposit_origin_from_receipt() -> Result<()> {
        let deposit_hash = B256::repeat_byte(0x44);
        let event = PropertyZonePortal::DepositMade {
            newCurrentDepositQueueHash: deposit_hash,
            sender: Address::repeat_byte(0x11),
            token: Address::repeat_byte(0x22),
            netAmount: 123,
            fee: 4,
            keyIndex: U256::ZERO,
            ephemeralPubkeyX: B256::ZERO,
            ephemeralPubkeyYParity: 2,
            ciphertext: Bytes::new(),
            nonce: [0_u8; 12].into(),
            tag: [0_u8; 16].into(),
            tempoRefundRecipient: Address::repeat_byte(0x33),
            depositNumber: 19,
        };
        let receipt =
            receipt_with_event(Address::repeat_byte(0x55), event, B256::repeat_byte(0x66));

        assert_eq!(
            origin_lifecycle(
                &ZoneAction::Deposit { raw_amount: 127, amount_mode: ZoneAmountMode::Raw },
                Address::repeat_byte(0x11),
                Some(B256::repeat_byte(0x66)),
                Some(&receipt),
            )?,
            Some(ZoneLifecycleOrigin::Deposit { deposit_number: 19, deposit_hash })
        );
        Ok(())
    }

    #[test]
    fn derives_withdrawal_sender_tag_from_origin_receipt() -> Result<()> {
        let account = Address::repeat_byte(0x11);
        let tx_hash = B256::repeat_byte(0x77);
        let event = PropertyZoneOutbox::WithdrawalRequested {
            withdrawalIndex: 8,
            sender: account,
            token: Address::repeat_byte(0x22),
            to: account,
            amount: 99,
            fee: 3,
            memo: B256::ZERO,
            gasLimit: 0,
            fallbackNonce: 12,
            data: Bytes::new(),
            revealTo: Bytes::new(),
        };
        let receipt = receipt_with_event(ZONE_OUTBOX, event, tx_hash);

        assert_eq!(
            origin_lifecycle(
                &ZoneAction::Withdraw { raw_amount: 99, amount_mode: ZoneAmountMode::Raw },
                account,
                Some(tx_hash),
                Some(&receipt),
            )?,
            Some(ZoneLifecycleOrigin::Withdrawal {
                withdrawal_index: 8,
                fallback_nonce: 12,
                sender_tag: withdrawal_sender_tag(account, tx_hash, 12),
            })
        );
        Ok(())
    }

    #[derive(Debug)]
    struct MemoryBackend {
        portal_balance: U256,
        zone_supply: U256,
        approvals: usize,
        block: u64,
    }

    impl MemoryBackend {
        fn report(&self) -> PortalBackingReport {
            PortalBackingReport {
                portal: Address::ZERO,
                token: Address::ZERO,
                l1_snapshot_block: self.block,
                zone_snapshot_block: self.block,
                l1_snapshot_hash: B256::from(U256::from(self.block)),
                zone_snapshot_hash: B256::from(U256::from(self.block)),
                l1_from_block: 0,
                zone_from_block: 0,
                l1_chain_id: 1,
                zone_chain_id: 2,
                portal_zone_id: 1,
                portal_balance: self.portal_balance,
                zone_total_supply: self.zone_supply,
                deposit_count: 0,
                l1_processed_deposits: 0,
                zone_processed_deposits: 0,
                withdrawal_queue_head: U256::ZERO,
                withdrawal_queue_tail: U256::ZERO,
                pending_deposit_liability: U256::ZERO,
                pending_withdrawal_liability: U256::ZERO,
                portal_refund_liability: U256::ZERO,
                inbox_refund_liability: U256::ZERO,
                required_backing: self.zone_supply,
                backing_surplus: self.portal_balance.saturating_sub(self.zone_supply),
                backing_deficit: self.zone_supply.saturating_sub(self.portal_balance),
            }
        }
    }

    impl ZonePropertyBackend for MemoryBackend {
        async fn ensure_approvals(&mut self) -> Result<()> {
            self.approvals += 1;
            Ok(())
        }

        async fn execute(&mut self, action: &ZoneAction) -> Result<ZoneExecutionTrace> {
            self.block += 1;
            let layer = match action {
                ZoneAction::Deposit { .. } => ZoneLayer::Tempo,
                ZoneAction::Withdraw { .. } => ZoneLayer::Zone,
            };
            Ok(ZoneExecutionTrace {
                layer,
                transaction_hash: None,
                outcome: ZoneOutcome::Revert,
                submitted_amount: None,
                lifecycle: None,
                receipt: None,
            })
        }

        async fn await_terminal(
            &mut self,
            _action: &ZoneAction,
            trace: &ZoneExecutionTrace,
        ) -> Result<Option<ZoneTerminalEvidence>> {
            Ok(Some(ZoneTerminalEvidence {
                transaction_hash: trace.transaction_hash,
                lifecycle: trace.lifecycle.clone(),
                terminal_reason: "transaction_reverted".to_string(),
                terminal_events: Vec::new(),
                backing: self.report(),
            }))
        }

        async fn verify_backing(&mut self) -> Result<PortalBackingReport> {
            Ok(self.report())
        }
    }

    #[tokio::test]
    async fn model_free_campaign_runs_raw_abi_actions_and_shared_verifier() -> Result<()> {
        let backend = MemoryBackend {
            portal_balance: U256::from(1_000),
            zone_supply: U256::from(1_000),
            approvals: 0,
            block: 0,
        };
        let mut harness = ZonePropertyHarness::new(backend);
        let result = run(&ZoneWorkload, &mut harness, RunConfig::seeded(10, 10, 0x5eed)).await?;

        assert!(result.failure.is_none(), "{:?}", result.failure);
        assert_eq!(result.report.completed_cases, 10);
        assert!(result.report.completed_steps > 0);
        assert!(result.report.completed_verifications >= result.report.completed_steps);
        assert_eq!(harness.backend.approvals, 1);
        Ok(())
    }

    #[tokio::test]
    async fn complete_backing_report_drives_failure_artifact() -> Result<()> {
        let backend = MemoryBackend {
            portal_balance: U256::from(999),
            zone_supply: U256::from(1_000),
            approvals: 0,
            block: 0,
        };
        let mut harness = ZonePropertyHarness::new(backend);
        let result = run(&ZoneWorkload, &mut harness, RunConfig::seeded(1, 1, 7)).await?;
        let failure = result.failure.expect("insolvency must fail");

        assert_eq!(failure.campaign, ZoneWorkload::NAME);
        assert_eq!(failure.verification["backing_deficit"], "0x1");
        assert!(failure.verification.get("pending_deposit_liability").is_some());
        assert!(failure.verification.get("pending_withdrawal_liability").is_some());
        Ok(())
    }
}
