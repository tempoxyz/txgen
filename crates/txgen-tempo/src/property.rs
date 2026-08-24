//! Live Tempo/Zone RPC harness and solvency model.

use std::{
    collections::BTreeSet,
    future::Future,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy_dyn_abi::{DynSolType, DynSolValue};
use alloy_network::Ethereum;
use alloy_primitives::{Address, Bytes, TxKind, B256, U256};
use alloy_rpc_types_eth::{TransactionInput, TransactionRequest};
use alloy_sol_types::{sol, SolCall};
use eyre::{bail, ensure, Result, WrapErr};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::time::Instant;
use txgen_cli::sign_standard_request;
use txgen_core::{EcdsaSigner, TxPhase};
use txgen_property::{
    AbiStrategy, GenerateContext, Prediction, PropertyHarness, PropertyModel, SwarmPolicy,
};

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

sol! {
    interface PropertyTip20 {
        function balanceOf(address account) external view returns (uint256);
        function totalSupply() external view returns (uint256);
        function allowance(address owner, address spender) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
    }

    interface PropertyZonePortal {
        function calculateDepositFee() external view returns (uint128);
        function deposit(
            address token,
            address to,
            uint128 amount,
            bytes32 memo,
            address tempoRefundRecipient
        ) external returns (bytes32);
    }

    interface PropertyZoneOutbox {
        function calculateWithdrawalFee(uint64 gasLimit) external view returns (uint128);
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
    }
}

/// Canonical ZoneOutbox predeploy.
pub const ZONE_OUTBOX: Address =
    alloy_primitives::address!("0x1c00000000000000000000000000000000000002");

/// Live RPC endpoint and protocol configuration.
#[derive(Clone, Debug)]
pub struct ZoneLiveConfig {
    /// Tempo L1 HTTP RPC endpoint.
    pub l1_rpc_url: String,
    /// Public Zone HTTP RPC endpoint used for transaction submission.
    pub zone_rpc_url: String,
    /// Authenticated private Zone HTTP RPC endpoint used for state observations.
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
    /// Fixed transaction gas limit. Invalid model actions still reach execution.
    pub transaction_gas_limit: u64,
    /// Maximum cross-layer convergence wait.
    pub settlement_timeout: Duration,
    /// Poll interval while waiting for cross-layer convergence.
    pub settlement_poll_interval: Duration,
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
            transaction_gas_limit: 500_000,
            settlement_timeout: Duration::from_secs(120),
            settlement_poll_interval: Duration::from_millis(500),
        }
    }
}

/// Exact protocol state plus refreshed account balances used for generation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ZoneSolvencyState {
    /// TIP-20 held by the L1 portal.
    pub portal_balance: U256,
    /// Total supply of the corresponding Zone TIP-20.
    pub zone_total_supply: U256,
    /// Fuzz account's L1 TIP-20 balance.
    pub l1_user_balance: U256,
    /// Fuzz account's Zone TIP-20 balance.
    pub zone_user_balance: U256,
    /// Current portal deposit fee.
    #[serde(with = "u128_decimal")]
    pub deposit_fee: u128,
    /// Current zero-callback withdrawal fee.
    #[serde(with = "u128_decimal")]
    pub withdrawal_fee: u128,
    /// Active deposit checkpoint used by the explicit closed-loop action.
    pub loop_checkpoint: Option<LoopCheckpoint>,
}

/// State captured before an explicit deposit/withdrawal closed loop.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LoopCheckpoint {
    /// Portal escrow before the deposit.
    pub portal_balance: U256,
    /// Zone supply before the deposit.
    pub zone_total_supply: U256,
    /// Net amount minted by the opening deposit.
    #[serde(with = "u128_decimal")]
    pub minted: u128,
}

/// Optional behavior families selected independently for one case.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ZoneActionKind {
    /// L1 portal deposit.
    Deposit,
    /// Zone withdrawal request.
    Withdraw,
    /// State-driven completion of a tracked deposit/withdrawal loop.
    CloseLoop,
}

/// One concrete replayable protocol action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum ZoneAction {
    /// Deposit `amount`, optionally recording a loop checkpoint.
    Deposit {
        /// Total amount passed to the portal.
        #[serde(with = "u128_decimal")]
        amount: u128,
        /// Whether this deposit opens an explicit closed-loop checkpoint.
        track_loop: bool,
    },
    /// Withdraw `amount` to the same account.
    Withdraw {
        /// Amount delivered on Tempo, excluding the withdrawal fee.
        #[serde(with = "u128_decimal")]
        amount: u128,
    },
    /// Withdraw the tracked deposit's net mint, less the withdrawal fee.
    CloseLoop {
        /// Amount delivered on Tempo, excluding the withdrawal fee.
        #[serde(with = "u128_decimal")]
        amount: u128,
    },
}

/// Per-case randomized swarm configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ZoneSwarm {
    /// Optional independently selected action families.
    pub actions: BTreeSet<ZoneActionKind>,
    /// ABI-fuzz integer generator selected for this case.
    pub abi_strategy: AbiStrategy,
    /// Whether raw ABI values are mapped into currently executable ranges.
    pub executable_amounts: bool,
    /// Whether deposits may open explicit closed-loop checkpoints.
    pub closed_loops: bool,
}

/// Expected transaction classification.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ZoneExpected {
    /// Transaction should be included successfully.
    Success,
    /// Transaction should reject or produce a reverted receipt.
    Revert,
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

/// Secret-free transaction execution result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ZoneExecutionTrace {
    /// Execution layer.
    pub layer: ZoneLayer,
    /// Submitted transaction hash, absent when execution was rejected before acceptance.
    pub transaction_hash: Option<B256>,
    /// Observed execution classification.
    pub outcome: ZoneExpected,
}

/// Observation target used to wait for asynchronous cross-layer settlement.
#[derive(Clone, Debug)]
pub enum ZoneObservationRequest {
    /// Read current state once.
    Immediate,
    /// Poll until exact portal escrow and Zone supply targets are visible.
    Until {
        /// Expected portal balance.
        portal_balance: U256,
        /// Expected Zone total supply.
        zone_total_supply: U256,
    },
}

/// State returned by the RPC harness.
#[derive(Clone, Debug, Serialize)]
pub struct ZoneObservation {
    /// Latest observed state.
    pub state: ZoneSolvencyState,
    /// Whether an asynchronous observation target was reached before timeout.
    pub target_reached: bool,
}

/// Rust model for Tempo/Zone solvency and closed-loop accounting.
#[derive(Clone, Debug)]
pub struct ZoneSolvencyModel {
    state: ZoneSolvencyState,
}

impl ZoneSolvencyModel {
    /// Initialize from a live full-state observation.
    pub fn new(state: ZoneSolvencyState) -> Result<Self> {
        verify_solvency(&state)?;
        Ok(Self { state })
    }

    fn raw_amount(swarm: &ZoneSwarm, context: &mut GenerateContext<'_>) -> Result<u128> {
        match context.abi_value(swarm.abi_strategy, &DynSolType::Uint(128), None) {
            DynSolValue::Uint(value, 128) => Ok(value.to::<u128>()),
            value => bail!("ABI generator returned unexpected uint128 value {value:?}"),
        }
    }

    fn bounded_amount(raw: u128, minimum: u128, balance: U256) -> Option<u128> {
        let maximum =
            if balance > U256::from(u128::MAX) { u128::MAX } else { balance.to::<u128>() };
        if maximum < minimum {
            return None;
        }
        let width = maximum.saturating_sub(minimum);
        Some(if width == u128::MAX { raw } else { minimum + raw % (width + 1) })
    }

    fn predict_state(
        &self,
        action: &ZoneAction,
    ) -> Result<Prediction<ZoneSolvencyState, ZoneExpected>> {
        let mut next = self.state.clone();
        let expected = match *action {
            ZoneAction::Deposit { amount, track_loop }
                if amount > next.deposit_fee && U256::from(amount) <= next.l1_user_balance =>
            {
                let minted = amount - next.deposit_fee;
                next.portal_balance = checked_add(next.portal_balance, minted, "portal deposit")?;
                next.zone_total_supply = checked_add(next.zone_total_supply, minted, "Zone mint")?;
                if track_loop && minted > next.withdrawal_fee {
                    next.loop_checkpoint = Some(LoopCheckpoint {
                        portal_balance: self.state.portal_balance,
                        zone_total_supply: self.state.zone_total_supply,
                        minted,
                    });
                }
                ZoneExpected::Success
            }
            ZoneAction::Deposit { .. } => ZoneExpected::Revert,
            ZoneAction::Withdraw { amount } | ZoneAction::CloseLoop { amount } => {
                let Some(burn) = amount.checked_add(next.withdrawal_fee) else {
                    return Ok(Prediction {
                        state: self.state.clone(),
                        expected: ZoneExpected::Revert,
                    });
                };
                if U256::from(amount) <= next.portal_balance
                    && U256::from(burn) <= next.zone_user_balance
                    && U256::from(burn) <= next.zone_total_supply
                {
                    next.portal_balance -= U256::from(amount);
                    next.zone_total_supply -= U256::from(burn);
                    if matches!(action, ZoneAction::CloseLoop { .. }) {
                        next.loop_checkpoint = None;
                    }
                    ZoneExpected::Success
                } else {
                    ZoneExpected::Revert
                }
            }
        };
        Ok(Prediction { state: next, expected })
    }
}

impl PropertyModel for ZoneSolvencyModel {
    const NAME: &'static str = "zone-solvency";
    const VERSION: &'static str = "1";

    type State = ZoneSolvencyState;
    type Swarm = ZoneSwarm;
    type ActionKind = ZoneActionKind;
    type Action = ZoneAction;
    type Expected = ZoneExpected;
    type Trace = ZoneExecutionTrace;
    type ObservationRequest = ZoneObservationRequest;
    type Observation = ZoneObservation;

    fn state(&self) -> &Self::State {
        &self.state
    }

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
        Ok(ZoneSwarm {
            actions,
            abi_strategy,
            executable_amounts: policy.include(rng),
            closed_loops: policy.include(rng),
        })
    }

    fn enabled_actions(&self, swarm: &Self::Swarm) -> Vec<Self::ActionKind> {
        if self
            .state
            .loop_checkpoint
            .as_ref()
            .is_some_and(|checkpoint| checkpoint.minted > self.state.withdrawal_fee)
        {
            return vec![ZoneActionKind::CloseLoop];
        }

        let mut enabled = Vec::new();
        if swarm.actions.contains(&ZoneActionKind::Deposit)
            && self.state.l1_user_balance > U256::from(self.state.deposit_fee)
        {
            enabled.push(ZoneActionKind::Deposit);
        }
        if swarm.actions.contains(&ZoneActionKind::Withdraw)
            && self.state.zone_user_balance > U256::from(self.state.withdrawal_fee)
        {
            enabled.push(ZoneActionKind::Withdraw);
        }
        enabled
    }

    fn generate_action(
        &self,
        swarm: &Self::Swarm,
        kind: &Self::ActionKind,
        context: &mut GenerateContext<'_>,
    ) -> Result<Self::Action> {
        if *kind == ZoneActionKind::CloseLoop {
            let checkpoint =
                self.state.loop_checkpoint.as_ref().expect("CloseLoop is enabled by a checkpoint");
            return Ok(ZoneAction::CloseLoop {
                amount: checkpoint.minted - self.state.withdrawal_fee,
            });
        }

        let raw = Self::raw_amount(swarm, context)?;
        match kind {
            ZoneActionKind::Deposit => {
                let track_loop = swarm.closed_loops && self.state.loop_checkpoint.is_none();
                let minimum = self
                    .state
                    .deposit_fee
                    .saturating_add(if track_loop { self.state.withdrawal_fee } else { 0 })
                    .saturating_add(1);
                let amount = if swarm.executable_amounts {
                    Self::bounded_amount(raw, minimum, self.state.l1_user_balance).unwrap_or(raw)
                } else {
                    raw
                };
                Ok(ZoneAction::Deposit { amount, track_loop })
            }
            ZoneActionKind::Withdraw => {
                let available = self
                    .state
                    .zone_user_balance
                    .saturating_sub(U256::from(self.state.withdrawal_fee));
                let amount = if swarm.executable_amounts {
                    Self::bounded_amount(raw, 1, available).unwrap_or(raw)
                } else {
                    raw
                };
                Ok(ZoneAction::Withdraw { amount })
            }
            ZoneActionKind::CloseLoop => unreachable!("handled above"),
        }
    }

    fn predict(&self, action: &Self::Action) -> Result<Prediction<Self::State, Self::Expected>> {
        self.predict_state(action)
    }

    fn transition_observation(&self, action: &Self::Action) -> Self::ObservationRequest {
        match self.predict_state(action) {
            Ok(Prediction { state, expected: ZoneExpected::Success }) => {
                ZoneObservationRequest::Until {
                    portal_balance: state.portal_balance,
                    zone_total_supply: state.zone_total_supply,
                }
            }
            _ => ZoneObservationRequest::Immediate,
        }
    }

    fn verify_transition(
        &self,
        prediction: &Prediction<Self::State, Self::Expected>,
        action: &Self::Action,
        trace: &Self::Trace,
        observation: &Self::Observation,
    ) -> Result<Self::State> {
        ensure!(trace.outcome == prediction.expected, "execution outcome disagreed with model");
        verify_solvency(&observation.state)?;

        match prediction.expected {
            ZoneExpected::Success => {
                ensure!(observation.target_reached, "cross-layer settlement target timed out");
                ensure!(
                    observation.state.portal_balance == prediction.state.portal_balance,
                    "portal escrow disagreed with model"
                );
                ensure!(
                    observation.state.zone_total_supply == prediction.state.zone_total_supply,
                    "Zone total supply disagreed with model"
                );
                if matches!(action, ZoneAction::CloseLoop { .. }) {
                    let checkpoint =
                        self.state.loop_checkpoint.as_ref().expect("close-loop checkpoint");
                    ensure!(
                        observation.state.zone_total_supply == checkpoint.zone_total_supply,
                        "closed loop did not restore Zone total supply"
                    );
                    ensure!(
                        observation.state.portal_balance
                            == checkpoint.portal_balance + U256::from(self.state.withdrawal_fee),
                        "closed loop portal escrow did not retain exactly the withdrawal fee"
                    );
                }
            }
            ZoneExpected::Revert => {
                ensure!(
                    observation.state.portal_balance == self.state.portal_balance,
                    "reverted action changed portal escrow"
                );
                ensure!(
                    observation.state.zone_total_supply == self.state.zone_total_supply,
                    "reverted action changed Zone total supply"
                );
            }
        }

        let mut reconciled = observation.state.clone();
        reconciled.loop_checkpoint = prediction.state.loop_checkpoint.clone();
        Ok(reconciled)
    }

    fn final_observation(&self) -> Self::ObservationRequest {
        ZoneObservationRequest::Immediate
    }

    fn verify_all(&self, observation: &Self::Observation) -> Result<()> {
        verify_solvency(&observation.state)?;
        ensure!(
            observation.state.portal_balance == self.state.portal_balance,
            "final portal escrow disagreed with committed model"
        );
        ensure!(
            observation.state.zone_total_supply == self.state.zone_total_supply,
            "final Zone supply disagreed with committed model"
        );
        Ok(())
    }

    fn commit(&mut self, state: Self::State) {
        self.state = state;
    }
}

fn verify_solvency(state: &ZoneSolvencyState) -> Result<()> {
    ensure!(
        state.portal_balance >= state.zone_total_supply,
        "Zone is insolvent: portal escrow {} is below Zone supply {}",
        state.portal_balance,
        state.zone_total_supply
    );
    Ok(())
}

fn checked_add(value: U256, amount: u128, label: &str) -> Result<U256> {
    value.checked_add(U256::from(amount)).ok_or_else(|| eyre::eyre!("{label} overflowed"))
}

/// Execution and observation boundary used by the concrete harness.
pub trait ZonePropertyBackend {
    /// Ensure both bridge contracts have sufficient token allowances.
    fn ensure_approvals(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Execute one model action.
    fn execute<'a>(
        &'a mut self,
        action: &'a ZoneAction,
    ) -> impl Future<Output = Result<ZoneExecutionTrace>> + Send + 'a;

    /// Observe immediate or converged protocol state.
    fn observe<'a>(
        &'a mut self,
        request: &'a ZoneObservationRequest,
    ) -> impl Future<Output = Result<ZoneObservation>> + Send + 'a;
}

/// Adapter from a Zone backend to the generic txgen property harness.
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

impl<B> PropertyHarness<ZoneSolvencyModel> for ZonePropertyHarness<B>
where
    B: ZonePropertyBackend + Send,
{
    async fn reset_and_initialize(&mut self) -> Result<ZoneSolvencyModel> {
        if !self.approvals_ready {
            self.backend.ensure_approvals().await?;
            self.approvals_ready = true;
        }
        let observation = self.backend.observe(&ZoneObservationRequest::Immediate).await?;
        ZoneSolvencyModel::new(observation.state)
    }

    async fn execute(&mut self, action: &ZoneAction) -> Result<ZoneExecutionTrace> {
        self.backend.execute(action).await
    }

    async fn observe(&mut self, request: &ZoneObservationRequest) -> Result<ZoneObservation> {
        self.backend.observe(request).await
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
        let result = envelope
            .result
            .ok_or_else(|| eyre::eyre!("RPC method {method} returned neither result nor error"))?;
        serde_json::from_value(result)
            .wrap_err_with(|| format!("invalid JSON-RPC result for {method}"))
    }

    async fn call(&self, from: Address, to: Address, data: Bytes) -> Result<Bytes> {
        self.request("eth_call", json!([{"from": from, "to": to, "data": data}, "latest"])).await
    }

    async fn word(&self, from: Address, to: Address, data: Bytes) -> Result<U256> {
        let output = self.call(from, to, data).await?;
        ensure!(output.len() >= 32, "eth_call returned less than one ABI word");
        Ok(U256::from_be_slice(&output[output.len() - 32..]))
    }
}

#[derive(Debug, Deserialize)]
struct RpcEnvelope {
    result: Option<Value>,
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

/// Native live implementation using signed transactions and authenticated private observations.
#[derive(Clone, Debug)]
pub struct LiveZoneBackend {
    config: ZoneLiveConfig,
    signer: EcdsaSigner,
    account: Address,
    l1: JsonRpcClient,
    zone: JsonRpcClient,
    private_zone: JsonRpcClient,
    l1_chain_id: u64,
}

impl LiveZoneBackend {
    /// Connect to all endpoints, validate chain IDs, and prepare a live backend.
    pub async fn connect(config: ZoneLiveConfig, signer: EcdsaSigner) -> Result<Self> {
        let account = signer.address();
        let l1 = JsonRpcClient::new(config.l1_rpc_url.clone());
        let zone = JsonRpcClient::new(config.zone_rpc_url.clone());
        let private_zone = JsonRpcClient::with_zone_auth(
            config.zone_private_rpc_url.clone(),
            signer.clone(),
            config.zone_id,
            config.zone_chain_id,
        );
        let l1_chain_id = rpc_u64(&l1, "eth_chainId", json!([])).await?;
        let zone_chain_id = rpc_u64(&private_zone, "eth_chainId", json!([])).await?;
        ensure!(
            zone_chain_id == config.zone_chain_id,
            "private Zone RPC chain ID {zone_chain_id} does not match configured {}",
            config.zone_chain_id
        );
        eprintln!(
            "[zone-property] connected account={account} l1_chain_id={l1_chain_id} \
             zone_id={} zone_chain_id={zone_chain_id}",
            config.zone_id
        );
        Ok(Self { config, signer, account, l1, zone, private_zone, l1_chain_id })
    }

    async fn read_state(&self) -> Result<ZoneSolvencyState> {
        let balance_call =
            |account| Bytes::from(PropertyTip20::balanceOfCall { account }.abi_encode());
        let (
            portal_balance,
            zone_total_supply,
            l1_user_balance,
            zone_user_balance,
            deposit_fee,
            withdrawal_fee,
        ) = tokio::try_join!(
            self.l1.word(self.account, self.config.token, balance_call(self.config.portal)),
            self.private_zone.word(
                self.account,
                self.config.token,
                Bytes::from(PropertyTip20::totalSupplyCall {}.abi_encode())
            ),
            self.l1.word(self.account, self.config.token, balance_call(self.account)),
            self.private_zone.word(self.account, self.config.token, balance_call(self.account)),
            self.l1.word(
                self.account,
                self.config.portal,
                Bytes::from(PropertyZonePortal::calculateDepositFeeCall {}.abi_encode())
            ),
            self.private_zone.word(
                self.account,
                self.config.outbox,
                Bytes::from(
                    PropertyZoneOutbox::calculateWithdrawalFeeCall { gasLimit: 0 }.abi_encode()
                )
            ),
        )?;
        Ok(ZoneSolvencyState {
            portal_balance,
            zone_total_supply,
            l1_user_balance,
            zone_user_balance,
            deposit_fee: u256_to_u128(deposit_fee, "deposit fee")?,
            withdrawal_fee: u256_to_u128(withdrawal_fee, "withdrawal fee")?,
            loop_checkpoint: None,
        })
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
        let trace = self.send_transaction(client, chain_id, self.config.token, data, layer).await?;
        ensure!(trace.outcome == ZoneExpected::Success, "{layer:?} approval reverted");
        Ok(())
    }

    async fn send_transaction(
        &self,
        client: &JsonRpcClient,
        chain_id: u64,
        to: Address,
        input: Bytes,
        layer: ZoneLayer,
    ) -> Result<ZoneExecutionTrace> {
        let nonce =
            rpc_u64(client, "eth_getTransactionCount", json!([self.account, "pending"])).await?;
        let gas_price = client.request::<U256>("eth_gasPrice", json!([])).await?;
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
        let transaction_hash =
            match client.request::<B256>("eth_sendRawTransaction", json!([signed.raw])).await {
                Ok(hash) => hash,
                Err(error) if is_execution_rejection(&error) => {
                    return Ok(ZoneExecutionTrace {
                        layer,
                        transaction_hash: None,
                        outcome: ZoneExpected::Revert,
                    });
                }
                Err(error) => return Err(error),
            };
        eprintln!("[zone-property] submitted layer={layer:?} tx={transaction_hash}");

        let deadline = Instant::now() + self.config.settlement_timeout;
        loop {
            let receipt: Value =
                client.request("eth_getTransactionReceipt", json!([transaction_hash])).await?;
            if !receipt.is_null() {
                let status = receipt
                    .get("status")
                    .and_then(Value::as_str)
                    .ok_or_else(|| eyre::eyre!("transaction receipt is missing status"))?;
                let outcome = if parse_quantity(status)? == 1 {
                    ZoneExpected::Success
                } else {
                    ZoneExpected::Revert
                };
                eprintln!(
                    "[zone-property] included layer={layer:?} tx={transaction_hash} \
                     outcome={outcome:?}"
                );
                return Ok(ZoneExecutionTrace {
                    layer,
                    transaction_hash: Some(transaction_hash),
                    outcome,
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
            &self.zone,
            self.config.zone_chain_id,
            self.config.outbox,
            ZoneLayer::Zone,
        )
        .await
    }

    async fn execute(&mut self, action: &ZoneAction) -> Result<ZoneExecutionTrace> {
        match *action {
            ZoneAction::Deposit { amount, .. } => {
                let data = Bytes::from(
                    PropertyZonePortal::depositCall {
                        token: self.config.token,
                        to: self.account,
                        amount,
                        memo: B256::ZERO,
                        tempoRefundRecipient: self.account,
                    }
                    .abi_encode(),
                );
                self.send_transaction(
                    &self.l1,
                    self.l1_chain_id,
                    self.config.portal,
                    data,
                    ZoneLayer::Tempo,
                )
                .await
            }
            ZoneAction::Withdraw { amount } | ZoneAction::CloseLoop { amount } => {
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
                self.send_transaction(
                    &self.zone,
                    self.config.zone_chain_id,
                    self.config.outbox,
                    data,
                    ZoneLayer::Zone,
                )
                .await
            }
        }
    }

    async fn observe(&mut self, request: &ZoneObservationRequest) -> Result<ZoneObservation> {
        let deadline = Instant::now() + self.config.settlement_timeout;
        loop {
            let state = self.read_state().await?;
            let target_reached = match request {
                ZoneObservationRequest::Immediate => true,
                ZoneObservationRequest::Until { portal_balance, zone_total_supply } => {
                    state.portal_balance == *portal_balance
                        && state.zone_total_supply == *zone_total_supply
                }
            };
            if target_reached
                || matches!(request, ZoneObservationRequest::Immediate)
                || Instant::now() >= deadline
            {
                eprintln!(
                    "[zone-property] observed portal={} supply={} l1_user={} \
                     zone_user={} settled={target_reached}",
                    state.portal_balance,
                    state.zone_total_supply,
                    state.l1_user_balance,
                    state.zone_user_balance,
                );
                return Ok(ZoneObservation { state, target_reached });
            }
            tokio::time::sleep(self.config.settlement_poll_interval).await;
        }
    }
}

fn is_execution_rejection(error: &eyre::Report) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    [
        "execution reverted",
        "revert",
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
    use txgen_property::{run, PropertyHarness, PropertyModel, RunConfig};

    #[derive(Debug)]
    struct MemoryBackend {
        state: ZoneSolvencyState,
        approvals: usize,
    }

    impl MemoryBackend {
        fn solvent() -> Self {
            Self {
                state: ZoneSolvencyState {
                    portal_balance: U256::from(1_000),
                    zone_total_supply: U256::from(1_000),
                    l1_user_balance: U256::from(10_000),
                    zone_user_balance: U256::from(1_000),
                    deposit_fee: 10,
                    withdrawal_fee: 5,
                    loop_checkpoint: None,
                },
                approvals: 0,
            }
        }
    }

    impl ZonePropertyBackend for MemoryBackend {
        async fn ensure_approvals(&mut self) -> Result<()> {
            self.approvals += 1;
            Ok(())
        }

        async fn execute(&mut self, action: &ZoneAction) -> Result<ZoneExecutionTrace> {
            let mut outcome = ZoneExpected::Revert;
            let layer = match *action {
                ZoneAction::Deposit { amount, .. } => {
                    if amount > self.state.deposit_fee
                        && U256::from(amount) <= self.state.l1_user_balance
                    {
                        let minted = amount - self.state.deposit_fee;
                        self.state.portal_balance += U256::from(minted);
                        self.state.zone_total_supply += U256::from(minted);
                        self.state.l1_user_balance -= U256::from(amount);
                        self.state.zone_user_balance += U256::from(minted);
                        outcome = ZoneExpected::Success;
                    }
                    ZoneLayer::Tempo
                }
                ZoneAction::Withdraw { amount } | ZoneAction::CloseLoop { amount } => {
                    if let Some(burn) = amount.checked_add(self.state.withdrawal_fee)
                        && U256::from(amount) <= self.state.portal_balance
                        && U256::from(burn) <= self.state.zone_user_balance
                    {
                        self.state.portal_balance -= U256::from(amount);
                        self.state.zone_total_supply -= U256::from(burn);
                        self.state.zone_user_balance -= U256::from(burn);
                        self.state.l1_user_balance += U256::from(amount);
                        outcome = ZoneExpected::Success;
                    }
                    ZoneLayer::Zone
                }
            };
            Ok(ZoneExecutionTrace { layer, transaction_hash: None, outcome })
        }

        async fn observe(&mut self, request: &ZoneObservationRequest) -> Result<ZoneObservation> {
            let target_reached = match request {
                ZoneObservationRequest::Immediate => true,
                ZoneObservationRequest::Until { portal_balance, zone_total_supply } => {
                    self.state.portal_balance == *portal_balance
                        && self.state.zone_total_supply == *zone_total_supply
                }
            };
            let mut state = self.state.clone();
            state.loop_checkpoint = None;
            Ok(ZoneObservation { state, target_reached })
        }
    }

    async fn apply(
        harness: &mut ZonePropertyHarness<MemoryBackend>,
        model: &mut ZoneSolvencyModel,
        action: ZoneAction,
    ) -> Result<()> {
        let prediction = model.predict(&action)?;
        let trace = harness.execute(&action).await?;
        let observation = harness.observe(&model.transition_observation(&action)).await?;
        let state = model.verify_transition(&prediction, &action, &trace, &observation)?;
        model.commit(state);
        Ok(())
    }

    #[tokio::test]
    async fn live_harness_contract_closes_the_fee_aware_loop() -> Result<()> {
        let mut harness = ZonePropertyHarness::new(MemoryBackend::solvent());
        let mut model = harness.reset_and_initialize().await?;

        apply(&mut harness, &mut model, ZoneAction::Deposit { amount: 100, track_loop: true })
            .await?;
        assert_eq!(
            model.state.loop_checkpoint,
            Some(LoopCheckpoint {
                portal_balance: U256::from(1_000),
                zone_total_supply: U256::from(1_000),
                minted: 90,
            })
        );

        apply(&mut harness, &mut model, ZoneAction::CloseLoop { amount: 85 }).await?;
        assert_eq!(model.state.zone_total_supply, U256::from(1_000));
        assert_eq!(model.state.portal_balance, U256::from(1_005));
        assert!(model.state.loop_checkpoint.is_none());
        verify_solvency(model.state())?;
        Ok(())
    }

    #[tokio::test]
    async fn concrete_harness_runs_randomized_abi_fuzz_swarms() -> Result<()> {
        let mut harness = ZonePropertyHarness::new(MemoryBackend::solvent());
        let result =
            run::<ZoneSolvencyModel, _>(&mut harness, RunConfig::seeded(25, 25, 0x5eed)).await?;

        assert!(result.failure.is_none(), "{:?}", result.failure);
        assert_eq!(result.report.completed_cases, 25);
        assert!(result.report.completed_steps > 0);
        assert_eq!(harness.backend.approvals, 1);
        Ok(())
    }

    #[test]
    fn rejects_an_insolvent_initial_observation() {
        let mut state = MemoryBackend::solvent().state;
        state.zone_total_supply = state.portal_balance + U256::from(1);
        assert!(ZoneSolvencyModel::new(state).is_err());
    }
}
