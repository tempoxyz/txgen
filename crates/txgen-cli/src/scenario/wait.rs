use super::{
    error::StepError,
    report::unix_ms,
    schema::WaitLogStep,
    value::{coerce_event_filter, eval_expression, RuntimeContext, RuntimeValue},
};
use alloy_dyn_abi::{DynSolType, DynSolValue, EventExt, Specifier};
use alloy_eips::BlockNumberOrTag;
use alloy_json_abi::{Event, JsonAbi};
use alloy_network::{primitives::ReceiptResponse, AnyNetwork, AnyTransactionReceipt};
use alloy_primitives::{keccak256, Address, TxHash, B256, U256};
use alloy_provider::{DynProvider, Provider};
use alloy_rpc_types_eth::{Filter, Log};
use bench_core::RpcSubmitter;
use eyre::{bail, Result, WrapErr};
use std::{collections::BTreeMap, time::Duration};

pub(crate) const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(500);
pub(crate) const DEFAULT_MAX_BLOCK_RANGE: u64 = 1_000;
const REORG_RESCAN_BLOCKS: u64 = 64;

pub(crate) struct ReceiptResult {
    pub value: RuntimeValue,
    pub status: bool,
}

pub(crate) async fn wait_for_receipt(
    query_provider: &DynProvider<AnyNetwork>,
    submitter: &RpcSubmitter,
    chain: &str,
    sender: Option<Address>,
    transaction_hash: TxHash,
    poll_interval: Duration,
    confirmations: u64,
) -> Result<ReceiptResult, StepError> {
    loop {
        let receipt = submitter
            .get_transaction_receipt(sender, transaction_hash)
            .await
            .map_err(StepError::rpc)?;
        let Some(receipt) = receipt else {
            tokio::time::sleep(poll_interval).await;
            continue;
        };

        let Some(block_number) = receipt.block_number() else {
            tokio::time::sleep(poll_interval).await;
            continue;
        };
        wait_for_confirmations(query_provider, block_number, confirmations, poll_interval).await?;

        // Re-fetch after the confirmation wait. A receipt that vanished or moved
        // was reorged and must not be exposed through an immutable save.
        let canonical = submitter
            .get_transaction_receipt(sender, transaction_hash)
            .await
            .map_err(StepError::rpc)?;
        let Some(canonical) = canonical else {
            continue;
        };
        if canonical.block_hash() != receipt.block_hash() ||
            canonical.block_number() != receipt.block_number()
        {
            continue;
        }
        if !receipt_is_canonical_on_query(query_provider, &canonical).await? {
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        return Ok(receipt_runtime_value(chain, &canonical));
    }
}

async fn receipt_is_canonical_on_query(
    provider: &DynProvider<AnyNetwork>,
    receipt: &AnyTransactionReceipt,
) -> Result<bool, StepError> {
    let (Some(block_number), Some(receipt_block_hash)) =
        (receipt.block_number(), receipt.block_hash())
    else {
        return Ok(false);
    };
    let block = provider
        .client()
        .request::<_, Option<serde_json::Value>>(
            "eth_getBlockByNumber",
            (BlockNumberOrTag::Number(block_number), false),
        )
        .await
        .map_err(StepError::rpc)?;
    let Some(block) = block else { return Ok(false) };
    let block_hash = block
        .get("hash")
        .cloned()
        .ok_or_else(|| StepError::rpc("query RPC block response omitted its hash"))?;
    let block_hash: B256 = serde_json::from_value(block_hash)
        .map_err(|_| StepError::rpc("query RPC block response had an invalid hash"))?;
    Ok(block_hash == receipt_block_hash)
}

fn receipt_runtime_value(chain: &str, receipt: &AnyTransactionReceipt) -> ReceiptResult {
    let status = receipt.status();
    ReceiptResult {
        status,
        value: object([
            ("chain", RuntimeValue::String(chain.to_string())),
            ("transaction_hash", RuntimeValue::Bytes32(receipt.transaction_hash())),
            ("tx_hash", RuntimeValue::Bytes32(receipt.transaction_hash())),
            (
                "block_hash",
                receipt.block_hash().map(RuntimeValue::Bytes32).unwrap_or(RuntimeValue::Null),
            ),
            (
                "block_number",
                receipt
                    .block_number()
                    .map(|value| RuntimeValue::Uint(U256::from(value)))
                    .unwrap_or(RuntimeValue::Null),
            ),
            ("status", RuntimeValue::Bool(status)),
            ("gas_used", RuntimeValue::Uint(U256::from(receipt.gas_used()))),
            ("observed_at", RuntimeValue::Uint(U256::from(unix_ms(std::time::SystemTime::now())))),
        ]),
    }
}

async fn wait_for_confirmations(
    provider: &DynProvider<AnyNetwork>,
    block_number: u64,
    confirmations: u64,
    poll_interval: Duration,
) -> Result<(), StepError> {
    let target = block_number.saturating_add(confirmations);
    loop {
        let current = provider.get_block_number().await.map_err(StepError::rpc)?;
        if current >= target {
            return Ok(());
        }
        tokio::time::sleep(poll_interval).await;
    }
}

pub(crate) async fn wait_for_log(
    query_provider: &DynProvider<AnyNetwork>,
    submitter: &RpcSubmitter,
    chain: &str,
    abi: &JsonAbi,
    step: &WaitLogStep,
    context: &RuntimeContext,
) -> Result<RuntimeValue, StepError> {
    let matcher =
        EventMatcher::new(abi, &step.event, &step.where_value, context).map_err(StepError::abi)?;
    let address = step
        .address
        .as_ref()
        .map(|value| expression_address(value, context))
        .transpose()
        .map_err(StepError::expression)?;
    let transaction_hash = step
        .transaction_hash
        .as_ref()
        .map(|value| expression_hash(value, context))
        .transpose()
        .map_err(StepError::expression)?;
    let sender = step
        .sender
        .as_ref()
        .map(|value| expression_address(value, context))
        .transpose()
        .map_err(StepError::expression)?;
    let from_block = step
        .from_block
        .as_ref()
        .map(|value| expression_u64(value, context))
        .transpose()
        .map_err(StepError::expression)?;
    let poll_interval = step.poll_interval.unwrap_or(DEFAULT_POLL_INTERVAL);
    let confirmations = step.confirmations.unwrap_or(0);

    if from_block.is_none() {
        let transaction_hash = transaction_hash.ok_or_else(|| {
            StepError::missing("wait_log requires a start block or transaction hash")
        })?;
        return wait_for_transaction_log(
            query_provider,
            submitter,
            TransactionLogWait {
                chain,
                sender,
                transaction_hash,
                address,
                matcher: &matcher,
                poll_interval,
                confirmations,
            },
        )
        .await;
    }

    let start_block = from_block.expect("checked above");
    let mut cursor = start_block;
    let max_range = step.max_block_range.unwrap_or(DEFAULT_MAX_BLOCK_RANGE);
    loop {
        let head = query_provider.get_block_number().await.map_err(StepError::rpc)?;
        let Some(safe_head) = head.checked_sub(confirmations) else {
            tokio::time::sleep(poll_interval).await;
            continue;
        };

        while cursor <= safe_head {
            let end = bounded_range_end(cursor, safe_head, max_range);
            let filter = matcher.rpc_filter(cursor, end, address);
            let mut logs = query_provider.get_logs(&filter).await.map_err(StepError::rpc)?;
            sort_logs(&mut logs);
            let first_observed_at = unix_ms(std::time::SystemTime::now());

            for candidate in logs {
                if candidate.removed ||
                    transaction_hash
                        .is_some_and(|expected| candidate.transaction_hash != Some(expected))
                {
                    continue;
                }
                let Some(_) = matcher.decode_if_matches(&candidate).map_err(StepError::abi)? else {
                    continue;
                };

                // Re-query through the candidate before saving. Start in the
                // preceding bounded reorg window, rather than at this scan
                // chunk, so a replacement log introduced into the prior chunk
                // cannot lose first-match ordering to this later candidate.
                let block_number = candidate
                    .block_number
                    .ok_or_else(|| StepError::missing("matching log omitted block_number"))?;
                let canonical_start = canonical_recheck_start(start_block, cursor);
                if let Some((canonical, canonical_decoded)) = find_first_canonical_log(
                    query_provider,
                    &matcher,
                    address,
                    transaction_hash,
                    canonical_start,
                    block_number,
                    max_range,
                )
                .await?
                {
                    return log_runtime_value(
                        chain,
                        &matcher.event,
                        &canonical,
                        canonical_decoded,
                        first_observed_at,
                    )
                    .map_err(StepError::abi);
                }
            }

            cursor = end.saturating_add(1);
        }

        // Revisit a bounded recent window. Besides honoring explicit `removed`
        // flags and exact-block rechecks for candidates, this catches a reorg
        // that introduces the target into a block scanned as empty earlier.
        cursor = safe_head.saturating_sub(REORG_RESCAN_BLOCKS.saturating_sub(1)).max(start_block);
        tokio::time::sleep(poll_interval).await;
    }
}

fn canonical_recheck_start(start_block: u64, cursor: u64) -> u64 {
    cursor.saturating_sub(REORG_RESCAN_BLOCKS.saturating_sub(1)).max(start_block)
}

fn bounded_range_end(start: u64, through: u64, max_range: u64) -> u64 {
    start.saturating_add(max_range.saturating_sub(1)).min(through)
}

async fn find_first_canonical_log(
    provider: &DynProvider<AnyNetwork>,
    matcher: &EventMatcher,
    address: Option<Address>,
    transaction_hash: Option<TxHash>,
    start: u64,
    through: u64,
    max_range: u64,
) -> Result<Option<(Log, Vec<DynSolValue>)>, StepError> {
    if start > through {
        return Ok(None);
    }

    let mut cursor = start;
    loop {
        let end = bounded_range_end(cursor, through, max_range);
        let filter = matcher.rpc_filter(cursor, end, address);
        let mut logs = provider.get_logs(&filter).await.map_err(StepError::rpc)?;
        sort_logs(&mut logs);

        for log in logs {
            if log.removed ||
                transaction_hash.is_some_and(|expected| log.transaction_hash != Some(expected))
            {
                continue;
            }
            if let Some(decoded) = matcher.decode_if_matches(&log).map_err(StepError::abi)? {
                return Ok(Some((log, decoded)));
            }
        }

        if end == through {
            return Ok(None);
        }
        cursor = end.saturating_add(1);
    }
}

struct TransactionLogWait<'a> {
    chain: &'a str,
    sender: Option<Address>,
    transaction_hash: TxHash,
    address: Option<Address>,
    matcher: &'a EventMatcher,
    poll_interval: Duration,
    confirmations: u64,
}

async fn wait_for_transaction_log(
    query_provider: &DynProvider<AnyNetwork>,
    submitter: &RpcSubmitter,
    request: TransactionLogWait<'_>,
) -> Result<RuntimeValue, StepError> {
    let TransactionLogWait {
        chain,
        sender,
        transaction_hash,
        address,
        matcher,
        poll_interval,
        confirmations,
    } = request;
    loop {
        let receipt = submitter
            .get_transaction_receipt(sender, transaction_hash)
            .await
            .map_err(StepError::rpc)?;
        let Some(receipt) = receipt else {
            tokio::time::sleep(poll_interval).await;
            continue;
        };
        let Some(block_number) = receipt.block_number() else {
            tokio::time::sleep(poll_interval).await;
            continue;
        };
        wait_for_confirmations(query_provider, block_number, confirmations, poll_interval).await?;

        let canonical = submitter
            .get_transaction_receipt(sender, transaction_hash)
            .await
            .map_err(StepError::rpc)?;
        let Some(canonical) = canonical else { continue };
        if canonical.block_hash() != receipt.block_hash() ||
            canonical.block_number() != receipt.block_number()
        {
            continue;
        }
        if !receipt_is_canonical_on_query(query_provider, &canonical).await? {
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        let mut logs = canonical.logs().to_vec();
        sort_logs(&mut logs);
        let first_observed_at = unix_ms(std::time::SystemTime::now());
        for log in logs {
            if log.removed || address.is_some_and(|expected| log.address() != expected) {
                continue;
            }
            if let Some(decoded) = matcher.decode_if_matches(&log).map_err(StepError::abi)? {
                return log_runtime_value(chain, &matcher.event, &log, decoded, first_observed_at)
                    .map_err(StepError::abi);
            }
        }
        return Err(StepError::missing(
            "confirmed transaction receipt contained no matching canonical event",
        ));
    }
}

struct EventMatcher {
    event: Event,
    parameters: Vec<EventParameter>,
    expected: BTreeMap<String, DynSolValue>,
}

struct EventParameter {
    name: String,
    indexed: bool,
    sol_type: DynSolType,
}

impl EventMatcher {
    fn new(
        abi: &JsonAbi,
        event_name: &str,
        filters: &BTreeMap<String, serde_yaml::Value>,
        context: &RuntimeContext,
    ) -> Result<Self> {
        let (event, parameters, names) = resolve_event_parameters(abi, event_name)?;
        validate_filter_names(&parameters, &names, filters.keys().map(String::as_str))?;

        let mut expected = BTreeMap::new();
        for (name, expression) in filters {
            let parameter = parameters
                .iter()
                .find(|parameter| parameter.name == *name)
                .expect("filter names were validated");
            let value = if parameter.indexed && indexed_value_is_hashed(&parameter.sol_type) {
                let runtime = eval_expression(expression, context)
                    .wrap_err_with(|| format!("failed to resolve event filter '{name}'"))?;
                match runtime {
                    RuntimeValue::Bytes32(hash) => DynSolValue::FixedBytes(hash, 32),
                    runtime => {
                        let value = runtime
                            .coerce_dyn_sol(&parameter.sol_type)
                            .wrap_err_with(|| format!("failed to resolve event filter '{name}'"))?;
                        DynSolValue::FixedBytes(keccak256(indexed_value_preimage(&value)), 32)
                    }
                }
            } else {
                coerce_event_filter(expression, &parameter.sol_type, context)
                    .wrap_err_with(|| format!("failed to resolve event filter '{name}'"))?
            };
            expected.insert(name.clone(), value);
        }

        Ok(Self { event, parameters, expected })
    }

    fn rpc_filter(&self, from_block: u64, to_block: u64, address: Option<Address>) -> Filter {
        let mut filter = Filter::new().from_block(from_block).to_block(to_block);
        if let Some(address) = address {
            filter = filter.address(address);
        }
        if !self.event.anonymous {
            filter = filter.event_signature(self.event.selector());
        }
        filter
    }

    fn decode_if_matches(&self, log: &Log) -> Result<Option<Vec<DynSolValue>>> {
        let decoded = match self.event.decode_log(&log.inner.data) {
            Ok(decoded) => decoded,
            Err(_) => return Ok(None),
        };
        let mut indexed = decoded.indexed.into_iter();
        let mut body = decoded.body.into_iter();
        let mut values = Vec::with_capacity(self.parameters.len());
        for parameter in &self.parameters {
            let value = if parameter.indexed { indexed.next() } else { body.next() }
                .ok_or_else(|| eyre::eyre!("decoded event argument count mismatch"))?;
            if let Some(expected) = self.expected.get(&parameter.name) &&
                *expected != value
            {
                return Ok(None);
            }
            values.push(value);
        }
        Ok(Some(values))
    }
}

fn resolve_event_parameters(
    abi: &JsonAbi,
    event_name: &str,
) -> Result<(Event, Vec<EventParameter>, BTreeMap<String, usize>)> {
    let event = resolve_event(abi, event_name)?.clone();
    let mut parameters = Vec::with_capacity(event.inputs.len());
    let mut names = BTreeMap::<String, usize>::new();
    for (index, parameter) in event.inputs.iter().enumerate() {
        let sol_type = parameter.resolve().wrap_err_with(|| {
            format!("failed to parse event parameter {} type '{}'", index, parameter.ty)
        })?;
        if !parameter.name.is_empty() {
            *names.entry(parameter.name.clone()).or_default() += 1;
        }
        parameters.push(EventParameter {
            name: parameter.name.clone(),
            indexed: parameter.indexed,
            sol_type,
        });
    }
    Ok((event, parameters, names))
}

fn validate_filter_names<'a>(
    parameters: &[EventParameter],
    names: &BTreeMap<String, usize>,
    filters: impl IntoIterator<Item = &'a str>,
) -> Result<()> {
    for name in filters {
        if names.get(name).copied().unwrap_or(0) > 1 {
            bail!("event argument filter '{name}' is ambiguous");
        }
        if !parameters.iter().any(|parameter| parameter.name == name) {
            bail!("event has no argument named '{name}'");
        }
    }
    Ok(())
}

pub(crate) struct EventFilterType {
    pub sol_type: DynSolType,
    pub accepts_precomputed_hash: bool,
}

pub(crate) fn resolve_event_filter_types(
    abi: &JsonAbi,
    event_name: &str,
    filters: &BTreeMap<String, serde_yaml::Value>,
) -> Result<BTreeMap<String, EventFilterType>> {
    let (_, parameters, names) = resolve_event_parameters(abi, event_name)?;
    validate_filter_names(&parameters, &names, filters.keys().map(String::as_str))?;
    Ok(filters
        .keys()
        .map(|name| {
            let parameter = parameters
                .iter()
                .find(|parameter| parameter.name == *name)
                .expect("filter names were validated");
            (
                name.clone(),
                EventFilterType {
                    sol_type: parameter.sol_type.clone(),
                    accepts_precomputed_hash: parameter.indexed &&
                        indexed_value_is_hashed(&parameter.sol_type),
                },
            )
        })
        .collect())
}

pub(crate) fn validate_constant_event_filter(
    expression: &serde_yaml::Value,
    filter_type: &EventFilterType,
) -> Result<()> {
    let runtime = eval_expression(expression, &RuntimeContext::empty())?;
    if filter_type.accepts_precomputed_hash && matches!(runtime, RuntimeValue::Bytes32(_)) {
        return Ok(());
    }
    runtime.coerce_dyn_sol(&filter_type.sol_type).map(|_| ())
}

fn indexed_value_is_hashed(sol_type: &DynSolType) -> bool {
    !matches!(
        sol_type,
        DynSolType::Address |
            DynSolType::Function |
            DynSolType::Bool |
            DynSolType::FixedBytes(_) |
            DynSolType::Int(_) |
            DynSolType::Uint(_)
    )
}

fn indexed_value_preimage(value: &DynSolValue) -> Vec<u8> {
    fn encode(value: &DynSolValue, nested: bool, output: &mut Vec<u8>) {
        match value {
            DynSolValue::String(value) => encode_bytes(value.as_bytes(), nested, output),
            DynSolValue::Bytes(value) => encode_bytes(value, nested, output),
            DynSolValue::Array(values) |
            DynSolValue::FixedArray(values) |
            DynSolValue::Tuple(values) => {
                for value in values {
                    encode(value, true, output);
                }
            }
            DynSolValue::CustomStruct { tuple, .. } => {
                for value in tuple {
                    encode(value, true, output);
                }
            }
            _ => output.extend_from_slice(
                value.as_word().expect("non-composite ABI value is one word").as_slice(),
            ),
        }
    }

    fn encode_bytes(value: &[u8], nested: bool, output: &mut Vec<u8>) {
        output.extend_from_slice(value);
        if nested {
            let padding = if value.is_empty() { 32 } else { (32 - value.len() % 32) % 32 };
            output.resize(output.len() + padding, 0);
        }
    }

    let mut output = Vec::new();
    encode(value, false, &mut output);
    output
}

fn resolve_event<'a>(abi: &'a JsonAbi, name_or_signature: &str) -> Result<&'a Event> {
    if name_or_signature.contains('(') {
        return abi
            .events()
            .find(|event| event.signature() == name_or_signature)
            .ok_or_else(|| eyre::eyre!("event signature '{name_or_signature}' not found in ABI"));
    }

    let events = abi
        .event(name_or_signature)
        .ok_or_else(|| eyre::eyre!("event '{name_or_signature}' not found in ABI"))?;
    match events.as_slice() {
        [event] => Ok(event),
        [] => unreachable!("JsonAbi::event returned an empty overload list"),
        _ => {
            bail!("event name '{name_or_signature}' is overloaded; use an exact signature");
        }
    }
}

fn log_runtime_value(
    chain: &str,
    event: &Event,
    log: &Log,
    values: Vec<DynSolValue>,
    first_observed_at: u64,
) -> Result<RuntimeValue> {
    let mut arguments = BTreeMap::new();
    for (index, (parameter, value)) in event.inputs.iter().zip(values).enumerate() {
        let name =
            if parameter.name.is_empty() { index.to_string() } else { parameter.name.clone() };
        arguments.insert(name, RuntimeValue::from_dyn_sol(&value)?);
    }

    Ok(object([
        ("chain", RuntimeValue::String(chain.to_string())),
        ("address", RuntimeValue::Address(log.address())),
        ("contract_address", RuntimeValue::Address(log.address())),
        (
            "transaction_hash",
            log.transaction_hash.map(RuntimeValue::Bytes32).unwrap_or(RuntimeValue::Null),
        ),
        ("tx_hash", log.transaction_hash.map(RuntimeValue::Bytes32).unwrap_or(RuntimeValue::Null)),
        ("block_hash", log.block_hash.map(RuntimeValue::Bytes32).unwrap_or(RuntimeValue::Null)),
        (
            "block_number",
            log.block_number
                .map(|value| RuntimeValue::Uint(U256::from(value)))
                .unwrap_or(RuntimeValue::Null),
        ),
        (
            "log_index",
            log.log_index
                .map(|value| RuntimeValue::Uint(U256::from(value)))
                .unwrap_or(RuntimeValue::Null),
        ),
        ("event", RuntimeValue::String(event.name.clone())),
        ("event_name", RuntimeValue::String(event.name.clone())),
        ("args", RuntimeValue::Object(arguments)),
        ("first_observed_at", RuntimeValue::Uint(U256::from(first_observed_at))),
        ("observed_at", RuntimeValue::Uint(U256::from(first_observed_at))),
    ]))
}

pub(crate) fn expression_address(
    value: &serde_yaml::Value,
    context: &RuntimeContext,
) -> Result<Address> {
    match eval_expression(value, context)?.coerce_dyn_sol(&DynSolType::Address)? {
        DynSolValue::Address(value) => Ok(value),
        _ => unreachable!("address coercion returned another type"),
    }
}

fn expression_hash(value: &serde_yaml::Value, context: &RuntimeContext) -> Result<B256> {
    match eval_expression(value, context)?.coerce_dyn_sol(&DynSolType::FixedBytes(32))? {
        DynSolValue::FixedBytes(value, 32) => Ok(value),
        _ => unreachable!("bytes32 coercion returned another type"),
    }
}

fn expression_u64(value: &serde_yaml::Value, context: &RuntimeContext) -> Result<u64> {
    match eval_expression(value, context)?.coerce_dyn_sol(&DynSolType::Uint(64))? {
        DynSolValue::Uint(value, 64) => {
            value.try_into().map_err(|_| eyre::eyre!("block number expression exceeds u64"))
        }
        _ => unreachable!("uint64 coercion returned another type"),
    }
}

fn sort_logs(logs: &mut [Log]) {
    logs.sort_by_key(|log| {
        (log.block_number.unwrap_or(u64::MAX), log.log_index.unwrap_or(u64::MAX))
    });
}

fn object<const N: usize>(values: [(&str, RuntimeValue); N]) -> RuntimeValue {
    RuntimeValue::Object(values.into_iter().map(|(key, value)| (key.to_string(), value)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Bytes, LogData};
    use alloy_provider::ProviderBuilder;
    use alloy_transport::mock::Asserter;
    use bench_core::SenderConfig;

    fn mocked_provider(asserter: Asserter) -> DynProvider<AnyNetwork> {
        ProviderBuilder::new_with_network::<AnyNetwork>().connect_mocked_client(asserter).erased()
    }

    fn mock_submitter(provider: &DynProvider<AnyNetwork>) -> RpcSubmitter {
        RpcSubmitter::new(vec![provider.clone()], SenderConfig::default()).unwrap()
    }

    fn event_abi() -> JsonAbi {
        serde_json::from_str(
            r#"[{"type":"event","name":"Moved","anonymous":false,"inputs":[{"name":"from","type":"address","indexed":true},{"name":"amount","type":"uint256","indexed":false}]}]"#,
        )
        .unwrap()
    }

    fn encoded_log() -> Log {
        let event = resolve_event(&event_abi(), "Moved").unwrap().clone();
        let from = Address::repeat_byte(0x11);
        let amount = U256::from(7);
        Log {
            inner: alloy_primitives::Log {
                address: Address::repeat_byte(0x22),
                data: LogData::new_unchecked(
                    vec![event.selector(), B256::left_padding_from(from.as_slice())],
                    Bytes::from(DynSolValue::Uint(amount, 256).abi_encode()),
                ),
            },
            block_hash: Some(B256::repeat_byte(0x33)),
            block_number: Some(4),
            transaction_hash: Some(B256::repeat_byte(0x44)),
            log_index: Some(2),
            ..Default::default()
        }
    }

    #[test]
    fn decodes_indexed_and_unindexed_arguments_and_filters() {
        let abi = event_abi();
        let filters = BTreeMap::from([(
            "amount".to_string(),
            serde_yaml::Value::Number(serde_yaml::Number::from(7)),
        )]);
        let matcher = EventMatcher::new(&abi, "Moved", &filters, &RuntimeContext::empty()).unwrap();
        let values = matcher.decode_if_matches(&encoded_log()).unwrap().unwrap();
        assert_eq!(values[0], DynSolValue::Address(Address::repeat_byte(0x11)));
        assert_eq!(values[1], DynSolValue::Uint(U256::from(7), 256));
    }

    #[test]
    fn rejects_nonmatching_filter_and_ambiguous_event_name() {
        let mut abi = event_abi();
        let filters = BTreeMap::from([(
            "amount".to_string(),
            serde_yaml::Value::Number(serde_yaml::Number::from(8)),
        )]);
        let matcher = EventMatcher::new(&abi, "Moved", &filters, &RuntimeContext::empty()).unwrap();
        assert!(matcher.decode_if_matches(&encoded_log()).unwrap().is_none());

        let duplicate = resolve_event(&abi, "Moved").unwrap().clone();
        abi.events.entry("Moved".into()).or_default().push(duplicate);
        assert!(resolve_event(&abi, "Moved").is_err());
        assert!(resolve_event(&abi, "Moved(address,uint256)").is_ok());
    }

    #[test]
    fn hashes_indexed_dynamic_event_filters() {
        let abi: JsonAbi = serde_json::from_str(
            r#"[{"type":"event","name":"Tagged","anonymous":false,"inputs":[{"name":"tag","type":"string","indexed":true}]}]"#,
        )
        .unwrap();
        let event = resolve_event(&abi, "Tagged").unwrap();
        let tag_hash = keccak256("hello");
        let log = Log {
            inner: alloy_primitives::Log {
                address: Address::repeat_byte(0x22),
                data: LogData::new_unchecked(vec![event.selector(), tag_hash], Bytes::new()),
            },
            ..Default::default()
        };
        let filters =
            BTreeMap::from([("tag".to_string(), serde_yaml::Value::String("hello".to_string()))]);
        let matcher =
            EventMatcher::new(&abi, "Tagged", &filters, &RuntimeContext::empty()).unwrap();

        let decoded = matcher.decode_if_matches(&log).unwrap().unwrap();
        assert_eq!(decoded, vec![DynSolValue::FixedBytes(tag_hash, 32)]);
    }

    #[test]
    fn validates_constant_event_filters_before_execution() {
        let address_filter =
            EventFilterType { sol_type: DynSolType::Address, accepts_precomputed_hash: false };
        let number = serde_yaml::Value::Number(serde_yaml::Number::from(7));
        assert!(validate_constant_event_filter(&number, &address_filter).is_err());

        let indexed_string =
            EventFilterType { sol_type: DynSolType::String, accepts_precomputed_hash: true };
        let precomputed: serde_yaml::Value = serde_yaml::from_str("{ keccak256: hello }").unwrap();
        validate_constant_event_filter(&precomputed, &indexed_string).unwrap();
    }

    #[test]
    fn resolves_tuple_event_parameter_components() {
        let abi: JsonAbi = serde_json::from_str(
            r#"[{"type":"event","name":"Structured","anonymous":false,"inputs":[{"name":"payload","type":"tuple","indexed":false,"components":[{"name":"who","type":"address"},{"name":"amount","type":"uint256"}]}]}]"#,
        )
        .unwrap();
        let event = resolve_event(&abi, "Structured").unwrap();
        let payload = DynSolValue::Tuple(vec![
            DynSolValue::Address(Address::repeat_byte(0x11)),
            DynSolValue::Uint(U256::from(7), 256),
        ]);
        let log = Log {
            inner: alloy_primitives::Log {
                address: Address::repeat_byte(0x22),
                data: LogData::new_unchecked(
                    vec![event.selector()],
                    Bytes::from(DynSolValue::Tuple(vec![payload.clone()]).abi_encode_params()),
                ),
            },
            ..Default::default()
        };
        let matcher =
            EventMatcher::new(&abi, "Structured", &BTreeMap::new(), &RuntimeContext::empty())
                .unwrap();
        assert_eq!(matcher.decode_if_matches(&log).unwrap().unwrap(), vec![payload]);
    }

    #[test]
    fn log_save_contains_typed_arguments() {
        let abi = event_abi();
        let event = resolve_event(&abi, "Moved").unwrap();
        let matcher =
            EventMatcher::new(&abi, "Moved", &BTreeMap::new(), &RuntimeContext::empty()).unwrap();
        let log = encoded_log();
        let values = matcher.decode_if_matches(&log).unwrap().unwrap();
        let saved = log_runtime_value("chain_a", event, &log, values, 123).unwrap();
        let RuntimeValue::Object(saved) = saved else { panic!("expected object") };
        let RuntimeValue::Object(args) = &saved["args"] else { panic!("expected args") };
        assert_eq!(args["amount"], RuntimeValue::Uint(U256::from(7)));
        assert_eq!(args["from"], RuntimeValue::Address(Address::repeat_byte(0x11)));
    }

    #[test]
    fn sorts_by_block_and_log_index() {
        let mut logs = vec![
            Log { block_number: Some(2), log_index: Some(0), ..Default::default() },
            Log { block_number: Some(1), log_index: Some(3), ..Default::default() },
            Log { block_number: Some(1), log_index: Some(1), ..Default::default() },
        ];
        sort_logs(&mut logs);
        assert_eq!(
            logs.iter().map(|log| (log.block_number, log.log_index)).collect::<Vec<_>>(),
            vec![(Some(1), Some(1)), (Some(1), Some(3)), (Some(2), Some(0))]
        );
    }

    #[test]
    fn canonical_recheck_crosses_the_previous_scan_chunk() {
        let current_chunk_start = 100;
        let candidate_block = 110;
        let max_range = 10;
        let mut cursor = canonical_recheck_start(0, current_chunk_start);
        let mut ranges = Vec::new();
        loop {
            let end = bounded_range_end(cursor, candidate_block, max_range);
            ranges.push((cursor, end));
            if end == candidate_block {
                break;
            }
            cursor = end + 1;
        }

        assert_eq!(ranges.first(), Some(&(37, 46)));
        assert!(ranges.iter().any(|(_, end)| *end < current_chunk_start));
        assert_eq!(ranges.last(), Some(&(107, 110)));
    }

    #[tokio::test]
    async fn wait_log_backfills_before_polling() {
        let asserter = Asserter::new();
        let log = encoded_log();
        asserter.push_success(&10u64);
        asserter.push_success(&vec![log.clone()]);
        asserter.push_success(&vec![log]);
        let provider = mocked_provider(asserter.clone());
        let submitter = mock_submitter(&provider);
        let step = WaitLogStep {
            chain: "chain_a".to_string(),
            from_block: Some(serde_yaml::Value::Number(serde_yaml::Number::from(4))),
            address: None,
            transaction_hash: None,
            sender: None,
            abi: "events".to_string(),
            event: "Moved".to_string(),
            where_value: BTreeMap::new(),
            poll_interval: Some(Duration::from_millis(1)),
            confirmations: Some(0),
            max_block_range: Some(100),
        };

        let saved = wait_for_log(
            &provider,
            &submitter,
            "chain_a",
            &event_abi(),
            &step,
            &RuntimeContext::empty(),
        )
        .await
        .unwrap();
        let RuntimeValue::Object(saved) = saved else { panic!("expected object") };
        assert_eq!(saved["block_number"], RuntimeValue::Uint(U256::from(4)));
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn wait_log_recheck_preserves_first_match_across_a_reorg() {
        let mut later = encoded_log();
        later.block_number = Some(10);
        later.log_index = Some(0);
        let mut earlier = encoded_log();
        earlier.block_number = Some(5);
        earlier.log_index = Some(3);
        earlier.block_hash = Some(B256::repeat_byte(0x55));

        let asserter = Asserter::new();
        asserter.push_success(&10u64);
        asserter.push_success(&vec![later.clone()]);
        asserter.push_success(&vec![later, earlier]);
        let provider = mocked_provider(asserter.clone());
        let submitter = mock_submitter(&provider);
        let step = WaitLogStep {
            chain: "chain_a".to_string(),
            from_block: Some(serde_yaml::Value::Number(serde_yaml::Number::from(0))),
            address: None,
            transaction_hash: None,
            sender: None,
            abi: "events".to_string(),
            event: "Moved".to_string(),
            where_value: BTreeMap::new(),
            poll_interval: Some(Duration::from_millis(1)),
            confirmations: Some(0),
            max_block_range: Some(100),
        };

        let saved = wait_for_log(
            &provider,
            &submitter,
            "chain_a",
            &event_abi(),
            &step,
            &RuntimeContext::empty(),
        )
        .await
        .unwrap();
        let RuntimeValue::Object(saved) = saved else { panic!("expected object") };
        assert_eq!(saved["block_number"], RuntimeValue::Uint(U256::from(5)));
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn wait_log_does_not_scan_before_confirmation_depth_exists() {
        let asserter = Asserter::new();
        asserter.push_success(&0u64);
        let provider = mocked_provider(asserter.clone());
        let submitter = mock_submitter(&provider);
        let step = WaitLogStep {
            chain: "chain_a".to_string(),
            from_block: Some(serde_yaml::Value::Number(serde_yaml::Number::from(0))),
            address: None,
            transaction_hash: None,
            sender: None,
            abi: "events".to_string(),
            event: "Moved".to_string(),
            where_value: BTreeMap::new(),
            poll_interval: Some(Duration::from_secs(1)),
            confirmations: Some(1),
            max_block_range: Some(100),
        };

        assert!(tokio::time::timeout(
            Duration::from_millis(10),
            wait_for_log(
                &provider,
                &submitter,
                "chain_a",
                &event_abi(),
                &step,
                &RuntimeContext::empty(),
            ),
        )
        .await
        .is_err());
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn wait_log_rescans_recent_blocks_after_reorg() {
        let asserter = Asserter::new();
        let log = encoded_log();
        asserter.push_success(&4u64);
        asserter.push_success(&Vec::<Log>::new());
        asserter.push_success(&4u64);
        asserter.push_success(&vec![log.clone()]);
        asserter.push_success(&vec![log]);
        let provider = mocked_provider(asserter.clone());
        let submitter = mock_submitter(&provider);
        let step = WaitLogStep {
            chain: "chain_a".to_string(),
            from_block: Some(serde_yaml::Value::Number(serde_yaml::Number::from(4))),
            address: None,
            transaction_hash: None,
            sender: None,
            abi: "events".to_string(),
            event: "Moved".to_string(),
            where_value: BTreeMap::new(),
            poll_interval: Some(Duration::from_millis(1)),
            confirmations: Some(0),
            max_block_range: Some(100),
        };

        let saved = wait_for_log(
            &provider,
            &submitter,
            "chain_a",
            &event_abi(),
            &step,
            &RuntimeContext::empty(),
        )
        .await
        .unwrap();
        let RuntimeValue::Object(saved) = saved else { panic!("expected object") };
        assert_eq!(saved["block_number"], RuntimeValue::Uint(U256::from(4)));
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn receipt_wait_returns_revert_status_and_can_be_timed_out() {
        let transaction_hash = B256::repeat_byte(0x44);
        let block_hash = B256::repeat_byte(0x33);
        let bloom = format!("0x{}", "00".repeat(256));
        let receipt = serde_json::json!({
            "status": "0x0",
            "cumulativeGasUsed": "0x5208",
            "logs": [],
            "logsBloom": bloom,
            "type": "0x2",
            "transactionHash": transaction_hash,
            "transactionIndex": "0x0",
            "blockHash": block_hash,
            "blockNumber": "0x4",
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x1",
            "from": Address::repeat_byte(0x11),
            "to": Address::repeat_byte(0x22),
            "contractAddress": null
        });
        let asserter = Asserter::new();
        asserter.push_success(&receipt);
        asserter.push_success(&4u64);
        asserter.push_success(&receipt);
        asserter.push_success(&serde_json::json!({ "hash": block_hash }));
        let provider = mocked_provider(asserter);
        let submitter = mock_submitter(&provider);
        let result = wait_for_receipt(
            &provider,
            &submitter,
            "chain_a",
            None,
            transaction_hash,
            Duration::from_millis(1),
            0,
        )
        .await
        .unwrap();
        assert!(!result.status);

        let asserter = Asserter::new();
        for _ in 0..100 {
            asserter.push_success(&Option::<serde_json::Value>::None);
        }
        let provider = mocked_provider(asserter);
        let submitter = mock_submitter(&provider);
        assert!(tokio::time::timeout(
            Duration::from_millis(10),
            wait_for_receipt(
                &provider,
                &submitter,
                "chain_a",
                None,
                transaction_hash,
                Duration::from_millis(1),
                0,
            ),
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn receipt_canonicality_uses_the_query_block_hash() {
        let transaction_hash = B256::repeat_byte(0x44);
        let receipt: AnyTransactionReceipt = serde_json::from_value(serde_json::json!({
            "status": "0x1",
            "cumulativeGasUsed": "0x5208",
            "logs": [],
            "logsBloom": format!("0x{}", "00".repeat(256)),
            "type": "0x2",
            "transactionHash": transaction_hash,
            "transactionIndex": "0x0",
            "blockHash": B256::repeat_byte(0x33),
            "blockNumber": "0x4",
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x1",
            "from": Address::repeat_byte(0x11),
            "to": Address::repeat_byte(0x22),
            "contractAddress": null
        }))
        .unwrap();
        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!({ "hash": B256::repeat_byte(0x99) }));
        let provider = mocked_provider(asserter);

        assert!(!receipt_is_canonical_on_query(&provider, &receipt).await.unwrap());
    }
}
