//! Native TIP-20 Channel Reserve settlement from a signed opening transaction.

use alloy_consensus::transaction::SignerRecoverable;
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::{address, keccak256, Address, Bytes, TxKind, B256, U256};
use alloy_signer::SignerSync;
use alloy_sol_types::{sol, SolCall, SolValue};
use eyre::{bail, Result, WrapErr};
use serde::Deserialize;
use tempo_primitives::{transaction::Call, TempoTxEnvelope};
use txgen_core::{AccountRef, BuildContext};

const RESERVE: Address = address!("4d50500000000000000000000000000000000000");

sol! {
    struct ChannelDescriptor {
        address payer;
        address payee;
        address operator;
        address token;
        bytes32 salt;
        address authorizedSigner;
        bytes32 expiringNonceHash;
    }

    function open(address payee, address operator, address token, uint96 deposit,
                  bytes32 salt, address authorizedSigner) returns (bytes32 channelId);
    function settle(ChannelDescriptor descriptor, uint96 cumulativeAmount, bytes signature);
}

/// A settlement's payload is derived from the actual signed open, never a guessed channel ID.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MppSettleDef {
    pub open_transaction: Bytes,
    /// Index of the native `open` call in the opening transaction's call array.
    #[serde(default)]
    pub call_index: usize,
    pub voucher_signer: AccountRef,
    pub cumulative_amount: alloy_primitives::aliases::U96,
}

pub(crate) fn settlement_call(
    def: &MppSettleDef,
    sender: Address,
    ctx: &mut BuildContext<'_>,
) -> Result<Call> {
    let mut encoded = def.open_transaction.as_ref();
    let envelope = TempoTxEnvelope::decode_2718(&mut encoded)
        .wrap_err("mpp_settle requires a signed opening transaction")?;
    if !encoded.is_empty() {
        bail!("mpp_settle opening transaction has trailing bytes");
    }
    let signed = envelope
        .as_aa()
        .ok_or_else(|| eyre::eyre!("mpp_settle requires a Tempo opening transaction"))?;
    let tx = signed.tx();
    if tx.chain_id != ctx.chain_id {
        bail!("mpp_settle opening transaction belongs to another chain");
    }
    let call = tx
        .calls
        .get(def.call_index)
        .ok_or_else(|| eyre::eyre!("mpp_settle opening call index out of bounds"))?;
    if call.to != TxKind::Call(RESERVE) || !call.value.is_zero() {
        bail!("mpp_settle opening call must target the native Channel Reserve with zero value");
    }
    let open = openCall::abi_decode_validate(&call.input)
        .wrap_err("mpp_settle opening call is not a valid native open")?;
    let payer = signed.recover_signer().wrap_err("invalid opening transaction signature")?;
    let descriptor = ChannelDescriptor {
        payer,
        payee: open.payee,
        operator: open.operator,
        token: open.token,
        salt: open.salt,
        authorizedSigner: open.authorizedSigner,
        expiringNonceHash: signed.expiring_nonce_hash(payer),
    };
    if sender != descriptor.payee &&
        (descriptor.operator.is_zero() || sender != descriptor.operator)
    {
        bail!("mpp_settle sender must be the channel payee or operator");
    }
    if def.cumulative_amount.is_zero() || def.cumulative_amount > open.deposit {
        bail!(
            "mpp_settle cumulative_amount must be positive and no greater than the opening deposit"
        );
    }
    let selected = ctx.select_signer(&def.voucher_signer)?;
    let expected_signer =
        if descriptor.authorizedSigner.is_zero() { payer } else { descriptor.authorizedSigner };
    if selected.address != expected_signer {
        bail!("mpp_settle voucher_signer does not match the channel's authorized signer");
    }
    let channel_id = keccak256(
        (
            descriptor.payer,
            descriptor.payee,
            descriptor.operator,
            descriptor.token,
            descriptor.salt,
            descriptor.authorizedSigner,
            descriptor.expiringNonceHash,
            RESERVE,
            U256::from(ctx.chain_id),
        )
            .abi_encode(),
    );
    let digest = voucher_digest(ctx.chain_id, channel_id, def.cumulative_amount);
    let signer = ctx.accounts.get_by_index(&selected.pool, selected.index)?;
    let signature = signer.sign_hash_sync(&digest)?;
    Ok(Call {
        to: TxKind::Call(RESERVE),
        value: U256::ZERO,
        input: settleCall {
            descriptor,
            cumulativeAmount: def.cumulative_amount,
            signature: signature.as_bytes().into(),
        }
        .abi_encode()
        .into(),
    })
}

fn voucher_digest(chain_id: u64, channel_id: B256, amount: alloy_primitives::aliases::U96) -> B256 {
    let domain = keccak256((
        keccak256(b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
        keccak256(b"TIP20 Channel Reserve"),
        keccak256(b"1"),
        U256::from(chain_id),
        RESERVE,
    ).abi_encode());
    let message = keccak256(
        (keccak256(b"Voucher(bytes32 channelId,uint96 cumulativeAmount)"), channel_id, amount)
            .abi_encode(),
    );
    let mut input = [0u8; 66];
    input[..2].copy_from_slice(&[0x19, 0x01]);
    input[2..34].copy_from_slice(domain.as_slice());
    input[34..].copy_from_slice(message.as_slice());
    keccak256(input)
}
