use alloy_primitives::{Address, B256, Bytes, ChainId, Signature, TxKind, U256, keccak256};
use alloy_rlp::{BufMut, EMPTY_STRING_CODE, Encodable};

/// Tempo transaction type byte (0x76)
pub const TEMPO_TX_TYPE_ID: u8 = 0x76;

/// Magic byte for fee payer signature
pub const FEE_PAYER_SIGNATURE_MAGIC_BYTE: u8 = 0x78;

/// A call within a Tempo transaction.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Call {
    /// Call target.
    pub to: TxKind,
    /// Call value.
    pub value: U256,
    /// Call input data.
    pub input: Bytes,
}

impl Encodable for Call {
    fn encode(&self, out: &mut dyn BufMut) {
        let payload_length = self.to.length() + self.value.length() + self.input.length();
        alloy_rlp::Header {
            list: true,
            payload_length,
        }
        .encode(out);
        self.to.encode(out);
        self.value.encode(out);
        self.input.encode(out);
    }

    fn length(&self) -> usize {
        let payload_length = self.to.length() + self.value.length() + self.input.length();
        alloy_rlp::Header {
            list: true,
            payload_length,
        }
        .length_with_payload()
    }
}

/// Tempo transaction (type 0x76).
///
/// Supports:
/// - Parallelizable nonces via 2D nonce system (nonce_key + nonce)
/// - Gas sponsorship via fee payer
/// - Scheduled transactions (valid_before/valid_after)
/// - Batched calls
/// - Fee payment in stablecoins
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TempoTransaction {
    /// Chain ID (EIP-155).
    pub chain_id: ChainId,
    /// Optional fee token address.
    pub fee_token: Option<Address>,
    /// Max priority fee per gas (EIP-1559).
    pub max_priority_fee_per_gas: u128,
    /// Max fee per gas (EIP-1559).
    pub max_fee_per_gas: u128,
    /// Gas limit.
    pub gas_limit: u64,
    /// Calls to execute atomically.
    pub calls: Vec<Call>,
    /// Nonce key for 2D nonce system.
    pub nonce_key: U256,
    /// Nonce value for this nonce key.
    pub nonce: u64,
    /// Optional fee payer signature for sponsored transactions.
    pub fee_payer_signature: Option<Signature>,
    /// Transaction valid before this timestamp.
    pub valid_before: Option<u64>,
    /// Transaction valid after this timestamp.
    pub valid_after: Option<u64>,
}

/// Signed Tempo transaction.
pub struct SignedTempoTransaction {
    tx: TempoTransaction,
    signature: Signature,
}

impl SignedTempoTransaction {
    /// Get the transaction.
    pub fn tx(&self) -> &TempoTransaction {
        &self.tx
    }

    /// Get the signature.
    pub fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Encode the signed transaction as EIP-2718 envelope.
    pub fn encode_2718(&self, out: &mut dyn BufMut) {
        self.tx.encode_signed(&self.signature, out);
    }
}

impl TempoTransaction {
    /// Get the transaction type.
    pub const fn tx_type() -> u8 {
        TEMPO_TX_TYPE_ID
    }

    /// Convert into a signed transaction.
    pub fn into_signed(self, signature: Signature) -> SignedTempoTransaction {
        SignedTempoTransaction {
            tx: self,
            signature,
        }
    }

    /// Calculate the signature hash for signing.
    pub fn signature_hash(&self) -> B256 {
        let mut buf = Vec::new();
        self.encode_for_signing(&mut buf);
        keccak256(&buf)
    }

    /// Calculate the fee payer signature hash.
    pub fn fee_payer_signature_hash(&self, sender: Address) -> B256 {
        let payload_length = self.rlp_encoded_fields_length(sender.length(), false);

        let mut buf = Vec::with_capacity(1 + rlp_header(payload_length).length_with_payload());
        buf.put_u8(FEE_PAYER_SIGNATURE_MAGIC_BYTE);
        rlp_header(payload_length).encode(&mut buf);
        self.rlp_encode_fields_with_sig(&mut buf, |out| sender.encode(out), false);

        keccak256(&buf)
    }

    /// Encode for signing (what the sender signs).
    fn encode_for_signing(&self, out: &mut dyn BufMut) {
        let skip_fee_token = self.fee_payer_signature.is_some();
        out.put_u8(Self::tx_type());

        let sig_len = 1;
        let payload_length = self.rlp_encoded_fields_length(sig_len, skip_fee_token);
        rlp_header(payload_length).encode(out);
        self.rlp_encode_fields_with_sig(
            out,
            |out| {
                if self.fee_payer_signature.is_some() {
                    out.put_u8(0);
                } else {
                    out.put_u8(EMPTY_STRING_CODE);
                }
            },
            skip_fee_token,
        );
    }

    /// Encode the signed transaction.
    fn encode_signed(&self, signature: &Signature, out: &mut dyn BufMut) {
        let sig_payload_len = signature.rlp_rs_len() + signature.v().length();
        let sig_len = rlp_header(sig_payload_len).length_with_payload();

        let fee_payer_len = self.fee_payer_signature.as_ref().map_or(1, |s| {
            let payload = s.rlp_rs_len() + s.v().length();
            rlp_header(payload).length_with_payload()
        });

        let payload_length = self.rlp_encoded_fields_length(fee_payer_len, false) + sig_len;

        rlp_header(payload_length).encode(out);
        self.rlp_encode_fields_with_sig(
            out,
            |out| {
                if let Some(sig) = &self.fee_payer_signature {
                    let payload = sig.rlp_rs_len() + sig.v().length();
                    rlp_header(payload).encode(out);
                    sig.write_rlp_vrs(out, sig.v());
                } else {
                    out.put_u8(EMPTY_STRING_CODE);
                }
            },
            false,
        );

        rlp_header(sig_payload_len).encode(out);
        signature.write_rlp_vrs(out, signature.v());
    }

    fn rlp_encoded_fields_length(&self, signature_length: usize, skip_fee_token: bool) -> usize {
        self.chain_id.length()
            + self.max_priority_fee_per_gas.length()
            + self.max_fee_per_gas.length()
            + self.gas_limit.length()
            + self.calls.length()
            + 1 // empty access_list
            + self.nonce_key.length()
            + self.nonce.length()
            + self.valid_before.map_or(1, |v| v.length())
            + self.valid_after.map_or(1, |v| v.length())
            + match (skip_fee_token, self.fee_token) {
                (false, Some(addr)) => addr.length(),
                _ => 1,
            }
            + signature_length
            + 1 // empty tempo_authorization_list
    }

    fn rlp_encode_fields_with_sig(
        &self,
        out: &mut dyn BufMut,
        encode_signature: impl FnOnce(&mut dyn BufMut),
        skip_fee_token: bool,
    ) {
        self.chain_id.encode(out);
        self.max_priority_fee_per_gas.encode(out);
        self.max_fee_per_gas.encode(out);
        self.gas_limit.encode(out);
        self.calls.encode(out);

        // Empty access list
        out.put_u8(0xc0);

        self.nonce_key.encode(out);
        self.nonce.encode(out);

        if let Some(valid_before) = self.valid_before {
            valid_before.encode(out);
        } else {
            out.put_u8(EMPTY_STRING_CODE);
        }

        if let Some(valid_after) = self.valid_after {
            valid_after.encode(out);
        } else {
            out.put_u8(EMPTY_STRING_CODE);
        }

        if !skip_fee_token {
            if let Some(addr) = self.fee_token {
                addr.encode(out);
            } else {
                out.put_u8(EMPTY_STRING_CODE);
            }
        } else {
            out.put_u8(EMPTY_STRING_CODE);
        }

        encode_signature(out);

        // Empty tempo_authorization_list
        out.put_u8(0xc0);
    }
}

#[inline]
fn rlp_header(payload_length: usize) -> alloy_rlp::Header {
    alloy_rlp::Header {
        list: true,
        payload_length,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_call_rlp_encoding() {
        let call = Call {
            to: TxKind::Call(Address::ZERO),
            value: U256::from(1000),
            input: Bytes::from(vec![1, 2, 3, 4]),
        };

        let mut buf = Vec::new();
        call.encode(&mut buf);

        assert!(!buf.is_empty());
        assert_eq!(buf.len(), call.length());
    }

    #[test]
    fn test_tempo_tx_type() {
        assert_eq!(TempoTransaction::tx_type(), 0x76);
    }

    #[test]
    fn test_tempo_tx_signature_hash() {
        let tx = TempoTransaction {
            chain_id: 1,
            fee_token: None,
            max_priority_fee_per_gas: 1_000_000_000,
            max_fee_per_gas: 2_000_000_000,
            gas_limit: 21000,
            calls: vec![Call {
                to: TxKind::Call(Address::ZERO),
                value: U256::from(1000),
                input: Bytes::new(),
            }],
            nonce_key: U256::ZERO,
            nonce: 0,
            fee_payer_signature: None,
            valid_before: None,
            valid_after: None,
        };

        let hash = tx.signature_hash();
        assert!(!hash.is_zero());
    }

    #[test]
    fn test_tempo_tx_encode_signed() {
        let tx = TempoTransaction {
            chain_id: 1,
            fee_token: None,
            max_priority_fee_per_gas: 1_000_000_000,
            max_fee_per_gas: 2_000_000_000,
            gas_limit: 21000,
            calls: vec![Call {
                to: TxKind::Call(Address::ZERO),
                value: U256::from(1000),
                input: Bytes::new(),
            }],
            nonce_key: U256::ZERO,
            nonce: 0,
            fee_payer_signature: None,
            valid_before: None,
            valid_after: None,
        };

        let signature = Signature::test_signature();
        let signed = tx.into_signed(signature);

        let mut buf = Vec::new();
        signed.encode_2718(&mut buf);

        assert!(!buf.is_empty());
    }
}
