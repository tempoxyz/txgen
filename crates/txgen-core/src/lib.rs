// `eyre::bail!` expands with a trailing semicolon, which Rust 1.93 warns about when the macro is
// used as the value of a match arm. Keep these idiomatic call sites until eyre updates the macro.
#![allow(semicolon_in_expressions_from_macros)]

mod abi;
mod accounts;
mod nonce;
pub mod output;
mod plugin;
mod scheduling_key;
mod spec;
mod value;
mod yaml;

pub use abi::{ArtifactDef, ArtifactManager, CallDef, EncodedCall};
pub use accounts::{
    derive_mnemonic_signer, AccountManager, AccountPoolDef, AccountRef, AddressPoolDef,
    AddressPoolManager, EcdsaSigner, FastAddressPoolDef, SelectMode, SignerExt,
};
pub use nonce::NonceTracker;
pub use output::{GeneratedTx, NdjsonWriter, TxPhase};
pub use plugin::{BuildContext, SelectedSigner};
pub use scheduling_key::{dedup_scheduling_keys, SchedulingKey};
pub use spec::{
    AbiEncodePackedDef, AbiHashDef, GasConfig, MixEntry, MixItem, SequenceBinding, SequenceDef,
    SequenceStep, SetupDef, SetupStep, WorkloadSpec,
};
pub use value::{FromGenerator, GenValue, Generator, ValueResolver};
pub use yaml::{append_yaml, merge_yaml};
