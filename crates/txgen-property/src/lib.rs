//! Swarm-based, model-driven property testing primitives for txgen.
//!
//! This crate deliberately does not choose a protocol model, RPC transport, or
//! coverage engine. A model owns generation and correctness; a harness owns
//! topology reset, execution, and observation; the runner owns swarm/case
//! scheduling and replayable failure artifacts.

#![warn(missing_docs, unreachable_pub)]

mod abi;
mod artifact;
mod registry;
mod runner;
mod swarm;

pub use abi::{AbiStrategy, AbiValueGenerator, GenerateContext};
pub use artifact::{FailureArtifact, FailureStage};
pub use registry::{ModelDescriptor, ModelRegistry};
pub use runner::{
    run, Prediction, PropertyHarness, PropertyModel, RunConfig, RunReport, RunResult,
};
pub use swarm::SwarmPolicy;
