//! Swarm-based, model-free property testing primitives for txgen.
//!
//! A workload generator owns randomized actions. A live harness owns execution,
//! terminal lifecycle correlation, and independent chain-derived verification.
//! The runner owns swarm scheduling and replayable failure artifacts.

#![warn(missing_docs, unreachable_pub)]

mod abi;
mod artifact;
mod registry;
mod runner;
mod swarm;

pub use abi::{AbiStrategy, AbiValueGenerator, GenerateContext};
pub use artifact::{ActionArtifact, FailureArtifact, VerificationTrigger};
pub use registry::{CampaignDescriptor, CampaignRegistry};
pub use runner::{run, CampaignHarness, RunConfig, RunReport, RunResult, WorkloadGenerator};
pub use swarm::SwarmPolicy;
