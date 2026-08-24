use std::{
    fs,
    path::{Path, PathBuf},
};

use eyre::{Result, WrapErr};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Reason an independent invariant verification was run.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationTrigger {
    /// An action reached its cross-layer terminal lifecycle state.
    TerminalTransition,
    /// The configured long-running workload interval elapsed.
    Periodic,
    /// A case completed and received its mandatory final verification.
    Final,
}

/// One concrete action and the evidence returned by the live harness.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ActionArtifact {
    /// Generated, replayable action.
    pub action: Value,
    /// Receipt or execution trace observed from RPC.
    pub trace: Value,
    /// Correlated terminal lifecycle evidence, when the action has a terminal transition.
    pub terminal_evidence: Option<Value>,
}

/// Secret-free, concrete invariant failure suitable for replay tooling.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FailureArtifact {
    /// Registered campaign name.
    pub campaign: String,
    /// Registered campaign serialization/semantics version.
    pub campaign_version: String,
    /// RNG seed generated for or supplied to the run.
    pub seed: u64,
    /// Zero-based case index.
    pub case_index: u64,
    /// Step that triggered verification, absent for final verification.
    pub step_index: Option<usize>,
    /// Why the verifier ran.
    pub trigger: VerificationTrigger,
    /// Human-readable invariant violation.
    pub error: String,
    /// Concrete swarm used for this case.
    pub swarm: Value,
    /// All executed actions and their actual chain evidence.
    pub actions: Vec<ActionArtifact>,
    /// Complete independent verifier report, including pinned snapshots and liabilities.
    pub verification: Value,
}

impl FailureArtifact {
    /// Write a YAML artifact, creating the destination directory.
    pub fn write_yaml(&self, directory: &Path) -> Result<PathBuf> {
        fs::create_dir_all(directory).wrap_err_with(|| {
            format!("failed to create property failure directory {}", directory.display())
        })?;
        let path = directory.join(format!(
            "{}-seed-{}-case-{}-{}.yml",
            self.campaign,
            self.seed,
            self.case_index,
            self.step_index
                .map(|step| format!("step-{step}"))
                .unwrap_or_else(|| "final".to_string())
        ));
        let yaml = serde_yaml::to_string(self).wrap_err("failed to serialize property failure")?;
        fs::write(&path, yaml)
            .wrap_err_with(|| format!("failed to write property failure {}", path.display()))?;
        Ok(path)
    }
}
