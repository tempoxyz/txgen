use std::{
    fs,
    path::{Path, PathBuf},
};

use eyre::{Result, WrapErr};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Point at which a property case failed.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureStage {
    /// The executed transition disagreed with the prediction.
    Transition,
    /// The final full-state verification failed.
    FinalVerification,
}

/// Secret-free, concrete property failure suitable for replay tooling.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FailureArtifact {
    /// Registered model name.
    pub model: String,
    /// Registered model version.
    pub model_version: String,
    /// RNG seed used by the run.
    pub seed: u64,
    /// Zero-based case index.
    pub case_index: u64,
    /// Step index, absent for a final verification failure.
    pub step_index: Option<usize>,
    /// Failure stage.
    pub stage: FailureStage,
    /// Human-readable verification error.
    pub error: String,
    /// Concrete swarm used for this case.
    pub swarm: Value,
    /// Concrete actions generated before the failure, including the failing action.
    pub actions: Vec<Value>,
    /// Last committed model state.
    pub committed_state: Value,
    /// Predicted state for a transition failure.
    pub predicted_state: Option<Value>,
    /// Expected outcome for a transition failure.
    pub expected: Option<Value>,
    /// Execution trace for a transition failure.
    pub trace: Option<Value>,
    /// Observation compared with the model.
    pub observation: Value,
}

impl FailureArtifact {
    /// Write a YAML artifact, creating the destination directory.
    pub fn write_yaml(&self, directory: &Path) -> Result<PathBuf> {
        fs::create_dir_all(directory).wrap_err_with(|| {
            format!("failed to create property failure directory {}", directory.display())
        })?;
        let path = directory.join(format!(
            "{}-seed-{}-case-{}-{}.yml",
            self.model,
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
