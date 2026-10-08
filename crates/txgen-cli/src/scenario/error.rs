use super::report::ProtocolMilestone;
use std::fmt;

/// Reported category of a scenario-step failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StepErrorKind {
    Timeout,
    Rpc,
    Expression,
    Abi,
    MissingData,
    Context,
    Binding,
    Configuration,
    UnsafeParallelNonce,
    NonceStateAmbiguous,
    NonceRecovery,
    RpcHashMismatch,
    RevertedReceipt,
    Invoke,
    Materialization,
    Template,
    SubmissionAmbiguous,
    SubmissionRejected,
}

impl StepErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Rpc => "rpc_error",
            Self::Expression => "expression_error",
            Self::Abi => "abi_error",
            Self::MissingData => "missing_data",
            Self::Context => "context_error",
            Self::Binding => "binding_error",
            Self::Configuration => "configuration_error",
            Self::UnsafeParallelNonce => "unsafe_parallel_nonce",
            Self::NonceStateAmbiguous => "nonce_state_ambiguous",
            Self::NonceRecovery => "nonce_recovery_error",
            Self::RpcHashMismatch => "rpc_hash_mismatch",
            Self::RevertedReceipt => "reverted_receipt",
            Self::Invoke => "invoke_error",
            Self::Materialization => "materialization_error",
            Self::Template => "template_error",
            Self::SubmissionAmbiguous => "submission_ambiguous",
            Self::SubmissionRejected => "submission_rejected",
        }
    }
}

/// Sanitizable scenario-step failure.
///
/// Reports always include `classification`. Only bounded diagnostics from a
/// fixed allowlist of secret-free error categories are serialized.
#[derive(Debug)]
pub(crate) struct StepError {
    pub classification: StepErrorKind,
    diagnostic: String,
    milestones: Vec<ProtocolMilestone>,
}

impl StepError {
    pub fn new(classification: StepErrorKind, diagnostic: impl Into<String>) -> Self {
        Self { classification, diagnostic: diagnostic.into(), milestones: Vec::new() }
    }

    pub fn timeout() -> Self {
        Self::new(StepErrorKind::Timeout, "step timeout elapsed")
    }

    pub fn rpc(error: impl fmt::Display) -> Self {
        Self::new(StepErrorKind::Rpc, error.to_string())
    }

    pub fn expression(error: impl fmt::Display) -> Self {
        Self::new(StepErrorKind::Expression, error.to_string())
    }

    pub fn abi(error: impl fmt::Display) -> Self {
        Self::new(StepErrorKind::Abi, error.to_string())
    }

    pub fn missing(diagnostic: impl Into<String>) -> Self {
        Self::new(StepErrorKind::MissingData, diagnostic)
    }

    pub fn with_milestones(mut self, milestones: Vec<ProtocolMilestone>) -> Self {
        self.milestones = milestones;
        self
    }

    pub fn milestones(&self) -> &[ProtocolMilestone] {
        &self.milestones
    }

    /// Return a bounded diagnostic only for categories whose messages are
    /// derived from secret-free runtime paths, ABI metadata, or fixed text.
    pub fn sanitized_detail(&self) -> Option<String> {
        use StepErrorKind::*;
        match self.classification {
            // These diagnostics can include the runtime value that failed
            // evaluation or coercion, so reports expose only fixed text.
            Expression => return Some("expression evaluation failed".to_string()),
            Abi => return Some("ABI operation failed".to_string()),
            Rpc | Invoke | Materialization | Template | SubmissionAmbiguous |
            SubmissionRejected => return None,
            Timeout | MissingData | Context | Binding | Configuration | UnsafeParallelNonce |
            NonceStateAmbiguous | NonceRecovery | RpcHashMismatch | RevertedReceipt => {}
        }
        let mut detail = self.diagnostic.replace(['\r', '\n'], " ");
        if detail.len() > 512 {
            let mut boundary = 512;
            while !detail.is_char_boundary(boundary) {
                boundary -= 1;
            }
            detail.truncate(boundary);
        }
        Some(detail)
    }
}

impl fmt::Display for StepError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.diagnostic)
    }
}

impl std::error::Error for StepError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitized_unicode_diagnostic_is_truncated_on_a_character_boundary() {
        let error = StepError::missing("界".repeat(300));
        let detail = error.sanitized_detail().unwrap();
        assert!(detail.len() <= 512);
        assert!(detail.is_char_boundary(detail.len()));
    }

    #[test]
    fn expression_and_abi_details_never_echo_runtime_values() {
        assert_eq!(
            StepError::expression("secret runtime string").sanitized_detail().as_deref(),
            Some("expression evaluation failed")
        );
        assert_eq!(
            StepError::abi("failed to coerce \"secret runtime string\"")
                .sanitized_detail()
                .as_deref(),
            Some("ABI operation failed")
        );
    }
}
