use super::report::ProtocolMilestone;
use alloy_transport::{RpcError, TransportError};
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
/// fixed allowlist of secret-free error categories or typed adapter diagnostics
/// are serialized.
#[derive(Debug)]
pub(crate) struct StepError {
    pub classification: StepErrorKind,
    diagnostic: String,
    milestones: Vec<ProtocolMilestone>,
    invoke_detail: Option<String>,
}

impl StepError {
    pub fn new(classification: StepErrorKind, diagnostic: impl Into<String>) -> Self {
        Self {
            classification,
            diagnostic: diagnostic.into(),
            milestones: Vec::new(),
            invoke_detail: None,
        }
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
        if let Some(detail) = &self.invoke_detail {
            return Some(bounded_detail(detail));
        }
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
        Some(bounded_detail(&self.diagnostic))
    }

    pub fn invoke(error: eyre::Report) -> Self {
        let mut details = error
            .downcast_ref::<ScenarioActionDiagnostic>()
            .map(|diagnostic| diagnostic.0)
            .into_iter()
            .collect::<Vec<_>>();
        let rpc = error.downcast_ref::<TransportError>();
        let rpc_detail = rpc.map(|error| match error {
            RpcError::ErrorResp(payload) => format!("RPC error code {}", payload.code),
            RpcError::NullResp => "empty RPC response".to_string(),
            RpcError::DeserError { .. } => "invalid RPC response".to_string(),
            RpcError::Transport(kind) => kind.as_http_error().map_or_else(
                || "RPC transport failed".to_string(),
                |error| format!("RPC HTTP status {}", error.status),
            ),
            _ => "RPC client failed".to_string(),
        });
        if let Some(detail) = &rpc_detail {
            details.push(detail);
        }
        if details.is_empty() {
            details.push("adapter invocation failed; no safe diagnostic provided");
        }
        let detail = bounded_detail(&details.join(": "));
        let mut step_error = Self::new(StepErrorKind::Invoke, detail.clone());
        step_error.invoke_detail = Some(detail);
        step_error
    }
}

impl fmt::Display for StepError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.diagnostic)
    }
}

impl std::error::Error for StepError {}

/// An adapter diagnostic that is safe to include in scenario reports.
///
/// Use fixed text to describe the failed operation or validation. Never include
/// credentials, arguments, RPC response bodies, or other runtime values.
#[derive(Debug)]
pub struct ScenarioActionDiagnostic(pub &'static str);

impl fmt::Display for ScenarioActionDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for ScenarioActionDiagnostic {}

fn bounded_detail(diagnostic: &str) -> String {
    let mut detail = diagnostic.replace(['\r', '\n'], " ");
    if detail.len() > 512 {
        let mut boundary = 512;
        while !detail.is_char_boundary(boundary) {
            boundary -= 1;
        }
        detail.truncate(boundary);
    }
    detail
}

#[cfg(test)]
mod tests {
    use super::*;
    use eyre::WrapErr;

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

    #[test]
    fn invoke_detail_keeps_operation_and_rpc_code_without_remote_message_or_data() {
        let rpc: TransportError = TransportError::ErrorResp(
            serde_json::from_value(serde_json::json!({
                "code": -32005,
                "message": "request https://user:password@rpc.invalid/private-token failed",
                "data": {"authorization": "secret-token"}
            }))
            .unwrap(),
        );
        let report = Err::<(), _>(rpc)
            .wrap_err(ScenarioActionDiagnostic("call ZonePortal.encryptionKeyCount()"))
            .unwrap_err();
        let error = StepError::invoke(report);
        assert_eq!(
            error.sanitized_detail().as_deref(),
            Some("call ZonePortal.encryptionKeyCount(): RPC error code -32005")
        );
        assert_eq!(
            error.to_string(),
            "call ZonePortal.encryptionKeyCount(): RPC error code -32005"
        );
    }

    #[test]
    fn invoke_detail_retains_typed_validation_but_hides_arbitrary_adapter_errors() {
        let report = eyre::eyre!("sensitive runtime argument")
            .wrap_err(ScenarioActionDiagnostic("invalid sequencer encryption public key"));
        assert_eq!(
            StepError::invoke(report).sanitized_detail().as_deref(),
            Some("invalid sequencer encryption public key")
        );
        assert_eq!(
            StepError::invoke(eyre::eyre!("sensitive runtime argument"))
                .sanitized_detail()
                .as_deref(),
            Some("adapter invocation failed; no safe diagnostic provided")
        );
    }

    #[test]
    fn invoke_http_failure_reports_status_without_response_body() {
        let rpc: TransportError =
            RpcError::Transport(alloy_transport::TransportErrorKind::HttpError(
                alloy_transport::HttpError { status: 503, body: "secret response body".into() },
            ));
        assert_eq!(
            StepError::invoke(eyre::Report::new(rpc)).sanitized_detail().as_deref(),
            Some("RPC HTTP status 503")
        );
    }
}
