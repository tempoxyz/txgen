use super::report::ProtocolMilestone;
use std::fmt;

/// Sanitizable scenario-step failure.
///
/// Reports always include `classification`. Only bounded diagnostics from a
/// fixed allowlist of secret-free error categories are serialized.
#[derive(Debug)]
pub(crate) struct StepError {
    pub classification: &'static str,
    diagnostic: String,
    milestones: Vec<ProtocolMilestone>,
}

impl StepError {
    pub fn new(classification: &'static str, diagnostic: impl Into<String>) -> Self {
        Self { classification, diagnostic: diagnostic.into(), milestones: Vec::new() }
    }

    pub fn timeout() -> Self {
        Self::new("timeout", "step timeout elapsed")
    }

    pub fn rpc(error: impl fmt::Display) -> Self {
        Self::new("rpc_error", error.to_string())
    }

    pub fn expression(error: impl fmt::Display) -> Self {
        Self::new("expression_error", error.to_string())
    }

    pub fn abi(error: impl fmt::Display) -> Self {
        Self::new("abi_error", error.to_string())
    }

    pub fn missing(diagnostic: impl Into<String>) -> Self {
        Self::new("missing_data", diagnostic)
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
        let fixed = match self.classification {
            // These diagnostics can include the runtime value that failed
            // evaluation or coercion, so reports expose only fixed text.
            "expression_error" => Some("expression evaluation failed"),
            "abi_error" => Some("ABI operation failed"),
            _ => None,
        };
        if let Some(detail) = fixed {
            return Some(detail.to_string());
        }
        if !matches!(
            self.classification,
            "timeout" |
                "missing_data" |
                "context_error" |
                "binding_error" |
                "configuration_error" |
                "unsafe_parallel_nonce" |
                "nonce_state_ambiguous" |
                "nonce_recovery_error" |
                "rpc_hash_mismatch" |
                "reverted_receipt"
        ) {
            return None;
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
