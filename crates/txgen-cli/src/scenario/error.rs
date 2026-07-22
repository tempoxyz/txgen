use std::fmt;

/// Sanitizable scenario-step failure.
///
/// Reports always include `classification`. Only bounded diagnostics from a
/// fixed allowlist of secret-free error categories are serialized.
#[derive(Debug)]
pub(crate) struct StepError {
    pub classification: &'static str,
    diagnostic: String,
}

impl StepError {
    pub fn new(classification: &'static str, diagnostic: impl Into<String>) -> Self {
        Self { classification, diagnostic: diagnostic.into() }
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

    /// Return a bounded diagnostic only for categories whose messages are
    /// derived from secret-free runtime paths, ABI metadata, or fixed text.
    pub fn sanitized_detail(&self) -> Option<String> {
        if !matches!(
            self.classification,
            "timeout" |
                "expression_error" |
                "abi_error" |
                "missing_data" |
                "context_error" |
                "binding_error" |
                "configuration_error" |
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
        let error = StepError::expression("界".repeat(300));
        let detail = error.sanitized_detail().unwrap();
        assert!(detail.len() <= 512);
        assert!(detail.is_char_boundary(detail.len()));
    }
}
