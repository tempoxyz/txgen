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

    pub fn command_input_invalid() -> Self {
        Self::new("command_input_invalid", "command argument or environment expression is invalid")
    }

    pub fn command_spawn() -> Self {
        Self::new("command_spawn_error", "failed to start command")
    }

    pub fn command_io() -> Self {
        Self::new("command_io_error", "failed to capture command output")
    }

    pub fn command_exit_nonzero() -> Self {
        Self::new("command_exit_nonzero", "command exited unsuccessfully")
    }

    pub fn command_output_too_large() -> Self {
        Self::new("command_output_too_large", "command output exceeded the size limit")
    }

    pub fn command_output_invalid() -> Self {
        Self::new("command_output_invalid", "command stdout is not valid JSON output")
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
                "reverted_receipt" |
                "command_input_invalid" |
                "command_spawn_error" |
                "command_io_error" |
                "command_exit_nonzero" |
                "command_output_too_large" |
                "command_output_invalid"
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
