//! Command redaction for safe logging.

/// Redact common secret patterns from a command string for safe logging.
///
/// Replaces values matching bearer tokens, API keys, export statements with
/// secret-like variable names, and password flags with `[REDACTED]`.
///
/// This is intentionally conservative — it only redacts well-known patterns
/// to avoid false positives that would make logs unreadable.
pub fn redact_command(command: &str) -> String {
    use regex::Regex;
    use std::sync::LazyLock;

    static PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
        vec![
            // Bearer tokens: "Bearer sk-abc123..." or "Bearer eyJ..."
            // [^\s'"]+ avoids eating surrounding quotes
            Regex::new(r#"(?i)(Bearer\s+)[^\s'"]+"#).unwrap(),
            // API key patterns: sk-..., sk_live_..., etc.
            Regex::new(r"\bsk[-_][a-zA-Z0-9._-]{8,}\b").unwrap(),
            // export KEY=value, export SECRET=value, export TOKEN=value, export PASSWORD=value
            Regex::new(r"(?i)(export\s+[A-Z_]*(?:KEY|SECRET|TOKEN|PASSWORD)\s*=)\S+").unwrap(),
            // --password=value or --password value
            Regex::new(r"(?i)(--password[= ])\S+").unwrap(),
        ]
    });

    let mut result = command.to_string();
    for pattern in PATTERNS.iter() {
        result = pattern
            .replace_all(&result, |caps: &regex::Captures| {
                // Preserve the prefix (e.g., "Bearer ", "export API_KEY=", "--password=")
                if let Some(prefix) = caps.get(1) {
                    format!("{}[REDACTED]", prefix.as_str())
                } else {
                    "[REDACTED]".to_string()
                }
            })
            .to_string();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redact_bearer_token() {
        assert_eq!(
            redact_command(r#"curl -H "Authorization: Bearer sk-abc123xyz""#),
            r#"curl -H "Authorization: Bearer [REDACTED]""#
        );
    }

    #[test]
    fn test_redact_bearer_case_insensitive() {
        assert_eq!(
            redact_command("curl -H 'bearer eyJhbGciOiJI.long.token'"),
            "curl -H 'bearer [REDACTED]'"
        );
    }

    #[test]
    fn test_redact_api_key_pattern() {
        assert_eq!(
            redact_command("echo sk-proj-abcdefghij123456"),
            "echo [REDACTED]"
        );
        assert_eq!(
            redact_command("echo sk_live_abcdefghij123456"),
            "echo [REDACTED]"
        );
    }

    #[test]
    fn test_redact_export_secret_vars() {
        assert_eq!(
            redact_command("export API_KEY=supersecret123"),
            "export API_KEY=[REDACTED]"
        );
        assert_eq!(
            redact_command("export OPENAI_SECRET=sk-abc123456789"),
            "export OPENAI_SECRET=[REDACTED]"
        );
        assert_eq!(
            redact_command("export AUTH_TOKEN=eyJhbGciOiJI"),
            "export AUTH_TOKEN=[REDACTED]"
        );
        assert_eq!(
            redact_command("export DB_PASSWORD=hunter2"),
            "export DB_PASSWORD=[REDACTED]"
        );
    }

    #[test]
    fn test_redact_password_flag() {
        assert_eq!(
            redact_command("mysql --password=secret123 -u root"),
            "mysql --password=[REDACTED] -u root"
        );
        assert_eq!(
            redact_command("mysql --password secret123 -u root"),
            "mysql --password [REDACTED] -u root"
        );
    }

    #[test]
    fn test_redact_normal_commands_unchanged() {
        let normal = "git status --short";
        assert_eq!(redact_command(normal), normal);

        let normal2 = "ls -la /tmp";
        assert_eq!(redact_command(normal2), normal2);

        let normal3 = "cargo build --release";
        assert_eq!(redact_command(normal3), normal3);
    }

    #[test]
    fn test_redact_multiple_secrets_in_one_command() {
        let cmd = "export API_KEY=secret1 && curl -H 'Bearer sk-abc123456789'";
        let redacted = redact_command(cmd);
        assert!(!redacted.contains("secret1"));
        assert!(!redacted.contains("sk-abc123456789"));
        assert!(redacted.contains("[REDACTED]"));
    }

    #[test]
    fn test_redact_short_sk_not_matched() {
        // Short sk- patterns (less than 8 chars after prefix) should NOT be redacted
        assert_eq!(redact_command("echo sk-short"), "echo sk-short");
    }
}
