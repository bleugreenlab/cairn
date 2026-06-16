/// Claude context window opt-in state.
///
/// Cairn currently uses subscription auth by default. Sonnet's 1M long-context
/// beta switches billing to API pricing, so callers pass `Subscription` until
/// that opt-in is explicitly wired through configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClaudeContextOptIn {
    #[default]
    Subscription,
    #[allow(dead_code)]
    LongContext1M,
}

/// Subscription-auth context windows by Claude alias. Sonnet's 1M long-context
/// beta switches billing to API pricing and is not wired in Cairn, so Sonnet
/// stays at its 200k subscription window. Opus and Fable offer 1M. Haiku is a
/// confirmed 200k model. Unknown aliases use the conservative 200k default.
pub fn claude_context_window(model: &str, opt_in: ClaudeContextOptIn) -> i64 {
    match (model, opt_in) {
        ("opus", _) => 1_000_000,
        ("fable", _) => 1_000_000,
        ("sonnet", ClaudeContextOptIn::LongContext1M) => 1_000_000,
        ("sonnet", _) => 200_000,
        ("haiku", _) => 200_000,
        _ => 200_000,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_context_window_matches_subscription_catalog() {
        assert_eq!(
            claude_context_window("opus", ClaudeContextOptIn::Subscription),
            1_000_000
        );
        assert_eq!(
            claude_context_window("fable", ClaudeContextOptIn::Subscription),
            1_000_000
        );
        assert_eq!(
            claude_context_window("sonnet", ClaudeContextOptIn::Subscription),
            200_000
        );
        assert_eq!(
            claude_context_window("sonnet", ClaudeContextOptIn::LongContext1M),
            1_000_000
        );
        assert_eq!(
            claude_context_window("haiku", ClaudeContextOptIn::Subscription),
            200_000
        );
        assert_eq!(
            claude_context_window("unknown", ClaudeContextOptIn::Subscription),
            200_000
        );
    }
}
