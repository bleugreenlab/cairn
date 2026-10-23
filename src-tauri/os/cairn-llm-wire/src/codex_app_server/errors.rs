//! The Codex error texts Cairn branches on.
//!
//! Codex reports these conditions only as human-readable JSON-RPC error
//! messages, so each predicate here is a claim about the exact wording the
//! pinned binary emits. The `codex-contract` suite asserts every predicate
//! against the real binary's message, which is what makes a reworded string in
//! a new Codex release fail the bump rather than silently change behavior.

use serde_json::Value;

pub use super::notification::is_selected_model_capacity_error;

/// The human-readable message inside a JSON-RPC error, whether the transport
/// handed over the bare error object, a whole response envelope, or plain text.
pub fn json_rpc_error_message(error: &str) -> String {
    serde_json::from_str::<Value>(error)
        .ok()
        .and_then(|value| {
            value
                .get("message")
                .and_then(Value::as_str)
                .or_else(|| value.pointer("/error/message").and_then(Value::as_str))
                .map(str::to_owned)
        })
        .unwrap_or_else(|| error.to_string())
}

/// `turn/interrupt` against a thread whose turn already ended.
pub fn is_no_active_turn_error(error: &str) -> bool {
    json_rpc_error_message(error).contains("no active turn to interrupt")
}

/// The live turn id named by a `turn/interrupt` that targeted a stale turn
/// ("expected active turn id A but found B"), so the interrupt can retry once.
pub fn active_turn_id_from_mismatch(error: &str) -> Option<String> {
    let message = json_rpc_error_message(error);
    let found = message.split(" but found ").nth(1)?.trim();
    let active_turn_id = found
        .trim_matches(|ch: char| ch == '`' || ch == '\'' || ch == '"')
        .split_whitespace()
        .next()?
        .trim_matches(|ch: char| ch == '`' || ch == '\'' || ch == '"' || ch == '.' || ch == ',');
    (!active_turn_id.is_empty() && active_turn_id != "none" && active_turn_id != "null")
        .then(|| active_turn_id.to_string())
}

/// `thread/resume` (or fork) of a thread id Codex has no rollout for, which
/// Cairn answers by starting a fresh thread. An empty `thread_id` matches any
/// thread.
pub fn is_missing_rollout_error(err: &str, thread_id: &str) -> bool {
    err.contains("no rollout found for thread id")
        && (thread_id.is_empty() || err.contains(thread_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn error_message_reads_bare_error_envelope_and_plain_text() {
        let bare = json!({"code": -32600, "message": "m1"}).to_string();
        let envelope = json!({"id": 3, "error": {"code": -32600, "message": "m2"}}).to_string();
        assert_eq!(json_rpc_error_message(&bare), "m1");
        assert_eq!(json_rpc_error_message(&envelope), "m2");
        assert_eq!(json_rpc_error_message("plain"), "plain");
    }

    #[test]
    fn no_active_turn_matches_the_interrupt_refusal() {
        let error = json!({"code": -32600, "message": "no active turn to interrupt"}).to_string();
        assert!(is_no_active_turn_error(&error));
        assert!(!is_no_active_turn_error(
            &json!({"message": "turn not found"}).to_string()
        ));
    }

    #[test]
    fn mismatch_yields_the_found_turn_id() {
        for message in [
            "expected active turn id turn-stale but found turn-active",
            "expected active turn id `turn-stale` but found `turn-active`.",
            "expected active turn id 'turn-stale' but found 'turn-active', retry",
        ] {
            let error = json!({"code": -32600, "message": message}).to_string();
            assert_eq!(
                active_turn_id_from_mismatch(&error).as_deref(),
                Some("turn-active"),
                "{message}"
            );
        }
        for message in [
            "expected active turn id turn-stale but found none",
            "expected active turn id turn-stale but found null",
            "no active turn to interrupt",
        ] {
            assert_eq!(active_turn_id_from_mismatch(message), None, "{message}");
        }
    }

    #[test]
    fn missing_rollout_matches_only_the_named_thread() {
        let error = "no rollout found for thread id 019a-thread";
        assert!(is_missing_rollout_error(error, "019a-thread"));
        assert!(is_missing_rollout_error(error, ""));
        assert!(!is_missing_rollout_error(error, "other-thread"));
        assert!(!is_missing_rollout_error("thread not loaded", ""));
    }
}
