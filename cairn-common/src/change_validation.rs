//! Single-pass, pure validation for the canonical `write` (change) carrier.
//!
//! This is the one place that decides whether a `write` payload is well-formed,
//! shared by every call site so the error text is identical everywhere:
//!
//! - `cairn-cli`'s `write()` runs it pre-flight on the raw arguments, so the
//!   model gets every problem in one response with no server round-trip. The
//!   rmcp-facing input type is deliberately lenient (all fields optional) so
//!   serde never hard-rejects with an opaque `-32602 missing field 'changes'`;
//!   this validator owns the message instead.
//! - `cairn-core`'s `handle_change` runs the same function as the authoritative
//!   gate before the strict typed deserialize, so external MCP clients and the
//!   headless server are held to the same contract.
//!
//! It operates on the raw `serde_json::Value` (not a typed struct) so it can run
//! before any rewrite/expansion and report the *actual* JSON shape it found.
//! Crucially, it collects **all** blocking problems and returns them together
//! rather than stopping at the first — the model should learn every requirement
//! in a single pass.

use crate::contract::ChangeMode;
use serde_json::Value;

/// Coarse classification of a change target, used by validation and (via
/// `cairn-core`'s `target_family`) by the dispatcher. A strict superset of the
/// dispatcher's rule: it accepts both `cairn://` and `cairn:~` as resources so
/// it is correct on raw, pre-rewrite input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetKind {
    File,
    Resource,
    Invalid,
}

/// Classify a change target by its URI scheme.
///
/// - `file:...` -> File (worktree-relative, absolute, or bare `file:` root)
/// - `cairn:...` -> Resource (covers canonical `cairn://` and home-relative `cairn:~`)
/// - anything else -> Invalid
pub fn classify_target(target: &str) -> TargetKind {
    if target.starts_with("file:") {
        TargetKind::File
    } else if target.starts_with("cairn:") {
        TargetKind::Resource
    } else {
        TargetKind::Invalid
    }
}

/// One blocking problem with a `write` payload. `index` is the offending
/// `changes[]` position when the problem is item-scoped, or `None` for a
/// whole-call problem (e.g. `changes` absent).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeValidationError {
    pub index: Option<usize>,
    pub field: &'static str,
    pub message: String,
}

/// Human-readable name for a JSON value's type, for shape-mismatch messages.
fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Comma-separated list of every accepted mode, for invalid-mode messages.
fn allowed_modes() -> String {
    ChangeMode::ALL
        .iter()
        .map(|mode| mode.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Parse a mode string against the canonical `ChangeMode` enum (lowercase).
fn parse_mode(raw: &str) -> Option<ChangeMode> {
    serde_json::from_value::<ChangeMode>(Value::String(raw.to_string())).ok()
}

const COMMIT_MSG_GUIDANCE: &str =
    "file-target changes require a commit_msg so the work is committed to git — \
     uncommitted worktree edits are lost permanently if the worktree is cleaned up. \
     Pass a descriptive commit_msg. commit_msg: \"NO_COMMIT\" is reserved for resolving \
     an in-progress merge or rebase; the mutation handler rejects it (and restores the \
     worktree to HEAD) anywhere else.";

/// Validate a raw `write` payload and return **every** blocking problem.
///
/// An empty result means the payload is structurally valid (the strict typed
/// deserialize that follows should then succeed). A non-empty result should be
/// rendered with [`render_validation_errors`] and returned to the caller
/// without forwarding the call.
pub fn validate_change_value(payload: &Value) -> Vec<ChangeValidationError> {
    let mut errors = Vec::new();

    let obj = match payload.as_object() {
        Some(obj) => obj,
        None => {
            errors.push(ChangeValidationError {
                index: None,
                field: "changes",
                message: format!(
                    "the write payload must be a JSON object with a 'changes' array; got {}",
                    json_type_name(payload)
                ),
            });
            return errors;
        }
    };

    // Absent and explicit-null are treated identically: the field the model is
    // required to provide simply was not there. This is the precise message that
    // replaces the opaque rmcp `-32602 missing field 'changes'`.
    let changes = match obj.get("changes") {
        None | Some(Value::Null) => {
            errors.push(ChangeValidationError {
                index: None,
                field: "changes",
                message: "the 'changes' field is required and was not present in the call"
                    .to_string(),
            });
            return errors;
        }
        Some(value) => value,
    };

    let array = match changes.as_array() {
        Some(array) => array,
        None => {
            errors.push(ChangeValidationError {
                index: None,
                field: "changes",
                message: format!(
                    "'changes' must be an array of change items; got {}",
                    json_type_name(changes)
                ),
            });
            return errors;
        }
    };

    if array.is_empty() {
        errors.push(ChangeValidationError {
            index: None,
            field: "changes",
            message: "'changes' must contain at least one change item; the array was empty"
                .to_string(),
        });
        return errors;
    }

    // A null commit_msg is treated as absent (the lenient cairn-cli struct may
    // serialize a missing commit_msg as null).
    let commit_msg_present = obj
        .get("commit_msg")
        .map(|value| !value.is_null())
        .unwrap_or(false);
    let mut first_file_without_commit: Option<usize> = None;

    for (index, item) in array.iter().enumerate() {
        let item_obj = match item.as_object() {
            Some(item_obj) => item_obj,
            None => {
                errors.push(ChangeValidationError {
                    index: Some(index),
                    field: "item",
                    message: format!(
                        "each change item must be an object with 'target' and 'mode'; got {}",
                        json_type_name(item)
                    ),
                });
                continue;
            }
        };

        // --- target ---
        let target_str = match item_obj.get("target") {
            None | Some(Value::Null) => {
                errors.push(ChangeValidationError {
                    index: Some(index),
                    field: "target",
                    message: "'target' is required (a file: or cairn: URI) and was not present"
                        .to_string(),
                });
                None
            }
            Some(Value::String(s)) if s.trim().is_empty() => {
                errors.push(ChangeValidationError {
                    index: Some(index),
                    field: "target",
                    message: "'target' must be a non-empty string (a file: or cairn: URI)"
                        .to_string(),
                });
                None
            }
            Some(Value::String(s)) => Some(s.as_str()),
            Some(other) => {
                errors.push(ChangeValidationError {
                    index: Some(index),
                    field: "target",
                    message: format!(
                        "'target' must be a string (a file: or cairn: URI); got {}",
                        json_type_name(other)
                    ),
                });
                None
            }
        };

        // --- mode ---
        match item_obj.get("mode") {
            None | Some(Value::Null) => {
                errors.push(ChangeValidationError {
                    index: Some(index),
                    field: "mode",
                    message: format!(
                        "'mode' is required and was not present; allowed: {}",
                        allowed_modes()
                    ),
                });
            }
            Some(Value::String(s)) => {
                if parse_mode(s).is_none() {
                    errors.push(ChangeValidationError {
                        index: Some(index),
                        field: "mode",
                        message: format!(
                            "'{}' is not a valid mode; allowed: {}",
                            s,
                            allowed_modes()
                        ),
                    });
                }
            }
            Some(other) => {
                errors.push(ChangeValidationError {
                    index: Some(index),
                    field: "mode",
                    message: format!(
                        "'mode' must be a string; got {}; allowed: {}",
                        json_type_name(other),
                        allowed_modes()
                    ),
                });
            }
        }

        // --- target classification + commit_msg tracking ---
        if let Some(target) = target_str {
            match classify_target(target) {
                TargetKind::Invalid => {
                    errors.push(ChangeValidationError {
                        index: Some(index),
                        field: "target",
                        message: format!(
                            "invalid target '{}': expected a file: URI (file:relative/path, \
                             file:/absolute/path, or bare file:) or a cairn: resource URI",
                            target
                        ),
                    });
                }
                TargetKind::File => {
                    // Structural validation only checks that *some* commit_msg is
                    // present for file targets. Whether the `NO_COMMIT` sentinel
                    // is actually allowed depends on repo state (mid-merge /
                    // mid-rebase), which needs filesystem access this crate does
                    // not have; that gate lives in the mutation handler.
                    if !commit_msg_present && first_file_without_commit.is_none() {
                        first_file_without_commit = Some(index);
                    }
                }
                TargetKind::Resource => {}
            }
        }
    }

    // commit_msg is surfaced in the same pass as structural errors, keyed to the
    // first file target that lacks it.
    if let Some(index) = first_file_without_commit {
        errors.push(ChangeValidationError {
            index: Some(index),
            field: "commit_msg",
            message: COMMIT_MSG_GUIDANCE.to_string(),
        });
    }

    errors
}

/// Render a non-empty error list into one human-readable, multi-error message.
pub fn render_validation_errors(errors: &[ChangeValidationError]) -> String {
    if errors.is_empty() {
        return String::new();
    }

    let mut out = String::from(
        "write rejected before anything was applied. Fix all of the following and resend:\n",
    );
    for error in errors {
        match error.index {
            Some(index) => {
                out.push_str(&format!(
                    "  - changes[{}] ({}): {}\n",
                    index, error.field, error.message
                ));
            }
            None => {
                out.push_str(&format!("  - {}: {}\n", error.field, error.message));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn absent_changes_reports_not_present() {
        let errors = validate_change_value(&json!({}));
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].field, "changes");
        assert!(errors[0].message.contains("was not present"));
    }

    #[test]
    fn null_changes_reports_not_present() {
        let errors = validate_change_value(&json!({ "changes": null }));
        assert_eq!(errors.len(), 1);
        assert!(errors[0].message.contains("was not present"));
    }

    #[test]
    fn changes_wrong_type_reports_actual_type() {
        let errors = validate_change_value(&json!({ "changes": "oops" }));
        assert_eq!(errors.len(), 1);
        assert!(errors[0].message.contains("got string"));
    }

    #[test]
    fn changes_empty_array_reports_empty() {
        let errors = validate_change_value(&json!({ "changes": [] }));
        assert_eq!(errors.len(), 1);
        assert!(errors[0].message.contains("at least one"));
    }

    #[test]
    fn item_missing_target_reported() {
        let errors = validate_change_value(&json!({
            "changes": [{ "mode": "append" }]
        }));
        assert!(errors
            .iter()
            .any(|e| e.field == "target" && e.index == Some(0)));
    }

    #[test]
    fn item_missing_mode_reported_with_allowed_list() {
        let errors = validate_change_value(&json!({
            "changes": [{ "target": "cairn://p/CAIRN/1/messages" }]
        }));
        let mode_err = errors
            .iter()
            .find(|e| e.field == "mode")
            .expect("mode error");
        assert!(mode_err.message.contains("create"));
        assert!(mode_err.message.contains("unified_patch"));
    }

    #[test]
    fn invalid_mode_lists_allowed_modes() {
        let errors = validate_change_value(&json!({
            "changes": [{ "target": "cairn://p/CAIRN/1/messages", "mode": "bogus" }]
        }));
        let mode_err = errors
            .iter()
            .find(|e| e.field == "mode")
            .expect("mode error");
        assert!(mode_err.message.contains("bogus"));
        assert!(mode_err.message.contains("append"));
    }

    #[test]
    fn invalid_target_scheme_reported() {
        let errors = validate_change_value(&json!({
            "changes": [{ "target": "oops/path", "mode": "create" }]
        }));
        assert!(errors
            .iter()
            .any(|e| e.field == "target" && e.message.contains("invalid target")));
    }

    #[test]
    fn file_target_without_commit_msg_reported() {
        let errors = validate_change_value(&json!({
            "changes": [{ "target": "file:src/lib.rs", "mode": "create", "payload": { "content": "x" } }]
        }));
        let commit_err = errors
            .iter()
            .find(|e| e.field == "commit_msg")
            .expect("commit_msg error");
        assert_eq!(commit_err.index, Some(0));
        assert!(commit_err.message.contains("commit_msg"));
    }

    #[test]
    fn file_target_with_commit_msg_ok() {
        let errors = validate_change_value(&json!({
            "changes": [{ "target": "file:src/lib.rs", "mode": "create", "payload": { "content": "x" } }],
            "commit_msg": "add lib"
        }));
        assert!(errors.is_empty(), "unexpected: {errors:?}");
    }

    #[test]
    fn atomic_flag_is_accepted() {
        for atomic in [true, false] {
            let errors = validate_change_value(&json!({
                "changes": [{ "target": "file:src/lib.rs", "mode": "create", "payload": { "content": "x" } }],
                "commit_msg": "add lib",
                "atomic": atomic
            }));
            assert!(errors.is_empty(), "atomic {atomic} rejected: {errors:?}");
        }
    }

    #[test]
    fn no_commit_and_amend_sentinels_accepted() {
        for sentinel in ["NO_COMMIT", "^"] {
            let errors = validate_change_value(&json!({
                "changes": [{ "target": "file:a.rs", "mode": "create", "payload": { "content": "x" } }],
                "commit_msg": sentinel
            }));
            assert!(
                errors.is_empty(),
                "sentinel {sentinel} rejected: {errors:?}"
            );
        }
    }

    #[test]
    fn resource_only_needs_no_commit_msg() {
        let canonical = validate_change_value(&json!({
            "changes": [{ "target": "cairn://p/CAIRN/1/messages", "mode": "append", "payload": { "content": "hi" } }]
        }));
        assert!(canonical.is_empty(), "unexpected: {canonical:?}");

        let home = validate_change_value(&json!({
            "changes": [{ "target": "cairn:~/todos", "mode": "replace", "payload": { "todos": [] } }]
        }));
        assert!(home.is_empty(), "unexpected: {home:?}");
    }

    #[test]
    fn multiple_problems_all_reported_in_one_pass() {
        // index 0: file target, no commit_msg; index 1: bad mode + missing target.
        let errors = validate_change_value(&json!({
            "changes": [
                { "target": "file:src/lib.rs", "mode": "create", "payload": { "content": "x" } },
                { "mode": "bogus" }
            ]
        }));
        assert!(errors.iter().any(|e| e.field == "commit_msg"));
        assert!(errors
            .iter()
            .any(|e| e.field == "target" && e.index == Some(1)));
        assert!(errors
            .iter()
            .any(|e| e.field == "mode" && e.index == Some(1)));
        assert!(errors.len() >= 3, "expected >=3 errors, got {errors:?}");
    }

    #[test]
    fn render_lists_every_error() {
        let errors = validate_change_value(&json!({
            "changes": [{ "target": "file:a.rs", "mode": "bogus" }]
        }));
        let rendered = render_validation_errors(&errors);
        assert!(rendered.contains("changes[0]"));
        assert!(rendered.contains("mode"));
        assert!(rendered.contains("commit_msg"));
    }

    #[test]
    fn valid_mixed_batch_passes() {
        let errors = validate_change_value(&json!({
            "changes": [
                { "target": "file:src/lib.rs", "mode": "patch", "payload": { "old_string": "a", "new_string": "b" } },
                { "target": "cairn:~/messages", "mode": "append", "payload": { "content": "done" } }
            ],
            "commit_msg": "edit"
        }));
        assert!(errors.is_empty(), "unexpected: {errors:?}");
    }
}
