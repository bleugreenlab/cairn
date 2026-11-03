//! Shared wire format types for MCP callback communication.
//!
//! These types define the HTTP request/response contract between the
//! MCP server binary and the Tauri backend callback server.

use serde::{Deserialize, Serialize};

/// Request from MCP server to Tauri backend (HTTP callback).
///
/// Derives `Default` so adding an optional field never ripples across the ~70
/// struct-literal construction sites: existing literals keep compiling, and new
/// optional fields can be filled in only where they matter via `..Default::default()`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CallbackRequest {
    /// Process residence reported for diagnostics and non-project host operations.
    /// Repository identity and relative logical paths never resolve from this value.
    pub cwd: String,
    /// Authenticated run identity required by project-scoped agent operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Tool name to invoke.
    pub tool: String,
    /// Tool-specific payload.
    pub payload: serde_json::Value,
    /// Tool use ID from MCP protocol - tracks parent for batch_tasks children.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_use_id: Option<String>,
    /// Codex pooled-app-server thread id (CAIRN-2549). Set by `cairn-cmd` from
    /// the `tools/call` request's `_meta.threadId` when Codex injects it (one
    /// shared app-server hosts N ephemeral call threads). The host maps this
    /// thread id back to the owning run, overriding the process-global
    /// `CAIRN_RUN_ID`/`cwd`, so each pooled call's tool results and `cairn:~/`
    /// targets attribute to the right run. `None` for every non-pooled caller
    /// (Claude, CLI, SDK, and Codex node/task sessions), which is unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    /// The longest, in milliseconds, the sender will wait for the host's
    /// answer. Set by every current client from
    /// `callback_contract::host_answer_bound`, and capped by every relay it
    /// crosses. The host answers within
    /// `callback_contract::effective_answer_bound`, which falls back to the
    /// legacy grace-sized bound when this is absent: an older client, or an
    /// older relay that re-serialized the request without it, never waits
    /// longer than that.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer_bound_ms: Option<u64>,
    /// Who dispatched this request when it is not an agent run. Only Rust sets
    /// it, and only from a verified credential.
    ///
    /// Never serialized and never deserialized: a request rebuilt from JSON (an
    /// agent's HTTP body, a suspended call re-driven from storage) always has
    /// `None` here, so an origin cannot be forged by a payload and cannot
    /// survive a round trip that would detach it from the check that set it.
    #[serde(skip)]
    pub origin: Option<DispatchOrigin>,
}

/// A non-run dispatcher of a [`CallbackRequest`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchOrigin {
    /// The verified operator writing rows of one thread's database through an
    /// instrument. Boxed: the appearance snapshot is large, and every request
    /// carries this field.
    RowWrite(Box<RowWriteOrigin>),
}

/// What an operator's row write was made through. The runner sets it from the
/// door the request came in by (the `page_write` or `write_db_rows` invoke),
/// never from anything the caller sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowWriteInstrument {
    /// A page's frame, at the page version it showed.
    Page {
        /// The canonical page URI.
        page: String,
        version: u32,
    },
    /// The app's database browser, in the operator's own webview.
    DbBrowser,
}

impl RowWriteInstrument {
    /// The instrument as a refusal names it.
    pub fn noun(&self) -> &'static str {
        match self {
            Self::Page { .. } => "a page",
            Self::DbBrowser => "the database browser",
        }
    }
}

/// The verified actor behind an operator's row write, the instrument it acted
/// through, and the one thread whose database it may touch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowWriteOrigin {
    pub instrument: RowWriteInstrument,
    /// The id of the one thread whose database this write may touch: a page's
    /// owner, or the thread whose rows the browser is showing.
    pub owner_thread_id: String,
    /// That thread's canonical URI.
    pub owner: String,
    pub project_id: String,
    /// The principal the verified invoke appearance names.
    pub actor: crate::identity::PrincipalRef,
    pub appearance: crate::identity::AppearanceSnapshot,
}

impl CallbackRequest {
    /// The operator's row write this request carries, if any.
    pub fn row_write_origin(&self) -> Option<&RowWriteOrigin> {
        match &self.origin {
            Some(DispatchOrigin::RowWrite(origin)) => Some(&**origin),
            None => None,
        }
    }
}

/// Response from Tauri backend to MCP server.
///
/// `result` is the handler's output and is NEVER mutated by the augmentation
/// layer. System-reminder augmentation (for example queued direct messages and
/// terminal polling guidance) rides separately in `reminders` as data, so a handler
/// that returns structured JSON (e.g. the read-batch envelope, a change report)
/// stays parseable end-to-end. The CLI assembles the model-visible text at the
/// transport edge: `result` first, then each reminder wrapped in a
/// `<system-reminder>` block, in delivery order.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CallbackResponse {
    pub result: String,
    /// Cairn artifact URI for this task's output, if available.
    /// Set by the task handler so batch_tasks can surface it in truncation messages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_uri: Option<String>,
    /// `<system-reminder>` bodies to append after `result`, in delivery order
    /// (queued DMs, then the dirty-worktree notice). Each entry is the inner
    /// text only; the CLI wraps it in the `<system-reminder>` envelope.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reminders: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The origin grants owner standing, so no JSON body may carry one: not
    /// under its own name, and not under any spelling a deserializer might
    /// map to it.
    #[test]
    fn an_origin_cannot_be_set_through_deserialization() {
        let forged = serde_json::json!({
            "cwd": "",
            "tool": "write",
            "payload": {"changes": []},
            "origin": {"RowWrite": {"instrument": "DbBrowser", "owner_thread_id": "thread-1"}},
            "Origin": "page",
        });
        let request: CallbackRequest = serde_json::from_value(forged).unwrap();
        assert_eq!(request.origin, None);
        assert!(request.row_write_origin().is_none());
    }

    #[test]
    fn an_origin_does_not_survive_a_round_trip() {
        use crate::identity::{
            Address, AppearanceEvidence, AppearanceSnapshot, AppearanceTransport, CredentialRef,
            PrincipalRef, VerificationMethod, VerificationRecord, VerificationStatus,
            VerificationStrength,
        };
        let actor = PrincipalRef::Machine {
            device_id: "device-1".to_string(),
        };
        let verification = VerificationRecord::new(
            VerificationMethod::DesktopCredential,
            VerificationStatus::Verified,
            None,
            None,
            None,
            Some(CredentialRef::new("desktop_operator_credential").unwrap()),
            VerificationStrength::new("local_shared_secret").unwrap(),
            1,
        )
        .unwrap();
        let evidence = AppearanceEvidence::new(
            AppearanceTransport::AuthenticatedDesktop,
            Address::Desktop {
                device_id: "device-1".to_string(),
            },
            verification,
            1,
            None,
        )
        .unwrap();
        let appearance =
            AppearanceSnapshot::new(actor.clone(), evidence, Vec::new(), None).unwrap();
        let request = CallbackRequest {
            tool: "write".to_string(),
            origin: Some(DispatchOrigin::RowWrite(Box::new(RowWriteOrigin {
                instrument: RowWriteInstrument::Page {
                    page: "cairn://p/cairn/t/pages/x".to_string(),
                    version: 1,
                },
                owner_thread_id: "thread-1".to_string(),
                owner: "cairn://p/cairn/t".to_string(),
                project_id: "project-1".to_string(),
                actor,
                appearance,
            }))),
            ..Default::default()
        };
        let json = serde_json::to_value(&request).unwrap();
        assert!(json.get("origin").is_none());
        let back: CallbackRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back.origin, None);
    }
}
