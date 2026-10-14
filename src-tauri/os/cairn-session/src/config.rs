//! Resolved session and backend capability values.
use cairn_models::Fence;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

/// One conversational message in a backend-neutral completion request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompletionRole {
    User,
    Assistant,
}

/// Canonical descriptor for backends whose agent loop performs provider calls
/// in-process and owns no resident provider child process.
pub fn stateless_http_runtime_capability(lane: &str) -> RuntimeLaunchCapability {
    RuntimeLaunchCapability {
        class: RuntimeClass::StatelessHttp,
        lane: RuntimeLane::new(lane),
        host_requirement: RuntimeHostRequirement::None,
        claim: RuntimeResourceClaim {
            resident_process_units: 0,
            server_instance_key: None,
            logical_stream_units: 1,
            estimated_memory_bytes: None,
        },
        supports_warm_reuse: false,
    }
}

/// The kind of runtime whose physical cost is incurred by one launch.
///
/// This vocabulary is deliberately backend-neutral: admission policy branches
/// on this enum and the accompanying claim, never on provider display names.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeClass {
    DedicatedProcess,
    MultiplexedServer,
    StatelessHttp,
}

/// Stable policy lookup key published by a backend.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct RuntimeLane(pub String);

impl RuntimeLane {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

/// Kind of host on which a resident runtime must physically exist.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeHostKind {
    Runner,
    Executor,
}

/// Placement requirement for a runtime claim. Stateless work has no resident
/// host; current CLI and app-server processes live on the runner.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(tag = "requirement", content = "host_kind", rename_all = "snake_case")]
pub enum RuntimeHostRequirement {
    None,
    ResidentOn(RuntimeHostKind),
}

/// Stable identity of the machine actually paying a resident runtime's cost.
/// It is intentionally independent of build-executor placement.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RuntimeHostIdentity {
    pub kind: RuntimeHostKind,
    pub id: String,
}

/// Resource units charged for one concrete launch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeResourceClaim {
    pub resident_process_units: u32,
    pub server_instance_key: Option<String>,
    pub logical_stream_units: u32,
    pub estimated_memory_bytes: Option<u64>,
}

/// Information known only after the launch path (ordinary session versus
/// pooled call) and backend configuration have been resolved.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeLaunch {
    pub is_ephemeral_call: bool,
    /// Required for a multiplexed launch. This must be the same configuration /
    /// authentication identity used to key the backend's real process pool.
    pub server_instance_key: Option<String>,
}

/// Backend-published admission metadata for one concrete launch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeLaunchCapability {
    pub class: RuntimeClass,
    pub lane: RuntimeLane,
    pub host_requirement: RuntimeHostRequirement,
    pub claim: RuntimeResourceClaim,
    pub supports_warm_reuse: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompletionMessage {
    pub role: CompletionRole,
    pub content: String,
}

/// A single model round-trip. Completion requests never expose tools or create a
/// session, run, transcript, or branch.
#[derive(Debug, Clone)]
pub struct CompletionRequest {
    pub system: Option<String>,
    pub messages: Vec<CompletionMessage>,
    pub model: String,
    /// Project whose provider-account scope should govern backend routing.
    pub project_id: Option<String>,
    pub extras: Value,
    pub output_schema: Option<Value>,
    pub timeout: Duration,
}

impl CompletionRequest {
    /// The whole request as one prompt, for a provider that takes a single
    /// message (a CLI one-shot). A lone user message is passed verbatim;
    /// earlier turns (examples, a contract retry) are rendered as a labelled
    /// transcript ahead of the final message.
    pub fn single_prompt(&self) -> String {
        let Some((last, earlier)) = self.messages.split_last() else {
            return String::new();
        };
        if earlier.is_empty() {
            return last.content.clone();
        }
        let mut prompt = String::from("Earlier turns of this conversation, oldest first:\n");
        for message in earlier {
            let role = match message.role {
                CompletionRole::User => "user",
                CompletionRole::Assistant => "assistant",
            };
            prompt.push_str(&format!("\n<{role}>\n{}\n</{role}>\n", message.content));
        }
        prompt.push_str("\nRespond to this final message:\n\n");
        prompt.push_str(&last.content);
        prompt
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompletionTokens {
    pub input: Option<u64>,
    pub output: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompletionOutcome {
    pub text: String,
    pub parsed: Option<Value>,
    pub model: String,
    pub tokens: CompletionTokens,
    pub cost: Option<f64>,
    pub latency_ms: u64,
    /// The Cairn account that served the completion, when the backend chose
    /// one. Journaled with the invocation so usage can be attributed to it.
    pub account_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompletionShape {
    InProcess,
    PooledSessions,
    DedicatedProcess,
}

/// A backend-diagnosed failure the lifecycle layer reacts to structurally.
///
/// Recorded on the run as `runs.exit_reason` before finalization, so the
/// reaction reads a typed fact rather than re-parsing provider CLI text. The
/// only layer that matches provider wording is the backend that emitted it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendFailure {
    /// The backend could not resolve the native conversation handle it was
    /// asked to resume. Cairn's own transcript is intact; only the
    /// provider-side conversation is unreachable.
    SessionUnresolvable,
}

impl BackendFailure {
    /// The token written to `runs.exit_reason`.
    pub fn exit_reason(self) -> &'static str {
        match self {
            BackendFailure::SessionUnresolvable => "session_unresolvable",
        }
    }

    /// Inverse of [`BackendFailure::exit_reason`]. Returns `None` for the exit
    /// reasons written by other paths (`capacity_retry`, `turn_failed`, …).
    pub fn from_exit_reason(value: &str) -> Option<Self> {
        match value {
            "session_unresolvable" => Some(BackendFailure::SessionUnresolvable),
            _ => None,
        }
    }
}

/// Backend-resolved agent permissions.
///
/// Each backend translates the canonical [`Fence`] into its own CLI flags or
/// protocol fields. Actual enforcement lives in Cairn's verb handlers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentPermissions {
    pub fence: Fence,
}

impl AgentPermissions {
    pub fn new(fence: Fence) -> Self {
        Self { fence }
    }

    /// Convert to a legacy permission mode string for the runtime stdin protocol.
    pub fn to_legacy_str(&self) -> &'static str {
        self.fence.to_legacy_permission_mode()
    }
}

// ============================================================================
// Tool resolution
// ============================================================================

/// Backend-resolved tool configuration.
#[derive(Debug, Clone)]
pub struct ResolvedTools {
    /// Tools the backend's native runtime should allow.
    pub allowed: Vec<String>,
    /// Tools the backend's native runtime should disallow.
    pub disallowed: Vec<String>,
}

/// Trait for agent execution backends.
///
/// Backends are responsible for spawning the agent process, registering it
/// in process state, and starting the reader thread that streams events
/// into the database.
/// Which process-minimization shape a backend offers for ephemeral (call)
/// fan-out. This is descriptive metadata the per-backend enforcement branches
/// on; the generic admission machinery only reads [`CallBatchCapability::max_concurrency`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallBatchShape {
    /// Whole agentic loop runs inside the runner; no child process per call
    /// (OpenRouter's async HTTP turn loop).
    InProcess,
    /// One long-lived pooled process; each call is a lightweight session on it
    /// (Codex app-server `thread/start` per call).
    PooledSessions,
    /// Each call is a dedicated backend process, admitted under a concurrency cap
    /// (Claude CLI, permanently CLI-bound).
    DedicatedProcess,
}

/// A backend's ephemeral-call batching capability: the shape it offers plus the
/// max number of concurrent ephemeral calls it will admit at once. `None` =
/// unbounded (pure passthrough — no admission bookkeeping).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallBatchCapability {
    pub shape: CallBatchShape,
    pub max_concurrency: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(messages: &[(CompletionRole, &str)]) -> CompletionRequest {
        CompletionRequest {
            system: None,
            messages: messages
                .iter()
                .map(|(role, content)| CompletionMessage {
                    role: role.clone(),
                    content: content.to_string(),
                })
                .collect(),
            model: "m".to_string(),
            project_id: None,
            extras: Value::Null,
            output_schema: None,
            timeout: Duration::from_secs(1),
        }
    }

    #[test]
    fn a_lone_message_is_the_prompt_verbatim() {
        assert_eq!(
            request(&[(CompletionRole::User, "Summarize {x}")]).single_prompt(),
            "Summarize {x}"
        );
    }

    /// Examples and a contract retry arrive as earlier turns; each keeps its
    /// role and order, and the final message is what the model answers.
    #[test]
    fn earlier_turns_render_as_a_labelled_transcript() {
        let prompt = request(&[
            (CompletionRole::User, "example in"),
            (CompletionRole::Assistant, "example out"),
            (CompletionRole::User, "real in"),
            (CompletionRole::Assistant, "bad answer"),
            (CompletionRole::User, "violated the contract; correct it"),
        ])
        .single_prompt();
        let order = [
            "<user>\nexample in\n</user>",
            "<assistant>\nexample out\n</assistant>",
            "<user>\nreal in\n</user>",
            "<assistant>\nbad answer\n</assistant>",
            "Respond to this final message:\n\nviolated the contract; correct it",
        ];
        let positions: Vec<usize> = order
            .iter()
            .map(|part| {
                prompt
                    .find(part)
                    .unwrap_or_else(|| panic!("{part:?} in {prompt}"))
            })
            .collect();
        assert!(
            positions.windows(2).all(|pair| pair[0] < pair[1]),
            "{prompt}"
        );
        assert!(prompt.ends_with("violated the contract; correct it"));
    }
}
