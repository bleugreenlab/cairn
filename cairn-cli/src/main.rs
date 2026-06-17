use anyhow::Result;
use clap::Parser;
use rmcp::{
    handler::server::tool::{Parameters, ToolCallContext, ToolRouter},
    model::{
        CallToolRequestParam, CallToolResult, Content, Implementation, ListToolsResult,
        PaginatedRequestParam, ProtocolVersion, ReadResourceRequestParam, ReadResourceResult,
        ResourceContents, ServerCapabilities, ServerInfo,
    },
    service::RequestContext,
    tool, tool_router, RoleServer, ServerHandler, ServiceExt,
};
use serde::{Deserialize, Serialize};
use std::env;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cairn_common::query::split_target_query;
use cairn_common::read::ReadBatchEnvelope;
use cairn_common::uri::{parse_uri as parse_cairn_uri, CairnResource};

/// Cairn MCP Server - tools for Claude to interact with Cairn
#[derive(Parser)]
#[command(name = "cairn", version)]
struct Args {
    /// Subcommand. When omitted (or `mcp`), runs the stdio MCP server.
    #[command(subcommand)]
    command: Option<Command>,

    /// JSON-encoded list of available agents [{name, description}, ...]
    #[arg(long)]
    agents: Option<String>,
}

/// Top-level CLI subcommands.
///
/// The `cairn` binary is a thin client: `read`/`write` build a callback
/// request and forward it to the running Cairn app over the same HTTP callback
/// the MCP server uses, then print the result to stdout. The binary never opens
/// the database itself (Turso holds a process-exclusive lock; the app is the
/// sole owner). If the app is unreachable, we autostart it and retry once.
#[derive(clap::Subcommand)]
enum Command {
    /// Run the stdio MCP server (default when no subcommand is given).
    Mcp,
    /// Read one or more files or Cairn resources and print them to stdout (pipeable).
    Read {
        /// One or more targets: `file:path` (worktree-relative), `file:/abs/path`, `cairn://p/PROJECT/...`, or `cairn:~/...`.
        /// Append `?key=value` to a target for per-target scoping.
        #[arg(required = true, num_args = 1..)]
        targets: Vec<String>,
        /// Start reading from this line number. Single-target convenience: folded
        /// into that target's query string. Rejected with multiple targets
        /// (scope each target via its own query string instead).
        #[arg(long)]
        offset: Option<usize>,
        /// Read at most this many lines. Single-target convenience (see --offset).
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Apply ordered file/resource mutations (ChangeInput JSON via --json or stdin).
    #[command(alias = "change")]
    Write {
        /// ChangeInput JSON (an object with `changes`, or a bare `changes` array).
        /// If omitted, JSON is read from stdin.
        #[arg(long)]
        json: Option<String>,
        /// Commit message for file-target changes (`^` to amend).
        #[arg(long = "commit-msg")]
        commit_msg: Option<String>,
    },
    /// Block until an issue needs attention (no polling), then print it and exit.
    ///
    /// A single `cairn watch` is one continuous blocking call to the caller: it
    /// long-polls the running app and transparently re-issues across the server
    /// budget, carrying the last-seen cursor as `--since` so nothing is missed.
    Watch {
        /// Issue URI, e.g. `cairn://p/PROJECT/NUMBER`.
        issue_uri: String,
        /// Only return attention newer than this issue `updated_at` (unix seconds).
        #[arg(long)]
        since: Option<i64>,
    },
}

/// Agent info for tool description
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentInfo {
    name: String,
    description: String,
}

// ---------------------------------------------------------------------------
// cairn-cli -> host HTTP callback timeout
//
// The host owns execution. Every outer layer's ceiling is derived to sit
// strictly *above* the layer below it, so the HTTP socket never fires before
// the host's own timeout returns a (partial) result. See
// `mcp/handlers/bash.rs` for the host per-item budget this mirrors.
// ---------------------------------------------------------------------------

/// Host per-item execution budget for `run` items, mirroring
/// `mcp/handlers/bash.rs` (`timeout.unwrap_or(120_000).min(600_000)` ms). The
/// host is the only layer that returns partial output on expiry.
const HOST_ITEM_TIMEOUT_DEFAULT_MS: u64 = 120_000;
const HOST_ITEM_TIMEOUT_MAX_MS: u64 = 600_000;

/// Margin added above a `run` batch's host budget so the HTTP socket always
/// outlives the host's own per-item timeout and the host's (partial) result
/// wins the race.
const CALLBACK_TIMEOUT_MARGIN: Duration = Duration::from_secs(60);

/// HTTP callback ceiling for verbs whose host work is bounded and short: a
/// `read`, a non-blocking `write`, a resource read, or a `watch` long-poll (the
/// host returns its `pending` sentinel at 290s, comfortably under this).
const DEFAULT_CALLBACK_TIMEOUT: Duration = Duration::from_secs(600);

/// HTTP callback ceiling for verbs that block on an unbounded external event
/// with no host-side timeout below them: a blocking `write` append to a node's
/// tasks/questions collection (a sub-agent task or user question that may
/// legitimately run far longer than any `run` batch). The host owns these
/// awaits; the socket must not undercut
/// them. Six days is effectively "no ceiling" while still bounding a wedged
/// socket, and sits strictly below the spawned agent's MCP tool timeout
/// (`MCP_TOOL_TIMEOUT` / Codex `tool_timeout_sec`, set to 7 days) so the agent
/// never abandons cairn-cli mid-await.
const UNBOUNDED_CALLBACK_TIMEOUT: Duration = Duration::from_secs(6 * 24 * 60 * 60);

use cairn_common::protocol::{CallbackRequest, CallbackResponse};

/// Effective host timeout for one `run` item, in milliseconds (mirrors
/// `bash.rs`: caller value or the 120s default, clamped to the 600s ceiling).
fn effective_run_item_timeout_ms(item: &RunItemInput) -> u64 {
    item.timeout
        .map(u64::from)
        .unwrap_or(HOST_ITEM_TIMEOUT_DEFAULT_MS)
        .min(HOST_ITEM_TIMEOUT_MAX_MS)
}

/// HTTP callback timeout for a `run` batch. The host runs items sequentially
/// (sum of per-item budgets) or in parallel (max of per-item budgets), plus a
/// margin so the socket outlives the host's own per-item timeout. An empty
/// batch falls back to the single-item default.
fn run_callback_timeout(input: &RunInput) -> Duration {
    let sequential = input.sequential.unwrap_or(false);
    let budget_ms: u64 = if sequential {
        input
            .commands
            .iter()
            .map(effective_run_item_timeout_ms)
            .sum()
    } else {
        input
            .commands
            .iter()
            .map(effective_run_item_timeout_ms)
            .max()
            .unwrap_or(HOST_ITEM_TIMEOUT_DEFAULT_MS)
    };
    Duration::from_millis(budget_ms) + CALLBACK_TIMEOUT_MARGIN
}

/// True if a `write` payload contains a blocking append to a node's
/// tasks/questions collection — an await with no host-side timeout below it
/// (the host blocks until the sub-agent task completes or the user answers).
fn change_has_blocking_append(payload: &serde_json::Value) -> bool {
    let Some(changes) = payload.get("changes").and_then(|c| c.as_array()) else {
        return false;
    };
    changes.iter().any(|item| {
        let is_append = item.get("mode").and_then(|m| m.as_str()) == Some("append");
        is_append
            && item
                .get("target")
                .and_then(|t| t.as_str())
                .and_then(parse_cairn_uri)
                .is_some_and(|r| {
                    matches!(
                        r,
                        CairnResource::NodeTasks { .. } | CairnResource::NodeQuestions { .. }
                    )
                })
    })
}

/// HTTP callback timeout for a request. The host owns execution; this ceiling
/// must sit strictly above whatever the host can legally take so the HTTP layer
/// never undercuts the host's own timeout. `run` derives its budget from the
/// batch; blocking `write` appends await an unbounded external event; everything
/// else uses the short default.
fn callback_timeout(request: &CallbackRequest) -> Duration {
    match request.tool.as_str() {
        "run" => serde_json::from_value::<RunInput>(request.payload.clone())
            .map(|input| run_callback_timeout(&input))
            .unwrap_or(DEFAULT_CALLBACK_TIMEOUT),
        "write" if change_has_blocking_append(&request.payload) => UNBOUNDED_CALLBACK_TIMEOUT,
        _ => DEFAULT_CALLBACK_TIMEOUT,
    }
}

#[derive(Debug, Clone, Default)]
struct CallbackOutcome {
    result: String,
    artifact_uri: Option<String>,
    /// `<system-reminder>` bodies from the callback response, in delivery order.
    /// Assembled into the model-visible text at the transport edge by
    /// [`assemble_reminders`]; never spliced into `result`, which stays
    /// parseable as data (e.g. the read-batch envelope, a change report).
    reminders: Vec<String>,
    transport_ok: bool,
}

/// Cairn MCP Server - tools for Claude to interact with Cairn during planning
#[derive(Clone)]
struct CairnMcp {
    callback_url: Arc<String>,
    /// Current working directory - used by backend to identify the active run
    cwd: Arc<String>,
    /// Run ID - preferred method to identify the active run (avoids cwd ambiguity)
    run_id: Option<Arc<String>>,
    /// Shared secret (base64-encoded string from env var, sent directly as bearer token)
    mcp_secret: Option<Arc<String>>,
    /// Stable shorthand root for `cairn:~/...` resolution.
    home_uri: Option<Arc<String>>,
    /// Last successful resource read URI (navigation context).
    base_uri: Arc<Mutex<Option<String>>>,
    tool_router: ToolRouter<Self>,
    /// Available agents for task tool description
    available_agents: Vec<AgentInfo>,
}

/// One change item as received from the MCP client.
///
/// Every field is optional and `skip_serializing_if` so serde never hard-rejects
/// a malformed item before `write()` runs: control always reaches our own
/// validator, which owns the friendly error text. The advertised contract (the
/// manual `JsonSchema` on `ChangeInput`) still marks `target`/`mode` required —
/// the schema guides the model; the lenient struct is the runtime gate.
///
/// Scope: `#[serde(default)]` only supplies the default when a field is *absent*
/// or null, so this disambiguates the absent/null `changes` case (the reported
/// `-32602` symptom). A present-but-wrong-typed `changes` (e.g. a string, or an
/// item that isn't an object) still fails rmcp deserialization before `write()`;
/// cairn-core's `handle_change` runs the same validator on the raw `Value` and
/// catches those shapes authoritatively.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChangeItemInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload: Option<serde_json::Value>,
}

/// Input for canonical change tool.
/// Manual JsonSchema impl to produce a flat, inline schema without $ref.
///
/// Fields are lenient (see `ChangeItemInput`). A genuinely-absent `changes`
/// stays absent on re-serialization (`skip_serializing_if`), so the validator
/// can emit a precise "required and not present" message instead of the opaque
/// rmcp `-32602 missing field 'changes'`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChangeInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    changes: Option<Vec<ChangeItemInput>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    commit_msg: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    preview: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    atomic: Option<bool>,
}

impl schemars::JsonSchema for ChangeInput {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ChangeInput".into()
    }

    fn json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
        serde_json::from_value::<schemars::Schema>(serde_json::json!({
            "type": "object",
            "required": ["changes"],
            "properties": {
                "changes": {
                    "description": "Ordered mutations to apply. By default, matching items apply and failures are reported per item; set atomic:true to stop at the first apply failure.",
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["target", "mode"],
                        "properties": {
                            "target": {
                                "type": "string",
                                "description": "File URI like file:src/lib.rs (worktree-relative) or file:/abs/path, or a canonical cairn://p/... resource URI"
                            },
                            "mode": {
                                "type": "string",
                                "enum": ["create", "append", "patch", "unified_patch", "replace", "delete", "rename", "apply"],
                                "description": "Mutation mode. Use unified_patch for native *** Begin Patch envelopes on file: targets, rename for an LSP-backed semantic identifier rename on a file: target, and apply only with a single transcript event URI from a pending preview. Unsupported target/mode pairs fail explicitly."
                            },
                            "payload": {
                                "type": "object",
                                "description": "Structured payload carrying this item's keys for file and resource targets alike. File targets: create/replace/append take {content}; patch takes {diff} OR {old_string, new_string} (optional {replace_all}); unified_patch takes {patch} containing a native *** Begin Patch envelope; delete needs no payload; rename takes {new_name, and exactly one of old_name | symbol_at} and resolves every edit site through the language server. The ~~*~~ wildcard marker applies inside old_string. Resource targets carry keys like {title} or {content}; read the target URI for its exact payload keys.",
                                "additionalProperties": true
                            }
                        }
                    }
                },
                "commit_msg": {
                    "type": "string",
                    "description": "Git commit message. REQUIRED when the batch contains any file-target change (the edits are committed so they survive worktree cleanup); omit it for resource-only batches. Use '^' to amend the previous commit. 'NO_COMMIT' is valid only while resolving an in-progress merge or rebase; anywhere else the worktree is restored to HEAD."
                },
                "preview": {
                    "type": "boolean",
                    "description": "When true, validate and compute the change report without applying side effects. Apply later with a single change item using mode=apply and the preview event URI."
                },
                "atomic": {
                    "type": "boolean",
                    "description": "Apply-phase atomicity opt-in. Default false applies every item whose anchor matches, reports per-item failures, and commits only files that applied. true preserves fail-fast behavior."
                }
            }
        }))
        .unwrap()
    }
}

/// Input for the always-array read tool.
///
/// `paths` is a non-empty list of self-contained target URIs. All per-target
/// scoping (`offset`, `limit`, `glob`, `grep`, `issue_history`) rides in each
/// URI's query string (e.g. `file:x.rs?offset=10&limit=20`,
/// `cairn://p/CAIRN/123/changed?grep=foo`). There is no top-level offset/limit:
/// they are meaningless across N targets, and query-string scoping is the one
/// canonical per-target mechanism.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct ReadFileInput {
    /// One or more targets to read, applied in order. Each is a canonical file
    /// URI (`file:...`, bare `file:` for the worktree root), a Cairn resource URI (`cairn://...`), or a
    /// web/PDF URL. Append `?key=value&...` to a URI for per-target scoping.
    paths: Vec<String>,
}

/// Validate a run batch: non-empty, and each item has exactly one of
/// `command` / `target`. Returns the first problem as a user-facing message.
fn validate_run_input(input: &RunInput) -> Result<(), String> {
    if input.commands.is_empty() {
        return Err("`commands` must contain at least one item".to_string());
    }
    for (i, item) in input.commands.iter().enumerate() {
        match (item.command.as_deref(), item.target.as_deref()) {
            (Some(_), Some(_)) => {
                return Err(format!(
                    "commands[{i}] has both `command` and `target`; provide exactly one"
                ));
            }
            (None, None) => {
                return Err(format!(
                    "commands[{i}] has neither `command` nor `target`; provide exactly one"
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Input for run tool: an ordered batch of invocations.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct RunInput {
    /// Ordered list of invocations. Each item is either a shell `command` or a
    /// `target` skill-script URI. Must contain at least one item.
    commands: Vec<RunItemInput>,
    /// Run items in input order instead of concurrently (default: false = parallel).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sequential: Option<bool>,
    /// In sequential mode, abort remaining items after a failure (default: true).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stop_on_error: Option<bool>,
    /// Commit message for successful worktree-bound batches that dirty the tree.
    /// Stages all changes and commits once after success. Use "^" to amend the
    /// previous commit; "NO_COMMIT" is valid only while resolving an in-progress
    /// merge or rebase; anywhere else the worktree is restored to HEAD.
    #[serde(
        rename = "commit_msg",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    commit_msg: Option<String>,
}

/// A single run item: exactly one of `command` (shell) or `target` (skill script).
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct RunItemInput {
    /// Shell command to execute. Mutually exclusive with `target`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    command: Option<String>,
    /// Short description of what this command does (5-10 words).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    /// Timeout in milliseconds (default: 120000, max: 600000).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    timeout: Option<u32>,
    /// A `cairn://skills/<id>/scripts/<name>` target. Mutually exclusive with `command`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target: Option<String>,
    /// Structured args for a `target` skill script.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload: Option<RunItemPayloadInput>,
}

/// Structured args for a `target`: positional `args` for a skill script, or a
/// named-argument `args_json` object for an MCP tool call.
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
struct RunItemPayloadInput {
    /// Positional arguments appended to the script's argv (skill-script targets).
    #[serde(default)]
    args: Vec<String>,
    /// Named-argument object for an MCP tool call
    /// (`cairn://mcp/<server>/<tool>`), forwarded to the server's `tools/call`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    args_json: Option<serde_json::Value>,
}

const CAIRN_URI_PREFIX: &str = "cairn://";
const CAIRN_HOME_PREFIX: &str = "cairn:~/";
const CAIRN_RESOURCE_ROOT_SEGMENTS: usize = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResolvedTarget {
    CairnUri(String),
    FileUri(String),
}

fn invalid_target_message(target: &str) -> String {
    format!(
        "Invalid target: expected cairn://... or file:...; use file:relative/path (worktree-relative), file:/absolute/path, or bare file: for the worktree root instead of '{target}'"
    )
}

fn unsupported_positional_shorthand_message(target: &str) -> String {
    format!(
        "Unsupported shorthand: positional Cairn URIs (`cairn:./...`, `cairn:../...`) are not supported; use canonical `cairn://p/...` or home-anchored `cairn:~/...` instead ({target})"
    )
}

#[derive(Debug, Deserialize)]
struct ChangeReportSummary {
    #[serde(default)]
    failures: Vec<ChangeFailureSummary>,
    #[serde(default)]
    applied: Vec<serde_json::Value>,
    #[serde(default)]
    commit: Option<ChangeCommitSummary>,
}

#[derive(Debug, Deserialize)]
struct ChangeFailureSummary {
    index: usize,
    target: String,
    mode: String,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ChangeCommitSummary {
    status: String,
}

/// Max chars in a tool result (~20k tokens, safely under Claude Code's 25k token limit)
const MAX_RESULT_CHARS: usize = 45_000;

/// If text exceeds MAX_RESULT_CHARS, truncate at a line boundary and append continuation info.
/// Redact common secret patterns from a command string for safe logging.
fn redact_command(command: &str) -> String {
    use std::sync::LazyLock;

    static PATTERNS: LazyLock<Vec<regex::Regex>> = LazyLock::new(|| {
        vec![
            regex::Regex::new(r#"(?i)(Bearer\s+)[^\s'"]+"#).unwrap(),
            regex::Regex::new(r"\bsk[-_][a-zA-Z0-9._-]{8,}\b").unwrap(),
            regex::Regex::new(r"(?i)(export\s+[A-Z_]*(?:KEY|SECRET|TOKEN|PASSWORD)\s*=)\S+")
                .unwrap(),
            regex::Regex::new(r"(?i)(--password[= ])\S+").unwrap(),
        ]
    });

    let mut result = command.to_string();
    for pattern in PATTERNS.iter() {
        result = pattern
            .replace_all(&result, |caps: &regex::Captures| {
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

fn cap_text_result(text: &str, offset: usize) -> String {
    if text.len() <= MAX_RESULT_CHARS {
        return text.to_string();
    }

    // Find a safe byte position on a char boundary (avoids panic on multi-byte UTF-8)
    let mut safe_end = MAX_RESULT_CHARS;
    while safe_end > 0 && !text.is_char_boundary(safe_end) {
        safe_end -= 1;
    }

    // Find last newline before the safe limit
    let truncation_point = text[..safe_end].rfind('\n').unwrap_or(safe_end);

    let truncated = &text[..truncation_point];
    let lines_shown = truncated.lines().count();
    let total_lines = text.lines().count();
    let next_offset = offset + lines_shown;

    format!(
        "{}\n\n--- truncated (lines {}-{} of {}, {} of {} chars) ---\nCall again with offset={} to continue.",
        truncated,
        offset + 1,
        offset + lines_shown,
        total_lines,
        truncation_point,
        text.len(),
        next_offset
    )
}

/// Final guard for `run` results. Core's `compose_run_output` already caps batch
/// output to its `MAX_RUN_RESULT_CHARS` (deliberately equal to `MAX_RESULT_CHARS`)
/// with tail-biased per-item capping, so a well-formed core result is at or under
/// the cap and passes through untouched. This is a defensive backstop only: an
/// over-cap result is tail-biased here too — keeping the end (error summaries, the
/// last `&&` segment) rather than the head — and never carries the `offset=`
/// continuation footer that `read` uses. A finished command's output is not
/// re-readable, so the actionable advice is to re-run scoped or use a terminal.
fn cap_run_result(text: &str) -> String {
    if text.len() <= MAX_RESULT_CHARS {
        return text.to_string();
    }
    const ADVICE: &str =
        "--- run output exceeded the char cap; showing the tail. Re-run scoped (grep/tail the command) or use a terminal resource for full output. ---\n";
    // Keep the last bytes, leaving room for the advice prefix so the whole result
    // stays within the cap. Snap up to a char boundary to avoid splitting UTF-8.
    let tail_budget = MAX_RESULT_CHARS.saturating_sub(ADVICE.len());
    let mut start = text.len().saturating_sub(tail_budget);
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }
    format!("{ADVICE}{}", &text[start..])
}

/// Append each system-reminder body to `content`, wrapped in the
/// `<system-reminder>` envelope. This is the single assembly point for
/// augmentation reminders across every verb: the backend ships them as data on
/// the callback response (`CallbackResponse::reminders`) and the CLI renders
/// them into the model-visible text here, reproducing the legacy wrapping and
/// ordering byte-for-byte. Handler content therefore stays parseable as data
/// end-to-end — no JSON payload is ever followed by trailing reminder text.
fn assemble_reminders(mut content: String, reminders: &[String]) -> String {
    for body in reminders {
        content.push_str("\n\n<system-reminder>\n");
        content.push_str(body);
        content.push_str("\n</system-reminder>");
    }
    content
}

fn sanitize_callback_url(callback_url: &str) -> String {
    match url::Url::parse(callback_url) {
        Ok(mut parsed) => {
            let _ = parsed.set_username("");
            let _ = parsed.set_password(None);
            parsed.set_query(None);
            parsed.set_fragment(None);
            parsed.to_string()
        }
        Err(_) => callback_url.to_string(),
    }
}

fn callback_failure_class(result: &str) -> &'static str {
    if result.starts_with("Error calling Tauri:") {
        "request"
    } else if result.starts_with("Error parsing response:") {
        "response_parse"
    } else if result.starts_with("Error reading response body:") {
        "response_body"
    } else if result.starts_with("Error building HTTP client:") {
        "client_build"
    } else {
        "http_status"
    }
}

fn callback_transport_error_result(
    outcome: &CallbackOutcome,
    callback_url: &str,
    tool: &str,
) -> CallToolResult {
    let message = format!(
        "MCP callback transport failed\nurl: {}\ntool: {}\nclass: {}\nerror: {}",
        sanitize_callback_url(callback_url),
        tool,
        callback_failure_class(&outcome.result),
        outcome.result
    );
    CallToolResult::error(vec![Content::text(cap_text_result(&message, 0))])
}

fn change_report_failure_reason(raw: &str) -> Option<String> {
    let report: ChangeReportSummary = serde_json::from_str(raw)
        .or_else(|_| {
            let first_chunk = raw.trim_start().split_once("\n\n").map(|(head, _)| head);
            first_chunk
                .ok_or_else(|| serde_json::Error::io(std::io::ErrorKind::InvalidData.into()))
                .and_then(serde_json::from_str)
        })
        .ok()?;
    if !report.failures.is_empty() {
        let total = report.applied.len() + report.failures.len();
        let failures = report
            .failures
            .iter()
            .map(|failure| {
                format!(
                    "[{}] {} {}: {}",
                    failure.index, failure.target, failure.mode, failure.error
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        return Some(format!(
            "{} of {} changes failed: {}",
            report.failures.len(),
            total,
            failures
        ));
    }

    let commit_status = report.commit?.status;
    if commit_status.eq_ignore_ascii_case("failed") {
        return Some("change commit failed".to_string());
    }

    None
}

/// Truncate to at most `max` characters on a char boundary. Response/error
/// bodies are arbitrary bytes; slicing by byte offset (`&s[..n]`) panics when a
/// multi-byte char straddles the cut, so clamp by chars instead.
fn truncate_chars(text: &str, max: usize) -> String {
    text.chars().take(max).collect()
}

/// Build the user-facing message for a non-2xx MCP callback response. HTTP 413
/// gets actionable guidance about the body-size limit; other statuses report
/// the status and the request size so the failure is diagnosable.
fn http_status_error_message(status: u16, reason: &str, request_bytes: usize) -> String {
    let mut message = format!(
        "MCP callback returned HTTP {status} {reason}. Request body was {request_bytes} bytes."
    );
    if status == 413 {
        message.push_str(
            " The payload exceeds the callback body-size limit; split the change into smaller \
             writes (fewer files per call, or a smaller content/patch).",
        );
    }
    message
}

fn change_callback_result(
    outcome: CallbackOutcome,
    callback_url: &str,
    tool: &str,
) -> CallToolResult {
    if !outcome.transport_ok {
        return callback_transport_error_result(&outcome, callback_url, tool);
    }

    // Parse the change report from the clean handler content first, then
    // assemble augmentation reminders into the model-visible text.
    let failure = change_report_failure_reason(&outcome.result);
    let body = assemble_reminders(outcome.result, &outcome.reminders);
    match failure {
        Some(reason) => {
            let text = format!("{}\n\n{}", reason, body);
            CallToolResult::error(vec![Content::text(cap_text_result(&text, 0))])
        }
        None => CallToolResult::success(vec![Content::text(cap_text_result(&body, 0))]),
    }
}

fn find_cairn_uri_end(text: &str) -> usize {
    text.find(|c: char| {
        c.is_whitespace()
            || matches!(
                c,
                '<' | '>' | '"' | '\'' | '`' | '(' | ')' | '[' | ']' | '{' | '}'
            )
    })
    .unwrap_or(text.len())
}

fn split_uri_suffix(candidate: &str) -> (&str, &str) {
    let mut split_at = candidate.len();
    while split_at > 0 {
        let ch = candidate[..split_at].chars().next_back().unwrap();
        if matches!(ch, '.' | ',' | ':' | ';' | '!' | '?') {
            split_at -= ch.len_utf8();
        } else {
            break;
        }
    }
    (&candidate[..split_at], &candidate[split_at..])
}

#[tool_router]
impl CairnMcp {
    fn new(
        callback_url: String,
        cwd: String,
        run_id: Option<String>,
        mcp_secret: Option<String>,
        available_agents: Vec<AgentInfo>,
    ) -> Self {
        Self::new_with_home_uri(
            callback_url,
            cwd,
            run_id,
            mcp_secret,
            available_agents,
            None,
        )
    }

    fn new_with_home_uri(
        callback_url: String,
        cwd: String,
        run_id: Option<String>,
        mcp_secret: Option<String>,
        available_agents: Vec<AgentInfo>,
        home_uri: Option<String>,
    ) -> Self {
        let home_uri = home_uri.map(Arc::new);
        let base_uri = Arc::new(Mutex::new(home_uri.as_ref().map(|uri| uri.to_string())));

        Self {
            callback_url: Arc::new(callback_url),
            cwd: Arc::new(cwd),
            run_id: run_id.map(Arc::new),
            mcp_secret: mcp_secret.map(|s| Arc::new(s)),
            home_uri,
            base_uri,
            tool_router: Self::tool_router(),
            available_agents,
        }
    }

    fn current_base_uri(&self) -> Option<String> {
        self.base_uri.lock().unwrap().clone()
    }

    fn home_uri_string(&self) -> Option<String> {
        self.home_uri.as_ref().map(|uri| uri.to_string())
    }

    fn set_base_uri(&self, uri: String) {
        *self.base_uri.lock().unwrap() = Some(uri);
    }

    fn note_successful_resource_read(&self, uri: &str) {
        self.set_base_uri(uri.to_string());
    }

    fn relativize_cairn_uri_for_display(&self, uri: &str) -> String {
        let target_segments = match Self::canonical_uri_segments(uri) {
            Ok(segments) => segments,
            Err(_) => return uri.to_string(),
        };

        let mut best = uri.to_string();

        if let Some(home_uri) = self.home_uri_string() {
            if let Ok(home_segments) = Self::canonical_uri_segments(&home_uri) {
                if let Some(relative) =
                    Self::relative_display_from_home(&home_segments, &target_segments)
                {
                    if relative.len() < best.len() {
                        best = relative;
                    }
                }
            }
        }

        best
    }

    fn relativize_cairn_uris_in_text(&self, text: &str) -> String {
        let mut rendered = String::with_capacity(text.len());
        let mut remaining = text;

        while let Some(index) = remaining.find(CAIRN_URI_PREFIX) {
            rendered.push_str(&remaining[..index]);
            let candidate = &remaining[index..];
            let raw_end = find_cairn_uri_end(candidate);
            let raw_uri = &candidate[..raw_end];
            let (uri, suffix) = split_uri_suffix(raw_uri);

            if parse_cairn_uri(uri).is_some() {
                rendered.push_str(&self.relativize_cairn_uri_for_display(uri));
                rendered.push_str(suffix);
            } else {
                rendered.push_str(raw_uri);
            }

            remaining = &candidate[raw_end..];
        }

        rendered.push_str(remaining);
        rendered
    }

    fn resolve_target(&self, target: &str) -> Result<ResolvedTarget, String> {
        if target.starts_with(CAIRN_URI_PREFIX) {
            if parse_cairn_uri(target).is_none() {
                return Err(format!("Invalid cairn resource URI: {}", target));
            }
            return Ok(ResolvedTarget::CairnUri(target.to_string()));
        }

        if let Some(suffix) = target.strip_prefix(CAIRN_HOME_PREFIX) {
            let home_uri = self
                .home_uri
                .as_ref()
                .map(|uri| uri.as_ref().clone())
                .ok_or_else(|| {
                    format!(
                        "Cannot resolve home-relative Cairn URI without home_uri: {}",
                        target
                    )
                })?;
            let resolved = Self::resolve_uri_reference(&home_uri, suffix)?;
            return Ok(ResolvedTarget::CairnUri(resolved));
        }

        if let Some(reference) = target.strip_prefix("cairn:") {
            if reference.starts_with("./") || reference.starts_with("../") {
                return Err(unsupported_positional_shorthand_message(target));
            }
        }

        // Forward the `file:` identity to core unchanged; core owns resolution
        // semantics (relative vs absolute, jailing, the file:~ hard cut).
        if target.starts_with("file:") {
            return Ok(ResolvedTarget::FileUri(target.to_string()));
        }

        Err(invalid_target_message(target))
    }

    fn rewrite_change_targets(&self, input: &ChangeInput) -> Result<ChangeInput, String> {
        let mut rewritten = input.clone();
        // Targets are guaranteed present by `validate_change_value`, which runs
        // before this; the Option handling keeps the lenient types honest.
        if let Some(changes) = rewritten.changes.as_mut() {
            for change in changes.iter_mut() {
                if let Some(target) = change.target.as_ref() {
                    let resolved = match self.resolve_target(target)? {
                        ResolvedTarget::CairnUri(uri) | ResolvedTarget::FileUri(uri) => uri,
                    };
                    change.target = Some(resolved);
                }
            }
        }
        Ok(rewritten)
    }

    fn resolve_read_target(&self, target: &str) -> Result<String, String> {
        let split = split_target_query(target)?;
        let resolved = match self.resolve_target(&split.identity)? {
            ResolvedTarget::CairnUri(uri) | ResolvedTarget::FileUri(uri) => uri,
        };

        Ok(match split.raw_query {
            Some(query) if !query.is_empty() => format!("{resolved}?{query}"),
            _ => resolved,
        })
    }

    fn should_update_base_uri_after_read(
        &self,
        tool_name: &str,
        response: &CallbackOutcome,
    ) -> bool {
        if !response.transport_ok || Self::resource_result_looks_like_error(&response.result) {
            return false;
        }

        if tool_name == "read_resource" {
            return serde_json::from_str::<TerminalReadResult>(&response.result).is_ok();
        }

        true
    }

    fn resource_result_looks_like_error(result: &str) -> bool {
        [
            "Error ",
            "Invalid ",
            "Database error:",
            "Issue ",
            "Project '",
            "Run ",
            "No jobs found for issue ",
            "Missing Authorization header",
            "Invalid token:",
            "Failed to parse request:",
        ]
        .iter()
        .any(|prefix| result.starts_with(prefix))
    }

    fn resolve_uri_reference(base_uri: &str, reference: &str) -> Result<String, String> {
        let mut segments = Self::canonical_uri_segments(base_uri)?;

        for segment in reference.split('/') {
            match segment {
                "" | "." => continue,
                ".." => {
                    if segments.len() <= CAIRN_RESOURCE_ROOT_SEGMENTS {
                        return Err(format!(
                            "Cairn URI traversal cannot escape above cairn://p/<project>: {} from {}",
                            reference, base_uri
                        ));
                    }
                    segments.pop();
                }
                _ => segments.push(segment.to_string()),
            }
        }

        let resolved = format!("{}{}", CAIRN_URI_PREFIX, segments.join("/"));
        if parse_cairn_uri(&resolved).is_none() {
            return Err(format!("Invalid Cairn shorthand target: {}", resolved));
        }

        Ok(resolved)
    }

    fn relative_display_from_home(home: &[String], target: &[String]) -> Option<String> {
        if target.len() < home.len() || home != &target[..home.len()] {
            return None;
        }

        let remainder = &target[home.len()..];
        let suffix = if remainder.is_empty() {
            "~/".to_string()
        } else {
            format!("~/{}", remainder.join("/"))
        };

        Some(format!("cairn:{}", suffix))
    }

    fn canonical_uri_segments(uri: &str) -> Result<Vec<String>, String> {
        if parse_cairn_uri(uri).is_none() {
            return Err(format!("Invalid cairn resource URI: {}", uri));
        }

        let identity = split_target_query(uri)?.identity;
        let stripped = identity
            .strip_prefix(CAIRN_URI_PREFIX)
            .ok_or_else(|| format!("Unknown resource scheme (expected cairn://): {}", uri))?;
        let segments = stripped
            .split('/')
            .filter(|segment| !segment.is_empty())
            .map(|segment| segment.to_string())
            .collect::<Vec<_>>();

        if segments.len() < CAIRN_RESOURCE_ROOT_SEGMENTS
            || segments.first().map(String::as_str) != Some("p")
        {
            return Err(format!("Invalid cairn resource URI: {}", uri));
        }

        Ok(segments)
    }

    /// Apply ordered file and resource mutations through the canonical change carrier.
    #[tool(
        description = r#"Apply ordered file and resource mutations through one carrier. Items in `changes` apply in input order.

Targets:
- File: `file:path/to/file` (worktree-relative; bare `file:` is the worktree root, `file:/abs` is absolute). Every item carries its keys under `payload`: create/replace/append take `payload:{content}`; patch takes `payload:{diff}` OR `payload:{old_string, new_string}` (optional `replace_all`); unified_patch takes `payload:{patch}` containing a native `*** Begin Patch` envelope with add/update/delete sections; delete needs no payload; `rename` takes `payload:{new_name, and exactly one of old_name | symbol_at}` and performs an LSP-backed semantic rename of an identifier across the worktree, applying every edit site (and any module file move) as one commit. A bare `rename` returns a preview by default; land it with the `apply` round-trip. For a structural gap in a patch, put `~~*~~` in `old_string` between a head and tail anchor to replace everything in between — span by default, balanced only with the contiguous delimiter-pair token (`{~~*~~}`, delimiters immediately adjacent to the marker; the own-line `{\n~~*~~\n}` form spans). Multiple `~~*~~` markers span non-contiguous regions; escape a literal marker with a leading backslash. Example: `payload:{old_string: "fn validate(t) {~~*~~}", new_string: "fn validate(t) { verify(t); }"}`.
- Resource: canonical `cairn://p/PROJECT/...` or home-relative `cairn:~/...`. Modes: create, append, patch, replace, delete.

Don't guess a resource's payload: `read` the target URI first — its affordance block lists the exact actions (mode + required/optional payload keys + a copy-paste example) and read filters. If a mutation is unsupported or missing a required key, the rejection enumerates what the resource accepts.

Notes: `atomic` defaults to false: matching items apply, failed items are reported in `failures`, and `commit_msg` commits only files that applied; set `atomic:true` for fail-fast apply behavior. `cairn:~/...` resolves against your running node; `preview:true` returns an `apply_uri` to re-submit with `mode=apply`; `commit_msg` is REQUIRED whenever the batch touches a file target (`\"^\"` amends the previous commit; `\"NO_COMMIT\"` is valid only while resolving an in-progress merge or rebase, anywhere else the worktree is restored to HEAD) — uncommitted worktree edits are lost if the worktree is cleaned up; task/question appends to `cairn:~/tasks` and `cairn:~/questions` block until results return. Change reports list per-item `applied` and `failures`."#
    )]
    async fn write(
        &self,
        params: Parameters<ChangeInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;

        // Validate the raw input ourselves, in one pass, before any rewrite or
        // forward. This owns the error text the model sees (the rmcp-facing
        // struct is lenient precisely so control reaches here) and returns every
        // problem at once with no server round-trip.
        let raw = serde_json::to_value(&input).unwrap_or(serde_json::Value::Null);
        let payload_bytes = serde_json::to_vec(&input).map(|v| v.len()).unwrap_or(0);
        let change_count = input.changes.as_ref().map(|c| c.len()).unwrap_or(0);
        let changes_present = input.changes.is_some();
        tracing::info!(
            "write called: {} changes, changes_present={}, payload {} bytes",
            change_count,
            changes_present,
            payload_bytes
        );

        let validation_errors = cairn_common::change_validation::validate_change_value(&raw);
        if !validation_errors.is_empty() {
            let text =
                cairn_common::change_validation::render_validation_errors(&validation_errors);
            return Ok(CallToolResult::success(vec![Content::text(text)]));
        }

        let rewritten = match self.rewrite_change_targets(&input) {
            Ok(rewritten) => rewritten,
            Err(message) => return Ok(CallToolResult::success(vec![Content::text(message)])),
        };

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "write".to_string(),
            payload: serde_json::to_value(&rewritten).unwrap_or_default(),
            tool_use_id: None,
        };

        let outcome = self.call_tauri_full(&request).await;
        Ok(change_callback_result(
            outcome,
            self.callback_url.as_str(),
            "write",
        ))
    }

    /// Read one or more files, directories, or Cairn resources in a single call.
    #[tool(
        description = r#"Read one or more files, directories, or Cairn resources in a single call. `paths` is an ordered, non-empty array of target URIs; results return in order, each under a `=== <uri> [suffix] ===` header (the optional bracketed suffix is a terse count drawn from a small closed vocabulary: `lines A–B of T`, `N matches[ in M files]`, `M files`, or `truncated`).

Targets (mix freely within one call):
- File: `file:` (worktree root), `file:src/lib.rs` (worktree-relative), `file:/abs/path` (absolute / global)
- Resource: canonical `cairn://p/PROJECT[/NUMBER[/EXEC/NODE[/sub]]]` plus collections `/issues`, `/messages`, `/changed`. `cairn:~/...` resolves against the run home.
- Web/PDF: `http(s)://...` URLs and local `.pdf` paths return markdown via the active web-fetch provider (raw capped markdown; no extraction prompt). The default built-in provider is a plain HTTP fetch (HTML→markdown); PDF extraction needs a configured provider such as `bmd`. Providers are configured in Settings → Web Services.

Per-target scoping rides in each URI's query string — append `?key=value&...`:
- Files: `offset=N` skips N leading lines (0-based — line N is at `offset N−1`); `limit=N` returns N lines; `offset=-N` returns the last N lines (tail). `glob=PATTERN` selects matched files (`output_mode=files_with_matches|content|count`; a directory grep defaults to `files_with_matches`, a single-file grep to `content`). `issue_history=true|verbose` appends issues that touched the file.
- Grep is universal: `grep=REGEX` matches over ANY target's rendered text. A file tree greps with ripgrep and labels each line `path:N:text`; a single file or any rendered resource/web body greps in memory and drops the path prefix (`N:text`). Modifiers: `-i` (case-insensitive), `-A=N`/`-B=N`/`-C=N`/`context=N` (context lines), `head_limit=N` to cap matches (`limit=N` aliases it under grep). `offset` is NOT allowed with grep — paginate matches with `head_limit`. The header suffix counts real matches: `[N matches]` for one body, `[N matches in M files]` for a tree.
- Escaping: `&` and `+` are literal inside a value (so `grep=&mut self` and `grep=\d+` work as written); use `%26` for a literal `&` that immediately precedes a recognized key token
- Resources: `offset=N` skips rendered resource lines client-side; `limit=N` is resource-specific unless reading a single transcript event, where it is a client-side line count
- `/issues`: `status=backlog,active,...` (comma-separated), `limit=N`, `sort=updated_desc|created_asc|...`, `ready=true|false`
- `/messages`: `before=`, `after=`, `since=EPOCH`, `limit=N`
- Project search: `cairn://p/PROJECT?search=QUERY&limit=N&since=EPOCH`

Partial failures never abort: a target that errors shows its message inline as that target's block, and every requested target still contributes a block. A multi-target read shares a single ~45k-char total budget across targets (water-filled so small targets render whole and large ones fair-share); every requested target is included, and a windowed or truncated segment carries an always-valid `continue:` footer — `[lines A–B of T — output truncated to fit budget; continue: ...]`, advancing `offset`/`head_limit`, or a `char_offset=` resume when a single line is itself larger than the budget."#
    )]
    async fn read(
        &self,
        params: Parameters<ReadFileInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;
        if input.paths.is_empty() {
            return Ok(CallToolResult::error(vec![Content::text(
                "read requires a non-empty `paths` array (one or more target URIs).".to_string(),
            )]));
        }
        tracing::info!("read called: {} paths", input.paths.len());

        // Resolve each target client-side (home-URI + base-URI shorthand). Web
        // URLs pass through unresolved — the backend classifies and fetches them.
        // A target that fails resolution is forwarded as-is so the backend emits
        // it as that target's inline error block (partial failure never aborts).
        let resolved: Vec<String> = input
            .paths
            .iter()
            .map(|path| {
                if path.starts_with("http://") || path.starts_with("https://") {
                    path.clone()
                } else {
                    self.resolve_read_target(path)
                        .unwrap_or_else(|_| path.clone())
                }
            })
            .collect();

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "read_batch".to_string(),
            payload: serde_json::json!({ "paths": resolved }),
            tool_use_id: None,
        };

        let outcome = self.call_tauri_full(&request).await;

        // The handler content is the bare envelope JSON — augmentation reminders
        // ride separately, so this parses cleanly with no trailing-text split.
        let envelope = match serde_json::from_str::<ReadBatchEnvelope>(&outcome.result) {
            Ok(envelope) => envelope,
            Err(_) => {
                // Transport/parse failure: surface the raw result as text.
                return Ok(CallToolResult::success(vec![Content::text(
                    self.relativize_cairn_uris_in_text(&outcome.result),
                )]));
            }
        };
        let text = self.relativize_cairn_uris_in_text(&envelope.text);
        let text = assemble_reminders(text, &outcome.reminders);

        let mut blocks: Vec<Content> = Vec::with_capacity(1 + envelope.images.len());
        blocks.push(Content::text(text));
        for image in envelope.images {
            blocks.push(Content::image(image.data, image.mime_type));
        }
        Ok(CallToolResult::success(blocks))
    }

    /// Execute an ordered batch of shell commands and skill-script invocations,
    /// synchronously. Parallel by default; `sequential: true` runs in order.
    /// Long-running terminals are managed by `write` on terminal resources.
    #[tool(
        description = "Execute an ordered batch of synchronous invocations. `commands` is a non-empty array; each item is a shell `command`, a `target` skill-script URI (cairn://skills/<id>/scripts/<name>) with optional `payload.args`, or a `target` external MCP tool (cairn://mcp/<server>/<tool>) with its named arguments in `payload.args_json` (e.g. `{target:\"cairn://mcp/axon/look\", payload:{args_json:{app:\"Finder\"}}}` — read cairn://mcp/<server> for each tool's arg shape). Items run in PARALLEL by default; set `sequential: true` for ordered execution (fail-fast unless `stop_on_error: false`). Output is composed under `=== <command-or-target> ===` headers in input order. If a successful worktree-bound batch dirties the tree, `commit_msg` is required and commits all worktree changes ONCE after the batch succeeds; `^` amends. `NO_COMMIT` is valid only while resolving an in-progress merge or rebase; anywhere else the worktree is restored to HEAD. Not for long-lived/background processes — use a terminal resource via `write` for those. Each item's `timeout` (ms, default 120000, max 600000) is honored by the host: when an item exceeds it, that item is terminated and its result block reports the timeout with whatever output it produced so far — the batch is never aborted with no output. Outer layers (the callback transport and the agent's own tool timeout) are sized strictly above the host budget, so a legal batch always runs to completion: sequential batches get the sum of per-item timeouts, parallel batches the max."
    )]
    async fn run(&self, params: Parameters<RunInput>) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;

        if let Err(msg) = validate_run_input(&input) {
            return Ok(CallToolResult::error(vec![Content::text(msg)]));
        }

        let first = input
            .commands
            .first()
            .and_then(|item| item.command.clone().or_else(|| item.target.clone()))
            .unwrap_or_default();
        let redacted = redact_command(&first);
        tracing::info!(
            "run called: {} item(s), first={}",
            input.commands.len(),
            &redacted[..redacted.len().min(100)]
        );

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            run_id: self.run_id.as_ref().map(|r| r.to_string()),
            tool: "run".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
            tool_use_id: None,
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(
            cap_run_result(&result),
        )]))
    }
}

impl CairnMcp {
    /// Like `call_tauri` but returns the full callback outcome (including `artifact_uri`).
    async fn call_tauri_full(&self, request: &CallbackRequest) -> CallbackOutcome {
        let client = match reqwest::Client::builder()
            .timeout(callback_timeout(request))
            .build()
        {
            Ok(client) => client,
            Err(e) => {
                tracing::error!("Failed to build HTTP client: {}", e);
                return CallbackOutcome {
                    result: format!("Error building HTTP client: {}", e),
                    ..Default::default()
                };
            }
        };
        let mut req = client.post(self.callback_url.as_str()).json(request);

        if let Some(secret) = &self.mcp_secret {
            req = req.header("Authorization", format!("Bearer {}", secret));
        }

        let request_bytes = serde_json::to_vec(request).map(|v| v.len()).unwrap_or(0);
        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                let http_ok = status.is_success();
                match resp.text().await {
                    // A non-2xx callback (e.g. HTTP 413 when the body exceeds the
                    // callback limit) is not a `CallbackResponse`; surface the
                    // status and request size explicitly instead of failing to
                    // parse the error body into an opaque message.
                    Ok(text) if !http_ok => {
                        tracing::error!(
                            "MCP callback returned HTTP {} for tool {}: {}",
                            status,
                            request.tool,
                            truncate_chars(&text, 500)
                        );
                        CallbackOutcome {
                            result: http_status_error_message(
                                status.as_u16(),
                                status.canonical_reason().unwrap_or("error"),
                                request_bytes,
                            ),
                            ..Default::default()
                        }
                    }
                    Ok(text) => match serde_json::from_str::<CallbackResponse>(&text) {
                        Ok(r) => CallbackOutcome {
                            result: r.result,
                            artifact_uri: r.artifact_uri,
                            reminders: r.reminders,
                            transport_ok: http_ok,
                        },
                        Err(e) => {
                            tracing::error!(
                                "Failed to parse response (status {}): {} - body: {}",
                                status,
                                e,
                                truncate_chars(&text, 500)
                            );
                            CallbackOutcome {
                                result: format!(
                                    "Error parsing response: {} (body: {})",
                                    e,
                                    truncate_chars(&text, 200)
                                ),
                                ..Default::default()
                            }
                        }
                    },
                    Err(e) => CallbackOutcome {
                        result: format!("Error reading response body: {}", e),
                        ..Default::default()
                    },
                }
            }
            Err(e) => CallbackOutcome {
                result: format!("Error calling Tauri: {}", e),
                ..Default::default()
            },
        }
    }

    /// Convenience wrapper for verbs whose result is opaque text: returns the
    /// handler content with augmentation reminders assembled in. Verbs that
    /// parse the result as structured data (read-batch envelope, change report,
    /// terminal poll) must call `call_tauri_full` and assemble reminders only
    /// after parsing, so the data payload stays clean.
    async fn call_tauri(&self, request: &CallbackRequest) -> String {
        let outcome = self.call_tauri_full(request).await;
        assemble_reminders(outcome.result, &outcome.reminders)
    }
}

impl ServerHandler for CairnMcp {
    fn get_info(&self) -> ServerInfo {
        let mut instructions = "Cairn MCP server for agent orchestration.\n\n\
             Issue tools:\n\
             - read: Read issue and node resources (cairn://p/PROJECT/NUMBER, cairn://p/PROJECT/NUMBER/EXEC/NODE/chat)\n\n\
             Implementation tools:\n\
             - read: Read file contents, directory listings, canonical cairn://p/... resources, and read-query projections such as `src?glob=**/*.rs`, `src?grep=foo&glob=*.ts`, or `cairn://p/CAIRN?search=uri&limit=5`\n\\

             - write: Apply ordered file and cairn:// resource mutations — files, terminals, delegated tasks (append to a node's tasks collection), user questions (append to a node's questions collection), and output artifacts (write/patch your node's artifact via cairn:~/<name>)\n\
             - run: Execute an ordered batch of synchronous shell commands and skill-script targets (parallel by default; `sequential` for ordered). If a successful worktree-bound run dirties the tree, pass `commit_msg` to commit it (`^` amends). `NO_COMMIT` is valid only while resolving an in-progress merge or rebase; anywhere else the worktree is restored to HEAD.\n\n\
             Output artifact: when your node declares an output schema, write your result with `write` to `cairn:~/<name>` (mode create, then mode patch to revise). The payload is validated against the schema server-side. Do this as the last action of your turn; the write pauses the run for user review, and the session resumes on their reply."
            .to_string();

        // Add available agents to instructions
        if !self.available_agents.is_empty() {
            instructions.push_str(
                "\n\nAvailable agents (use as subagentType when appending to a node's tasks collection):\n",
            );
            for agent in &self.available_agents {
                instructions.push_str(&format!("- {}: {}\n", agent.name, agent.description));
            }
        }

        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
            server_info: Implementation {
                name: "cairn-cli".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            instructions: Some(instructions),
        }
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParam>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListToolsResult, rmcp::ErrorData>> + Send + '_ {
        async move {
            // Get static tools from the router
            let mut tools = self.tool_router.list_all();

            // Append available agents to the write tool description so task
            // appends know which subagentType values are valid.
            if !self.available_agents.is_empty() {
                for tool in &mut tools {
                    if tool.name == "write" {
                        let mut desc = tool
                            .description
                            .as_ref()
                            .map(|d| d.to_string())
                            .unwrap_or_default();
                        desc.push_str("\n\nAvailable agents for task appends (subagentType):\n");
                        for agent in &self.available_agents {
                            desc.push_str(&format!("- {}: {}\n", agent.name, agent.description));
                        }
                        tool.description = Some(std::borrow::Cow::Owned(desc));
                        break;
                    }
                }
            }

            Ok(ListToolsResult::with_all_items(tools))
        }
    }

    fn call_tool(
        &self,
        request: CallToolRequestParam,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<CallToolResult, rmcp::ErrorData>> + Send + '_ {
        async move {
            // Delegate to router for static tools
            let tcc = ToolCallContext::new(self, request, context);
            self.tool_router.call(tcc).await
        }
    }

    fn read_resource(
        &self,
        request: ReadResourceRequestParam,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ReadResourceResult, rmcp::ErrorData>> + Send + '_ {
        async move {
            let uri = &request.uri;
            tracing::info!("read_resource called: uri={}", uri);

            // Determine which callback to use based on URI scheme
            let tool_name = if uri.starts_with("cairn://") {
                // Parse cairn:// URI to determine resource type
                match parse_cairn_uri(uri) {
                    Some(resource) => {
                        match resource {
                            // Terminal resources use read_resource
                            CairnResource::NodeTerminal { .. }
                            | CairnResource::ProjectTerminal { .. } => "read_resource",
                            // All other resources use read_issue_resource
                            _ => "read_issue_resource",
                        }
                    }
                    None => {
                        return Err(rmcp::ErrorData::invalid_request(
                            format!("Invalid cairn resource URI: {}", uri),
                            None,
                        ));
                    }
                }
            } else {
                return Err(rmcp::ErrorData::invalid_request(
                    format!("Unknown resource scheme: {}", uri),
                    None,
                ));
            };

            // Call Tauri callback
            let callback_request = CallbackRequest {
                cwd: self.cwd.to_string(),
                run_id: self.run_id.as_ref().map(|r| r.to_string()),
                tool: tool_name.to_string(),
                payload: serde_json::json!({ "uri": uri }),
                tool_use_id: None,
            };

            let response = self.call_tauri_full(&callback_request).await;
            if self.should_update_base_uri_after_read(tool_name, &response) {
                self.note_successful_resource_read(uri);
            }

            // For terminal resources, parse the structured response
            if tool_name == "read_resource" {
                match serde_json::from_str::<TerminalReadResult>(&response.result) {
                    Ok(terminal_result) => {
                        let rendered = self.relativize_cairn_uris_in_text(&terminal_result.output);
                        let rendered = assemble_reminders(rendered, &response.reminders);
                        let contents =
                            vec![ResourceContents::text(cap_text_result(&rendered, 0), uri)];
                        return Ok(ReadResourceResult { contents });
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to parse terminal read result: {} - response: {}",
                            e,
                            response.result
                        );
                    }
                }
            }

            // For issue resources (or fallback), return the result directly
            // The backend returns canonical content; display rendering can relativize URIs.
            let rendered = self.relativize_cairn_uris_in_text(&response.result);
            let rendered = assemble_reminders(rendered, &response.reminders);
            let contents = vec![ResourceContents::text(cap_text_result(&rendered, 0), uri)];
            Ok(ReadResourceResult { contents })
        }
    }
}

/// Terminal read result returned from Tauri
#[derive(Debug, Clone, Deserialize)]
struct TerminalReadResult {
    output: String,
    status: String,
    #[serde(default)]
    exit_code: Option<i32>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // CLI subcommands (read/write) keep stderr clean for piping — logs go to
    // the file only. The MCP server path also logs to stderr.
    let is_cli = matches!(
        args.command,
        Some(Command::Read { .. }) | Some(Command::Write { .. }) | Some(Command::Watch { .. })
    );
    let _log_guard = cairn_common::logging::init(cairn_common::logging::LogConfig {
        process: cairn_common::logging::ProcessTag::Mcp,
        log_dir: None,
        stderr: !is_cli,
    })
    .expect("Failed to initialize logging");

    // CLI subcommands: thin forward to the running app, print to stdout, exit
    // with a code reflecting success. The MCP server path falls through below.
    match &args.command {
        Some(Command::Read {
            targets,
            offset,
            limit,
        }) => {
            let ok = run_cli_read(targets, *offset, *limit).await;
            std::process::exit(if ok { 0 } else { 1 });
        }
        Some(Command::Write { json, commit_msg }) => {
            let ok = run_cli_change(json.clone(), commit_msg.clone()).await;
            std::process::exit(if ok { 0 } else { 1 });
        }
        Some(Command::Watch { issue_uri, since }) => {
            let ok = run_cli_watch(issue_uri.clone(), *since).await;
            std::process::exit(if ok { 0 } else { 1 });
        }
        Some(Command::Mcp) | None => {}
    }

    // Callback URL - passed from main app via MCP config env var.
    // If omitted, fall back to the build-profile default app port.
    let callback_url = env::var("CAIRN_CALLBACK_URL").unwrap_or_else(|_| default_callback_url());
    // Get current working directory - fallback for run identification
    let cwd = env::current_dir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    // Run ID - preferred method for accurate run identification (avoids cwd ambiguity)
    let run_id = env::var("CAIRN_RUN_ID").ok();
    // Shared secret for bearer token authentication (base64-encoded)
    let mcp_secret = env::var("CAIRN_MCP_SECRET")
        .ok()
        .or_else(cairn_common::auth::load_local_mcp_token);
    // Stable home URI for client-side Cairn shorthand resolution.
    let home_uri = env::var("CAIRN_HOME_URI").ok().and_then(|uri| {
        if parse_cairn_uri(&uri).is_some() {
            Some(uri)
        } else {
            tracing::warn!("Ignoring invalid CAIRN_HOME_URI: {}", uri);
            None
        }
    });

    tracing::info!("Starting cairn-cli server");
    tracing::info!("Callback URL: {}", callback_url);
    tracing::info!("Working directory: {}", cwd);
    if let Some(ref id) = run_id {
        tracing::info!("Run ID: {}", id);
    }
    if let Some(ref uri) = home_uri {
        tracing::info!("Home URI: {}", uri);
    }

    // Parse available agents from JSON argument
    let available_agents: Vec<AgentInfo> = if let Some(ref agents_json) = args.agents {
        match serde_json::from_str::<Vec<AgentInfo>>(agents_json) {
            Ok(agents) => {
                tracing::info!("Loaded {} available agents", agents.len());
                agents
            }
            Err(e) => {
                tracing::warn!("Failed to parse agents JSON: {}", e);
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let service = CairnMcp::new_with_home_uri(
        callback_url,
        cwd,
        run_id,
        mcp_secret,
        available_agents,
        home_uri,
    );

    // Create stdio transport and run the server
    let transport = rmcp::transport::stdio();
    let server = service.serve(transport).await?;

    // Wait for the server to complete
    server.waiting().await?;

    Ok(())
}

// ============================================================================
// CLI subcommands (thin client: forward to the running app, print to stdout)
// ============================================================================

fn default_callback_url() -> String {
    // Use the shared resolver so cairn-cli (built --release) targets the same
    // port as the app that spawned it. The app sets CAIRN_ENV in the MCP child
    // env; manual terminal use selects the port via CAIRN_ENV=dev.
    let port = cairn_common::paths::callback_port();
    format!("http://127.0.0.1:{}/api/mcp", port)
}

/// Callback URL for CLI use: explicit env var (set by the app for in-run
/// invocations), else the build-profile default app port.
fn cli_callback_url() -> String {
    env::var("CAIRN_CALLBACK_URL").unwrap_or_else(|_| default_callback_url())
}

/// Build a thin `CairnMcp` client from the environment for CLI forwarding.
fn build_cli_client(callback_url: String) -> CairnMcp {
    let cwd = env::current_dir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let run_id = env::var("CAIRN_RUN_ID").ok();
    let mcp_secret = env::var("CAIRN_MCP_SECRET")
        .ok()
        .or_else(cairn_common::auth::load_local_mcp_token);
    let home_uri = env::var("CAIRN_HOME_URI")
        .ok()
        .filter(|uri| parse_cairn_uri(uri).is_some());
    CairnMcp::new_with_home_uri(callback_url, cwd, run_id, mcp_secret, Vec::new(), home_uri)
}

/// Extract printable text and the error flag from a tool result
/// (version-tolerant: reads the serialized JSON rather than rmcp internals).
fn tool_result_text(result: &CallToolResult) -> (String, bool) {
    let value = serde_json::to_value(result).unwrap_or_default();
    let is_error = value
        .get("isError")
        .or_else(|| value.get("is_error"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let mut out = String::new();
    if let Some(items) = value.get("content").and_then(|c| c.as_array()) {
        for item in items {
            if let Some(t) = item.get("text").and_then(|t| t.as_str()) {
                out.push_str(t);
            }
        }
    }
    (out, is_error)
}

/// Print tool output to the right stream and return success.
fn emit_tool_result(result: &CallToolResult) -> bool {
    let (text, is_error) = tool_result_text(result);
    if is_error {
        if !text.is_empty() {
            eprint!("{text}");
            if !text.ends_with('\n') {
                eprintln!();
            }
        }
    } else {
        print!("{text}");
        if !text.ends_with('\n') {
            println!();
        }
    }
    !is_error
}

/// Parse `host`/`port` from a callback URL.
fn callback_host_port(callback_url: &str) -> Option<(String, u16)> {
    let parsed = url::Url::parse(callback_url).ok()?;
    let host = parsed.host_str()?.to_string();
    let port = parsed.port().unwrap_or(80);
    Some((host, port))
}

/// True if something is listening on the callback port.
fn probe_callback(callback_url: &str) -> bool {
    use std::net::{TcpStream, ToSocketAddrs};
    let Some((host, port)) = callback_host_port(callback_url) else {
        return false;
    };
    (host.as_str(), port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut addrs| addrs.next())
        .map(|addr| TcpStream::connect_timeout(&addr, Duration::from_millis(300)).is_ok())
        .unwrap_or(false)
}

/// Launch the Cairn app in the background (macOS). Returns whether launch was
/// dispatched; readiness is confirmed separately by polling the callback.
fn autostart_app(callback_url: &str) -> bool {
    #[cfg(target_os = "macos")]
    {
        let dev = callback_host_port(callback_url)
            .map(|(_, port)| port == 3857)
            .unwrap_or(false);
        let bundle = if dev {
            "com.cairn.desktop.dev"
        } else {
            "com.cairn.desktop"
        };
        // -g: don't foreground, -j: launch hidden, -b: by bundle id.
        match std::process::Command::new("open")
            .args(["-gjb", bundle])
            .status()
        {
            Ok(status) => status.success(),
            Err(e) => {
                tracing::warn!("failed to launch Cairn app: {e}");
                false
            }
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = callback_url;
        false
    }
}

/// Ensure the app is reachable: probe, and if not, autostart and poll (~30s).
async fn ensure_app_reachable(callback_url: &str) -> bool {
    if probe_callback(callback_url) {
        return true;
    }
    if !autostart_app(callback_url) {
        return probe_callback(callback_url);
    }
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        if probe_callback(callback_url) {
            return true;
        }
    }
    false
}

/// Parse a `ChangeInput` from a JSON object (with `changes`) or a bare array.
fn parse_change_input(raw: &str) -> Result<ChangeInput, String> {
    let value: serde_json::Value = serde_json::from_str(raw).map_err(|e| e.to_string())?;
    if value.is_array() {
        let changes: Vec<ChangeItemInput> =
            serde_json::from_value(value).map_err(|e| e.to_string())?;
        Ok(ChangeInput {
            changes: Some(changes),
            commit_msg: None,
            preview: None,
            atomic: None,
        })
    } else {
        serde_json::from_value(value).map_err(|e| e.to_string())
    }
}

async fn run_cli_read(targets: &[String], offset: Option<usize>, limit: Option<usize>) -> bool {
    // --offset/--limit are a single-target human convenience: fold them into the
    // lone target's query string. With multiple targets they are ambiguous, so
    // scope must be expressed per-URI in each target's query.
    let paths = match fold_cli_scope(targets, offset, limit) {
        Ok(paths) => paths,
        Err(message) => {
            eprintln!("cairn read: {message}");
            return false;
        }
    };

    let callback_url = cli_callback_url();
    if !ensure_app_reachable(&callback_url).await {
        eprintln!("cairn: requires a running Cairn app (could not reach or start it).");
        return false;
    }
    let client = build_cli_client(callback_url);
    let input = ReadFileInput { paths };
    match client.read(Parameters(input)).await {
        Ok(result) => emit_tool_result(&result),
        Err(e) => {
            eprintln!("cairn read failed: {e}");
            false
        }
    }
}

/// Fold CLI `--offset`/`--limit` flags into a single target's query string.
/// Errors if either flag is given alongside multiple targets.
fn fold_cli_scope(
    targets: &[String],
    offset: Option<usize>,
    limit: Option<usize>,
) -> Result<Vec<String>, String> {
    if offset.is_none() && limit.is_none() {
        return Ok(targets.to_vec());
    }
    if targets.len() != 1 {
        return Err(
            "--offset/--limit require exactly one target; scope each target via its own query string (e.g. 'file:x.rs?offset=10&limit=20')".to_string(),
        );
    }
    let mut extra: Vec<String> = Vec::new();
    if let Some(offset) = offset {
        extra.push(format!("offset={offset}"));
    }
    if let Some(limit) = limit {
        extra.push(format!("limit={limit}"));
    }
    let target = &targets[0];
    let separator = if target.contains('?') { '&' } else { '?' };
    Ok(vec![format!("{target}{separator}{}", extra.join("&"))])
}

/// Block until an issue needs attention or is done, re-issuing the long-poll
/// transparently.
///
/// Each iteration is a real server-side broadcast await (no client polling); on
/// a `pending` sentinel the loop re-calls carrying the latest `updated_at` as
/// `--since`, so a change that lands between chunks is caught by the next call's
/// current-state check. Returns when the issue is `actionable` (needs the
/// driver) or `resolved` (reached a terminal status — merged/closed/failed).
async fn run_cli_watch(issue_uri: String, since: Option<i64>) -> bool {
    let callback_url = cli_callback_url();
    if !ensure_app_reachable(&callback_url).await {
        eprintln!("cairn: requires a running Cairn app (could not reach or start it).");
        return false;
    }
    let client = build_cli_client(callback_url);
    let mut cursor = since;
    loop {
        let payload = match cursor {
            Some(s) => serde_json::json!({ "issue_uri": issue_uri, "since": s }),
            None => serde_json::json!({ "issue_uri": issue_uri }),
        };
        let request = CallbackRequest {
            cwd: client.cwd.to_string(),
            run_id: client.run_id.as_ref().map(|r| r.to_string()),
            tool: "watch".to_string(),
            payload,
            tool_use_id: None,
        };
        let outcome = client.call_tauri_full(&request).await;
        if !outcome.transport_ok {
            eprintln!("cairn watch: {}", outcome.result.trim_end());
            return false;
        }
        let value: serde_json::Value = match serde_json::from_str(&outcome.result) {
            Ok(v) => v,
            Err(_) => {
                eprintln!("cairn watch: {}", outcome.result.trim_end());
                return false;
            }
        };
        // Augmentation reminders (e.g. a queued DM to the watcher's run) ride as
        // data alongside the JSON event; surface them so they are not lost.
        if !outcome.reminders.is_empty() {
            eprintln!(
                "cairn watch:{}",
                assemble_reminders(String::new(), &outcome.reminders)
            );
        }
        match value.get("status").and_then(|s| s.as_str()) {
            // The issue needs the driver (a question, gated artifact, review),
            // or it reached a terminal status (merged/closed/failed). Either
            // ends the loop: print the typed event JSON the server returned.
            // The `fact` block carries the inline content (question text,
            // permission tool, artifact summary, PR state) so the caller does
            // not have to do a follow-up `read` to act.
            Some("actionable") | Some("resolved") => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&value).unwrap_or(outcome.result)
                );
                return true;
            }
            Some("pending") => {
                // Advance the cursor and re-issue; the server call blocks, so
                // there is no client-side sleep or poll here.
                cursor = value.get("updated_at").and_then(|u| u.as_i64()).or(cursor);
                continue;
            }
            _ => {
                eprintln!("cairn watch: {}", outcome.result.trim_end());
                return false;
            }
        }
    }
}

async fn run_cli_change(json: Option<String>, commit_msg: Option<String>) -> bool {
    let raw = match json {
        Some(j) => j,
        None => {
            use std::io::Read;
            let mut buf = String::new();
            if std::io::stdin().read_to_string(&mut buf).is_err() {
                eprintln!("cairn write: failed to read JSON from stdin");
                return false;
            }
            buf
        }
    };
    let mut input = match parse_change_input(&raw) {
        Ok(input) => input,
        Err(e) => {
            eprintln!("cairn write: invalid JSON: {e}");
            return false;
        }
    };
    if commit_msg.is_some() {
        input.commit_msg = commit_msg;
    }
    let callback_url = cli_callback_url();
    if !ensure_app_reachable(&callback_url).await {
        eprintln!("cairn: requires a running Cairn app (could not reach or start it).");
        return false;
    }
    let client = build_cli_client(callback_url);
    match client.write(Parameters(input)).await {
        Ok(result) => emit_tool_result(&result),
        Err(e) => {
            eprintln!("cairn write failed: {e}");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_mcp_with_home_uri(home_uri: Option<&str>) -> CairnMcp {
        CairnMcp::new_with_home_uri(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None,
            None,
            vec![],
            home_uri.map(str::to_string),
        )
    }

    #[test]
    fn test_available_agents_stored_for_change_description() {
        let agents = vec![
            AgentInfo {
                name: "Explore".to_string(),
                description: "Search and explore the codebase".to_string(),
            },
            AgentInfo {
                name: "Research".to_string(),
                description: "Research a topic in depth".to_string(),
            },
        ];

        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None, // run_id
            None, // mcp_secret
            agents,
        );

        let tools = mcp.tool_router.list_all();
        assert!(
            tools.iter().any(|t| t.name == "write"),
            "write tool should exist"
        );

        // Agent-list injection into the change description happens in list_tools();
        // here we verify the agents are stored for that step.
        assert_eq!(mcp.available_agents.len(), 2);
        assert_eq!(mcp.available_agents[0].name, "Explore");
        assert_eq!(mcp.available_agents[1].name, "Research");
    }

    #[test]
    fn test_server_info_includes_agents_in_instructions() {
        let agents = vec![AgentInfo {
            name: "Explore".to_string(),
            description: "Search the codebase".to_string(),
        }];

        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None, // run_id
            None, // mcp_secret
            agents,
        );

        let info = mcp.get_info();
        let instructions = info.instructions.unwrap();

        assert!(
            instructions.contains("Available agents"),
            "Instructions should mention available agents"
        );
        assert!(
            instructions.contains("Explore"),
            "Instructions should include agent name"
        );
        assert!(
            instructions.contains("Search the codebase"),
            "Instructions should include agent description"
        );
    }

    #[test]
    fn test_server_info_excludes_agents_when_empty() {
        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None,   // run_id
            None,   // mcp_secret
            vec![], // No agents
        );

        let info = mcp.get_info();
        let instructions = info.instructions.unwrap();

        assert!(
            !instructions.contains("Available agents"),
            "Instructions should not mention agents when none available"
        );
    }

    #[test]
    fn test_base_uri_initializes_from_home_uri() {
        let home_uri = "cairn://p/CAIRN/1086/1/builder";
        let mcp = create_test_mcp_with_home_uri(Some(home_uri));

        assert_eq!(
            mcp.home_uri.as_ref().map(|uri| uri.as_str()),
            Some(home_uri)
        );
        assert_eq!(mcp.current_base_uri().as_deref(), Some(home_uri));
    }

    #[test]
    fn test_resolve_home_shorthand_with_home_uri() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086/1/builder"));

        assert_eq!(
            mcp.resolve_target("cairn:~/chat"),
            Ok(ResolvedTarget::CairnUri(
                "cairn://p/CAIRN/1086/1/builder/chat".to_string()
            ))
        );
    }

    #[test]
    fn test_resolve_home_shorthand_root_with_home_uri() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086/1/builder"));

        assert_eq!(
            mcp.resolve_target("cairn:~/"),
            Ok(ResolvedTarget::CairnUri(
                "cairn://p/CAIRN/1086/1/builder".to_string()
            ))
        );
    }

    #[test]
    fn test_resolve_home_shorthand_without_home_uri() {
        let mcp = create_test_mcp_with_home_uri(None);

        let err = mcp.resolve_target("cairn:~/messages").unwrap_err();
        assert!(err.contains("home_uri"));
    }

    #[test]
    fn test_resolve_target_rejects_positional_shorthands() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086/1/builder"));

        let current_err = mcp.resolve_target("cairn:./messages").unwrap_err();
        assert!(current_err.contains("Unsupported shorthand"));

        let parent_err = mcp.resolve_target("cairn:../artifact").unwrap_err();
        assert!(parent_err.contains("Unsupported shorthand"));
    }

    #[test]
    fn test_relativize_cairn_uri_uses_home_relative_display() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086/1/builder"));
        mcp.set_base_uri("cairn://p/CAIRN/1086".to_string());

        assert_eq!(
            mcp.relativize_cairn_uri_for_display("cairn://p/CAIRN/1086/1/builder/chat"),
            "cairn:~/chat"
        );
    }

    #[test]
    fn test_relativize_cairn_uri_keeps_cross_project_canonical() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086/1/builder"));
        mcp.set_base_uri("cairn://p/CAIRN/1086".to_string());

        assert_eq!(
            mcp.relativize_cairn_uri_for_display("cairn://p/OTHER/99"),
            "cairn://p/OTHER/99"
        );
    }

    #[test]
    fn test_relativize_cairn_uris_in_text_updates_markdown_and_punctuation() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086"));
        let text =
            "See [messages](cairn://p/CAIRN/1086/messages), then cairn://p/CAIRN/1086/messages.";

        let rendered = mcp.relativize_cairn_uris_in_text(text);

        assert_eq!(
            rendered,
            "See [messages](cairn:~/messages), then cairn:~/messages."
        );
    }

    #[test]
    fn test_resolve_read_target_accepts_home_shorthand() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086"));

        assert_eq!(
            mcp.resolve_read_target("cairn:~/messages"),
            Ok("cairn://p/CAIRN/1086/messages".to_string())
        );
    }

    #[test]
    fn test_resolve_target_rejects_bare_filesystem_paths() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086/1/builder"));

        let err = mcp.resolve_target("src/main.rs").unwrap_err();
        assert!(err.contains("expected cairn://... or file:..."));
    }

    #[test]
    fn test_resolve_target_forwards_file_targets_unchanged() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086/1/builder"));

        // The CLI no longer normalizes file targets; core owns resolution.
        // Worktree root, relative, and absolute identities all forward verbatim.
        assert_eq!(
            mcp.resolve_target("file:"),
            Ok(ResolvedTarget::FileUri("file:".to_string()))
        );
        assert_eq!(
            mcp.resolve_target("file:src/lib.rs"),
            Ok(ResolvedTarget::FileUri("file:src/lib.rs".to_string()))
        );
        assert_eq!(
            mcp.resolve_target("file:/Users/x/abs.rs"),
            Ok(ResolvedTarget::FileUri("file:/Users/x/abs.rs".to_string()))
        );
    }

    #[test]
    fn test_successful_resource_read_updates_base_uri() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086/1/builder"));

        mcp.note_successful_resource_read("cairn://p/CAIRN/1086/messages");

        assert_eq!(
            mcp.current_base_uri().as_deref(),
            Some("cairn://p/CAIRN/1086/messages")
        );
    }

    #[test]
    fn test_failed_issue_read_does_not_update_base_uri() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086"));
        let before = mcp.current_base_uri();
        let response = CallbackOutcome {
            result: "Issue CAIRN-999 not found".to_string(),
            transport_ok: true,
            ..Default::default()
        };

        assert!(!mcp.should_update_base_uri_after_read("read_issue_resource", &response));
        assert_eq!(mcp.current_base_uri(), before);
    }

    #[test]
    fn test_transport_failure_does_not_update_base_uri() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086"));
        let before = mcp.current_base_uri();
        let response = CallbackOutcome {
            result: "Error calling Tauri: boom".to_string(),
            transport_ok: false,
            ..Default::default()
        };

        assert!(!mcp.should_update_base_uri_after_read("read_issue_resource", &response));
        assert_eq!(mcp.current_base_uri(), before);
    }

    #[test]
    fn test_terminal_parse_failure_does_not_update_base_uri() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086"));
        let response = CallbackOutcome {
            result: "not-json".to_string(),
            transport_ok: true,
            ..Default::default()
        };

        assert!(!mcp.should_update_base_uri_after_read("read_resource", &response));
    }

    #[test]
    fn test_file_target_resolution_does_not_update_base_uri() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086/1/builder"));
        let before = mcp.current_base_uri();

        let resolved = mcp.resolve_target("file:Cargo.toml").unwrap();

        assert_eq!(
            resolved,
            ResolvedTarget::FileUri("file:Cargo.toml".to_string())
        );
        assert_eq!(mcp.current_base_uri(), before);
    }

    #[test]
    fn test_rewrite_change_targets_resolves_home_shorthand_without_mutating_base_uri() {
        let mcp = create_test_mcp_with_home_uri(Some("cairn://p/CAIRN/1086"));
        let before = mcp.current_base_uri();
        let input = ChangeInput {
            changes: Some(vec![
                ChangeItemInput {
                    target: Some("cairn:~/messages".to_string()),
                    mode: Some("append".to_string()),
                    payload: Some(serde_json::json!({ "content": "hello" })),
                },
                ChangeItemInput {
                    target: Some("file:docs/notes.md".to_string()),
                    mode: Some("patch".to_string()),
                    payload: Some(serde_json::json!({ "old_string": "old", "new_string": "new" })),
                },
            ]),
            commit_msg: None,
            preview: None,
            atomic: None,
        };

        let rewritten = mcp.rewrite_change_targets(&input).unwrap();
        let changes = rewritten.changes.as_ref().unwrap();

        assert_eq!(
            changes[0].target.as_deref(),
            Some("cairn://p/CAIRN/1086/messages")
        );
        assert_eq!(changes[1].target.as_deref(), Some("file:docs/notes.md"));
        assert_eq!(mcp.current_base_uri(), before);
    }

    /// The advertised JSON-schema `mode` enum must list exactly the canonical
    /// `ChangeMode` variants, so the schema the model sees never drifts from the
    /// modes the shared validator accepts.
    #[test]
    fn change_input_schema_mode_enum_matches_change_mode() {
        let mut generator = schemars::SchemaGenerator::default();
        let schema = <ChangeInput as schemars::JsonSchema>::json_schema(&mut generator);
        let value = serde_json::to_value(&schema).unwrap();
        let enum_values = value["properties"]["changes"]["items"]["properties"]["mode"]["enum"]
            .as_array()
            .expect("mode enum array");
        let mut from_schema: Vec<String> = enum_values
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        from_schema.sort();
        let mut from_enum: Vec<String> = cairn_common::contract::ChangeMode::ALL
            .iter()
            .map(|m| m.as_str().to_string())
            .collect();
        from_enum.sort();
        assert_eq!(from_schema, from_enum);
    }

    /// A genuinely-absent `changes` survives the lenient deserialize + the
    /// re-serialization the validator runs on, producing the precise "not
    /// present" message rather than an opaque rmcp parse error.
    #[test]
    fn absent_changes_round_trips_to_not_present() {
        let input: ChangeInput = serde_json::from_str("{}").unwrap();
        let raw = serde_json::to_value(&input).unwrap();
        let errors = cairn_common::change_validation::validate_change_value(&raw);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].message.contains("was not present"));
    }

    /// File target without commit_msg + a bad mode + a missing target are all
    /// reported together in a single validation pass over the lenient input.
    #[test]
    fn multiple_problems_reported_in_one_pass_via_lenient_input() {
        let input: ChangeInput = serde_json::from_value(serde_json::json!({
            "changes": [
                { "target": "file:src/lib.rs", "mode": "create", "payload": { "content": "x" } },
                { "mode": "bogus" }
            ]
        }))
        .unwrap();
        let raw = serde_json::to_value(&input).unwrap();
        let errors = cairn_common::change_validation::validate_change_value(&raw);
        assert!(errors.iter().any(|e| e.field == "commit_msg"));
        assert!(errors
            .iter()
            .any(|e| e.field == "target" && e.index == Some(1)));
        assert!(errors
            .iter()
            .any(|e| e.field == "mode" && e.index == Some(1)));
    }

    #[test]
    fn http_status_error_message_explains_413() {
        let msg = http_status_error_message(413, "Payload Too Large", 5_000_000);
        assert!(msg.contains("413"));
        assert!(msg.contains("5000000 bytes"));
        assert!(msg.contains("body-size limit"));
    }

    #[test]
    fn http_status_error_message_reports_other_statuses() {
        let msg = http_status_error_message(500, "Internal Server Error", 42);
        assert!(msg.contains("500"));
        assert!(msg.contains("42 bytes"));
        assert!(!msg.contains("body-size limit"));
    }

    #[test]
    fn truncate_chars_does_not_panic_on_multibyte_boundary() {
        // A run of multi-byte chars whose byte length exceeds the limit but whose
        // char count is under it: byte slicing would panic mid-codepoint.
        let body = "é".repeat(400); // 400 chars, 800 bytes
        let truncated = truncate_chars(&body, 500);
        assert_eq!(truncated.chars().count(), 400);

        let long = "€".repeat(1000); // 1000 chars, 3000 bytes
        let clamped = truncate_chars(&long, 200);
        assert_eq!(clamped.chars().count(), 200);
    }

    #[test]
    fn test_cap_text_result_short_text_unchanged() {
        let text = "line 1\nline 2\nline 3";
        let result = cap_text_result(text, 0);
        assert_eq!(result, text);
    }

    #[test]
    fn test_cap_text_result_truncates_at_line_boundary() {
        // Build text that exceeds MAX_RESULT_CHARS
        let line = "x".repeat(100);
        let lines: Vec<&str> = std::iter::repeat(line.as_str())
            .take(MAX_RESULT_CHARS / 100 + 100)
            .collect();
        let text = lines.join("\n");
        assert!(text.len() > MAX_RESULT_CHARS);

        let result = cap_text_result(&text, 0);

        // Should be truncated
        assert!(result.len() < text.len());
        // Should contain continuation hint
        assert!(result.contains("--- truncated"));
        assert!(result.contains("Call again with offset="));
        // The truncated content should end at a line boundary (before the hint)
        let content_part = result.split("\n\n--- truncated").next().unwrap();
        // Each line is 100 chars, so content should be made of complete lines
        assert!(content_part.lines().all(|l| l.len() == 100));
    }

    #[test]
    fn test_cap_text_result_with_nonzero_offset() {
        let line = "x".repeat(100);
        let lines: Vec<&str> = std::iter::repeat(line.as_str())
            .take(MAX_RESULT_CHARS / 100 + 100)
            .collect();
        let text = lines.join("\n");

        let result = cap_text_result(&text, 50);
        // Continuation hint should show offset-based line numbers
        assert!(result.contains("lines 51-"));
        assert!(result.contains("Call again with offset="));
    }

    #[test]
    fn test_cap_text_result_no_newlines() {
        // A single long line with no newlines should still truncate
        let text = "x".repeat(MAX_RESULT_CHARS + 1000);
        let result = cap_text_result(&text, 0);
        assert!(result.contains("--- truncated"));
        // Should truncate at MAX_RESULT_CHARS since no newline found
        assert!(result.starts_with(&"x".repeat(MAX_RESULT_CHARS)));
    }

    #[test]
    fn test_cap_text_result_multibyte_utf8_boundary() {
        // Build text where MAX_RESULT_CHARS falls inside a multi-byte char.
        // U+00E9 (é) is 2 bytes in UTF-8 (0xC3 0xA9).
        // Fill with ASCII up to one byte before the limit, then a 2-byte char
        // so the limit lands on the second byte of the é.
        let prefix = "a".repeat(MAX_RESULT_CHARS - 1);
        // é adds 2 bytes → total = MAX_RESULT_CHARS + 1 bytes, triggers truncation
        // and MAX_RESULT_CHARS falls on byte index inside the é
        let text = format!("{}é{}", prefix, "b".repeat(2000));
        assert!(text.len() > MAX_RESULT_CHARS);

        // Before the fix this would panic with "byte index is not a char boundary"
        let result = cap_text_result(&text, 0);
        assert!(result.contains("--- truncated"));
        // The é should be excluded (boundary rounds down), so content is just the prefix
        assert!(result.starts_with(&prefix));
    }

    #[test]
    fn test_cap_text_result_multibyte_utf8_with_newlines() {
        // Mix multi-byte chars with newlines to test both boundary + line truncation
        // Each line: 99 ASCII chars + é (2 bytes) + newline = 102 bytes per line
        let line = format!("{}é", "z".repeat(99));
        assert_eq!(line.len(), 101); // 99 + 2 bytes for é
        let num_lines = MAX_RESULT_CHARS / 101 + 100;
        let text = std::iter::repeat(line.as_str())
            .take(num_lines)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.len() > MAX_RESULT_CHARS);

        let result = cap_text_result(&text, 0);
        assert!(result.contains("--- truncated"));
        // Content before the truncation marker should end at a complete line
        let content_part = result.split("\n\n--- truncated").next().unwrap();
        for line in content_part.lines() {
            assert!(line.ends_with('é'), "Line should be intact: {:?}", line);
        }
    }

    #[test]
    fn test_cap_run_result_passthrough_under_cap() {
        // Core caps run output at MAX_RUN_RESULT_CHARS == MAX_RESULT_CHARS, so a
        // well-formed result passes through unchanged.
        let text = "=== git diff ===\nsome output\n--- elided 10 lines / 200 chars; tail kept below ---\ntail";
        assert_eq!(cap_run_result(text), text);
    }

    #[test]
    fn test_cap_run_result_tail_biased_no_offset_footer() {
        // Defensive backstop: an over-cap result is tail-biased, never carrying the
        // read-style offset continuation footer.
        let text = format!(
            "HEAD-START\n{}\nTAIL-SIGNAL",
            "x".repeat(MAX_RESULT_CHARS * 2)
        );
        let result = cap_run_result(&text);
        assert!(result.len() <= MAX_RESULT_CHARS);
        assert!(
            result.ends_with("TAIL-SIGNAL"),
            "keeps the tail, not the head"
        );
        assert!(!result.contains("Call again with offset"));
        assert!(result.contains("Re-run scoped"));
    }

    #[test]
    fn test_cap_run_result_multibyte_boundary_safe() {
        // A long multibyte body must not split a char when tail-slicing.
        let text = "é".repeat(MAX_RESULT_CHARS); // 2 bytes each → 2x the cap
        let result = cap_run_result(&text);
        assert!(result.len() <= MAX_RESULT_CHARS);
        assert!(!result.contains("Call again with offset"));
    }

    #[test]
    fn test_change_transport_failure_becomes_tool_error() {
        let outcome = CallbackOutcome {
            result: "Error calling Tauri: connection refused".to_string(),
            transport_ok: false,
            ..Default::default()
        };

        let result = change_callback_result(
            outcome,
            "http://user:secret@127.0.0.1:3847/api/mcp?token=secret",
            "write",
        );

        assert!(result.is_error.unwrap_or(false));
        let text = get_text(&result);
        assert!(text.contains("MCP callback transport failed"));
        assert!(text.contains("tool: write"));
        assert!(text.contains("class: request"));
        assert!(text.contains("http://127.0.0.1:3847/api/mcp"));
        assert!(!text.contains("user:secret"));
        assert!(!text.contains("token=secret"));
    }

    #[test]
    fn test_change_report_failure_becomes_tool_error() {
        // Mirrors the real failure shape: empty/null/false fields are omitted,
        // but genuine `failures` and `partial_success: true` are still present.
        let report = serde_json::json!({
            "applied": [],
            "failures": [{
                "index": 1,
                "target": "src/lib.rs",
                "mode": "patch",
                "kind": "file",
                "error": "old_string not found"
            }],
            "partial_success": true
        })
        .to_string();
        let outcome = CallbackOutcome {
            result: report,
            transport_ok: true,
            ..Default::default()
        };

        let result = change_callback_result(outcome, "http://127.0.0.1:3847/api/mcp", "write");

        assert!(result.is_error.unwrap_or(false));
        let text = get_text(&result);
        assert!(text.contains("1 of 1 changes failed: [1] src/lib.rs patch"));
        assert!(text.contains("old_string not found"));
        assert!(text.contains("partial_success"));
    }

    #[test]
    fn test_change_report_failure_before_blocking_result_becomes_tool_error() {
        let report = serde_json::json!({
            "applied": [],
            "failures": [{
                "index": 0,
                "target": "cairn://p/MISSING/messages",
                "mode": "append",
                "kind": "resource",
                "error": "No project found"
            }]
        })
        .to_string();
        let outcome = CallbackOutcome {
            result: format!("{report}\n\nTask append target node not found"),
            transport_ok: true,
            ..Default::default()
        };

        let result = change_callback_result(outcome, "http://127.0.0.1:3847/api/mcp", "write");

        assert!(result.is_error.unwrap_or(false));
        let text = get_text(&result);
        assert!(text.contains("1 of 1 changes failed: [0] cairn://p/MISSING/messages append"));
        assert!(text.contains("Task append target node not found"));
    }

    #[test]
    fn test_change_commit_failure_becomes_tool_error() {
        // Mirrors the real shape: `failure`, `partial_success`, and
        // `transactional` are omitted; the failing `commit` carries the signal.
        let report = serde_json::json!({
            "applied": [{
                "index": 0,
                "target": "src/lib.rs",
                "mode": "patch",
                "kind": "file",
                "summary": "updated"
            }],
            "commit": {
                "status": "failed",
                "sha": null,
                "pr_number": null,
                "message": "git commit failed"
            }
        })
        .to_string();
        let outcome = CallbackOutcome {
            result: report,
            transport_ok: true,
            ..Default::default()
        };

        let result = change_callback_result(outcome, "http://127.0.0.1:3847/api/mcp", "write");

        assert!(result.is_error.unwrap_or(false));
        let text = get_text(&result);
        assert!(text.contains("change commit failed"));
        assert!(text.contains("git commit failed"));
    }

    #[test]
    fn test_successful_change_report_remains_success() {
        // Mirrors a clean commit success: only `applied` and `commit` remain.
        let report = serde_json::json!({
            "applied": [],
            "commit": {
                "status": "committed",
                "sha": "abc1234",
                "pr_number": null,
                "message": null
            }
        })
        .to_string();
        let outcome = CallbackOutcome {
            result: report,
            transport_ok: true,
            ..Default::default()
        };

        let result = change_callback_result(outcome, "http://127.0.0.1:3847/api/mcp", "write");

        assert!(!result.is_error.unwrap_or(false));
        assert!(get_text(&result).contains("committed"));
    }

    fn get_text(result: &CallToolResult) -> &str {
        result
            .content
            .first()
            .and_then(|c| c.as_text())
            .map(|t| t.text.as_ref())
            .unwrap()
    }

    #[test]
    fn test_unified_edit_tool_visible() {
        let mcp = CairnMcp::new(
            "http://localhost:3847".to_string(),
            "/test/path".to_string(),
            None,
            None,
            vec![],
        );

        let all_tools = mcp.tool_router.list_all();
        let all_names: Vec<&str> = all_tools.iter().map(|t| t.name.as_ref()).collect();

        assert!(
            all_names.contains(&"write"),
            "write tool should be in tool router"
        );
        assert!(
            !all_names.contains(&"edit"),
            "edit tool should not exist after replacement"
        );
        assert!(
            !all_names.contains(&"message"),
            "message tool should not exist after replacement"
        );
        assert!(
            !all_names.contains(&"add_comment"),
            "add_comment tool should not exist after replacement"
        );
        assert!(
            !all_names.contains(&"update_issue"),
            "update_issue tool should not exist after replacement"
        );
    }

    fn run_input(value: serde_json::Value) -> RunInput {
        serde_json::from_value(value).expect("valid RunInput")
    }

    #[test]
    fn read_envelope_parses_clean_with_reminders_as_data() {
        // CAIRN-1590 regression: the callback response carries the read-batch
        // envelope JSON in `result` and the system-reminder bodies separately
        // in `reminders`. The JSON parses cleanly (no trailing text to split)
        // and the assembled model-visible text contains every reminder.
        let envelope = cairn_common::read::ReadBatchEnvelope {
            text: "=== file:a.rs [lines 1\u{2013}1 of 1] ===\n     1\ta".to_string(),
            images: vec![],
            segments: vec![],
        };
        let response = CallbackResponse {
            result: serde_json::to_string(&envelope).unwrap(),
            artifact_uri: None,
            reminders: vec![
                "[Direct message from planner] hello mid-turn".to_string(),
                "The worktree has uncommitted changes. Commit them.".to_string(),
            ],
        };

        // Content is pure JSON: it deserializes as the envelope with no leftover.
        let parsed: ReadBatchEnvelope = serde_json::from_str(&response.result)
            .expect("handler content stays parseable as the bare envelope");
        assert_eq!(parsed.text, envelope.text);

        // Assembly appends every reminder, each wrapped in a system-reminder block.
        let assembled = assemble_reminders(parsed.text, &response.reminders);
        assert!(assembled.contains("[Direct message from planner] hello mid-turn"));
        assert!(assembled.contains("The worktree has uncommitted changes"));
        assert_eq!(assembled.matches("<system-reminder>").count(), 2);
    }

    #[test]
    fn assemble_reminders_is_noop_without_reminders() {
        let content = "plain result".to_string();
        assert_eq!(assemble_reminders(content.clone(), &[]), content);
    }

    #[test]
    fn assemble_reminders_wraps_each_body_in_delivery_order() {
        let out = assemble_reminders(
            "body".to_string(),
            &["first".to_string(), "second".to_string()],
        );
        assert_eq!(
            out,
            "body\n\n<system-reminder>\nfirst\n</system-reminder>\n\n<system-reminder>\nsecond\n</system-reminder>"
        );
    }

    #[test]
    fn run_input_parses_commands_array() {
        let input = run_input(serde_json::json!({
            "commands": [
                { "command": "npm test", "description": "tests", "timeout": 1000 },
                { "target": "cairn://skills/ui/scripts/check.sh", "payload": { "args": ["--fast"] } }
            ],
            "sequential": true,
            "stop_on_error": false,
            "commit_msg": "done"
        }));
        assert_eq!(input.commands.len(), 2);
        assert_eq!(input.sequential, Some(true));
        assert_eq!(input.stop_on_error, Some(false));
        assert_eq!(input.commit_msg.as_deref(), Some("done"));
        assert_eq!(input.commands[0].command.as_deref(), Some("npm test"));
        assert_eq!(
            input.commands[1].target.as_deref(),
            Some("cairn://skills/ui/scripts/check.sh")
        );
        assert_eq!(
            input.commands[1].payload.as_ref().unwrap().args,
            vec!["--fast".to_string()]
        );
    }

    #[test]
    fn run_input_preserves_mcp_args_json() {
        // An MCP tool call carries its named arguments in payload.args_json.
        // The field must survive deserialize so the re-serialized payload
        // forwarded to the backend still contains the args.
        let input = run_input(serde_json::json!({
            "commands": [
                { "target": "cairn://mcp/axon/look", "payload": { "args_json": { "app": "Finder" } } }
            ]
        }));
        let payload = input.commands[0].payload.as_ref().expect("payload present");
        assert_eq!(
            payload.args_json,
            Some(serde_json::json!({ "app": "Finder" }))
        );
        // Round-trip: re-serializing the input (as `run` does before forwarding)
        // keeps args_json intact.
        let reser = serde_json::to_value(&input).expect("serialize RunInput");
        assert_eq!(
            reser["commands"][0]["payload"]["args_json"],
            serde_json::json!({ "app": "Finder" })
        );
    }

    #[test]
    fn validate_run_input_rejects_empty_commands() {
        let input = run_input(serde_json::json!({ "commands": [] }));
        assert!(validate_run_input(&input).is_err());
    }

    #[test]
    fn validate_run_input_rejects_item_with_both_command_and_target() {
        let input = run_input(serde_json::json!({
            "commands": [{ "command": "echo hi", "target": "cairn://skills/ui/scripts/x.sh" }]
        }));
        let err = validate_run_input(&input).unwrap_err();
        assert!(err.contains("both"));
    }

    #[test]
    fn validate_run_input_rejects_item_with_neither() {
        let input = run_input(serde_json::json!({
            "commands": [{ "description": "nothing" }]
        }));
        let err = validate_run_input(&input).unwrap_err();
        assert!(err.contains("neither"));
    }

    #[test]
    fn validate_run_input_accepts_well_formed_batch() {
        let input = run_input(serde_json::json!({
            "commands": [
                { "command": "echo a" },
                { "target": "cairn://skills/ui/scripts/x.sh" }
            ]
        }));
        assert!(validate_run_input(&input).is_ok());
    }

    #[test]
    fn fold_cli_scope_folds_single_target() {
        let paths = fold_cli_scope(&["file:x.rs".to_string()], Some(10), Some(20)).unwrap();
        assert_eq!(paths, vec!["file:x.rs?offset=10&limit=20".to_string()]);
    }

    #[test]
    fn fold_cli_scope_appends_with_ampersand_when_query_present() {
        let paths = fold_cli_scope(&["file:x.rs?grep=foo".to_string()], Some(10), None).unwrap();
        assert_eq!(paths, vec!["file:x.rs?grep=foo&offset=10".to_string()]);
    }

    #[test]
    fn fold_cli_scope_passes_targets_through_without_flags() {
        let targets = vec!["file:a.rs".to_string(), "file:b.rs".to_string()];
        let paths = fold_cli_scope(&targets, None, None).unwrap();
        assert_eq!(paths, targets);
    }

    #[test]
    fn fold_cli_scope_rejects_flags_with_multiple_targets() {
        let targets = vec!["file:a.rs".to_string(), "file:b.rs".to_string()];
        let err = fold_cli_scope(&targets, Some(10), None).unwrap_err();
        assert!(err.contains("exactly one target"));
    }

    #[tokio::test]
    async fn read_rejects_empty_paths() {
        let mcp = create_test_mcp_with_home_uri(None);
        let result = mcp
            .read(Parameters(ReadFileInput { paths: vec![] }))
            .await
            .unwrap();
        assert!(result.is_error.unwrap_or(false));
        assert!(get_text(&result).contains("non-empty"));
    }

    // ---- callback timeout derivation -------------------------------------

    fn callback_request(tool: &str, payload: serde_json::Value) -> CallbackRequest {
        CallbackRequest {
            cwd: "/tmp".to_string(),
            run_id: None,
            tool: tool.to_string(),
            payload,
            tool_use_id: None,
        }
    }

    #[test]
    fn effective_item_timeout_defaults_to_120s() {
        let input = run_input(serde_json::json!({ "commands": [{ "command": "echo hi" }] }));
        assert_eq!(
            effective_run_item_timeout_ms(&input.commands[0]),
            HOST_ITEM_TIMEOUT_DEFAULT_MS
        );
    }

    #[test]
    fn effective_item_timeout_clamps_to_host_ceiling() {
        let input = run_input(serde_json::json!({
            "commands": [{ "command": "sleep 1", "timeout": 5_000_000u32 }]
        }));
        assert_eq!(
            effective_run_item_timeout_ms(&input.commands[0]),
            HOST_ITEM_TIMEOUT_MAX_MS
        );
    }

    #[test]
    fn run_timeout_sequential_sums_item_budgets_plus_margin() {
        // Two items at 300s each, run in order: host budget is the sum.
        let input = run_input(serde_json::json!({
            "commands": [
                { "command": "sleep 150", "timeout": 300_000u32 },
                { "command": "sleep 150", "timeout": 300_000u32 }
            ],
            "sequential": true
        }));
        assert_eq!(
            run_callback_timeout(&input),
            Duration::from_millis(600_000) + CALLBACK_TIMEOUT_MARGIN
        );
    }

    #[test]
    fn run_timeout_parallel_takes_max_item_budget_plus_margin() {
        // Default (parallel): host runs concurrently, so the budget is the max.
        let input = run_input(serde_json::json!({
            "commands": [
                { "command": "sleep 100", "timeout": 200_000u32 },
                { "command": "sleep 300", "timeout": 500_000u32 }
            ]
        }));
        assert_eq!(
            run_callback_timeout(&input),
            Duration::from_millis(500_000) + CALLBACK_TIMEOUT_MARGIN
        );
    }

    #[test]
    fn run_timeout_uses_defaults_when_items_omit_timeout() {
        let input = run_input(serde_json::json!({
            "commands": [{ "command": "a" }, { "command": "b" }],
            "sequential": true
        }));
        assert_eq!(
            run_callback_timeout(&input),
            Duration::from_millis(2 * HOST_ITEM_TIMEOUT_DEFAULT_MS) + CALLBACK_TIMEOUT_MARGIN
        );
    }

    #[test]
    fn callback_timeout_run_derives_from_batch() {
        let payload = serde_json::json!({
            "commands": [
                { "command": "sleep 150", "timeout": 300_000u32 },
                { "command": "sleep 150", "timeout": 300_000u32 }
            ],
            "sequential": true
        });
        let request = callback_request("run", payload);
        assert_eq!(
            callback_timeout(&request),
            Duration::from_millis(600_000) + CALLBACK_TIMEOUT_MARGIN
        );
    }

    #[test]
    fn callback_timeout_blocking_write_append_is_unbounded() {
        for collection in ["tasks", "questions"] {
            let payload = serde_json::json!({
                "changes": [{
                    "target": format!("cairn://p/CAIRN/1621/1/builder/{collection}"),
                    "mode": "append",
                    "payload": { "prompt": "do a thing" }
                }]
            });
            let request = callback_request("write", payload);
            assert_eq!(
                callback_timeout(&request),
                UNBOUNDED_CALLBACK_TIMEOUT,
                "{collection} append should not be undercut"
            );
        }
    }

    #[test]
    fn callback_timeout_plain_write_uses_default() {
        // A file edit is bounded host work: the short default applies.
        let payload = serde_json::json!({
            "changes": [{ "target": "file:src/lib.rs", "mode": "create", "payload": { "content": "x" } }]
        });
        let request = callback_request("write", payload);
        assert_eq!(callback_timeout(&request), DEFAULT_CALLBACK_TIMEOUT);
    }

    #[test]
    fn callback_timeout_non_append_to_tasks_uses_default() {
        // Only `mode=append` to tasks/questions blocks; a read/patch does not.
        let payload = serde_json::json!({
            "changes": [{ "target": "cairn://p/CAIRN/1621/1/builder/tasks", "mode": "patch", "payload": {} }]
        });
        let request = callback_request("write", payload);
        assert_eq!(callback_timeout(&request), DEFAULT_CALLBACK_TIMEOUT);
    }

    #[test]
    fn callback_timeout_read_uses_default() {
        let request = callback_request("read_batch", serde_json::json!({ "paths": ["file:x"] }));
        assert_eq!(callback_timeout(&request), DEFAULT_CALLBACK_TIMEOUT);
    }
}
