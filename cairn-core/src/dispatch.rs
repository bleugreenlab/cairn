//! Shared tool dispatch for first-party MCP callback hosts.

/// A dispatched tool call's result: the handler's `content` plus any
/// system-reminder bodies the augmentation layer collected for this run.
///
/// Augmentation is data-only — it never splices reminder text into `content`,
/// so a handler that returns structured JSON (the read-batch envelope, a change
/// report) stays parseable end-to-end. The CLI assembles the model-visible text
/// at the transport edge: `content` first, then each reminder wrapped in a
/// `<system-reminder>` block, in delivery order.
#[derive(Debug, Clone, Default)]
pub struct DispatchOutput {
    pub content: String,
    pub reminders: Vec<String>,
}

/// Dispatch a tool call to the appropriate cairn-core handler.
///
/// On the way out, queued user follow-ups, child side-channel notices, and this
/// busy agent's rousing attention pushes (direct messages now ride the push
/// queue — CAIRN-1900) plus the dirty-worktree notice are collected into
/// [`DispatchOutput::reminders`] for the calling run. This is the Codex-side
/// delivery path (Codex has no Claude-CLI hook surface) and a redundant
/// belt-and-suspenders path for Claude. Push delivery is stamped atomically with
/// its carrying event, so neither boundary double-delivers.
pub async fn dispatch_tool(
    orch: &crate::orchestrator::Orchestrator,
    request: &crate::mcp::types::McpCallbackRequest,
    read_cursors: &std::sync::Mutex<std::collections::HashMap<String, usize>>,
) -> DispatchOutput {
    let content = dispatch_with_dedup(orch, request, read_cursors).await;
    let mut reminders = Vec::new();
    augment_with_queued_dms(orch, request, &mut reminders).await;
    augment_with_dirty_worktree_notice(orch, request, &mut reminders).await;
    DispatchOutput { content, reminders }
}

/// Execute a read-family call with content-aware dedup, or execute any other
/// tool directly.
///
/// Content-aware flail dedup (CAIRN-1271, supersedes the fingerprint-only dedup
/// of CAIRN-1230): a read-family call always executes, then its returned
/// content is hashed and compared against the prior identical call's content
/// this turn. Identical content short-circuits to a `[duplicate call]` stub;
/// changed content returns the fresh result and updates the stored hash so the
/// agent sees the update. Re-reading a resource whose state advanced within the
/// turn (a question just answered, an issue whose status moved, a node that just
/// produced an artifact) therefore returns fresh content rather than a stale
/// stub.
///
/// There is no terminal special-case: a cursor-advancing terminal poll via the
/// `read` tool naturally returns changed content while there is new output (so
/// it is never stubbed then) and dedups only once the terminal has gone
/// genuinely quiet. The dedicated `read_resource` cursor-poll path is excluded
/// by [`is_read_family`] entirely and is unaffected.
///
/// Trade-off: content-aware dedup must execute the read to learn whether the
/// content changed, where the old fingerprint-only dedup could skip execution.
/// The cairn:// resources and files this covers are cheap DB and file reads, so
/// the correctness win outweighs the cost.
async fn dispatch_with_dedup(
    orch: &crate::orchestrator::Orchestrator,
    request: &crate::mcp::types::McpCallbackRequest,
    read_cursors: &std::sync::Mutex<std::collections::HashMap<String, usize>>,
) -> String {
    let result = execute_tool(orch, request, read_cursors).await;

    // Only read-family calls with an active run/turn are dedup-eligible.
    if !is_read_family(&request.tool) {
        return result;
    }
    let Some(run_id) = request.run_id.as_deref() else {
        return result;
    };

    let fp = call_fingerprint(&request.tool, &request.payload);
    let content_hash = hash_content(&result);
    match orch
        .process_state
        .check_and_record_content(run_id, &fp, content_hash)
    {
        crate::agent_process::process::DedupOutcome::Duplicate {
            count,
            distinct_dupes,
        } => {
            let target = request
                .payload
                .get("uri")
                .or_else(|| request.payload.get("path"))
                .and_then(|v| v.as_str())
                .unwrap_or("this target");
            log::info!(
                "flail_dedup: tool={} run={} count={} distinct_dupes={} target={}",
                request.tool,
                run_id,
                count,
                distinct_dupes,
                target
            );
            build_dup_stub(&request.tool, target, count, distinct_dupes)
        }
        // First occurrence, changed content, or PassThrough -> fresh result.
        _ => result,
    }
}

/// Dispatch a tool call to the appropriate cairn-core handler.
async fn execute_tool(
    orch: &crate::orchestrator::Orchestrator,
    request: &crate::mcp::types::McpCallbackRequest,
    read_cursors: &std::sync::Mutex<std::collections::HashMap<String, usize>>,
) -> String {
    match request.tool.as_str() {
        // File and resource mutation tool
        "write" => crate::mcp::handlers::files::handle_change(orch, request).await,
        "read" => crate::mcp::handlers::files::handle_read_file(orch, request).await,
        "read_batch" => {
            crate::mcp::handlers::read::handle_read_batch(orch, request, read_cursors).await
        }

        // Host process introspection: the answering server's own OS process id.
        // `cairn://dev/pid` relays this to a dev instance over its callback
        // channel to learn the instance's pid authoritatively (no `lsof`).
        "process_info" => std::process::id().to_string(),

        // Bash tools
        "run" => crate::mcp::handlers::bash::handle_run(orch, request).await,

        // Externally-driven attention long-poll (no polling)
        "watch" => crate::mcp::handlers::watch::handle_watch(orch, request).await,

        // Web + local-PDF reads (routed from `read` to the active web-fetch
        // provider and PDF service)
        "read_web" => crate::mcp::handlers::web::handle_read_web(orch, request).await,

        // Resource tools
        "read_resource" => {
            crate::mcp::handlers::resources::handle_read_resource(orch, request, read_cursors).await
        }
        "read_issue_resource" => {
            crate::mcp::handlers::issue_resources::handle_read_issue_resource(orch, request).await
        }

        _ => format!("Unknown tool: {}", request.tool),
    }
}

/// Read-family tools whose identical-content repeats within a turn are flail,
/// not work, and short-circuit to a stub.
///
/// `read_resource` is deliberately excluded: in the CLI/server routing it is
/// exclusively the cursor-based terminal-output poll, where an identical call
/// intentionally returns only the new bytes since the last read — the same
/// intentional-repeat pattern as `watch`. Terminal URIs that arrive via the
/// unified `read` tool route to the same cursor handler; they are dedup-eligible
/// but content-aware dedup leaves them effectively unaffected (a poll that
/// returns new bytes has different content and is never stubbed), so no target
/// type special-case is needed.
///
/// `watch` (a blocking attention long-poll), `write`, and `run` are likewise
/// never deduped — a write must never be silently dropped.
///
/// `read_batch` is deliberately excluded: collapsing dedup to batch granularity
/// would lose the per-target `[duplicate call]` stub. The batch handler re-applies
/// dedup per segment instead (see [`crate::mcp::handlers::read::batch`]).
fn is_read_family(tool: &str) -> bool {
    matches!(tool, "read" | "read_issue_resource" | "read_web")
}

/// Hash a tool result for content-aware dedup. A 64-bit hash keeps the per-turn
/// seen-set small; the worst case of a collision is one missed re-fetch (a stub
/// where fresh content was due), which the next turn corrects.
pub(crate) fn hash_content(content: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    content.hash(&mut hasher);
    hasher.finish()
}

/// Thresholds at which the duplicate stub appends a calming nudge. Conservative
/// to start; tune from the `flail_dedup` telemetry.
const NUDGE_COUNT: u32 = 3;
const NUDGE_DISTINCT: usize = 3;

/// Canonical JSON for a value with every object's keys recursively sorted.
///
/// Byte-identical payloads already produce the same string; sorting is cheap
/// insurance against object key-order variance between otherwise-equal calls.
fn canonical_json(value: &serde_json::Value) -> String {
    fn write_canonical(value: &serde_json::Value, out: &mut String) {
        match value {
            serde_json::Value::Object(map) => {
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                out.push('{');
                for (i, k) in keys.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str(&serde_json::Value::String((*k).clone()).to_string());
                    out.push(':');
                    write_canonical(&map[*k], out);
                }
                out.push('}');
            }
            serde_json::Value::Array(arr) => {
                out.push('[');
                for (i, v) in arr.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_canonical(v, out);
                }
                out.push(']');
            }
            other => out.push_str(&other.to_string()),
        }
    }
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

/// Collect a persistent clean-worktree reminder for worktree-bound agent runs
/// while the worktree has uncommitted changes. The body is pushed as data; the
/// CLI wraps it in the `<system-reminder>` envelope.
async fn augment_with_dirty_worktree_notice(
    orch: &crate::orchestrator::Orchestrator,
    request: &crate::mcp::types::McpCallbackRequest,
    reminders: &mut Vec<String>,
) {
    if !dirty_notice_applies_to_request(orch, request).await {
        return;
    }

    // Route the dirty check through the VCS seam so it is jj-aware: a `.jj`-only
    // workspace has no `.git`, so a raw `git status` would error and fail open,
    // never warning the agent about un-sealed `@` dirt.
    let cwd = std::path::Path::new(&request.cwd);
    let vcs = crate::mcp::vcs::resolve_worktree_vcs(orch, cwd);
    if let Ok(true) = vcs.is_dirty(cwd) {
        reminders.push(
            "The worktree has uncommitted changes. The tree must be clean between tool calls — commit them with a run/write commit_msg, or discard them. Uncommitted edits are lost if the worktree is cleaned up.".to_string(),
        );
    }
}

async fn dirty_notice_applies_to_request(
    orch: &crate::orchestrator::Orchestrator,
    request: &crate::mcp::types::McpCallbackRequest,
) -> bool {
    if let Some(run_id) = request.run_id.as_deref() {
        if let Some(applies) = dirty_notice_scope_cache()
            .lock()
            .ok()
            .and_then(|cache| cache.get(run_id).copied())
        {
            return applies;
        }

        let applies = matches!(
            crate::mcp::handlers::fence::resolve_run_fence(orch, request).await,
            Some((
                _run_id,
                crate::models::Fence::Ask | crate::models::Fence::Deny
            ))
        );
        if let Ok(mut cache) = dirty_notice_scope_cache().lock() {
            cache.insert(run_id.to_string(), applies);
        }
        return applies;
    }

    matches!(
        crate::mcp::handlers::fence::resolve_run_fence(orch, request).await,
        Some((
            _run_id,
            crate::models::Fence::Ask | crate::models::Fence::Deny
        ))
    )
}

fn dirty_notice_scope_cache() -> &'static std::sync::Mutex<std::collections::HashMap<String, bool>>
{
    static CACHE: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<String, bool>>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Stable key for a call: tool name plus the canonical JSON of the full
/// payload. Every query param (`?offset`, `?limit`, `?grep`, ...) is part of
/// the payload, so a different slice of the same target is a different call and
/// is never stubbed. This is strict call-identity, not a freshness check.
fn call_fingerprint(tool: &str, payload: &serde_json::Value) -> String {
    format!("{tool}\u{1}{}", canonical_json(payload))
}

/// Build the duplicate-call stub returned when a read-family call returned the
/// same content as its prior identical occurrence this turn.
///
/// Because dedup is now content-aware (CAIRN-1271), the stub truthfully reports
/// that the content is unchanged: the call *was* re-executed and its result
/// compared against the earlier one. On broad thrash a calming nudge is
/// appended.
pub(crate) fn build_dup_stub(
    tool: &str,
    target: &str,
    count: u32,
    distinct_dupes: usize,
) -> String {
    let mut stub = format!(
        "[duplicate call] You already made this exact `{tool}` call this turn (call #{count}): \
         `{target}`, and its content is unchanged since the earlier call — refer to that result. \
         If you need different information, read a different slice (?offset / ?limit / ?grep) or a \
         different target."
    );
    if count >= NUDGE_COUNT || distinct_dupes >= NUDGE_DISTINCT {
        stub.push_str(
            "\n\nRepeated identical calls detected — this usually signals a retry loop, not \
             progress. Slow down: re-read the result you already have, or change the query, \
             before calling again.",
        );
    }
    stub
}

/// Collect queued direct messages and child side-channel notices (one reminder
/// body per item) for a tool result. Atomically claims and stamps each item so
/// the Claude hook path can't double-deliver. Each pushed entry is the inner
/// reminder text; the CLI wraps it in the `<system-reminder>` envelope.
async fn augment_with_queued_dms(
    orch: &crate::orchestrator::Orchestrator,
    request: &crate::mcp::types::McpCallbackRequest,
    reminders: &mut Vec<String>,
) {
    let Some(run_id) = request.run_id.as_deref() else {
        return;
    };

    let job_id = crate::messages::side_channel::job_id_for_run(&orch.db.local, run_id).await;

    let side_channel_notices = match job_id.as_deref() {
        Some(job_id) => {
            match crate::messages::side_channel::claim_pending_side_channel_for_job_async(
                &orch.db.local,
                job_id,
            )
            .await
            {
                Ok(notices) => notices,
                Err(e) => {
                    log::warn!(
                        "Failed to claim pending side-channel notices for job {}: {}",
                        &job_id[..job_id.len().min(8)],
                        e
                    );
                    Vec::new()
                }
            }
        }
        None => Vec::new(),
    };

    // CAIRN-1309: the user's `steer` follow-ups are delivered at the tool
    // boundary, mid-turn. `queue` rows are intentionally left pending for the
    // turn-end/resume sweep in `continue_job_impl`.
    let queued_steer = match job_id.as_deref() {
        Some(job_id) => {
            match crate::messages::queued::claim_tool_boundary_for_job_async(&orch.db.local, job_id)
                .await
            {
                Ok(msgs) => msgs,
                Err(e) => {
                    log::warn!(
                        "Failed to claim steer queued messages for job {}: {}",
                        &job_id[..job_id.len().min(8)],
                        e
                    );
                    Vec::new()
                }
            }
        }
        None => Vec::new(),
    };

    // CAIRN-1881: drain this busy agent's rousing pushes at the event boundary —
    // the one new delivery site for `wake`/`interrupt` pushes that must land at
    // the next tool-call return rather than wait for turn end. The running job is
    // the recipient. Lazy-resolved so a push whose referent already resolved is
    // skipped; `passive` pushes are excluded here (they ride along on resume).
    let pushes = match job_id.as_deref() {
        Some(job_id) => crate::orchestrator::attention_push::pending_waking_live(
            &orch.db.local,
            job_id,
            crate::orchestrator::attention_push::Boundary::Event,
        )
        .await
        .unwrap_or_else(|e| {
            log::warn!(
                "Failed to drain attention pushes for job {}: {}",
                &job_id[..job_id.len().min(8)],
                e
            );
            Vec::new()
        }),
        None => Vec::new(),
    };

    if side_channel_notices.is_empty() && queued_steer.is_empty() && pushes.is_empty() {
        return;
    }

    // Record the same `system:message` transcript events the Claude hook
    // writes when it claims pending messages, so cross-agent communication shows
    // up in the transcript view whichever boundary delivered the message.
    let session_id =
        crate::messages::transcript::run_session_for_event(&orch.db.local, run_id).await;
    if !side_channel_notices.is_empty() {
        crate::messages::transcript::insert_side_channel_events(
            orch,
            run_id,
            session_id.as_deref(),
            &side_channel_notices,
        )
        .await;
    }
    if !queued_steer.is_empty() {
        crate::messages::transcript::insert_queued_user_events(
            orch,
            run_id,
            session_id.as_deref(),
            &queued_steer,
        )
        .await;
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "queued_messages", "action": "update"}),
        );
    }

    // Order within the queued-DM block: user follow-ups (steer), then directed
    // messages, then child side-channel notices. The CLI wraps each body in a
    // `<system-reminder>` envelope, reproducing the legacy text byte-for-byte.
    for msg in &queued_steer {
        reminders.push(format!(
            "The user sent a follow-up message:\n{}",
            msg.content
        ));
    }
    for notice in &side_channel_notices {
        reminders.push(notice.render());
    }

    // CAIRN-1881: persist a single carrying event for the drained pushes and
    // stamp each delivered by it (atomic with the event INSERT), then render each
    // into the reminders the CLI wraps in a `<system-reminder>` block.
    // CAIRN-1891: the agent-facing reminder carries the push's referent content
    // resolved inline (the PR summary/diff, plan, question, or permission) so it
    // acts without a round-trip read; the persisted carrying event keeps the lean
    // URI summary.
    if !pushes.is_empty() {
        // Resolve each push once: the same rendered content feeds both the
        // persisted carrying event (for the detail modal) and the agent reminder.
        let mut resolved_parts = Vec::with_capacity(pushes.len());
        for push in &pushes {
            resolved_parts.push(
                crate::orchestrator::attention_delivery::render_push_resolved(orch, push).await,
            );
        }
        let resolved = resolved_parts.join("\n\n");
        crate::messages::transcript::insert_attention_push_events(
            orch,
            run_id,
            session_id.as_deref(),
            &pushes,
            &resolved,
        )
        .await;
        for part in resolved_parts {
            reminders.push(part);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_read_family_covers_static_reads_excludes_terminal_mutations_and_watch() {
        assert!(is_read_family("read"));
        assert!(is_read_family("read_issue_resource"));
        assert!(is_read_family("read_web"));
        // read_resource is the cursor-based terminal poll path -> never deduped.
        assert!(!is_read_family("read_resource"));
        // Mutations and the blocking long-poll are never deduped.
        assert!(!is_read_family("write"));
        // Defensive: the legacy verb name should also be non-read-family if it
        // ever appears in a resumed mid-turn conversation.
        assert!(!is_read_family("change"));
        assert!(!is_read_family("run"));
        assert!(!is_read_family("watch"));
        // read_batch re-applies dedup per segment; the outer family must exclude it.
        assert!(!is_read_family("read_batch"));
    }

    #[test]
    fn hash_content_matches_identical_and_differs_on_change() {
        // Identical content hashes equal; changed content hashes differ. This is
        // the freshness signal content-aware dedup keys on.
        assert_eq!(hash_content("same body"), hash_content("same body"));
        assert_ne!(hash_content("before"), hash_content("after"));
    }

    #[test]
    fn call_fingerprint_is_key_order_independent() {
        let a = serde_json::json!({ "uri": "cairn://p/CAIRN/1", "full": true });
        let b = serde_json::json!({ "full": true, "uri": "cairn://p/CAIRN/1" });
        assert_eq!(
            call_fingerprint("read_resource", &a),
            call_fingerprint("read_resource", &b),
            "object key order must not change the fingerprint"
        );
    }

    #[test]
    fn canonical_json_sorts_nested_object_keys() {
        let a = serde_json::json!({ "b": { "y": 1, "x": 2 }, "a": 3 });
        let b = serde_json::json!({ "a": 3, "b": { "x": 2, "y": 1 } });
        assert_eq!(canonical_json(&a), canonical_json(&b));
    }

    #[test]
    fn call_fingerprint_distinguishes_tool_and_slice() {
        let p = serde_json::json!({ "path": "file:src/lib.rs" });
        // Different tool name -> different fingerprint.
        assert_ne!(
            call_fingerprint("read", &p),
            call_fingerprint("read_web", &p)
        );
        // A different slice of the same target is a different call, never stubbed.
        let s0 = serde_json::json!({ "path": "file:src/lib.rs?offset=0" });
        let s100 = serde_json::json!({ "path": "file:src/lib.rs?offset=100" });
        assert_ne!(
            call_fingerprint("read", &s0),
            call_fingerprint("read", &s100)
        );
    }

    #[test]
    fn build_dup_stub_is_content_unchanged_framed_with_target_and_count() {
        let stub = build_dup_stub("read", "cairn://p/CAIRN/1", 2, 1);
        assert!(stub.starts_with("[duplicate call]"));
        assert!(stub.contains("call #2"));
        assert!(stub.contains("cairn://p/CAIRN/1"));
        // Content-aware dedup re-executed and compared, so the stub truthfully
        // reports the content is unchanged since the earlier call.
        assert!(stub.to_lowercase().contains("unchanged"));
    }

    #[test]
    fn build_dup_stub_omits_nudge_below_threshold() {
        // count=2, distinct_dupes=1: below both thresholds -> no nudge.
        let stub = build_dup_stub("read", "x", 2, 1);
        assert!(!stub.contains("retry loop"));
    }

    #[test]
    fn build_dup_stub_appends_nudge_on_high_count() {
        let stub = build_dup_stub("read", "x", NUDGE_COUNT, 1);
        assert!(stub.contains("retry loop"));
    }

    #[test]
    fn build_dup_stub_appends_nudge_on_broad_thrash() {
        // Count below threshold, but many distinct fingerprints repeated.
        let stub = build_dup_stub("read", "x", 2, NUDGE_DISTINCT);
        assert!(stub.contains("retry loop"));
    }
}
