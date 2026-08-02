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

pub(crate) fn is_standalone_cli(request: &crate::mcp::types::McpCallbackRequest) -> bool {
    request
        .payload
        .get("_cairn_origin")
        .and_then(|value| value.as_str())
        == Some("cli")
}

/// `process_info` response when the windowless runner has no desktop connected.
/// This must never fall back to the runner's own pid: callers use the value to
/// target the native GUI, and the daemon cannot own that window.
pub(crate) const NO_ATTACHED_UI_PID: &str = "No desktop UI is attached";

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
    // Pooled Codex call (CAIRN-2549): one app-server hosts N call threads sharing
    // ONE `cairn-cmd` MCP subprocess, so the process-global `CAIRN_RUN_ID`
    // cannot attribute a tool call to the right run. Codex injects the
    // originating thread as `_meta.threadId`, which `cairn-cmd` forwards as
    // `thread_id`. Resolve it back to the owning run before any handler reads
    // `run_id`, so persistence and logical project operations use the correct
    // authenticated coordinate. Absent `thread_id` (every non-pooled caller)
    // leaves the request untouched.
    let resolved = resolve_pooled_thread_request(orch, request);
    let request = resolved.as_ref().unwrap_or(request);

    // Ownership fence (CAIRN-3287). Fenced AFTER pooled resolution so a pooled
    // Codex call is judged on the run it actually belongs to, and BEFORE any
    // handler runs so a refusal costs no side effects.
    if let Some(refusal) = refuse_disowned_run(orch, request).await {
        return refusal;
    }

    let content = dispatch_with_dedup(orch, request, read_cursors).await;
    let mut reminders = Vec::new();
    augment_with_queued_dms(orch, request, &mut reminders).await;
    augment_with_poll_nudge(orch, request, &mut reminders).await;
    DispatchOutput { content, reminders }
}

/// Override a pooled Codex call's `run_id` from its `thread_id` (CAIRN-2549).
///
/// Returns an owned request with `run_id` set to the pool-resolved run when the
/// request carries a `thread_id` that maps to a live pooled call; returns `None`
/// otherwise (the caller keeps the borrowed request unchanged). Fail-loud by
/// design: a `thread_id` that resolves REPLACES any stale env `run_id`; a
/// `thread_id` with no mapping is left as-is so the run lookup errors visibly
/// rather than silently attributing to the pool-spawner's run.
fn resolve_pooled_thread_request(
    orch: &crate::orchestrator::Orchestrator,
    request: &crate::mcp::types::McpCallbackRequest,
) -> Option<crate::mcp::types::McpCallbackRequest> {
    let thread_id = request.thread_id.as_deref()?;
    let binding = orch.codex_pool.binding_for_thread(thread_id)?;
    let mut resolved = request.clone();
    resolved.run_id = Some(binding.run_id);
    Some(resolved)
}

/// Refuse a tool call whose run this runner process does not own, before any
/// handler can produce a side effect (CAIRN-3287).
///
/// An agent process outlives the runner that spawned it: the child handle's drop
/// does not signal the OS process, so a runner that exits leaves its agents
/// reparented and running. Their tool calls keep arriving here with a bearer
/// token that is machine-global and stable across restarts, and the successor
/// runner used to serve them — for 38 minutes in the specimen incident, changing
/// files the whole time. Nothing recorded any of it, because transcript
/// ingestion is an in-process reader thread over the agent's stdout that died
/// with its runner. Serving such a call is therefore strictly worse than
/// refusing it: the work happens and leaves no record that it happened.
///
/// Two independent signals, both required before refusing, so a false positive
/// (which would block every tool call of a genuinely live agent) needs both to
/// be wrong at once:
///
/// 1. The run has no handle in this runner's process registry. That registry is
///    in-memory and starts empty at boot, so a run it holds was spawned here by
///    definition — and every serving run reaches the registry at spawn. This is
///    also the fast path: an owned run pays one mutex, no query.
/// 2. The run's own row disowns it — see [`disowned_reason`].
///
/// Fail-open everywhere else: a request with no run id, a run that resolves in
/// no database, or a database that cannot answer all dispatch normally, leaving
/// each handler's existing "No run found with id" behavior alone. The fence adds
/// a refusal for runs positively identified as unowned; it does not become a
/// second gate on run resolution.
async fn refuse_disowned_run(
    orch: &crate::orchestrator::Orchestrator,
    request: &crate::mcp::types::McpCallbackRequest,
) -> Option<DispatchOutput> {
    let run_id = request
        .run_id
        .as_deref()
        .filter(|run_id| !run_id.is_empty())?;

    if orch
        .process_state
        .processes
        .lock()
        .is_ok_and(|processes| processes.contains_key(run_id))
    {
        return None;
    }

    // Route the lookup the way every other run-scoped read routes it: a team
    // replica's run must resolve against that replica rather than missing in the
    // private database and falling through unfenced.
    let db = crate::execution::routing::routing_db_for_id(&orch.db, run_id)
        .await
        .ok()?;
    let reason = disowned_reason(&load_run_ownership(&db, run_id).await?, orch.boot_at)?;

    // An operator seeing this line is seeing a zombie run — the signal that was
    // entirely absent from the incident's logs.
    log::warn!(
        "run_ownership_fence: refused tool={} run={run_id} reason={reason:?} boot_at={}",
        request.tool,
        orch.boot_at
    );
    Some(DispatchOutput {
        content: reason.refusal(),
        reminders: Vec::new(),
    })
}

/// The `runs` columns that decide whether this runner owns a run.
#[derive(Debug, Clone)]
struct RunOwnership {
    started_at: Option<i64>,
    created_at: i64,
    status: Option<String>,
}

/// Why a run is not this runner process's to serve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Disowned {
    /// Spawned before this runner booted, so its owner was a predecessor process
    /// — which took the transcript reader with it when it exited.
    PredatesBoot,
    /// Already finalized: whatever is still calling tools for it has outlived
    /// the bookkeeping that would record what it does.
    Finalized,
}

impl Disowned {
    /// What the agent reads instead of a tool result.
    ///
    /// [`DispatchOutput`] has no error channel by design, so a refusal is
    /// content. Phrased to make a model stop rather than retry: it names the
    /// terminal fact (nothing is recording this run), rules out retrying, and
    /// names what would work instead. Modelled on the closed-session refusal in
    /// `execution::jobs::lifecycle`.
    fn refusal(self) -> String {
        let cause = match self {
            Disowned::PredatesBoot => {
                "The Cairn runner process that started this session has exited"
            }
            Disowned::Finalized => "This run has already been finalized",
        };
        format!(
            "This run is no longer served. {cause}, so nothing is recording this session's \
             transcript: any file this call changed would be changed with no record that it \
             happened. Stop here. Retrying will not help and no further tool call for this run \
             will be served. The work carries forward in a new execution on the issue, which \
             starts a fresh session whose transcript is intact."
        )
    }
}

/// Whether a run's own row disowns it, given the host's boot time.
///
/// The pre-boot test is [`crate::runs::ownership::predates_host_boot`], the single
/// definition of that boundary — startup reconciliation selects its candidates
/// with the same predicate, so the fence and the sweep cannot disagree about which
/// runs a predecessor left behind.
fn disowned_reason(ownership: &RunOwnership, boot_at: i64) -> Option<Disowned> {
    if crate::runs::ownership::predates_host_boot(
        ownership.started_at,
        ownership.created_at,
        boot_at,
    ) {
        return Some(Disowned::PredatesBoot);
    }
    // Defense in depth for the window before startup reconciliation has swept —
    // it took 15s in the specimen incident while the callback was already being
    // served. Every path that marks a run terminal either kills its process
    // first (`lifecycle::kill_session_with_reason`, `finalize_run` on exit) or is
    // reconciling a run whose process is already unregistered, so a terminal run
    // still calling tools is an anomaly by construction. An absent or
    // unparseable status is not terminal, and fails open.
    ownership
        .status
        .as_deref()
        .and_then(|status| status.parse::<crate::models::RunStatus>().ok())
        .filter(|status| status.is_terminal())
        .map(|_| Disowned::Finalized)
}

/// Read one run's ownership columns. `None` when the row is absent or the read
/// fails, both of which leave the call unfenced.
async fn load_run_ownership(db: &crate::storage::LocalDb, run_id: &str) -> Option<RunOwnership> {
    use crate::storage::RowExt;
    let run_id = run_id.to_string();
    db.read(|conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT started_at, created_at, status FROM runs WHERE id = ?1 LIMIT 1",
                    cairn_db::turso::params![run_id.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            Ok(Some(RunOwnership {
                started_at: row.opt_i64(0)?,
                created_at: row.i64(1)?,
                status: row.opt_text(2)?,
            }))
        })
    })
    .await
    .ok()
    .flatten()
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

    // The duplicate stub is guidance for an agent that already holds the prior
    // body in context. Standalone CLI stdout may be consumed by a polling program,
    // so it must always receive the real body.
    if is_standalone_cli(request) || !is_read_family(&request.tool) {
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
        "write" => crate::mcp::handlers::write::handle_write(orch, request).await,
        "read" => crate::mcp::handlers::read::handle_read_file(orch, request).await,
        "read_batch" => {
            crate::mcp::handlers::read::handle_read_batch(orch, request, read_cursors).await
        }

        // Host process introspection for `cairn://dev/pid`, which relays this to
        // a dev instance so an external tool (e.g. Axon accessibility) can target
        // the instance's window. Since the runner-daemon split the callback is
        // served by the windowless `cairn-runner`, so return the desktop UI pid
        // it registered over `/ws` when a window is attached. A runner pid is
        // never a valid substitute because the daemon owns no native window.
        "process_info" => orch
            .attached_ui_pid()
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| NO_ATTACHED_UI_PID.to_string()),

        // Run tools. `check_run` is the trusted configured-suite producer; unlike
        // `run`, its payload cannot carry executable text or asserted evidence.
        "run" => crate::mcp::handlers::run::handle_run(orch, request).await,
        "check_run" => crate::mcp::handlers::check_run::handle_check_run(orch, request).await,

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

/// Reads of the same terminal within one turn before nudging the agent to set a
/// wake instead of polling. Re-fires every multiple thereafter (the 3rd, 6th,
/// 9th… read). Conservative to start; tune from the `terminal_poll_nudge`
/// telemetry, mirroring [`NUDGE_COUNT`].
const TERMINAL_POLL_NUDGE_COUNT: u32 = 3;

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

/// Nudge an agent that is busy-polling a still-moving resource -- a running
/// terminal, or a node's unsettled check lanes -- toward waiting on it instead.
///
/// A terminal read can never trip the content-aware flail dedup: its status
/// banner carries `elapsed`/`last output ... ago` fields that advance on every
/// poll (and an actively compiling terminal's buffer changes too), so the
/// content hash differs every call and the `Duplicate` branch never fires. The
/// only reliable busy-poll signal is therefore a content-independent *call
/// count* — "this agent read this terminal N times this turn" — tracked per turn
/// via [`crate::agent_process::process::ProcessState::record_repeat`].
///
/// A poll loop is a single turn (an agent that reads a still-running terminal and
/// *ends* its turn goes idle, and with no wake nothing resumes it), so the
/// per-turn grain is exactly right. The nudge rides [`DispatchOutput::reminders`]
/// as a `<system-reminder>`, leaving the structured read envelope untouched, and
/// never blocks the read: the agent always gets the live terminal output plus the
/// reminder, so a single confirming read is never penalized.
async fn augment_with_poll_nudge(
    orch: &crate::orchestrator::Orchestrator,
    request: &crate::mcp::types::McpCallbackRequest,
    reminders: &mut Vec<String>,
) {
    let Some(run_id) = request.run_id.as_deref() else {
        return;
    };
    // Collect targets from the read-batch (`paths`) and single-read
    // (`uri`/`path`) payload shapes; a call naming neither kind costs two
    // prefix scans and nothing else.
    let targets = collect_terminal_targets(&request.payload);
    augment_with_checks_poll_nudge(orch, run_id, &request.payload, reminders).await;

    for (identity, slug) in targets {
        // Content-independent: key on the query-stripped identity so `?new=true`
        // and a plain read of the same terminal count as one target.
        let fingerprint = format!("terminal_poll\u{1}{identity}");
        let count = orch.process_state.record_repeat(run_id, &fingerprint);
        // Fire on the Nth read and every multiple thereafter (3rd, 6th, 9th…),
        // not on every read past the threshold.
        if count < TERMINAL_POLL_NUDGE_COUNT || !count.is_multiple_of(TERMINAL_POLL_NUDGE_COUNT) {
            continue;
        }
        // The agent that already subscribed a wake on this terminal is doing the
        // right thing (a single confirming read) -> do not nudge.
        if terminal_wake_active(orch, run_id, &slug).await {
            continue;
        }
        log::info!("terminal_poll_nudge: run={run_id} slug={slug} count={count}");
        reminders.push(build_terminal_poll_nudge(&slug, count));
    }
}

/// Nudge an agent that is busy-polling a node's check lanes toward waiting on
/// them instead.
///
/// The same call-count signal the terminal nudge uses, and for the same reason:
/// a `/checks` render carries a running lane's live log tail, so its content
/// changes on every poll and no content-equality dedup can catch the loop. This
/// is the read the specimen in CAIRN-3437 was stuck in.
async fn augment_with_checks_poll_nudge(
    orch: &crate::orchestrator::Orchestrator,
    run_id: &str,
    payload: &serde_json::Value,
    reminders: &mut Vec<String>,
) {
    let targets = collect_checks_targets(payload);
    if targets.is_empty() {
        return;
    }
    let Some(job_id) = crate::messages::side_channel::job_id_for_run(&orch.db.local, run_id).await
    else {
        return;
    };
    let home = crate::jobs::queries::home_uri_for_job(&orch.db.local, &job_id)
        .await
        .ok()
        .flatten();
    for identity in targets {
        let fingerprint = format!("checks_poll\u{1}{identity}");
        let count = orch.process_state.record_repeat(run_id, &fingerprint);
        if count < TERMINAL_POLL_NUDGE_COUNT || !count.is_multiple_of(TERMINAL_POLL_NUDGE_COUNT) {
            continue;
        }
        let Ok(coords) =
            crate::execution::checks_settlement::checks_coords(&identity, home.as_deref())
        else {
            continue;
        };
        // Already waiting on this node the right way -> a confirming read is
        // not a poll loop.
        if checks_wake_active(orch, &job_id, &coords.canonical).await {
            continue;
        }
        // A node reading its OWN lanes cannot wait on them inline: the wave is
        // armed by its turn ENDING. The wake is the only shape that serves it,
        // so that is the only one offered.
        let own = home
            .as_deref()
            .is_some_and(|home| coords.canonical == format!("{home}/checks"));
        log::info!(
            "checks_poll_nudge: run={run_id} checks={} count={count}",
            coords.canonical
        );
        reminders.push(build_checks_poll_nudge(&coords.canonical, own, count));
    }
}

/// Whether the agent's job already holds an active settled-checks subscription
/// on this node. Fail-open, like [`terminal_wake_active`]: the only failure
/// direction is a nudge that fires when it need not have.
async fn checks_wake_active(
    orch: &crate::orchestrator::Orchestrator,
    job_id: &str,
    checks_uri: &str,
) -> bool {
    let (job_id, checks_uri) = (job_id.to_string(), checks_uri.to_string());
    orch.db
        .local
        .read(move |conn| {
            let (job_id, checks_uri) = (job_id.clone(), checks_uri.clone());
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT 1 FROM wake_subscriptions
                         WHERE job_id = ?1 AND state = 'active'
                           AND source_kind = 'condition' AND source_ref = ?2
                         LIMIT 1",
                        cairn_db::turso::params![job_id.as_str(), checks_uri.as_str()],
                    )
                    .await?;
                Ok(rows.next().await?.is_some())
            })
        })
        .await
        .unwrap_or(false)
}

/// Build the checks-poll nudge: name the node and the poll count, then give the
/// copy-pasteable call that actually waits.
fn build_checks_poll_nudge(checks_uri: &str, own: bool, count: u32) -> String {
    let head = format!(
        "You've read `{checks_uri}` {count} times this turn and its lanes haven't settled. Polling spends the current turn waiting, and re-reading the same URI can also be suppressed as a duplicate call."
    );
    if own {
        return format!(
            "{head}\n\nThese are your OWN lanes, and your turn-end wave is armed by this turn ENDING \u{2014} so no inline wait can ever see it settle. Subscribe and end your turn:\n  write({{changes:[{{target:\"cairn:~/wakes\",mode:\"append\",payload:{{subscribe:{{kind:\"checks\"}}}}}}]}})"
        );
    }
    format!(
        "{head}\n\nWait for every lane to settle:\n  run({{commands:[{{waitFor:{{kind:\"checks\",ref:\"{checks_uri}\",on:\"settled\"}}}}]}})\n\nWait for one suite:\n  run({{commands:[{{waitFor:{{kind:\"checks\",ref:\"{checks_uri}\",on:\"verdict\",suite:\"<name>\"}}}}]}})\n\nUse `cairn:~/wakes` ({{subscribe:{{kind:\"checks\",ref:\"{checks_uri}\"}}}}) only if your turn is otherwise complete and you want a later notification. Do not keep reading the checks resource."
    )
}

/// Every read target named by a tool payload, across the read-batch (`paths`)
/// and single-read (`uri`/`path`) shapes.
fn read_targets(payload: &serde_json::Value) -> Vec<String> {
    let mut raw: Vec<String> = Vec::new();
    if let Some(paths) = payload.get("paths").and_then(|value| value.as_array()) {
        raw.extend(
            paths
                .iter()
                .filter_map(|entry| entry.as_str().map(str::to_string)),
        );
    }
    for key in ["uri", "path"] {
        if let Some(target) = payload.get(key).and_then(|value| value.as_str()) {
            raw.push(target.to_string());
        }
    }
    raw
}

/// Collect distinct checks read targets from a tool payload, query-stripped.
///
/// Deliberately syntactic: the coordinates are only resolved once a target has
/// actually crossed the poll threshold, so the hot path costs a prefix test.
fn collect_checks_targets(payload: &serde_json::Value) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for target in read_targets(payload) {
        let identity = target.split('?').next().unwrap_or(&target).trim();
        let names_checks = identity == "cairn:~/checks"
            || matches!(
                cairn_common::uri::parse_uri(identity),
                Some(
                    cairn_common::uri::CairnResource::NodeChecks { .. }
                        | cairn_common::uri::CairnResource::TaskChecks { .. }
                )
            );
        if names_checks && !out.iter().any(|existing| existing == identity) {
            out.push(identity.to_string());
        }
    }
    out
}

/// Collect distinct terminal read targets from a tool payload as
/// `(query-stripped identity, slug)` pairs. Any non-terminal target is skipped.
fn collect_terminal_targets(payload: &serde_json::Value) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for target in read_targets(payload) {
        if let Some((identity, slug)) = terminal_identity_and_slug(&target) {
            // The same terminal listed twice in one call is one read of it.
            if !out.iter().any(|(existing, _)| existing == &identity) {
                out.push((identity, slug));
            }
        }
    }
    out
}

/// Resolve a read target to its `(query-stripped identity, terminal slug)` when
/// it names a terminal, else `None`.
///
/// Handles both the agent-facing home-relative `cairn:~/terminal/<slug>` form —
/// which `parse_uri` does **not** resolve, yet is exactly what agents type — and
/// a canonical `.../terminal/<slug>` URI (node, task, or project terminal).
fn terminal_identity_and_slug(target: &str) -> Option<(String, String)> {
    let identity = target.split('?').next().unwrap_or(target).trim();
    if identity.is_empty() {
        return None;
    }
    // Home-relative `cairn:~/terminal/<slug>` (exactly one trailing segment).
    if let Some(rest) = identity.strip_prefix("cairn:~/terminal/") {
        if rest.is_empty() || rest.contains('/') {
            return None;
        }
        return Some((identity.to_string(), rest.to_string()));
    }
    // Canonical node/task/project terminal URIs.
    match cairn_common::uri::parse_uri(identity) {
        Some(
            cairn_common::uri::CairnResource::NodeTerminal { slug, .. }
            | cairn_common::uri::CairnResource::TaskTerminal { slug, .. }
            | cairn_common::uri::CairnResource::ProjectTerminal { slug, .. },
        ) => Some((identity.to_string(), slug)),
        _ => None,
    }
}

/// Whether the agent's job already holds an active wake subscription on this
/// terminal. A best-effort, fail-open check: a slug-suffix match against the
/// `process`-kind subscriptions avoids re-deriving the full canonical URI while
/// staying specific (subscriptions store the canonical `.../terminal/<slug>` URI
/// as `source_ref`). Any query error or unresolved job returns `false` so the
/// nudge still fires rather than being silently swallowed.
async fn terminal_wake_active(
    orch: &crate::orchestrator::Orchestrator,
    run_id: &str,
    slug: &str,
) -> bool {
    let Some(job_id) = crate::messages::side_channel::job_id_for_run(&orch.db.local, run_id).await
    else {
        return false;
    };
    // Escape LIKE metacharacters in the slug so a slug containing `_` or `%`
    // (e.g. `my_dev`) matches literally rather than as a wildcard — a raw slug
    // here could otherwise suppress the nudge for a *different* terminal whose
    // slug differs only at that position. (Two same-slug terminals across nodes
    // in one job still cross-suppress via this suffix match; that is acceptable:
    // the only failure direction is a missed nudge, never a wrong one, and it
    // avoids re-deriving the full canonical URI here.)
    let escaped = slug
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    let pattern = format!("%/terminal/{escaped}");
    orch.db
        .local
        .read(move |conn| {
            let job_id = job_id.clone();
            let pattern = pattern.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT 1 FROM wake_subscriptions
                         WHERE job_id = ?1 AND state = 'active'
                           AND source_kind = 'process'
                           AND source_ref LIKE ?2 ESCAPE '\\'
                         LIMIT 1",
                        cairn_db::turso::params![job_id.as_str(), pattern.as_str()],
                    )
                    .await?;
                Ok(rows.next().await?.is_some())
            })
        })
        .await
        .unwrap_or(false)
}

/// Build the terminal-poll nudge: name the terminal and the poll count, then give
/// copy-pasteable subscribe calls for both the finishing-command (exit) and
/// never-exiting-process (output phrase) cases. Uses the agent-facing
/// `cairn:~/terminal/<slug>` form so the suggested calls paste directly.
fn build_terminal_poll_nudge(slug: &str, count: u32) -> String {
    format!(
        "You've read `cairn:~/terminal/{slug}` {count} times this turn and it hasn't finished. Polling spends the current turn waiting.\n\nWait for exit:\n  run({{commands:[{{waitFor:{{kind:\"terminal\",ref:\"cairn:~/terminal/{slug}\",on:\"exit\"}}}}]}})\n\nWait for readiness output:\n  run({{commands:[{{waitFor:{{kind:\"terminal\",ref:\"cairn:~/terminal/{slug}\",on:\"output\",phrase:\"<ready line>\"}}}}]}})\n\nUse `cairn:~/wakes` only if your turn is otherwise complete and you want a later notification. Do not keep reading the terminal."
    )
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

    // Drain every live push for this busy agent at the event boundary, regardless
    // of wake level. The running job is the recipient. Wake level governs whether
    // an *idle* agent is roused, not whether an active one sees a push: `passive`
    // pushes never *woke* this agent (it is already running), but they ride along
    // at the next tool-call return just like rousing pushes. Lazy-resolved so a
    // push whose referent already resolved is skipped.
    let pushes = match job_id.as_deref() {
        Some(job_id) => crate::orchestrator::attention_push::pending_deliverable_live(
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
    use crate::mcp::types::McpCallbackRequest;

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

    fn ownership(started_at: Option<i64>, created_at: i64, status: &str) -> RunOwnership {
        RunOwnership {
            started_at,
            created_at,
            status: Some(status.to_string()),
        }
    }

    #[test]
    fn disowned_reason_refuses_a_run_spawned_before_boot() {
        assert_eq!(
            disowned_reason(&ownership(Some(100), 99, "live"), 200),
            Some(Disowned::PredatesBoot)
        );
    }

    #[test]
    fn disowned_reason_serves_a_run_spawned_during_the_boot_second() {
        // Seconds granularity: `started_at == boot_at` is a run this runner
        // spawned, so the comparison must be strict `<` and never `<=`.
        assert_eq!(
            disowned_reason(&ownership(Some(200), 199, "live"), 200),
            None
        );
        assert_eq!(
            disowned_reason(&ownership(Some(201), 200, "live"), 200),
            None
        );
    }

    #[test]
    fn disowned_reason_falls_back_to_created_at_when_started_at_is_null() {
        // A run still `starting` has no `started_at` yet; `created_at` decides.
        assert_eq!(
            disowned_reason(&ownership(None, 100, "starting"), 200),
            Some(Disowned::PredatesBoot)
        );
        assert_eq!(
            disowned_reason(&ownership(None, 250, "starting"), 200),
            None
        );
    }

    #[test]
    fn disowned_reason_refuses_a_post_boot_run_that_is_already_finalized() {
        for status in ["exited", "crashed"] {
            assert_eq!(
                disowned_reason(&ownership(Some(250), 250, status), 200),
                Some(Disowned::Finalized),
                "{status} is terminal"
            );
        }
        for status in ["starting", "live"] {
            assert_eq!(
                disowned_reason(&ownership(Some(250), 250, status), 200),
                None,
                "{status} is a run still doing work"
            );
        }
    }

    #[test]
    fn disowned_reason_fails_open_on_an_unreadable_status() {
        // An absent or unrecognized status says nothing about liveness, and the
        // fence must never refuse on a guess.
        for status in [None, Some("running".to_string()), Some(String::new())] {
            assert_eq!(
                disowned_reason(
                    &RunOwnership {
                        started_at: Some(250),
                        created_at: 250,
                        status,
                    },
                    200
                ),
                None
            );
        }
    }

    #[test]
    fn every_refusal_tells_the_agent_to_stop_rather_than_retry() {
        for reason in [Disowned::PredatesBoot, Disowned::Finalized] {
            let refusal = reason.refusal();
            assert!(refusal.contains("no longer served"), "{refusal}");
            assert!(refusal.contains("Stop here"), "{refusal}");
            assert!(refusal.contains("Retrying will not help"), "{refusal}");
            assert!(refusal.contains("new execution"), "{refusal}");
        }
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

    #[test]
    fn terminal_identity_and_slug_handles_home_relative_and_canonical() {
        // The agent-facing home-relative form (what agents actually type) is
        // recognized even though `parse_uri` does not resolve it.
        assert_eq!(
            terminal_identity_and_slug("cairn:~/terminal/run-1"),
            Some(("cairn:~/terminal/run-1".to_string(), "run-1".to_string()))
        );
        // Query is stripped from the identity; slug preserved.
        assert_eq!(
            terminal_identity_and_slug("cairn:~/terminal/dev?new=true"),
            Some(("cairn:~/terminal/dev".to_string(), "dev".to_string()))
        );
        // Canonical node terminal URI.
        assert_eq!(
            terminal_identity_and_slug("cairn://p/CAIRN/1/1/builder/terminal/build"),
            Some((
                "cairn://p/CAIRN/1/1/builder/terminal/build".to_string(),
                "build".to_string()
            ))
        );
        // Non-terminal targets and deeper home-relative shapes are rejected.
        assert_eq!(terminal_identity_and_slug("file:src/lib.rs"), None);
        assert_eq!(terminal_identity_and_slug("cairn://p/CAIRN/1"), None);
        assert_eq!(terminal_identity_and_slug("cairn:~/terminal/a/b"), None);
        assert_eq!(terminal_identity_and_slug("cairn:~/todos"), None);
    }

    /// Build an orchestrator over a fresh migrated db with one tracked process
    /// serving `turn_id`, the minimal state the nudge augmentation reads.
    async fn orch_with_tracked_turn(
        run_id: &str,
        turn_id: &str,
    ) -> crate::orchestrator::Orchestrator {
        use crate::db::DbState;
        use crate::orchestrator::Orchestrator;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;
        use std::sync::Arc;

        let db = crate::storage::migrated_test_db("dispatch_terminal_nudge.db").await;
        let dir = tempfile::tempdir().unwrap().keep();
        let index = Arc::new(SearchIndex::open_or_create(dir.join("idx.db")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), index));
        let orch =
            Orchestrator::builder(db_state, Arc::new(TestServicesBuilder::new().build()), dir)
                .build();
        orch.process_state.processes.lock().unwrap().register(
            run_id.to_string(),
            crate::agent_process::process::RunHandle::test_handle(Some("s"), Some("j")),
        );
        orch.process_state
            .set_current_turn_id(run_id, Some(turn_id));
        orch
    }

    fn terminal_read_request(run_id: &str, paths: serde_json::Value) -> McpCallbackRequest {
        McpCallbackRequest {
            thread_id: None,
            cwd: "/tmp".to_string(),
            run_id: Some(run_id.to_string()),
            tool: "read_batch".to_string(),
            payload: serde_json::json!({ "paths": paths }),
            tool_use_id: None,
        }
    }

    async fn nudge_for(
        orch: &crate::orchestrator::Orchestrator,
        request: &McpCallbackRequest,
    ) -> Vec<String> {
        let mut reminders = Vec::new();
        augment_with_poll_nudge(orch, request, &mut reminders).await;
        reminders
    }

    #[tokio::test]
    async fn terminal_poll_nudge_fires_on_third_read_not_before() {
        let orch = orch_with_tracked_turn("run-1", "turn-1").await;
        let req = terminal_read_request("run-1", serde_json::json!(["cairn:~/terminal/run-1"]));

        assert!(nudge_for(&orch, &req).await.is_empty(), "read 1");
        assert!(nudge_for(&orch, &req).await.is_empty(), "read 2");
        let fired = nudge_for(&orch, &req).await;
        assert_eq!(fired.len(), 1, "read 3 should nudge");
        let body = &fired[0];
        assert!(body.contains("waitFor"));
        assert!(body.contains("on:\"exit\""));
        assert!(body.contains("cairn:~/terminal/run-1"));
    }

    #[tokio::test]
    async fn no_nudge_for_single_read_or_distinct_terminals() {
        let orch = orch_with_tracked_turn("run-1", "turn-1").await;
        let a = nudge_for(
            &orch,
            &terminal_read_request("run-1", serde_json::json!(["cairn:~/terminal/a"])),
        )
        .await;
        let b = nudge_for(
            &orch,
            &terminal_read_request("run-1", serde_json::json!(["cairn:~/terminal/b"])),
        )
        .await;
        assert!(a.is_empty() && b.is_empty());
    }

    #[tokio::test]
    async fn no_terminal_nudge_for_non_terminal_reads() {
        let orch = orch_with_tracked_turn("run-1", "turn-1").await;
        let req = terminal_read_request(
            "run-1",
            serde_json::json!(["file:src/lib.rs", "cairn://p/CAIRN/1"]),
        );
        for _ in 0..5 {
            assert!(nudge_for(&orch, &req).await.is_empty());
        }
    }

    #[tokio::test]
    async fn terminal_poll_nudge_refires_on_every_third_read() {
        let orch = orch_with_tracked_turn("run-1", "turn-1").await;
        let req = terminal_read_request("run-1", serde_json::json!(["cairn:~/terminal/dev"]));
        let mut fired = Vec::new();
        for _ in 1..=6 {
            fired.push(!nudge_for(&orch, &req).await.is_empty());
        }
        // Fires on the 3rd and 6th read, not on 1, 2, 4, or 5.
        assert_eq!(fired, vec![false, false, true, false, false, true]);
    }

    #[tokio::test]
    async fn active_subscription_suppresses_terminal_poll_nudge() {
        let orch = orch_with_tracked_turn("run-1", "turn-1").await;
        // job_id_for_run reads the `runs` table; seed the parent chain plus a run
        // mapping run-1 -> job j, then an active terminal-exit subscription whose
        // canonical source_ref ends with /terminal/dev.
        orch.db
            .local
            .execute_script(
                "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
                 INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES('p','w','P','P','/tmp',1,1);
                 INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES('i','p',1,'I','active','active','none',1,1);
                 INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at) VALUES('j','p','i','running','s',1,1);
                 INSERT INTO runs(id, project_id, issue_id, job_id, status, created_at, updated_at) VALUES('run-1','p','i','j','running',1,1);",
            )
            .await
            .unwrap();
        crate::orchestrator::wakes::subscribe_one_shot(
            &orch.db.local,
            "j",
            "process",
            Some("cairn://p/P/1/1/builder/terminal/dev"),
            Some(&["terminal_exit".to_string()]),
            "agent",
        )
        .await
        .unwrap();

        let req = terminal_read_request("run-1", serde_json::json!(["cairn:~/terminal/dev"]));
        for _ in 0..3 {
            assert!(
                nudge_for(&orch, &req).await.is_empty(),
                "an active wake on the terminal should suppress the nudge"
            );
        }
    }

    #[tokio::test]
    async fn underscore_slug_does_not_cross_match_via_like_wildcard() {
        // A subscription on terminal `axb` must NOT suppress the nudge for a
        // *different* terminal `a_b`: the `_` is a LIKE wildcard and would match
        // `axb` unless it is escaped. Regression test for the suppression query.
        let orch = orch_with_tracked_turn("run-1", "turn-1").await;
        orch.db
            .local
            .execute_script(
                "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
                 INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES('p','w','P','P','/tmp',1,1);
                 INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at) VALUES('i','p',1,'I','active','active','none',1,1);
                 INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at) VALUES('j','p','i','running','s',1,1);
                 INSERT INTO runs(id, project_id, issue_id, job_id, status, created_at, updated_at) VALUES('run-1','p','i','j','running',1,1);",
            )
            .await
            .unwrap();
        // Active subscription is on `axb`, not `a_b`.
        crate::orchestrator::wakes::subscribe_one_shot(
            &orch.db.local,
            "j",
            "process",
            Some("cairn://p/P/1/1/builder/terminal/axb"),
            Some(&["terminal_exit".to_string()]),
            "agent",
        )
        .await
        .unwrap();

        let req = terminal_read_request("run-1", serde_json::json!(["cairn:~/terminal/a_b"]));
        assert!(nudge_for(&orch, &req).await.is_empty(), "read 1");
        assert!(nudge_for(&orch, &req).await.is_empty(), "read 2");
        // The `axb` subscription must not match `a_b`, so the nudge still fires.
        assert_eq!(
            nudge_for(&orch, &req).await.len(),
            1,
            "an unrelated same-shape slug must not suppress via a LIKE wildcard"
        );
    }
}
