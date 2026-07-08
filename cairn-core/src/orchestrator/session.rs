//! Session management (configuration resolution, agent dispatch).
//!
//! This module handles resolving session configuration (tools, model, prompt,
//! MCP config) from agent configs and database state, then delegates to an
//! `AgentBackend` for process spawning and event streaming.
//!
//! All functions take `&Orchestrator` instead of framework-specific handles.

use crate::agent_process::stream::{ClaudeEvent, TranscriptEvent};
use crate::backends::{self, SessionConfig, SessionStart};
use crate::models::Model;

use crate::storage::{run_db_blocking, DbError, DbResult, LocalDb, RowExt};
use cairn_common::ids;
use cairn_db::turso::params;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;

use super::Orchestrator;

fn sha256_hex(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

/// Insert the assembled system prompt once per session/content hash so the UI
/// can display the exact prompt without re-running prompt construction code.
///
/// Returns the next transcript sequence the backend reader should use. Even when
/// the prompt event dedupes, this returns `MAX(sequence) + 1` so resumed streams
/// append after the existing transcript instead of restarting at zero.
pub fn persist_system_prompt_event(
    orch: &Orchestrator,
    run_id: &str,
    session_id: Option<&str>,
    backend: &str,
    segments: &[PromptSegment],
) -> i32 {
    // Concatenate the segments into the full prompt and record their byte spans as
    // data on the event. Teardown archival uses the spans to content-address the
    // static segments and inline only the dynamic tail, never re-running assembly.
    let mut full_prompt = String::new();
    let mut segment_map: Vec<serde_json::Value> = Vec::with_capacity(segments.len());
    for seg in segments {
        let byte_offset = full_prompt.len();
        full_prompt.push_str(&seg.text);
        segment_map.push(serde_json::json!({
            "kind": seg.kind,
            "byteOffset": byte_offset,
            "byteLen": seg.text.len(),
        }));
    }

    let hash = sha256_hex(&full_prompt);
    let now = chrono::Utc::now().timestamp() as i32;
    let event_id = ids::mint_child(run_id);
    let run_id_owned = run_id.to_string();
    let session_id_owned = session_id.map(|s| s.to_string());
    let full_prompt_owned = full_prompt.clone();

    let transcript_event = TranscriptEvent {
        event_type: "system:prompt".to_string(),
        session_id: session_id_owned.clone(),
        parent_tool_use_id: None,
        content: Some(full_prompt_owned.clone()),
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id: None,
        tool_result: None,
        is_error: false,
        thinking_ms: None,
        raw: Some(serde_json::json!({
            "backend": backend,
            "bytes": full_prompt.len(),
            "hash": hash,
            "segments": segment_map,
        })),
    };

    let data = serde_json::to_string(&transcript_event).unwrap_or_default();
    let insert_result = run_db_blocking({
        let dbs = orch.db.clone();
        let event_id = event_id.clone();
        let run_id = run_id_owned.clone();
        let session_id = session_id_owned.clone();
        let data = data.clone();
        let hash = hash.clone();
        move || async move {
            let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|e| e.to_string())?;
            db.write(|conn| {
                let event_id = event_id.clone();
                let run_id = run_id.clone();
                let session_id = session_id.clone();
                let data = data.clone();
                let hash = hash.clone();
                Box::pin(async move {
                    // Live-only reader (no archival reconstruction): session
                    // setup reads the active session's own `system:prompt`
                    // events while it is still running, before teardown makes
                    // anything archivable. It never sees a gitcoord/zstd stub.
                    let latest_data = if let Some(ref session_id) = session_id {
                        let mut rows = conn
                            .query(
                                "SELECT data
                                 FROM events
                                 WHERE event_type = 'system:prompt'
                                   AND session_id = ?1
                                 ORDER BY sequence DESC
                                 LIMIT 1",
                                (session_id.as_str(),),
                            )
                            .await?;
                        crate::storage::next_text(&mut rows, 0).await?
                    } else {
                        let mut rows = conn
                            .query(
                                "SELECT data
                                 FROM events
                                 WHERE event_type = 'system:prompt'
                                   AND run_id = ?1
                                   AND session_id IS NULL
                                 ORDER BY sequence DESC
                                 LIMIT 1",
                                (run_id.as_str(),),
                            )
                            .await?;
                        crate::storage::next_text(&mut rows, 0).await?
                    };

                    let mut rows = conn
                        .query(
                            "SELECT MAX(sequence)
                             FROM events
                             WHERE run_id = ?1",
                            (run_id.as_str(),),
                        )
                        .await?;
                    let next_sequence = rows
                        .next()
                        .await?
                        .map(|row| row.opt_i64(0))
                        .transpose()?
                        .flatten()
                        .unwrap_or(-1)
                        + 1;

                    if latest_data
                        .as_deref()
                        .and_then(|data| serde_json::from_str::<TranscriptEvent>(data).ok())
                        .and_then(|event| event.raw)
                        .and_then(|raw| raw.get("hash").and_then(|value| value.as_str()).map(str::to_string))
                        .as_deref()
                        == Some(hash.as_str())
                    {
                        return Ok((None, next_sequence as i32));
                    }

                    conn.execute(
                        "INSERT INTO events (
                            id, run_id, session_id, sequence, timestamp, event_type, data,
                            parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                            cache_create_tokens, output_tokens, turn_id
                         ) VALUES (?1, ?2, ?3, ?4, ?5, 'system:prompt', ?6, NULL, ?5, NULL, NULL, NULL, NULL, NULL)",
                        (
                            event_id.as_str(),
                            run_id.as_str(),
                            session_id.as_deref(),
                            next_sequence,
                            i64::from(now),
                            data.as_str(),
                        ),
                    )
                    .await?;
                    Ok((Some(next_sequence as i32), next_sequence as i32 + 1))
                })
            })
            .await
            .map_err(|e| e.to_string())
        }
    });

    let Ok((inserted_sequence, next_sequence)) = insert_result else {
        return 0;
    };

    if inserted_sequence.is_some() {
        let _ = orch.services.emitter.emit(
            "db-change",
            crate::notify::event_db_change_for_run(
                orch.db.local.clone(),
                run_id,
                session_id,
                "insert",
            ),
        );
    }

    next_sequence
}

struct SessionDbContext {
    /// Stable home URI for this run (always a full node URI: cairn://p/PROJECT/N/EXEC/NODE).
    /// Required — session startup fails if the run lacks the components to build it.
    home_uri: String,
    run_issue_id: Option<String>,
    project_id: Option<String>,
    project_key: Option<String>,
    project_path: Option<std::path::PathBuf>,
    /// Effective base branch for this run's job (worktree base / PR target),
    /// falling back to the project default for project-level runs or legacy rows.
    effective_base_branch: Option<String>,
    /// The run's job id, used to key the per-job scratch dir surfaced in the
    /// orientation block. `None` for runs with no owning job.
    job_id: Option<String>,
    /// The run's recipe node id, used to resolve this node's `context-self`
    /// living-doc targets for the prompt affordance. `None` for sub-agent task
    /// jobs with no recipe node.
    recipe_node_id: Option<String>,
    /// The run's job worktree path. `None` for an ambient (no-worktree) job that
    /// runs directly on the project's live checkout, and also for scratch-dir
    /// (`CallWorktree::None`) calls/workflows — the two are told apart at the
    /// orientation-block assembly site by comparing `working_dir` to the repo root
    /// (see [`is_ambient_run`]).
    worktree_path: Option<String>,
}

fn session_db_context(orch: &Orchestrator, run_id: &str) -> Result<SessionDbContext, String> {
    let dbs = orch.db.clone();
    let run_id = run_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
            .await
            .map_err(|e| e.to_string())?;
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT runs.issue_id,
                                COALESCE(runs.project_id, issues.project_id) AS project_id,
                                projects.key,
                                projects.repo_path,
                                issues.number,
                                jobs.uri_segment,
                                executions.seq,
                                parent_jobs.uri_segment AS parent_uri_segment,
                                COALESCE(jobs.base_branch, projects.default_branch) AS effective_base_branch,
                                runs.job_id,
                                jobs.recipe_node_id,
                                jobs.worktree_path
                         FROM runs
                         LEFT JOIN issues ON runs.issue_id = issues.id
                         LEFT JOIN projects ON COALESCE(runs.project_id, issues.project_id) = projects.id
                         LEFT JOIN jobs ON runs.job_id = jobs.id
                         LEFT JOIN jobs AS parent_jobs ON jobs.parent_job_id = parent_jobs.id
                         LEFT JOIN executions ON jobs.execution_id = executions.id
                         WHERE runs.id = ?1",
                        (run_id.as_str(),),
                    )
                    .await?;

                let Some(row) = rows.next().await? else {
                    return Err(DbError::internal(format!("Failed to get run: {}", run_id)));
                };

                let run_issue_id = row.opt_text(0)?;
                let project_id = row.opt_text(1)?;
                let project_key = row.opt_text(2)?;
                let project_path = row.opt_text(3)?.map(std::path::PathBuf::from);
                let issue_number = row.opt_i64(4)?.map(|n| n as i32);
                let uri_segment = row.opt_text(5)?;
                let exec_seq = row.opt_i64(6)?.map(|n| n as i32);
                // Present only for sub-agent task jobs; a top-level node has no parent.
                let parent_uri_segment = row.opt_text(7)?;
                let effective_base_branch = row.opt_text(8)?;
                let job_id = row.opt_text(9)?;
                let recipe_node_id = row.opt_text(10)?;
                let worktree_path = row.opt_text(11)?;

                // All four components are required. A missing component means the run
                // record is corrupt or incomplete — fail rather than produce a partial URI.
                // A task job nests under its parent node (`.../{parent}/task/{segment}`);
                // a top-level node is `.../{segment}`.
                let home_uri = match (
                    project_key.as_deref(),
                    issue_number,
                    exec_seq,
                    uri_segment.as_deref(),
                ) {
                    (Some(key), Some(num), Some(seq), Some(segment)) => {
                        cairn_common::uri::build_job_base_uri(
                            key,
                            num,
                            seq,
                            segment,
                            parent_uri_segment.as_deref(),
                        )
                    }
                    _ => {
                        return Err(DbError::internal(format!(
                            "Cannot build home URI for run {}: project_key={:?}, issue_number={:?}, exec_seq={:?}, uri_segment={:?}",
                            run_id, project_key, issue_number, exec_seq, uri_segment
                        )));
                    }
                };

                Ok(SessionDbContext {
                    home_uri,
                    run_issue_id,
                    project_id,
                    project_key,
                    project_path,
                    effective_base_branch,
                    job_id,
                    recipe_node_id,
                    worktree_path,
                })
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

/// Resolve a node's `context-self` living-doc targets for the prompt affordance:
/// the ArtifactNodes this node owns and patches across its life (name + schema).
/// Empty for jobs with no recipe node or no
/// `context-self` edges. A read failure degrades to no affordance rather than
/// failing session startup.
fn resolve_ctx_self_targets(
    orch: &Orchestrator,
    execution_id: &str,
    node_id: &str,
) -> Vec<crate::models::OutputSchemaInfo> {
    let dbs = orch.db.clone();
    let execution_id = execution_id.to_string();
    let node_id = node_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_execution(&dbs, &execution_id)
            .await
            .map_err(|e| e.to_string())?;
        db.read(|conn| {
            let execution_id = execution_id.clone();
            let node_id = node_id.clone();
            Box::pin(async move {
                crate::execution::jobs::resolve_ctx_self_schemas_conn(conn, &node_id, &execution_id)
                    .await
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .unwrap_or_default()
}

/// Resolve the system-prompt instruction text a running node inherits from its
/// upstream Instruction nodes (see
/// [`crate::execution::jobs::resolve_instruction_prompt_conn`]). Returns an empty
/// string for any node with no Instruction edge, and on any read error, so
/// session startup never fails on this and a recipe with no Instruction node
/// yields the bare role prompt.
fn resolve_instruction_prompt(orch: &Orchestrator, execution_id: &str, node_id: &str) -> String {
    let dbs = orch.db.clone();
    let execution_id = execution_id.to_string();
    let node_id = node_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_execution(&dbs, &execution_id)
            .await
            .map_err(|e| e.to_string())?;
        db.read(|conn| {
            let execution_id = execution_id.clone();
            let node_id = node_id.clone();
            Box::pin(async move {
                crate::execution::jobs::resolve_instruction_prompt_conn(
                    conn,
                    &node_id,
                    &execution_id,
                )
                .await
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .unwrap_or_default()
}

/// Render a JSON Schema object's properties as a `Fields:` list for the prompt:
/// each property's name, type, required/optional flag, and description. Empty
/// string when the schema declares no `properties`. Shared by the terminal
/// output-artifact affordance and the living-doc (context-self) affordance so
/// both surface a node's typed shape identically.
fn render_schema_fields(schema_value: &serde_json::Value) -> String {
    let required: Vec<String> = schema_value
        .get("required")
        .and_then(|r| r.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let Some(props) = schema_value.get("properties").and_then(|p| p.as_object()) else {
        return String::new();
    };
    let mut out = String::from("Fields:\n");
    for (key, val) in props {
        let ty = val.get("type").and_then(|t| t.as_str()).unwrap_or("any");
        let desc = val
            .get("description")
            .and_then(|d| d.as_str())
            .unwrap_or("");
        let req = if required.iter().any(|r| r == key) {
            "required"
        } else {
            "optional"
        };
        let sep = if desc.is_empty() { "" } else { ": " };
        out.push_str(&format!("- `{key}` ({ty}, {req}){sep}{desc}\n"));
    }
    out
}

/// Render the `## Living working documents` affordance for a node's
/// `context-self` targets. Generic and recipe-driven: for each ArtifactNode the
/// node owns it names the artifact (`cairn:~/<name>`) and lists its fields, then
/// states the living-doc contract — create AND patch repeatedly across the whole
/// run, which never ends the turn and never advances the DAG (the explicit
/// contrast with the single terminal `context-out` artifact). Returns `None`
/// when the node owns no named, schema-bearing ctx-self targets.
fn build_ctx_self_section(
    targets: &[crate::models::OutputSchemaInfo],
    schema_dir: Option<&std::path::Path>,
) -> Option<String> {
    let entries: Vec<(String, serde_json::Value)> = targets
        .iter()
        .filter_map(|t| {
            let name = t.artifact_name.clone()?;
            let schema =
                crate::output_schemas::resolve_output_schema(schema_dir, &t.schema).ok()?;
            Some((name, schema))
        })
        .collect();
    if entries.is_empty() {
        return None;
    }

    let mut section = String::from(
        "## Living working documents\n\n\
         This node owns living working docs (e.g. a scratchpad or status board) \
         at `cairn:~/<name>`. Create one with `write` (mode `create`) and revise \
         it anytime with mode `patch` — this never ends your turn or advances \
         the workflow, so keep it current as you go.\n\n\
         Your living documents:\n\n",
    );
    for (name, schema_value) in &entries {
        section.push_str(&format!("### `cairn:~/{name}`\n\n"));
        let fields = render_schema_fields(schema_value);
        if !fields.is_empty() {
            section.push_str(&fields);
            section.push('\n');
        }
    }
    Some(section)
}

/// Build the orientation block: the agent's concrete coordinates for this run —
/// where it sits on disk, its canonical node URI (which `cairn:~/` resolves to),
/// the project + repo root it operates against, the base branch worktrees fork
/// from, and the host platform. Folds in the home-URI "Current Location" pointer
/// that previously stood alone, so an agent no longer has to probe for paths it
/// can simply be told.
#[allow(clippy::too_many_arguments)]
fn build_orientation_block(
    working_dir: &str,
    home_uri: &str,
    project_key: Option<&str>,
    repo_root: Option<&str>,
    base_branch: Option<&str>,
    scratch_dir: Option<&str>,
    model: Option<&str>,
    // Ambient (no-worktree) run: cwd IS the project's live checkout. The block
    // relabels the cwd and drops the "NOT your working tree" repo-root line so
    // the coordinates stay accurate. The Version Control tiering itself lives in
    // the shared CAIRN segment (`cairn_system_prompt(ambient)`), not here — this
    // function only adjusts per-run coordinates.
    ambient: bool,
) -> String {
    let mut out = String::from("## Orientation\n\nYour coordinates for this run:\n\n");
    if ambient {
        out.push_str(&format!(
            "- Working directory (cwd): `{}` \u{2014} the project's live checkout (shared with the user)\n",
            working_dir
        ));
    } else {
        out.push_str(&format!("- Working directory (cwd): `{}`\n", working_dir));
    }
    out.push_str(&format!(
        "- Node (home URI): `{}` \u{2014} `cairn:~/` resolves here\n",
        home_uri
    ));
    if let Some(key) = project_key {
        out.push_str(&format!("- Project: `{}`\n", key));
    }
    // In ambient mode the repo root IS the cwd, so the standalone
    // "NOT your working tree; do not `cd` here" repo-root line would contradict
    // the cwd line above — omit it. A worktree-backed run keeps it.
    if !ambient {
        if let Some(root) = repo_root {
            out.push_str(&format!(
                "- Repository root (the project's primary checkout, on its own branch \u{2014} NOT your working tree; do not `cd` here): `{}`\n",
                root
            ));
        }
    }
    if let Some(branch) = base_branch {
        out.push_str(&format!("- Base branch: `{}`\n", branch));
    }
    out.push_str(&format!(
        "- Platform: `{}/{}`\n",
        std::env::consts::OS,
        std::env::consts::ARCH
    ));
    if let Some(model) = model {
        out.push_str(&format!("- Model: `{}`\n", model));
    }
    if let Some(scratch) = scratch_dir {
        out.push_str(&format!(
            "- Scratch dir (TMPDIR): `{}` \u{2014} already exported as `$TMPDIR`/`$TMP`/`$TEMP`; reclaimed at teardown\n",
            scratch
        ));
    }
    out.push_str(
        "\nKeep scratch and temp files in the working directory or the scratch dir above (your `$TMPDIR`); other paths outside the worktree are gated by the fence.",
    );
    out
}

/// Whether this run is an ambient (no-worktree) job that runs directly on the
/// project's live checkout. True only when the job has NO worktree AND its cwd is
/// exactly the project repo root. The repo-root equality guard is load-bearing:
/// a `CallWorktree::None` call or a workflow also has a NULL `worktree_path`, but
/// runs in a scratch dir under `$TMPDIR` — those must NOT receive the ambient
/// framing (they cannot even read the project tree).
fn is_ambient_run(worktree_path: Option<&str>, working_dir: &str, repo_root: Option<&str>) -> bool {
    worktree_path.is_none() && repo_root == Some(working_dir)
}

/// Render the `## Project checks` section: the project's configured `checks`
/// contract (`.cairn/config.yaml`), each check with its command, policy, and
/// cadence, over framing on WHEN the engine runs each cadence and HOW verdicts
/// are delivered — so an agent trusts the automated cadence instead of
/// re-testing by hand. Returns `None` for an empty contract so the section is
/// omitted, mirroring the Available Agents / Skills empty-gating.
fn build_project_checks_section(
    checks: &std::collections::HashMap<String, crate::config::project_settings::CheckCommand>,
) -> Option<String> {
    if checks.is_empty() {
        return None;
    }

    let mut entries: Vec<_> = checks.iter().collect();
    entries.sort_by(|(left, _), (right, _)| left.cmp(right));

    let mut out = String::from("## Project checks\n\n");
    out.push_str(
        "Configured checks from `.cairn/config.yaml`, run automatically on a cadence:\n\n\
         - `when: write` checks run right after each source-touching commit; their verdicts \
         (with test counts) are appended to the committing tool result.\n\
         - `when: review` checks run in the background at every turn-end, re-running the \
         fuller suites. A failing turn-end check wakes this session with the results inlined; \
         passing results ride along passively into your next turn.\n\n\
         Trust the cadence: a green verdict with a test count covers the diff, so the per-suite \
         commands exist for iterating on a failure a check surfaced, not for re-verifying a \
         finished diff. What checks do NOT cover: live UI/feature verification and cross-system \
         integration still need a browser or a dev instance.\n\n",
    );
    for (name, check) in entries {
        out.push_str(&format!(
            "- **{}**: `{}` (policy: `{}`, when: `{}`)\n",
            name,
            check.command,
            check.policy.as_str(),
            check.when.as_str()
        ));
    }
    Some(out)
}

fn build_available_terminals_section(
    terminal_commands: &[crate::models::TerminalCommand],
) -> Option<String> {
    if terminal_commands.is_empty() {
        return None;
    }
    let mut out = String::from("## Available Terminals\n\n");
    out.push_str(
        "The project's named/suggested terminals. Create one with a `write` to its terminal URI \
         (`cairn:~/terminal/<slug>` for a node terminal, `cairn://p/PROJECT/terminal/<slug>` for a \
         project terminal), passing the command in the payload. You may also create ad-hoc \
         terminals with any command.\n\n",
    );
    for tc in terminal_commands {
        out.push_str(&format!("- **{}**: `{}`\n", tc.name, tc.command));
    }
    let first = &terminal_commands[0];
    // Show the slug the system would generate so the example is runnable; fall
    // back to a command-derived slug when the name slugifies empty.
    let slug = {
        let from_name = crate::mcp::handlers::slug::slugify(&first.name);
        if from_name.is_empty() {
            crate::mcp::handlers::slug::slugify_command(&first.command)
        } else {
            from_name
        }
    };
    out.push_str(&format!(
        "\nExample:\n`write({{changes:[{{target:\"cairn:~/terminal/{slug}\", mode:\"create\", payload:{{command:\"{}\"}}}}]}})`\n",
        first.command
    ));
    Some(out)
}

pub struct PromptMessage {
    /// `messages.rowid` — monotonic with insertion order; the key for the
    /// per-session channel-injection cursor (`Option<i64>`).
    pub rowid: i64,
    pub sender_name: String,
    pub content: String,
    pub created_at: i64,
}

struct PeerAgent {
    node_name: String,
    status: String,
    uri: String,
}

fn build_messaging_context(
    orch: &Orchestrator,
    project_key: &str,
    issue_id: &str,
    current_run_id: &str,
) -> String {
    let dbs = orch.db.clone();
    let project_key = project_key.to_string();
    let issue_id = issue_id.to_string();
    let current_run_id = current_run_id.to_string();
    let data = run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_run(&dbs, &current_run_id)
            .await
            .map_err(|e| e.to_string())?;
        // Read phase: peers + channel messages newer than this session's
        // injection cursor.
        let (peers, recent, session_id) = db
            .read(|conn| {
                Box::pin(async move {
                    let peers =
                        find_peer_agents(conn, &project_key, &issue_id, &current_run_id).await?;
                    let issue_key = issue_key(conn, &project_key, &issue_id).await?;
                    let (session_id, job_id) =
                        session_and_job_id_for_run(conn, &current_run_id).await?;
                    let cursor = match session_id.as_deref() {
                        Some(sid) => read_channel_cursor(conn, sid).await?,
                        None => None,
                    };
                    let recent = recent_messages_for_run(
                        conn,
                        &project_key,
                        issue_key.as_deref(),
                        job_id.as_deref(),
                        cursor,
                        20,
                        true,
                    )
                    .await?;
                    Ok((peers, recent, session_id))
                })
            })
            .await
            .map_err(|e| e.to_string())?;

        // Advance phase: record the newest injected message so the next cold
        // resume of this session does not re-inject it. Runs without a session
        // (legacy) carry no cursor and fall back to the recent-history view.
        if let Some(session_id) = session_id {
            if let Some(max_rowid) = recent.iter().map(|m| m.rowid).max() {
                db.write(|conn| {
                    let session_id = session_id.clone();
                    Box::pin(
                        async move { advance_channel_cursor(conn, &session_id, max_rowid).await },
                    )
                })
                .await
                .map_err(|e| e.to_string())?;
            }
        }

        Ok::<_, String>((peers, recent))
    })
    .unwrap_or_default();

    let (peers, recent) = data;
    if peers.is_empty() && recent.is_empty() {
        return String::new();
    }

    let mut out = String::from("## Agent Messaging\n\n");
    out.push_str(
        "You can communicate with other agents using `write` with message-channel URIs.\n",
    );
    out.push_str("Messages sent to the issue channel are visible to all agents on the issue.\n");
    out.push_str(
        "Direct messages go to a specific agent via their URI and auto-resume them if idle.\n\n",
    );

    if !peers.is_empty() {
        out.push_str("### Active Peers\n\n");
        for peer in &peers {
            out.push_str(&format!(
                "- **{}** ({}) - `{}`\n",
                peer.node_name, peer.status, peer.uri
            ));
        }
        out.push('\n');
    }

    if !recent.is_empty() {
        out.push_str("### Recent Messages\n\n");
        for msg in &recent {
            let ts = chrono::DateTime::from_timestamp(msg.created_at, 0)
                .map(|dt| dt.format("%H:%M:%S").to_string())
                .unwrap_or_else(|| "??:??:??".to_string());
            out.push_str(&format!("[{}] {}: {}\n", ts, msg.sender_name, msg.content));
        }
    }

    out
}

/// Compose the per-run user message from the base task prompt and the dynamic
/// messaging catch-up. Messaging rides the user turn (not the cached system
/// prompt) so the system-prompt prefix stays byte-identical across resumes; an
/// empty piece on either side collapses cleanly so a pure wake (no task text)
/// or a quiet channel (no messaging) never injects stray separators.
fn compose_user_message(prompt: &str, messaging: &str) -> String {
    match (prompt.is_empty(), messaging.is_empty()) {
        (_, true) => prompt.to_string(),
        (true, false) => messaging.to_string(),
        (false, false) => format!("{prompt}\n\n{messaging}"),
    }
}

async fn issue_key(
    conn: &cairn_db::turso::Connection,
    project_key: &str,
    issue_id: &str,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT number
             FROM issues
             WHERE id = ?1",
            (issue_id,),
        )
        .await?;
    rows.next()
        .await?
        .map(|row| row.i64(0).map(|n| format!("{}/{}", project_key, n)))
        .transpose()
}

async fn find_peer_agents(
    conn: &cairn_db::turso::Connection,
    project_key: &str,
    issue_id: &str,
    current_run_id: &str,
) -> DbResult<Vec<PeerAgent>> {
    let mut current_rows = conn
        .query(
            "SELECT job_id
             FROM runs
             WHERE id = ?1",
            (current_run_id,),
        )
        .await?;
    let current_job_id = current_rows
        .next()
        .await?
        .map(|row| row.opt_text(0))
        .transpose()?
        .flatten();

    let mut rows = conn
        .query(
            "SELECT jobs.id, jobs.node_name, jobs.uri_segment, jobs.status, executions.seq,
                    parent_jobs.uri_segment AS parent_uri_segment
             FROM jobs
             LEFT JOIN executions ON jobs.execution_id = executions.id
             LEFT JOIN jobs AS parent_jobs ON jobs.parent_job_id = parent_jobs.id
             WHERE jobs.issue_id = ?1
               AND jobs.status != 'pending'",
            (issue_id,),
        )
        .await?;

    let mut peers = Vec::new();
    let issue_number = issue_key(conn, project_key, issue_id)
        .await?
        .and_then(|key| key.rsplit('/').next().and_then(|n| n.parse::<i32>().ok()));
    let Some(issue_number) = issue_number else {
        return Ok(peers);
    };

    while let Some(row) = rows.next().await? {
        let job_id = row.text(0)?;
        if current_job_id.as_deref() == Some(job_id.as_str()) {
            continue;
        }
        let node_name = row.opt_text(1)?.unwrap_or_else(|| "unknown".to_string());
        let uri_segment = row.opt_text(2)?;
        let status = row.text(3)?;
        let exec_seq = row.opt_i64(4)?.map(|n| n as i32).unwrap_or(1);
        let parent_uri_segment = row.opt_text(5)?;
        let Some(uri_segment) = uri_segment else {
            continue;
        };
        let uri = cairn_common::uri::build_job_base_uri(
            project_key,
            issue_number,
            exec_seq,
            &uri_segment,
            parent_uri_segment.as_deref(),
        );
        peers.push(PeerAgent {
            node_name,
            status,
            uri,
        });
    }
    Ok(peers)
}

async fn recent_messages_for_run(
    conn: &cairn_db::turso::Connection,
    project_key: &str,
    issue_key: Option<&str>,
    exclude_job_id: Option<&str>,
    cursor: Option<i64>,
    limit: i64,
    include_system: bool,
) -> DbResult<Vec<PromptMessage>> {
    let include_system = if include_system { 1_i64 } else { 0_i64 };
    let mut rows = if let Some(issue_key) = issue_key {
        conn.query(
            "SELECT rowid, sender_name, content, created_at
             FROM messages
             WHERE (sender_run_id IS NULL
                    OR ?1 IS NULL
                    OR sender_run_id NOT IN (SELECT id FROM runs WHERE job_id = ?1))
               AND (?6 = 1 OR sender_name != 'system')
               AND (
                    (channel_type = 'project' AND channel_id = ?2)
                    OR (channel_type = 'issue' AND channel_id = ?3)
               )
               AND (?5 IS NULL OR rowid > ?5)
             ORDER BY rowid DESC
             LIMIT ?4",
            params![
                exclude_job_id,
                project_key,
                issue_key,
                limit,
                cursor,
                include_system
            ],
        )
        .await?
    } else {
        conn.query(
            "SELECT rowid, sender_name, content, created_at
             FROM messages
             WHERE (sender_run_id IS NULL
                    OR ?1 IS NULL
                    OR sender_run_id NOT IN (SELECT id FROM runs WHERE job_id = ?1))
               AND (?5 = 1 OR sender_name != 'system')
               AND channel_type = 'project'
               AND channel_id = ?2
               AND (?4 IS NULL OR rowid > ?4)
             ORDER BY rowid DESC
             LIMIT ?3",
            params![exclude_job_id, project_key, limit, cursor, include_system],
        )
        .await?
    };

    let mut messages = Vec::new();
    while let Some(row) = rows.next().await? {
        messages.push(PromptMessage {
            rowid: row.i64(0)?,
            sender_name: row.text(1)?,
            content: row.text(2)?,
            created_at: row.i64(3)?,
        });
    }
    messages.reverse();
    Ok(messages)
}

/// Non-stamping peek at project/issue channel messages that should surface as
/// pending-delivery chips. This intentionally returns the non-system subset of
/// messages that may be injected on the next messaging-context build, so passive
/// lifecycle awareness can reach agents without becoming dismissible UI noise.
pub async fn pending_channel_messages_for_job(
    db: &LocalDb,
    job_id: &str,
    limit: i64,
) -> DbResult<Vec<PromptMessage>> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT p.key, i.id, i.number, j.current_session_id
                     FROM jobs j
                     JOIN issues i ON i.id = j.issue_id
                     JOIN projects p ON p.id = i.project_id
                     WHERE j.id = ?1
                     LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(Vec::new());
            };
            let project_key = row.text(0)?;
            let issue_number = row.i64(2)?;
            let session_id = row.opt_text(3)?;
            let Some(session_id) = session_id else {
                return Ok(Vec::new());
            };
            let issue_key = format!("{project_key}/{issue_number}");
            let cursor = read_channel_cursor(conn, &session_id).await?;
            recent_messages_for_run(
                conn,
                &project_key,
                Some(&issue_key),
                Some(&job_id),
                cursor,
                limit,
                false,
            )
            .await
        })
    })
    .await
}

/// Mark channel messages seen through `rowid` for the job's current session.
/// The cursor is monotonic and channel-scoped: dismissing one channel chip marks
/// that message and all older pending channel messages caught up, matching the
/// injection cursor used when messages are delivered to the agent.
pub async fn dismiss_channel_message_for_job(
    db: &LocalDb,
    job_id: &str,
    rowid: i64,
) -> DbResult<()> {
    let job_id = job_id.to_string();
    db.write(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT current_session_id FROM jobs WHERE id = ?1 LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(());
            };
            let Some(session_id) = row.opt_text(0)? else {
                return Ok(());
            };
            advance_channel_cursor(conn, &session_id, rowid).await
        })
    })
    .await
}

/// Resolve the session and job backing a run, used to scope the channel-injection
/// cursor and to exclude every run from the recipient's own job across resumes.
/// Returns `(None, None)` for legacy runs missing either relation.
async fn session_and_job_id_for_run(
    conn: &cairn_db::turso::Connection,
    run_id: &str,
) -> DbResult<(Option<String>, Option<String>)> {
    let mut rows = conn
        .query(
            "SELECT session_id, job_id FROM runs WHERE id = ?1",
            params![run_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok((row.opt_text(0)?, row.opt_text(1)?)),
        None => Ok((None, None)),
    }
}

async fn read_channel_cursor(
    conn: &cairn_db::turso::Connection,
    session_id: &str,
) -> DbResult<Option<i64>> {
    let mut rows = conn
        .query(
            "SELECT channel_cursor_rowid FROM sessions WHERE id = ?1",
            params![session_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => row.opt_i64(0),
        None => Ok(None),
    }
}

/// Move the session's channel-injection cursor forward to `rowid`. The guard
/// keeps the cursor monotonic so a stale/concurrent build can never rewind it
/// and re-surface already-injected messages.
async fn advance_channel_cursor(
    conn: &cairn_db::turso::Connection,
    session_id: &str,
    rowid: i64,
) -> DbResult<()> {
    conn.execute(
        "UPDATE sessions
            SET channel_cursor_rowid = ?2
          WHERE id = ?1
            AND (channel_cursor_rowid IS NULL OR ?2 > channel_cursor_rowid)",
        params![session_id, rowid],
    )
    .await?;
    Ok(())
}

/// Insert a synthetic system:error event for display in the transcript
pub fn insert_error_event(
    orch: &Orchestrator,
    run_id: &str,
    session_id: Option<&str>,
    error_message: &str,
) {
    let now = chrono::Utc::now().timestamp() as i32;
    let event_id = ids::mint_child(run_id);
    let run_id_owned = run_id.to_string();
    let session_id_owned = session_id.map(|s| s.to_string());
    let error_message = error_message.to_string();

    let transcript_event = TranscriptEvent {
        event_type: "system:error".to_string(),
        session_id: session_id_owned.clone(),
        parent_tool_use_id: None,
        content: Some(error_message.clone()),
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id: None,
        tool_result: None,
        is_error: true,
        thinking_ms: None,
        raw: Some(serde_json::json!({"error": error_message})),
    };

    let data = serde_json::to_string(&transcript_event).unwrap_or_default();
    let insert_result = run_db_blocking({
        let dbs = orch.db.clone();
        let event_id = event_id.clone();
        let run_id = run_id_owned.clone();
        let session_id = session_id_owned.clone();
        let data = data.clone();
        move || async move {
            let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|e| e.to_string())?;
            db.write(|conn| {
                let event_id = event_id.clone();
                let run_id = run_id.clone();
                let session_id = session_id.clone();
                let data = data.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT MAX(sequence)
                             FROM events
                             WHERE run_id = ?1",
                            (run_id.as_str(),),
                        )
                        .await?;
                    let sequence = rows
                        .next()
                        .await?
                        .map(|row| row.opt_i64(0))
                        .transpose()?
                        .flatten()
                        .unwrap_or(-1)
                        + 1;

                    conn.execute(
                        "INSERT INTO events (
                            id, run_id, session_id, sequence, timestamp, event_type, data,
                            parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                            cache_create_tokens, output_tokens, turn_id
                         ) VALUES (?1, ?2, ?3, ?4, ?5, 'system:error', ?6, NULL, ?5, NULL, NULL, NULL, NULL, NULL)",
                        (
                            event_id.as_str(),
                            run_id.as_str(),
                            session_id.as_deref(),
                            sequence,
                            i64::from(now),
                            data.as_str(),
                        ),
                    )
                    .await?;
                    Ok(sequence as i32)
                })
            })
            .await
            .map_err(|e| e.to_string())
        }
    });

    if insert_result.is_err() {
        return;
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::event_db_change_for_run(orch.db.local.clone(), run_id, session_id, "insert"),
    );
}

/// Find the claude binary path, caching the result
pub fn get_claude_path(
    state: &crate::agent_process::process::AgentProcessState,
) -> Result<String, String> {
    log::debug!("get_claude_path: starting");

    // Check cache first
    {
        let cached = state.cli_binary_path.lock().map_err(|e| e.to_string())?;
        if let Some(path) = cached.as_ref() {
            log::debug!("get_claude_path: using cached path: {}", path);
            return Ok(path.clone());
        }
    }

    log::debug!("get_claude_path: no cache, resolving...");
    let path = crate::env::find_binary("claude").map_err(|e| {
        log::debug!("get_claude_path: {}", e);
        e
    })?;

    log::debug!("get_claude_path: found claude at: {}", path);

    // Cache and return
    {
        let mut cached = state.cli_binary_path.lock().map_err(|e| e.to_string())?;
        *cached = Some(path.clone());
    }

    log::info!("Resolved claude path: {}", path);
    Ok(path)
}

/// Extract session ID from a ClaudeEvent.
#[allow(dead_code)]
pub fn extract_session_id(event: &ClaudeEvent) -> Option<String> {
    match event {
        ClaudeEvent::System { session_id, .. } => Some(session_id.clone()),
        ClaudeEvent::User { session_id, .. } => Some(session_id.clone()),
        ClaudeEvent::Assistant { session_id, .. } => Some(session_id.clone()),
        ClaudeEvent::Result { session_id, .. } => Some(session_id.clone()),
        ClaudeEvent::StreamEvent { session_id, .. } => Some(session_id.clone()),
        ClaudeEvent::ControlResponse { .. } => None,
        ClaudeEvent::RateLimitEvent { .. } | ClaudeEvent::Unknown => None,
    }
}

/// Get the tmp directory for system prompt files
fn get_prompt_tmp_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".cairn")
        .join("tmp")
}

/// Write the fully assembled system prompt to a per-run temp file and return its
/// path. Claude delivers the entire uniform stack (cairn + workspace + project +
/// role + orientation) as one `--system-prompt-file`, fully replacing CC's
/// default; the file bytes equal the persisted segment concatenation exactly.
pub fn write_system_prompt_file(run_id: &str, content: &str) -> Result<PathBuf, String> {
    let tmp_dir = get_prompt_tmp_dir();
    fs::create_dir_all(&tmp_dir).map_err(|e| format!("Failed to create tmp dir: {}", e))?;

    let file_path = tmp_dir.join(format!("prompt-{}.md", run_id));

    fs::write(&file_path, content)
        .map_err(|e| format!("Failed to write system prompt file: {}", e))?;

    log::debug!(
        "Wrote system prompt to {:?} ({} bytes)",
        file_path,
        content.len()
    );

    Ok(file_path)
}

/// Segment kinds in an assembled system prompt's recorded boundary map. Every
/// kind except [`SEGMENT_KIND_DYNAMIC`] is static across runs (a backend or app
/// constant, the workspace doctrine, or an agent's role body) and is
/// content-addressed into `archival_blobs` at teardown; the dynamic tail (the
/// per-run orientation block) stays inline on the event.
pub const SEGMENT_KIND_CAIRN: &str = "cairn";
pub const SEGMENT_KIND_WORKSPACE: &str = "workspace";
pub const SEGMENT_KIND_PROJECT: &str = "project";
pub const SEGMENT_KIND_AGENT: &str = "agent";
pub const SEGMENT_KIND_DYNAMIC: &str = "dynamic";

/// One labeled span of an assembled system prompt. Concatenating every segment's
/// `text` in order reproduces the full prompt byte for byte; persisting the
/// segments (not just the string) lets teardown archival content-address the
/// static spans and inline only the per-run dynamic tail.
#[derive(Debug, Clone)]
pub struct PromptSegment {
    pub kind: &'static str,
    pub text: String,
}

impl PromptSegment {
    pub fn new(kind: &'static str, text: impl Into<String>) -> Self {
        Self {
            kind,
            text: text.into(),
        }
    }
}

/// Assemble the ordered system-prompt segments shared by every backend. Their
/// concatenation is the full system prompt, identical across Claude, Codex, and
/// OpenRouter:
/// `cairn + ["\n\n## Workspace Instructions\n\n" + workspace] +
///  ["\n\n## Project Instructions\n\n" + project] + ["\n\n" + agent]`,
/// where the agent piece splits into a static head and the inlined dynamic tail
/// when `dynamic_tail` is a non-empty suffix of `agent`.
///
/// `cairn` is the Cairn base prompt (always first). `workspace` is the raw
/// `~/.cairn/AGENTS.md` body and `project` the raw run repo-root `AGENTS.md`
/// body (each header is added here). `agent` is the full agent role content;
/// when `dynamic_tail` does not apply, the whole agent content stays one static
/// segment (still correct, just less dedup).
pub fn assemble_prompt_segments(
    cairn: &str,
    workspace: Option<&str>,
    project: Option<&str>,
    agent: Option<&str>,
    dynamic_tail: Option<&str>,
) -> Vec<PromptSegment> {
    let mut segments: Vec<PromptSegment> = Vec::new();
    // The first top-level piece carries no leading separator; every subsequent
    // piece is prefixed "\n\n".
    macro_rules! lead {
        () => {
            if segments.is_empty() {
                ""
            } else {
                "\n\n"
            }
        };
    }

    segments.push(PromptSegment::new(
        SEGMENT_KIND_CAIRN,
        format!("{}{}", lead!(), cairn),
    ));
    if let Some(ws) = workspace.filter(|content| !content.trim().is_empty()) {
        segments.push(PromptSegment::new(
            SEGMENT_KIND_WORKSPACE,
            format!("{}## Workspace Instructions\n\n{}", lead!(), ws.trim()),
        ));
    }
    if let Some(proj) = project.filter(|content| !content.trim().is_empty()) {
        segments.push(PromptSegment::new(
            SEGMENT_KIND_PROJECT,
            format!("{}## Project Instructions\n\n{}", lead!(), proj.trim()),
        ));
    }
    if let Some(agent) = agent.filter(|content| !content.trim().is_empty()) {
        let lead = lead!();
        match dynamic_tail.filter(|tail| !tail.is_empty() && agent.ends_with(*tail)) {
            Some(tail) => {
                let head = &agent[..agent.len() - tail.len()];
                segments.push(PromptSegment::new(
                    SEGMENT_KIND_AGENT,
                    format!("{lead}{head}"),
                ));
                segments.push(PromptSegment::new(SEGMENT_KIND_DYNAMIC, tail.to_string()));
            }
            None => {
                segments.push(PromptSegment::new(
                    SEGMENT_KIND_AGENT,
                    format!("{lead}{agent}"),
                ));
            }
        }
    }
    segments
}

/// Concatenate every segment's text into the full system prompt string. Used by
/// backends that deliver the prompt as a single blob (Claude's
/// `--system-prompt-file`, OpenRouter's `system` message).
pub fn flatten_prompt_segments(segments: &[PromptSegment]) -> String {
    segments.iter().map(|s| s.text.as_str()).collect()
}

/// The Codex `baseInstructions` payload: the `cairn` + `workspace` + `project`
/// segment texts concatenated. Slicing the assembled segments (rather than
/// rebuilding the string independently) makes Codex's sent bytes equal the
/// persisted segments by construction, not by coincidence.
pub fn base_instructions_from_segments(segments: &[PromptSegment]) -> String {
    segments
        .iter()
        .filter(|s| {
            s.kind == SEGMENT_KIND_CAIRN
                || s.kind == SEGMENT_KIND_WORKSPACE
                || s.kind == SEGMENT_KIND_PROJECT
        })
        .map(|s| s.text.as_str())
        .collect()
}

/// The Codex `developerInstructions` payload: the `agent` + `dynamic` segment
/// texts concatenated, or `None` when the agent content is empty. Paired with
/// [`base_instructions_from_segments`] so base + developer == the full prompt.
pub fn developer_instructions_from_segments(segments: &[PromptSegment]) -> Option<String> {
    let combined: String = segments
        .iter()
        .filter(|s| s.kind == SEGMENT_KIND_AGENT || s.kind == SEGMENT_KIND_DYNAMIC)
        .map(|s| s.text.as_str())
        .collect();
    if combined.trim().is_empty() {
        None
    } else {
        Some(combined)
    }
}

/// Clean up a specific prompt file after a run completes
pub fn cleanup_prompt_file(run_id: &str) {
    let file_path = get_prompt_tmp_dir().join(format!("prompt-{}.md", run_id));
    if file_path.exists() {
        if let Err(e) = fs::remove_file(&file_path) {
            log::warn!("Failed to remove prompt file {:?}: {}", file_path, e);
        } else {
            log::debug!("Cleaned up prompt file {:?}", file_path);
        }
    }
}

/// Clean up stale prompt files (older than 24 hours)
/// Called on startup to remove orphaned files from crashed runs
pub fn cleanup_stale_prompt_files() {
    let tmp_dir = get_prompt_tmp_dir();
    if !tmp_dir.exists() {
        return;
    }

    let cutoff = std::time::SystemTime::now() - std::time::Duration::from_secs(24 * 60 * 60);

    let entries = match fs::read_dir(&tmp_dir) {
        Ok(entries) => entries,
        Err(e) => {
            log::warn!("Failed to read tmp dir {:?}: {}", tmp_dir, e);
            return;
        }
    };

    let mut cleaned = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("prompt-") && n.ends_with(".md"))
        {
            if let Ok(metadata) = entry.metadata() {
                if let Ok(modified) = metadata.modified() {
                    if modified < cutoff {
                        if let Err(e) = fs::remove_file(&path) {
                            log::warn!("Failed to remove stale prompt file {:?}: {}", path, e);
                        } else {
                            cleaned += 1;
                        }
                    }
                }
            }
        }
    }

    if cleaned > 0 {
        log::info!("Cleaned up {} stale prompt files", cleaned);
    }
}

/// Start an agent session (Claude or Codex).
///
/// Session ID handling:
/// - `session_id`: Cairn's internal session ID (always set). Used for event storage.
/// - `backend_id`: Backend conversation ID for resume. Claude session ID or Codex thread ID.
///   None on first run; set from Session.backend_id on subsequent runs.
///
/// Callers are responsible for creating run/job/chat/event records with the correct
/// session_id BEFORE calling this function.
#[allow(clippy::too_many_arguments)]
pub fn start_agent_session(
    orch: &Orchestrator,
    run_id: &str,
    prompt: &str,
    working_dir: &str,
    session_start: SessionStart,
    model: Option<Model>,
    _initial_user_message: Option<&str>,
    agent_config: Option<&crate::models::AgentConfig>,
    _output_schema: Option<&crate::models::OutputSchemaInfo>,
    // When true, the session's output is constrained to `_output_schema`'s
    // resolved JSON Schema via the backend's NATIVE structured-output mechanism
    // (CAIRN-2505). Set only for node-less ephemeral calls; every other session
    // (recipe nodes, delegated task nodes) passes `false` and is unchanged.
    constrain_output_natively: bool,
    _is_job_level: bool,
    _execution_id: Option<&str>,
    identity_override: Option<crate::identity::UserIdentity>,
) -> Result<(), String> {
    log::debug!("start_agent_session: entered");
    let start_time = std::time::Instant::now();
    log::info!("[PROFILE] start_agent_session begin");

    // Ensure MCP config file exists and get its path. The output schema is no
    // longer plumbed to cairn-cmd — agents write their artifact via `write`
    // (validated server-side), and the schema is surfaced in the prompt.
    log::debug!("start_agent_session: ensuring MCP config");

    // Resolve session DB context early — home_uri is required for the MCP config.
    let db_context = session_db_context(orch, run_id)?;
    let home_uri = db_context.home_uri.clone();
    log::info!("Session home URI: {}", home_uri);

    // Serialize available agents for MCP config
    let (agents_json, _session_project_path, session_project_id) = {
        let project_path = db_context.project_path.clone();
        let project_id = db_context.project_id.clone();

        // Get available agents from files
        let agents = {
            use crate::config::{agents as config_agents, ConfigResult};

            let file_agents = config_agents::list_agents(&orch.config_dir, project_path.as_deref())
                .unwrap_or_default();

            let mut agent_infos: Vec<serde_json::Value> = file_agents
                .into_iter()
                .filter_map(|r| match r {
                    ConfigResult::Ok(agent) => Some(
                        serde_json::json!({"name": agent.name, "description": agent.description}),
                    ),
                    ConfigResult::Err { .. } => None,
                })
                .collect();

            agent_infos.sort_by(|a, b| {
                let a_name = a.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let b_name = b.get("name").and_then(|v| v.as_str()).unwrap_or("");
                a_name.cmp(b_name)
            });

            if !agent_infos.is_empty() {
                serde_json::to_string(&agent_infos).ok()
            } else {
                None
            }
        };

        (agents, project_path, project_id)
    };

    // Build the MCP config inline (passed per-run to the backend, never written
    // to a shared file) so concurrent sessions can't clobber each other's
    // `CAIRN_HOME_URI` / `--agents` payload.
    let cairn_home = cairn_common::paths::cairn_home();
    let cairn_home_str = cairn_home.to_string_lossy();
    let log_level = crate::config::settings::load_log_level(&orch.config_dir);
    let mcp_config_json = crate::config::mcp_setup::build_mcp_config_string(
        &orch.mcp_binary_path,
        orch.mcp_callback_port,
        agents_json.as_deref(),
        Some(home_uri.as_str()),
        cairn_common::paths::env_str(),
        Some(cairn_home_str.as_ref()),
        log_level.as_str(),
    );
    log::debug!("start_agent_session: MCP config built inline");
    log::info!("[PROFILE] MCP config done: {:?}", start_time.elapsed());

    let workspace_settings = crate::config::settings::load_settings(&orch.config_dir);

    // Use provided agent config (agents are now always explicitly passed)
    let agent_config = agent_config.cloned();

    // Ambient run: no job worktree AND cwd is the project repo root, so the run
    // operates directly on the user's live checkout (the Manager recipe's
    // `worktreeMode: none`). A scratch-dir call/workflow also has no worktree but
    // is NOT ambient — the repo-root guard tells them apart. Resolved once here,
    // above the prompt-content block, so it selects both the CAIRN system-prompt
    // tier (via `SessionConfig.ambient`) and the orientation framing, and stays
    // in scope at `SessionConfig` construction below.
    let ambient = is_ambient_run(
        db_context.worktree_path.as_deref(),
        working_dir,
        db_context.project_path.as_deref().and_then(|p| p.to_str()),
    );

    // Resolve tools, model, prompt, permissions, and select backend.
    // All operations that need agent_config + DB access are grouped here.
    let (
        allowed_tools,
        disallowed_tools,
        effective_model,
        final_prompt,
        system_prompt_content,
        system_prompt_dynamic_tail,
        backend,
        permissions,
        max_thinking_tokens,
        reasoning_effort,
        service_tier,
    ) = {
        use crate::config::{agents as config_agents, skills as config_skills, ConfigResult};
        let run_issue_id = db_context.run_issue_id.clone();
        let project_key = db_context.project_key.clone();
        let project_path_for_prompt = db_context.project_path.clone();

        // Resolve-early: runtime extras (effort, thinking) come straight from the
        // AgentConfig, resolved at launch/edit time — never recomputed against
        // current presets here. This is the deferred-resolution deletion that
        // keeps a resumed session stable across workspace-settings changes.
        let (max_thinking_tokens, reasoning_effort, service_tier) = {
            let extras = agent_config
                .as_ref()
                .and_then(|ac| ac.extras.clone())
                .unwrap_or_default();
            (
                extras
                    .max_thinking_tokens
                    .or(workspace_settings.max_thinking_tokens),
                extras.reasoning_effort.clone(),
                extras.service_tier.clone(),
            )
        };

        // Inherited workspace agents/skills a project has disabled drop out of the
        // prompt surfaces entirely. Skills auto-surface into the prompt, so this is
        // the real per-project disable lever for an inherited skill.
        let (disabled_agents, disabled_skills) = match session_project_id.as_deref() {
            Some(pid) => (
                super::config_resource::disabled_keys_blocking(orch, pid, "agent")
                    .unwrap_or_default(),
                super::config_resource::disabled_keys_blocking(orch, pid, "skill")
                    .unwrap_or_default(),
            ),
            None => (
                std::collections::HashSet::new(),
                std::collections::HashSet::new(),
            ),
        };

        // Get list of available agents from files
        let available_agents: Vec<(String, String, String)> = {
            let agents =
                config_agents::list_agents(&orch.config_dir, project_path_for_prompt.as_deref())
                    .unwrap_or_default();
            let mut by_id = std::collections::BTreeMap::new();
            for result in agents {
                if let ConfigResult::Ok(agent) = result {
                    // config_root_subdirs yields project first, so keep the first
                    // occurrence for each id to avoid duplicate prompt entries.
                    by_id
                        .entry(agent.id)
                        .or_insert((agent.name, agent.description));
                }
            }
            by_id.retain(|id, _| !disabled_agents.contains(id));
            let mut result: Vec<(String, String, String)> = by_id
                .into_iter()
                .map(|(id, (name, description))| (id, name, description))
                .collect();
            result.sort_by(|a, b| a.1.cmp(&b.1));
            result
        };

        // Get list of available skills from files
        let available_skills_for_prompt: Vec<(String, String, String)> = {
            let skills =
                config_skills::list_skills(&orch.config_dir, project_path_for_prompt.as_deref())
                    .unwrap_or_default();
            let mut by_id = std::collections::BTreeMap::new();
            for result in skills {
                if let ConfigResult::Ok(skill) = result {
                    // config_root_subdirs yields project first, so keep the first
                    // occurrence for each id to avoid duplicate prompt entries.
                    by_id
                        .entry(skill.id)
                        .or_insert((skill.name, skill.description));
                }
            }
            by_id.retain(|id, _| !disabled_skills.contains(id));
            let mut result: Vec<(String, String, String)> = by_id
                .into_iter()
                .map(|(id, (name, description))| (id, name, description))
                .collect();
            result.sort_by(|a, b| a.1.cmp(&b.1));
            result
        };

        // ================================================================
        // Backend selection (moved before tool resolution so the backend
        // can control which tools are allowed/disallowed).
        // ================================================================

        // Resolve-early: the concrete backend comes from the AgentConfig's atomic
        // selection. backend_preference and model-derivation are fallbacks only
        // for configs that lack a resolved selection.
        let agent_backend_name = agent_config.as_ref().and_then(|ac| {
            ac.selection
                .as_ref()
                .map(|s| s.backend.clone())
                .or_else(|| ac.backend_preference.clone())
        });

        // Runtime model should already be resolved before session start.
        let resolved_model = model.clone();

        let effective_backend_name = agent_backend_name.clone().or_else(|| {
            resolved_model
                .as_ref()
                .and_then(|m| backends::backend_for_model(m.as_str()))
                .map(|s| s.to_string())
        });

        let backend = backends::backend_for_name(effective_backend_name.as_deref());

        // Build canonical fence permissions. Escape gating happens in the verb handlers.
        let permissions = backends::AgentPermissions::new(
            agent_config
                .as_ref()
                .and_then(|ac| ac.fence)
                .unwrap_or_default(),
        );

        // ================================================================
        // Tool resolution via backend adapter
        // ================================================================

        let agent_tools: Vec<String> = agent_config
            .as_ref()
            .map(|a| a.tools.clone())
            .unwrap_or_default();

        let agent_disallowed: Vec<String> = agent_config
            .as_ref()
            .and_then(|a| a.disallowed_tools.clone())
            .unwrap_or_default();

        let resolved = backend.resolve_tools(&agent_tools, &agent_disallowed);
        let allowed = resolved.allowed;
        let disallowed = resolved.disallowed;

        // No per-scope tool stripping: the three verbs are always allow-listed
        // and out-of-worktree file/shell access is gated by the worktree fence
        // in the verb handlers (governed by the agent's fence setting), not by
        // removing tools from the allow-list. See CAIRN-1172.

        // The agent submits its output by writing its artifact via `write`
        // (cairn:~/<name>); there is no dedicated submission tool to allow.

        // Build system prompt content from agent prompt + context. Also yields the
        // per-run dynamic tail (the orientation block + wrapper close) so archival
        // can content-address the static agent head and inline only this suffix.
        let (system_prompt_content, system_prompt_dynamic_tail) = {
            let mut content = agent_config
                .as_ref()
                .map(|a| a.prompt.clone())
                .unwrap_or_default();

            // Inject any upstream Instruction-node content into the system prompt,
            // right after the role prompt and before the Available Agents section.
            // Recipe authors carry role framing (for example, a coordinator's
            // feature-specific tail) on Instruction nodes wired context-out -> context-in; a
            // recipe with no Instruction node yields the bare role prompt.
            let instruction = match (db_context.recipe_node_id.as_deref(), _execution_id) {
                (Some(node_id), Some(execution_id)) => {
                    resolve_instruction_prompt(orch, execution_id, node_id)
                }
                _ => String::new(),
            };
            if !instruction.is_empty() {
                if !content.is_empty() {
                    content.push_str("\n\n");
                }
                content.push_str(&instruction);
            }

            // Append available agents list if the change tool is available.
            // Sub-agents are spawned by appending to the node's tasks collection
            // (`cairn:~/tasks`) via `write`, so the roster is gated on `write`.
            if allowed.contains(&"mcp__cairn__write".to_string()) && !available_agents.is_empty() {
                if !content.is_empty() {
                    content.push_str("\n\n");
                }
                content.push_str("## Available Agents\n\n");
                content.push_str(
                    "Spawn these agents by appending to your node's tasks collection (`cairn:~/tasks`) via `write`, using the agent name as `subagentType`:\n\n",
                );
                for (_id, name, description) in &available_agents {
                    content.push_str(&format!("- **{}**: {}\n", name, description));
                }
            }

            // Append skills resource pointer if the read tool is available
            if allowed.contains(&"mcp__cairn__read".to_string())
                && !available_skills_for_prompt.is_empty()
            {
                if !content.is_empty() {
                    content.push_str("\n\n");
                }
                content.push_str("## Skills\n\n");
                content.push_str(
                    "Skills are readable resources. Read `cairn://skills` for the full list, or read a specific skill with `cairn://skills/<id>`:\n\n",
                );
                for (id, name, description) in &available_skills_for_prompt {
                    content.push_str(&format!("- **{}** (`{}`): {}\n", name, id, description));
                }
            }

            // MCP servers affordance block: configured (enabled) external servers
            // reachable through the cairn://mcp gateway, each server's tools listed
            // by name from the persisted tool store (captured when the server was
            // saved or authorized in Settings, so it's available synchronously on
            // the very first session). Full argument schemas stay a
            // `read cairn://mcp/<srv>` away; a server with no captured tools renders
            // a read pointer. Gated on `read` since discovery and invocation go
            // through it.
            if allowed.contains(&"mcp__cairn__read".to_string()) {
                let mcp_servers = crate::config::mcp_servers::resolve_mcp_servers(
                    &orch.config_dir,
                    project_path_for_prompt.as_deref(),
                );
                if !mcp_servers.is_empty() {
                    let tools_by_server = crate::config::mcp_tools::resolve_tools(
                        &orch.config_dir,
                        project_path_for_prompt.as_deref(),
                    );
                    if let Some(section) =
                        crate::mcp::handlers::mcp_resources::render_mcp_affordance_block(
                            &mcp_servers,
                            &tools_by_server,
                        )
                    {
                        if !content.is_empty() {
                            content.push_str("\n\n");
                        }
                        content.push_str(&section);
                    }
                }
            }

            // Inject project-config-derived sections: available terminals and
            // project references. Both read from the same loaded project config.
            if let Some(ref project_path) = project_path_for_prompt {
                let proj_config =
                    crate::config::project_settings::load_project_settings(project_path);

                if let Some(ref checks) = proj_config.checks {
                    if let Some(section) = build_project_checks_section(checks) {
                        if !content.is_empty() {
                            content.push_str("\n\n");
                        }
                        content.push_str(&section);
                    }
                }

                // Available Terminals: the project's named terminal shortcuts.
                // Terminals are created via `write`, so gate on it (mirroring
                // the Available Agents gate). Absent when none are configured.
                if allowed.contains(&"mcp__cairn__write".to_string()) {
                    if let Some(ref terminal_commands) = proj_config.terminal_commands {
                        if let Some(section) = build_available_terminals_section(terminal_commands)
                        {
                            if !content.is_empty() {
                                content.push_str("\n\n");
                            }
                            content.push_str(&section);
                        }
                    }
                }

                if let Some(ref references) = proj_config.references {
                    if !references.is_empty() {
                        let references_section = crate::references::build_references_prompt(
                            &orch.config_dir,
                            references,
                        );
                        if !references_section.is_empty() {
                            if !content.is_empty() {
                                content.push_str("\n\n");
                            }
                            content.push_str(&references_section);
                        }
                    }
                }
            }

            // Inject the output-artifact instruction when this node produces one.
            // The schema is no longer a visible tool input, so the agent learns
            // the target URI, the fields, and the submit-then-stop protocol here.
            if let Some(info) = _output_schema {
                if let Ok(schema_value) = crate::output_schemas::resolve_output_schema(
                    orch.schema_dir.as_deref(),
                    &info.schema,
                ) {
                    let name = info.artifact_name.as_deref().unwrap_or("artifact");
                    let mut section = format!(
                        "## Output artifact\n\nWhen your work is complete, record your result as this node's output artifact: write it with the `write` verb to `cairn:~/{name}` (mode `create`; use mode `patch` to revise). The payload is validated against the schema below before it is accepted.\n\n"
                    );
                    section.push_str(&render_schema_fields(&schema_value));
                    // Writing the artifact is itself the signal that the work is
                    // ready — it notifies the user and advances the workflow. The
                    // gating sentence is only true when the node waits on a human.
                    section.push_str(
                        "\nWriting this artifact is the last action of your turn. The write itself signals that your work is ready — it notifies the user and pauses the run for review; a reply to this same session continues it.",
                    );
                    if matches!(info.confirm_policy, crate::models::ConfirmPolicy::User) {
                        section.push_str(
                            " The artifact is held for user confirmation before downstream work proceeds.",
                        );
                    }
                    if !content.is_empty() {
                        content.push_str("\n\n");
                    }
                    content.push_str(&section);
                }
            }

            // Inject the living-doc (context-self) affordance: any ArtifactNode
            // this node's `context-self` port targets. Generic and recipe-driven
            // — a node with no ctx-self targets (most nodes, and every sub-agent
            // task) gets nothing here. Resolved fresh from the execution snapshot
            // so it always reflects the running recipe's ports.
            if let (Some(node_id), Some(execution_id)) =
                (db_context.recipe_node_id.as_deref(), _execution_id)
            {
                let ctx_self_targets = resolve_ctx_self_targets(orch, execution_id, node_id);
                if let Some(section) =
                    build_ctx_self_section(&ctx_self_targets, orch.schema_dir.as_deref())
                {
                    if !content.is_empty() {
                        content.push_str("\n\n");
                    }
                    content.push_str(&section);
                }
            }

            // Orientation block: the agent's coordinates for this run. Folds in
            // the home-URI pointer that previously stood alone here. Everything
            // appended from here on is the per-run dynamic tail.
            let dynamic_start = content.len();
            if !content.is_empty() {
                content.push_str("\n\n");
            }
            // Provision the per-job scratch dir up front so it exists the moment
            // the agent reads the orientation block (before any `run` spawn that
            // would otherwise lazily create it). The same path is exported as
            // TMPDIR for each spawned command in `execute_process`.
            let scratch_dir = db_context.job_id.as_deref().map(|jid| {
                crate::scratch::ensure_job_scratch_dir(jid)
                    .to_string_lossy()
                    .to_string()
            });
            content.push_str(&build_orientation_block(
                working_dir,
                &home_uri,
                project_key.as_deref(),
                project_path_for_prompt.as_ref().and_then(|p| p.to_str()),
                db_context.effective_base_branch.as_deref(),
                scratch_dir.as_deref(),
                resolved_model.as_ref().map(|m| m.as_str()),
                ambient,
            ));

            // The dynamic tail = everything appended from `dynamic_start` (the
            // orientation block and its leading separator) plus the wrapper close,
            // a suffix of the wrapped content. `build_orientation_block` is never
            // empty, so the wrapped content is always present.
            let dynamic_tail = format!("{}\n</agent_role>", &content[dynamic_start..]);
            // Wrap in <agent_role> tags to distinguish from MCP instructions.
            let wrapped = format!("<agent_role>\n{}\n</agent_role>", content);
            (Some(wrapped), Some(dynamic_tail))
        };

        // Per-run messaging catch-up (active peers + channel messages newer
        // than this session's injection cursor) is dynamic per-turn state, not
        // durable instruction. It rides the user message instead of the cached
        // system prompt, holding the system-prompt prefix byte-identical across
        // every resume in a session so the provider can reuse the prompt cache
        // rather than re-prime the whole context on each wake.
        let messaging_section = match run_issue_id.as_deref() {
            Some(issue_id) => build_messaging_context(
                orch,
                project_key.as_deref().unwrap_or(""),
                issue_id,
                run_id,
            ),
            None => String::new(),
        };
        let resolved_prompt = compose_user_message(prompt, &messaging_section);

        (
            allowed,
            disallowed,
            resolved_model,
            resolved_prompt,
            system_prompt_content,
            system_prompt_dynamic_tail,
            backend,
            permissions,
            max_thinking_tokens,
            reasoning_effort,
            service_tier,
        )
    };

    // Resolve identity: explicit override (server) > ambient identity store (desktop)
    let resolved_identity = identity_override.or_else(|| {
        let project_overrides = session_project_id.as_ref().and_then(|pid| {
            orch.get_identity_store()
                .and_then(|store| store.project_overrides.get(pid).cloned())
        });
        orch.resolve_identity_for_project(session_project_id.as_deref(), project_overrides.as_ref())
    });

    // Resolve the native output-constraint schema for a schema-constrained call.
    // Only calls opt in (`constrain_output_natively`); the same contract that
    // validates the stored artifact drives the constraint, so they cannot drift.
    let native_output_schema = if constrain_output_natively {
        _output_schema.and_then(|info| {
            crate::output_schemas::resolve_output_schema(orch.schema_dir.as_deref(), &info.schema)
                .ok()
        })
    } else {
        None
    };

    let session_config = SessionConfig {
        run_id: run_id.to_string(),
        working_dir: working_dir.to_string(),
        prompt: final_prompt,
        system_prompt_content,
        system_prompt_dynamic_tail,
        model: effective_model,
        session_start,
        allowed_tools,
        disallowed_tools,
        mcp_config_json,
        home_uri: home_uri.clone(),
        max_thinking_tokens,
        reasoning_effort,
        service_tier,
        permissions,
        bidirectional: true,
        identity: resolved_identity,
        output_schema: native_output_schema,
        ambient,
        // Only the calls path passes `constrain_output_natively = true`, so this
        // flags a node-less ephemeral call (CAIRN-2549). Codex pools these.
        is_ephemeral_call: constrain_output_natively,
    };

    backend.start_session(session_config, orch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, RowExt, SearchIndex};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn project_checks_section_lists_check_contract() {
        let checks = std::collections::HashMap::from([
            (
                "rust".to_string(),
                crate::config::project_settings::CheckCommand {
                    command: "cargo test {targets}".to_string(),
                    impact: Some(vec!["src-tauri/**".to_string()]),
                    policy: crate::config::project_settings::CheckPolicy::Gate,
                    when: crate::config::project_settings::CheckWhen::Review,
                    timeout: None,
                },
            ),
            (
                "frontend".to_string(),
                crate::config::project_settings::CheckCommand {
                    command: "vitest run".to_string(),
                    impact: None,
                    policy: crate::config::project_settings::CheckPolicy::Advisory,
                    when: crate::config::project_settings::CheckWhen::Write,
                    timeout: None,
                },
            ),
        ]);

        let section = build_project_checks_section(&checks).unwrap();
        assert!(section.contains("## Project checks"));
        assert!(section.contains("**frontend**: `vitest run`"));
        assert!(section.contains("policy: `advisory`"));
        assert!(section.contains("when: `write`"));
        assert!(section.contains("**rust**: `cargo test {targets}`"));
        assert!(section.contains("policy: `gate`"));
        assert!(section.contains("when: `review`"));
    }

    #[test]
    fn orientation_block_states_run_coordinates() {
        let block = build_orientation_block(
            "/work/CAIRN-1288-builder-0",
            "cairn://p/CAIRN/1288/1/builder",
            Some("CAIRN"),
            Some("/repos/cairn"),
            Some("main"),
            Some("/tmp/cairn-scratch-job-1"),
            Some("claude-opus-4"),
            false,
        );
        assert!(block.contains("## Orientation"));
        // The resolved model is surfaced as part of the per-run dynamic tail.
        assert!(block.contains("Model: `claude-opus-4`"));
        assert!(block.contains("/work/CAIRN-1288-builder-0"));
        assert!(block.contains("cairn://p/CAIRN/1288/1/builder"));
        assert!(block.contains("cairn:~/"));
        assert!(block.contains("Project: `CAIRN`"));
        // Repository root is relabeled so it can't be mistaken for the working tree.
        assert!(block.contains("Repository root"));
        assert!(block.contains("do not `cd` here"));
        assert!(block.contains("`/repos/cairn`"));
        assert!(block.contains("Base branch: `main`"));
        // Platform is always present, regardless of optional fields.
        assert!(block.contains(std::env::consts::OS));
        // Scratch dir is surfaced as the agent's TMPDIR when provided.
        assert!(block.contains("Scratch dir (TMPDIR): `/tmp/cairn-scratch-job-1`"));
        assert!(block.contains("$TMPDIR"));
        // A worktree-backed run carries no ambient framing.
        assert!(!block.contains("## Capability tier"));
    }

    #[test]
    fn orientation_block_ambient_variant_relabels_coordinates() {
        // Ambient run: cwd IS the repo root, no worktree.
        let block = build_orientation_block(
            "/repos/cairn",
            "cairn://p/CAIRN/1/1/manager",
            Some("CAIRN"),
            Some("/repos/cairn"),
            Some("main"),
            Some("/tmp/cairn-scratch-job-1"),
            Some("claude-opus-4"),
            true,
        );
        // cwd is relabeled as the live checkout.
        assert!(block.contains("the project's live checkout (shared with the user)"));
        // The contradictory repo-root "do not cd here" line is omitted.
        assert!(!block.contains("do not `cd` here"));
        assert!(!block.contains("NOT your working tree"));
        // Version-control tiering now lives in the shared CAIRN segment
        // (`cairn_system_prompt(ambient)`), not an orientation override paragraph.
        assert!(!block.contains("## Capability tier"));
        assert!(!block.contains("overrides the Version Control section"));
    }

    #[test]
    fn is_ambient_run_only_for_no_worktree_on_repo_root() {
        // Ambient: no worktree, cwd == repo root.
        assert!(is_ambient_run(None, "/repos/cairn", Some("/repos/cairn")));
        // Worktree-backed: cwd is the worktree, not the repo root.
        assert!(!is_ambient_run(
            Some("/work/wt"),
            "/work/wt",
            Some("/repos/cairn")
        ));
        // Scratch-dir call/workflow (CallWorktree::None): no worktree, but cwd is
        // a scratch dir, not the repo root — NOT ambient.
        assert!(!is_ambient_run(
            None,
            "/tmp/cairn-call-xyz",
            Some("/repos/cairn")
        ));
        // No repo root known — cannot be ambient.
        assert!(!is_ambient_run(None, "/repos/cairn", None));
    }

    #[tokio::test]
    async fn session_context_uses_job_base_branch_for_child_issue_runs() {
        let db = Arc::new(migrated_db().await);
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at)
             VALUES('w', 'Workspace', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES('proj', 'w', 'Project', 'CAIRN', '/repos/cairn', 'main', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES('parent', 'proj', 1, 'Parent', 'active', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, parent_issue_id, created_at, updated_at)
             VALUES('child', 'proj', 2, 'Child', 'active', 'parent', 1, 1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES('exec-child', 'recipe', 'child', 'proj', 'running', 1, 1);
            INSERT INTO jobs(id, execution_id, issue_id, project_id, status, uri_segment, node_name, base_branch, created_at, updated_at)
             VALUES('job-child', 'exec-child', 'child', 'proj', 'running', 'builder', 'builder', 'agent/CAIRN-1-coordinator-0', 1, 1);
            INSERT INTO runs(id, issue_id, project_id, job_id, status, created_at, updated_at)
             VALUES('run-child', 'child', 'proj', 'job-child', 'running', 1, 1);
            ",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(Arc::clone(&db));

        let context = session_db_context(&orch, "run-child").unwrap();

        assert_eq!(
            context.effective_base_branch.as_deref(),
            Some("agent/CAIRN-1-coordinator-0")
        );
        assert_ne!(context.effective_base_branch.as_deref(), Some("main"));
    }

    #[test]
    fn orientation_block_omits_missing_optionals() {
        let block = build_orientation_block(
            "/work/wt",
            "cairn://p/P/1/1/node",
            None,
            None,
            None,
            None,
            None,
            false,
        );
        assert!(block.contains("Working directory (cwd): `/work/wt`"));
        assert!(!block.contains("Project:"));
        assert!(!block.contains("Repository root"));
        assert!(!block.contains("Base branch:"));
        assert!(!block.contains("Model:"));
        assert!(block.contains("Platform:"));
        // No scratch line when the run has no owning job.
        assert!(!block.contains("Scratch dir"));
    }

    fn ctx_self_target(name: &str, schema: serde_json::Value) -> crate::models::OutputSchemaInfo {
        crate::models::OutputSchemaInfo {
            schema: crate::models::OutputSchema::Custom(schema),
            artifact_name: Some(name.to_string()),
            confirm_policy: crate::models::ConfirmPolicy::Auto,
            tool_name: None,
            description: None,
        }
    }

    #[test]
    fn ctx_self_section_absent_for_empty_targets() {
        assert!(build_ctx_self_section(&[], None).is_none());
    }

    #[test]
    fn ctx_self_section_states_living_doc_contract() {
        let targets = vec![ctx_self_target(
            "board",
            serde_json::json!({
                "type": "object",
                "required": ["scratch"],
                "properties": {
                    "scratch": {"type": "string", "description": "freeform notes"},
                    "items": {"type": "array"}
                }
            }),
        )];
        let section = build_ctx_self_section(&targets, None)
            .expect("a named, schema-bearing target should render a section");
        // Names the living doc by its addressable URI.
        assert!(section.contains("cairn:~/board"));
        // States the defining contract: repeated create/patch, no turn end, no
        // DAG advance — the contrast with the terminal output artifact.
        assert!(section.contains("`patch`"));
        assert!(section.contains("never ends your turn"));
        assert!(section.contains("advances the workflow"));
        // Surfaces the typed fields, required vs optional.
        assert!(section.contains("`scratch` (string, required): freeform notes"));
        assert!(section.contains("`items` (array, optional)"));
    }

    #[test]
    fn ctx_self_section_lists_every_named_target() {
        let targets = vec![
            ctx_self_target("plan", serde_json::json!({"type": "object"})),
            ctx_self_target("notes", serde_json::json!({"type": "object"})),
        ];
        let section = build_ctx_self_section(&targets, None).expect("two targets render");
        assert!(section.contains("cairn:~/plan"));
        assert!(section.contains("cairn:~/notes"));
    }

    #[test]
    fn ctx_self_section_skips_unnamed_targets() {
        let unnamed = crate::models::OutputSchemaInfo {
            schema: crate::models::OutputSchema::Custom(serde_json::json!({"type": "object"})),
            artifact_name: None,
            confirm_policy: crate::models::ConfirmPolicy::Auto,
            tool_name: None,
            description: None,
        };
        assert!(build_ctx_self_section(&[unnamed], None).is_none());
    }

    #[test]
    fn render_schema_fields_marks_required_and_optional() {
        let schema = serde_json::json!({
            "type": "object",
            "required": ["title"],
            "properties": {
                "title": {"type": "string", "description": "the heading"},
                "body": {"type": "string"}
            }
        });
        let rendered = render_schema_fields(&schema);
        assert!(rendered.starts_with("Fields:\n"));
        assert!(rendered.contains("`title` (string, required): the heading"));
        assert!(rendered.contains("`body` (string, optional)"));
    }

    #[test]
    fn render_schema_fields_empty_without_properties() {
        assert!(render_schema_fields(&serde_json::json!({"type": "object"})).is_empty());
    }

    #[test]
    fn available_terminals_section_renders_for_non_empty_list() {
        let cmds = vec![
            crate::models::TerminalCommand {
                name: "Dev Server".to_string(),
                command: "npm run dev".to_string(),
                write: vec![],
            },
            crate::models::TerminalCommand {
                name: "Tests".to_string(),
                command: "bun run test".to_string(),
                write: vec![],
            },
        ];
        let section = build_available_terminals_section(&cmds)
            .expect("non-empty list should render a section");
        assert!(section.contains("## Available Terminals"));
        // Each shortcut by name + command.
        assert!(section.contains("- **Dev Server**: `npm run dev`"));
        assert!(section.contains("- **Tests**: `bun run test`"));
        // A runnable create example: the system-generated slug for the first
        // shortcut plus the command in the payload (create requires it).
        assert!(section.contains("cairn:~/terminal/dev-server"));
        assert!(section.contains("mode:\"create\""));
        assert!(section.contains("command:\"npm run dev\""));
    }

    #[test]
    fn available_terminals_section_absent_for_empty_list() {
        assert!(build_available_terminals_section(&[]).is_none());
    }

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("session-channel-cursor.db").await
    }

    fn test_orchestrator(db: Arc<LocalDb>) -> Orchestrator {
        let temp = tempdir().unwrap();
        let config_dir = temp.keep();
        let index_path = config_dir.join("search-index.db");
        let db_state = Arc::new(DbState::new(
            db,
            Arc::new(SearchIndex::open_or_create(index_path).unwrap()),
        ));
        let services = Arc::new(TestServicesBuilder::new().build());
        Orchestrator::builder(db_state, services, config_dir).build()
    }

    async fn seed_run(db: &LocalDb, run_id: &str) {
        let run_id = run_id.to_string();
        db.write(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO runs(id, status, created_at, updated_at) VALUES(?1, 'running', 1, 1)",
                    (run_id.as_str(),),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn fetch_system_prompt_events(db: &LocalDb, run_id: &str) -> Vec<TranscriptEvent> {
        let run_id = run_id.to_string();
        db.read(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT data FROM events WHERE run_id = ?1 AND event_type = 'system:prompt' ORDER BY sequence ASC",
                        (run_id.as_str(),),
                    )
                    .await?;
                let mut events = Vec::new();
                while let Some(row) = rows.next().await? {
                    let data = row.text(0)?;
                    events.push(serde_json::from_str::<TranscriptEvent>(&data).unwrap());
                }
                Ok(events)
            })
        })
        .await
        .unwrap()
    }

    async fn insert_backend_like_event(
        db: &LocalDb,
        run_id: &str,
        session_id: Option<&str>,
        sequence: i32,
        event_type: &str,
    ) {
        let run_id = run_id.to_string();
        let session_id = session_id.map(str::to_string);
        let event_type = event_type.to_string();
        db.write(|conn| {
            let run_id = run_id.clone();
            let session_id = session_id.clone();
            let event_type = event_type.clone();
            Box::pin(async move {
                let event = TranscriptEvent {
                    event_type: event_type.clone(),
                    session_id: session_id.clone(),
                    parent_tool_use_id: None,
                    content: Some("normal event".to_string()),
                    thinking: None,
                    tool_name: None,
                    tool_input: None,
                    tool_uses: None,
                    tool_use_id: None,
                    tool_result: None,
                    is_error: false,
                    thinking_ms: None,
                    raw: None,
                };
                let data = serde_json::to_string(&event).unwrap();
                conn.execute(
                    "INSERT INTO events (
                        id, run_id, session_id, sequence, timestamp, event_type, data,
                        parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                        cache_create_tokens, output_tokens, turn_id
                     ) VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6, NULL, 1, NULL, NULL, NULL, NULL, NULL)",
                    params![
                        ids::mint_child(&run_id),
                        run_id,
                        session_id,
                        sequence,
                        event_type,
                        data,
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn fetch_event_sequences(db: &LocalDb, run_id: &str) -> Vec<(String, i64)> {
        let run_id = run_id.to_string();
        db.read(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT event_type, sequence FROM events WHERE run_id = ?1 ORDER BY sequence ASC",
                        (run_id.as_str(),),
                    )
                    .await?;
                let mut events = Vec::new();
                while let Some(row) = rows.next().await? {
                    events.push((row.text(0)?, row.i64(1)?));
                }
                Ok(events)
            })
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn persist_system_prompt_event_inserts_prompt_content() {
        let db = Arc::new(migrated_db().await);
        seed_run(&db, "run-prompt").await;
        let orch = test_orchestrator(Arc::clone(&db));

        let next_sequence = persist_system_prompt_event(
            &orch,
            "run-prompt",
            Some("sess"),
            "codex",
            &[PromptSegment::new(SEGMENT_KIND_DYNAMIC, "rendered prompt")],
        );

        assert_eq!(next_sequence, 1);
        let events = fetch_system_prompt_events(&db, "run-prompt").await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "system:prompt");
        assert_eq!(events[0].session_id.as_deref(), Some("sess"));
        assert_eq!(events[0].content.as_deref(), Some("rendered prompt"));
        let raw = events[0].raw.as_ref().unwrap();
        assert_eq!(raw.get("backend").and_then(|v| v.as_str()), Some("codex"));
        assert_eq!(raw.get("bytes").and_then(|v| v.as_u64()), Some(15));
        assert!(raw.get("includesBackendBase").is_none());
        assert!(raw.get("hash").and_then(|v| v.as_str()).unwrap().len() >= 64);
    }

    #[tokio::test]
    async fn persist_system_prompt_event_returns_sequence_for_next_backend_event() {
        let db = Arc::new(migrated_db().await);
        seed_run(&db, "run-prompt-sequence").await;
        let orch = test_orchestrator(Arc::clone(&db));

        let mut backend_sequence = persist_system_prompt_event(
            &orch,
            "run-prompt-sequence",
            Some("sess"),
            "codex",
            &[PromptSegment::new(SEGMENT_KIND_DYNAMIC, "same")],
        );
        insert_backend_like_event(
            &db,
            "run-prompt-sequence",
            Some("sess"),
            backend_sequence,
            "system:init",
        )
        .await;
        backend_sequence += 1;

        assert_eq!(backend_sequence, 2);
        assert_eq!(
            fetch_event_sequences(&db, "run-prompt-sequence").await,
            vec![
                ("system:prompt".to_string(), 0),
                ("system:init".to_string(), 1),
            ]
        );
    }

    #[tokio::test]
    async fn persist_system_prompt_event_dedupe_returns_sequence_after_existing_events() {
        let db = Arc::new(migrated_db().await);
        seed_run(&db, "run-prompt-dedupe").await;
        let orch = test_orchestrator(Arc::clone(&db));

        let first_next = persist_system_prompt_event(
            &orch,
            "run-prompt-dedupe",
            Some("sess"),
            "codex",
            &[PromptSegment::new(SEGMENT_KIND_DYNAMIC, "same")],
        );
        insert_backend_like_event(
            &db,
            "run-prompt-dedupe",
            Some("sess"),
            first_next,
            "system:init",
        )
        .await;

        let deduped_next = persist_system_prompt_event(
            &orch,
            "run-prompt-dedupe",
            Some("sess"),
            "codex",
            &[PromptSegment::new(SEGMENT_KIND_DYNAMIC, "same")],
        );
        let events = fetch_system_prompt_events(&db, "run-prompt-dedupe").await;
        assert_eq!(events.len(), 1);
        assert_eq!(deduped_next, 2);

        persist_system_prompt_event(
            &orch,
            "run-prompt-dedupe",
            Some("sess"),
            "codex",
            &[PromptSegment::new(SEGMENT_KIND_DYNAMIC, "changed")],
        );
        let events = fetch_system_prompt_events(&db, "run-prompt-dedupe").await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].content.as_deref(), Some("changed"));
        assert_eq!(
            fetch_event_sequences(&db, "run-prompt-dedupe").await,
            vec![
                ("system:prompt".to_string(), 0),
                ("system:init".to_string(), 1),
                ("system:prompt".to_string(), 2),
            ]
        );
    }

    /// The uniform assembly (cairn + workspace + project + agent) yields an
    /// ordered segment map whose concatenation is the composed prompt, and whose
    /// agent content is split into a static head and the inlined dynamic tail.
    /// Messaging catch-up rides the user turn, not the cached system prompt.
    /// The composer appends it after the task with a blank-line separator, and
    /// collapses cleanly when either side is empty so a pure wake or a quiet
    /// channel never injects stray whitespace.
    #[test]
    fn compose_user_message_appends_messaging_after_task() {
        assert_eq!(
            compose_user_message("do the thing", "## Agent Messaging\n\nhi"),
            "do the thing\n\n## Agent Messaging\n\nhi"
        );
        assert_eq!(compose_user_message("do the thing", ""), "do the thing");
        assert_eq!(
            compose_user_message("", "## Agent Messaging\n\nhi"),
            "## Agent Messaging\n\nhi"
        );
        assert_eq!(compose_user_message("", ""), "");
    }

    #[test]
    fn assemble_prompt_segments_splits_static_head_from_dynamic_tail() {
        let agent = "<agent_role>\nbuilder body\n\nORIENTATION\n</agent_role>";
        let dynamic = "\n\nORIENTATION\n</agent_role>";
        let segs = assemble_prompt_segments(
            "CAIRN",
            Some("ws doctrine"),
            Some("proj doctrine"),
            Some(agent),
            Some(dynamic),
        );
        let kinds: Vec<&str> = segs.iter().map(|s| s.kind).collect();
        assert_eq!(
            kinds,
            vec![
                SEGMENT_KIND_CAIRN,
                SEGMENT_KIND_WORKSPACE,
                SEGMENT_KIND_PROJECT,
                SEGMENT_KIND_AGENT,
                SEGMENT_KIND_DYNAMIC,
            ]
        );
        let full: String = segs.iter().map(|s| s.text.as_str()).collect();
        assert_eq!(
            full,
            "CAIRN\n\n## Workspace Instructions\n\nws doctrine\n\n## Project Instructions\n\nproj doctrine\n\n<agent_role>\nbuilder body\n\nORIENTATION\n</agent_role>"
        );
        assert_eq!(
            segs.iter()
                .find(|s| s.kind == SEGMENT_KIND_DYNAMIC)
                .unwrap()
                .text,
            dynamic
        );
        assert_eq!(
            segs.iter()
                .find(|s| s.kind == SEGMENT_KIND_AGENT)
                .unwrap()
                .text,
            "\n\n<agent_role>\nbuilder body"
        );
    }

    /// A run with no workspace or project doctrine omits those segments, and with
    /// no dynamic tail the agent content stays one static segment.
    #[test]
    fn assemble_prompt_segments_omits_absent_pieces() {
        let segs = assemble_prompt_segments(
            "CAIRN",
            None,
            None,
            Some("<agent_role>\nx\n</agent_role>"),
            None,
        );
        let kinds: Vec<&str> = segs.iter().map(|s| s.kind).collect();
        assert_eq!(kinds, vec![SEGMENT_KIND_CAIRN, SEGMENT_KIND_AGENT]);
        let full: String = segs.iter().map(|s| s.text.as_str()).collect();
        assert_eq!(full, "CAIRN\n\n<agent_role>\nx\n</agent_role>");
    }

    /// Codex derives its two payloads by slicing the same assembled segments, so
    /// base + developer reproduces the full prompt (and the persisted segments)
    /// exactly — byte-identity by construction.
    #[test]
    fn codex_base_and_developer_slices_recompose_the_full_prompt() {
        let agent = "<agent_role>\nbuilder body\n\nORIENTATION\n</agent_role>";
        let dynamic = "\n\nORIENTATION\n</agent_role>";
        let segs = assemble_prompt_segments(
            "CAIRN",
            Some("ws doctrine"),
            Some("proj doctrine"),
            Some(agent),
            Some(dynamic),
        );
        let base = base_instructions_from_segments(&segs);
        let developer = developer_instructions_from_segments(&segs).unwrap();
        // Base = cairn + workspace + project; developer = agent + dynamic.
        assert_eq!(
            base,
            "CAIRN\n\n## Workspace Instructions\n\nws doctrine\n\n## Project Instructions\n\nproj doctrine"
        );
        assert_eq!(
            developer,
            "\n\n<agent_role>\nbuilder body\n\nORIENTATION\n</agent_role>"
        );
        // base + developer == the flattened full prompt == persisted segments.
        assert_eq!(format!("{base}{developer}"), flatten_prompt_segments(&segs));
    }

    /// With no agent content there is nothing to send as developer instructions.
    #[test]
    fn developer_instructions_absent_without_agent_segment() {
        let segs = assemble_prompt_segments("CAIRN", Some("ws"), None, None, None);
        assert!(developer_instructions_from_segments(&segs).is_none());
        assert_eq!(
            base_instructions_from_segments(&segs),
            "CAIRN\n\n## Workspace Instructions\n\nws"
        );
    }

    /// Seed the minimal workspace/project/issue/job/session rows needed for the
    /// per-session channel cursor (`sess`).
    async fn seed_session(db: &LocalDb) {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
            VALUES('p', 'w', 'Project', 'PROJ', '/tmp/repo', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES('i', 'p', 1, 'Issue', 'backlog', 'backlog', 'none', 1, 1);
            INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
            VALUES('j', 'p', 'i', 'running', 1, 1);
            INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
            VALUES('sess', 'j', 'claude', 'open', 1, 1, 1);
            UPDATE jobs SET current_session_id = 'sess' WHERE id = 'j';
            ",
        )
        .await
        .unwrap();
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_msg(
        db: &LocalDb,
        id: &str,
        channel_type: &str,
        channel_id: &str,
        sender_run_id: Option<&str>,
        sender_name: &str,
        content: &str,
        created_at: i64,
    ) {
        let id = id.to_string();
        let channel_type = channel_type.to_string();
        let channel_id = channel_id.to_string();
        let sender_run_id = sender_run_id.map(str::to_string);
        let sender_name = sender_name.to_string();
        let content = content.to_string();
        db.write(|conn| {
            let id = id.clone();
            let channel_type = channel_type.clone();
            let channel_id = channel_id.clone();
            let sender_run_id = sender_run_id.clone();
            let sender_name = sender_name.clone();
            let content = content.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO messages(id, channel_type, channel_id, sender_run_id, sender_name, content, created_at)
                     VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![id, channel_type, channel_id, sender_run_id, sender_name, content, created_at],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn read_cursor(db: &LocalDb, session_id: &str) -> Option<i64> {
        let session_id = session_id.to_string();
        db.read(|conn| {
            let session_id = session_id.clone();
            Box::pin(async move { read_channel_cursor(conn, &session_id).await })
        })
        .await
        .unwrap()
    }

    async fn advance(db: &LocalDb, session_id: &str, rowid: i64) {
        let session_id = session_id.to_string();
        db.write(|conn| {
            let session_id = session_id.clone();
            Box::pin(async move { advance_channel_cursor(conn, &session_id, rowid).await })
        })
        .await
        .unwrap();
    }

    async fn fetch_recent(
        db: &LocalDb,
        issue_key: &str,
        exclude_job_id: Option<&str>,
        cursor: Option<i64>,
    ) -> Vec<PromptMessage> {
        let issue_key = issue_key.to_string();
        let exclude_job_id = exclude_job_id.map(str::to_string);
        db.read(|conn| {
            let issue_key = issue_key.clone();
            let exclude_job_id = exclude_job_id.clone();
            Box::pin(async move {
                recent_messages_for_run(
                    conn,
                    "PROJ",
                    Some(&issue_key),
                    exclude_job_id.as_deref(),
                    cursor,
                    20,
                    true,
                )
                .await
            })
        })
        .await
        .unwrap()
    }

    /// CAIRN-1302 Part 2: a channel message is injected at most once per
    /// session (the cursor advances), while a newer message still surfaces.
    #[tokio::test]
    async fn channel_injection_dedupes_per_session() {
        let db = migrated_db().await;
        seed_session(&db).await;
        insert_msg(
            &db,
            "m1",
            "issue",
            "PROJ/1",
            Some("run-child"),
            "system",
            "salvage-frozen finished successfully",
            1000,
        )
        .await;

        // First injection: empty cursor surfaces the message.
        let cursor0 = read_cursor(&db, "sess").await;
        assert!(cursor0.is_none());
        let first = fetch_recent(&db, "PROJ/1", Some("j"), cursor0).await;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].content, "salvage-frozen finished successfully");

        // Advancing to the newest injected rowid persists the cursor.
        let max_rowid = first.iter().map(|m| m.rowid).max().unwrap();
        advance(&db, "sess", max_rowid).await;
        let cursor1 = read_cursor(&db, "sess").await;
        assert_eq!(cursor1, Some(max_rowid));

        // Second injection on the same session: the message is deduped.
        let second = fetch_recent(&db, "PROJ/1", Some("j"), cursor1).await;
        assert!(
            second.is_empty(),
            "already-injected message must not re-surface"
        );

        // A later message still surfaces.
        insert_msg(
            &db,
            "m2",
            "issue",
            "PROJ/1",
            Some("run-child"),
            "system",
            "new notice",
            2000,
        )
        .await;
        let third = fetch_recent(&db, "PROJ/1", Some("j"), cursor1).await;
        assert_eq!(third.len(), 1);
        assert_eq!(third[0].content, "new notice");
    }

    /// Regression: a message arriving in the same wall-clock second as the
    /// cursor but inserted later (larger rowid) must still surface. A
    /// (created_at, id) cursor over random-UUID ids would drop it whenever the
    /// later id sorts lower; the monotonic rowid cursor never does.
    #[tokio::test]
    async fn channel_injection_surfaces_same_second_later_arrival() {
        let db = migrated_db().await;
        seed_session(&db).await;
        // First message at second 1000 with a high-sorting id.
        insert_msg(
            &db,
            "ffff",
            "issue",
            "PROJ/1",
            Some("run-child"),
            "system",
            "first",
            1000,
        )
        .await;
        let first = fetch_recent(&db, "PROJ/1", Some("j"), None).await;
        advance(&db, "sess", first.iter().map(|m| m.rowid).max().unwrap()).await;
        let cursor = read_cursor(&db, "sess").await;

        // Second message: SAME second, inserted later (larger rowid), but a
        // lexicographically-smaller id than the cursor's id.
        insert_msg(
            &db,
            "0000",
            "issue",
            "PROJ/1",
            Some("run-child"),
            "system",
            "same-second later",
            1000,
        )
        .await;
        let next = fetch_recent(&db, "PROJ/1", Some("j"), cursor).await;
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].content, "same-second later");
    }

    #[tokio::test]
    async fn pending_channel_messages_exclude_system_lifecycle_noise() {
        let db = migrated_db().await;
        seed_session(&db).await;
        insert_msg(
            &db,
            "lifecycle",
            "issue",
            "PROJ/1",
            Some("run-peer"),
            "system",
            "builder finished successfully",
            1000,
        )
        .await;
        insert_msg(
            &db,
            "agent-message",
            "issue",
            "PROJ/1",
            Some("run-peer"),
            "builder",
            "I need the parent to review this output",
            2000,
        )
        .await;

        let pending = pending_channel_messages_for_job(&db, "j", 20)
            .await
            .unwrap();

        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].sender_name, "builder");
        assert_eq!(
            pending[0].content,
            "I need the parent to review this output"
        );
    }

    #[tokio::test]
    async fn recent_messages_exclude_every_run_from_recipient_job() {
        let db = migrated_db().await;
        seed_session(&db).await;
        db.execute_script(
            "
            INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
            VALUES('j-other', 'p', 'i', 'running', 1, 1);
            INSERT INTO runs(id, issue_id, project_id, job_id, status, created_at, updated_at)
            VALUES('run-self', 'i', 'p', 'j', 'completed', 1, 1),
                  ('run-other', 'i', 'p', 'j-other', 'completed', 1, 1);
            ",
        )
        .await
        .unwrap();
        insert_msg(
            &db,
            "self-lifecycle",
            "issue",
            "PROJ/1",
            Some("run-self"),
            "system",
            "builder finished successfully",
            1000,
        )
        .await;
        insert_msg(
            &db,
            "other-lifecycle",
            "issue",
            "PROJ/1",
            Some("run-other"),
            "system",
            "planner finished successfully",
            2000,
        )
        .await;

        let recent = fetch_recent(&db, "PROJ/1", Some("j"), None).await;

        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].content, "planner finished successfully");
    }
}
