//! MCP request handlers organized by domain.
//!
//! Framework-agnostic handler logic. Both Tauri and cairn-server dispatch to these.

use crate::jobs::queries::{find_node_in_snapshot, load_execution_snapshot};
use crate::models::CheckpointType;
use crate::schema::{executions, issues as issues_table, jobs, projects, runs};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

pub mod agents;
pub mod bash;
pub mod bug_report;
pub mod custom_tool;
pub mod execute;
pub mod external;
pub mod files;
pub mod implementation;
pub mod issue_resources;
pub mod issues;
pub mod memories;
pub mod messages;
pub mod permission;
pub mod planning;
pub mod resources;
pub mod search;
pub mod skills;
pub mod slug;
pub mod todos;

// Re-export slug utilities
pub use slug::{
    build_terminal_uri, ensure_unique_slug, generate_slug_from_title, generate_terminal_slug,
    slugify, slugify_command,
};

/// Payload for `agent-attention` events.
pub struct AttentionEvent<'a> {
    pub attention_type: &'a str,
    pub project_key: &'a str,
    pub issue_number: Option<i32>,
    pub issue_title: Option<&'a str>,
    pub node_name: Option<&'a str>,
    pub exec_seq: Option<i32>,
    pub tool_name: Option<&'a str>,
}

/// Emit an `agent-attention` event to notify the frontend that user attention is needed.
///
/// Used for: ask_user prompts, permission requests, job completed/failed.
pub fn emit_attention(emitter: &dyn crate::services::EventEmitter, event: &AttentionEvent) {
    let _ = emitter.emit(
        "agent-attention",
        serde_json::json!({
            "type": event.attention_type,
            "projectKey": event.project_key,
            "issueNumber": event.issue_number,
            "issueTitle": event.issue_title,
            "nodeName": event.node_name,
            "execSeq": event.exec_seq,
            "toolName": event.tool_name,
        }),
    );
}

/// Look up an issue's title by its ID.
pub fn get_issue_title(conn: &mut SqliteConnection, issue_id: &str) -> Option<String> {
    issues_table::table
        .find(issue_id)
        .select(issues_table::title)
        .first::<String>(conn)
        .ok()
}

/// Information about a run looked up by worktree path
pub struct RunContext {
    pub run_id: String,
    pub job_id: String,
    pub execution_id: Option<String>, // Groups related jobs in a recipe execution
    pub exec_seq: Option<i32>,        // Monotonic execution sequence number (for URIs)
    pub issue_id: Option<String>,     // Null for project-level runs
    pub issue_number: Option<i32>, // Issue number for building issue keys (e.g., 123 for CAIRN-123)
    pub project_id: String,
    pub project_key: String,
    pub job_type: String,         // From recipe_step node_type or inferred
    pub job_name: Option<String>, // Human-readable job name from execution snapshot (e.g., "builder-1")
}

impl RunContext {
    /// Get the issue key (e.g., "CAIRN-123") or None for project-level runs
    #[allow(dead_code)]
    pub fn issue_key(&self) -> Option<String> {
        self.issue_number
            .map(|num| format!("{}-{}", self.project_key, num))
    }
}

/// Minimal project context for external tools (no active run required)
#[derive(Debug)]
pub struct ProjectContext {
    pub project_id: String,
    pub project_key: String,
}

/// Look up the active run by working directory (worktree_path or repo_path).
/// For issue jobs: matches worktree_path
/// For project jobs: matches repo_path directly
/// Returns run context or an error message.
pub fn lookup_run_by_cwd(conn: &mut SqliteConnection, cwd: &str) -> Result<RunContext, String> {
    // First try: worktree-based lookup (issue jobs)
    let worktree_result: Result<RunContext, _> = runs::table
        .inner_join(jobs::table.on(runs::job_id.eq(jobs::id.nullable())))
        .inner_join(issues_table::table.on(runs::issue_id.eq(issues_table::id.nullable())))
        .inner_join(projects::table.on(issues_table::project_id.eq(projects::id)))
        .filter(jobs::worktree_path.eq(cwd))
        .filter(runs::status.eq_any(&["starting", "live"]))
        .order(runs::created_at.desc())
        .select((
            runs::id,
            runs::job_id,
            jobs::execution_id,
            jobs::recipe_node_id,
            runs::issue_id,
            issues_table::number,
            issues_table::project_id,
            projects::key,
            jobs::node_name,
        ))
        .first::<(
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            i32,
            String,
            String,
            Option<String>,
        )>(conn)
        .map(
            |(
                run_id,
                job_id,
                execution_id,
                recipe_node_id,
                issue_id,
                issue_number,
                project_id,
                project_key,
                stored_node_name,
            )| {
                // Get node_type and node_name from execution snapshot (recipe nodes)
                let (node_type, node_name) = execution_id
                    .as_ref()
                    .and_then(|exec_id| load_execution_snapshot(conn, exec_id).ok())
                    .and_then(|snapshot| {
                        recipe_node_id
                            .as_ref()
                            .and_then(|nid| find_node_in_snapshot(&snapshot, nid))
                            .map(|n| (Some(n.node_type.to_string()), Some(n.name.clone())))
                    })
                    .unwrap_or((None, None));

                // For task-spawned agents: fall back to stored node_name from DB
                let node_name = node_name.or(stored_node_name);

                // Resolve exec_seq from execution_id
                let exec_seq = execution_id.as_deref().and_then(|eid| {
                    executions::table
                        .find(eid)
                        .select(executions::seq)
                        .first::<Option<i32>>(conn)
                        .ok()
                        .flatten()
                });

                RunContext {
                    run_id,
                    job_id: job_id.unwrap_or_default(),
                    execution_id,
                    exec_seq,
                    issue_id,
                    issue_number: Some(issue_number),
                    project_id,
                    project_key,
                    // Map node_type: "agent" is the new implementation type
                    job_type: match node_type.as_deref() {
                        Some("agent") => "implementation".to_string(),
                        Some(t) => t.to_string(),
                        None => "implementation".to_string(),
                    },
                    job_name: node_name,
                }
            },
        );

    if worktree_result.is_ok() {
        return worktree_result.map_err(|e| e.to_string());
    }

    // Fallback: repo_path lookup (project-level jobs use repo_path directly)
    // Project jobs are identified by project_id set and issue_id null
    runs::table
        .inner_join(jobs::table.on(runs::job_id.eq(jobs::id.nullable())))
        .inner_join(projects::table.on(jobs::project_id.eq(projects::id)))
        .filter(projects::repo_path.eq(cwd))
        .filter(runs::status.eq_any(&["starting", "live"]))
        .filter(jobs::issue_id.is_null())
        .order(runs::created_at.desc())
        .select((
            runs::id,
            runs::job_id,
            jobs::execution_id,
            runs::issue_id,
            jobs::project_id,
            projects::key,
        ))
        .first::<(
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
            String,
        )>(conn)
        .map(
            |(run_id, job_id, execution_id, issue_id, project_id, project_key)| {
                RunContext {
                    run_id,
                    job_id: job_id.unwrap_or_default(),
                    execution_id,
                    exec_seq: None, // Project-level jobs have no execution sequence
                    issue_id,
                    issue_number: None, // Project-level jobs have no issue
                    project_id,
                    project_key,
                    job_type: "project".to_string(), // Project-level jobs
                    job_name: None,                  // Project-level jobs have no node name
                }
            },
        )
        .map_err(|e| format!("No active run found for path '{}': {}", cwd, e))
}

/// Look up run by ID directly (preferred for accuracy).
/// This avoids the ambiguity of cwd-based lookup when multiple runs share a worktree.
pub fn lookup_run_by_id(conn: &mut SqliteConnection, run_id: &str) -> Result<RunContext, String> {
    runs::table
        .inner_join(jobs::table.on(runs::job_id.eq(jobs::id.nullable())))
        .left_join(issues_table::table.on(runs::issue_id.eq(issues_table::id.nullable())))
        .inner_join(projects::table.on(jobs::project_id.eq(projects::id)))
        .filter(runs::id.eq(run_id))
        .select((
            runs::id,
            runs::job_id,
            jobs::execution_id,
            jobs::recipe_node_id,
            runs::issue_id,
            issues_table::number.nullable(),
            jobs::project_id,
            projects::key,
            jobs::node_name,
        ))
        .first::<(
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<i32>,
            String,
            String,
            Option<String>,
        )>(conn)
        .map(
            |(
                run_id,
                job_id,
                execution_id,
                recipe_node_id,
                issue_id,
                issue_number,
                project_id,
                project_key,
                stored_node_name,
            )| {
                // Get node_type and node_name from execution snapshot (recipe nodes)
                let (node_type, node_name) = execution_id
                    .as_ref()
                    .and_then(|exec_id| load_execution_snapshot(conn, exec_id).ok())
                    .and_then(|snapshot| {
                        recipe_node_id
                            .as_ref()
                            .and_then(|nid| find_node_in_snapshot(&snapshot, nid))
                            .map(|n| (Some(n.node_type.to_string()), Some(n.name.clone())))
                    })
                    .unwrap_or((None, None));

                // For task-spawned agents: fall back to stored node_name from DB
                let node_name = node_name.or(stored_node_name);

                // Resolve exec_seq from execution_id
                let exec_seq = execution_id.as_deref().and_then(|eid| {
                    executions::table
                        .find(eid)
                        .select(executions::seq)
                        .first::<Option<i32>>(conn)
                        .ok()
                        .flatten()
                });

                RunContext {
                    run_id,
                    job_id: job_id.unwrap_or_default(),
                    execution_id,
                    exec_seq,
                    issue_id,
                    issue_number,
                    project_id,
                    project_key,
                    job_type: match node_type.as_deref() {
                        Some("agent") => "implementation".to_string(),
                        Some(t) => t.to_string(),
                        None => "implementation".to_string(),
                    },
                    job_name: node_name,
                }
            },
        )
        .map_err(|e| format!("No run found with id '{}': {}", run_id, e))
}

/// Look up run using run_id if provided, otherwise fall back to cwd.
///
/// Internal agents always have run_id (set via CAIRN_RUN_ID env var). When run_id is
/// present it is authoritative — a lookup failure means the run is gone, not a signal
/// to guess by CWD (which would silently resolve to the wrong project in a worktree).
///
/// External callers (e.g. Claude Code) never have run_id, so CWD fallback is used.
pub fn lookup_run(
    conn: &mut SqliteConnection,
    request: &crate::mcp::types::McpCallbackRequest,
) -> Result<RunContext, String> {
    if let Some(ref run_id) = request.run_id {
        // run_id is authoritative for internal agents — don't fall back to CWD
        lookup_run_by_id(conn, run_id)
    } else {
        lookup_run_by_cwd(conn, &request.cwd)
    }
}

/// Look up project context via the run chain (run_id → job → project).
/// Use this for internal tools that only need project_id (list_issues, skills, etc.)
pub fn lookup_project_context(
    conn: &mut SqliteConnection,
    request: &crate::mcp::types::McpCallbackRequest,
) -> Result<ProjectContext, String> {
    lookup_run(conn, request).map(|run_ctx| ProjectContext {
        project_id: run_ctx.project_id,
        project_key: run_ctx.project_key,
    })
}

/// Parse issue identifier into optional project key and issue number.
/// Returns (project_key, issue_number) - project_key is None if not specified.
///
/// Supported formats:
/// - "37" -> (None, 37)
/// - "#37" -> (None, 37)
/// - "CAIRN-37" -> (Some("CAIRN"), 37)
pub fn parse_issue_identifier(input: &str) -> Option<(Option<String>, i32)> {
    let trimmed = input.trim();

    // Try direct parse first (e.g., "37")
    if let Ok(n) = trimmed.parse::<i32>() {
        return Some((None, n));
    }

    // Handle "#37" format
    if let Some(stripped) = trimmed.strip_prefix('#') {
        if let Ok(n) = stripped.parse::<i32>() {
            return Some((None, n));
        }
    }

    // Handle "PREFIX-37" format (case-insensitive)
    if let Some(pos) = trimmed.rfind('-') {
        let prefix = &trimmed[..pos];
        if let Ok(n) = trimmed[pos + 1..].parse::<i32>() {
            return Some((Some(prefix.to_uppercase()), n));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diesel_models::{NewExecution, NewJob, NewRun};
    use crate::schema::runs;
    use crate::test_utils::{create_test_issue, create_test_project, test_diesel_conn};

    /// Helper: insert a job with optional node_name, worktree_path, execution_id, agent_config_id
    fn insert_job(
        conn: &mut SqliteConnection,
        id: &str,
        project_id: &str,
        issue_id: Option<&str>,
        node_name: Option<&str>,
        worktree_path: Option<&str>,
        execution_id: Option<&str>,
        parent_job_id: Option<&str>,
        agent_config_id: Option<&str>,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_job = NewJob {
            id,
            execution_id,
            manager_id: None,
            recipe_node_id: None,
            parent_job_id,
            worktree_path,
            branch: None,
            base_commit: None,
            current_session_id: None,
            resume_session_id: None,
            status: "running",
            agent_config_id,
            issue_id,
            project_id,
            task_description: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
            parent_tool_use_id: None,
            task_index: None,
            started_at: Some(now),
            model: None,
            node_name,
            base_branch: None,
            current_turn_id: None,
        };
        diesel::insert_into(jobs::table)
            .values(&new_job)
            .execute(conn)
            .expect("insert job");
    }

    /// Helper: insert a run linked to a job
    fn insert_run(
        conn: &mut SqliteConnection,
        id: &str,
        job_id: &str,
        issue_id: Option<&str>,
        project_id: Option<&str>,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_run = NewRun {
            id,
            issue_id,
            project_id,
            job_id: Some(job_id),
            status: Some("live"),
            session_id: None,
            error_message: None,
            started_at: Some(now),
            exited_at: None,
            created_at: now,
            updated_at: now,
            backend: None,
            exit_reason: None,
            start_mode: None,
            chat_id: None,
        };
        diesel::insert_into(runs::table)
            .values(&new_run)
            .execute(conn)
            .expect("insert run");
    }

    // =========================================================================
    // lookup_run_by_id: node_name fallback
    // =========================================================================

    #[test]
    fn lookup_run_by_id_returns_stored_node_name_for_task_agent() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test issue");

        // Job with node_name but no execution snapshot (task-spawned agent)
        insert_job(
            &mut conn,
            "job-1",
            &project_id,
            Some(&issue_id),
            Some("Explore"),
            None,
            None,
            None,
            Some("Explore"),
        );
        insert_run(
            &mut conn,
            "run-1",
            "job-1",
            Some(&issue_id),
            Some(&project_id),
        );

        let ctx = lookup_run_by_id(&mut conn, "run-1").unwrap();
        assert_eq!(ctx.job_name, Some("Explore".to_string()));
    }

    #[test]
    fn lookup_run_by_id_returns_none_name_when_no_node_name_stored() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test issue");

        // Job with no node_name and no execution snapshot
        insert_job(
            &mut conn,
            "job-1",
            &project_id,
            Some(&issue_id),
            None, // no node_name
            None,
            None,
            None,
            None,
        );
        insert_run(
            &mut conn,
            "run-1",
            "job-1",
            Some(&issue_id),
            Some(&project_id),
        );

        let ctx = lookup_run_by_id(&mut conn, "run-1").unwrap();
        assert_eq!(ctx.job_name, None);
    }

    #[test]
    fn normalize_command_unwraps_shell_launcher_prefixes() {
        assert_eq!(
            normalize_command(r#"/bin/zsh -lc "git show HEAD~1:src/lib.rs | sed -n '1,5p'""#),
            "git show HEAD~1:src/lib.rs | sed -n '1,5p'"
        );
        assert_eq!(
            normalize_command("bash -lc 'git status --short'"),
            "git status --short"
        );
        assert_eq!(normalize_command("  bun   run   test  "), "bun run test");
    }

    #[test]
    fn lookup_run_by_id_returns_suffixed_name_for_second_task() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test issue");

        // Parent job
        insert_job(
            &mut conn,
            "parent-job",
            &project_id,
            Some(&issue_id),
            Some("Builder"),
            None,
            None,
            None,
            Some("Build"),
        );

        // Second Explore task — should have "Explore-2" stored in DB
        insert_job(
            &mut conn,
            "job-2",
            &project_id,
            Some(&issue_id),
            Some("Explore-2"),
            None,
            None,
            Some("parent-job"),
            Some("Explore"),
        );
        insert_run(
            &mut conn,
            "run-2",
            "job-2",
            Some(&issue_id),
            Some(&project_id),
        );

        let ctx = lookup_run_by_id(&mut conn, "run-2").unwrap();
        assert_eq!(ctx.job_name, Some("Explore-2".to_string()));
    }

    // =========================================================================
    // lookup_run_by_cwd: node_name fallback
    // =========================================================================

    #[test]
    fn lookup_run_by_cwd_returns_stored_node_name() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test issue");

        // Job with node_name and worktree_path
        insert_job(
            &mut conn,
            "job-1",
            &project_id,
            Some(&issue_id),
            Some("Explore"),
            Some("/tmp/worktree-1"),
            None,
            None,
            Some("Explore"),
        );
        insert_run(
            &mut conn,
            "run-1",
            "job-1",
            Some(&issue_id),
            Some(&project_id),
        );

        let ctx = lookup_run_by_cwd(&mut conn, "/tmp/worktree-1").unwrap();
        assert_eq!(ctx.job_name, Some("Explore".to_string()));
    }

    #[test]
    fn lookup_run_by_cwd_returns_none_name_without_node_name() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test issue");

        insert_job(
            &mut conn,
            "job-1",
            &project_id,
            Some(&issue_id),
            None,
            Some("/tmp/worktree-2"),
            None,
            None,
            None,
        );
        insert_run(
            &mut conn,
            "run-1",
            "job-1",
            Some(&issue_id),
            Some(&project_id),
        );

        let ctx = lookup_run_by_cwd(&mut conn, "/tmp/worktree-2").unwrap();
        assert_eq!(ctx.job_name, None);
    }

    // =========================================================================
    // exec_seq resolution
    // =========================================================================

    /// Helper: insert an execution with a given seq
    fn insert_execution(
        conn: &mut SqliteConnection,
        id: &str,
        issue_id: Option<&str>,
        project_id: Option<&str>,
        seq: Option<i32>,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::insert_into(executions::table)
            .values(&NewExecution {
                id,
                recipe_id: "recipe-1",
                issue_id,
                project_id,
                status: "running",
                started_at: now,
                completed_at: None,
                snapshot: None,
                seq,
                initiator_sub: None,
                initiator_auth_mode: None,
                initiator_org_id: None,
                triggered_by: "manual",
            })
            .execute(conn)
            .expect("insert execution");
    }

    #[test]
    fn lookup_run_by_id_resolves_exec_seq_from_execution() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test issue");

        insert_execution(
            &mut conn,
            "exec-1",
            Some(&issue_id),
            Some(&project_id),
            Some(3),
        );
        insert_job(
            &mut conn,
            "job-1",
            &project_id,
            Some(&issue_id),
            Some("Builder"),
            None,
            Some("exec-1"),
            None,
            None,
        );
        insert_run(
            &mut conn,
            "run-1",
            "job-1",
            Some(&issue_id),
            Some(&project_id),
        );

        let ctx = lookup_run_by_id(&mut conn, "run-1").unwrap();
        assert_eq!(ctx.exec_seq, Some(3));
    }

    #[test]
    fn lookup_run_by_id_returns_none_exec_seq_without_execution() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test issue");

        // Job with no execution_id
        insert_job(
            &mut conn,
            "job-1",
            &project_id,
            Some(&issue_id),
            Some("Builder"),
            None,
            None,
            None,
            None,
        );
        insert_run(
            &mut conn,
            "run-1",
            "job-1",
            Some(&issue_id),
            Some(&project_id),
        );

        let ctx = lookup_run_by_id(&mut conn, "run-1").unwrap();
        assert_eq!(ctx.exec_seq, None);
    }

    #[test]
    fn lookup_run_by_cwd_resolves_exec_seq_from_execution() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test issue");

        insert_execution(
            &mut conn,
            "exec-2",
            Some(&issue_id),
            Some(&project_id),
            Some(5),
        );
        insert_job(
            &mut conn,
            "job-1",
            &project_id,
            Some(&issue_id),
            Some("Builder"),
            Some("/tmp/worktree-exec"),
            Some("exec-2"),
            None,
            None,
        );
        insert_run(
            &mut conn,
            "run-1",
            "job-1",
            Some(&issue_id),
            Some(&project_id),
        );

        let ctx = lookup_run_by_cwd(&mut conn, "/tmp/worktree-exec").unwrap();
        assert_eq!(ctx.exec_seq, Some(5));
    }

    #[test]
    fn lookup_run_by_cwd_project_level_has_none_exec_seq() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");

        // Project-level job (no issue, uses repo_path)
        insert_job(
            &mut conn,
            "job-1",
            &project_id,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        insert_run(&mut conn, "run-1", "job-1", None, Some(&project_id));

        // Need to set repo_path on the project to match cwd lookup
        diesel::update(projects::table.find(&project_id))
            .set(projects::repo_path.eq("/tmp/project-repo"))
            .execute(&mut conn)
            .unwrap();

        let ctx = lookup_run_by_cwd(&mut conn, "/tmp/project-repo").unwrap();
        assert_eq!(ctx.exec_seq, None);
    }

    // =========================================================================
    // get_issue_title
    // =========================================================================

    #[test]
    fn get_issue_title_returns_title_for_existing_issue() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Fix the widget");

        let title = get_issue_title(&mut conn, &issue_id);
        assert_eq!(title, Some("Fix the widget".to_string()));
    }

    #[test]
    fn get_issue_title_returns_none_for_missing_issue() {
        let mut conn = test_diesel_conn();

        let title = get_issue_title(&mut conn, "nonexistent-id");
        assert_eq!(title, None);
    }

    // =========================================================================
    // emit_attention
    // =========================================================================

    #[test]
    fn emit_attention_emits_correct_payload() {
        use crate::services::testing::CapturingEmitter;

        let emitter = CapturingEmitter::new();
        emit_attention(
            &emitter,
            &AttentionEvent {
                attention_type: "permission",
                project_key: "CAIRN",
                issue_number: Some(42),
                issue_title: Some("Fix bug"),
                node_name: Some("Builder"),
                exec_seq: Some(1),
                tool_name: Some("bash"),
            },
        );

        assert!(emitter.has_event("agent-attention"));
        let events = emitter.events_named("agent-attention");
        assert_eq!(events.len(), 1);
        let payload = &events[0];
        assert_eq!(payload["type"], "permission");
        assert_eq!(payload["projectKey"], "CAIRN");
        assert_eq!(payload["issueNumber"], 42);
        assert_eq!(payload["issueTitle"], "Fix bug");
        assert_eq!(payload["nodeName"], "Builder");
        assert_eq!(payload["execSeq"], 1);
        assert_eq!(payload["toolName"], "bash");
    }

    #[test]
    fn emit_attention_handles_none_fields() {
        use crate::services::testing::CapturingEmitter;

        let emitter = CapturingEmitter::new();
        emit_attention(
            &emitter,
            &AttentionEvent {
                attention_type: "completed",
                project_key: "TST",
                issue_number: None,
                issue_title: None,
                node_name: None,
                exec_seq: None,
                tool_name: None,
            },
        );

        let events = emitter.events_named("agent-attention");
        let payload = &events[0];
        assert_eq!(payload["type"], "completed");
        assert_eq!(payload["projectKey"], "TST");
        assert!(payload["issueNumber"].is_null());
        assert!(payload["issueTitle"].is_null());
        assert!(payload["nodeName"].is_null());
        assert!(payload["execSeq"].is_null());
        assert!(payload["toolName"].is_null());
    }

    // =========================================================================
    // lookup_run / lookup_project_context: run_id authoritative, no CWD fallback
    // =========================================================================

    fn make_request(cwd: &str, run_id: Option<&str>) -> crate::mcp::types::McpCallbackRequest {
        crate::mcp::types::McpCallbackRequest {
            cwd: cwd.to_string(),
            run_id: run_id.map(|s| s.to_string()),
            tool: "test".to_string(),
            payload: serde_json::Value::Null,
            tool_use_id: None,
        }
    }

    #[test]
    fn lookup_run_with_run_id_does_not_fallback_to_cwd() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test issue");
        insert_job(
            &mut conn,
            "job-1",
            &project_id,
            Some(&issue_id),
            None,
            Some("/tmp/worktree-run"),
            None,
            None,
            None,
        );
        insert_run(
            &mut conn,
            "run-1",
            "job-1",
            Some(&issue_id),
            Some(&project_id),
        );

        // Bogus run_id with a valid CWD — should NOT resolve via CWD
        let req = make_request("/tmp/worktree-run", Some("bogus-run-id"));
        let result = lookup_run(&mut conn, &req);
        assert!(result.is_err(), "expected error, not CWD fallback");
    }

    #[test]
    fn lookup_run_without_run_id_uses_cwd() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test issue");
        insert_job(
            &mut conn,
            "job-1",
            &project_id,
            Some(&issue_id),
            None,
            Some("/tmp/worktree-cwd"),
            None,
            None,
            None,
        );
        insert_run(
            &mut conn,
            "run-1",
            "job-1",
            Some(&issue_id),
            Some(&project_id),
        );

        // No run_id — CWD lookup should succeed
        let req = make_request("/tmp/worktree-cwd", None);
        let result = lookup_run(&mut conn, &req);
        assert!(result.is_ok(), "expected CWD lookup to succeed");
        assert_eq!(result.unwrap().project_key, "TST");
    }

    #[test]
    fn lookup_project_context_with_run_id_does_not_fallback_to_cwd() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test issue");
        insert_job(
            &mut conn,
            "job-1",
            &project_id,
            Some(&issue_id),
            None,
            Some("/tmp/worktree-ctx"),
            None,
            None,
            None,
        );
        insert_run(
            &mut conn,
            "run-1",
            "job-1",
            Some(&issue_id),
            Some(&project_id),
        );

        // Bogus run_id with a valid CWD — should NOT resolve via CWD
        let req = make_request("/tmp/worktree-ctx", Some("bogus-run-id"));
        let result = lookup_project_context(&mut conn, &req);
        assert!(result.is_err(), "expected error, not CWD fallback");
    }
}

/// Look up project by key (for cross-project queries).
/// Returns project context or an error message.
pub fn lookup_project_by_key(
    conn: &mut SqliteConnection,
    key: &str,
) -> Result<ProjectContext, String> {
    projects::table
        .filter(projects::key.eq(key.to_uppercase()))
        .select((projects::id, projects::key, projects::repo_path))
        .first::<(String, String, String)>(conn)
        .map(|(project_id, project_key, _repo_path)| ProjectContext {
            project_id,
            project_key,
        })
        .map_err(|_| format!("No project found with key '{}'", key))
}

// ============================================================================
// Checkpoint Cache Helper Functions
// ============================================================================

/// Strip a shell launcher wrapper and return the semantic inner command when possible.
///
/// This handles cases where a model passes a launcher command like
/// `/bin/zsh -lc "git status"` to the bash MCP tool instead of just `git status`.
pub fn unwrap_shell_launcher(cmd: &str) -> String {
    let trimmed = cmd.trim();
    let prefixes = [
        "/bin/zsh -lc ",
        "/bin/bash -lc ",
        "/bin/sh -lc ",
        "zsh -lc ",
        "bash -lc ",
        "sh -lc ",
        "/bin/zsh -c ",
        "/bin/bash -c ",
        "/bin/sh -c ",
        "zsh -c ",
        "bash -c ",
        "sh -c ",
    ];

    for prefix in prefixes {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            let rest = rest.trim();
            if rest.len() >= 2 {
                if let Some(inner) = rest
                    .strip_prefix('"')
                    .and_then(|s| s.strip_suffix('"'))
                    .map(|s| {
                        let mut out = String::with_capacity(s.len());
                        let mut chars = s.chars();
                        while let Some(ch) = chars.next() {
                            if ch == '\\' {
                                if let Some(next) = chars.next() {
                                    match next {
                                        '\\' | '"' | '$' | '`' => out.push(next),
                                        '\n' => {}
                                        other => {
                                            out.push('\\');
                                            out.push(other);
                                        }
                                    }
                                } else {
                                    out.push('\\');
                                }
                            } else {
                                out.push(ch);
                            }
                        }
                        out
                    })
                {
                    return inner;
                }

                if let Some(inner) = rest
                    .strip_prefix('\'')
                    .and_then(|s| s.strip_suffix('\''))
                    .map(ToOwned::to_owned)
                {
                    return inner;
                }
            }

            return rest.to_string();
        }
    }

    trimmed.to_string()
}

/// Normalize command for matching: strip shell launchers, trim, collapse whitespace.
/// This ensures `/bin/zsh -lc "bun run ci"` matches `bun  run   ci`.
pub fn normalize_command(cmd: &str) -> String {
    unwrap_shell_launcher(cmd)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Get current HEAD SHA for a worktree.
pub fn get_current_head_sha(worktree_path: &str) -> Result<String, String> {
    let output = crate::env::git()
        .args(["rev-parse", "HEAD"])
        .current_dir(worktree_path)
        .output()
        .map_err(|e| format!("git rev-parse failed: {}", e))?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    } else {
        Err("Failed to get HEAD".to_string())
    }
}

/// Check if worktree has uncommitted changes.
pub fn is_worktree_dirty(worktree_path: &str) -> Result<bool, String> {
    let output = crate::env::git()
        .args(["status", "--porcelain"])
        .current_dir(worktree_path)
        .output()
        .map_err(|e| format!("git status failed: {}", e))?;

    Ok(!output.stdout.is_empty())
}

/// Get checkpoint command for a job (from its parent agent's recipe node config).
/// This looks for checkpoint nodes connected to the agent node to determine
/// if the job has an associated programmatic checkpoint command.
pub fn get_job_checkpoint_command(conn: &mut SqliteConnection, job_id: &str) -> Option<String> {
    use crate::diesel_models::DbJob;
    use crate::jobs::queries::load_node_for_job;
    use crate::schema::jobs as jobs_table;

    // Get the job and its recipe_node_id
    let job: DbJob = jobs_table::table.find(job_id).first(conn).ok()?;
    let agent_node_id = job.recipe_node_id.as_ref()?;

    // Load the agent node from execution snapshot
    let (agent_node, snapshot) = load_node_for_job(conn, job_id).ok()??;

    // First, check if the agent node itself has a checkpoint in its agent_config
    if let Some(ref agent_cfg) = agent_node.agent_config {
        if let Some(ref checkpoint) = agent_cfg.checkpoint {
            if matches!(checkpoint.checkpoint_type, CheckpointType::Programmatic) {
                return checkpoint.command.clone();
            }
        }
    }

    // Second, look for slot-based checkpoint node attached to this agent
    // Slot checkpoints have parent_id pointing to the agent node
    let checkpoint_node = snapshot.recipe.nodes.iter().find(|n| {
        n.parent_id.as_deref() == Some(agent_node_id) && n.node_type.to_string() == "checkpoint"
    });

    if let Some(node) = checkpoint_node {
        if let Some(ref checkpoint_cfg) = node.checkpoint_config {
            if matches!(checkpoint_cfg.checkpoint_type, CheckpointType::Programmatic) {
                return checkpoint_cfg.command.clone();
            }
        }
    }

    None
}
