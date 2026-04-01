//! Manager wake/sleep mechanics.
//!
//! Managers are persistent coordination agents with a long-lived Job.
//! They are woken by external triggers (PR merge, issue failure, user message,
//! branch conflict) and go back to sleep (warm/idle) when their turn completes.
//!
//! ## Wake flow
//!
//! 1. Check manager status == Active
//! 2. Load the manager's job
//! 3. Format the wake trigger into a prompt message
//! 4. **First wake** (job has no `current_session_id`):
//!    - Transition job Pending → Ready → Running
//!    - Call `prepare_job()` → sets up worktree, creates Run
//!    - Return PreparedJob for the caller to start the session
//! 5. **Subsequent wakes** (job has `current_session_id`):
//!    - Call `continue_job_impl()` which handles warm (stdin push) and cold (`--resume`)
//! 6. **Concurrency guard**: If job is already running, the trigger is silently
//!    delivered via the existing messages system (stdin push for warm, cursor pull
//!    for active processes).

use crate::diesel_models::{NewJob, UpdateManagerChangeset};
use crate::execution::jobs::{continue_job_impl, prepare_job, PreparedJob};
use crate::managers::{crud, mailbox};
use crate::models::{JobStatus, Manager, ManagerStatus, Run};
use crate::orchestrator::Orchestrator;
use crate::schema::{jobs, managers, projects};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

/// What caused the manager to wake up.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WakeTrigger {
    /// A managed issue's PR was merged.
    IssueMerged {
        issue_number: i32,
        issue_title: String,
        pr_number: i64,
        pr_title: String,
        additions: Option<i64>,
        deletions: Option<i64>,
    },
    /// A managed issue's job failed.
    IssueFailed {
        issue_number: i32,
        issue_title: String,
        error: Option<String>,
    },
    /// The user sent a message to the manager.
    UserMessage { content: String },
    /// A managed issue's PR has merge conflicts.
    BranchConflict {
        issue_number: i32,
        pr_number: i64,
        conflicting_branch: String,
    },
    /// The project's default branch was updated and this manager's branch is stale.
    MainBranchUpdated {
        commits_behind: i32,
        default_branch: String,
    },
}

/// Result of a wake attempt.
#[allow(clippy::large_enum_variant)]
pub enum WakeResult {
    /// First wake — caller must start the agent session with this PreparedJob.
    FirstWake(PreparedManagerWake),
    /// Subsequent wake — session already resumed via continue_job_impl.
    Resumed(Run),
    /// Manager is already running — trigger was delivered as a message.
    AlreadyRunning,
    /// Manager is not active (paused or completed) — ignored for delivery.
    Inactive,
}

pub struct PreparedManagerWake {
    pub prepared_job: PreparedJob,
    pub mailbox_entry_ids: Vec<String>,
    pub wake_batch_id: String,
}

impl WakeTrigger {
    pub fn kind(&self) -> &'static str {
        match self {
            WakeTrigger::IssueMerged { .. } => "issue_merged",
            WakeTrigger::IssueFailed { .. } => "issue_failed",
            WakeTrigger::UserMessage { .. } => "user_message",
            WakeTrigger::BranchConflict { .. } => "branch_conflict",
            WakeTrigger::MainBranchUpdated { .. } => "main_branch_updated",
        }
    }
}

/// Wake a manager with the given trigger.
///
/// For first wake (no current_session_id), returns `WakeResult::FirstWake` with a
/// `PreparedJob` — the caller is responsible for calling `start_agent_session`.
/// This is because `start_agent_session` has different call patterns in Tauri vs
/// cairn-server, so the host layer handles the final process spawn.
///
/// For subsequent wakes, calls `continue_job_impl` directly (which handles
/// warm stdin push and cold `--resume` internally).
pub fn wake_manager(
    orch: &Orchestrator,
    manager_id: &str,
    trigger: WakeTrigger,
) -> Result<WakeResult, String> {
    let now = chrono::Utc::now().timestamp() as i64;

    let (manager, mailbox_entries, wake_batch_id) = {
        let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;
        let manager = crud::get(&mut conn, manager_id)?
            .ok_or_else(|| format!("Manager not found: {}", manager_id))?;

        mailbox::enqueue_manager_wake(&mut conn, manager_id, &trigger, now)?;
        let pending_entries =
            mailbox::list_pending_manager_mailbox_entries(&mut conn, manager_id, now)?;
        let wake_batch_id = mailbox::create_wake_batch(&mut conn, manager_id, now)?;
        let entry_ids: Vec<String> = pending_entries
            .iter()
            .map(|entry| entry.id.clone())
            .collect();
        mailbox::claim_mailbox_entries(&mut conn, &entry_ids, &wake_batch_id, now)?;

        (manager, pending_entries, wake_batch_id)
    };

    if manager.status != ManagerStatus::Active {
        log::info!(
            "Manager {} is {:?}, mailbox updated but wake delivery is deferred",
            manager_id,
            manager.status
        );
        return Ok(WakeResult::Inactive);
    }

    let combined_message = format_wake_batch_message(&mailbox_entries);
    let inline_user_message = mailbox_entries.len() == 1
        && matches!(mailbox_entries[0].trigger, WakeTrigger::UserMessage { .. });
    let entry_ids: Vec<String> = mailbox_entries
        .iter()
        .map(|entry| entry.id.clone())
        .collect();

    if manager
        .current_session_id
        .as_deref()
        .is_some_and(|sid| orch.process_state.find_process_by_session(sid).is_some())
    {
        deliver_to_running_job(orch, &manager, &combined_message, inline_user_message)?;
        let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;
        mailbox::mark_mailbox_entries_processed(&mut conn, &entry_ids, now)?;
        mailbox::complete_wake_batch(&mut conn, &wake_batch_id, now, "inline")?;
        return Ok(WakeResult::AlreadyRunning);
    }

    let wake_job_id = {
        let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;
        let wake_job_id = create_manager_wake_job(&mut conn, &manager, now)?;
        update_manager_runtime_state(
            &mut conn,
            &manager.id,
            Some(&wake_job_id),
            manager.current_session_id.as_deref(),
            manager.current_turn_id.as_deref(),
            now,
            None,
        )?;
        wake_job_id
    };

    if manager.current_session_id.is_none() {
        {
            let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;
            crate::transitions::transition_job(
                &mut conn,
                &*orch.services.emitter,
                &wake_job_id,
                JobStatus::Ready,
                &orch.trigger_events,
            )
            .map_err(|e| format!("Failed to transition manager wake job to ready: {}", e))?;
            crate::transitions::transition_job(
                &mut conn,
                &*orch.services.emitter,
                &wake_job_id,
                JobStatus::Running,
                &orch.trigger_events,
            )
            .map_err(|e| format!("Failed to transition manager wake job to running: {}", e))?;
        }

        let prepared = prepare_job(orch, &wake_job_id)?;
        crate::execution::jobs::store_user_event(
            orch,
            &prepared.run_id,
            &prepared.session_id,
            &combined_message,
            now as i32,
            -1,
        )?;

        let prompt = format!("{}\n\n---\n\n{}", prepared.prompt, combined_message);
        let prepared = PreparedJob { prompt, ..prepared };

        let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;
        update_manager_runtime_state(
            &mut conn,
            &manager.id,
            Some(&wake_job_id),
            Some(&prepared.session_id),
            Some(&prepared.turn_id),
            now,
            None,
        )?;
        return Ok(WakeResult::FirstWake(PreparedManagerWake {
            prepared_job: prepared,
            mailbox_entry_ids: entry_ids,
            wake_batch_id,
        }));
    }

    let run = continue_job_impl(orch, &wake_job_id, Some(&combined_message), None)?;
    let resumed_session_id = run
        .session_id
        .clone()
        .or_else(|| manager.current_session_id.clone());
    let resumed_turn_id = {
        let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;
        let current_turn_id: Option<String> = jobs::table
            .find(&wake_job_id)
            .select(jobs::current_turn_id)
            .first(&mut *conn)
            .map_err(|e| format!("Failed to load wake job turn pointer: {}", e))?;
        update_manager_runtime_state(
            &mut conn,
            &manager.id,
            Some(&wake_job_id),
            resumed_session_id.as_deref(),
            current_turn_id.as_deref(),
            now,
            None,
        )?;
        mailbox::mark_mailbox_entries_processed(&mut conn, &entry_ids, now)?;
        mailbox::complete_wake_batch(&mut conn, &wake_batch_id, now, "resumed")?;
        current_turn_id
    };

    if let Some(turn_id) = resumed_turn_id {
        orch.process_state
            .set_current_turn_id(&run.id, Some(&turn_id));
    }

    Ok(WakeResult::Resumed(run))
}

pub fn acknowledge_prepared_manager_wake(
    orch: &Orchestrator,
    prepared_wake: &PreparedManagerWake,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp() as i64;
    let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;
    mailbox::mark_mailbox_entries_processed(&mut conn, &prepared_wake.mailbox_entry_ids, now)?;
    mailbox::complete_wake_batch(&mut conn, &prepared_wake.wake_batch_id, now, "prepared")?;
    Ok(())
}

pub fn release_prepared_manager_wake(
    orch: &Orchestrator,
    prepared_wake: &PreparedManagerWake,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp() as i64;
    let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;
    mailbox::release_mailbox_entries(&mut conn, &prepared_wake.mailbox_entry_ids)?;
    mailbox::complete_wake_batch(&mut conn, &prepared_wake.wake_batch_id, now, "spawn_failed")?;
    Ok(())
}

fn format_wake_batch_message(entries: &[mailbox::ManagerMailboxEntry]) -> String {
    entries
        .iter()
        .map(|entry| format_wake_message(&entry.trigger))
        .collect::<Vec<_>>()
        .join("\n\n---\n\n")
}

fn create_manager_wake_job(
    conn: &mut diesel::sqlite::SqliteConnection,
    manager: &Manager,
    now: i64,
) -> Result<String, String> {
    let job_id = Uuid::new_v4().to_string();
    let project_id = manager
        .home_project_id
        .as_deref()
        .unwrap_or(&manager.project_id);
    let branch = (!manager.branch.is_empty()).then_some(manager.branch.as_str());
    let status = if manager.current_session_id.is_some() {
        JobStatus::Running.to_string()
    } else {
        JobStatus::Pending.to_string()
    };

    let new_job = NewJob {
        id: &job_id,
        execution_id: manager.execution_id.as_deref(),
        manager_id: Some(&manager.id),
        recipe_node_id: None,
        parent_job_id: None,
        worktree_path: None,
        branch,
        base_commit: None,
        current_session_id: manager.current_session_id.as_deref(),
        resume_session_id: manager.current_session_id.as_deref(),
        status: &status,
        agent_config_id: manager.agent_config_id.as_deref(),
        issue_id: None,
        project_id,
        task_description: Some("Manager wake turn"),
        created_at: now as i32,
        updated_at: now as i32,
        completed_at: None,
        parent_tool_use_id: None,
        task_index: None,
        started_at: None,
        model: None,
        node_name: Some("Manager"),
        base_branch: None,
        current_turn_id: manager.current_turn_id.as_deref(),
    };

    diesel::insert_into(jobs::table)
        .values(&new_job)
        .execute(conn)
        .map_err(|e| format!("Failed to create manager wake job: {}", e))?;

    Ok(job_id)
}

fn update_manager_runtime_state(
    conn: &mut diesel::sqlite::SqliteConnection,
    manager_id: &str,
    job_id: Option<&str>,
    session_id: Option<&str>,
    turn_id: Option<&str>,
    now: i64,
    last_error: Option<&str>,
) -> Result<(), String> {
    diesel::update(managers::table.find(manager_id))
        .set(UpdateManagerChangeset {
            job_id: job_id.map(Some),
            current_session_id: Some(session_id),
            current_turn_id: Some(turn_id),
            last_wake_at: Some(Some(now as i32)),
            last_error: Some(last_error),
            updated_at: Some(now as i32),
            ..Default::default()
        })
        .execute(conn)
        .map_err(|e| format!("Failed to update manager runtime state: {}", e))?;
    Ok(())
}

/// Deliver a message to an already-running manager job via existing process stdin.
/// Prepends manager context if it has changed since last delivery.
fn deliver_to_running_job(
    orch: &Orchestrator,
    manager: &Manager,
    message: &str,
    is_user_message: bool,
) -> Result<(), String> {
    let (session_id, worktree_path, project_path) = {
        let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;
        let project_id = manager
            .home_project_id
            .as_deref()
            .unwrap_or(&manager.project_id);
        let project_path: Option<PathBuf> = projects::table
            .find(project_id)
            .select(projects::repo_path)
            .first::<String>(&mut *conn)
            .ok()
            .map(PathBuf::from);

        let worktree_path = if let Some(job_id) = manager.job_id.as_deref() {
            jobs::table
                .find(job_id)
                .select(jobs::worktree_path)
                .first::<Option<String>>(&mut *conn)
                .ok()
                .flatten()
        } else {
            None
        };

        (
            manager.current_session_id.clone(),
            worktree_path,
            project_path,
        )
    };

    let session_id = session_id.ok_or_else(|| format!("Manager {} has no session", manager.id))?;
    let context_prefix = build_context_if_changed(orch, &manager.id, &worktree_path, &project_path);

    let run_id = orch
        .process_state
        .find_process_by_session(&session_id)
        .ok_or_else(|| format!("No live process found for manager session {}", session_id))?;

    let base = if is_user_message {
        message.to_string()
    } else {
        format!("[System] {}", message)
    };
    let content = match context_prefix {
        Some(ctx) => format!("{}\n\n---\n\n{}", ctx, base),
        None => base,
    };

    crate::backends::stdin::send_user_message(
        &orch.process_state,
        &run_id,
        &content,
        &session_id,
        None,
        None,
    )
    .map_err(|e| format!("Failed to deliver to running manager: {}", e))?;

    let now = chrono::Utc::now().timestamp() as i32;
    let next_seq = {
        let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;
        use crate::schema::events;
        let max_seq: Option<i32> = events::table
            .filter(events::session_id.eq(&session_id))
            .select(diesel::dsl::max(events::sequence))
            .first(&mut *conn)
            .unwrap_or(None);
        max_seq.unwrap_or(-1) + 1
    };
    crate::execution::jobs::store_user_event(orch, &run_id, &session_id, message, now, next_seq)?;
    Ok(())
}

/// Build manager context and return it only if it differs from last time.
/// Stores the new context in `process_state.last_manager_context`.
fn build_context_if_changed(
    orch: &Orchestrator,
    manager_id: &str,
    worktree_path: &Option<String>,
    project_path: &Option<PathBuf>,
) -> Option<String> {
    let context = {
        let Ok(mut conn) = orch.db.conn.lock() else {
            return None;
        };
        let manager = crate::managers::crud::get(&mut conn, manager_id).ok()??;
        let wt = worktree_path
            .as_ref()
            .map(PathBuf::from)
            .or_else(|| project_path.clone());
        crate::managers::context::build_manager_context(
            &mut conn,
            &manager,
            project_path.as_deref(),
            wt.as_deref(),
        )
        .ok()?
    };

    let changed = {
        let Ok(mut cache) = orch.process_state.last_manager_context.lock() else {
            return Some(context);
        };
        let prev = cache.get(manager_id);
        if prev == Some(&context) {
            false
        } else {
            cache.insert(manager_id.to_string(), context.clone());
            true
        }
    };

    if changed {
        Some(context)
    } else {
        None
    }
}

/// Format a wake trigger into a human-readable prompt message.
fn format_wake_message(trigger: &WakeTrigger) -> String {
    match trigger {
        WakeTrigger::IssueMerged {
            issue_number,
            issue_title,
            pr_number,
            pr_title,
            additions,
            deletions,
        } => {
            let mut msg = format!(
                "## Managed Issue PR Merged\n\n\
                 Issue #{} \"{}\" — PR #{} \"{}\" merged.",
                issue_number, issue_title, pr_number, pr_title
            );
            if let (Some(add), Some(del)) = (additions, deletions) {
                msg.push_str(&format!("\nChanges: +{}/-{} lines.", add, del));
            }
            msg
        }
        WakeTrigger::IssueFailed {
            issue_number,
            issue_title,
            error,
        } => {
            let mut msg = format!(
                "## Managed Issue Failed\n\n\
                 Issue #{} \"{}\" failed.",
                issue_number, issue_title
            );
            if let Some(err) = error {
                msg.push_str(&format!("\nError: {}", err));
            }
            msg
        }
        WakeTrigger::UserMessage { content } => content.clone(),
        WakeTrigger::BranchConflict {
            issue_number,
            pr_number,
            conflicting_branch,
        } => {
            format!(
                "## Branch Conflict Detected\n\n\
                 Issue #{} — PR #{} has merge conflicts on branch `{}`.",
                issue_number, pr_number, conflicting_branch
            )
        }
        WakeTrigger::MainBranchUpdated {
            commits_behind,
            default_branch,
        } => {
            format!(
                "## Main Branch Updated\n\n\
                 Your feature branch is {} commits behind `{}`.\n\n\
                 Consider:\n\
                 1. Review what changed on {default_branch} (`git log origin/{default_branch} --oneline -20`)\n\
                 2. Rebase: `git fetch origin && git rebase origin/{default_branch}`\n\
                 3. If conflicts arise, resolve them or abort and create an issue for resolution\n\
                 4. Force-push: `git push --force-with-lease`\n\
                 5. After rebasing, open PRs targeting your branch will get conflict notifications — \
                 builders handle this automatically",
                commits_behind, default_branch
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::models::CreateManager;
    use crate::orchestrator::Orchestrator;
    use crate::services::testing::{MockClock, TestServicesBuilder};
    use crate::test_utils::{create_test_project, test_diesel_conn};
    use std::sync::{Arc, Mutex};

    fn test_orchestrator(conn: diesel::sqlite::SqliteConnection) -> Orchestrator {
        let db = Arc::new(DbState {
            conn: Mutex::new(conn),
        });
        let services = Arc::new(TestServicesBuilder::new().build());
        let account_manager = Arc::new(crate::orchestrator::AccountManager::new(
            db.clone(),
            services.emitter.clone(),
        ));
        let sync_tx = Arc::new(Mutex::new(None));
        Orchestrator {
            db,
            services: services.clone(),
            process_state: Arc::new(crate::agent_process::process::AgentProcessState::default()),
            mcp_auth: Arc::new(crate::mcp::McpAuthState::new(std::path::PathBuf::from(
                "/tmp",
            ))),
            warm_gc: None,
            pty_state: Arc::new(crate::services::PtyState::default()),
            permission_responses: tokio::sync::broadcast::channel(16).0,
            run_completions: tokio::sync::broadcast::channel(64).0,
            prompt_responses: tokio::sync::broadcast::channel(16).0,
            trigger_events: tokio::sync::broadcast::channel(256).0,
            session_allowed_tools: Arc::new(Mutex::new(std::collections::HashSet::new())),
            identity_store: Arc::new(Mutex::new(None)),
            mcp_binary_path: "cairn-mcp".to_string(),
            config_dir: std::path::PathBuf::from("/tmp"),
            schema_dir: None,
            mcp_callback_port: 3847,
            embedding_engine: None,
            vibe_state: None,
            account_manager,
            sync_tx: sync_tx.clone(),
            notifier: crate::notify::Notifier::new(sync_tx, services.emitter.clone()),
            api_config: crate::api::ApiConfig::default(),
            effect_tx: None,
            model_catalog: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            provider_usage_snapshots: Default::default(),
            executor: std::sync::Arc::new(std::sync::OnceLock::new()),
        }
    }

    fn create_manager_for_test(
        conn: &mut diesel::sqlite::SqliteConnection,
        project_id: &str,
    ) -> crate::models::Manager {
        let mut clock = MockClock::new();
        clock.expect_now().returning(|| 1700000000);
        crate::managers::crud::create(
            conn,
            &clock,
            CreateManager {
                project_id: project_id.to_string(),
                home_project_id: None,
                scope_kind: None,
                name: "Test Manager".to_string(),
                branch: "mgr/test".to_string(),
                description: Some("Test description".to_string()),
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap()
    }

    #[test]
    fn test_build_context_if_changed_returns_some_on_first_call() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let manager = create_manager_for_test(&mut conn, &project_id);

        let orch = test_orchestrator(conn);
        let result = build_context_if_changed(&orch, &manager.id, &None, &None);
        assert!(
            result.is_some(),
            "First call should return Some (no cached value)"
        );
        assert!(result.unwrap().contains("Manager Context"));
    }

    #[test]
    fn test_build_context_if_changed_returns_none_when_unchanged() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let manager = create_manager_for_test(&mut conn, &project_id);

        let orch = test_orchestrator(conn);
        // First call — populates cache
        let first = build_context_if_changed(&orch, &manager.id, &None, &None);
        assert!(first.is_some());

        // Second call — same context, should return None
        let second = build_context_if_changed(&orch, &manager.id, &None, &None);
        assert!(
            second.is_none(),
            "Second call with unchanged context should return None"
        );
    }

    #[test]
    fn test_build_context_if_changed_returns_some_when_context_differs() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let manager = create_manager_for_test(&mut conn, &project_id);

        let orch = test_orchestrator(conn);
        // First call
        let first = build_context_if_changed(&orch, &manager.id, &None, &None);
        assert!(first.is_some());

        // Mutate the manager's state so context changes — add a managed issue
        {
            let mut conn = orch.db.conn.lock().unwrap();
            let mut clock = MockClock::new();
            clock.expect_now().returning(|| 1700001000);
            crate::issues::crud::create(
                &mut conn,
                &clock,
                crate::models::CreateIssue {
                    project_id: project_id.clone(),
                    title: "New managed issue".to_string(),
                    description: None,
                    backend_override: None,
                    manager_id: Some(manager.id.clone()),
                },
            )
            .unwrap();
        }

        // Third call — context has changed, should return Some
        let third = build_context_if_changed(&orch, &manager.id, &None, &None);
        assert!(
            third.is_some(),
            "Call after context change should return Some"
        );
    }

    #[test]
    fn test_build_context_if_changed_caches_per_manager_id() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let manager = create_manager_for_test(&mut conn, &project_id);

        let orch = test_orchestrator(conn);

        let r1 = build_context_if_changed(&orch, &manager.id, &None, &None);
        assert!(r1.is_some());

        let r2 = build_context_if_changed(&orch, &manager.id, &None, &None);
        assert!(r2.is_none(), "Manager cache should be keyed by manager id");
    }

    #[test]
    fn test_format_wake_message_issue_merged() {
        let trigger = WakeTrigger::IssueMerged {
            issue_number: 42,
            issue_title: "Fix auth bug".to_string(),
            pr_number: 87,
            pr_title: "Fix auth token refresh".to_string(),
            additions: Some(120),
            deletions: Some(45),
        };
        let msg = format_wake_message(&trigger);
        assert!(msg.contains("Issue #42"));
        assert!(msg.contains("PR #87"));
        assert!(msg.contains("+120/-45"));
    }

    #[test]
    fn test_format_wake_message_issue_failed() {
        let trigger = WakeTrigger::IssueFailed {
            issue_number: 43,
            issue_title: "Add caching".to_string(),
            error: Some("Build failed".to_string()),
        };
        let msg = format_wake_message(&trigger);
        assert!(msg.contains("Issue #43"));
        assert!(msg.contains("Build failed"));
    }

    #[test]
    fn test_format_wake_message_user_message() {
        let trigger = WakeTrigger::UserMessage {
            content: "Check on the auth work".to_string(),
        };
        let msg = format_wake_message(&trigger);
        assert_eq!(msg, "Check on the auth work");
    }

    #[test]
    fn test_format_wake_message_branch_conflict() {
        let trigger = WakeTrigger::BranchConflict {
            issue_number: 44,
            pr_number: 90,
            conflicting_branch: "feature/auth".to_string(),
        };
        let msg = format_wake_message(&trigger);
        assert!(msg.contains("Issue #44"));
        assert!(msg.contains("PR #90"));
        assert!(msg.contains("feature/auth"));
    }

    #[test]
    fn test_format_wake_message_merged_no_stats() {
        let trigger = WakeTrigger::IssueMerged {
            issue_number: 1,
            issue_title: "Test".to_string(),
            pr_number: 2,
            pr_title: "Test PR".to_string(),
            additions: None,
            deletions: None,
        };
        let msg = format_wake_message(&trigger);
        assert!(msg.contains("Issue #1"));
        assert!(!msg.contains("Changes:"));
    }

    #[test]
    fn test_format_wake_message_merged_partial_stats() {
        // Only additions set, deletions None — should NOT show changes line
        let trigger = WakeTrigger::IssueMerged {
            issue_number: 3,
            issue_title: "Partial".to_string(),
            pr_number: 4,
            pr_title: "Partial PR".to_string(),
            additions: Some(50),
            deletions: None,
        };
        let msg = format_wake_message(&trigger);
        assert!(msg.contains("Issue #3"));
        assert!(!msg.contains("Changes:"));
    }

    #[test]
    fn test_format_wake_message_main_branch_updated() {
        let trigger = WakeTrigger::MainBranchUpdated {
            commits_behind: 15,
            default_branch: "main".to_string(),
        };
        let msg = format_wake_message(&trigger);
        assert!(msg.contains("## Main Branch Updated"));
        assert!(msg.contains("15 commits behind `main`"));
        assert!(msg.contains("git rebase origin/main"));
        assert!(msg.contains("git push --force-with-lease"));
    }

    #[test]
    fn test_format_wake_message_failed_no_error() {
        let trigger = WakeTrigger::IssueFailed {
            issue_number: 5,
            issue_title: "Broken".to_string(),
            error: None,
        };
        let msg = format_wake_message(&trigger);
        assert!(msg.contains("Issue #5"));
        assert!(!msg.contains("Error:"));
    }
}
