//! Manager context builder.
//!
//! Builds a structured markdown block injected into manager session prompts.
//! Provides the manager with a reconstructed view of its state after context
//! compaction or cold resume.

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use std::collections::HashSet;
use std::path::Path;

use crate::managers::crud;
use crate::models::{ChannelType, Manager, ManagerScopeKind, Memory};
use crate::schema::{jobs, manager_mailbox, projects, runs};

/// Build the full manager context string for injection into a manager's prompt.
///
/// Includes: dashboard artifact, managed issues status, branch status (if repo_path
/// provided), branch-scoped memories, and queued messages since last activity.
///
/// When `repo_path` is provided, a branch status section shows how far the manager's
/// feature branch has diverged from the default branch. When absent (e.g., in tests
/// without a real git repo), the section is omitted.
pub fn build_manager_context(
    conn: &mut SqliteConnection,
    manager: &Manager,
    repo_path: Option<&Path>,
    worktree_path: Option<&Path>,
) -> Result<String, String> {
    let mut sections = Vec::new();

    // 1. Dashboard (read from file on disk)
    sections.push(build_dashboard_section(manager, worktree_path));

    // 2. Managed issues status
    sections.push(build_managed_issues_section(conn, manager)?);

    // 3. Branch status (requires git repo, skip when branch == default branch)
    if let Some(path) = repo_path {
        let default_branch: String = projects::table
            .find(&manager.project_id)
            .select(projects::default_branch)
            .first::<Option<String>>(conn)
            .ok()
            .flatten()
            .unwrap_or_else(|| "main".to_string());

        if manager.branch != default_branch {
            sections.push(build_branch_status_section(
                path,
                &manager.branch,
                &default_branch,
            ));
        }
    }

    // 4. Branch-scoped memories
    sections.push(build_memories_section(conn, manager)?);

    // 5. Queued messages
    sections.push(build_messages_section(conn, manager)?);

    let body = sections
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");

    Ok(format!("# Manager Context (reconstructed)\n\n{}", body))
}

/// Build the dashboard section by reading from the file on disk.
fn build_dashboard_section(manager: &Manager, worktree_path: Option<&Path>) -> String {
    let rel_path = super::dashboard_path(&manager.name);

    if let Some(wt) = worktree_path {
        let abs_path = wt.join(&rel_path);
        if let Ok(content) = std::fs::read_to_string(&abs_path) {
            if !content.trim().is_empty() {
                return format!(
                    "## Dashboard\n\nPath: `{}`\n\n{}",
                    rel_path.display(),
                    content,
                );
            }
        }
    }

    format!(
        "## Dashboard\n\nNo dashboard yet — write to `{}` to create one.",
        rel_path.display(),
    )
}

/// Build the managed issues status table.
fn build_managed_issues_section(
    conn: &mut SqliteConnection,
    manager: &Manager,
) -> Result<String, String> {
    let issues = crud::list_managed_issues(conn, &manager.id)?;

    if issues.is_empty() {
        return Ok("## Managed Issues\n\nNo issues currently managed.".to_string());
    }

    let mut lines = vec![
        "## Managed Issues".to_string(),
        String::new(),
        "| # | Title | Status | Branch |".to_string(),
        "|---|-------|--------|--------|".to_string(),
    ];

    for issue in &issues {
        let status = &issue.status;

        // Find the latest job branch for this issue
        let branch: Option<String> = jobs::table
            .filter(jobs::issue_id.eq(&issue.id))
            .filter(jobs::branch.is_not_null())
            .order(jobs::created_at.desc())
            .select(jobs::branch)
            .first::<Option<String>>(conn)
            .ok()
            .flatten();

        let branch_str = branch.as_deref().unwrap_or("—");
        lines.push(format!(
            "| {} | {} | {} | {} |",
            issue.number, issue.title, status, branch_str
        ));
    }

    Ok(lines.join("\n"))
}

/// Build the branch-scoped memories section.
fn build_memories_section(
    conn: &mut SqliteConnection,
    manager: &Manager,
) -> Result<String, String> {
    let project_id = Some(
        manager
            .home_project_id
            .as_deref()
            .unwrap_or(manager.project_id.as_str()),
    );
    let all_memories = crate::memories::db::load_active_memories(conn, project_id)?;

    let branch_scope = format!("branch:{}", manager.branch);
    let relevant: Vec<&Memory> = all_memories
        .iter()
        .filter(|memory| match manager.scope_kind {
            ManagerScopeKind::Branch => {
                memory.scope == branch_scope || memory.scope == "project" || memory.scope.is_empty()
            }
            ManagerScopeKind::Project => memory.scope == "project" || memory.scope.is_empty(),
            ManagerScopeKind::Workspace => {
                memory.scope == "workspace" || memory.scope == "project" || memory.scope.is_empty()
            }
        })
        .collect();

    if relevant.is_empty() {
        return Ok(String::new());
    }

    let title = match manager.scope_kind {
        ManagerScopeKind::Branch => "## Branch Memories",
        ManagerScopeKind::Project => "## Project Memories",
        ManagerScopeKind::Workspace => "## Workspace Memories",
    };

    let mut lines = vec![title.to_string(), String::new()];
    for memory in relevant {
        lines.push(format!("- [{}] {}", memory.confidence, memory.content));
    }

    Ok(lines.join("\n"))
}

/// Build the branch status section showing divergence from the default branch.
///
/// Non-failable — returns empty string if git commands fail (e.g., no remote).
fn build_branch_status_section(
    repo_path: &Path,
    feature_branch: &str,
    default_branch: &str,
) -> String {
    use super::branch::{check_staleness, DEFAULT_STALENESS_THRESHOLD};

    let status = match check_staleness(repo_path, feature_branch, default_branch) {
        Ok(Some(s)) => s,
        Ok(None) => {
            // Branch doesn't exist on origin yet — omit section
            return String::new();
        }
        Err(e) => {
            log::debug!("Could not check branch staleness: {}", e);
            return String::new();
        }
    };

    let mut lines = Vec::new();
    lines.push("## Branch Status".to_string());
    lines.push(String::new());
    lines.push(format!("- **Branch:** {}", feature_branch));
    lines.push(format!("- **Default branch:** {}", default_branch));
    lines.push(format!("- **Commits ahead:** {}", status.commits_ahead));

    if status.commits_behind > 0 {
        lines.push(format!(
            "- **Commits behind:** {} ⚠️",
            status.commits_behind
        ));
    } else {
        lines.push(format!("- **Commits behind:** {}", status.commits_behind));
    }

    if status.commits_behind >= DEFAULT_STALENESS_THRESHOLD {
        lines.push(String::new());
        lines.push(
            "> Your branch is significantly behind main. Consider rebasing before dispatching new work."
                .to_string(),
        );
    }

    lines.join("\n")
}

/// Build the queued messages section.
fn build_messages_section(
    conn: &mut SqliteConnection,
    manager: &Manager,
) -> Result<String, String> {
    let since_ts = manager.last_wake_at.unwrap_or(0);
    let manager_run_ids: HashSet<String> = match manager.job_id.as_deref() {
        Some(job_id) => runs::table
            .filter(runs::job_id.eq(job_id))
            .select(runs::id)
            .load(conn)
            .map_err(|e| format!("Failed to query manager runs: {}", e))?,
        None => Vec::new(),
    }
    .into_iter()
    .collect();
    let project_id = manager
        .home_project_id
        .as_deref()
        .unwrap_or(manager.project_id.as_str());

    let project_key: String = projects::table
        .find(project_id)
        .select(projects::key)
        .first(conn)
        .map_err(|e| format!("Failed to get project key: {}", e))?;

    let direct_messages = crate::messages::db::query_channel(
        conn,
        &ChannelType::Direct,
        None,
        None,
        None,
        Some(since_ts),
        Some(50),
    )?
    .into_iter()
    .filter(|message| {
        message.recipient_manager_id.as_deref() == Some(manager.id.as_str())
            || message
                .recipient_run_id
                .as_ref()
                .is_some_and(|run_id| manager_run_ids.contains(run_id))
    })
    .collect::<Vec<_>>();

    let channel_messages = crate::messages::db::query_channel(
        conn,
        &ChannelType::Project,
        Some(&project_key),
        None,
        None,
        Some(since_ts),
        Some(50),
    )?;

    let pending_mailbox: Vec<String> = manager_mailbox::table
        .filter(manager_mailbox::manager_id.eq(&manager.id))
        .filter(manager_mailbox::processed_at.is_null())
        .order(manager_mailbox::created_at.asc())
        .select(manager_mailbox::cause_json)
        .load(conn)
        .map_err(|e| format!("Failed to query manager mailbox: {}", e))?;

    let mut all_messages = direct_messages;
    all_messages.extend(channel_messages);
    all_messages.sort_by_key(|m| m.created_at);

    if all_messages.is_empty() && pending_mailbox.is_empty() {
        return Ok(String::new());
    }

    let mut lines = vec!["## Messages Since Last Wake".to_string(), String::new()];

    for msg in &all_messages {
        let ts = chrono::DateTime::from_timestamp(msg.created_at, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_else(|| msg.created_at.to_string());

        lines.push(format!("[From {} at {}]", msg.sender_name, ts));
        lines.push(msg.content.clone());
        lines.push(String::new());
    }

    if !pending_mailbox.is_empty() {
        lines.push("[Pending mailbox events]".to_string());
        for cause_json in pending_mailbox {
            lines.push(cause_json);
        }
        lines.push(String::new());
    }

    Ok(lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diesel_models::NewMessage;
    use crate::managers::branch::DEFAULT_STALENESS_THRESHOLD;
    use crate::models::CreateManager;
    use crate::schema::{messages, runs};
    use crate::services::testing::MockClock;
    use crate::test_utils::{create_test_project, test_diesel_conn};

    fn create_manager_for_test(conn: &mut SqliteConnection, project_id: &str) -> Manager {
        let mut clock = MockClock::new();
        clock.expect_now().returning(|| 1700000000);

        crud::create(
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
    fn test_build_context_empty_manager() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let manager = create_manager_for_test(&mut conn, &project_id);

        let context = build_manager_context(&mut conn, &manager, None, None).unwrap();

        assert!(context.contains("# Manager Context (reconstructed)"));
        assert!(context.contains("## Dashboard"));
        assert!(context.contains("No dashboard yet"));
        assert!(context.contains(".cairn/managers/test-manager.md"));
        assert!(context.contains("## Managed Issues"));
        assert!(context.contains("No issues currently managed"));
    }

    #[test]
    fn test_build_context_with_dashboard_file() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let manager = create_manager_for_test(&mut conn, &project_id);

        // Create a temp directory with a dashboard file
        let wt_dir = tempfile::tempdir().unwrap();
        let dashboard_rel = super::super::dashboard_path(&manager.name);
        let dashboard_abs = wt_dir.path().join(&dashboard_rel);
        std::fs::create_dir_all(dashboard_abs.parent().unwrap()).unwrap();
        std::fs::write(
            &dashboard_abs,
            "# Auth System\n\n## Objective\nBuild the auth system\n\n## Next Steps\n- Add rate limiting",
        )
        .unwrap();

        let context =
            build_manager_context(&mut conn, &manager, None, Some(wt_dir.path())).unwrap();

        assert!(context.contains("## Dashboard"));
        assert!(context.contains("Build the auth system"));
        assert!(context.contains("Add rate limiting"));
        assert!(!context.contains("No dashboard yet"));
        // Path included so manager knows where to write
        assert!(context.contains(".cairn/managers/test-manager.md"));
    }

    #[test]
    fn test_build_context_no_dashboard_file_shows_path() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let manager = create_manager_for_test(&mut conn, &project_id);

        // Worktree exists but no dashboard file
        let wt_dir = tempfile::tempdir().unwrap();
        let context =
            build_manager_context(&mut conn, &manager, None, Some(wt_dir.path())).unwrap();

        assert!(context.contains("No dashboard yet"));
        assert!(context.contains(".cairn/managers/test-manager.md"));
    }

    #[test]
    fn test_build_context_with_managed_issues() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let manager = create_manager_for_test(&mut conn, &project_id);

        // Create a managed issue
        let mut clock = MockClock::new();
        clock.expect_now().returning(|| 1700001000);

        crate::issues::crud::create(
            &mut conn,
            &clock,
            crate::models::CreateIssue {
                project_id: project_id.clone(),
                title: "Add auth middleware".to_string(),
                description: None,
                backend_override: None,
                manager_id: Some(manager.id.clone()),
            },
        )
        .unwrap();

        let context = build_manager_context(&mut conn, &manager, None, None).unwrap();

        assert!(context.contains("Add auth middleware"));
        assert!(context.contains("| # | Title | Status | Branch |"));
    }

    #[test]
    fn test_build_context_with_memories() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let manager = create_manager_for_test(&mut conn, &project_id);

        // Create a branch-scoped memory that should appear
        crate::memories::db::create_memory(
            &mut conn,
            "mem-branch",
            "Branch-scoped insight",
            Some(&project_id),
            "established",
            None,
            &[],
            &format!("branch:{}", manager.branch),
            None,
            None,
        )
        .unwrap();

        // Create a project-scoped memory that should also appear
        crate::memories::db::create_memory(
            &mut conn,
            "mem-project",
            "Project-wide note",
            Some(&project_id),
            "tentative",
            None,
            &[],
            "project",
            None,
            None,
        )
        .unwrap();

        // Create a memory scoped to a different branch — should NOT appear
        crate::memories::db::create_memory(
            &mut conn,
            "mem-other",
            "Other branch note",
            Some(&project_id),
            "established",
            None,
            &[],
            "branch:other/branch",
            None,
            None,
        )
        .unwrap();

        let context = build_manager_context(&mut conn, &manager, None, None).unwrap();

        assert!(context.contains("## Branch Memories"));
        assert!(context.contains("Branch-scoped insight"));
        assert!(context.contains("Project-wide note"));
        assert!(!context.contains("Other branch note"));
    }

    #[test]
    fn test_build_context_with_messages() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let manager = create_manager_for_test(&mut conn, &project_id);
        let job_id = manager.job_id.as_ref().unwrap();

        // Create a run for the manager job
        let run_id = uuid::Uuid::new_v4().to_string();
        let run_status = crate::models::RunStatus::Exited.to_string();
        diesel::insert_into(runs::table)
            .values(&crate::diesel_models::NewRun {
                id: &run_id,
                issue_id: None,
                project_id: Some(&project_id),
                job_id: Some(job_id),
                chat_id: None,
                status: Some(&run_status),
                session_id: None,
                error_message: None,
                started_at: Some(1700000000),
                exited_at: Some(1700000500),
                created_at: 1700000000,
                updated_at: 1700000500,
                backend: None,
                exit_reason: None,
                start_mode: None,
            })
            .execute(&mut conn)
            .unwrap();

        // Insert a direct message to this run (after completion)
        let msg_id = uuid::Uuid::new_v4().to_string();
        diesel::insert_into(messages::table)
            .values(&NewMessage {
                id: &msg_id,
                channel_type: "direct",
                channel_id: None,
                sender_run_id: Some("other-run"),
                sender_name: "builder-1",
                recipient_run_id: Some(&run_id),
                recipient_manager_id: None,
                content: "Build complete, PR ready.",
                created_at: 1700000600,
            })
            .execute(&mut conn)
            .unwrap();

        let context = build_manager_context(&mut conn, &manager, None, None).unwrap();

        assert!(context.contains("## Messages Since Last Wake"));
        assert!(context.contains("Build complete, PR ready."));
        assert!(context.contains("builder-1"));
    }

    // ── Git fixture helpers ───────────────────────────────────────

    fn run_git(dir: &tempfile::TempDir, args: &[&str]) {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(dir.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// Create a git repo with a bare origin, main branch, and a feature branch.
    /// `main_ahead` controls how many commits main is ahead of the feature branch.
    fn setup_branch_fixture(
        feature_branch: &str,
        main_ahead: usize,
        feature_commits: usize,
    ) -> (tempfile::TempDir, tempfile::TempDir) {
        let origin_dir = tempfile::tempdir().unwrap();
        let work_dir = tempfile::tempdir().unwrap();

        run_git(&origin_dir, &["init", "--bare"]);
        run_git(&work_dir, &["init"]);
        run_git(
            &work_dir,
            &[
                "remote",
                "add",
                "origin",
                origin_dir.path().to_str().unwrap(),
            ],
        );
        run_git(&work_dir, &["config", "user.email", "test@test.com"]);
        run_git(&work_dir, &["config", "user.name", "Test"]);

        // Initial commit on main
        std::fs::write(work_dir.path().join("init.txt"), "initial").unwrap();
        run_git(&work_dir, &["add", "."]);
        run_git(&work_dir, &["commit", "-m", "initial"]);
        run_git(&work_dir, &["branch", "-M", "main"]);
        run_git(&work_dir, &["push", "-u", "origin", "main"]);

        // Create feature branch
        run_git(&work_dir, &["checkout", "-b", feature_branch]);
        for i in 0..feature_commits {
            std::fs::write(
                work_dir.path().join(format!("feat_{}.txt", i)),
                format!("{}", i),
            )
            .unwrap();
            run_git(&work_dir, &["add", "."]);
            run_git(&work_dir, &["commit", "-m", &format!("feat {}", i)]);
        }
        run_git(&work_dir, &["push", "-u", "origin", feature_branch]);

        // Advance main
        run_git(&work_dir, &["checkout", "main"]);
        for i in 0..main_ahead {
            std::fs::write(
                work_dir.path().join(format!("main_{}.txt", i)),
                format!("{}", i),
            )
            .unwrap();
            run_git(&work_dir, &["add", "."]);
            run_git(&work_dir, &["commit", "-m", &format!("main {}", i)]);
        }
        run_git(&work_dir, &["push", "origin", "main"]);
        run_git(&work_dir, &["fetch", "origin"]);

        (work_dir, origin_dir)
    }

    // ── Branch status section tests ──────────────────────────────

    #[test]
    fn test_branch_status_section_up_to_date() {
        let (work_dir, _origin) = setup_branch_fixture("feature/current", 0, 1);

        let section = build_branch_status_section(work_dir.path(), "feature/current", "main");

        assert!(section.contains("## Branch Status"));
        assert!(section.contains("**Commits behind:** 0"));
        // No ⚠️ when up to date
        assert!(!section.contains("⚠️"));
        assert!(!section.contains("significantly behind"));
    }

    #[test]
    fn test_branch_status_section_behind_below_threshold() {
        let (work_dir, _origin) = setup_branch_fixture("feature/slightly-behind", 3, 1);

        let section =
            build_branch_status_section(work_dir.path(), "feature/slightly-behind", "main");

        assert!(section.contains("## Branch Status"));
        assert!(section.contains("**Commits behind:** 3 ⚠️"));
        assert!(section.contains("**Commits ahead:** 1"));
        // Below threshold — no rebase advisory
        assert!(!section.contains("significantly behind"));
    }

    #[test]
    fn test_branch_status_section_at_threshold() {
        let (work_dir, _origin) =
            setup_branch_fixture("feature/stale", DEFAULT_STALENESS_THRESHOLD as usize, 2);

        let section = build_branch_status_section(work_dir.path(), "feature/stale", "main");

        assert!(section.contains("## Branch Status"));
        assert!(section.contains(&format!(
            "**Commits behind:** {} ⚠️",
            DEFAULT_STALENESS_THRESHOLD
        )));
        assert!(section.contains("**Commits ahead:** 2"));
        // At threshold — rebase advisory shown
        assert!(section.contains("significantly behind main"));
    }

    #[test]
    fn test_branch_status_section_not_on_origin() {
        let (work_dir, _origin) = setup_branch_fixture("feature/pushed", 0, 0);

        // Ask about a branch that was never pushed
        let section = build_branch_status_section(work_dir.path(), "feature/nonexistent", "main");

        assert!(
            section.is_empty(),
            "Expected empty section for missing branch"
        );
    }

    #[test]
    fn test_branch_status_section_git_error() {
        let dir = tempfile::tempdir().unwrap();
        // Not a git repo — should degrade gracefully
        let section = build_branch_status_section(dir.path(), "feature", "main");

        assert!(section.is_empty(), "Expected empty section on git error");
    }

    // ── Integration: build_manager_context with repo_path ────────

    #[test]
    fn test_build_context_with_branch_status() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let manager = create_manager_for_test(&mut conn, &project_id);

        // Set up a git repo where the manager's branch ("mgr/test") is behind main
        let (work_dir, _origin) = setup_branch_fixture(&manager.branch, 12, 3);

        let context =
            build_manager_context(&mut conn, &manager, Some(work_dir.path()), None).unwrap();

        // Should include all standard sections plus branch status
        assert!(context.contains("## Dashboard"));
        assert!(context.contains("## Branch Status"));
        assert!(context.contains(&format!("**Branch:** {}", manager.branch)));
        assert!(context.contains("**Commits behind:** 12 ⚠️"));
        assert!(context.contains("**Commits ahead:** 3"));
        assert!(context.contains("significantly behind main"));
    }

    #[test]
    fn test_build_context_skips_branch_status_for_default_branch() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        // Create a manager whose branch matches the project's default branch ("main")
        let mut clock = MockClock::new();
        clock.expect_now().returning(|| 1700000000);

        let manager = crud::create(
            &mut conn,
            &clock,
            CreateManager {
                project_id: project_id.to_string(),
                home_project_id: None,
                scope_kind: None,
                name: "Default Manager".to_string(),
                branch: "main".to_string(),
                description: Some("Runs on default branch".to_string()),
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        // Set up a git repo — use a different feature branch name for the fixture,
        // then pass the repo to build_manager_context. The guard should prevent
        // branch status from being generated since manager.branch == "main".
        let (work_dir, _origin) = setup_branch_fixture("feature/other", 5, 2);

        let context =
            build_manager_context(&mut conn, &manager, Some(work_dir.path()), None).unwrap();

        // Branch status should be omitted since manager.branch == default_branch
        assert!(
            !context.contains("## Branch Status"),
            "Expected no branch status for default-branch manager"
        );

        // Other sections should still be present
        assert!(context.contains("## Dashboard"));
        assert!(context.contains("## Managed Issues"));
    }

    #[test]
    fn test_build_context_without_repo_path_omits_branch_status() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let manager = create_manager_for_test(&mut conn, &project_id);

        let context = build_manager_context(&mut conn, &manager, None, None).unwrap();

        assert!(!context.contains("## Branch Status"));
    }
}
