//! Memory triage issue creation mechanism.
//!
//! This is deliberately not a recipe trigger node. It is a system-level
//! threshold check that claims exact-scope pending memories and starts a normal
//! issue execution on the `memory-triage` recipe.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::path::PathBuf;

use crate::mcp::handlers::issues::{create_issue_in_project, CreateExecutionSpec};
use crate::memories::canon::{resolve_role_canon_home, RoleCanonHome};
use crate::models::Memory;
use crate::orchestrator::Orchestrator;

const MEMORY_TRIAGE_RECIPE: &str = "memory-triage";
const TRIAGE_NEIGHBOR_MIN_SIMILARITY: f32 = 0.72;
const TRIAGE_NEIGHBOR_LIMIT: usize = 5;

async fn record_batch_or_revert(
    orch: &Orchestrator,
    issue_id: &str,
    memories: &[Memory],
) -> Result<(), String> {
    let ids: Vec<String> = memories.iter().map(|memory| memory.id.clone()).collect();
    match crate::memories::db::record_triage_issue_batch(&orch.db.local, issue_id, &ids).await {
        Ok(()) => Ok(()),
        Err(error) => {
            revert_claimed(orch, memories).await;
            Err(error.to_string())
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ScopeTarget {
    scope: String,
    scope_value: String,
    project_id: String,
    project_key: String,
}

pub(crate) fn role_home_project_id(orch: &Orchestrator, role: &str) -> Result<String, String> {
    let projects = orch.all_project_paths()?;
    let project_refs = projects
        .iter()
        .map(|(project_id, path)| (project_id.as_str(), path.as_path()));
    match resolve_role_canon_home(&orch.config_dir, role, project_refs) {
        RoleCanonHome::Project { project_id } => Ok(project_id),
        RoleCanonHome::Workspace => Ok("workspace".to_string()),
    }
}

async fn scope_target(
    orch: &Orchestrator,
    scope: &str,
    scope_value: &str,
) -> Result<ScopeTarget, String> {
    let project_id = match scope {
        "project" => scope_value.to_string(),
        "role" => role_home_project_id(orch, scope_value)?,
        "workspace" => "workspace".to_string(),
        _ => "workspace".to_string(),
    };
    let project_key = crate::memories::db::project_key_by_id(&orch.db.local, &project_id)
        .await
        .map_err(|error| error.to_string())?;
    Ok(ScopeTarget {
        scope: scope.to_string(),
        scope_value: scope_value.to_string(),
        project_id,
        project_key,
    })
}

fn read_optional(path: PathBuf) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .filter(|s| !s.trim().is_empty())
}

fn skill_list_markdown(orch: &Orchestrator, target: &ScopeTarget) -> String {
    let skills = match target.scope.as_str() {
        "project" => orch.list_skills_for_context(&target.project_id),
        _ => orch.list_skill_configs(Some("default"), None),
    };
    match skills {
        Ok(skills) if !skills.is_empty() => skills
            .into_iter()
            .map(|skill| format!("- `{}` — {}", skill.name, skill.description))
            .collect::<Vec<_>>()
            .join("\n"),
        Ok(_) => "_No workspace/repo skills found._".to_string(),
        Err(error) => format!("_Unable to load skills: {error}_"),
    }
}

fn current_canon_markdown(orch: &Orchestrator, target: &ScopeTarget) -> String {
    match target.scope.as_str() {
        "role" => match orch.get_agent_config(
            &target.scope_value,
            (target.project_id != "workspace").then_some(target.project_id.as_str()),
        ) {
            Ok(Some(agent)) => format!(
                "Role prompt for `{}`:\n\n```markdown\n{}\n```",
                target.scope_value, agent.prompt
            ),
            Ok(None) => format!("_No live role prompt found for `{}`._", target.scope_value),
            Err(error) => format!(
                "_Unable to load role prompt `{}`: {error}_",
                target.scope_value
            ),
        },
        "project" => match orch.project_path(&target.project_id) {
            Ok(path) => read_optional(path.join("AGENTS.md"))
                .map(|content| format!("Project `AGENTS.md`:\n\n```markdown\n{}\n```", content))
                .unwrap_or_else(|| "_No project AGENTS.md found._".to_string()),
            Err(error) => format!("_Unable to resolve project path: {error}_"),
        },
        _ => read_optional(orch.config_dir.join("AGENTS.md"))
            .map(|content| format!("Workspace `AGENTS.md`:\n\n```markdown\n{}\n```", content))
            .unwrap_or_else(|| "_No workspace AGENTS.md found._".to_string()),
    }
}

fn format_neighbor(neighbor: &crate::memories::db::MemoryTriageNeighbor) -> String {
    let reason = neighbor
        .memory
        .reason
        .as_deref()
        .unwrap_or("no reason recorded");
    let promoted = neighbor
        .memory
        .promoted_commit_sha
        .as_deref()
        .map(|sha| format!("; commit `{sha}`"))
        .unwrap_or_default();
    format!(
        "   - {} ({:.2}) — status `{}`; reason: {}{}",
        neighbor.uri, neighbor.similarity, neighbor.memory.status, reason, promoted
    )
}

fn markdown_link_text(content: &str) -> String {
    content
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace('[', "\\[")
        .replace(']', "\\]")
}

async fn seed_description(
    orch: &Orchestrator,
    target: &ScopeTarget,
    memories: &[Memory],
) -> String {
    let mut out = format!(
        "# Memory triage\n\nScope: `{}={}`\nTarget project: `{}`\n\n## Current canon context\n\n{}\n\n## Workspace/repo skills\n\n{}\n\n## Seeded memories\n\n",
        target.scope,
        target.scope_value,
        target.project_id,
        current_canon_markdown(orch, target),
        skill_list_markdown(orch, target),
    );
    let batch_ids: Vec<String> = memories.iter().map(|memory| memory.id.clone()).collect();
    for memory in memories {
        let uri = crate::memories::db::build_node_memory_uri_for_memory(&orch.db.local, memory)
            .await
            .unwrap_or_else(|_| "unavailable".to_string());
        let _ = writeln!(out, "- [{}]({})", markdown_link_text(&memory.content), uri);
        match crate::memories::db::similar_memory_neighbors(
            &orch.db.local,
            memory,
            &uri,
            &batch_ids,
            TRIAGE_NEIGHBOR_MIN_SIMILARITY,
            TRIAGE_NEIGHBOR_LIMIT,
        )
        .await
        {
            Ok(neighbors) if !neighbors.is_empty() => {
                out.push_str("   - similar prior memories with outcomes:\n");
                for neighbor in neighbors {
                    out.push_str(&format_neighbor(&neighbor));
                    out.push('\n');
                }
                out.push('\n');
            }
            Ok(_) | Err(_) => out.push('\n'),
        }
    }
    out
}

async fn revert_claimed(orch: &Orchestrator, memories: &[Memory]) {
    let ids: Vec<String> = memories.iter().map(|memory| memory.id.clone()).collect();
    if let Err(error) =
        crate::memories::db::set_memories_status(&orch.db.local, &ids, "pending").await
    {
        log::warn!("failed to revert claimed memory triage rows: {error}");
    }
}

pub(crate) fn distinct_scopes_from_memories(memories: &[Memory]) -> Vec<(String, String)> {
    let mut seen = HashSet::new();
    let mut scopes = Vec::new();
    for memory in memories {
        let key = (memory.scope.to_string(), memory.scope_value.clone());
        if seen.insert(key.clone()) {
            scopes.push(key);
        }
    }
    scopes
}

/// Claim a full threshold batch for one exact-scope pending pool and create a
/// normal issue execution to triage it. Returns the created issue URI, or `None`
/// when the pool is below threshold, already drained, or already covered by an
/// open triage issue for that scope. Shared core for both the event-driven fast
/// path (`maybe_spawn_triage`) and the reconciliation sweep
/// (`reconcile_pending_triage`), so the two can never diverge.
async fn spawn_triage_for_scope(
    orch: &Orchestrator,
    scope: &str,
    scope_value: &str,
    threshold: i64,
) -> Result<Option<String>, String> {
    let count =
        crate::memories::db::count_pending_memories_for_scope(&orch.db.local, scope, scope_value)
            .await
            .map_err(|error| error.to_string())?;
    if count < threshold {
        return Ok(None);
    }

    // Don't spawn a second issue for a scope already covered by a live triage
    // issue. Catches both an in-flight batch and an orphan issue whose execution
    // start failed (batch reverted, issue left open), so a scope that keeps
    // getting re-fed pending memories does not churn duplicate issues.
    if crate::memories::db::open_triage_issue_for_scope(&orch.db.local, scope, scope_value)
        .await
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Ok(None);
    }

    let target = scope_target(orch, scope, scope_value).await?;
    let claimed = crate::memories::db::claim_pending_memories_for_scope(
        &orch.db.local,
        scope,
        scope_value,
        threshold,
    )
    .await
    .map_err(|error| error.to_string())?;

    if claimed.is_empty() {
        return Ok(None);
    }
    let description = seed_description(orch, &target, &claimed).await;
    let title = format!(
        "Memory triage: {}={} ({} pending)",
        target.scope,
        target.scope_value,
        claimed.len()
    );
    let outcome = create_issue_in_project(
        orch,
        &target.project_key,
        title,
        Some(description),
        None,
        Some(CreateExecutionSpec {
            recipe: Some(MEMORY_TRIAGE_RECIPE.to_string()),
            backend: None,
        }),
        None,
        None,
    )
    .await;

    match outcome {
        Ok(outcome) => {
            record_batch_or_revert(orch, &outcome.issue_id, &claimed).await?;
            Ok(Some(outcome.uri))
        }
        Err(error) => {
            revert_claimed(orch, &claimed).await;
            Err(error)
        }
    }
}

/// For each just-confirmed exact-scope pool, claim a full threshold batch of
/// globally pending memories and create normal issue executions to triage them.
/// The event-driven fast path; delegates to the shared `spawn_triage_for_scope`
/// core. Returns an empty vec when no scoped pool meets the threshold.
pub async fn maybe_spawn_triage(
    orch: Orchestrator,
    confirmed_scopes: Vec<(String, String)>,
) -> Result<Vec<String>, String> {
    let threshold = orch.get_settings().pending_memory_threshold.max(1) as i64;
    let mut spawned = Vec::new();
    for (scope, scope_value) in confirmed_scopes {
        if let Some(uri) = spawn_triage_for_scope(&orch, &scope, &scope_value, threshold).await? {
            spawned.push(uri);
        }
    }
    Ok(spawned)
}

/// Reconciliation step 3: spawn a triage issue for every at-threshold pending
/// pool discovered directly from DB state. Idempotent — pools already covered by
/// an open triage issue are skipped.
pub async fn reconcile_pending_triage(orch: &Orchestrator) -> Result<Vec<String>, String> {
    let threshold = orch.get_settings().pending_memory_threshold.max(1) as i64;
    let scopes = crate::memories::db::distinct_pending_scopes(&orch.db.local)
        .await
        .map_err(|error| error.to_string())?;
    let mut spawned = Vec::new();
    for (scope, scope_value) in scopes {
        if let Some(uri) = spawn_triage_for_scope(orch, &scope, &scope_value, threshold).await? {
            spawned.push(uri);
        }
    }
    Ok(spawned)
}

/// State-driven reconciliation sweep guaranteeing every qualifying memory pool
/// has a triage issue. Idempotent and safe to run repeatedly. Runs in order:
/// confirm drafts stranded on terminal jobs (recovers stuck `draft`); re-home
/// claimed memories whose triage issue was never created (recovers stuck
/// `claimed`); recover batches stranded on a *failed* triage execution back to
/// `pending`; finalize batches whose triage issue already merged but never had
/// its decisions applied (catch-up and safety net for the canon merge gate);
/// then spawn a triage issue for every at-threshold pending pool (recovers stuck
/// `pending`). Entry point for the startup + periodic loop.
pub async fn reconcile_memory_triage(orch: Orchestrator) -> Result<(), String> {
    let confirmed = crate::memories::commands::confirm_orphaned_drafts(&orch)?;
    if confirmed > 0 {
        log::info!("memory triage reconcile: confirmed {confirmed} orphaned draft memories");
    }

    let reverted = crate::memories::db::revert_orphaned_claimed_memories(&orch.db.local)
        .await
        .map_err(|error| error.to_string())?;
    if !reverted.is_empty() {
        log::info!(
            "memory triage reconcile: re-homed {} orphaned claimed memories",
            reverted.len()
        );
    }

    let recovered = crate::memories::db::revert_claimed_for_failed_triage_issues(&orch.db.local)
        .await
        .map_err(|error| error.to_string())?;
    if !recovered.is_empty() {
        log::info!(
            "memory triage reconcile: recovered {} memories stranded on failed triage issues",
            recovered.len()
        );
    }

    let merged_issues =
        crate::memories::db::merged_triage_issues_with_claimed_memories(&orch.db.local)
            .await
            .map_err(|error| error.to_string())?;
    let mut finalized = 0usize;
    for issue_id in &merged_issues {
        match crate::memories::db::resolve_triage_batch_on_merge(&orch.db.local, issue_id).await {
            Ok(ids) => finalized += ids.len(),
            Err(error) => log::warn!(
                "memory triage reconcile: failed to finalize merged triage batch for issue {issue_id}: {error}"
            ),
        }
    }
    if finalized > 0 {
        log::info!(
            "memory triage reconcile: finalized {finalized} memories on already-merged triage issues"
        );
    }

    let spawned = reconcile_pending_triage(&orch).await?;
    if !spawned.is_empty() {
        log::info!(
            "memory triage reconcile: spawned {} triage issue(s)",
            spawned.len()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        current_canon_markdown, distinct_scopes_from_memories, format_neighbor,
        role_home_project_id, ScopeTarget,
    };
    use crate::db::DbState;
    use crate::memories::db::MemoryTriageNeighbor;
    use crate::models::{Memory, MemoryScope, MemoryStatus};
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, MigrationRunner, RowExt, SearchIndex, TURSO_MIGRATIONS};
    use tempfile::TempDir;
    use turso::params;

    struct TestOrch {
        _temp: TempDir,
        orch: crate::orchestrator::Orchestrator,
        project_dir: std::path::PathBuf,
    }

    async fn test_orch() -> TestOrch {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join("config");
        let workspace_dir = temp.path().join("workspace");
        let project_dir = temp.path().join("project");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        std::fs::create_dir_all(&workspace_dir).unwrap();
        std::fs::create_dir_all(project_dir.join(".cairn/agents")).unwrap();
        std::fs::write(
            config_dir.join("recipes/memory-triage.yaml"),
            include_str!("../../../../recipes/memory-triage.yaml"),
        )
        .unwrap();
        std::fs::write(
            config_dir.join("agents/integrator.md"),
            include_str!("../../../../agents/integrator.md"),
        )
        .unwrap();

        let local = Arc::new(
            LocalDb::open(temp.path().join("triage-test.db"))
                .await
                .unwrap(),
        );
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        local
            .write(|conn| {
                let workspace_path = workspace_dir.to_string_lossy().to_string();
                let project_path = project_dir.to_string_lossy().to_string();
                Box::pin(async move {
                    conn.execute(
                        "INSERT OR IGNORE INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace) VALUES ('workspace', 'default', 'Workspace', 'WKS', ?1, 1, 1, 1)",
                        params![workspace_path.as_str()],
                    )
                    .await?;
                    conn.execute(
                        "INSERT OR IGNORE INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('project-1', 'default', 'Project', 'PRJ', ?1, 1, 1)",
                        params![project_path.as_str()],
                    )
                    .await?;
                    conn.execute(
                        "INSERT OR IGNORE INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('issue-main', 'project-1', 42, 'Main', 'active', 1, 1)",
                        (),
                    )
                    .await?;
                    conn.execute(
                        "INSERT OR IGNORE INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('exec-main', 'recipe', 'issue-main', 'project-1', 'running', 1, 1)",
                        (),
                    )
                    .await?;
                    conn.execute(
                        "INSERT OR IGNORE INTO jobs (id, execution_id, issue_id, project_id, status, node_name, uri_segment, created_at, updated_at) VALUES ('job-main', 'exec-main', 'issue-main', 'project-1', 'running', 'builder', 'builder', 1, 1)",
                        (),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let search_index =
            Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let db = Arc::new(DbState::new(local, search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = OrchestratorBuilder::new(db, services, config_dir).build();
        TestOrch {
            _temp: temp,
            orch,
            project_dir,
        }
    }

    fn agent_markdown(name: &str, prompt: &str) -> String {
        format!("---\nname: {name}\ndescription: test agent\ntools:\n  - Read\n---\n\n{prompt}")
    }

    async fn insert_pending_memory(
        test: &TestOrch,
        id: &str,
        project_id: &str,
        scope: &str,
        scope_value: &str,
        created_at: i64,
    ) {
        test.orch
            .db
            .local
            .execute(
                "INSERT INTO memories (id, name, project_id, content, status, scope, scope_value, job_id, node_seq, provenance_uri, created_at, updated_at)
                 VALUES (?1, ?1, ?2, ?1, 'pending', ?3, ?4, 'job-main', ?5, 'cairn://p/CAIRN/42/1/builder/chat/turn/2', ?5, ?5)",
                params![id, project_id, scope, scope_value, created_at],
            )
            .await
            .unwrap();
    }

    async fn issue_count(test: &TestOrch, project_id: &str) -> i64 {
        test.orch
            .db
            .local
            .query_one(
                "SELECT COUNT(*) FROM issues WHERE project_id = ?1",
                params![project_id],
                |row| row.i64(0),
            )
            .await
            .unwrap()
    }

    async fn memory_status_count(
        test: &TestOrch,
        status: &str,
        scope: &str,
        scope_value: &str,
    ) -> i64 {
        test.orch
            .db
            .local
            .query_one(
                "SELECT COUNT(*) FROM memories WHERE status = ?1 AND scope = ?2 AND scope_value = ?3",
                params![status, scope, scope_value],
                |row| row.i64(0),
            )
            .await
            .unwrap()
    }

    fn memory(id: &str, project_id: &str, content: &str) -> Memory {
        Memory {
            id: id.to_string(),
            name: Some(id.to_string()),
            project_id: Some(project_id.to_string()),
            content: content.to_string(),
            status: MemoryStatus::Claimed,
            scope: MemoryScope::Project,
            scope_value: project_id.to_string(),
            job_id: None,
            node_seq: None,
            promoted_commit_sha: None,
            reason: None,
            triage_decision: None,
            deferred_scope: None,
            deferred_scope_value: None,
            provenance_uri: Some("cairn://p/CAIRN/1/1/builder/chat/turn/2".to_string()),
            created_at: 1,
            updated_at: 1,
        }
    }

    #[test]
    fn distinct_scopes_preserves_first_seen_scope_pools() {
        let mut workspace = memory("workspace", "workspace", "workspace");
        workspace.scope = MemoryScope::Workspace;
        workspace.scope_value = "workspace".to_string();
        let mut role = memory("role", "project-1", "role");
        role.scope = MemoryScope::Role;
        role.scope_value = "builder".to_string();
        let project = memory("project", "project-1", "project");
        let duplicate_role = role.clone();

        assert_eq!(
            distinct_scopes_from_memories(&[workspace, role, project, duplicate_role]),
            vec![
                ("workspace".to_string(), "workspace".to_string()),
                ("role".to_string(), "builder".to_string()),
                ("project".to_string(), "project-1".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn workspace_scope_accumulated_pool_spawns_triage_issue() {
        let test = test_orch().await;
        for idx in 0..4 {
            insert_pending_memory(
                &test,
                &format!("old-workspace-{idx}"),
                "project-1",
                "workspace",
                "workspace",
                idx + 1,
            )
            .await;
        }
        insert_pending_memory(
            &test,
            "just-confirmed-workspace",
            "project-1",
            "workspace",
            "workspace",
            10,
        )
        .await;

        let spawned = super::maybe_spawn_triage(
            test.orch.clone(),
            vec![("workspace".to_string(), "workspace".to_string())],
        )
        .await
        .unwrap();

        assert_eq!(spawned.len(), 1);
        assert_eq!(issue_count(&test, "workspace").await, 1);
        assert_eq!(
            memory_status_count(&test, "claimed", "workspace", "workspace").await,
            5
        );
    }

    #[tokio::test]
    async fn multi_scope_confirmed_set_triggers_each_full_pool() {
        let test = test_orch().await;
        for idx in 0..5 {
            insert_pending_memory(
                &test,
                &format!("workspace-{idx}"),
                "project-1",
                "workspace",
                "workspace",
                idx + 1,
            )
            .await;
            insert_pending_memory(
                &test,
                &format!("role-{idx}"),
                "project-1",
                "role",
                "builder",
                idx + 10,
            )
            .await;
        }
        for idx in 0..4 {
            insert_pending_memory(
                &test,
                &format!("project-{idx}"),
                "project-1",
                "project",
                "project-1",
                idx + 20,
            )
            .await;
        }

        let spawned = super::maybe_spawn_triage(
            test.orch.clone(),
            vec![
                ("workspace".to_string(), "workspace".to_string()),
                ("role".to_string(), "builder".to_string()),
                ("project".to_string(), "project-1".to_string()),
            ],
        )
        .await
        .unwrap();

        assert_eq!(spawned.len(), 2);
        assert_eq!(issue_count(&test, "workspace").await, 2);
        assert_eq!(
            memory_status_count(&test, "claimed", "workspace", "workspace").await,
            5
        );
        assert_eq!(
            memory_status_count(&test, "claimed", "role", "builder").await,
            5
        );
        assert_eq!(
            memory_status_count(&test, "pending", "project", "project-1").await,
            4
        );
    }

    #[tokio::test]
    async fn below_threshold_scope_does_not_spawn_triage_issue() {
        let test = test_orch().await;
        for idx in 0..4 {
            insert_pending_memory(
                &test,
                &format!("workspace-{idx}"),
                "project-1",
                "workspace",
                "workspace",
                idx + 1,
            )
            .await;
        }

        let spawned = super::maybe_spawn_triage(
            test.orch.clone(),
            vec![("workspace".to_string(), "workspace".to_string())],
        )
        .await
        .unwrap();

        assert!(spawned.is_empty());
        assert_eq!(issue_count(&test, "workspace").await, 0);
        assert_eq!(
            memory_status_count(&test, "pending", "workspace", "workspace").await,
            4
        );
    }

    #[test]
    fn scope_target_carries_scope_for_description_title() {
        let target = ScopeTarget {
            scope: "role".to_string(),
            scope_value: "builder".to_string(),
            project_id: "workspace".to_string(),
            project_key: "WKS".to_string(),
        };
        assert_eq!(target.scope, "role");
        assert_eq!(target.scope_value, "builder");
    }

    #[tokio::test]
    async fn project_defined_role_targets_project_and_loads_project_prompt() {
        let test = test_orch().await;
        std::fs::write(
            test.project_dir.join(".cairn/agents/builder.md"),
            agent_markdown("Project Builder", "Project role prompt."),
        )
        .unwrap();

        let project_id = role_home_project_id(&test.orch, "builder").unwrap();
        assert_eq!(project_id, "project-1");

        let target = ScopeTarget {
            scope: "role".to_string(),
            scope_value: "builder".to_string(),
            project_id,
            project_key: "PRJ".to_string(),
        };
        let markdown = current_canon_markdown(&test.orch, &target);
        assert!(markdown.contains("Project role prompt."));
    }

    #[tokio::test]
    async fn workspace_defined_role_targets_workspace_and_loads_workspace_prompt() {
        let test = test_orch().await;
        std::fs::write(
            test.orch.config_dir.join("agents/builder.md"),
            agent_markdown("Workspace Builder", "Workspace role prompt."),
        )
        .unwrap();

        let project_id = role_home_project_id(&test.orch, "builder").unwrap();
        assert_eq!(project_id, "workspace");

        let target = ScopeTarget {
            scope: "role".to_string(),
            scope_value: "builder".to_string(),
            project_id,
            project_key: "WKS".to_string(),
        };
        let markdown = current_canon_markdown(&test.orch, &target);
        assert!(markdown.contains("Workspace role prompt."));
    }

    #[test]
    fn neighbor_format_includes_resolution_issue_commit_and_reason() {
        let mut neighbor = MemoryTriageNeighbor {
            memory: memory("prior", "project-1", "old"),
            uri: "cairn://p/CAIRN/1/1/builder/memories/1".to_string(),
            similarity: 0.91,
            triage_issue_uri: Some("cairn://p/CAIRN/7".to_string()),
        };
        neighbor.memory.status = MemoryStatus::Promoted;
        neighbor.memory.promoted_commit_sha = Some("abc123".to_string());
        neighbor.memory.reason = Some("generalized into AGENTS".to_string());

        let formatted = format_neighbor(&neighbor);
        assert!(formatted.starts_with("   - cairn://p/CAIRN/1/1/builder/memories/1 (0.91)"));
        assert!(!formatted.contains("prior"));
        assert!(!formatted.contains("via"));
        assert!(!formatted.contains("cairn://p/CAIRN/7"));
        assert!(formatted.contains("commit `abc123`"));
        assert!(formatted.contains("generalized into AGENTS"));
    }

    #[tokio::test]
    async fn seed_description_uses_memory_uris_without_uuid_or_provenance_cruft() {
        let test = test_orch().await;
        let uuid = "cbd10c2e-765b-4235-9b65-ccaf62f4572d";
        insert_pending_memory(&test, uuid, "project-1", "project", "project-1", 4).await;
        test.orch
            .db
            .local
            .execute(
                "UPDATE memories SET content = 'durable   behavior\nnote' WHERE id = ?1",
                params![uuid],
            )
            .await
            .unwrap();
        let memory = crate::memories::db::load_memory(&test.orch.db.local, uuid)
            .await
            .unwrap();
        let target = ScopeTarget {
            scope: "project".to_string(),
            scope_value: "project-1".to_string(),
            project_id: "project-1".to_string(),
            project_key: "PRJ".to_string(),
        };

        let description = super::seed_description(&test.orch, &target, &[memory]).await;

        assert!(description
            .contains("- [durable behavior note](cairn://p/PRJ/42/1/builder/memories/4)"));
        assert!(!description.contains("```text"));
        assert!(!description.contains("content:"));
        assert!(!description.contains("none above threshold"));
        assert!(!description.contains("unavailable"));
        assert!(!description.contains(uuid));
        assert!(!description.contains("provenance:"));
        assert!(!description.contains("chat/turn"));
        assert!(
            !description.contains("The Integrator should judge each seeded memory independently")
        );
        assert!(!description.contains("Threshold note:"));
    }

    // Test seed helper mirrors the memories table's full column set; the long
    // parameter list is inherent to inserting a complete row.
    #[allow(clippy::too_many_arguments)]
    async fn insert_memory_with_status(
        test: &TestOrch,
        id: &str,
        status: &str,
        scope: &str,
        scope_value: &str,
        job_id: &str,
        node_seq: i64,
        created_at: i64,
    ) {
        test.orch
            .db
            .local
            .execute(
                "INSERT INTO memories (id, name, project_id, content, status, scope, scope_value, job_id, node_seq, provenance_uri, created_at, updated_at)
                 VALUES (?1, ?1, 'project-1', ?1, ?2, ?3, ?4, ?5, ?6, 'cairn://p/CAIRN/42/1/builder/chat/turn/2', ?7, ?7)",
                params![id, status, scope, scope_value, job_id, node_seq, created_at],
            )
            .await
            .unwrap();
    }

    async fn insert_terminal_job(
        test: &TestOrch,
        id: &str,
        status: &str,
        review_state: Option<&str>,
        uri_segment: &str,
    ) {
        test.orch
            .db
            .local
            .execute(
                "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, node_name, uri_segment, memory_review_state, created_at, updated_at)
                 VALUES (?1, 'exec-main', 'issue-main', 'project-1', ?2, 'builder', ?3, ?4, 1, 1)",
                params![id, status, uri_segment, review_state],
            )
            .await
            .unwrap();
    }

    async fn insert_memory_review_turn(test: &TestOrch, job_id: &str, state: &str) {
        test.orch
            .db
            .local
            .write(|conn| {
                let job_id = job_id.to_string();
                let state = state.to_string();
                Box::pin(async move {
                    let session_id = format!("s-{job_id}");
                    let turn_id = format!("t-{job_id}");
                    conn.execute(
                        "INSERT OR IGNORE INTO sessions (id, job_id, status, created_at, updated_at) VALUES (?1, ?2, 'open', 1, 1)",
                        params![session_id.as_str(), job_id.as_str()],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, updated_at) VALUES (?1, ?2, ?3, 1, ?4, 'memory_review', 2, 2)",
                        params![turn_id.as_str(), session_id.as_str(), job_id.as_str(), state.as_str()],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }

    async fn link_memory_to_issue(test: &TestOrch, issue_id: &str, memory_id: &str) {
        test.orch
            .db
            .local
            .execute(
                "INSERT INTO memory_triage_issue_memories (issue_id, memory_id) VALUES (?1, ?2)",
                params![issue_id, memory_id],
            )
            .await
            .unwrap();
    }

    async fn triage_issue_count(test: &TestOrch) -> i64 {
        test.orch
            .db
            .local
            .query_one(
                "SELECT COUNT(*) FROM issues WHERE title LIKE 'Memory triage:%'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap()
    }

    async fn draft_count(test: &TestOrch, job_id: &str) -> i64 {
        test.orch
            .db
            .local
            .query_one(
                "SELECT COUNT(*) FROM memories WHERE job_id = ?1 AND status = 'draft'",
                params![job_id],
                |row| row.i64(0),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn reconcile_spawns_for_unconfirmed_pending_pool() {
        let test = test_orch().await;
        for idx in 0..5 {
            insert_pending_memory(
                &test,
                &format!("p-{idx}"),
                "project-1",
                "project",
                "project-1",
                idx + 1,
            )
            .await;
        }

        super::reconcile_memory_triage(test.orch.clone())
            .await
            .unwrap();

        assert_eq!(triage_issue_count(&test).await, 1);
        assert_eq!(
            memory_status_count(&test, "claimed", "project", "project-1").await,
            5
        );
    }

    #[tokio::test]
    async fn reconcile_is_idempotent_when_nothing_stranded() {
        let test = test_orch().await;
        for idx in 0..5 {
            insert_pending_memory(
                &test,
                &format!("p-{idx}"),
                "project-1",
                "project",
                "project-1",
                idx + 1,
            )
            .await;
        }

        super::reconcile_memory_triage(test.orch.clone())
            .await
            .unwrap();
        super::reconcile_memory_triage(test.orch.clone())
            .await
            .unwrap();

        assert_eq!(triage_issue_count(&test).await, 1);
        assert_eq!(
            memory_status_count(&test, "claimed", "project", "project-1").await,
            5
        );
        assert_eq!(
            memory_status_count(&test, "pending", "project", "project-1").await,
            0
        );
    }

    #[tokio::test]
    async fn reconcile_recreates_issue_after_closed_batch_revert() {
        let test = test_orch().await;
        // A prior triage batch was closed and reverted: its issue is closed and
        // the memories are back to pending. The close route never re-checks the
        // threshold, so without reconcile they sit pending forever.
        test.orch
            .db
            .local
            .execute(
                "INSERT INTO issues (id, project_id, number, title, status, closed_at, created_at, updated_at)
                 VALUES ('issue-closed', 'project-1', 7, 'Memory triage: project=project-1 (5 pending)', 'closed', 5, 1, 1)",
                (),
            )
            .await
            .unwrap();
        for idx in 0..5 {
            insert_pending_memory(
                &test,
                &format!("p-{idx}"),
                "project-1",
                "project",
                "project-1",
                idx + 1,
            )
            .await;
            link_memory_to_issue(&test, "issue-closed", &format!("p-{idx}")).await;
        }

        super::reconcile_memory_triage(test.orch.clone())
            .await
            .unwrap();

        // The closed issue is terminal, so the guard allows a fresh spawn.
        assert_eq!(triage_issue_count(&test).await, 2);
        assert_eq!(
            memory_status_count(&test, "claimed", "project", "project-1").await,
            5
        );
    }

    #[tokio::test]
    async fn reconcile_respects_lowered_threshold() {
        let test = test_orch().await;
        std::fs::write(
            test.orch.config_dir.join("settings.yaml"),
            "pendingMemoryThreshold: 3\n",
        )
        .unwrap();
        for idx in 0..3 {
            insert_pending_memory(
                &test,
                &format!("p-{idx}"),
                "project-1",
                "project",
                "project-1",
                idx + 1,
            )
            .await;
        }

        super::reconcile_memory_triage(test.orch.clone())
            .await
            .unwrap();

        assert_eq!(triage_issue_count(&test).await, 1);
        assert_eq!(
            memory_status_count(&test, "claimed", "project", "project-1").await,
            3
        );
    }

    #[tokio::test]
    async fn reconcile_rehomes_orphaned_claims_and_leaves_linked_untouched() {
        let test = test_orch().await;
        // Three claimed memories never linked to a triage issue — a crash between
        // claim and issue creation. Below threshold, so they simply return to
        // pending after reconcile.
        for idx in 0..3 {
            insert_memory_with_status(
                &test,
                &format!("orphan-{idx}"),
                "claimed",
                "project",
                "project-1",
                "job-main",
                idx + 1,
                idx + 1,
            )
            .await;
        }
        // A claimed memory tied to a live triage issue must be left alone.
        insert_memory_with_status(
            &test, "linked", "claimed", "role", "builder", "job-main", 20, 20,
        )
        .await;
        link_memory_to_issue(&test, "issue-main", "linked").await;

        super::reconcile_memory_triage(test.orch.clone())
            .await
            .unwrap();

        assert_eq!(
            memory_status_count(&test, "pending", "project", "project-1").await,
            3
        );
        assert_eq!(
            memory_status_count(&test, "claimed", "project", "project-1").await,
            0
        );
        assert_eq!(
            memory_status_count(&test, "claimed", "role", "builder").await,
            1
        );
        assert_eq!(triage_issue_count(&test).await, 0);
    }

    #[tokio::test]
    async fn reconcile_confirms_failed_job_drafts_but_skips_inflight_review() {
        let test = test_orch().await;
        insert_terminal_job(&test, "job-failed", "failed", None, "builder-failed").await;
        for idx in 0..2 {
            insert_memory_with_status(
                &test,
                &format!("failed-{idx}"),
                "draft",
                "project",
                "project-1",
                "job-failed",
                idx + 1,
                idx + 1,
            )
            .await;
        }
        insert_terminal_job(&test, "job-sent", "complete", Some("sent"), "builder-sent").await;
        for idx in 0..2 {
            insert_memory_with_status(
                &test,
                &format!("sent-{idx}"),
                "draft",
                "project",
                "project-1",
                "job-sent",
                idx + 1,
                idx + 10,
            )
            .await;
        }
        // job-sent's review turn is still running: its drafts must stay draft.
        insert_memory_review_turn(&test, "job-sent", "running").await;

        super::reconcile_memory_triage(test.orch.clone())
            .await
            .unwrap();

        assert_eq!(draft_count(&test, "job-failed").await, 0);
        assert_eq!(draft_count(&test, "job-sent").await, 2);
        assert_eq!(
            memory_status_count(&test, "pending", "project", "project-1").await,
            2
        );
    }

    #[tokio::test]
    async fn reconcile_skips_scope_with_open_triage_issue() {
        let test = test_orch().await;
        // An open (non-terminal) triage issue already covers this scope.
        test.orch
            .db
            .local
            .execute(
                "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                 VALUES ('issue-open', 'project-1', 8, 'Memory triage: project=project-1 (5 pending)', 'active', 1, 1)",
                (),
            )
            .await
            .unwrap();
        for idx in 0..5 {
            insert_pending_memory(
                &test,
                &format!("p-{idx}"),
                "project-1",
                "project",
                "project-1",
                idx + 1,
            )
            .await;
        }

        super::reconcile_memory_triage(test.orch.clone())
            .await
            .unwrap();

        // No duplicate issue, and the pool stays pending (no fresh claim).
        assert_eq!(triage_issue_count(&test).await, 1);
        assert_eq!(
            memory_status_count(&test, "pending", "project", "project-1").await,
            5
        );
        assert_eq!(
            memory_status_count(&test, "claimed", "project", "project-1").await,
            0
        );
    }

    #[tokio::test]
    async fn reconcile_recovers_failed_triage_batch_and_respawns() {
        let test = test_orch().await;
        // A prior triage execution FAILED after its batch was claimed and linked:
        // status='failed', neither merged nor closed. Without recovery the batch
        // stays 'claimed' and the failed issue keeps open_triage_issue_for_scope
        // reporting a live cover, wedging the scope against every future spawn.
        test.orch
            .db
            .local
            .execute(
                "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                 VALUES ('issue-failed', 'project-1', 9, 'Memory triage: project=project-1 (5 pending)', 'failed', 1, 1)",
                (),
            )
            .await
            .unwrap();
        for idx in 0..5 {
            insert_memory_with_status(
                &test,
                &format!("stranded-{idx}"),
                "claimed",
                "project",
                "project-1",
                "job-main",
                idx + 1,
                idx + 1,
            )
            .await;
            link_memory_to_issue(&test, "issue-failed", &format!("stranded-{idx}")).await;
        }
        // A decision recorded before the failure must be cleared on recovery.
        crate::memories::db::record_triage_decision(
            &test.orch.db.local,
            "stranded-0",
            crate::models::MemoryTriageDecision::Discard,
            "decided before the execution failed",
            None,
            None,
        )
        .await
        .unwrap();

        super::reconcile_memory_triage(test.orch.clone())
            .await
            .unwrap();

        // The failed issue no longer blocks: the recovered pool spawns a fresh
        // triage issue that re-claims a full batch.
        assert_eq!(triage_issue_count(&test).await, 2);
        assert_eq!(
            memory_status_count(&test, "claimed", "project", "project-1").await,
            5
        );
        assert_eq!(
            memory_status_count(&test, "pending", "project", "project-1").await,
            0
        );
        let recovered = crate::memories::db::load_memory(&test.orch.db.local, "stranded-0")
            .await
            .unwrap();
        assert!(recovered.triage_decision.is_none());
    }

    #[tokio::test]
    async fn reconcile_finalizes_merged_triage_batch_left_claimed() {
        let test = test_orch().await;
        // A triage issue MERGED but its batch was never resolved (the canon merge
        // gate missed it), so decided memories sit 'claimed' forever. Reconcile
        // applies the recorded decisions.
        test.orch
            .db
            .local
            .execute(
                "INSERT INTO issues (id, project_id, number, title, status, merged_at, created_at, updated_at)
                 VALUES ('issue-merged', 'project-1', 11, 'Memory triage: project=project-1 (3 pending)', 'merged', 100, 1, 1)",
                (),
            )
            .await
            .unwrap();
        for (idx, decision) in ["promote", "discard"].iter().enumerate() {
            let id = format!("decided-{idx}");
            insert_memory_with_status(
                &test,
                &id,
                "claimed",
                "project",
                "project-1",
                "job-main",
                (idx as i64) + 1,
                (idx as i64) + 1,
            )
            .await;
            link_memory_to_issue(&test, "issue-merged", &id).await;
            crate::memories::db::record_triage_decision(
                &test.orch.db.local,
                &id,
                decision
                    .parse::<crate::models::MemoryTriageDecision>()
                    .unwrap(),
                "decided at triage time",
                None,
                None,
            )
            .await
            .unwrap();
        }
        // An undecided claimed memory on the same merged issue returns to pending.
        insert_memory_with_status(
            &test,
            "undecided",
            "claimed",
            "project",
            "project-1",
            "job-main",
            3,
            3,
        )
        .await;
        link_memory_to_issue(&test, "issue-merged", "undecided").await;

        super::reconcile_memory_triage(test.orch.clone())
            .await
            .unwrap();

        assert_eq!(
            memory_status_count(&test, "promoted", "project", "project-1").await,
            1
        );
        assert_eq!(
            memory_status_count(&test, "discarded", "project", "project-1").await,
            1
        );
        // Undecided returns to pending; with only 1 pending (< threshold 5) no new
        // triage issue spawns.
        assert_eq!(
            memory_status_count(&test, "pending", "project", "project-1").await,
            1
        );
        assert_eq!(triage_issue_count(&test).await, 1);
    }
}
