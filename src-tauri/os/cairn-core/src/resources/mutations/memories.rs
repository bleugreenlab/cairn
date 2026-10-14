//! Memory resource mutations (append / patch / delete via `write`).
//!
//! Agents create and mutate memories only through node-scoped memory URIs.
//! The agent-supplied `name` is display-only; identity is the node URI.

use crate::mcp::handlers::run_context;
use crate::mcp::types::McpCallbackRequest;
use crate::models::{MemoryScope, MemoryStatus, MemoryTriageDecision};
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;
use cairn_common::uri::build_node_memory_uri;

use super::PromotedMemoryRef;

pub(super) struct MemoryCreateTarget<'a> {
    pub project: &'a str,
    pub number: i32,
    pub exec_seq: i32,
    pub node_id: &'a str,
}

pub(super) struct MemoryResourceTarget<'a> {
    pub project: &'a str,
    pub number: i32,
    pub exec_seq: i32,
    pub node_id: &'a str,
    pub memory_seq: i32,
}

async fn resolve_target_memory_id(
    orch: &Orchestrator,
    target: &MemoryResourceTarget<'_>,
) -> Result<String, String> {
    crate::memories::db::resolve_node_memory_id(
        orch.db.for_project(target.project).await.as_ref(),
        target.project,
        target.number,
        target.exec_seq,
        target.node_id,
        target.memory_seq,
    )
    .await
    .map_err(|e| e.to_string())?
    .ok_or_else(|| {
        format!(
            "Memory not found at {}",
            build_node_memory_uri(
                target.project,
                target.number,
                target.exec_seq,
                target.node_id,
                target.memory_seq,
            )
        )
    })
}

fn parse_triage_action(raw: &str) -> Result<MemoryTriageDecision, String> {
    raw.parse::<MemoryTriageDecision>()
        .map_err(|_| "payload.action must be one of promote, discard, defer".to_string())
}

/// Parse and resolve `payload.newScope` for a `defer` decision.
///
/// A project defer's stored value must be a `projects.id`: applying the decision
/// writes it to both the memory's `scope_value` (a project pool is keyed by id)
/// and its `project_id`, which is an enforced foreign key. Agents name projects by
/// key (`CAIRN`), so a key is resolved here and a reference naming no project is
/// rejected at the crossing — rather than becoming a row that only fails, much
/// later, when the merged batch tries to finalize (CAIRN-3289).
async fn parse_deferred_scope(
    db: &crate::storage::LocalDb,
    payload: &serde_json::Value,
) -> Result<(Option<MemoryScope>, Option<String>), String> {
    let Some(new_scope) = payload.get("newScope").or_else(|| payload.get("new_scope")) else {
        return Ok((None, None));
    };
    let scope_raw = new_scope
        .get("scope")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or("payload.newScope.scope is required when newScope is supplied")?;
    let scope = scope_raw
        .parse::<MemoryScope>()
        .map_err(|e| format!("payload.newScope.scope: {e}"))?;
    let value = new_scope
        .get("value")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| match scope {
            MemoryScope::Workspace => Some("workspace".to_string()),
            _ => None,
        });
    let Some(value) = value else {
        return Err("payload.newScope.value is required for project and role defers".to_string());
    };
    let value = match scope {
        MemoryScope::Project => crate::memories::db::project_id_for_reference(db, &value)
            .await
            .map_err(|e| e.to_string())?
        .ok_or_else(|| {
            format!("payload.newScope.value '{value}' does not name a project (use its project key, e.g. CAIRN)")
        })?,
        MemoryScope::Role | MemoryScope::Workspace => value,
    };
    Ok((Some(scope), Some(value)))
}

/// Build the provenance URI for the turn that is currently executing this write.
async fn appending_turn_uri(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Result<Option<String>, String> {
    let Some(run_id) = request.run_id.as_deref() else {
        return Ok(None);
    };
    let Some(turn_id) = orch.process_state.get_current_turn_id(run_id) else {
        return Ok(None);
    };
    let base_uri = run_context::lookup_home_uri(&orch.db.local, request).await?;
    let turn_id_for_query = turn_id.clone();
    let sequence = orch
        .db
        .local
        .read(|conn| {
            let turn_id = turn_id_for_query.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT sequence FROM turns WHERE id = ?1 LIMIT 1",
                        (turn_id.as_str(),),
                    )
                    .await?;
                rows.next().await?.map(|row| row.i64(0)).transpose()
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    Ok(sequence.map(|seq| format!("{}/chat/turn/{seq}", base_uri.trim_end_matches('/'))))
}

pub(super) async fn apply_node_memory_append(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: &serde_json::Value,
    target: MemoryCreateTarget<'_>,
) -> Result<String, String> {
    let name = super::payload_trimmed_non_empty_str(payload, "name", &[]).map(str::to_string);
    let content = super::payload_trimmed_non_empty_str(payload, "content", &[])
        .ok_or("payload.content is required and must be a non-empty string")?
        .to_string();
    let scope = payload
        .get("scope")
        .and_then(|value| value.as_str())
        .unwrap_or("project")
        .parse::<crate::models::MemoryScope>()
        .map_err(|e| format!("payload.scope: {e}"))?;

    let target_job_id = crate::resources::node::resolve_node_or_task_job_id(
        orch.db.for_project(target.project).await.as_ref(),
        target.project,
        target.number,
        target.exec_seq,
        target.node_id,
        None,
    )
    .await?;
    let target_db = orch.db.for_project(target.project).await;
    let target_project_id = run_context::project_id_by_key(&target_db, target.project).await?;
    let node_seq = crate::memories::db::next_node_memory_seq(&target_db, &target_job_id)
        .await
        .map_err(|e| e.to_string())?;
    let scope_value = match scope {
        crate::models::MemoryScope::Project => target_project_id.clone(),
        // Role memories key on the agent's role (e.g. `builder`), the name that
        // resolves to the canon prompt `{role}.md` — not the recipe node name
        // (e.g. `agent-1`), which is only a layout label. Fall back to the node
        // id when the job carries no agent config id.
        crate::models::MemoryScope::Role => {
            crate::memories::db::role_for_job(&target_db, &target_job_id)
                .await
                .map_err(|e| e.to_string())?
                .unwrap_or_else(|| target.node_id.to_string())
        }
        crate::models::MemoryScope::Workspace => "workspace".to_string(),
    };
    let owner_project_id = match scope {
        crate::models::MemoryScope::Project => target_project_id,
        crate::models::MemoryScope::Role => {
            crate::memories::triage::role_home_project_id(orch, &scope_value)?
        }
        crate::models::MemoryScope::Workspace => "workspace".to_string(),
    };
    let provenance_uri = appending_turn_uri(orch, request).await?;
    let memory = crate::memories::commands::create(
        &target_db,
        crate::models::CreateMemory {
            name,
            content,
            project_id: Some(owner_project_id),
            scope,
            scope_value,
            job_id: Some(target_job_id),
            node_seq: Some(node_seq),
            provenance_uri,
        },
    )
    .await?;
    let uri = build_node_memory_uri(
        target.project,
        target.number,
        target.exec_seq,
        target.node_id,
        node_seq as i32,
    );
    Ok(format!("Appended draft memory '{}' at {uri}", memory.id))
}

pub(super) async fn apply_node_memory_patch(
    orch: &Orchestrator,
    payload: &serde_json::Value,
    target: MemoryResourceTarget<'_>,
) -> Result<String, String> {
    let memory_id = resolve_target_memory_id(orch, &target).await?;
    let content = payload
        .get("content")
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned);
    let status = match payload.get("status").and_then(|value| value.as_str()) {
        Some(raw) => raw
            .parse::<crate::models::MemoryStatus>()
            .map(Some)
            .map_err(|e| format!("payload.status: {e}"))?,
        None => None,
    };

    let input = crate::models::UpdateMemory {
        id: memory_id.clone(),
        content,
        status,
    };

    let memory = crate::memories::commands::update(
        orch.db.for_project(target.project).await.as_ref(),
        input,
    )
    .await?;
    let memory_uri = build_node_memory_uri(
        target.project,
        target.number,
        target.exec_seq,
        target.node_id,
        target.memory_seq,
    );
    orch.enqueue_resource_embed(&memory_uri, memory.content.clone());
    Ok(format!("Updated memory at {memory_uri}"))
}

pub(super) async fn apply_node_memory_delete(
    orch: &Orchestrator,
    target: MemoryResourceTarget<'_>,
) -> Result<String, String> {
    let memory_id = resolve_target_memory_id(orch, &target).await?;
    crate::memories::db::delete_memory(
        orch.db.for_project(target.project).await.as_ref(),
        &memory_id,
    )
    .await
    .map_err(|e| e.to_string())?;
    let memory_uri = build_node_memory_uri(
        target.project,
        target.number,
        target.exec_seq,
        target.node_id,
        target.memory_seq,
    );
    orch.enqueue_resource_delete(&memory_uri);
    Ok(format!("Deleted memory at {memory_uri}"))
}

pub(super) async fn apply_memory_triage_action(
    orch: &Orchestrator,
    payload: &serde_json::Value,
    target: MemoryResourceTarget<'_>,
) -> Result<(String, Option<PromotedMemoryRef>), String> {
    let memory_id = resolve_target_memory_id(orch, &target).await?;
    let action_raw = super::payload_trimmed_non_empty_str(payload, "action", &[])
        .ok_or("payload.action is required")?;
    let decision = parse_triage_action(action_raw)?;
    let reason = super::payload_trimmed_non_empty_str(payload, "reason", &[])
        .ok_or("payload.reason is required and must be a non-empty string")?;

    let target_db = orch.db.for_project(target.project).await;
    let memory = crate::memories::db::load_memory(&target_db, &memory_id)
        .await
        .map_err(|e| e.to_string())?;
    if memory.status != MemoryStatus::Claimed {
        return Err(format!(
            "Memory '{}' must be claimed before triage action '{}' (current status: {})",
            memory_id, action_raw, memory.status
        ));
    }

    let (deferred_scope, deferred_scope_value) = if decision == MemoryTriageDecision::Defer {
        parse_deferred_scope(&target_db, payload).await?
    } else {
        (None, None)
    };

    crate::memories::db::record_triage_decision(
        &target_db,
        &memory_id,
        decision.clone(),
        reason,
        deferred_scope,
        deferred_scope_value.as_deref(),
    )
    .await
    .map_err(|e| e.to_string())?;

    let memory_uri = build_node_memory_uri(
        target.project,
        target.number,
        target.exec_seq,
        target.node_id,
        target.memory_seq,
    );
    let promoted = (decision == MemoryTriageDecision::Promote).then(|| PromotedMemoryRef {
        memory_id: memory_id.clone(),
        memory_uri: memory_uri.clone(),
    });
    Ok((
        format!("Recorded {decision} decision for memory at {memory_uri}"),
        promoted,
    ))
}

#[cfg(test)]
mod tests {
    use super::parse_deferred_scope;
    use crate::models::MemoryScope;
    use crate::storage::{LocalDb, MigrationRunner, TURSO_MIGRATIONS};
    use serde_json::json;

    async fn db_with_project() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("defer-scope-test.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) \
             VALUES ('project-1', 'default', 'Project', 'prj', '/tmp/prj', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db
    }

    /// Agents name projects by key, and the stored value becomes the memory's
    /// `project_id` — an enforced foreign key. The key is resolved to the id here,
    /// at the crossing, instead of failing when the batch finalizes (CAIRN-3289).
    #[tokio::test]
    async fn project_defer_stores_the_project_id_for_a_key() {
        let db = db_with_project().await;

        let (scope, value) = parse_deferred_scope(
            &db,
            &json!({"newScope": {"scope": "project", "value": "prj"}}),
        )
        .await
        .unwrap();

        assert_eq!(scope, Some(MemoryScope::Project));
        assert_eq!(value.as_deref(), Some("project-1"));
    }

    #[tokio::test]
    async fn project_defer_rejects_a_value_that_names_no_project() {
        let db = db_with_project().await;

        let error = parse_deferred_scope(
            &db,
            &json!({"newScope": {"scope": "project", "value": "NOPE"}}),
        )
        .await
        .unwrap_err();

        assert!(error.contains("does not name a project"), "{error}");
    }

    #[tokio::test]
    async fn role_and_workspace_defers_keep_their_own_scope_values() {
        let db = db_with_project().await;

        let (scope, value) = parse_deferred_scope(
            &db,
            &json!({"newScope": {"scope": "role", "value": "builder"}}),
        )
        .await
        .unwrap();
        assert_eq!(scope, Some(MemoryScope::Role));
        assert_eq!(value.as_deref(), Some("builder"));

        let (scope, value) =
            parse_deferred_scope(&db, &json!({"newScope": {"scope": "workspace"}}))
                .await
                .unwrap();
        assert_eq!(scope, Some(MemoryScope::Workspace));
        assert_eq!(value.as_deref(), Some("workspace"));
    }

    #[tokio::test]
    async fn a_held_defer_records_no_scope() {
        let db = db_with_project().await;

        let (scope, value) = parse_deferred_scope(&db, &json!({"reason": "not yet"}))
            .await
            .unwrap();

        assert_eq!(scope, None);
        assert_eq!(value, None);
    }
}
