//! Standing instructions are attributed to the authenticated thread or operator,
//! never to an author supplied in the payload.

use super::super::{build_failure, ResourceMutationResult};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_uri::CairnResource;
use serde_json::Value;

async fn author(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: Option<&str>,
) -> Result<(String, String), String> {
    if request.is_desktop_operator() {
        return Ok((project.unwrap_or_default().to_string(), "operator".into()));
    }
    // Drain ownership already resolves delegation and issue ancestry, refuses
    // cycles, and excludes unverified, page and service callers.
    let actor = crate::fleet::management::thread_drain_actor(orch, request).await?;
    for db in orch.db.all_dbs().await {
        let found = db.query_opt(
            "SELECT p.key,t.name FROM threads t JOIN projects p ON p.id=t.project_id WHERE t.id=?1",
            (actor.thread_id.clone(),),
            |row| {
                use crate::storage::RowExt;
                Ok((row.text(0)?, row.text(1)?))
            },
        ).await.map_err(|error| error.to_string())?;
        if let Some((key, name)) = found {
            let key = cairn_uri::canonical_project(&key);
            if project.is_some_and(|project| cairn_uri::canonical_project(project) != key) {
                return Err("A thread pins notices in its own project; the target belongs to another project".into());
            }
            return Ok((key.clone(), format!("cairn://p/{key}/{name}")));
        }
    }
    Err("The authenticated thread no longer exists".into())
}

pub(super) async fn set_executor_notice(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    name: &str,
    notice: &Value,
    dry_run: bool,
) -> Result<String, String> {
    let executor_id = crate::fleet::management::resolve_executor_reference(orch, name)?;
    let name = crate::fleet::management::public_name(orch, &executor_id);
    let (project, author) = author(orch, request, None).await?;
    let payload = if notice.is_null() {
        serde_json::json!({"text":null})
    } else {
        notice.clone()
    };
    crate::resource_notices::set(
        orch,
        &project,
        &format!("cairn://executors/{name}"),
        &author,
        &payload,
        dry_run,
    )
    .await
}

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let (CairnResource::ProjectNotices { project }, ChangeMode::Patch) = (resource, item.mode)
    else {
        return Ok(None);
    };
    let result = async {
        let payload = item
            .payload
            .as_ref()
            .ok_or("A notice patch requires payload")?;
        let resource = payload
            .get("resource")
            .and_then(Value::as_str)
            .ok_or("payload.resource must name the resource or project rebase pattern")?;
        let (project, author) = author(orch, request, Some(project)).await?;
        let mut fields = payload
            .as_object()
            .cloned()
            .ok_or("A notice patch must be an object")?;
        fields.remove("resource");
        crate::resource_notices::set(
            orch,
            &project,
            resource,
            &author,
            &Value::Object(fields),
            dry_run,
        )
        .await
    }
    .await;
    result
        .map(Some)
        .map_err(|error: String| build_failure(index, item, error))
}
