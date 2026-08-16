use super::super::{build_failure, ResourceMutationResult};
use super::append_payload;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::models::{CreatePost, CreatePostComment};
use crate::orchestrator::Orchestrator;
use cairn_common::identity::{
    Address, AppearanceEvidence, AppearanceSnapshot, AppearanceTransport, PrincipalRef,
    VerificationMethod, VerificationRecord, VerificationStatus, VerificationStrength,
};
use cairn_common::uri::CairnResource;

fn content<'a>(
    index: usize,
    item: &ChangeItem,
    payload: &'a serde_json::Value,
) -> ResourceMutationResult<&'a str> {
    payload
        .get("content")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            build_failure(
                index,
                item,
                "payload.content is required and must be a non-empty string",
            )
        })
}

/// Fold a routing outcome into the mutation's summary.
///
/// The post or comment row is the durable content boundary and is already
/// committed by the time attention is routed, so a routing failure is reported,
/// never rolled back into the content. The converse holds too: the summary
/// counts only pushes that were durably recorded, so it can never claim a wake
/// that did not land.
fn attention(
    created: String,
    routed: Result<crate::orchestrator::wakes::PostAttention, String>,
) -> String {
    match routed {
        Ok(attention) => {
            let mut summary = created;
            if !attention.recorded.is_empty() {
                summary.push_str(&format!(
                    " (attention: {} node(s))",
                    attention.recorded.len()
                ));
            }
            if attention.failed > 0 {
                summary.push_str(&format!(
                    " ({} attention notice(s) could not be recorded)",
                    attention.failed
                ));
            }
            summary
        }
        Err(error) => {
            tracing::warn!(%error, "post attention routing failed");
            format!("{created} (attention routing failed: {error})")
        }
    }
}

fn emit_db_change(orch: &Orchestrator, table: &str, action: &str) {
    if let Err(error) = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": table, "action": action}),
    ) {
        tracing::warn!(%error, table, "failed to emit Posts db-change notification");
    }
}

async fn identity(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
) -> ResourceMutationResult<(PrincipalRef, AppearanceSnapshot, String)> {
    let (run, _) = crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request)
        .await
        .map_err(|error| build_failure(index, item, error))?;
    let live_run_id = request.run_id.as_deref().ok_or_else(|| {
        build_failure(
            index,
            item,
            "Authenticated agent request is missing its run ID",
        )
    })?;
    if run.run_id != live_run_id {
        return Err(build_failure(
            index,
            item,
            "Authenticated agent run does not match the resolved live run",
        ));
    }
    let node = crate::mcp::handlers::run_context::lookup_home_uri_routed(&orch.db, request)
        .await
        .map_err(|error| build_failure(index, item, error))?;
    let home = cairn_common::uri::parse_uri(&node)
        .ok_or_else(|| build_failure(index, item, "Authenticated run has an invalid home URI"))?;
    let project = home
        .project_key()
        .ok_or_else(|| build_failure(index, item, "Authenticated run has no project-scoped home"))?
        .to_string();
    let author = PrincipalRef::Agent {
        node: node.clone(),
        run_id: Some(live_run_id.to_string()),
    };
    let now = orch.services.clock.now();
    let verification = VerificationRecord::new(
        VerificationMethod::NodeSession,
        VerificationStatus::Verified,
        None,
        None,
        Some(live_run_id.to_string()),
        None,
        VerificationStrength::new("session-bound")
            .map_err(|error| build_failure(index, item, error.to_string()))?,
        now,
    )
    .map_err(|error| build_failure(index, item, error.to_string()))?;
    let evidence = AppearanceEvidence::new(
        AppearanceTransport::ResourcePatch,
        Address::Resource { node },
        verification,
        now,
        None,
    )
    .map_err(|error| build_failure(index, item, error.to_string()))?;
    let appearance = AppearanceSnapshot::new(author.clone(), evidence, vec![], None)
        .map_err(|error| build_failure(index, item, error.to_string()))?;
    Ok((author, appearance, project))
}

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
        (CairnResource::Posts, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let object = payload
                .as_object()
                .ok_or_else(|| build_failure(index, item, "payload must be an object"))?;
            if let Some(key) = object
                .keys()
                .find(|key| !matches!(key.as_str(), "content" | "title" | "scope"))
            {
                return Err(build_failure(
                    index,
                    item,
                    format!("unsupported post payload key: {key}; provenance is server-captured"),
                ));
            }
            let content = content(index, item, payload)?;
            let title = match payload.get("title") {
                None => None,
                Some(value) => Some(
                    value
                        .as_str()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .ok_or_else(|| {
                            build_failure(index, item, "payload.title must be a non-empty string")
                        })?,
                ),
            };
            if dry_run {
                format!("Would create post: {}", title.unwrap_or("Untitled"))
            } else {
                let (author, appearance, own_project) =
                    identity(orch, request, index, item).await?;
                let project_id = match payload.get("scope") {
                    None => None,
                    Some(value) => {
                        let scope = value.as_str().ok_or_else(|| {
                            build_failure(index, item, "payload.scope must be a string")
                        })?;
                        if scope != "project" && !scope.eq_ignore_ascii_case(&own_project) {
                            return Err(build_failure(index, item, "payload.scope may only be \"project\" or the authenticated caller's own project key"));
                        }
                        Some(
                            crate::mcp::handlers::run_context::project_id_by_key(
                                &orch.db.local,
                                &own_project,
                            )
                            .await
                            .map_err(|error| build_failure(index, item, error))?,
                        )
                    }
                };
                let post = orch
                    .db
                    .local
                    .create_post(CreatePost {
                        project_id,
                        title: title.map(str::to_string),
                        content: content.to_string(),
                        author,
                        appearance,
                    })
                    .await
                    .map_err(|error| build_failure(index, item, error.to_string()))?;
                emit_db_change(orch, "posts", "insert");
                let created = format!("Created post cairn://posts/{}", post.id);
                attention(
                    created,
                    crate::orchestrator::wakes::route_new_post(orch, &post).await,
                )
            }
        }
        (CairnResource::Post { id }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let object = payload
                .as_object()
                .ok_or_else(|| build_failure(index, item, "payload must be an object"))?;
            if object.keys().any(|key| key != "content") {
                return Err(build_failure(
                    index,
                    item,
                    "post comments accept content only; provenance is server-captured",
                ));
            }
            let content = content(index, item, payload)?;
            if dry_run {
                format!("Would comment on cairn://posts/{id}")
            } else {
                let (author, appearance, _) = identity(orch, request, index, item).await?;
                let comment = orch
                    .db
                    .local
                    .create_post_comment(CreatePostComment {
                        post_id: *id,
                        content: content.to_string(),
                        author,
                        appearance,
                    })
                    .await
                    .map_err(|error| build_failure(index, item, error.to_string()))?;
                emit_db_change(orch, "post_comments", "insert");
                let created = format!("Created comment {} on cairn://posts/{id}", comment.id);
                // The comment routes against the post it landed on, so the
                // author it replies to is read back from the durable row rather
                // than trusted from anything the caller supplied.
                match orch.db.local.get_post(*id).await {
                    Ok(Some(post)) => attention(
                        created,
                        crate::orchestrator::wakes::route_post_comment(orch, &post, &comment).await,
                    ),
                    Ok(None) => created,
                    Err(error) => {
                        tracing::warn!(%error, post = id, "post comment attention routing failed");
                        format!("{created} (attention routing failed: {error})")
                    }
                }
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
