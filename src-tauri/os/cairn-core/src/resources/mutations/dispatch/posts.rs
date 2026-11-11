use super::super::{build_failure, ResourceMutationResult};
use super::append_payload;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::posts::{PostTarget, PublishComment, PublishError, PublishPost};
use cairn_common::identity::{AppearanceSnapshot, PrincipalRef};
use cairn_uri::CairnResource;

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

fn title<'a>(
    index: usize,
    item: &ChangeItem,
    payload: &'a serde_json::Value,
) -> ResourceMutationResult<Option<&'a str>> {
    match payload.get("title") {
        None => Ok(None),
        Some(value) => value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(Some)
            .ok_or_else(|| build_failure(index, item, "payload.title must be a non-empty string")),
    }
}

/// Fold the fact log mint into the mutation's summary.
///
/// The post or comment row is the durable content boundary and is already
/// committed by the time its fact is minted, so a mint failure is reported,
/// never rolled back into the content. Who the fact reaches is decided later by
/// the log worker, so the summary claims no wake; it names the event row, whose
/// `?explain=<home>` says why a given home was or was not woken.
fn minted(created: String, event: Result<Option<String>, String>) -> String {
    match event {
        Ok(Some(event)) => format!("{created} (event {event})"),
        Ok(None) => created,
        Err(error) => format!("{created} (event log mint failed: {error})"),
    }
}

/// The authenticated author, their appearance, their project key, and the job
/// writing on their behalf.
async fn identity(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
) -> ResourceMutationResult<(PrincipalRef, AppearanceSnapshot, String, String)> {
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request)
        .await
        .map_err(|error| build_failure(index, item, error))?;
    let job_id = agent.job_id.clone();
    let home = cairn_uri::parse_uri(&agent.home)
        .ok_or_else(|| build_failure(index, item, "Authenticated run has an invalid home URI"))?;
    let project = home
        .project_key()
        .ok_or_else(|| build_failure(index, item, "Authenticated run has no project-scoped home"))?
        .to_string();
    let (author, appearance) = agent
        .appearance(orch.services.clock.now())
        .map_err(|error| build_failure(index, item, error))?;
    Ok((author, appearance, project, job_id))
}

/// The caller's own project's row id, in the private database posts live in.
async fn own_project_id(
    orch: &Orchestrator,
    project: &str,
    index: usize,
    item: &ChangeItem,
) -> ResourceMutationResult<String> {
    crate::mcp::handlers::run_context::project_id_by_key(&orch.db.local, project)
        .await
        .map_err(|error| build_failure(index, item, error))
}

/// The camp a `camp:` value or a camp posts collection names, which must be a
/// camp of the caller's own project.
async fn camp_target(
    orch: &Orchestrator,
    own_project: &str,
    camp_project: &str,
    slug: &str,
    index: usize,
    item: &ChangeItem,
) -> ResourceMutationResult<PostTarget> {
    if cairn_uri::canonical_project(camp_project) != cairn_uri::canonical_project(own_project) {
        return Err(build_failure(
            index,
            item,
            "a post addresses a camp of your own project only",
        ));
    }
    let project_id = own_project_id(orch, own_project, index, item).await?;
    crate::posts::camp_target(orch, &project_id, slug)
        .await
        .map_err(|error| build_failure(index, item, error.to_string()))
}

/// Where an append to `cairn://posts` files its post, from `scope` and `camp`.
async fn corpus_target(
    orch: &Orchestrator,
    payload: &serde_json::Value,
    own_project: &str,
    index: usize,
    item: &ChangeItem,
) -> ResourceMutationResult<PostTarget> {
    let scope = match payload.get("scope") {
        None => None,
        Some(value) => Some(
            value
                .as_str()
                .ok_or_else(|| build_failure(index, item, "payload.scope must be a string"))?,
        ),
    };
    let camp = payload.get("camp");
    match (scope, camp) {
        (Some("camp"), Some(camp)) => {
            let uri = camp.as_str().map(str::trim).unwrap_or_default();
            let Some(CairnResource::ProjectCamp { project, slug }) = cairn_uri::parse_uri(uri)
            else {
                return Err(build_failure(
                    index,
                    item,
                    format!(
                        "payload.camp must be a canonical camp URI like {}",
                        cairn_uri::build_project_camp_uri(own_project, "SLUG")
                    ),
                ));
            };
            camp_target(orch, own_project, &project, &slug, index, item).await
        }
        (Some("camp"), None) => Err(build_failure(
            index,
            item,
            "scope:\"camp\" needs payload.camp, the canonical URI of a camp of your project",
        )),
        (_, Some(_)) => Err(build_failure(
            index,
            item,
            "payload.camp is only accepted with scope:\"camp\"",
        )),
        (None, None) => Ok(PostTarget::Workspace),
        (Some(scope), None) => {
            if scope != "project" && !scope.eq_ignore_ascii_case(own_project) {
                return Err(build_failure(index, item, "payload.scope may only be \"project\", the authenticated caller's own project key, or \"camp\""));
            }
            Ok(PostTarget::Project {
                project_id: own_project_id(orch, own_project, index, item).await?,
            })
        }
    }
}

fn refused(
    index: usize,
    item: &ChangeItem,
    error: PublishError,
) -> super::super::ResourceMutationFailure {
    build_failure(index, item, error.to_string())
}

/// Posts and their comments carry no caller-supplied provenance: the author
/// principal and appearance below are captured from the live authenticated run
/// by [`identity`], never read from the payload.
///
/// A payload that tries to name one is refused before this arm runs. The
/// contract gate in the parent module enumerates exactly what each post
/// mutation accepts and rejects every other top-level key, so an `author` or
/// `appearance` key is a gate rejection rather than a field this arm has to
/// re-check. Every arm here is a thin caller of [`crate::posts`], the one
/// publish path.
pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
        (CairnResource::Posts, ChangeMode::Append)
        | (CairnResource::ProjectCampPosts { .. }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let content = content(index, item, payload)?;
            let title = title(index, item, payload)?;
            if dry_run {
                format!("Would create post: {}", title.unwrap_or("Untitled"))
            } else {
                let (author, appearance, own_project, job_id) =
                    identity(orch, request, index, item).await?;
                let target = match resource {
                    CairnResource::ProjectCampPosts { project, slug } => {
                        camp_target(orch, &own_project, project, slug, index, item).await?
                    }
                    _ => corpus_target(orch, payload, &own_project, index, item).await?,
                };
                let published = crate::posts::publish_post(
                    orch,
                    PublishPost {
                        target,
                        title: title.map(str::to_string),
                        content: content.to_string(),
                        author,
                        appearance,
                        actor_job_id: Some(job_id),
                    },
                )
                .await
                .map_err(|error| refused(index, item, error))?;
                minted(
                    format!("Created post cairn://posts/{}", published.record.id),
                    published.event,
                )
            }
        }
        (CairnResource::Post { id }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let content = content(index, item, payload)?;
            if dry_run {
                format!("Would comment on cairn://posts/{id}")
            } else {
                let (author, appearance, _, job_id) = identity(orch, request, index, item).await?;
                let published = crate::posts::publish_comment(
                    orch,
                    PublishComment {
                        post_id: *id,
                        content: content.to_string(),
                        author,
                        appearance,
                        actor_job_id: Some(job_id),
                    },
                )
                .await
                .map_err(|error| refused(index, item, error))?;
                minted(
                    format!(
                        "Created comment {} on cairn://posts/{id}",
                        published.record.id
                    ),
                    published.event,
                )
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
