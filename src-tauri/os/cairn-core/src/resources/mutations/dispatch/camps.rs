//! Camp mutations: a thread pitches a camp, retitles one it leads, and joins,
//! changes its own level in, or leaves a camp.
//!
//! Every act here is a thread's, taken by its authenticated run. A resource
//! write carries no verified operator credential, so the operator's camp
//! authority (reassigning a lead, assigning a confirm role) is not reachable
//! through this surface.

use super::super::{build_failure, ResourceMutationResult};
use super::append_payload;
use crate::camps::ActingThread;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::models::{Camp, CampLevel, CampSubject};
use crate::orchestrator::Orchestrator;
use cairn_db::storage::camps::{
    camp_by_slug_conn, leave_camp_conn, pitch_camp_conn, retitle_camp_conn, set_camp_level_conn,
    CampAssignedBy, CampLevelChange, NewCamp,
};
use cairn_uri::{build_project_camp_participants_uri, build_project_camp_uri, CairnResource};

async fn acting_thread(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
    index: usize,
    item: &ChangeItem,
) -> ResourceMutationResult<ActingThread> {
    crate::camps::acting_thread(orch, request, project)
        .await
        .map_err(|error| build_failure(index, item, error))
}

async fn camp(
    acting: &ActingThread,
    project: &str,
    slug: &str,
    index: usize,
    item: &ChangeItem,
) -> ResourceMutationResult<Camp> {
    let (project_id, slug_owned) = (acting.project_id.clone(), slug.to_string());
    acting
        .db
        .read(move |conn| {
            let (project_id, slug) = (project_id.clone(), slug_owned.clone());
            Box::pin(async move { camp_by_slug_conn(conn, &project_id, &slug).await })
        })
        .await
        .map_err(|error| build_failure(index, item, error.to_string()))?
        .ok_or_else(|| {
            build_failure(
                index,
                item,
                format!("no camp `{slug}` in project {project}"),
            )
        })
}

fn text<'a>(
    payload: &'a serde_json::Value,
    key: &str,
    index: usize,
    item: &ChangeItem,
) -> ResourceMutationResult<&'a str> {
    payload
        .get(key)
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            build_failure(
                index,
                item,
                format!("payload.{key} is required and must be a non-empty string"),
            )
        })
}

/// `{lead: <thread URI>}`: the lead offers the camp's lead to that thread, and
/// that thread, writing the same patch naming itself, accepts it.
#[allow(clippy::too_many_arguments)]
async fn handoff(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    project: &str,
    slug: &str,
    lead: &str,
    now: i64,
) -> ResourceMutationResult<String> {
    let uri = build_project_camp_uri(project, slug);
    let name = match cairn_uri::parse_uri(lead) {
        Some(CairnResource::Thread {
            project: thread_project,
            name,
            path,
        }) if path.is_empty() && thread_project == cairn_uri::canonical_project(project) => name,
        _ => {
            return Err(build_failure(
                index,
                item,
                format!(
                    "payload.lead must be the canonical URI of a thread of this project, like {}",
                    cairn_uri::build_thread_uri(project, "NAME")
                ),
            ))
        }
    };
    if dry_run {
        return Ok(format!("Would hand camp {uri}'s lead to {lead}"));
    }
    let acting = acting_thread(orch, request, project, index, item).await?;
    let camp = camp(&acting, project, slug, index, item).await?;
    let target = acting
        .db
        .query_opt_text(
            "SELECT id FROM threads WHERE project_id = ?1 AND name = ?2",
            (acting.project_id.clone(), name.clone()),
        )
        .await
        .map_err(|error| build_failure(index, item, error.to_string()))?
        .ok_or_else(|| build_failure(index, item, format!("no thread `{name}` in this project")))?;
    let actor = acting.actor.clone();
    let offering = acting.thread_id == camp.lead_thread_id;
    if !offering && acting.thread_id != target {
        return Err(build_failure(
            index,
            item,
            format!("only camp `{slug}`'s lead offers its lead, and only the thread it was offered to accepts it"),
        ));
    }
    let (camp_row, target_id) = (camp.clone(), target.clone());
    acting
        .db
        .write(move |conn| {
            let (camp, target, actor) = (camp_row.clone(), target_id.clone(), actor.clone());
            Box::pin(async move {
                if offering {
                    cairn_db::storage::camps::offer_lead_conn(
                        conn,
                        &camp,
                        &target,
                        Some(&actor),
                        now,
                    )
                    .await
                } else {
                    cairn_db::storage::camps::accept_lead_conn(
                        conn,
                        &camp,
                        &target,
                        Some(&actor),
                        now,
                    )
                    .await
                }
            })
        })
        .await
        .map_err(|error| build_failure(index, item, error.to_string()))?;
    emit_change(orch, "update");
    Ok(if offering {
        format!("Offered camp {uri}'s lead to {lead}; it becomes lead when that thread accepts by patching {uri} {{lead: {lead}}}.")
    } else {
        format!("Took the lead of camp {uri}.")
    })
}

fn emit_change(orch: &Orchestrator, action: &str) {
    for table in ["camps", "wake_subscriptions"] {
        if let Err(error) = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({ "table": table, "action": action }),
        ) {
            tracing::warn!(%error, table, "failed to emit camp db-change notification");
        }
    }
}

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let now = orch.services.clock.now();
    let summary = match (resource, item.mode) {
        (CairnResource::ProjectCamps { project }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let slug = text(payload, "slug", index, item)?.to_string();
            let title = text(payload, "title", index, item)?.to_string();
            crate::models::validate_camp_slug(&slug)
                .map_err(|error| build_failure(index, item, error))?;
            let uri = build_project_camp_uri(project, &slug);
            let participants_uri = build_project_camp_participants_uri(project, &slug);
            if dry_run {
                return Ok(Some(format!("Would pitch camp {uri}: {title}")));
            }
            let acting = acting_thread(orch, request, project, index, item).await?;
            let input = NewCamp {
                project_id: acting.project_id.clone(),
                slug,
                title,
                lead_thread_id: acting.thread_id.clone(),
            };
            let actor = acting.actor.clone();
            acting
                .db
                .write(move |conn| {
                    let (input, actor) = (input.clone(), actor.clone());
                    Box::pin(async move { pitch_camp_conn(conn, &input, Some(&actor), now).await })
                })
                .await
                .map_err(|error| build_failure(index, item, error.to_string()))?;
            emit_change(orch, "insert");
            format!("Pitched camp {uri}, led by your thread. Other threads join it at {participants_uri}.")
        }
        (CairnResource::ProjectCamp { project, slug }, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            if let Some(key) = payload.as_object().and_then(|map| {
                map.keys()
                    .find(|key| !matches!(key.as_str(), "title" | "lead"))
            }) {
                return Err(build_failure(
                    index,
                    item,
                    format!("a camp patch takes title or lead; `{key}` is not something a thread changes here"),
                ));
            }
            if payload.get("lead").is_some() {
                if payload.get("title").is_some() {
                    return Err(build_failure(
                        index,
                        item,
                        "handing over the lead and retitling are separate acts; send each in its own patch",
                    ));
                }
                let lead = text(payload, "lead", index, item)?.to_string();
                return handoff(
                    orch, request, index, item, dry_run, project, slug, &lead, now,
                )
                .await
                .map(Some);
            }
            let title = text(payload, "title", index, item)?.to_string();
            let uri = build_project_camp_uri(project, slug);
            if dry_run {
                return Ok(Some(format!("Would retitle camp {uri} to \"{title}\"")));
            }
            let acting = acting_thread(orch, request, project, index, item).await?;
            let camp = camp(&acting, project, slug, index, item).await?;
            if camp.lead_thread_id != acting.thread_id {
                return Err(build_failure(
                    index,
                    item,
                    format!("only camp `{slug}`'s lead retitles it"),
                ));
            }
            let actor = acting.actor.clone();
            let changed = acting
                .db
                .write(move |conn| {
                    let (camp, title, actor) = (camp.clone(), title.clone(), actor.clone());
                    Box::pin(async move {
                        retitle_camp_conn(conn, &camp, &title, Some(&actor), now).await
                    })
                })
                .await
                .map_err(|error| build_failure(index, item, error.to_string()))?;
            if changed {
                emit_change(orch, "update");
                format!("Retitled camp {uri}.")
            } else {
                format!("Camp {uri} already has that title; nothing changed.")
            }
        }
        (CairnResource::ProjectCampParticipants { project, slug }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let level_text = text(payload, "level", index, item)?;
            let level = CampLevel::parse(level_text).ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    format!("payload.level must be watch or wake, not {level_text}; a confirm role is assigned by the camp's lead or the operator"),
                )
            })?;
            let reason = text(payload, "reason", index, item)?.to_string();
            let uri = build_project_camp_uri(project, slug);
            if dry_run {
                return Ok(Some(format!(
                    "Would take part in camp {uri} at {}",
                    level.as_str()
                )));
            }
            let acting = acting_thread(orch, request, project, index, item).await?;
            let camp = camp(&acting, project, slug, index, item).await?;
            let subject = CampSubject::Thread {
                thread_id: acting.thread_id.clone(),
            };
            let actor = acting.actor.clone();
            let change = acting
                .db
                .write(move |conn| {
                    let (camp, subject, reason, actor) =
                        (camp.clone(), subject.clone(), reason.clone(), actor.clone());
                    Box::pin(async move {
                        set_camp_level_conn(
                            conn,
                            &camp,
                            &subject,
                            level,
                            &reason,
                            CampAssignedBy::Agent,
                            Some(&actor),
                            now,
                        )
                        .await
                    })
                })
                .await
                .map_err(|error| build_failure(index, item, error.to_string()))?;
            match change {
                CampLevelChange::Joined(level) => {
                    emit_change(orch, "insert");
                    format!("Joined camp {uri} at {}.", level.as_str())
                }
                CampLevelChange::Changed { from, to } => {
                    emit_change(orch, "update");
                    format!(
                        "Moved from {} to {} in camp {uri}.",
                        from.as_str(),
                        to.as_str()
                    )
                }
                CampLevelChange::Unchanged(level) => format!(
                    "Already in camp {uri} at {} with that reason; nothing changed.",
                    level.as_str()
                ),
            }
        }
        (CairnResource::ProjectCampParticipants { project, slug }, ChangeMode::Delete) => {
            let reason = match item
                .payload
                .as_ref()
                .and_then(|payload| payload.get("reason"))
            {
                None | Some(serde_json::Value::Null) => None,
                Some(value) => Some(
                    value
                        .as_str()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .ok_or_else(|| {
                            build_failure(index, item, "payload.reason must be a non-empty string")
                        })?
                        .to_string(),
                ),
            };
            let uri = build_project_camp_uri(project, slug);
            if dry_run {
                return Ok(Some(format!("Would leave camp {uri}")));
            }
            let acting = acting_thread(orch, request, project, index, item).await?;
            let camp = camp(&acting, project, slug, index, item).await?;
            let subject = CampSubject::Thread {
                thread_id: acting.thread_id.clone(),
            };
            let actor = acting.actor.clone();
            let left = acting
                .db
                .write(move |conn| {
                    let (camp, subject, reason, actor) =
                        (camp.clone(), subject.clone(), reason.clone(), actor.clone());
                    Box::pin(async move {
                        leave_camp_conn(conn, &camp, &subject, reason.as_deref(), Some(&actor), now)
                            .await
                    })
                })
                .await
                .map_err(|error| build_failure(index, item, error.to_string()))?;
            if left {
                emit_change(orch, "delete");
                format!("Left camp {uri}.")
            } else {
                format!("Your thread is not a participant of camp {uri}; nothing changed.")
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
