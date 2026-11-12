//! Camp mutations, a thread's side: create a camp with its first members,
//! rename or archive one it is a member of, add a member, or leave.
//!
//! Every act here is a thread's, taken by its authenticated run. A resource
//! write carries no verified operator credential, so the operator acts through
//! the desktop's camp commands instead (`sync_commands/camps.rs`).

use super::super::{build_failure, ResourceMutationResult};
use super::append_payload;
use crate::camps::ActingThread;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::models::{Camp, CampStatus, CampSubject};
use crate::orchestrator::Orchestrator;
use cairn_db::storage::camps::{
    add_camp_member_conn, camp_by_slug_conn, create_camp_conn, is_camp_member_conn,
    leave_camp_conn, MemberAdded, NewCamp,
};
use cairn_uri::{build_project_camp_members_uri, build_project_camp_uri, CairnResource};

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

/// Refuse an act on a camp the acting thread is not a member of.
async fn require_member(
    acting: &ActingThread,
    camp: &Camp,
    project: &str,
    index: usize,
    item: &ChangeItem,
) -> ResourceMutationResult<()> {
    let (camp_row, subject) = (camp.clone(), acting_subject(acting));
    let member = acting
        .db
        .read(move |conn| {
            let (camp, subject) = (camp_row.clone(), subject.clone());
            Box::pin(async move { is_camp_member_conn(conn, &camp, &subject).await })
        })
        .await
        .map_err(|error| build_failure(index, item, error.to_string()))?;
    if member {
        return Ok(());
    }
    Err(build_failure(
        index,
        item,
        format!(
            "your thread is not a member of camp `{}`; a member adds you, or add yourself by appending {{member: \"<your thread URI>\"}} to {}",
            camp.slug,
            build_project_camp_members_uri(project, &camp.slug)
        ),
    ))
}

fn acting_subject(acting: &ActingThread) -> CampSubject {
    CampSubject::Thread {
        thread_id: acting.thread_id.clone(),
    }
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
            let named: Vec<String> = match payload.get("members") {
                None | Some(serde_json::Value::Null) => Vec::new(),
                Some(serde_json::Value::Array(values)) => values
                    .iter()
                    .map(|value| {
                        value.as_str().map(str::to_string).ok_or_else(|| {
                            build_failure(
                                index,
                                item,
                                "payload.members must be an array of thread URIs or `operator`",
                            )
                        })
                    })
                    .collect::<Result<_, _>>()?,
                Some(_) => {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.members must be an array of thread URIs or `operator`",
                    ))
                }
            };
            let uri = build_project_camp_uri(project, &slug);
            if dry_run {
                return Ok(Some(format!("Would create camp {uri}: {title}")));
            }
            let acting = acting_thread(orch, request, project, index, item).await?;
            let mut members = Vec::with_capacity(named.len());
            for value in &named {
                let (subject, _) =
                    crate::camps::parse_member(&acting.db, &acting.project_id, project, value)
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                members.push(subject);
            }
            let input = NewCamp {
                project_id: acting.project_id.clone(),
                slug,
                title,
            };
            let creator = acting_subject(&acting);
            let created = acting
                .db
                .write(move |conn| {
                    let (input, creator, members) =
                        (input.clone(), creator.clone(), members.clone());
                    Box::pin(async move {
                        let camp = create_camp_conn(conn, &input, &creator, &members, now).await?;
                        let count = cairn_db::storage::camps::camp_members_conn(conn, &camp)
                            .await?
                            .len();
                        Ok((camp, count))
                    })
                })
                .await
                .map_err(|error| build_failure(index, item, error.to_string()))?;
            emit_change(orch, "insert");
            format!("Created camp {uri} with {} members.", created.1)
        }
        (CairnResource::ProjectCamp { project, slug }, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            if let Some(key) = payload.as_object().and_then(|map| {
                map.keys()
                    .find(|key| !matches!(key.as_str(), "title" | "status"))
            }) {
                return Err(build_failure(
                    index,
                    item,
                    format!("a camp patch takes title or status; `{key}` is not something a member changes here"),
                ));
            }
            let title = match payload.get("title") {
                None => None,
                Some(_) => Some(text(payload, "title", index, item)?.to_string()),
            };
            let status = match payload.get("status") {
                None => None,
                Some(_) => {
                    let value = text(payload, "status", index, item)?;
                    Some(CampStatus::parse(value).ok_or_else(|| {
                        build_failure(
                            index,
                            item,
                            format!("payload.status must be active or archived, not {value}"),
                        )
                    })?)
                }
            };
            if title.is_none() && status.is_none() {
                return Err(build_failure(
                    index,
                    item,
                    "a camp patch needs title or status",
                ));
            }
            let uri = build_project_camp_uri(project, slug);
            if dry_run {
                return Ok(Some(format!("Would update camp {uri}")));
            }
            let acting = acting_thread(orch, request, project, index, item).await?;
            let camp = camp(&acting, project, slug, index, item).await?;
            if camp.is_base() {
                return Err(build_failure(
                    index,
                    item,
                    "base camp cannot be renamed or archived",
                ));
            }
            require_member(&acting, &camp, project, index, item).await?;
            let update = crate::camps::update_camp(&acting.db, &camp, title, status, now)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            if update.retitled || update.status.is_some() {
                emit_change(orch, "update");
            }
            if update.status == Some(CampStatus::Archived) {
                let by = crate::camps::thread_label(&acting.db, &acting.thread_id).await;
                if let Err(error) =
                    crate::camps::announce_archived(orch, &camp, project, &by, now).await
                {
                    tracing::warn!(%error, camp = %uri, "failed to mint the archived notice");
                }
                format!("Archived camp {uri}.")
            } else if update.status.is_some() {
                format!("Unarchived camp {uri}.")
            } else if update.retitled {
                format!("Renamed camp {uri}.")
            } else {
                format!("Camp {uri} already reads that way; nothing changed.")
            }
        }
        (CairnResource::ProjectCampMembers { project, slug }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let value = text(payload, "member", index, item)?.to_string();
            let uri = build_project_camp_uri(project, slug);
            if dry_run {
                return Ok(Some(format!("Would add {value} to camp {uri}")));
            }
            let acting = acting_thread(orch, request, project, index, item).await?;
            let camp = camp(&acting, project, slug, index, item).await?;
            let (subject, label) =
                crate::camps::parse_member(&acting.db, &acting.project_id, project, &value)
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
            let actor = acting_subject(&acting);
            if subject != actor {
                require_member(&acting, &camp, project, index, item).await?;
            }
            let (camp_row, subject_row, actor_row) = (camp.clone(), subject.clone(), actor.clone());
            let added = acting
                .db
                .write(move |conn| {
                    let (camp, subject, actor) =
                        (camp_row.clone(), subject_row.clone(), actor_row.clone());
                    Box::pin(async move {
                        add_camp_member_conn(conn, &camp, &subject, &actor, now).await
                    })
                })
                .await
                .map_err(|error| build_failure(index, item, error.to_string()))?;
            match added {
                MemberAdded::Added => {
                    emit_change(orch, "insert");
                    let by = crate::camps::thread_label(&acting.db, &acting.thread_id).await;
                    if let Err(error) = crate::camps::announce_member_added(
                        orch,
                        &camp,
                        project,
                        &subject.key(),
                        &label,
                        &by,
                        Some(&acting.job_id),
                        now,
                    )
                    .await
                    {
                        tracing::warn!(%error, camp = %uri, "failed to mint the member-added notice");
                    }
                    format!("Added {value} to camp {uri}.")
                }
                MemberAdded::AlreadyMember => {
                    format!("{value} is already a member of camp {uri}; nothing changed.")
                }
            }
        }
        (CairnResource::ProjectCampMembers { project, slug }, ChangeMode::Delete) => {
            let uri = build_project_camp_uri(project, slug);
            if dry_run {
                return Ok(Some(format!("Would leave camp {uri}")));
            }
            let acting = acting_thread(orch, request, project, index, item).await?;
            let camp = camp(&acting, project, slug, index, item).await?;
            let subject = acting_subject(&acting);
            let left = acting
                .db
                .write(move |conn| {
                    let (camp, subject) = (camp.clone(), subject.clone());
                    Box::pin(async move { leave_camp_conn(conn, &camp, &subject).await })
                })
                .await
                .map_err(|error| build_failure(index, item, error.to_string()))?;
            if left {
                emit_change(orch, "delete");
                format!("Left camp {uri}.")
            } else {
                format!("Your thread is not a member of camp {uri}; nothing changed.")
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
