//! Camps: a project's named conversations (cairn/6255, cut down by cairn/7002).
//!
//! Every project is born with base camp, the passive channel every open
//! thread of the project is in. A camp is a group conversation among the
//! threads and the operator that are its members: each post to it wakes every
//! other member. Storage and the member census live in
//! `cairn_db::storage::camps`; this module owns what needs the rest of core:
//! the notices a camp's own changes mint, the boot backfill, and resolving
//! the thread a run acts for. See `docs/camps.md`.

use crate::db::DbState;
use crate::error::CairnError;
use crate::models::{Camp, BASE_CAMP_SLUG};
use crate::storage::LocalDb;

pub mod view;

/// The fact source a notice addressed to a camp is minted on.
pub const CAMP_NOTICE: &str = "camp_notice";

/// The notice kind minted when a member is added to a camp.
pub const MEMBER_ADDED_NOTICE: &str = "member_added";
/// The notice kind minted when a camp is archived.
pub const ARCHIVED_NOTICE: &str = "archived";

/// A notice owed to everyone in a camp.
pub struct CampNotice<'a> {
    pub project_id: &'a str,
    pub project_key: &'a str,
    pub camp_slug: &'a str,
    /// What happened, as a short label (`rebuild`, `member_added`).
    pub kind: &'a str,
    /// Unique per notice: minting the same identity twice is one fact.
    pub identity: String,
    /// The line each member reads, in Cairn's own words.
    pub summary: String,
    pub detail_uri: Option<String>,
    /// The job whose act this notice records, which its own thread is never
    /// told about.
    pub actor_job_id: Option<&'a str>,
}

/// Mint one fact addressed to a camp, and nothing else. Who it reaches is the
/// fact log's to decide, by the camp's members at indexing: passively for
/// base camp, as a wake for a camp. The producer never names a recipient, and
/// the notice is one row however many threads exist. Returns the row id, or
/// `None` when the log already holds this identity.
///
/// **The producer contract.** A notice owed to every thread of a project is
/// ONE base-camp fact per project, minted here. No producer enumerates
/// threads or jobs to deliver it: the camp's members and their sessions are
/// resolved by the fact log, and a thread's own one-shot subscription on the
/// same event (a rebuild wake) is its own fact whose receipt rides beside
/// this one. See `docs/camps.md`.
pub async fn notify_camp(
    orch: &crate::orchestrator::Orchestrator,
    notice: CampNotice<'_>,
) -> Result<Option<i64>, String> {
    use serde_json::Value;
    let camp_uri = cairn_uri::build_project_camp_uri(notice.project_key, notice.camp_slug);
    let mut fields = std::collections::BTreeMap::from([
        (
            "project".to_string(),
            Value::String(notice.project_key.to_string()),
        ),
        ("camp".to_string(), Value::String(camp_uri.clone())),
        ("kind".to_string(), Value::String(notice.kind.to_string())),
        ("text".to_string(), Value::String(notice.summary.clone())),
    ]);
    if let Some(detail) = &notice.detail_uri {
        fields.insert("detailUri".to_string(), Value::String(detail.clone()));
    }
    let minted = crate::routes::mint(
        orch,
        crate::routes::Mint {
            fact: crate::routes::RouteFact {
                source: CAMP_NOTICE.to_string(),
                identity: notice.identity,
                fields,
                origin: None,
                summary: Some(notice.summary),
                route_provenance: None,
            },
            record_ref: notice.detail_uri.unwrap_or(camp_uri),
            record_id: None,
            project_id: Some(notice.project_id),
            presence: crate::routes::log::operator_presence().await,
            actor_job_id: notice.actor_job_id,
        },
    )
    .await?;
    // A notice is a line of the camp's conversation, so an open camp page
    // refreshes once the fact is committed.
    if minted.is_some() {
        if let Err(error) = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({ "table": "camps", "action": "notice" }),
        ) {
            tracing::warn!(%error, "failed to announce a camp notice");
        }
    }
    Ok(minted)
}

/// Tell a camp that `member` joined it, added by `by`. The new member hears it
/// too, which is how a thread learns it was added; the adder does not.
#[allow(clippy::too_many_arguments)]
pub async fn announce_member_added(
    orch: &crate::orchestrator::Orchestrator,
    camp: &Camp,
    project_key: &str,
    subject_key: &str,
    member: &str,
    by: &str,
    actor_job_id: Option<&str>,
    now: i64,
) -> Result<Option<i64>, String> {
    let summary = if member == by {
        format!("{member} joined the camp.")
    } else {
        format!("{by} added {member} to the camp.")
    };
    notify_camp(
        orch,
        CampNotice {
            project_id: &camp.project_id,
            project_key,
            camp_slug: &camp.slug,
            kind: MEMBER_ADDED_NOTICE,
            identity: format!("camp:{}:added:{subject_key}:{now}", camp.id),
            summary,
            detail_uri: None,
            actor_job_id,
        },
    )
    .await
}

/// Record in a camp's conversation that `by` archived it. An archived camp
/// delivers nothing, so the notice is a line on the camp's page and wakes no
/// one.
pub async fn announce_archived(
    orch: &crate::orchestrator::Orchestrator,
    camp: &Camp,
    project_key: &str,
    by: &str,
    now: i64,
) -> Result<Option<i64>, String> {
    notify_camp(
        orch,
        CampNotice {
            project_id: &camp.project_id,
            project_key,
            camp_slug: &camp.slug,
            kind: ARCHIVED_NOTICE,
            identity: format!("camp:{}:archived:{now}", camp.id),
            summary: format!("{by} archived the camp."),
            detail_uri: None,
            actor_job_id: None,
        },
    )
    .await
}

/// The member a written value names, with the name it reads by: `operator`,
/// or the canonical URI of a thread of `project_key`'s project.
pub async fn parse_member(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
    value: &str,
) -> Result<(crate::models::CampSubject, String), String> {
    use crate::models::{CampSubject, OPERATOR_MEMBER_KEY};
    let value = value.trim();
    if value == OPERATOR_MEMBER_KEY {
        return Ok((CampSubject::Operator, "the operator".to_string()));
    }
    let name = match cairn_uri::parse_uri(value) {
        Some(cairn_uri::CairnResource::Thread {
            project: thread_project,
            name,
            path,
        }) if path.is_empty() && thread_project == cairn_uri::canonical_project(project_key) => {
            name
        }
        _ => {
            return Err(format!(
                "a member is `operator` or the canonical URI of a thread of this project, like {}; got {value:?}",
                cairn_uri::build_thread_uri(project_key, "NAME")
            ))
        }
    };
    let thread_id = db
        .query_opt_text(
            "SELECT id FROM threads WHERE project_id = ?1 AND name = ?2",
            (project_id.to_string(), name.clone()),
        )
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("no thread `{name}` in this project"))?;
    Ok((CampSubject::Thread { thread_id }, name))
}

/// The name a thread reads by in a notice: its current name, or its id when
/// the row is gone.
pub async fn thread_label(db: &LocalDb, thread_id: &str) -> String {
    db.query_opt_text(
        "SELECT name FROM threads WHERE id = ?1",
        (thread_id.to_string(),),
    )
    .await
    .ok()
    .flatten()
    .unwrap_or_else(|| thread_id.to_string())
}

/// What [`update_camp`] changed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CampUpdate {
    pub retitled: bool,
    /// Set when the status moved, to the status it moved to.
    pub status: Option<crate::models::CampStatus>,
}

/// Rename a camp and archive or unarchive it, in one transaction. An archived
/// camp is read-only, so it is unarchived before it is renamed and archived
/// after. Base camp is refused by storage. Authorization is the caller's.
pub async fn update_camp(
    db: &LocalDb,
    camp: &Camp,
    title: Option<String>,
    status: Option<crate::models::CampStatus>,
    now: i64,
) -> Result<CampUpdate, String> {
    use crate::models::CampStatus;
    use cairn_db::storage::camps::{camp_by_id_conn, retitle_camp_conn, set_camp_status_conn};
    if camp.is_base() {
        return Err("base camp cannot be renamed or archived".to_string());
    }
    let camp = camp.clone();
    db.write(move |conn| {
        let (camp, title) = (camp.clone(), title.clone());
        Box::pin(async move {
            let mut update = CampUpdate::default();
            if status == Some(CampStatus::Active)
                && set_camp_status_conn(conn, &camp, CampStatus::Active, now).await?
            {
                update.status = Some(CampStatus::Active);
            }
            let current = camp_by_id_conn(conn, &camp.id).await?.unwrap_or(camp);
            if let Some(title) = &title {
                update.retitled = retitle_camp_conn(conn, &current, title, now).await?;
            }
            if status == Some(CampStatus::Archived)
                && set_camp_status_conn(conn, &current, CampStatus::Archived, now).await?
            {
                update.status = Some(CampStatus::Archived);
            }
            Ok(update)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Establish a project's base camp, returning it.
///
/// The one birth helper every project creation path and the boot backfill
/// share. Idempotent: a project that already has base camp is answered with a
/// read and nothing is written.
pub async fn ensure_project_base_camp(db: &LocalDb, project_id: &str) -> Result<Camp, CairnError> {
    if let Some(camp) = db.camp_by_slug(project_id, BASE_CAMP_SLUG).await? {
        return Ok(camp);
    }
    let (camp, _) = db
        .ensure_base_camp(project_id, chrono::Utc::now().timestamp())
        .await?;
    Ok(camp)
}

/// What the boot backfill did, for the boot log.
#[derive(Debug, Default)]
pub struct BaseCampBootstrap {
    pub projects: usize,
    pub established: Vec<String>,
    pub errors: Vec<String>,
}

/// Give every project in every open database its base camp, before anything
/// serves. A project that fails keeps its error in the report and is retried
/// at the next boot; camp surfaces name that state rather than pretending the
/// project has no camps.
pub async fn bootstrap_base_camps(dbs: &DbState) -> BaseCampBootstrap {
    let mut report = BaseCampBootstrap::default();
    for db in dbs.all_dbs().await {
        let projects = match crate::projects::crud::list_db(&db).await {
            Ok(projects) => projects,
            Err(error) => {
                report.errors.push(format!(
                    "could not list projects for base camp bootstrap: {error}"
                ));
                continue;
            }
        };
        for project in projects {
            report.projects += 1;
            match db.camp_by_slug(&project.id, BASE_CAMP_SLUG).await {
                Ok(Some(_)) => continue,
                Ok(None) => {}
                Err(error) => {
                    report.errors.push(format!("{}: {error}", project.key));
                    continue;
                }
            }
            match ensure_project_base_camp(&db, &project.id).await {
                Ok(_) => report.established.push(project.key.clone()),
                Err(error) => {
                    tracing::error!(project = %project.key, %error, "base camp bootstrap failed");
                    report.errors.push(format!("{}: {error}", project.key));
                }
            }
        }
    }
    report
}

/// The thread acting through an authenticated run, in the database that owns
/// `project`. A thread's camp acts are its own: a run that acts for no thread
/// (an issue node, an unauthenticated shell) is refused, and so is a thread of
/// another project.
pub(crate) struct ActingThread {
    pub db: std::sync::Arc<LocalDb>,
    pub project_id: String,
    pub thread_id: String,
    /// The authenticated job the thread acts through.
    pub job_id: String,
}

const NON_THREAD_REFUSAL: &str = "only a thread acts on camps: this run acts for no thread. Ask the thread that owns this work to create or join the camp.";

pub(crate) async fn acting_thread(
    orch: &crate::orchestrator::Orchestrator,
    request: &crate::mcp::types::McpCallbackRequest,
    project: &str,
) -> Result<ActingThread, String> {
    use crate::storage::RowExt;
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request)
        .await
        .map_err(|error| {
            format!("camps are a thread's act and need its authenticated run: {error}")
        })?;
    let db = orch.db.for_project(project).await;
    let lineage = db
        .query_opt(
            "SELECT j.thread_id, t.project_id
             FROM jobs j JOIN threads t ON t.id = j.thread_id
             WHERE j.id = ?1",
            (agent.job_id.clone(),),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .map_err(|error| format!("Failed to resolve the acting thread: {error}"))?;
    let Some((thread_id, thread_project)) = lineage else {
        return Err(NON_THREAD_REFUSAL.to_string());
    };
    let project_id = crate::mcp::handlers::run_context::project_id_by_key(&db, project).await?;
    if thread_project != project_id {
        return Err("a thread is a member of its own project's camps only".to_string());
    }
    Ok(ActingThread {
        db,
        project_id,
        thread_id,
        job_id: agent.job_id,
    })
}

#[cfg(test)]
mod tests;
