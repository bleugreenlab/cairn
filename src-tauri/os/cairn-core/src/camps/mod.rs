//! Camps: durable project-level coordination (cairn/6255).
//!
//! A camp owns intent and references; issues, posts, PRs, checks and gates
//! supply its live state. Every project is born with base camp, led by its
//! general thread, with every thread of the project a participant at `watch`.
//! Storage and the participant census live in `cairn_db::storage::camps`; this
//! module owns what needs the rest of core: establishing a project's general
//! thread (which has a database file of its own) and the boot backfill.

use crate::db::DbState;
use crate::error::CairnError;
use crate::models::{Camp, CreateThread, BASE_CAMP_SLUG};
use crate::storage::LocalDb;

pub mod view;

/// The thread every project is born with, and base camp's first lead.
pub const GENERAL_THREAD_NAME: &str = "general";

/// The fact source a notice addressed to a camp is minted on.
pub const CAMP_NOTICE: &str = "camp_notice";

/// A notice owed to everyone in a camp.
pub struct CampNotice<'a> {
    pub project_id: &'a str,
    pub project_key: &'a str,
    pub camp_slug: &'a str,
    /// What happened, as a short label (`rebuild`, `drain`).
    pub kind: &'a str,
    /// Unique per notice: minting the same identity twice is one fact.
    pub identity: String,
    /// The line each participant reads, in Cairn's own words.
    pub summary: String,
    pub detail_uri: Option<String>,
}

/// Mint one fact addressed to a camp, and nothing else. Who it reaches is the
/// fact log's to decide, by the camp's roles at indexing: the producer never
/// names a recipient, and the notice is one row however many threads exist.
/// Returns the row id, or `None` when the log already holds this identity.
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
    crate::routes::mint(
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
            actor_job_id: None,
        },
    )
    .await
}

/// The project's general thread, created if the project has none. It is base
/// camp's lead whether or not it is open: whether the lead hears its camp is
/// the lead-offer path's and the operator's concern, not birth's.
async fn general_thread_id(db: &LocalDb, project_id: &str) -> Result<String, CairnError> {
    let find = || async {
        db.query_opt_text(
            "SELECT id FROM threads WHERE project_id = ?1 AND name = ?2",
            (project_id.to_string(), GENERAL_THREAD_NAME),
        )
        .await
    };
    if let Some(id) = find().await? {
        return Ok(id);
    }
    let created = crate::threads::crud::create(
        db,
        CreateThread {
            project_id: project_id.to_string(),
            name: Some(GENERAL_THREAD_NAME.to_string()),
            jurisdiction: None,
            definition: None,
            migrated_from_number: None,
            model: None,
        },
    )
    .await;
    match created {
        Ok(thread) => Ok(thread.id),
        // A concurrent bootstrap created it first; its thread is ours too.
        Err(error) => match find().await? {
            Some(id) => Ok(id),
            None => Err(error),
        },
    }
}

/// Establish a project's general thread and base camp, returning base camp.
///
/// The one birth helper every project creation path and the boot backfill
/// share. Idempotent: a project that already has base camp is answered with a
/// read and nothing is written, so neither its lead nor its participants'
/// chosen levels are touched.
pub async fn ensure_project_base_camp(db: &LocalDb, project_id: &str) -> Result<Camp, CairnError> {
    if let Some(camp) = db.camp_by_slug(project_id, BASE_CAMP_SLUG).await? {
        return Ok(camp);
    }
    let lead = general_thread_id(db, project_id).await?;
    let (camp, _) = db
        .ensure_base_camp(project_id, &lead, chrono::Utc::now().timestamp())
        .await?;
    Ok(camp)
}

/// What the boot backfill did, for the boot log.
#[derive(Debug, Default)]
pub struct BaseCampBootstrap {
    pub projects: usize,
    pub established: Vec<String>,
    /// Projects whose base camp lead moved to a general thread minted for it.
    pub repaired: Vec<String>,
    pub errors: Vec<String>,
}

/// Give a base camp born before birth always minted `general` the general
/// thread it should have been led by. Returns the displaced lead, if any.
///
/// Earlier births seated the project's oldest active thread as lead when the
/// project had no general thread. Such a camp gets `general` minted through
/// the ordinary thread birth (which seats it in base camp) and the lead moved
/// to it, unless the lead was ever offered or handed over. A camp general
/// already leads is only read.
async fn repair_base_camp_lead(db: &LocalDb, camp: &Camp) -> Result<Option<String>, CairnError> {
    let general = db
        .query_opt_text(
            "SELECT id FROM threads WHERE project_id = ?1 AND name = ?2",
            (camp.project_id.clone(), GENERAL_THREAD_NAME),
        )
        .await?;
    if general.as_deref() == Some(camp.lead_thread_id.as_str()) {
        return Ok(None);
    }
    if db.camp_lead_was_handed(&camp.id).await? {
        return Ok(None);
    }
    let general = match general {
        Some(id) => id,
        None => general_thread_id(db, &camp.project_id).await?,
    };
    Ok(db
        .repair_base_camp_lead(&camp.project_id, &general, chrono::Utc::now().timestamp())
        .await?)
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
            let existing = match db.camp_by_slug(&project.id, BASE_CAMP_SLUG).await {
                Ok(camp) => camp,
                Err(error) => {
                    report.errors.push(format!("{}: {error}", project.key));
                    continue;
                }
            };
            if let Some(camp) = existing {
                match repair_base_camp_lead(&db, &camp).await {
                    Ok(Some(displaced)) => {
                        tracing::info!(project = %project.key, %displaced, "base camp lead moved to general");
                        report.repaired.push(project.key.clone());
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::error!(project = %project.key, %error, "base camp lead repair failed");
                        report.errors.push(format!("{}: {error}", project.key));
                    }
                }
                continue;
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
/// `project`. Camps are a thread's act: a run that acts for no thread (an issue
/// node, an unauthenticated shell) is refused, and so is a thread of another
/// project.
pub(crate) struct ActingThread {
    pub db: std::sync::Arc<LocalDb>,
    pub project_id: String,
    pub thread_id: String,
    pub actor: cairn_db::storage::camps::CampActor,
}

const NON_THREAD_REFUSAL: &str = "only a thread acts on camps: this run acts for no thread. Ask the thread that owns this work to pitch or join the camp.";

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
    let (principal, appearance) = agent.appearance(orch.services.clock.now())?;
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
        return Err("a thread takes part in its own project's camps only".to_string());
    }
    Ok(ActingThread {
        db,
        project_id,
        thread_id,
        actor: cairn_db::storage::camps::CampActor {
            principal,
            appearance,
        },
    })
}

/// The camp a `camp:` value on an issue write names, checked before anything
/// is written: a canonical camp URI of the issue's own project, live, and one
/// the acting thread takes part in.
pub(crate) async fn camp_for_issue_join(
    orch: &crate::orchestrator::Orchestrator,
    request: &crate::mcp::types::McpCallbackRequest,
    project: &str,
    camp_uri: &str,
) -> Result<(ActingThread, Camp), String> {
    let Some(cairn_uri::CairnResource::ProjectCamp {
        project: camp_project,
        slug,
    }) = cairn_uri::parse_uri(camp_uri.trim())
    else {
        return Err(format!(
            "payload.camp must be a canonical camp URI like {}, or null",
            cairn_uri::build_project_camp_uri(project, "SLUG")
        ));
    };
    if camp_project != cairn_uri::canonical_project(project) {
        return Err("an issue joins a camp of its own project only".to_string());
    }
    let acting = acting_thread(orch, request, project).await?;
    let camp = acting
        .db
        .camp_by_slug(&acting.project_id, &slug)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("no camp `{slug}` in project {project}"))?;
    if camp.is_struck() {
        return Err(format!("camp `{slug}` is struck; it takes no new issues"));
    }
    let takes_part = acting
        .db
        .camp_participants(&camp)
        .await
        .map_err(|error| error.to_string())?
        .iter()
        .any(|participant| participant.subject.thread_id() == Some(acting.thread_id.as_str()));
    if !takes_part {
        return Err(format!(
            "your thread does not take part in camp `{slug}`; join it at {} first",
            cairn_uri::build_project_camp_participants_uri(project, &slug)
        ));
    }
    Ok((acting, camp))
}

/// A camp transition for an existing issue, validated in full: who acts, which
/// issue, and the camp it moves to (`None` takes it out).
pub(crate) struct IssueCampPlan {
    pub acting: ActingThread,
    pub issue_id: String,
    pub camp: Option<Camp>,
}

/// Check every refusal an issue's camp change can meet before anything about
/// the issue is written: a canonical, live target camp the caller takes part
/// in; an issue not already in another camp; and, to take an issue out, a
/// caller taking part in the camp it leaves.
pub(crate) async fn plan_issue_camp(
    orch: &crate::orchestrator::Orchestrator,
    request: &crate::mcp::types::McpCallbackRequest,
    project: &str,
    number: i32,
    target: Option<&str>,
) -> Result<IssueCampPlan, String> {
    let (acting, camp) = match target {
        Some(uri) => {
            let (acting, camp) = camp_for_issue_join(orch, request, project, uri).await?;
            (acting, Some(camp))
        }
        None => (acting_thread(orch, request, project).await?, None),
    };
    let issue_id =
        crate::issues::relations::issue_id_for_project_number(&acting.db, project, number)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("Issue {project}/{number} not found"))?;
    let current = acting
        .db
        .query_opt_text(
            "SELECT camp_id FROM issues WHERE id = ?1",
            (issue_id.clone(),),
        )
        .await
        .map_err(|error| error.to_string())?;
    match (&camp, current) {
        (Some(target), Some(current)) if current != target.id => {
            let slug = acting
                .db
                .query_opt_text("SELECT slug FROM camps WHERE id = ?1", (current.clone(),))
                .await
                .ok()
                .flatten()
                .unwrap_or(current);
            return Err(format!(
                "issue #{number} is already joined to camp `{slug}`; take it out first (camp: null), then join the other"
            ));
        }
        (None, Some(current)) => {
            let takes_part = acting
                .db
                .camps_for_thread(&acting.thread_id)
                .await
                .map_err(|error| error.to_string())?
                .iter()
                .any(|(camp, _)| camp.id == current);
            if !takes_part {
                return Err(
                    "only a thread taking part in the issue's camp takes the issue out of it"
                        .to_string(),
                );
            }
        }
        _ => {}
    }
    Ok(IssueCampPlan {
        acting,
        issue_id,
        camp,
    })
}

/// Join issue `issue_id` to `camp`, or take it out of its camp, as `acting`.
pub(crate) async fn set_issue_camp(
    acting: &ActingThread,
    issue_id: &str,
    camp: Option<Camp>,
    now: i64,
) -> Result<cairn_db::storage::camps::IssueCampChange, String> {
    let (issue_id, actor) = (issue_id.to_string(), acting.actor.clone());
    acting
        .db
        .write(move |conn| {
            let (issue_id, actor, camp) = (issue_id.clone(), actor.clone(), camp.clone());
            Box::pin(async move {
                cairn_db::storage::camps::set_issue_camp_conn(
                    conn,
                    &issue_id,
                    camp.as_ref(),
                    Some(&actor),
                    now,
                )
                .await
            })
        })
        .await
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests;
