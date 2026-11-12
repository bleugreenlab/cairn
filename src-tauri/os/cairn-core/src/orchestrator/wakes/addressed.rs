//! What a fact row's addressee URIs reach, for the recipient resolver.
//!
//! A row stores the URIs it names, never job ids. The index lane asks here
//! what each one is now: an issue (whose audience the resolver derives in the
//! issue's own database), a camp (whose census it reads in the camp's own
//! database), or a home (a node, a task, a thread) as its stable recipient.
//! The session that receives a delivery is resolved only at delivery, so a
//! thread that rotates sessions receives on the new one and a retired home
//! receives nothing.

use std::sync::Arc;

use cairn_attention::wakes::StableRecipient;
use cairn_uri::CairnResource;

use crate::storage::LocalDb;

/// What one addressed URI reaches.
pub(crate) enum Addressed {
    /// An issue: its parent edge, subscriptions and camp, in its owning
    /// database.
    Issue { db: Arc<LocalDb>, uri: String },
    /// A camp: its members, in the camp's own project database, so a
    /// team camp is judged against its team project.
    Camp { db: Arc<LocalDb>, camp_id: String },
    /// A node, a sub-agent task or a thread: the recipient that home is.
    Home(StableRecipient),
    /// A citation, not a destination: a link says where to look, not who to
    /// tell, and routing it would wake a node for being mentioned in passing.
    Nothing,
}

/// What `uri` reaches now.
pub(crate) async fn addressed(dbs: &crate::db::DbState, uri: &str) -> Result<Addressed, String> {
    let Some(resource) = cairn_uri::parse_uri(uri) else {
        return Ok(Addressed::Nothing);
    };
    match &resource {
        CairnResource::Issue { project, number } => Ok(Addressed::Issue {
            db: dbs.for_project(project).await,
            uri: cairn_uri::build_issue_uri(project, *number),
        }),
        CairnResource::ProjectCamp { project, slug } => {
            let db = dbs.for_project(project).await;
            let project_id =
                crate::mcp::handlers::run_context::project_id_by_key(&db, project).await?;
            let slug = slug.clone();
            let camp = db
                .read(move |conn| {
                    let (project_id, slug) = (project_id.clone(), slug.clone());
                    Box::pin(async move {
                        cairn_db::storage::camps::camp_by_slug_conn(conn, &project_id, &slug).await
                    })
                })
                .await
                .map_err(|error| format!("Failed to resolve camp {uri}: {error}"))?;
            Ok(match camp {
                Some(camp) => Addressed::Camp {
                    db,
                    camp_id: camp.id,
                },
                None => Addressed::Nothing,
            })
        }
        CairnResource::Node { .. } | CairnResource::Task { .. } | CairnResource::Thread { .. } => {
            Ok(match home_recipient(&dbs.local, &resource).await? {
                Some(recipient) => Addressed::Home(recipient),
                None => Addressed::Nothing,
            })
        }
        _ => Ok(Addressed::Nothing),
    }
}

/// The stable recipient a home URI names, if it names a home.
pub(crate) async fn home_recipient_for_uri(
    db: &LocalDb,
    uri: &str,
) -> Result<Option<StableRecipient>, String> {
    match cairn_uri::parse_uri(uri) {
        Some(resource) => home_recipient(db, &resource).await,
        None => Ok(None),
    }
}

async fn home_recipient(
    db: &LocalDb,
    resource: &CairnResource,
) -> Result<Option<StableRecipient>, String> {
    let Some(job_id) = home_job_for_resource(db, resource).await? else {
        return Ok(None);
    };
    cairn_attention::wakes::stable_recipient_for_job(db, &job_id)
        .await
        .map(Some)
}

/// A home URI's current job, through the canonical coordinate resolvers.
///
/// Threads resolve through their own helpers rather than the node coordinate so
/// the *newest* session wins after a rotation, which is the whole point of
/// resolving late.
async fn home_job_for_resource(
    db: &LocalDb,
    resource: &CairnResource,
) -> Result<Option<String>, String> {
    let resolved = match resource {
        CairnResource::Node {
            project,
            number,
            exec_seq,
            node_id,
        } => {
            crate::jobs::queries::job_id_for_node_coordinate(
                db, project, *number, *exec_seq, node_id, None,
            )
            .await
        }
        CairnResource::Task {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        } => {
            crate::jobs::queries::job_id_for_node_coordinate(
                db,
                project,
                *number,
                *exec_seq,
                node_id,
                Some(task_name),
            )
            .await
        }
        CairnResource::Thread {
            project,
            name,
            path,
        } => {
            let (project, name) = (project.clone(), name.clone());
            // Only the thread itself and the tasks beneath it are homes; a
            // deeper sub-resource path is a citation of one of its surfaces.
            let task = match path.as_slice() {
                [] => None,
                [task, segment] if task == "task" => Some(segment.clone()),
                _ => return Ok(None),
            };
            db.read(move |conn| {
                let (project, name, task) = (project.clone(), name.clone(), task.clone());
                Box::pin(async move {
                    match task {
                        None => {
                            crate::threads::session_job_id_by_name_conn(conn, &project, &name).await
                        }
                        Some(task) => {
                            crate::threads::task_job_id_by_name_conn(conn, &project, &name, &task)
                                .await
                        }
                    }
                })
            })
            .await
        }
        _ => return Ok(None),
    };
    resolved.map_err(|error| format!("Failed to resolve an addressed home: {error}"))
}
