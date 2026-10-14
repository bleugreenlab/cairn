//! Who a fact row addresses, resolved at delivery.
//!
//! A row stores the URIs it names, never job ids. The index lane asks here
//! which jobs each one reaches right now, so a thread that rotated to a new
//! session receives on the new one and a retired home receives nothing.

use cairn_common::uri::CairnResource;

use crate::storage::LocalDb;

use super::child::watcher_jobs_for_issue;

/// The jobs one addressed URI reaches now.
///
/// An issue reaches its watchers, the same derived-plus-explicit set every
/// issue-sourced push uses. A node, a sub-agent task, and a thread reach the
/// job that is that home now. Every other shape (a diff, a chat window, a post
/// or a comment) is a citation, not a destination: a link says where to look,
/// not who to tell, and routing it would wake a node for being mentioned in
/// passing.
pub(crate) async fn addressed_jobs(db: &LocalDb, uri: &str) -> Result<Vec<String>, String> {
    let Some(resource) = cairn_common::uri::parse_uri(uri) else {
        return Ok(Vec::new());
    };
    match &resource {
        CairnResource::Issue { project, number } => {
            watcher_jobs_for_issue(db, &cairn_common::uri::build_issue_uri(project, *number)).await
        }
        CairnResource::Node { .. } | CairnResource::Task { .. } | CairnResource::Thread { .. } => {
            Ok(home_job_for_resource(db, &resource)
                .await?
                .into_iter()
                .collect())
        }
        _ => Ok(Vec::new()),
    }
}

/// The project a job's home belongs to, as `(projects.id, projects.key)`. An
/// issue job takes its issue's project; a thread session (which has no issue)
/// carries its own: the same COALESCE that names a job's home.
pub(crate) async fn job_project(
    db: &LocalDb,
    job_id: &str,
) -> Result<(Option<String>, Option<String>), String> {
    db.query_opt(
        "SELECT p.id, p.key FROM jobs j
           LEFT JOIN issues i ON i.id = j.issue_id
           LEFT JOIN projects p ON p.id = COALESCE(i.project_id, j.project_id)
          WHERE j.id = ?1 LIMIT 1",
        (job_id.to_string(),),
        |row| {
            Ok((
                crate::storage::RowExt::opt_text(row, 0)?,
                crate::storage::RowExt::opt_text(row, 1)?,
            ))
        },
    )
    .await
    .map(Option::unwrap_or_default)
    .map_err(|error| format!("Failed to resolve a job's home project: {error}"))
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
