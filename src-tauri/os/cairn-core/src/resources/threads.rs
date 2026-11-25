//! First-class thread resource rendering and migration-address resolution.
//! A thread's sub-path never reaches this module raw: read, write and `run`
//! first normalize it with `cairn_uri::delegate_thread_descendant`, which maps
//! it onto the node-family resource that answers for it.

use super::common::{lookup_project_by_key, read_resource};
use crate::execution::jobs::liveness::WorkObserver;
use cairn_db::storage::{LocalDb, RowExt};
use cairn_db::turso::params;

pub(super) async fn resolve_thread_id(
    db: &LocalDb,
    project_key: &str,
    name: &str,
) -> Result<Option<String>, String> {
    let project_key = project_key.to_string();
    let name = name.to_string();
    db.read(move |conn| Box::pin(async move {
        let mut rows = conn.query(
            "SELECT t.id FROM threads t JOIN projects p ON p.id = t.project_id WHERE LOWER(p.key) = LOWER(?1) AND t.name = ?2 LIMIT 1",
            params![project_key, name],
        ).await?;
        Ok(match rows.next().await? {
            Some(row) => Some(row.get::<String>(0)?),
            None => None,
        })
    })).await.map_err(|error| format!("Failed to resolve thread: {error}"))
}

/// A thread's queue is a filtered slice, never a second classification.
pub(crate) async fn read_thread_queue(
    orch: &crate::orchestrator::Orchestrator,
    db: &LocalDb,
    project_key: &str,
    thread: &str,
) -> String {
    let id = match resolve_thread_id(db, project_key, thread).await {
        Ok(Some(id)) => id,
        Ok(None) => return format!("Thread '{thread}' not found"),
        Err(error) => return error,
    };
    match super::queue::view(orch).await {
        Ok(mut view) => {
            view.items
                .retain(|row| row.item.thread_id.as_deref() == Some(id.as_str()));
            super::queue::render(&view)
        }
        Err(error) => format!("Could not read {thread}'s queue: {error}"),
    }
}

pub(super) async fn resolve_migrated_thread_uri(
    db: &LocalDb,
    identity: &str,
) -> Result<Option<(String, String)>, String> {
    let parts: Vec<&str> = identity
        .strip_prefix("cairn://p/")
        .unwrap_or("")
        .split('/')
        .collect();
    if parts.len() < 2 {
        return Ok(None);
    }
    let Ok(_number) = parts[1].parse::<i64>() else {
        return Ok(None);
    };
    // The alias is a best-effort fallback: a project (or thread) that does not
    // resolve means "no alias here", never a new failure path. Erroring out
    // would preempt the canonical handling downstream — e.g. the contract
    // gate's "Unsupported resource mutation" rejection, which must win even
    // when the database knows nothing about the project in the URI.
    let alias = format!("cairn://p/{}/{}", parts[0], parts[1]);
    let lookup = alias.clone();
    let resolved = read_resource(db, move |conn| {
        Box::pin(async move {
            crate::threads::resolve_parent_thread_uri_conn(conn, &lookup)
                .await
                .map_err(|e| e.to_string())
        })
    })
    .await?;
    let Some((_, _, name)) = resolved else {
        return Ok(None);
    };
    let canonical = format!("cairn://p/{}/{}", parts[0], name);
    let mapped = match parts.as_slice() {
        [_, _, _, "thread", tail @ ..] if !tail.is_empty() => {
            format!("{canonical}/{}", tail.join("/"))
        }
        _ => canonical.clone(),
    };
    Ok(Some((mapped, canonical)))
}

pub(super) async fn read_project_threads(db: &LocalDb, project_key: &str) -> String {
    let project_key = project_key.to_owned();
    read_resource(db, move |conn| {
        Box::pin(async move {
            let project_key = project_key.as_str();
            let project = match lookup_project_by_key(conn, project_key).await {
                Ok(v) => v,
                Err(e) => return Ok(e),
            };
            // Active only. This collection is what an agent reads to find the threads it
            // can address, so a closed thread must not appear in it — while its own
            // canonical URI keeps resolving, transcript and children intact, so it can be
            // read and reopened.
            let mut rows = match conn.query("SELECT name, jurisdiction, status, attention, updated_at FROM threads WHERE project_id = ?1 AND status = 'active' ORDER BY name", params![project.project_id.as_str()]).await {
                Ok(v) => v, Err(e) => return Ok(format!("Failed to load project threads: {e}"))
            };
            let mut out = format!("# Threads — {}\n\n", project.project_key);
            let mut count = 0;
            while let Ok(Some(row)) = rows.next().await {
                let (Ok(name), Ok(jurisdiction), Ok(status), Ok(attention), Ok(updated_at)) = (
                    row.text(0),
                    row.opt_text(1),
                    row.text(2),
                    row.text(3),
                    row.i64(4),
                ) else {
                    continue;
                };
                count += 1;
                let last_activity = crate::clock::age(updated_at);
                out.push_str(&format!("- [{name}](cairn://p/{}/{name})\n  {}Status: {status}. Attention: {attention}. Last activity: {last_activity}.\n", project.project_key, jurisdiction.map(|v| format!("Jurisdiction: {v}. ")).unwrap_or_default()));
            }
            if count == 0 {
                out.push_str("No threads.\n");
            }
            Ok(out)
        })
    })
    .await
    .unwrap_or_else(|error| error)
}

/// What appending to a thread does to its session, said where a caller decides
/// to append. Thread messages are delivered as `DeliveryUrgency::Steer`: an
/// idle (or not yet started) session is woken and the message starts its turn;
/// a running turn receives it at its next tool or turn boundary.
fn thread_delivery_semantics(status: &str) -> &'static str {
    if status == "closed" {
        "Messaging: this thread is closed, so an append is refused until it is patched back to `status:\"active\"`."
    } else {
        "Messaging: append `{content}` to this URI to message the thread's session. An idle session starts a turn on the message at once (a session not yet started is started); a running turn receives it at its next tool call or turn boundary, without being interrupted."
    }
}

/// Render a thread's overview.
///
/// Only the overview: every addressable descendant is normalized onto its
/// node-family resource by [`cairn_uri::delegate_thread_descendant`] before dispatch
/// reaches here, so there is no sub-path interpretation left in this function.
///
pub(super) async fn read_thread(
    db: &LocalDb,
    project_key: &str,
    name: &str,
    path: &[String],
    _default_open_children: i64,
    _observer: &WorkObserver,
) -> String {
    let project_key = project_key.to_owned();
    let name = name.to_owned();
    let path = path.to_vec();
    read_resource(db, move |conn| {
            Box::pin(async move {
            let project_key = project_key.as_str();
            let name = name.as_str();
            let path = path.as_slice();
            let project = match lookup_project_by_key(conn, project_key).await {
                Ok(v) => v,
                Err(e) => return Ok(e),
            };
            let mut rows = match conn.query("SELECT id, jurisdiction, status, attention, definition FROM threads WHERE project_id = ?1 AND name = ?2 LIMIT 1", params![project.project_id.as_str(), name]).await {
                Ok(v) => v, Err(e) => return Ok(format!("Failed to load thread: {e}"))
            };
            let Some(row) = rows.next().await.ok().flatten() else {
                return Ok(format!("Thread '{}' not found in {}", name, project.project_key));
            };
            let (Ok(id), Ok(jurisdiction), Ok(status), Ok(attention), Ok(definition)) = (
                row.text(0),
                row.opt_text(1),
                row.text(2),
                row.text(3),
                row.opt_text(4),
            ) else {
                return Ok("Failed to decode thread".into());
            };
            let mut jobs = match conn
                .query(
                    &format!(
                        "SELECT j.id, j.uri_segment, j.status FROM jobs j
                         WHERE j.thread_id = ?1 AND {}
                         ORDER BY j.created_at DESC, j.rowid DESC LIMIT 1",
                        crate::threads::SESSION_JOB_SHAPE
                    ),
                    params![id.as_str()],
                )
                .await
            {
                Ok(v) => v,
                Err(e) => return Ok(format!("Failed to load thread session: {e}")),
            };
            let session = jobs
                .next()
                .await
                .ok()
                .flatten()
                .and_then(|row| Some((row.text(0).ok()?, row.opt_text(1).ok()?, row.text(2).ok()?)));
            // Every addressable descendant was normalized onto its node-family resource
            // by `cairn_uri::delegate_thread_descendant` before dispatch reached here, so anything
            // still arriving with a path is a shape nothing answers for.
            if !path.is_empty() {
                return Ok(format!(
                    "Unknown thread session resource '{}'. Read {} for the thread overview.",
                    path.join("/"),
                    cairn_uri::build_thread_uri(&project.project_key, name)
                ));
            }
            let canonical = format!("cairn://p/{}/{}", project.project_key, name);
            let mut out = format!("# {name}\n\n");
            if let Some(jurisdiction) = jurisdiction {
                out.push_str(&format!("Jurisdiction: {jurisdiction}\n\n"));
            }
            out.push_str(&format!("Status: {status} · Attention: {attention}\n\n"));
            if let Some(definition) = definition {
                out.push_str(&format!("Definition: {definition}\n\n"));
            }
            let owner = crate::fleet::placement_default::DefaultOwner::Thread(id.clone());
            if let Ok(Some(default)) =
                crate::fleet::placement_default::load_conn(conn, &owner).await
            {
                out.push_str(&format!("{}\n\n", default.line()));
            }
            out.push_str(&format!(
                "- [intent]({canonical}/db/intent/1) · [journal]({canonical}/db/journal)\n"
            ));
            let read_through = match crate::threads::headlines::cursor_conn(conn, &id).await {
                Ok(cursor) => crate::threads::headlines::read_through_line(
                    cursor.as_ref(),
                    chrono::Utc::now().timestamp(),
                ),
                Err(error) => format!("How far the operator has read is unknown: {error}"),
            };
            out.push_str(&format!(
                "- [headlines]({canonical}/db/headlines) · {read_through}\n"
            ));
            match session {
                Some((job_id, segment, session_status)) => out.push_str(&format!(
                    "- Live session: {}{} ({})\n",
                    job_id,
                    segment.map(|v| format!(" / {v}")).unwrap_or_default(),
                    session_status
                )),
                None => out.push_str("- Live session: not started\n"),
            }
            out.push_str(&format!("\n{}\n", thread_delivery_semantics(&status)));
            let mut children = match conn
                .query(
                    "SELECT i.id, i.number, i.title, i.status
                       FROM issues i WHERE i.parent_thread_id = ?1 ORDER BY i.number",
                    params![id.as_str()],
                )
                .await
            {
                Ok(v) => v,
                Err(_) => return Ok(out),
            };
            let mut listed = Vec::new();
            while let Ok(Some(row)) = children.next().await {
                let (Ok(issue_id), Ok(number), Ok(title), Ok(status)) =
                    (row.text(0), row.i64(1), row.text(2), row.text(3))
                else {
                    continue;
                };
                listed.push((issue_id, number, title, status));
            }
            drop(children);
            out.push_str("\n## Children\n\n");
            for (issue_id, number, title, status) in &listed {
                let failed_line = if status == "failed" {
                    match cairn_attention::delivery::issue_failure_conn(conn, issue_id)
                        .await
                        .ok()
                        .flatten()
                    {
                        Some(failure) => format!(
                            " · Failed: {} — `{}`", failure.reason, failure.node_uri
                        ),
                        None => " · Failed".to_string(),
                    }
                } else {
                    String::new()
                };
                out.push_str(&format!(
                    "- [{number} — {title}](cairn://p/{}/{number}) · {status}{failed_line}\n",
                    project.project_key
                ));
            }
            if listed.is_empty() {
                out.push_str("None.\n");
            }
            Ok(out)
        })
    })
    .await
    .unwrap_or_else(|error| error)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fixture() -> LocalDb {
        let db = crate::storage::migrated_test_db("thread-resources.db").await;
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p-threads', 'default', 'Threads', 'thr', '/tmp/threads', 1, 1);
             INSERT INTO threads (id, project_id, name, jurisdiction, migrated_from_number, created_at, updated_at)
             VALUES ('t-design', 'p-threads', 'design-review', 'Own architecture decisions', 3404, 2, 3);
             INSERT INTO issues (id, project_id, number, title, description, status, progress, attention, priority, parent_thread_id, created_at, updated_at)
             VALUES ('i-child', 'p-threads', 12, 'Child issue', '', 'backlog', 'backlog', 'none', 0, 't-design', 4, 4);",
        )
        .await
        .unwrap();
        db
    }

    #[tokio::test]
    async fn collection_and_overview_render_first_class_thread_fields() {
        let db = fixture().await;
        let collection = read_project_threads(&db, "thr").await;
        assert!(collection.contains("[design-review](cairn://p/thr/design-review)"));
        assert!(collection.contains("Jurisdiction: Own architecture decisions"));
        assert!(collection.contains("Status: active"));
        assert!(collection.contains("Attention: none"));
        // A thread's last activity reads as an age with its absolute anchor
        // beside it, never as the bare `updated_at` epoch it is stored as.
        assert!(
            collection.contains("Last activity: ") && collection.contains(" ago ("),
            "{collection}"
        );
        assert!(
            !collection.contains("Last activity: 3."),
            "the stored epoch must not reach the surface: {collection}"
        );

        let overview = read_thread(
            &db,
            "thr",
            "design-review",
            &[],
            4,
            &WorkObserver::nothing(),
        )
        .await;
        // A thread's heading is its one identifier, the same string its URI
        // carries and its pane header shows.
        assert!(overview.contains("# design-review"));
        assert!(
            overview.contains("[intent](cairn://p/thr/design-review/db/intent/1) · [journal](cairn://p/thr/design-review/db/journal)"),
            "{overview}"
        );
        assert!(overview.contains("[12 — Child issue](cairn://p/thr/12)"));
        assert!(overview.contains("Live session: not started"));
        // The overview says what the seed says about the thread's reader.
        assert!(
            overview.contains(
                "- [headlines](cairn://p/thr/design-review/db/headlines) · Operator has never read your headlines\n"
            ),
            "{overview}"
        );
        let two_hours_ago = chrono::Utc::now().timestamp() - 2 * 3600 - 30;
        db.execute(
            "INSERT INTO headline_read_cursors (reader, thread_id, seen_through_id, seen_at, seen_on)
             VALUES ('operator', 't-design', 4, ?1, 'desktop')",
            (two_hours_ago,),
        )
        .await
        .unwrap();
        let overview = read_thread(
            &db,
            "thr",
            "design-review",
            &[],
            4,
            &WorkObserver::nothing(),
        )
        .await;
        assert!(
            overview.contains("· Operator has read through #4, 2h ago\n"),
            "{overview}"
        );
        // The page a caller appends to says what an append does to the session.
        assert!(
            overview.contains("An idle session starts a turn on the message at once"),
            "{overview}"
        );
        assert!(
            overview.contains("at its next tool call or turn boundary, without being interrupted"),
            "{overview}"
        );
    }

    /// The collection is an ACTIVE listing and the canonical URI is not. That
    /// asymmetry is the whole shape of dormancy: a closed thread stops being
    /// offered to agents while staying fully readable at its own address, which
    /// is what makes closing reversible from the outside.
    #[tokio::test]
    async fn the_collection_hides_a_closed_thread_that_its_own_uri_still_renders() {
        let db = fixture().await;
        db.execute_script(
            "INSERT INTO threads (id, project_id, name, jurisdiction, status, created_at, updated_at)
             VALUES ('t-retired', 'p-threads', 'retired-topic', 'Old ground', 'closed', 2, 3);
             INSERT INTO issues (id, project_id, number, title, description, status, progress, attention, priority, parent_thread_id, created_at, updated_at)
             VALUES ('i-retired-child', 'p-threads', 44, 'Still open child', '', 'backlog', 'backlog', 'none', 0, 't-retired', 4, 4);",
        )
        .await
        .unwrap();

        let collection = read_project_threads(&db, "thr").await;
        assert!(collection.contains("[design-review](cairn://p/thr/design-review)"));
        assert!(
            !collection.contains("retired-topic"),
            "a closed thread is not offered as somewhere to address: {collection}"
        );

        let overview = read_thread(
            &db,
            "thr",
            "retired-topic",
            &[],
            4,
            &WorkObserver::nothing(),
        )
        .await;
        assert!(overview.contains("# retired-topic"));
        assert!(overview.contains("Status: closed"));
        assert!(
            overview.contains("this thread is closed, so an append is refused"),
            "{overview}"
        );
        assert!(
            overview.contains("[44 — Still open child](cairn://p/thr/44)"),
            "closing routes attention, it does not disown children: {overview}"
        );
    }

    #[tokio::test]
    async fn migrated_number_resolves_only_when_no_issue_owns_it() {
        let db = fixture().await;
        assert_eq!(
            resolve_migrated_thread_uri(&db, "cairn://p/thr/3404")
                .await
                .unwrap(),
            Some((
                "cairn://p/thr/design-review".into(),
                "cairn://p/thr/design-review".into()
            ))
        );
        assert_eq!(
            resolve_migrated_thread_uri(&db, "cairn://p/thr/3404/1/thread/chat")
                .await
                .unwrap(),
            Some((
                "cairn://p/thr/design-review/chat".into(),
                "cairn://p/thr/design-review".into()
            ))
        );

        db.execute(
            "INSERT INTO issues (id, project_id, number, title, description, status, progress, attention, priority, created_at, updated_at)
             VALUES ('i-real', 'p-threads', 3404, 'Real issue', '', 'backlog', 'backlog', 'none', 0, 5, 5)",
            (),
        )
        .await
        .unwrap();
        assert_eq!(
            resolve_migrated_thread_uri(&db, "cairn://p/thr/3404")
                .await
                .unwrap(),
            None
        );
    }

    /// A thread's descendants are read by the ORDINARY node renderers, reached
    /// through the reserved coordinate the normalizer produces.
    ///
    /// Driving the node renderers directly is what this proves: no thread-specific
    /// read path is involved, and the URIs they render address the thread rather
    /// than a `0/0` placeholder.
    #[tokio::test]
    async fn thread_descendants_read_through_the_ordinary_node_renderers() {
        let db = fixture().await;
        let job_id = crate::threads::ensure_thread_session(&db, "t-design")
            .await
            .unwrap();
        db.execute(
            "INSERT INTO memories (id, name, project_id, content, scope, scope_value, job_id, node_seq, created_at, updated_at) VALUES ('m-thread', 'Thread fact', 'p-threads', 'Remember this detail', 'project', 'p-threads', ?1, 1, 6, 6)",
            (job_id.as_str(),),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO jobs (id, parent_job_id, project_id, status, node_name, uri_segment, created_at, updated_at) VALUES ('j-task', ?1, 'p-threads', 'complete', 'research', 'research', 7, 8)",
            (job_id.as_str(),),
        )
        .await
        .unwrap();

        // The session job is what a thread coordinate resolves to, and a task it
        // spawned resolves beneath it.
        assert_eq!(
            crate::jobs::queries::job_id_for_node_coordinate(
                &db,
                "thr",
                0,
                0,
                "design-review",
                None
            )
            .await
            .unwrap()
            .as_deref(),
            Some(job_id.as_str())
        );
        assert_eq!(
            crate::jobs::queries::job_id_for_node_coordinate(
                &db,
                "thr",
                0,
                0,
                "design-review",
                Some("research")
            )
            .await
            .unwrap()
            .as_deref(),
            Some("j-task")
        );

        // The node renderers, driven at the thread coordinate.
        let wakes = super::super::node::read_node_wakes(&db, "thr", 0, 0, "design-review").await;
        assert!(wakes.contains("# Wakes — design-review"), "{wakes}");
        // `ensure_thread_session` seeds the default system subscriptions so a
        // thread session hears messages from birth.
        assert!(wakes.contains("peer `*`"), "{wakes}");
        assert!(wakes.contains("user `*`"), "{wakes}");
        // The rendered address is the thread's, not a 0/0 placeholder.
        assert!(
            wakes.contains("cairn://p/thr/design-review/wakes"),
            "{wakes}"
        );
        assert!(!wakes.contains("/0/0/"), "{wakes}");

        let todos =
            super::super::node::read_job_todos(&db, "thr", 0, 0, "design-review", None).await;
        assert!(!todos.contains("/0/0/"), "{todos}");

        let tasks = super::super::node::read_node_tasks(
            &db,
            "thr",
            0,
            0,
            "design-review",
            &Default::default(),
        )
        .await;
        assert!(tasks.contains("research"), "{tasks}");
        assert!(!tasks.contains("/0/0/"), "{tasks}");

        let task = super::super::node::read_task(
            &db,
            &Default::default(),
            &WorkObserver::nothing(),
            "thr",
            0,
            0,
            "design-review",
            "research",
        )
        .await;
        assert!(task.contains("research"), "{task}");
        assert!(!task.contains("/0/0/"), "{task}");

        let memories = super::super::memories::render_job_memories(
            &db,
            "design-review",
            "cairn://p/thr/design-review/memories",
            &job_id,
        )
        .await;
        assert!(
            memories.contains("cairn://p/thr/design-review/memories/1"),
            "{memories}"
        );
        assert_eq!(
            crate::memories::db::resolve_node_memory_id(&db, "thr", 0, 0, "design-review", 1)
                .await
                .unwrap()
                .as_deref(),
            Some("m-thread"),
            "a thread's memories are individually addressable"
        );
    }

    /// A read of a dormant thread degrades to its overview and, crucially, does
    /// not bring the session job into existence.
    #[tokio::test]
    async fn reading_a_thread_without_a_session_creates_nothing() {
        let db = fixture().await;
        async fn job_count(db: &LocalDb) -> i64 {
            db.query_opt("SELECT COUNT(*) FROM jobs", (), |row| row.i64(0))
                .await
                .unwrap()
                .unwrap()
        }
        assert_eq!(job_count(&db).await, 0);

        for body in [
            super::super::node::read_node_wakes(&db, "thr", 0, 0, "design-review").await,
            super::super::node::read_job_todos(&db, "thr", 0, 0, "design-review", None).await,
            super::super::node::read_node_tasks(
                &db,
                "thr",
                0,
                0,
                "design-review",
                &Default::default(),
            )
            .await,
        ] {
            assert!(body.contains("has no session yet"), "{body}");
            assert!(body.contains("cairn://p/thr/design-review"), "{body}");
        }

        assert_eq!(
            job_count(&db).await,
            0,
            "a read must never mint a thread's session job"
        );

        let overview = read_thread(
            &db,
            "thr",
            "design-review",
            &[],
            4,
            &WorkObserver::nothing(),
        )
        .await;
        assert!(overview.contains("Live session: not started"), "{overview}");
    }

    /// An era's rendered range derives from the thread's CURRENT coordinates
    /// rather than a value baked in at mint time -- the point of not
    /// persisting `range_uri` (CAIRN-4302 amendment): a rename never orphans
    /// it.
    #[tokio::test]
    async fn thread_eras_render_through_the_node_family_with_a_live_derived_range() {
        let db = fixture().await;
        let job_id = crate::threads::ensure_thread_session(&db, "t-design")
            .await
            .unwrap();
        db.execute(
            "INSERT INTO thread_eras (
                 job_id, seq, start_block, end_block, started_at, ended_at, summary,
                 summary_status, summary_model, minted_generation, created_at
             ) VALUES (?1, 1, 0, 2, 10, 20, 'Settled the initial layout question.', 'fallback', NULL, 1, 30)",
            (job_id.as_str(),),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO thread_era_children (job_id, era_seq, child_issue_id, child_issue_uri, child_title, final_status)
             VALUES (?1, 1, 'i-child', 'cairn://p/thr/12', 'Child issue', 'merged')",
            (job_id.as_str(),),
        )
        .await
        .unwrap();

        let collection = super::super::eras::render_node_eras(
            &db,
            "design-review",
            "cairn://p/thr/design-review/eras",
            "cairn://p/thr/design-review/chat",
            &job_id,
            50,
            0,
        )
        .await;
        assert!(collection.contains("1 era(s)"), "{collection}");
        assert!(
            collection.contains("cairn://p/thr/design-review/chat?offset=0&limit=3"),
            "the range is derived from the thread's live chat URI, not persisted: {collection}"
        );
        assert!(!collection.contains("/0/0/"), "{collection}");

        let member = super::super::eras::render_node_era(
            &db,
            "design-review",
            "cairn://p/thr/design-review/eras/1",
            "cairn://p/thr/design-review/chat",
            &job_id,
            1,
        )
        .await;
        assert!(
            member.contains("Settled the initial layout question."),
            "{member}"
        );
        assert!(member.contains("Child issue"), "{member}");
        assert!(
            member.contains("cairn://p/thr/design-review/chat?offset=0&limit=3"),
            "{member}"
        );
        assert!(!member.contains("/0/0/"), "{member}");
    }
}
