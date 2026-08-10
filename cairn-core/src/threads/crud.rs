//! Persistence operations for first-class threads.

use crate::error::CairnError;
use crate::models::{CreateThread, Thread, UpdateThread};
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use cairn_common::{ids, thread_name::validate_thread_name};
use cairn_db::turso::params;

const COLUMNS: &str = "id, project_id, name, jurisdiction, status, attention,
    definition, migrated_from_number, created_at, updated_at";

fn from_row(row: &cairn_db::turso::Row) -> DbResult<Thread> {
    Ok(Thread {
        id: row.text(0)?,
        project_id: row.text(1)?,
        name: row.text(2)?,
        jurisdiction: row.opt_text(3)?,
        status: crate::models::ThreadStatus::parse(&row.text(4)?).map_err(DbError::Row)?,
        attention: row.text(5)?,
        definition: row.opt_text(6)?,
        migrated_from_number: row.opt_i64(7)?,
        created_at: row.i64(8)?,
        updated_at: row.i64(9)?,
    })
}

async fn get_conn(conn: &cairn_db::turso::Connection, id: &str) -> DbResult<Option<Thread>> {
    let sql = format!("SELECT {COLUMNS} FROM threads WHERE id = ?1");
    let mut rows = conn.query(&sql, params![id]).await?;
    rows.next().await?.map(|row| from_row(&row)).transpose()
}

pub async fn get(db: &LocalDb, id: &str) -> Result<Option<Thread>, CairnError> {
    let id = id.to_string();
    db.read(|conn| {
        let id = id.clone();
        Box::pin(async move { get_conn(conn, &id).await })
    })
    .await
    .map_err(Into::into)
}

/// Every thread in a project, closed ones included.
///
/// Deliberately unfiltered: this is the lookup that resolves a thread by name
/// for a rename, a reopen, or a delete, and a closed thread that could not be
/// found could never be reopened. Surfaces that enumerate threads for ATTENTION
/// — the sidebar, the `cairn://p/<project>/threads` collection, the injected
/// Project Threads block — apply the active predicate themselves.
pub async fn list(db: &LocalDb, project_id: &str) -> Result<Vec<Thread>, CairnError> {
    let project_id = project_id.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let sql = format!("SELECT {COLUMNS} FROM threads WHERE project_id = ?1 ORDER BY name");
            let mut rows = conn.query(&sql, params![project_id]).await?;
            let mut threads = Vec::new();
            while let Some(row) = rows.next().await? {
                threads.push(from_row(&row)?);
            }
            Ok(threads)
        })
    })
    .await
    .map_err(Into::into)
}

pub async fn create(db: &LocalDb, input: CreateThread) -> Result<Thread, CairnError> {
    validate_thread_name(&input.name).map_err(CairnError::Internal)?;
    if let Some(definition) = input.definition.as_deref() {
        super::resolve_thread_definition(Some(definition)).map_err(CairnError::Internal)?;
    }

    let id = ids::mint_child(&input.project_id);
    let now = chrono::Utc::now().timestamp();
    let thread = db
        .write(|conn| {
            let input = input.clone();
            let id = id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO threads
                 (id, project_id, name, jurisdiction, definition,
                  migrated_from_number, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
                    params![
                        id.as_str(),
                        input.project_id,
                        input.name.as_str(),
                        input.jurisdiction,
                        input.definition,
                        input.migrated_from_number,
                        now
                    ],
                )
                .await
                .map_err(|error| {
                    let message = error.to_string().to_ascii_lowercase();
                    if message.contains("migrated_from_number") {
                        DbError::Row(format!(
                            "thread migration number already exists in project: {}",
                            input.migrated_from_number.unwrap_or_default()
                        ))
                    } else if message.contains("unique") {
                        DbError::Row(format!(
                            "thread name already exists in project: {}",
                            input.name
                        ))
                    } else {
                        error.into()
                    }
                })?;
                let thread = get_conn(conn, &id)
                    .await?
                    .ok_or_else(|| DbError::Row("created thread was not found".into()))?;
                // The session is part of the creation contract: composed into
                // this same transaction so the row and its session commit or
                // roll back together — a durable named thread with no job
                // would be deaf, and its taken name would make a retry fail.
                super::ensure_thread_session_conn(conn, &id, input.model.as_ref()).await?;
                Ok(thread)
            })
        })
        .await
        .map_err(CairnError::from)?;
    Ok(thread)
}

/// The one write that edits a thread: its name, jurisdiction, definition,
/// lifecycle status, and session model.
///
/// Both entry points land here — the desktop `update_thread` command and the
/// agent-facing `write cairn://p/<project>/<name>` patch — so closing and
/// reopening a thread has a single implementation and the two surfaces cannot
/// reach different end states.
///
/// Metadata, definition, and session establishment commit as one transaction.
/// That matters in both directions: a name collision rolls the definition write
/// back rather than leaving a thread describing an agent it never adopted, and a
/// definition that lands is guaranteed to have had its derived trigger index
/// rebuilt before this returns.
pub async fn update(db: &LocalDb, input: UpdateThread) -> Result<Thread, CairnError> {
    let current = get(db, &input.id)
        .await?
        .ok_or_else(|| CairnError::Internal("thread not found".into()))?;
    let name = input.name.unwrap_or(current.name);
    validate_thread_name(&name).map_err(CairnError::Internal)?;
    let jurisdiction = input.jurisdiction.unwrap_or(current.jurisdiction);
    let definition_written = input.definition.is_some();
    let definition = input.definition.unwrap_or(current.definition);
    if let Some(value) = definition.as_deref() {
        super::resolve_thread_definition(Some(value)).map_err(CairnError::Internal)?;
    }
    let status = input.status.unwrap_or(current.status);
    // Reopening is the moment routing comes back, so it is also the moment the
    // session and its derived trigger index have to exist again: a thread whose
    // definition changed while it was closed would otherwise stay deaf to its own
    // triggers until something else happened to prompt it.
    let reopening = status.is_active() && !current.status.is_active();
    let id = input.id;
    let model = input.model;
    db.write(|conn| {
        let name = name.clone();
        let jurisdiction = jurisdiction.clone();
        let definition = definition.clone();
        let id = id.clone();
        let model = model.clone();
        Box::pin(async move {
        conn.execute(
            "UPDATE threads SET name=?1, jurisdiction=?2, definition=?3, status=?4, updated_at=unixepoch() WHERE id=?5",
            params![name, jurisdiction, definition, status.as_str(), id.as_str()],
        ).await?;
        if status.is_active() && (definition_written || reopening) {
            super::ensure_thread_session_conn(conn, &id, None).await?;
        }
        if let Some(model) = model.as_ref() {
            set_session_model_conn(conn, &id, model).await?;
        }
        get_conn(conn, &id).await?.ok_or_else(|| DbError::Row("updated thread was not found".into()))
        })
    }).await.map_err(Into::into)
}

/// Re-point a thread's session at another model.
///
/// The model is written to the session job and nowhere else, which is exactly
/// where [`super::ensure_thread_session_conn`] writes an explicit model chosen
/// at creation. Keeping both paths on one column is what makes a thread's model
/// a single fact rather than two that can disagree.
///
/// The selection's backend is deliberately not written here. `sessions.backend`
/// describes the session that is *currently open* — including the native resume
/// handle bound to it — so overwriting it in place would leave the row claiming
/// a provider whose handle it does not hold. Instead the next turn compares the
/// session's backend against the one this model resolves to
/// ([`crate::backends::effective_backend_name`], the resolution the spawn itself
/// uses) and rotates onto a fresh session when they differ. A warm process whose
/// model no longer matches is restarted by the same path. The change is
/// therefore effective on the thread's next turn, and it is the turn — which
/// owns the continuity machinery — that performs the switch.
///
/// Storing the name alone is sound because the name outranks the Thread agent's
/// own default at that comparison and at the spawn: a thread's session job
/// re-resolves its agent every turn, and a default that has since moved to
/// another provider must not reinterpret the model stored here (CAIRN-3798). The
/// model menu offers only runtime-representable selections, so the name a thread
/// stores always carries its provider.
pub(crate) async fn set_session_model_conn(
    conn: &cairn_db::turso::Connection,
    thread_id: &str,
    model: &crate::models::ModelSelection,
) -> DbResult<()> {
    let updated = conn
        .execute(
            &format!(
                "UPDATE jobs SET model = ?1, updated_at = unixepoch()
                 WHERE id = (SELECT j.id FROM jobs j
                              WHERE j.thread_id = ?2 AND {}
                              ORDER BY j.created_at DESC, j.rowid DESC LIMIT 1)",
                super::SESSION_JOB_SHAPE
            ),
            params![model.model.as_str(), thread_id],
        )
        .await?;
    if updated == 0 {
        return Err(DbError::Row(format!(
            "thread has no session job to re-model: {thread_id}"
        )));
    }
    Ok(())
}

/// Delete the direct thread-owned rows explicitly because Turso does not execute
/// runtime ON DELETE cascades.
pub async fn delete(db: &LocalDb, id: &str) -> Result<(), CairnError> {
    let id = id.to_string();
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE issues SET parent_thread_id = NULL WHERE parent_thread_id = ?1",
                params![id.as_str()],
            )
            .await?;
            conn.execute(
                "DELETE FROM comments WHERE thread_id = ?1",
                params![id.as_str()],
            )
            .await?;
            conn.execute(
                "DELETE FROM messages WHERE channel_type = 'thread' AND channel_id = ?1",
                params![id.as_str()],
            )
            .await?;

            crate::jobs::cleanup::delete_owned_jobs(
                conn,
                crate::jobs::cleanup::JobOwner::Thread,
                &id,
            )
            .await?;
            conn.execute("DELETE FROM threads WHERE id = ?1", params![id.as_str()])
                .await?;
            Ok(())
        })
    })
    .await
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn db(name: &str) -> LocalDb {
        crate::storage::migrated_test_db(name).await
    }

    async fn seed_project(db: &LocalDb, id: &str) {
        db.execute_script(&format!(
            "INSERT OR IGNORE INTO projects
             (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('{id}', 'default', '{id}', '{id}', '/tmp/{id}', 1, 1);"
        ))
        .await
        .unwrap();
    }

    fn input(project_id: &str, name: &str) -> CreateThread {
        CreateThread {
            project_id: project_id.into(),
            name: name.into(),
            jurisdiction: Some("Own this topic".into()),
            definition: None,
            migrated_from_number: None,
            model: None,
        }
    }

    #[tokio::test]
    async fn create_rolls_back_the_thread_row_when_session_establishment_fails() {
        let db = db("thread-create-atomicity.db").await;
        seed_project(&db, "project-a").await;

        // Force the composed session INSERT to fail by hiding its table
        // (DDL needs the exclusive transaction path).
        db.exclusive(|conn| {
            Box::pin(async move {
                conn.execute("ALTER TABLE sessions RENAME TO sessions_hidden", ())
                    .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        let refused = create(&db, input("project-a", "roadmap")).await;
        assert!(refused.is_err(), "creation must fail without sessions");
        let leftover = db
            .query_one("SELECT COUNT(*) FROM threads", (), |row| row.i64(0))
            .await
            .unwrap();
        assert_eq!(leftover, 0, "a failed create must leave no thread row");

        // The name is free again: restore the table and the same create lands
        // thread, job, and session together.
        db.exclusive(|conn| {
            Box::pin(async move {
                conn.execute("ALTER TABLE sessions_hidden RENAME TO sessions", ())
                    .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        let thread = create(&db, input("project-a", "roadmap")).await.unwrap();
        let sessions = db
            .query_one(
                "SELECT COUNT(*) FROM sessions s JOIN jobs j ON j.id = s.job_id WHERE j.thread_id = ?1",
                params![thread.id],
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(sessions, 1, "retry creates thread and session together");
    }

    #[tokio::test]
    async fn crud_roundtrip_and_project_scoped_uniqueness() {
        let db = db("thread-crud-roundtrip.db").await;
        seed_project(&db, "project-a").await;
        seed_project(&db, "project-b").await;

        let created = create(&db, input("project-a", "roadmap")).await.unwrap();
        let first_job = super::super::ensure_thread_session(&db, &created.id)
            .await
            .unwrap();
        let second_job = super::super::ensure_thread_session(&db, &created.id)
            .await
            .unwrap();
        assert_eq!(first_job, second_job, "a live thread session is reused");
        let shape: (Option<String>, Option<String>, Option<String>, String, String) = db
            .query_one(
                "SELECT execution_id, issue_id, thread_id, node_name, agent_config_id FROM jobs WHERE id = ?1",
                params![first_job],
                |row| Ok((row.opt_text(0)?, row.opt_text(1)?, row.opt_text(2)?, row.text(3)?, row.text(4)?)),
            )
            .await
            .unwrap();
        assert_eq!(
            shape,
            (
                None,
                None,
                Some(created.id.clone()),
                "thread".into(),
                "thread".into()
            )
        );
        assert_eq!(get(&db, &created.id).await.unwrap(), Some(created.clone()));
        assert_eq!(list(&db, "project-a").await.unwrap(), vec![created]);
        assert!(create(&db, input("project-a", "roadmap")).await.is_err());
        assert!(create(&db, input("project-b", "roadmap")).await.is_ok());

        let mut migrated = input("project-a", "legacy-one");
        migrated.migrated_from_number = Some(42);
        create(&db, migrated).await.unwrap();
        let mut duplicate_migration = input("project-a", "legacy-two");
        duplicate_migration.migrated_from_number = Some(42);
        let error = create(&db, duplicate_migration).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("migration number already exists"));
    }

    /// Reopening is when routing comes back, so it is when the session and its
    /// derived trigger index are rebuilt. A definition edited while the thread
    /// was closed would otherwise leave a reopened thread deaf to its own
    /// triggers until something unrelated happened to prompt it.
    #[tokio::test]
    async fn reopening_rebuilds_the_derived_trigger_index_from_the_stored_definition() {
        let db = db("thread-reopen-triggers.db").await;
        seed_project(&db, "project-a").await;
        let thread = create(&db, input("project-a", "roadmap")).await.unwrap();

        let update =
            |definition: Option<&str>, status: Option<crate::models::ThreadStatus>| UpdateThread {
                id: thread.id.clone(),
                name: None,
                jurisdiction: None,
                definition: definition.map(|value| Some(value.to_string())),
                status,
                model: None,
            };
        async fn derived_count(db: &LocalDb) -> i64 {
            db.query_one(
                "SELECT COUNT(*) FROM wake_subscriptions WHERE id LIKE 'derived:thread:%'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap()
        }

        update_thread(&db, update(None, Some(crate::models::ThreadStatus::Closed))).await;

        let definition = serde_json::json!({
            "agent": "thread",
            "artifacts": ["arc"],
            "triggers": [{
                "fact": "attention",
                "detailUri": "cairn://p/PROJECT-A/9",
                "status": ["merged", "closed", "failed"]
            }]
        })
        .to_string();
        update_thread(&db, update(Some(&definition), None)).await;
        assert_eq!(
            derived_count(&db).await,
            0,
            "a closed thread establishes nothing, so it grows no standing routes"
        );

        update_thread(&db, update(None, Some(crate::models::ThreadStatus::Active))).await;
        assert_eq!(
            derived_count(&db).await,
            1,
            "reopening rebuilds the index from the definition written while closed"
        );
    }

    async fn update_thread(db: &LocalDb, input: UpdateThread) {
        update(db, input).await.unwrap();
    }

    #[tokio::test]
    async fn create_stamps_an_explicit_model_and_backend_on_the_session_job() {
        let db = db("thread-create-model.db").await;
        seed_project(&db, "project-a").await;
        let mut create_input = input("project-a", "roadmap");
        create_input.model = Some(crate::models::ModelSelection::new(
            "codex",
            "gpt-5.6-sol".into(),
        ));
        let thread = create(&db, create_input).await.unwrap();
        // Address the session job by its shape, not by "some job of this thread":
        // descendants carry the thread's id too, so an unfiltered match is only
        // unambiguous while the thread has never spawned anything.
        let stamped: (Option<String>, String) = db
            .query_one(
                &format!(
                    "SELECT j.model, s.backend FROM jobs j
                     JOIN sessions s ON s.job_id = j.id
                     WHERE j.thread_id = ?1 AND {}",
                    crate::threads::SESSION_JOB_SHAPE
                ),
                params![thread.id],
                |row| Ok((row.opt_text(0)?, row.text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(stamped, (Some("gpt-5.6-sol".into()), "codex".into()));
    }

    /// Changing a live thread's model lands on the same column creation writes,
    /// so a thread has one model fact rather than a creation-time one and a
    /// later one that can disagree. Every other field survives the change.
    #[tokio::test]
    async fn update_re_models_the_session_job_without_disturbing_the_thread() {
        let db = db("thread-update-model.db").await;
        seed_project(&db, "project-a").await;
        let thread = create(&db, input("project-a", "roadmap")).await.unwrap();
        // A task the session spawned carries the thread's id as well; the model
        // change must address the session by its shape, never "a job of this
        // thread".
        db.execute_script(&format!(
            "INSERT INTO jobs(id, thread_id, parent_job_id, project_id, node_name, uri_segment,
                              status, model, created_at, updated_at)
             VALUES ('task-1', '{}', (SELECT id FROM jobs WHERE thread_id = '{}' AND parent_job_id IS NULL),
                     'project-a', 'Survey', 'survey', 'complete', 'sonnet', 9, 9);",
            thread.id, thread.id
        ))
        .await
        .unwrap();

        let updated = update(
            &db,
            UpdateThread {
                id: thread.id.clone(),
                name: None,
                jurisdiction: None,
                definition: None,
                status: None,
                model: Some(crate::models::ModelSelection::new(
                    "codex",
                    "gpt-5.6-sol".into(),
                )),
            },
        )
        .await
        .unwrap();

        assert_eq!(updated.name, thread.name, "the thread row is untouched");
        assert_eq!(updated.jurisdiction, thread.jurisdiction);
        let session_model = db
            .query_one(
                &format!(
                    "SELECT j.model FROM jobs j WHERE j.thread_id = ?1 AND {}",
                    crate::threads::SESSION_JOB_SHAPE
                ),
                params![thread.id],
                |row| row.opt_text(0),
            )
            .await
            .unwrap();
        assert_eq!(session_model.as_deref(), Some("gpt-5.6-sol"));
        let task_model = db
            .query_one("SELECT model FROM jobs WHERE id = 'task-1'", (), |row| {
                row.opt_text(0)
            })
            .await
            .unwrap();
        assert_eq!(
            task_model.as_deref(),
            Some("sonnet"),
            "a spawned task keeps the model it ran with"
        );
    }

    /// Re-modelling across providers must leave the thread in a state the next
    /// turn resolves as a provider change — in BOTH directions.
    ///
    /// The move back off Codex is the one that used to go silently wrong: the
    /// session kept its Codex backend and its native thread id while the model
    /// said Claude, so the turn resumed a Codex handle on the Claude CLI. What
    /// makes the next turn rotate is exactly this pair disagreeing — the open
    /// session's backend versus the provider the stored model resolves to (see
    /// `lifecycle::tests::a_move_back_off_codex_rotates_too` for the decision
    /// itself).
    #[tokio::test]
    async fn a_cross_provider_re_model_leaves_the_session_due_for_rotation() {
        let db = db("thread-remodel-provider.db").await;
        seed_project(&db, "project-a").await;
        let mut create_input = input("project-a", "roadmap");
        create_input.model = Some(crate::models::ModelSelection::new(
            "codex",
            "gpt-5.6-sol".into(),
        ));
        let thread = create(&db, create_input).await.unwrap();

        async fn session_backend(db: &LocalDb, thread_id: &str) -> String {
            db.query_one(
                &format!(
                    "SELECT s.backend FROM sessions s
                     JOIN jobs j ON j.current_session_id = s.id
                     WHERE j.thread_id = ?1 AND {}",
                    crate::threads::SESSION_JOB_SHAPE
                ),
                params![thread_id],
                |row| row.text(0),
            )
            .await
            .unwrap()
        }
        async fn stored_model(db: &LocalDb, thread_id: &str) -> String {
            db.query_one(
                &format!(
                    "SELECT j.model FROM jobs j WHERE j.thread_id = ?1 AND {}",
                    crate::threads::SESSION_JOB_SHAPE
                ),
                params![thread_id],
                |row| row.opt_text(0),
            )
            .await
            .unwrap()
            .expect("a stored model")
        }

        assert_eq!(session_backend(&db, &thread.id).await, "codex");

        // Codex -> Claude: the session is still Codex, and the model now
        // resolves to Claude, so the next turn has a difference to act on.
        update(
            &db,
            UpdateThread {
                id: thread.id.clone(),
                name: None,
                jurisdiction: None,
                definition: None,
                status: None,
                model: Some(crate::models::ModelSelection::new("claude", "fable".into())),
            },
        )
        .await
        .unwrap();

        let backend = session_backend(&db, &thread.id).await;
        let resolved =
            crate::backends::resolved_backend_for_model(&stored_model(&db, &thread.id).await);
        assert_eq!(resolved, "claude");
        assert_ne!(
            backend, resolved,
            "the open Codex session no longer matches the model, so the next turn rotates"
        );

        // ...and back the other way, from a Claude-modelled thread.
        update(
            &db,
            UpdateThread {
                id: thread.id.clone(),
                name: None,
                jurisdiction: None,
                definition: None,
                status: None,
                model: Some(crate::models::ModelSelection::new(
                    "codex",
                    "gpt-5.6-sol".into(),
                )),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            crate::backends::resolved_backend_for_model(&stored_model(&db, &thread.id).await),
            "codex"
        );
    }

    #[tokio::test]
    async fn delete_explicitly_removes_direct_dependents() {
        let db = db("thread-crud-delete.db").await;
        seed_project(&db, "project-a").await;
        let thread = create(&db, input("project-a", "roadmap")).await.unwrap();

        db.execute_script(&format!(
            "INSERT INTO comments(id, thread_id, content, source, created_at)
             VALUES ('comment-1', '{}', 'hello', 'user', 1);
             INSERT INTO messages(id, channel_type, channel_id, sender_name, content, created_at)
             VALUES ('message-1', 'thread', '{}', 'system', 'hello', 1);
             INSERT INTO issues(id, project_id, number, title, parent_thread_id, created_at, updated_at)
             VALUES ('child-1', 'project-a', 1, 'Child', '{}', 1, 1);
             INSERT INTO jobs(id, thread_id, project_id, node_name, recipe_node_id, status, created_at, updated_at)
             VALUES ('job-1', '{}', 'project-a', 'thread', 'thread', 'pending', 1, 1);
             INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at, start_mode)
             VALUES ('run-1', 'project-a', 'job-1', 'live', 1, 1, 'resume');
             INSERT INTO sessions(id, job_id, created_at, updated_at)
             VALUES ('session-1', 'job-1', 1, 1);
             INSERT INTO turns(id, session_id, run_id, job_id, sequence, created_at, updated_at)
             VALUES ('turn-1', 'session-1', 'run-1', 'job-1', 1, 1, 1);
             INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at, turn_id)
             VALUES ('event-1', 'run-1', 1, 1, 'message', '{{}}', 1, 'turn-1');
             INSERT INTO thread_compactions(job_id, generation, source_session_id, recency_start_block, source_bytes, candidate_bytes, trigger, created_at)
             VALUES ('job-1', 1, 'session-1', 0, 10, 5, 'manual', 1);
             INSERT INTO thread_compaction_entries(job_id, generation, position, source_kind, overview, content_uri, start_block, end_block)
             VALUES ('job-1', 1, 1, 'interstitial', 'summary', 'cairn://p/P/1', 0, 1);
             UPDATE jobs SET current_turn_id = 'turn-1', resume_session_id = 'session-1' WHERE id = 'job-1';",
            thread.id, thread.id, thread.id, thread.id
        ))
        .await
        .unwrap();

        delete(&db, &thread.id).await.unwrap();
        for table in [
            "comments",
            "messages",
            "events",
            "turns",
            "sessions",
            "runs",
            "thread_compaction_entries",
            "thread_compactions",
            "jobs",
            "threads",
        ] {
            let count = db
                .query_one(&format!("SELECT COUNT(*) FROM {table}"), (), |row| {
                    row.i64(0)
                })
                .await
                .unwrap();
            assert_eq!(count, 0, "{table}");
        }
        let parent = db
            .query_one(
                "SELECT parent_thread_id FROM issues WHERE id = 'child-1'",
                (),
                |row| row.opt_text(0),
            )
            .await
            .unwrap();
        assert_eq!(parent, None);
    }
}
