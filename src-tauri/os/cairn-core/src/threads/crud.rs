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

/// Update a thread and immediately enact any provider change implied by its
/// model. Host-facing edit surfaces use this wrapper so the database edit and
/// session lifecycle cannot drift apart.
pub async fn update_with_lifecycle(
    orch: &crate::orchestrator::Orchestrator,
    db: &LocalDb,
    input: UpdateThread,
) -> Result<ThreadUpdate, CairnError> {
    let selection = input.model.clone();
    let outcome = update(db, input).await?;
    if let Some(selection) = selection {
        let thread_id = outcome.thread.id.clone();
        let session_job_id = db
            .read(move |conn| {
                let thread_id = thread_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            &format!(
                                "SELECT j.id FROM jobs j
                                 WHERE j.thread_id = ?1 AND {}
                                 ORDER BY j.created_at DESC, j.rowid DESC LIMIT 1",
                                super::SESSION_JOB_SHAPE
                            ),
                            params![thread_id],
                        )
                        .await?;
                    rows.next().await?.map(|row| row.text(0)).transpose()
                })
            })
            .await?
            .ok_or_else(|| {
                CairnError::Internal("thread has no session job after model edit".into())
            })?;
        // Refresh the tracking host now rather than waiting for the next turn,
        // so jobs.model and the effective snapshot selection change together.
        // The snapshot is derived state a launch re-resolves regardless, so a
        // host that cannot be resolved is reported and stepped over rather than
        // failing the operator's edit.
        if let Err(error) =
            crate::execution::jobs::ensure_job_agent_snapshot(orch, &session_job_id).await
        {
            log::warn!("Failed to refresh a thread's host snapshot after a model edit: {error}");
        }
        crate::execution::provider_change::apply_for_job(
            orch,
            db,
            &session_job_id,
            &selection.backend,
            Some(selection.model.as_str()),
        )
        .await
        .map_err(CairnError::Internal)?;
    }
    Ok(outcome)
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
    .map_err(CairnError::from)
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
    .map_err(CairnError::from)
}

/// The shape the server allocates when a create arrives with no name.
///
/// A placeholder is an address, not a description. The `-` keeps it clear of
/// the purely-numeric refusal in [`validate_thread_name`], and `thread` is not
/// a reserved project segment (`threads` is), so an allocated name is routable
/// from the moment it is minted.
const PLACEHOLDER_PREFIX: &str = "thread-";

/// Spend the project's next thread address and return it.
///
/// The counter is durable (`projects.next_thread_number`) rather than derived
/// from the names currently in the table, and that is the whole point. Renaming
/// off the placeholder is this feature's designed path, not an edge case: a
/// derived maximum would see `thread-1` go free within a minute of being handed
/// out and reissue it, so a route or a history entry captured at creation would
/// later open an unrelated conversation. An address that has been handed out
/// stays spent. This is `projects.next_issue_number`'s mechanism and reasoning,
/// reused rather than re-invented.
///
/// A committed allocation is spent even if the thread is later renamed or
/// deleted; a rolled-back one is not, because the counter moves in the caller's
/// transaction alongside the row it names.
///
/// The scan is a guard, not the source: an explicit create may take `thread-7`
/// by name at any time, and stepping over what is actually present keeps that
/// from turning into a hard collision later.
async fn allocate_placeholder_name(
    conn: &cairn_db::turso::Connection,
    project_id: &str,
    now: i64,
) -> DbResult<String> {
    let mut rows = conn
        .query(
            "SELECT next_thread_number FROM projects WHERE id = ?1",
            params![project_id],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Row(format!("project not found: {project_id}")))?;
    let mut next = row.opt_i64(0)?.unwrap_or(1).max(1);

    let mut rows = conn
        .query(
            "SELECT name FROM threads WHERE project_id = ?1 AND name GLOB 'thread-[0-9]*'",
            params![project_id],
        )
        .await?;
    while let Some(row) = rows.next().await? {
        let name = row.text(0)?;
        // Only the exact `thread-<digits>` shape is a placeholder, so a thread
        // deliberately named `thread-safety` never moves the counter.
        let suffix = match name.strip_prefix(PLACEHOLDER_PREFIX) {
            Some(suffix) if suffix.bytes().all(|byte| byte.is_ascii_digit()) => suffix,
            _ => continue,
        };
        if let Ok(taken) = suffix.parse::<i64>() {
            next = next.max(taken + 1);
        }
    }

    conn.execute(
        "UPDATE projects SET next_thread_number = ?1, updated_at = ?2 WHERE id = ?3",
        params![next + 1, now, project_id],
    )
    .await?;
    Ok(format!("{PLACEHOLDER_PREFIX}{next}"))
}

pub async fn create(db: &LocalDb, input: CreateThread) -> Result<Thread, CairnError> {
    if let Some(name) = input.name.as_deref() {
        validate_thread_name(name).map_err(CairnError::Internal)?;
    }
    if let Some(definition) = input.definition.as_deref() {
        super::resolve_thread_definition(Some(definition)).map_err(CairnError::Internal)?;
    }

    let id = ids::mint_child(&input.project_id);
    let now = chrono::Utc::now().timestamp();
    // A thread's arc collections are app-owned SQL, so its database is part of
    // birth rather than lazy user state. Install it before establishing the
    // first session; if the primary transaction fails, remove the unowned file.
    let store_path = super::db_store::initialize_for_creation(&id, db.path())
        .await
        .map_err(CairnError::Internal)?;
    let created = db
        .write(|conn| {
            let input = input.clone();
            let id = id.clone();
            Box::pin(async move {
                // Resolving the name inside the transaction is what makes a
                // placeholder durable rather than optimistic: the counter moves,
                // the row is inserted, and its session is established together,
                // or none of it happened.
                let allocated = input.name.is_none();
                let name = match input.name.clone() {
                    Some(name) => name,
                    None => allocate_placeholder_name(conn, &input.project_id, now).await?,
                };
                conn.execute(
                    "INSERT INTO threads
                 (id, project_id, name, jurisdiction, definition,
                  migrated_from_number, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
                    params![
                        id.as_str(),
                        input.project_id,
                        name.as_str(),
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
                    } else if message.contains("unique") && allocated {
                        // The scan above and the unique index disagreed, which
                        // no ordinary retry produces. Say so distinctly rather
                        // than reporting a name the caller never chose, and
                        // never paper over it with a random suffix.
                        DbError::Row(format!(
                            "allocated thread placeholder collided with an existing name: {name}"
                        ))
                    } else if message.contains("unique") {
                        DbError::Row(format!("thread name already exists in project: {name}"))
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
        .await;
    match created {
        Ok(thread) => Ok(thread),
        Err(error) => {
            if let Err(cleanup_error) = super::db_store::remove_path(&store_path).await {
                tracing::error!(thread_id = %id, %cleanup_error, "failed to remove database for rolled-back thread creation");
            }
            Err(CairnError::from(error))
        }
    }
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
#[derive(Debug, Clone)]
pub struct ThreadUpdate {
    pub thread: Thread,
    pub previous: Thread,
}

pub async fn update(db: &LocalDb, input: UpdateThread) -> Result<ThreadUpdate, CairnError> {
    if let Some(name) = input.name.as_deref() {
        validate_thread_name(name).map_err(CairnError::Internal)?;
    }
    let definition_written = input.definition.is_some();
    if let Some(value) = input.definition.as_ref().and_then(|value| value.as_deref()) {
        super::resolve_thread_definition(Some(value)).map_err(CairnError::Internal)?;
    }
    let id = input.id;
    let input_name = input.name;
    let input_jurisdiction = input.jurisdiction;
    let input_definition = input.definition;
    let input_status = input.status;
    // `model` is applied by the provider-change transition, not here: it must
    // commit with the session swap it has to agree with.
    let _ = &input.model;
    db.write(|conn| {
        let id = id.clone();
        let input_name = input_name.clone();
        let input_jurisdiction = input_jurisdiction.clone();
        let input_definition = input_definition.clone();
        Box::pin(async move {
            // The predecessor and update share one serialized write transaction.
            // Rename notifications use this exact row as their old address, so a
            // read outside the transaction could make overlapping renames both
            // claim the same stale predecessor and strand name-addressed panes.
            let previous = get_conn(conn, &id)
                .await?
                .ok_or_else(|| DbError::Row("thread not found".into()))?;
            let name = input_name.unwrap_or_else(|| previous.name.clone());
            let jurisdiction =
                input_jurisdiction.unwrap_or_else(|| previous.jurisdiction.clone());
            let definition = input_definition.unwrap_or_else(|| previous.definition.clone());
            let status = input_status.unwrap_or(previous.status);
            // Closing is dormancy, not deletion: the session can reopen, so its
            // durable thread database deliberately survives this transition.
            let reopening = status.is_active() && !previous.status.is_active();

            conn.execute(
                "UPDATE threads SET name=?1, jurisdiction=?2, definition=?3, status=?4, updated_at=unixepoch() WHERE id=?5",
                params![name, jurisdiction, definition, status.as_str(), id.as_str()],
            ).await?;
            if status.is_active() && (definition_written || reopening) {
                super::ensure_thread_session_conn(conn, &id, None).await?;
            }
            let thread = get_conn(conn, &id)
                .await?
                .ok_or_else(|| DbError::Row("updated thread was not found".into()))?;
            Ok(ThreadUpdate { thread, previous })
        })
    })
    .await
    .map_err(CairnError::from)
}

/// Delete the direct thread-owned rows explicitly because Turso does not execute
/// runtime ON DELETE cascades.
///
/// `emitter` announces the tenets the deletion removed; the caller announces
/// the thread itself.
pub async fn delete(
    db: &LocalDb,
    emitter: &dyn crate::services::EventEmitter,
    id: &str,
) -> Result<(), CairnError> {
    let id = id.to_string();
    let store_id = id.clone();
    let released_tenets = db
        .write(|conn| {
            let id = id.clone();
            Box::pin(async move {
                // First, so a thread whose tenets carry verdicts is refused before
                // anything else it owns is touched.
                let released_tenets = crate::tenets::release_deleted_thread_conn(conn, &id).await?;
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
                conn.execute(
                    "DELETE FROM wake_conditions WHERE home_kind = 'thread' AND home_id = ?1",
                    params![id.as_str()],
                )
                .await?;
                conn.execute(
                    "DELETE FROM wake_schedules WHERE home_kind = 'thread' AND home_id = ?1",
                    params![id.as_str()],
                )
                .await?;
                conn.execute(
                    "DELETE FROM thread_automations WHERE thread_id = ?1",
                    params![id.as_str()],
                )
                .await?;
                conn.execute("DELETE FROM threads WHERE id = ?1", params![id.as_str()])
                    .await?;
                Ok(released_tenets)
            })
        })
        .await
        .map_err(CairnError::from)?;
    released_tenets.announce(emitter);
    if let Err(error) = super::db_store::remove(&store_id).await {
        tracing::error!(thread_id = %store_id, %error, "failed to remove deleted thread database");
    }
    Ok(())
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
            name: Some(name.into()),
            ..unnamed(project_id)
        }
    }

    /// A create as the launch surface sends it: an opening and a model, and no
    /// claim about what the topic is called.
    fn unnamed(project_id: &str) -> CreateThread {
        CreateThread {
            project_id: project_id.into(),
            name: None,
            jurisdiction: Some("Own this topic".into()),
            definition: None,
            migrated_from_number: None,
            model: None,
        }
    }

    #[tokio::test]
    async fn creation_mints_current_arc_tables_before_any_database_resource_access() {
        let db = db("thread-eager-database.db").await;
        seed_project(&db, "project-a").await;

        let thread = create(&db, input("project-a", "roadmap")).await.unwrap();
        let path = db
            .path()
            .parent()
            .unwrap()
            .join("thread-databases")
            .join(format!("{}.db", thread.id));
        assert!(path.is_file(), "thread creation must mint its database");

        let store = crate::threads::db_store::open_path(path).await.unwrap();
        let shape = store
            .query_one(
                "SELECT
                   (SELECT COUNT(*) FROM sqlite_schema WHERE name = '_cairn_rulings'),
                   (SELECT COUNT(*) FROM _cairn_open_questions),
                   (SELECT COUNT(*) FROM cairn_schema_migrations WHERE version IN ('0001', '0002'))",
                (),
                |row| Ok((row.i64(0)?, row.i64(1)?, row.i64(2)?)),
            )
            .await
            .unwrap();
        // A fresh store passes the import guard: 0002 runs on an empty table.
        assert_eq!(shape, (0, 0, 2));
    }

    /// An absent name is the server's cue to allocate, and allocation counts
    /// within one project only — a `thread-1` in another project is a different
    /// address, not a taken one.
    #[tokio::test]
    async fn an_absent_name_allocates_a_project_scoped_placeholder() {
        let db = db("thread-placeholder-allocation.db").await;
        seed_project(&db, "project-a").await;
        seed_project(&db, "project-b").await;

        let first = create(&db, unnamed("project-a")).await.unwrap();
        let second = create(&db, unnamed("project-a")).await.unwrap();
        assert_eq!(
            (first.name.as_str(), second.name.as_str()),
            ("thread-1", "thread-2")
        );
        assert_eq!(
            create(&db, unnamed("project-b")).await.unwrap().name,
            "thread-1"
        );

        // An explicit name is still taken verbatim, still validated, and still
        // collides with itself.
        assert_eq!(
            create(&db, input("project-a", "roadmap"))
                .await
                .unwrap()
                .name,
            "roadmap"
        );
        assert!(create(&db, input("project-a", "Roadmap")).await.is_err());
        let taken = create(&db, input("project-a", "roadmap"))
            .await
            .unwrap_err();
        assert!(taken.to_string().contains("already exists"), "{taken}");
    }

    /// Allocation is max-plus-one over the exact `thread-<n>` shape. A high
    /// suffix already in the project advances it, and names that merely start
    /// with `thread-` are somebody's chosen name rather than counters.
    #[tokio::test]
    async fn allocation_advances_past_the_highest_placeholder_and_ignores_lookalikes() {
        let db = db("thread-placeholder-max.db").await;
        seed_project(&db, "project-a").await;

        create(&db, input("project-a", "thread-41")).await.unwrap();
        create(&db, input("project-a", "thread-safety"))
            .await
            .unwrap();
        create(&db, input("project-a", "thread-9-notes"))
            .await
            .unwrap();
        assert_eq!(
            create(&db, unnamed("project-a")).await.unwrap().name,
            "thread-42"
        );
    }

    /// An address stays spent after the thread holding it stops using it.
    ///
    /// Renaming is this feature's designed path rather than an edge case — every
    /// thread leaves its placeholder inside its first turn — so an allocator
    /// reading only live names would put `thread-1` back in circulation within a
    /// minute of handing it out, and the route captured at creation would later
    /// open somebody else's conversation.
    #[tokio::test]
    async fn a_vacated_placeholder_is_never_reissued() {
        let db = db("thread-placeholder-durability.db").await;
        seed_project(&db, "project-a").await;

        let renamed = create(&db, unnamed("project-a")).await.unwrap();
        assert_eq!(renamed.name, "thread-1");
        update(
            &db,
            UpdateThread {
                id: renamed.id.clone(),
                name: Some("placement".into()),
                jurisdiction: None,
                definition: None,
                status: None,
                model: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            create(&db, unnamed("project-a")).await.unwrap().name,
            "thread-2",
            "renaming off thread-1 does not return that address to the pool"
        );

        let deleted = create(&db, unnamed("project-a")).await.unwrap();
        assert_eq!(deleted.name, "thread-3");
        delete(
            &db,
            &crate::services::testing::CapturingEmitter::new(),
            &deleted.id,
        )
        .await
        .unwrap();
        assert_eq!(
            create(&db, unnamed("project-a")).await.unwrap().name,
            "thread-4",
            "deleting the newest thread does not reissue its address either"
        );
    }

    /// Many launches at once must produce many distinct addresses. Reading the
    /// counter, bumping it, and inserting share one transaction, so a loser
    /// re-runs the whole decision instead of committing a name a winner already
    /// took — and each thread still gets exactly the one session creation
    /// promises.
    #[tokio::test]
    async fn concurrent_unnamed_creates_allocate_a_contiguous_unique_run() {
        let db = std::sync::Arc::new(db("thread-placeholder-concurrent.db").await);
        seed_project(&db, "project-a").await;

        const LAUNCHES: usize = 8;
        let mut handles = Vec::new();
        for _ in 0..LAUNCHES {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                create(&db, unnamed("project-a")).await
            }));
        }
        let mut names = Vec::new();
        for handle in handles {
            names.push(
                handle
                    .await
                    .unwrap()
                    .expect("every launch gets an address")
                    .name,
            );
        }
        names.sort();
        names.dedup();
        let mut expected: Vec<String> = (1..=LAUNCHES).map(|n| format!("thread-{n}")).collect();
        expected.sort();
        assert_eq!(names, expected, "one contiguous run of unique placeholders");

        let sessions = db
            .query_one(
                &format!(
                    "SELECT COUNT(*) FROM sessions s JOIN jobs j ON j.id = s.job_id
                     WHERE j.thread_id IS NOT NULL AND {}",
                    crate::threads::SESSION_JOB_SHAPE
                ),
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(
            sessions, LAUNCHES as i64,
            "each allocated thread has one session"
        );
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

    /// An allocated name is spent only if the whole creation commits. A session
    /// that fails takes the suffix down with the row, so the retry is not left
    /// stepping over a number nothing holds.
    #[tokio::test]
    async fn a_rolled_back_create_does_not_consume_the_allocated_suffix() {
        let db = db("thread-placeholder-rollback.db").await;
        seed_project(&db, "project-a").await;

        db.exclusive(|conn| {
            Box::pin(async move {
                conn.execute("ALTER TABLE sessions RENAME TO sessions_hidden", ())
                    .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        assert!(create(&db, unnamed("project-a")).await.is_err());
        db.exclusive(|conn| {
            Box::pin(async move {
                conn.execute("ALTER TABLE sessions_hidden RENAME TO sessions", ())
                    .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        assert_eq!(
            create(&db, unnamed("project-a")).await.unwrap().name,
            "thread-1",
            "the uncommitted suffix is still free"
        );
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
                "detailUri": "cairn://p/project-a/9",
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
    async fn update_leaves_the_session_job_model_to_the_provider_change_transition() {
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

        let session_model_sql = format!(
            "SELECT j.model FROM jobs j WHERE j.thread_id = ?1 AND {}",
            crate::threads::SESSION_JOB_SHAPE
        );
        let model_before = db
            .query_one(&session_model_sql, params![thread.id.clone()], |row| {
                row.opt_text(0)
            })
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

        assert_eq!(
            updated.thread.name, thread.name,
            "the thread row is untouched"
        );
        assert_eq!(updated.thread.jurisdiction, thread.jurisdiction);
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
        assert_eq!(
            session_model, model_before,
            "the model belongs to the provider-change transition, which commits it \
             with the session swap it has to agree with; writing it here too would \
             let it survive a rotation that failed"
        );
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
             VALUES ('job-1', 1, 1, 'interstitial', 'summary', 'cairn://p/p/1', 0, 1);
             UPDATE jobs SET current_turn_id = 'turn-1', resume_session_id = 'session-1' WHERE id = 'job-1';",
            thread.id, thread.id, thread.id, thread.id
        ))
        .await
        .unwrap();

        delete(
            &db,
            &crate::services::testing::CapturingEmitter::new(),
            &thread.id,
        )
        .await
        .unwrap();
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
