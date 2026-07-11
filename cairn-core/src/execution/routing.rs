//! Fail-closed owning-database resolvers for the execution layer (CAIRN-2182).
//!
//! Execution writes are CONTEXT-RESOLVED: the call sites hold an opaque
//! `execution_id` / `job_id` / `run_id`, not a project key. Yet an execution row
//! and everything entangled with it — its jobs, runs, turns, sessions, and the
//! full streaming transcript — live WHOLLY in one database: the private DB for a
//! local project, the team-owned synced replica for a team project. These
//! resolvers find that owning database by probing every OPEN database for the row,
//! PRIVATE-FIRST (the same Class-2 pattern as [`crate::issues::crud::owning_db_for_issue`]
//! and [`crate::projects::crud::owning_db`]).
//!
//! The one — load-bearing — difference from those issue/project resolvers is the
//! failure mode. They are FAIL-OPEN: on a miss they fall back to the private DB.
//! These are FAIL-CLOSED: when no OPEN database carries the row they return
//! `Err` instead of silently targeting the private DB. A team execution whose
//! replica is not open must NOT have its writes land in the private database —
//! that split-brain is the silent-non-propagation class CAIRN-2170 already burned
//! us on. For a local execution the private DB is always open and always carries
//! the row, so resolution short-circuits there and routing is a strict no-op.
//!
//! INITIAL inserts (where the execution/job/run row does not exist yet) cannot use
//! the id-keyed resolvers — there is no row to probe. They resolve the owning
//! database by PROJECT id instead via [`owning_db_for_project`], then thread that
//! handle through the create sequence.

use std::sync::Arc;

use cairn_common::ids::{parse_route_scope, RouteScope};

use crate::db::DbState;
use crate::error::CairnError;
use crate::storage::LocalDb;

/// Route an entity id to its owning database by PARSING the id's intrinsic team
/// prefix (CAIRN-2210) — O(1), no probe. A bare id (every local entity) routes to
/// the private database; a `{team}~{uuid}` id routes to that team's open replica.
///
/// **Routing is not existence.** The old probe conflated them — one all-databases
/// scan both chose the database AND proved the row was there. They are now
/// separate concerns: this function only CHOOSES a database. A caller that needs
/// to know the row is present must check explicitly (e.g. assert
/// `rows_affected == 1` on a write). **Routing is not authorization** either — the
/// prefix is a forgeable hint; the authenticating sync broker is the access
/// boundary, unchanged.
///
/// Fail-closed: a `{team}~…` id whose replica is not open is an error, never a
/// silent private fallback (the CAIRN-2170 split-brain class). A `~`-bearing id
/// that does not parse as the canonical prefixed form is a distinct error.
pub async fn routing_db_for_id(dbs: &DbState, id: &str) -> Result<Arc<LocalDb>, CairnError> {
    match parse_route_scope(id) {
        Ok(RouteScope::Local) => Ok(dbs.local.clone()),
        Ok(RouteScope::Team(team)) => dbs.team_db(&team).await.ok_or_else(|| {
            // Observability (CAIRN-2210): a routed id whose team replica is not
            // open is the fail-closed split-brain guard firing — log it so a
            // closed/misconfigured replica is visible, not just an opaque error.
            log::warn!(
                "routing_db_for_id: closed_replica — id {id} names team {team} whose \
                 replica is not open; refusing the private-database fallback"
            );
            CairnError::Internal(format!(
                "fail-closed routing: id {id} names team {team} whose replica is not \
                 open; refusing to fall back to the private database"
            ))
        }),
        Err(err) => {
            // A `~`-bearing id that does not parse as `{team}~{uuid}` signals a
            // forged or corrupted id; log it distinctly from closed_replica.
            log::warn!("routing_db_for_id: invalid_id — cannot route id {id:?}: {err}");
            Err(CairnError::Internal(format!(
                "cannot route id {id:?}: {err}"
            )))
        }
    }
}

/// Probe one database for a single-column existence row. The remaining use of the
/// legacy all-databases probe ([`resolve`]) below.
async fn row_exists(db: &LocalDb, sql: &'static str, id: &str) -> Result<bool, CairnError> {
    let id = id.to_string();
    Ok(db.query_text(sql, (id,)).await?.is_some())
}

/// The LEGACY all-databases probe, retained for [`owning_db_for_parent_tool_use`]
/// and [`owning_db_for_session`]. Provider tool-use ids and bare session ids are
/// not routable entity ids and so cannot be prefix-parsed. Every route-prefixed
/// id resolver now uses [`routing_db_for_id`] instead.
/// Scans every open database PRIVATE-FIRST, fail-closed on a miss.
async fn resolve(
    dbs: &DbState,
    sql: &'static str,
    entity: &'static str,
    id: &str,
) -> Result<Arc<LocalDb>, CairnError> {
    for db in dbs.all_dbs().await {
        match row_exists(&db, sql, id).await {
            Ok(true) => return Ok(db),
            Ok(false) => {}
            Err(error) => {
                log::warn!("owning_db_for_{entity}: existence probe failed: {error}")
            }
        }
    }
    Err(CairnError::Internal(format!(
        "fail-closed routing: no open database owns {entity} {id} \
         (a team replica may not be open); refusing to fall back to the private database"
    )))
}

/// The database that owns this execution. Fail-closed (see module docs).
pub async fn owning_db_for_execution(
    dbs: &DbState,
    execution_id: &str,
) -> Result<Arc<LocalDb>, CairnError> {
    routing_db_for_id(dbs, execution_id).await
}

/// The database that owns this job. Fail-closed (see module docs).
pub async fn owning_db_for_job(dbs: &DbState, job_id: &str) -> Result<Arc<LocalDb>, CairnError> {
    routing_db_for_id(dbs, job_id).await
}

/// The database that owns this run. Fail-closed (see module docs).
pub async fn owning_db_for_run(dbs: &DbState, run_id: &str) -> Result<Arc<LocalDb>, CairnError> {
    routing_db_for_id(dbs, run_id).await
}

/// The database that owns this turn. Fail-closed (see module docs).
pub async fn owning_db_for_turn(dbs: &DbState, turn_id: &str) -> Result<Arc<LocalDb>, CairnError> {
    routing_db_for_id(dbs, turn_id).await
}

/// The database that owns this queued message. Fail-closed (see module docs).
/// A team job's user-message queue lives wholly in its synced replica, so an
/// edit/delete/delivery change keyed by the message id must land there; a
/// fail-open fallback to the private DB would silently no-op against an absent
/// row (the same split-brain class CAIRN-2170 burned us on).
pub async fn owning_db_for_queued_message(
    dbs: &DbState,
    message_id: &str,
) -> Result<Arc<LocalDb>, CairnError> {
    routing_db_for_id(dbs, message_id).await
}

/// The database that owns this action run. Fail-closed (see module docs).
pub async fn owning_db_for_action_run(
    dbs: &DbState,
    action_run_id: &str,
) -> Result<Arc<LocalDb>, CairnError> {
    routing_db_for_id(dbs, action_run_id).await
}

/// The database that owns the sub-agent child jobs spawned by one `batch_tasks`
/// call, identified by the `parent_tool_use_id` those children carry. The reader
/// behind the task-window UI keys on this column; its children were written to the
/// owning replica, so reading them back from the private DB would render a team
/// run's task windows empty. Fail-closed (see module docs) — for an actively
/// executing team run the replica is open and carries the children, so the scan
/// finds them; a closed replica errors rather than silently returning the private
/// DB's empty result.
pub async fn owning_db_for_parent_tool_use(
    dbs: &DbState,
    parent_tool_use_id: &str,
) -> Result<Arc<LocalDb>, CairnError> {
    resolve(
        dbs,
        "SELECT id FROM jobs WHERE parent_tool_use_id = ?1 LIMIT 1",
        "parent_tool_use",
        parent_tool_use_id,
    )
    .await
}

/// The database that owns this session, resolved by the session row or a
/// co-located `runs.session_id` link. Fail-closed (see module docs). This is the
/// SECOND sanctioned link-probe (alongside [`owning_db_for_parent_tool_use`]): a
/// session id is a `BareSessionId` by design (claude rejects a non-uuid
/// `--session-id`), so it is NOT a routable id and cannot be prefix-parsed.
///
/// During cold job start there is a short, valid window where `sessions` has been
/// inserted and `jobs.current_session_id` has been backfilled, but the first run
/// row has not been inserted yet. Session-keyed UI reads in that window must still
/// route to the database that owns the session instead of returning a fail-closed
/// 500. Once a run exists, the run link remains the transcript-home proof.
pub async fn owning_db_for_session(
    dbs: &DbState,
    session_id: &str,
) -> Result<Arc<LocalDb>, CairnError> {
    resolve(
        dbs,
        "SELECT id FROM sessions WHERE id = ?1 UNION SELECT id FROM runs WHERE session_id = ?1 LIMIT 1",
        "session",
        session_id,
    )
    .await
}

/// The database that owns this transcript event. Fail-closed (see module docs).
pub async fn owning_db_for_event(
    dbs: &DbState,
    event_id: &str,
) -> Result<Arc<LocalDb>, CairnError> {
    routing_db_for_id(dbs, event_id).await
}

/// The database that owns this permission request. Fail-closed (see module docs).
///
/// A team execution's `permission_requests` row lives WHOLLY in its synced
/// replica (FK'd to a replica-resident `runs` row). The response path that
/// records the answer, recomputes the issue status, and drives the resume must
/// target that database — reading the private DB returns no row, so the answer
/// errors `Permission request not found` and the run never resumes past the
/// approval gate. This is the WRITE counterpart to CAIRN-2225's read routing
/// (`owning_db_for_run`/`owning_db_for_event`); fail-closed for the same
/// CAIRN-2170 silent-non-propagation reason. For a local execution the private
/// DB always carries the row, so resolution short-circuits there (a strict
/// no-op).
pub async fn owning_db_for_permission_request(
    dbs: &DbState,
    request_id: &str,
) -> Result<Arc<LocalDb>, CairnError> {
    routing_db_for_id(dbs, request_id).await
}

/// The database that owns this ask_user prompt. Fail-closed (see module docs).
///
/// Same class as [`owning_db_for_permission_request`]: a team prompt's row lives
/// in the synced replica, so recording the answer — and the successor turn plus
/// issue-status recompute done in that same write — must land there, never
/// split-brained against the private DB.
pub async fn owning_db_for_prompt(
    dbs: &DbState,
    prompt_id: &str,
) -> Result<Arc<LocalDb>, CairnError> {
    routing_db_for_id(dbs, prompt_id).await
}

/// The database that owns a project, for INITIAL execution/job/run inserts where
/// no row id exists yet. Fail-closed: a team project whose replica is not open
/// errors rather than creating the execution in the private database.
pub async fn owning_db_for_project(
    dbs: &DbState,
    project_id: &str,
) -> Result<Arc<LocalDb>, CairnError> {
    routing_db_for_id(dbs, project_id).await
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use super::*;
    use crate::db::DbState;
    use std::sync::Arc;

    async fn db_state_with_team() -> (Arc<DbState>, Arc<LocalDb>) {
        use crate::storage::SearchIndex;
        let local = Arc::new(crate::storage::migrated_test_db("routing-local.db").await);
        let team = Arc::new(crate::storage::migrated_test_db("routing-team.db").await);
        let index =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let dbs = Arc::new(DbState::new(local, index));
        (dbs, team)
    }

    /// A `{team}~…` id whose replica is not open resolves to Err — the fail-closed
    /// contract — rather than falling back to the private DB. A bare (local) id
    /// always routes to the private DB.
    #[tokio::test(flavor = "current_thread")]
    async fn fail_closed_when_no_open_db_owns_the_row() {
        let (dbs, _team) = db_state_with_team().await;
        // The team replica is NOT injected, so a team-prefixed id fails closed
        // rather than silently resolving to the private DB.
        let exec = "team1~00000000-0000-4000-8000-000000000001";
        let proj = "team1~00000000-0000-4000-8000-000000000002";
        assert!(owning_db_for_execution(&dbs, exec).await.is_err());
        assert!(owning_db_for_project(&dbs, proj).await.is_err());
        // A bare id routes to the private DB (local entities are unchanged).
        let local = owning_db_for_execution(&dbs, "00000000-0000-4000-8000-000000000003")
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&local, &dbs.local));
    }

    /// With the replica open (injected under the id's team prefix), a prefixed id
    /// resolves to the team DB, never to the private DB. Routing no longer needs
    /// the row to exist — it parses the prefix and looks the team up.
    #[tokio::test(flavor = "current_thread")]
    async fn resolves_to_open_team_replica() {
        let (dbs, team) = db_state_with_team().await;
        dbs.insert_team_db_for_test("team1", team.clone()).await;
        let exec = "team1~00000000-0000-4000-8000-000000000001";
        let proj = "team1~00000000-0000-4000-8000-000000000002";
        let resolved = owning_db_for_execution(&dbs, exec).await.unwrap();
        assert!(Arc::ptr_eq(&resolved, &team));
        let resolved_proj = owning_db_for_project(&dbs, proj).await.unwrap();
        assert!(Arc::ptr_eq(&resolved_proj, &team));
    }

    /// The task-window reader keys on a child job's `parent_tool_use_id`. With the
    /// team replica open the resolver finds the owning DB; closed, it fails closed
    /// rather than reading the private DB's empty result.
    #[tokio::test(flavor = "current_thread")]
    async fn resolves_parent_tool_use_to_open_team_replica() {
        let (dbs, team) = db_state_with_team().await;
        team.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('p1', 'default', 'P', 'P', '/r', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, project_id, status, started_at, seq)
                     VALUES ('e1', 'r', 'p1', 'running', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, execution_id, agent_config_id, project_id, node_name, status, uri_segment, parent_tool_use_id, created_at, updated_at)
                     VALUES ('child-1', 'e1', 'a1', 'p1', 'task', 'running', 'task', 'tool-use-1', 1, 1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        // Replica closed: fail-closed.
        assert!(owning_db_for_parent_tool_use(&dbs, "tool-use-1")
            .await
            .is_err());

        // Replica open: resolves to the team DB, never the private DB.
        dbs.insert_team_db_for_test("team-1", team.clone()).await;
        let resolved = owning_db_for_parent_tool_use(&dbs, "tool-use-1")
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&resolved, &team));
    }

    /// CAIRN-2208: the backend transcript-write seam
    /// ([`crate::transcripts::stream_store::open_stream`] / `insert_event`) must
    /// target the run's OWNING database. A team run's `runs` row lives wholly in
    /// the synced replica, so the same write against the PRIVATE database trips
    /// the `message_streams`/`events` → `runs` foreign key — the live
    /// `FOREIGN KEY constraint failed` failure. Routing through
    /// [`owning_db_for_run`] lands the rows in the replica with none leaking to
    /// private. This locks the stream_store boundary that all three backends
    /// (claude/codex/openrouter) share; each backend's resolve-once wiring threads
    /// the same resolved handle into its reader/streaming loop.
    #[tokio::test(flavor = "current_thread")]
    async fn stream_store_writes_route_to_team_replica() {
        use crate::storage::RowExt;
        use crate::transcripts::stream_store::{insert_event, open_stream, EventInsert};

        let (dbs, team) = db_state_with_team().await;
        let run = "team1~00000000-0000-4000-8000-00000000000a";
        // Seed only the run row, in the team replica.
        {
            let run = run.to_string();
            team.write(move |conn| {
                let run = run.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO runs (id, status, session_id, created_at, updated_at)
                         VALUES (?1, 'live', NULL, 1, 1)",
                        (run,),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        }
        dbs.insert_team_db_for_test("team1", team.clone()).await;

        // The run resolves to the replica and is absent from the private DB.
        let owning = owning_db_for_run(&dbs, run).await.unwrap();
        assert!(Arc::ptr_eq(&owning, &team));
        let private = dbs.local.clone();
        assert!(
            private
                .query_text("SELECT id FROM runs WHERE id = ?1", (run.to_string(),))
                .await
                .unwrap()
                .is_none(),
            "the team run must NOT exist in the private DB"
        );

        // The live failure: open_stream against the PRIVATE DB trips the
        // message_streams → runs foreign key.
        assert!(
            open_stream(private.clone(), run, None, None, "claude", Some(0)).is_err(),
            "open_stream against the private DB must fail the FK (the live condition)"
        );

        // Against the resolved owning DB it succeeds; the row lands in the replica.
        open_stream(owning.clone(), run, None, None, "claude", Some(0)).unwrap();
        let count = |db: Arc<LocalDb>, sql: String| async move {
            db.query_one(&sql, (), |row| row.i64(0)).await.unwrap()
        };
        assert_eq!(
            count(
                team.clone(),
                format!("SELECT COUNT(*) FROM message_streams WHERE run_id = '{run}'")
            )
            .await,
            1
        );
        assert_eq!(
            count(
                private.clone(),
                format!("SELECT COUNT(*) FROM message_streams WHERE run_id = '{run}'")
            )
            .await,
            0,
            "no stream row may leak to the private DB"
        );

        // Same contract for a transcript event insert.
        let now = chrono::Utc::now().timestamp() as i32;
        let event = EventInsert {
            id: "evt-1".to_string(),
            run_id: run.to_string(),
            session_id: None,
            sequence: 1,
            timestamp: now,
            event_type: "assistant".to_string(),
            data: "{}".to_string(),
            parent_tool_use_id: None,
            created_at: now,
            input_tokens: None,
            cache_read_tokens: None,
            cache_create_tokens: None,
            output_tokens: None,
            thinking_tokens: None,
            turn_id: None,
            cost_usd: None,
        };
        assert!(
            insert_event(private.clone(), event.clone()).is_err(),
            "insert_event against the private DB must fail the FK"
        );
        assert!(insert_event(owning.clone(), event).unwrap());
        assert_eq!(
            count(
                team.clone(),
                format!("SELECT COUNT(*) FROM events WHERE run_id = '{run}'")
            )
            .await,
            1
        );
        assert_eq!(
            count(
                private,
                format!("SELECT COUNT(*) FROM events WHERE run_id = '{run}'")
            )
            .await,
            0,
            "no event row may leak to the private DB"
        );
    }

    /// CAIRN-2225: the session- and event-keyed read resolvers route to the
    /// run's owning database. A team run's `runs`/`events` rows live wholly in the
    /// replica, so a session-keyed transcript read (`runs.session_id`) and an
    /// event-keyed read must resolve there — closed, they fail closed rather than
    /// reading the private DB's empty result (the loading-dots root cause).
    #[tokio::test(flavor = "current_thread")]
    async fn resolves_session_and_event_to_open_team_replica() {
        let (dbs, team) = db_state_with_team().await;
        team.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO runs (id, status, session_id, created_at, updated_at)
                     VALUES ('run-team', 'live', 'sess-1', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO events (id, run_id, session_id, sequence, timestamp, event_type, data, created_at)
                     VALUES ('team1~00000000-0000-4000-8000-000000000001', 'run-team', 'sess-1', 0, 1, 'assistant', '{}', 1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        // session stays a bare link-probe; the event id is team-prefixed (parse).
        let event = "team1~00000000-0000-4000-8000-000000000001";
        // Replica closed: fail-closed on both keys (no private-DB fallback).
        assert!(owning_db_for_session(&dbs, "sess-1").await.is_err());
        assert!(owning_db_for_event(&dbs, event).await.is_err());

        // Replica open: both resolve to the team DB, never the private DB.
        dbs.insert_team_db_for_test("team1", team.clone()).await;
        assert!(Arc::ptr_eq(
            &owning_db_for_session(&dbs, "sess-1").await.unwrap(),
            &team
        ));
        assert!(Arc::ptr_eq(
            &owning_db_for_event(&dbs, event).await.unwrap(),
            &team
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolves_session_from_session_row_before_first_run_exists() {
        let (dbs, team) = db_state_with_team().await;
        team.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('p-session', 'default', 'P', 'P', '/r', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, project_id, status, started_at, seq)
                     VALUES ('e-session', 'r', 'p-session', 'running', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, execution_id, agent_config_id, project_id, node_name, status, uri_segment, created_at, updated_at)
                     VALUES ('j-session', 'e-session', 'a1', 'p-session', 'agent', 'running', 'agent', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO sessions (id, job_id, created_at, updated_at)
                     VALUES ('sess-pending-run', 'j-session', 1, 1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        assert!(owning_db_for_session(&dbs, "sess-pending-run")
            .await
            .is_err());
        dbs.insert_team_db_for_test("team1", team.clone()).await;
        assert!(Arc::ptr_eq(
            &owning_db_for_session(&dbs, "sess-pending-run")
                .await
                .unwrap(),
            &team
        ));
    }

    /// CAIRN-2227: the permission/prompt RESPONSE resolvers route to the run's
    /// owning database. A team execution's `permission_requests`/`prompts` rows
    /// live wholly in the replica, so the answer that records the response must
    /// resolve there — closed, they fail closed rather than reading the private
    /// DB's empty result (the `Permission request not found` / run-never-resumes
    /// root cause).
    #[tokio::test(flavor = "current_thread")]
    async fn resolves_permission_request_and_prompt_to_open_team_replica() {
        let (dbs, team) = db_state_with_team().await;
        team.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO runs (id, status, session_id, created_at, updated_at)
                     VALUES ('run-team', 'live', 'sess-1', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO permission_requests (id, run_id, tool_use_id, tool_name, tool_input, status, created_at)
                     VALUES ('team1~00000000-0000-4000-8000-000000000002', 'run-team', 'tu-1', 'bash', '{}', 'pending', 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO prompts (id, run_id, questions, created_at)
                     VALUES ('team1~00000000-0000-4000-8000-000000000003', 'run-team', '[]', 1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let perm = "team1~00000000-0000-4000-8000-000000000002";
        let prompt = "team1~00000000-0000-4000-8000-000000000003";
        // Replica closed: fail-closed on both keys (no private-DB fallback).
        assert!(owning_db_for_permission_request(&dbs, perm).await.is_err());
        assert!(owning_db_for_prompt(&dbs, prompt).await.is_err());

        // Replica open: both resolve to the team DB, never the private DB.
        dbs.insert_team_db_for_test("team1", team.clone()).await;
        assert!(Arc::ptr_eq(
            &owning_db_for_permission_request(&dbs, perm).await.unwrap(),
            &team
        ));
        assert!(Arc::ptr_eq(
            &owning_db_for_prompt(&dbs, prompt).await.unwrap(),
            &team
        ));
    }
}
