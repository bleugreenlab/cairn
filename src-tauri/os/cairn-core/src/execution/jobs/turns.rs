use super::*;

#[derive(Clone)]
pub(super) struct TurnHead {
    id: String,
    state: TurnState,
}

#[cfg(test)]
mod codex_goal_admission_tests {
    use super::*;
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{migrated_test_db, SearchIndex};

    fn test_orchestrator(db: Arc<LocalDb>) -> Orchestrator {
        let root = tempfile::tempdir().unwrap().keep();
        let state = Arc::new(DbState::new(
            db,
            Arc::new(SearchIndex::open_or_create(root.join("search.db")).unwrap()),
        ));
        Orchestrator::builder(state, Arc::new(TestServicesBuilder::new().build()), root).build()
    }

    async fn seeded_db() -> Arc<LocalDb> {
        let db = Arc::new(migrated_test_db("codex-goal-admission").await);
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO jobs(id,project_id,status,current_session_id,created_at,updated_at) VALUES('j','p','running','s',1,1);
             INSERT INTO sessions(id,job_id,backend,status,created_at,updated_at) VALUES('s','j','codex','open',1,1);
             INSERT INTO runs(id,project_id,job_id,status,session_id,created_at,updated_at) VALUES('r','p','j','live','s',1,1);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES('t0','s','r','j',1,'running','initial',1,1);",
        ).await.unwrap();
        db
    }

    async fn event_count(db: &LocalDb, event_type: &str) -> i64 {
        let event_type = event_type.to_string();
        db.read(|conn| {
            let event_type = event_type.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT COUNT(*) FROM events WHERE event_type=?1",
                        (event_type.as_str(),),
                    )
                    .await?;
                rows.next().await?.unwrap().i64(0)
            })
        })
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn provider_turn_started_admits_the_active_launch_turn() {
        let db = seeded_db().await;
        let orch = test_orchestrator(db.clone());

        assert_eq!(
            admit_codex_goal_turn(
                &orch,
                db.clone(),
                "r",
                "s",
                "j",
                "provider-launch-turn",
                Some("t0"),
            )
            .unwrap(),
            CodexGoalTurnAdmission::Admitted("t0".to_string())
        );
        assert_eq!(
            event_count(&db, "system:codex_goal_turn_started").await,
            1,
            "the provider notification binds to the launch turn instead of entering continuation admission"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn provider_goal_turn_racing_stop_is_deferred_not_crashed() {
        let db = seeded_db().await;
        db.execute(
            "UPDATE turns SET state='interrupted', end_reason='user_stop' WHERE id='t0'",
            (),
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db.clone());

        assert_eq!(
            admit_codex_goal_turn(
                &orch,
                db.clone(),
                "r",
                "s",
                "j",
                "provider-raced-stop",
                Some("t0"),
            )
            .unwrap(),
            CodexGoalTurnAdmission::Deferred {
                predecessor_turn_id: "t0".to_string()
            }
        );
        assert_eq!(event_count(&db, "system:codex_goal_turn_started").await, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn provider_goal_turn_after_normal_completion_mints_successor() {
        let db = seeded_db().await;
        db.execute("UPDATE turns SET state='complete' WHERE id='t0'", ())
            .await
            .unwrap();
        let orch = test_orchestrator(db.clone());

        let admission = admit_codex_goal_turn(
            &orch,
            db.clone(),
            "r",
            "s",
            "j",
            "provider-after-complete",
            Some("t0"),
        )
        .unwrap();
        let CodexGoalTurnAdmission::Admitted(successor_id) = admission else {
            panic!("ordinary completion must admit the provider successor");
        };
        assert_ne!(successor_id, "t0");
        assert_eq!(event_count(&db, "system:codex_goal_turn_started").await, 1);
        assert_eq!(
            db.query_opt_text(
                "SELECT predecessor_id FROM turns WHERE id=?1",
                (successor_id,),
            )
            .await
            .unwrap()
            .as_deref(),
            Some("t0")
        );
    }
}

async fn claim_retry_turn_start_conn(
    conn: &cairn_db::turso::Connection,
    turn_id: &str,
    run_id: &str,
) -> DbResult<bool> {
    let now = chrono::Utc::now().timestamp() as i32;
    let changed = conn
        .execute(
            "UPDATE turns
                SET state = 'running', run_id = ?1,
                    started_at = ?2, updated_at = ?2
              WHERE id = ?3
                AND state = 'pending'
                AND start_reason = 'retry'
                AND job_id IS NOT NULL
                AND id = (SELECT current_turn_id FROM jobs WHERE id = turns.job_id)",
            params![run_id, now, turn_id],
        )
        .await?;
    Ok(changed > 0)
}

/// Atomically reserve an automatic retry turn for this run.
///
/// Manual continuation can only reclassify a pending retry. Once this conditional
/// transition wins, the turn is running and the user-facing path queues steering
/// rather than launching the same turn concurrently.
pub(crate) fn claim_retry_turn_start(
    orch: &Orchestrator,
    turn_id: &str,
    run_id: &str,
) -> Result<bool, String> {
    let (db, claimed) = run_db({
        let dbs = orch.db.clone();
        let turn_id = turn_id.to_string();
        let run_id = run_id.to_string();
        async move {
            let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|error| error.to_string())?;
            let claimed = db
                .write(|conn| {
                    let turn_id = turn_id.clone();
                    let run_id = run_id.clone();
                    Box::pin(
                        async move { claim_retry_turn_start_conn(conn, &turn_id, &run_id).await },
                    )
                })
                .await
                .map_err(|error| error.to_string())?;
            Ok((db, claimed))
        }
    })?;
    if claimed {
        let change = run_db({
            let db = db.clone();
            let turn_id = turn_id.to_string();
            async move { Ok(crate::notify::turn_db_change_for_id(&db, &turn_id, "update").await) }
        })?;
        let _ = orch.services.emitter.emit("db-change", change);
    }

    Ok(claimed)
}

async fn supersede_pending_retry_conn(
    conn: &cairn_db::turso::Connection,
    turn_id: &str,
) -> DbResult<bool> {
    let now = chrono::Utc::now().timestamp() as i32;
    let changed = conn
        .execute(
            "UPDATE turns
                SET start_reason = 'follow_up', updated_at = ?1
              WHERE id = ?2 AND state = 'pending' AND start_reason = 'retry'",
            params![now, turn_id],
        )
        .await?;
    Ok(changed > 0)
}

#[cfg(test)]
mod capacity_retry_tests {
    use super::*;
    use crate::storage::migrated_test_db;

    async fn seed_job(db: &LocalDb) {
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','prj','/tmp/p',1,1)",
            "INSERT INTO jobs (id, project_id, status, current_session_id, created_at, updated_at) VALUES ('j','p','running','s',1,1)",
            "INSERT INTO sessions (id, job_id, backend, status, created_at, updated_at) VALUES ('s','j','codex','open',1,1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
    }

    async fn insert_turn(
        db: &LocalDb,
        id: &str,
        predecessor: Option<&str>,
        reason: TurnStartReason,
        state: TurnState,
    ) {
        let id = id.to_string();
        let predecessor = predecessor.map(str::to_string);
        db.write(|conn| {
            let id = id.clone();
            let predecessor = predecessor.clone();
            let reason = reason.clone();
            let state = state.clone();
            Box::pin(async move {
                create_turn_conn(conn, &id, "s", "j", predecessor.as_deref(), reason).await?;
                conn.execute(
                    "UPDATE turns SET state = ?1, ended_at = 2, updated_at = 2 WHERE id = ?2",
                    params![state.to_string(), id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn consecutive_retries_count_and_manual_turn_resets_budget() {
        let db = migrated_test_db("capacity-retry-count").await;
        seed_job(&db).await;
        insert_turn(
            &db,
            "t0",
            None,
            TurnStartReason::Initial,
            TurnState::Interrupted,
        )
        .await;
        insert_turn(
            &db,
            "t1",
            Some("t0"),
            TurnStartReason::Retry,
            TurnState::Interrupted,
        )
        .await;
        insert_turn(
            &db,
            "t2",
            Some("t1"),
            TurnStartReason::Retry,
            TurnState::Interrupted,
        )
        .await;
        insert_turn(
            &db,
            "t3",
            Some("t2"),
            TurnStartReason::Retry,
            TurnState::Interrupted,
        )
        .await;

        let count = db
            .read(|conn| {
                Box::pin(async move { consecutive_retry_turn_count_conn(conn, "t3").await })
            })
            .await
            .unwrap();
        assert_eq!(count, 3);

        insert_turn(
            &db,
            "t4",
            Some("t3"),
            TurnStartReason::FollowUp,
            TurnState::Interrupted,
        )
        .await;
        let reset = db
            .read(|conn| {
                Box::pin(async move { consecutive_retry_turn_count_conn(conn, "t4").await })
            })
            .await
            .unwrap();
        assert_eq!(reset, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn retry_claim_is_single_and_fails_closed_when_head_changes() {
        let db = migrated_test_db("capacity-retry-claim").await;
        seed_job(&db).await;
        insert_turn(
            &db,
            "t0",
            None,
            TurnStartReason::Initial,
            TurnState::Interrupted,
        )
        .await;

        let first = db
            .write(|conn| {
                Box::pin(async move {
                    claim_retry_successor_if_head_matches_conn(conn, "j", "s", "t0").await
                })
            })
            .await
            .unwrap();
        assert!(first.is_some());
        let second = db
            .write(|conn| {
                Box::pin(async move {
                    claim_retry_successor_if_head_matches_conn(conn, "j", "s", "t0").await
                })
            })
            .await
            .unwrap();
        assert_eq!(second, None);

        let retry_id = first.unwrap();
        let actual = db
            .read(|conn| {
                let retry_id = retry_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT predecessor_id, state, start_reason FROM turns WHERE id = ?1",
                            (retry_id.as_str(),),
                        )
                        .await?;
                    let row = rows.next().await?.expect("retry row");
                    Ok((row.text(0)?, row.text(1)?, row.text(2)?))
                })
            })
            .await
            .unwrap();
        assert_eq!(
            actual,
            ("t0".to_string(), "pending".to_string(), "retry".to_string())
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn manual_continue_reclassifies_pending_retry_and_resets_budget() {
        let db = migrated_test_db("capacity-retry-manual-reset").await;
        seed_job(&db).await;
        insert_turn(
            &db,
            "t0",
            None,
            TurnStartReason::Initial,
            TurnState::Interrupted,
        )
        .await;
        let retry_id = db
            .write(|conn| {
                Box::pin(async move {
                    claim_retry_successor_if_head_matches_conn(conn, "j", "s", "t0").await
                })
            })
            .await
            .unwrap()
            .unwrap();
        let retry_for_update = retry_id.clone();
        assert!(db
            .write(|conn| {
                let retry_id = retry_for_update.clone();
                Box::pin(async move { supersede_pending_retry_conn(conn, &retry_id).await })
            })
            .await
            .unwrap());

        let (reason, count) = db
            .read(|conn| {
                let retry_id = retry_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT start_reason FROM turns WHERE id = ?1",
                            (retry_id.as_str(),),
                        )
                        .await?;
                    let reason = rows.next().await?.expect("retry row").text(0)?;
                    let count = consecutive_retry_turn_count_conn(conn, &retry_id).await?;
                    Ok((reason, count))
                })
            })
            .await
            .unwrap();
        assert_eq!(reason, "follow_up");
        assert_eq!(count, 0);
        assert!(!pending_retry_head_matches(Arc::new(db), "j", &retry_id).unwrap());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn manual_supersession_after_retry_read_prevents_automatic_start() {
        let db = migrated_test_db("capacity-retry-reservation-race").await;
        seed_job(&db).await;
        db.execute(
            "INSERT INTO runs (id, job_id, project_id, status, created_at, updated_at)
             VALUES ('r-auto', 'j', 'p', 'starting', 1, 1)",
            (),
        )
        .await
        .unwrap();
        insert_turn(
            &db,
            "t0",
            None,
            TurnStartReason::Initial,
            TurnState::Interrupted,
        )
        .await;
        let retry_id = db
            .write(|conn| {
                Box::pin(async move {
                    claim_retry_successor_if_head_matches_conn(conn, "j", "s", "t0").await
                })
            })
            .await
            .unwrap()
            .unwrap();

        let retry_was_pending = db
            .read(|conn| {
                let retry_id = retry_id.clone();
                Box::pin(async move {
                    let Some(head) = get_head_turn_conn(conn, "j").await? else {
                        return Ok(false);
                    };
                    Ok(head.id == retry_id && head.state == TurnState::Pending)
                })
            })
            .await
            .unwrap();
        assert!(retry_was_pending);
        let manual_id = retry_id.clone();
        assert!(db
            .write(|conn| {
                let retry_id = manual_id.clone();
                Box::pin(async move { supersede_pending_retry_conn(conn, &retry_id).await })
            })
            .await
            .unwrap());
        let automatic_id = retry_id.clone();
        assert!(!db
            .write(|conn| {
                let retry_id = automatic_id.clone();
                Box::pin(
                    async move { claim_retry_turn_start_conn(conn, &retry_id, "r-auto").await },
                )
            })
            .await
            .unwrap());

        db.execute(
            "UPDATE turns SET state = 'pending', start_reason = 'retry', run_id = NULL WHERE id = ?1",
            (retry_id.as_str(),),
        )
        .await
        .unwrap();
        let automatic_id = retry_id.clone();
        assert!(db
            .write(|conn| {
                let retry_id = automatic_id.clone();
                Box::pin(
                    async move { claim_retry_turn_start_conn(conn, &retry_id, "r-auto").await },
                )
            })
            .await
            .unwrap());
        let manual_id = retry_id.clone();
        assert!(!db
            .write(|conn| {
                let retry_id = manual_id.clone();
                Box::pin(async move { supersede_pending_retry_conn(conn, &retry_id).await })
            })
            .await
            .unwrap());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reserved_retry_can_be_abandoned_to_interrupted() {
        let db = migrated_test_db("capacity-retry-abandon-running").await;
        seed_job(&db).await;
        db.execute(
            "INSERT INTO runs (id, job_id, project_id, status, created_at, updated_at)
             VALUES ('r-auto', 'j', 'p', 'starting', 1, 1)",
            (),
        )
        .await
        .unwrap();
        insert_turn(
            &db,
            "t0",
            None,
            TurnStartReason::Initial,
            TurnState::Interrupted,
        )
        .await;
        let retry_id = db
            .write(|conn| {
                Box::pin(async move {
                    claim_retry_successor_if_head_matches_conn(conn, "j", "s", "t0").await
                })
            })
            .await
            .unwrap()
            .unwrap();
        let reserved_id = retry_id.clone();
        assert!(db
            .write(|conn| {
                let retry_id = reserved_id.clone();
                Box::pin(
                    async move { claim_retry_turn_start_conn(conn, &retry_id, "r-auto").await },
                )
            })
            .await
            .unwrap());
        let retry_arc = Arc::new(db);
        assert!(abandon_pending_retry_if_head_matches(retry_arc.clone(), "j", &retry_id,).unwrap());
        let state = retry_arc
            .read(|conn| {
                let retry_id = retry_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT state FROM turns WHERE id = ?1",
                            (retry_id.as_str(),),
                        )
                        .await?;
                    rows.next().await?.expect("retry row").text(0)
                })
            })
            .await
            .unwrap();
        assert_eq!(state, "interrupted");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn non_terminal_head_cannot_be_claimed() {
        let db = migrated_test_db("capacity-retry-nonterminal").await;
        seed_job(&db).await;
        insert_turn(
            &db,
            "t0",
            None,
            TurnStartReason::Initial,
            TurnState::Running,
        )
        .await;
        let claimed = db
            .write(|conn| {
                Box::pin(async move {
                    claim_retry_successor_if_head_matches_conn(conn, "j", "s", "t0").await
                })
            })
            .await
            .unwrap();
        assert_eq!(claimed, None);
    }
}

async fn claim_retry_successor_if_head_matches_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    session_id: &str,
    expected_head_turn_id: &str,
) -> DbResult<Option<String>> {
    let Some(head) = get_head_turn_conn(conn, job_id).await? else {
        return Ok(None);
    };
    if head.id != expected_head_turn_id || !head.state.is_terminal() {
        return Ok(None);
    }
    let mut active_rows = conn
        .query(
            "SELECT 1 FROM turns WHERE job_id = ?1 AND state IN ('pending', 'running') LIMIT 1",
            (job_id,),
        )
        .await?;
    if active_rows.next().await?.is_some() {
        return Ok(None);
    }
    let mut successor_rows = conn
        .query(
            "SELECT 1 FROM turns WHERE predecessor_id = ?1 LIMIT 1",
            (expected_head_turn_id,),
        )
        .await?;
    if successor_rows.next().await?.is_some() {
        return Ok(None);
    }
    let retry_turn_id = ids::mint_child(job_id);
    create_successor_turn_conn(
        conn,
        &retry_turn_id,
        session_id,
        job_id,
        expected_head_turn_id,
        TurnStartReason::Retry,
    )
    .await?;
    Ok(Some(retry_turn_id))
}

async fn consecutive_retry_turn_count_conn(
    conn: &cairn_db::turso::Connection,
    head_turn_id: &str,
) -> DbResult<u32> {
    let mut count = 0;
    let mut turn_id = Some(head_turn_id.to_string());
    while let Some(id) = turn_id {
        let mut rows = conn
            .query(
                "SELECT start_reason, predecessor_id FROM turns WHERE id = ?1",
                (id.as_str(),),
            )
            .await?;
        let Some(row) = rows.next().await? else {
            break;
        };
        if row.text(0)? != TurnStartReason::Retry.to_string() {
            break;
        }
        count += 1;
        turn_id = row.opt_text(1)?;
    }
    Ok(count)
}

pub(crate) fn consecutive_retry_turn_count(
    db: Arc<LocalDb>,
    head_turn_id: &str,
) -> Result<u32, String> {
    run_db({
        let head_turn_id = head_turn_id.to_string();
        async move {
            db.read(|conn| {
                let head_turn_id = head_turn_id.clone();
                Box::pin(
                    async move { consecutive_retry_turn_count_conn(conn, &head_turn_id).await },
                )
            })
            .await
            .map_err(|error| error.to_string())
        }
    })
}

/// Atomically claims one retry successor for the expected terminal job head.
///
/// The predecessor identity is the idempotency boundary: if a user continuation
/// or another retry claimant has already changed the head, this returns no work.
pub(crate) fn claim_retry_successor_if_head_matches(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    job_id: &str,
    session_id: &str,
    expected_head_turn_id: &str,
) -> Result<Option<String>, String> {
    let claimed = run_db({
        let job_id = job_id.to_string();
        let session_id = session_id.to_string();
        let expected_head_turn_id = expected_head_turn_id.to_string();
        let db = db.clone();
        async move {
            db.write(|conn| {
                let job_id = job_id.clone();
                let session_id = session_id.clone();
                let expected_head_turn_id = expected_head_turn_id.clone();
                Box::pin(async move {
                    claim_retry_successor_if_head_matches_conn(
                        conn,
                        &job_id,
                        &session_id,
                        &expected_head_turn_id,
                    )
                    .await
                })
            })
            .await
            .map_err(|error| error.to_string())
        }
    })?;
    if let Some(turn_id) = claimed.as_deref() {
        let change = run_db({
            let db = db.clone();
            let turn_id = turn_id.to_string();
            async move { Ok(crate::notify::turn_db_change_for_id(&db, &turn_id, "insert").await) }
        })?;
        let _ = orch.services.emitter.emit("db-change", change);
    }
    Ok(claimed)
}

pub(crate) fn pending_retry_head_matches(
    db: Arc<LocalDb>,
    job_id: &str,
    retry_turn_id: &str,
) -> Result<bool, String> {
    run_db({
        let job_id = job_id.to_string();
        let retry_turn_id = retry_turn_id.to_string();
        async move {
            db.read(|conn| {
                let job_id = job_id.clone();
                let retry_turn_id = retry_turn_id.clone();
                Box::pin(async move {
                    let Some(head) = get_head_turn_conn(conn, &job_id).await? else {
                        return Ok(false);
                    };
                    if head.id != retry_turn_id
                        || !matches!(head.state, TurnState::Pending | TurnState::Running)
                    {
                        return Ok(false);
                    }
                    let mut rows = conn
                        .query(
                            "SELECT start_reason FROM turns WHERE id = ?1",
                            (retry_turn_id.as_str(),),
                        )
                        .await?;
                    Ok(rows
                        .next()
                        .await?
                        .map(|row| row.text(0))
                        .transpose()?
                        .as_deref()
                        == Some("retry"))
                })
            })
            .await
            .map_err(|error| error.to_string())
        }
    })
}

pub(crate) fn abandon_pending_retry_if_head_matches(
    db: Arc<LocalDb>,
    job_id: &str,
    retry_turn_id: &str,
) -> Result<bool, String> {
    run_db({
        let job_id = job_id.to_string();
        let retry_turn_id = retry_turn_id.to_string();
        async move {
            db.write(|conn| {
                let job_id = job_id.clone();
                let retry_turn_id = retry_turn_id.clone();
                Box::pin(async move {
                    let Some(head) = get_head_turn_conn(conn, &job_id).await? else {
                        return Ok(false);
                    };
                    if head.id != retry_turn_id
                        || !matches!(head.state, TurnState::Pending | TurnState::Running)
                    {
                        return Ok(false);
                    }
                    let now = chrono::Utc::now().timestamp() as i32;
                    let changed = conn
                        .execute(
                            "UPDATE turns
                                SET state = 'interrupted', ended_at = ?1, updated_at = ?1
                              WHERE id = ?2
                                AND state IN ('pending', 'running')
                                AND start_reason = 'retry'",
                            params![now, retry_turn_id.as_str()],
                        )
                        .await?;
                    Ok(changed > 0)
                })
            })
            .await
            .map_err(|error| error.to_string())
        }
    })
}

async fn next_turn_sequence_conn(
    conn: &cairn_db::turso::Connection,
    session_id: &str,
) -> DbResult<i32> {
    let mut rows = conn
        .query(
            "SELECT MAX(sequence) FROM turns WHERE session_id = ?1",
            (session_id,),
        )
        .await?;
    let max_seq = rows
        .next()
        .await?
        .map(|row| row.opt_i64(0))
        .transpose()?
        .flatten();
    Ok(max_seq.map(|seq| seq as i32).unwrap_or(0) + 1)
}

async fn get_head_turn_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<Option<TurnHead>> {
    let mut rows = conn
        .query(
            "SELECT t.id, t.state
             FROM jobs j
             LEFT JOIN turns current ON current.id = j.current_turn_id
             JOIN turns t ON t.id = COALESCE(
                 current.id,
                 (SELECT fallback.id
                    FROM turns fallback
                   WHERE fallback.job_id = j.id
                   ORDER BY fallback.created_at DESC, fallback.sequence DESC
                   LIMIT 1)
             )
             WHERE j.id = ?1
             LIMIT 1",
            (job_id,),
        )
        .await?;
    rows.next()
        .await?
        .map(|row| {
            let state = row.text(1)?.parse().map_err(DbError::Row)?;
            Ok(TurnHead {
                id: row.text(0)?,
                state,
            })
        })
        .transpose()
}

/// Whether a live durable suspension owns `turn_id` on this job: a `pending` or
/// `resolving` `agent_waits` row that either parked this turn
/// (`predecessor_turn_id`) or has already claimed its continuation
/// (`successor_turn_id`).
///
/// Ownership is a fact about the wait, not about the turn's state. Parking a
/// turn YIELDS it and `yielded` is terminal, so a wait's window is exactly the
/// span in which the head reads as finished while its successor is still the
/// wait's to create. Anything deciding whether it may mint a successor must ask
/// this first (CAIRN-4721).
async fn durable_wait_owns_turn_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    turn_id: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT 1
               FROM agent_waits
              WHERE job_id = ?1
                AND state IN ('pending', 'resolving')
                AND (
                    predecessor_turn_id = ?2
                    OR successor_turn_id = ?2
                )
              LIMIT 1",
            params![job_id, turn_id],
        )
        .await?;
    Ok(rows.next().await?.is_some())
}

async fn create_turn_conn(
    conn: &cairn_db::turso::Connection,
    turn_id: &str,
    session_id: &str,
    job_id: &str,
    predecessor_id: Option<&str>,
    start_reason: TurnStartReason,
) -> DbResult<()> {
    let mut active_rows = conn
        .query(
            "SELECT COUNT(*) FROM turns
             WHERE job_id = ?1 AND (state = 'pending' OR state = 'running')",
            (job_id,),
        )
        .await?;
    let active_count = active_rows
        .next()
        .await?
        .map(|row| row.i64(0))
        .transpose()?
        .unwrap_or(0);
    if active_count > 0 {
        return Err(db_internal(format!(
            "Job {} already has an active turn (pending or running)",
            job_id
        )));
    }

    if let Some(pred_id) = predecessor_id {
        let mut successor_rows = conn
            .query(
                "SELECT COUNT(*) FROM turns WHERE predecessor_id = ?1",
                (pred_id,),
            )
            .await?;
        let successor_count = successor_rows
            .next()
            .await?
            .map(|row| row.i64(0))
            .transpose()?
            .unwrap_or(0);
        if successor_count > 0 {
            return Err(db_internal(format!(
                "Turn {} already has a successor",
                pred_id
            )));
        }
    }

    let now = chrono::Utc::now().timestamp() as i32;
    let sequence = next_turn_sequence_conn(conn, session_id).await?;
    let state = TurnState::Pending.to_string();
    let reason = start_reason.to_string();
    conn.execute(
        "INSERT INTO turns(
            id, session_id, run_id, job_id, sequence, predecessor_id,
            state, yield_reason, start_reason, created_at, started_at, ended_at, updated_at
        )
        VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, NULL, ?7, ?8, NULL, NULL, ?8)",
        params![
            turn_id,
            session_id,
            Some(job_id),
            sequence,
            predecessor_id,
            state.as_str(),
            reason.as_str(),
            now,
        ],
    )
    .await?;
    conn.execute(
        "UPDATE jobs SET current_turn_id = ?1 WHERE id = ?2",
        params![Some(turn_id), job_id],
    )
    .await?;
    Ok(())
}

pub(super) async fn create_initial_turn_db(
    db: Arc<LocalDb>,
    turn_id: String,
    session_id: String,
    job_id: String,
) -> Result<(), String> {
    db.write(|conn| {
        let turn_id = turn_id.clone();
        let session_id = session_id.clone();
        let job_id = job_id.clone();
        Box::pin(async move {
            create_turn_conn(
                conn,
                &turn_id,
                &session_id,
                &job_id,
                None,
                TurnStartReason::Initial,
            )
            .await
        })
    })
    .await
    .map_err(|e| e.to_string())
}

pub(super) fn create_initial_turn(
    orch: &Orchestrator,
    turn_id: &str,
    session_id: &str,
    job_id: &str,
) -> Result<(), String> {
    let db = run_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    run_db(create_initial_turn_db(
        db.clone(),
        turn_id.to_string(),
        session_id.to_string(),
        job_id.to_string(),
    ))?;
    let change = run_db({
        let db = db.clone();
        let turn_id = turn_id.to_string();
        async move { Ok(crate::notify::turn_db_change_for_id(&db, &turn_id, "insert").await) }
    })?;
    let _ = orch.services.emitter.emit("db-change", change);
    Ok(())
}

/// The start reason for a follow-up resume. A resume is the post-completion
/// MemoryReview turn only when it is the system-initiated reflection delivery:
/// the job's memory review is `sent` AND the resume carries no user-authored
/// content. A user follow-up — an explicit resume message, a prompt/permission
/// answer (both fold into `user_initiated`), or a still-pending queued message
/// the resume sweep is about to claim — is always a normal FollowUp work turn,
/// even while a review is `sent`.
///
/// This distinction is load-bearing: the job-status projection excludes
/// `memory_review` turns from the job's terminal outcome (it reads the latest
/// *work* turn). Tagging a real work turn `memory_review` therefore hides the
/// completed work, so a confirm-gated node can never derive Complete even after
/// its artifact is confirmed — freezing DAG advancement with no error
/// (CAIRN-2014). Tagging the genuine reflection turn here is what lets the
/// projection treat the review as post-completion housekeeping rather than the
/// job's latest work.
async fn followup_start_reason_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    user_initiated: bool,
) -> DbResult<TurnStartReason> {
    // User steering is always a work turn, never a reflection.
    if user_initiated {
        return Ok(TurnStartReason::FollowUp);
    }
    let mut rows = conn
        .query(
            "SELECT memory_review_state FROM jobs WHERE id = ?1",
            (job_id,),
        )
        .await?;
    let state = rows
        .next()
        .await?
        .and_then(|row| row.opt_text(0).ok().flatten());
    if state.as_deref() != Some("sent") {
        return Ok(TurnStartReason::FollowUp);
    }
    // No explicit resume message, but a pending queued user follow-up (delivered
    // by the resume sweep that runs just after turn creation) still makes this a
    // work turn — the user is steering, not reflecting.
    let mut queued = conn
        .query(
            "SELECT 1 FROM queued_messages WHERE job_id = ?1 AND delivered_at IS NULL LIMIT 1",
            (job_id,),
        )
        .await?;
    if queued.next().await?.is_some() {
        return Ok(TurnStartReason::FollowUp);
    }
    Ok(TurnStartReason::MemoryReview)
}

async fn create_successor_turn_conn(
    conn: &cairn_db::turso::Connection,
    turn_id: &str,
    session_id: &str,
    job_id: &str,
    predecessor_id: &str,
    start_reason: TurnStartReason,
) -> DbResult<()> {
    let mut rows = conn
        .query("SELECT state FROM turns WHERE id = ?1", (predecessor_id,))
        .await?;
    let pred_state = rows
        .next()
        .await?
        .map(|row| row.text(0))
        .transpose()?
        .ok_or_else(|| db_internal("Predecessor turn not found"))?;
    let pred_state: TurnState = pred_state
        .parse()
        .map_err(|e| db_internal(format!("Invalid predecessor state: {e}")))?;
    if !pred_state.is_terminal() {
        return Err(db_internal(format!(
            "Predecessor turn {} is in non-terminal state {:?}",
            predecessor_id, pred_state
        )));
    }

    create_turn_conn(
        conn,
        turn_id,
        session_id,
        job_id,
        Some(predecessor_id),
        start_reason,
    )
    .await
}

pub(crate) fn create_followup_turn(
    orch: &Orchestrator,
    session_id: &str,
    job_id: &str,
    user_initiated: bool,
    supersede_pending_retry: bool,
) -> Result<String, String> {
    let (db, turn_id, created) = run_db({
        let dbs = orch.db.clone();
        let session_id = session_id.to_string();
        let job_id = job_id.to_string();
        async move {
            let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())?;
            let (turn_id, created) = db
                .write(|conn| {
                    let session_id = session_id.clone();
                    let job_id = job_id.clone();
                    Box::pin(async move {
                        let head_turn = get_head_turn_conn(conn, &job_id).await?;
                        let start_reason =
                            followup_start_reason_conn(conn, &job_id, user_initiated).await?;
                        let new_turn_id = ids::mint_child(&job_id);
                        if let Some(head) = head_turn {
                            if head.state == TurnState::Pending {
                                if supersede_pending_retry {
                                    supersede_pending_retry_conn(conn, &head.id).await?;
                                }
                                return Ok((head.id, false));
                            }
                            match create_successor_turn_conn(
                                conn,
                                &new_turn_id,
                                &session_id,
                                &job_id,
                                &head.id,
                                start_reason.clone(),
                            )
                            .await
                            {
                                Ok(_) => Ok((new_turn_id, true)),
                                Err(error) => {
                                    log::warn!("Failed to create successor turn: {}", error);
                                    let fallback_id = ids::mint_child(&job_id);
                                    create_turn_conn(
                                        conn,
                                        &fallback_id,
                                        &session_id,
                                        &job_id,
                                        None,
                                        start_reason.clone(),
                                    )
                                    .await?;
                                    Ok((fallback_id, true))
                                }
                            }
                        } else {
                            create_turn_conn(
                                conn,
                                &new_turn_id,
                                &session_id,
                                &job_id,
                                None,
                                start_reason,
                            )
                            .await?;
                            Ok((new_turn_id, true))
                        }
                    })
                })
                .await
                .map_err(|e| e.to_string())?;
            Ok((db, turn_id, created))
        }
    })?;
    if created {
        let change = run_db({
            let db = db.clone();
            let turn_id = turn_id.clone();
            async move { Ok(crate::notify::turn_db_change_for_id(&db, &turn_id, "insert").await) }
        })?;
        let _ = orch.services.emitter.emit("db-change", change);
    }
    Ok(turn_id)
}

/// Durably bind one provider-created Codex turn to exactly one Cairn turn.
/// The event marker and successor start share a transaction, so notification
/// replay after a host restart returns the original turn instead of minting one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CodexGoalTurnAdmission {
    Admitted(String),
    Deferred { predecessor_turn_id: String },
}

pub(crate) fn admit_codex_goal_turn(
    orch: &Orchestrator,
    db: Arc<LocalDb>,
    run_id: &str,
    session_id: &str,
    job_id: &str,
    provider_turn_id: &str,
    current_turn_id: Option<&str>,
) -> Result<CodexGoalTurnAdmission, String> {
    let run_id = run_id.to_string();
    let session_id = session_id.to_string();
    let job_id = job_id.to_string();
    let provider_turn_id = provider_turn_id.to_string();
    let current_turn_id = current_turn_id.map(str::to_string);
    let marker_id = format!("codex-goal-turn:{provider_turn_id}");
    let turn_id = run_db({
        let db = db.clone();
        async move {
            db.write(|conn| {
                let run_id = run_id.clone();
                let session_id = session_id.clone();
                let job_id = job_id.clone();
                let provider_turn_id = provider_turn_id.clone();
                let current_turn_id = current_turn_id.clone();
                let marker_id = marker_id.clone();
                Box::pin(async move {
                    let mut existing = conn
                        .query(
                            "SELECT e.turn_id, e.run_id, e.session_id, t.job_id
                               FROM events e
                               LEFT JOIN turns t ON t.id = e.turn_id
                              WHERE e.id = ?1",
                            (marker_id.as_str(),),
                        )
                        .await?;
                    if let Some(row) = existing.next().await? {
                        let turn_id = row
                            .opt_text(0)?
                            .ok_or_else(|| db_internal("Codex goal turn marker has no Cairn turn"))?;
                        if row.text(1)? != run_id
                            || row.opt_text(2)?.as_deref() != Some(session_id.as_str())
                            || row.opt_text(3)?.as_deref() != Some(job_id.as_str())
                        {
                            return Err(db_internal(
                                "Codex provider turn identity is already bound to another Cairn run",
                            ));
                        }
                        return Ok(CodexGoalTurnAdmission::Admitted(turn_id));
                    }

                    let turn_id = if let Some(current_turn_id) = current_turn_id {
                        let mut rows = conn
                            .query(
                                "SELECT session_id, job_id, run_id, state, end_reason FROM turns WHERE id = ?1",
                                (current_turn_id.as_str(),),
                            )
                            .await?;
                        let row = rows
                            .next()
                            .await?
                            .ok_or_else(|| db_internal("active Cairn turn not found"))?;
                        if row.text(0)? != session_id
                            || row.opt_text(1)?.as_deref() != Some(job_id.as_str())
                            || row.opt_text(2)?.as_deref() != Some(run_id.as_str())
                        {
                            return Err(db_internal(
                                "Codex provider turn conflicts with active Cairn turn",
                            ));
                        }
                        let state = row.text(3)?;
                        if state != "running" {
                            if durable_wait_owns_turn_conn(conn, &job_id, &current_turn_id).await? {
                                return Ok(CodexGoalTurnAdmission::Deferred {
                                    predecessor_turn_id: current_turn_id,
                                });
                            }
                            let state: TurnState = state
                                .parse()
                                .map_err(|error: String| db_internal(error))?;
                            let end_reason = row.opt_text(4)?;
                            if state.is_terminal() && end_reason.as_deref() == Some("user_stop") {
                                // Stop terminalizes the Cairn turn before pausing and
                                // interrupting the provider under its stdin lock. A
                                // provider successor already in flight can therefore
                                // arrive while the process still points at that
                                // terminal turn. It belongs to the stopped goal, not
                                // to a new continuation: fence it exactly like a
                                // durable suspension instead of crashing the run.
                                return Ok(CodexGoalTurnAdmission::Deferred {
                                    predecessor_turn_id: current_turn_id,
                                });
                            }
                            if state.is_terminal() {
                                let new_turn_id = ids::mint_child(&job_id);
                                create_successor_turn_conn(
                                    conn,
                                    &new_turn_id,
                                    &session_id,
                                    &job_id,
                                    &current_turn_id,
                                    TurnStartReason::FollowUp,
                                )
                                .await?;
                                let now = chrono::Utc::now().timestamp() as i32;
                                conn.execute(
                                    "UPDATE turns SET state='running', run_id=?1, started_at=?2, updated_at=?2 WHERE id=?3 AND state='pending'",
                                    params![run_id.as_str(), now, new_turn_id.as_str()],
                                )
                                .await?;
                                new_turn_id
                            } else {
                                return Err(db_internal(
                                    "Codex provider turn conflicts with active Cairn turn",
                                ));
                            }
                        } else {
                            current_turn_id
                        }
                    } else {
                        let head = get_head_turn_conn(conn, &job_id)
                            .await?
                            .ok_or_else(|| db_internal("Codex goal continuation has no predecessor"))?;
                        // The wait comes before the state: a durable suspension
                        // parks its turn by yielding it, and `yielded` is terminal.
                        // Reading the state first let a provider goal notification
                        // mistake a parked head for a finished one and mint a
                        // successor competing with the `wait_resolved` turn the
                        // resolution creates, leaving the job with two.
                        if durable_wait_owns_turn_conn(conn, &job_id, &head.id).await? {
                            return Ok(CodexGoalTurnAdmission::Deferred {
                                predecessor_turn_id: head.id,
                            });
                        }
                        if !head.state.is_terminal() {
                            return Err(db_internal(format!(
                                "Codex goal continuation conflicts with non-terminal turn {} without durable suspension ownership",
                                head.id
                            )));
                        }
                        let new_turn_id = ids::mint_child(&job_id);
                        create_successor_turn_conn(
                            conn,
                            &new_turn_id,
                            &session_id,
                            &job_id,
                            &head.id,
                            TurnStartReason::FollowUp,
                        )
                        .await?;
                        let now = chrono::Utc::now().timestamp() as i32;
                        conn.execute(
                            "UPDATE turns SET state='running', run_id=?1, started_at=?2, updated_at=?2 WHERE id=?3 AND state='pending'",
                            params![run_id.as_str(), now, new_turn_id.as_str()],
                        )
                        .await?;
                        new_turn_id
                    };
                    let now = chrono::Utc::now().timestamp() as i32;
                    let mut sequences = conn
                        .query(
                            "SELECT COALESCE(MAX(sequence), 0) + 1 FROM events WHERE run_id = ?1",
                            (run_id.as_str(),),
                        )
                        .await?;
                    let sequence = sequences.next().await?.unwrap().i64(0)?;
                    conn.execute(
                        "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at, turn_id)
                         VALUES(?1, ?2, ?3, ?4, ?5, 'system:codex_goal_turn_started', ?6, ?5, ?7)",
                        params![
                            marker_id.as_str(), run_id.as_str(), session_id.as_str(), sequence,
                            now, serde_json::json!({"providerTurnId": provider_turn_id}).to_string(),
                            turn_id.as_str()
                        ],
                    )
                    .await?;
                    conn.execute(
                        "UPDATE jobs SET current_turn_id=?1, status='running', updated_at=?2 WHERE id=?3",
                        params![turn_id.as_str(), now, job_id.as_str()],
                    )
                    .await?;
                    Ok(CodexGoalTurnAdmission::Admitted(turn_id))
                })
            })
            .await
            .map_err(|error| error.to_string())
        }
    })?;
    let CodexGoalTurnAdmission::Admitted(admitted_turn_id) = &turn_id else {
        return Ok(turn_id);
    };
    let change = run_db({
        let db = db.clone();
        let turn_id = admitted_turn_id.clone();
        async move { Ok(crate::notify::turn_db_change_for_id(&db, &turn_id, "update").await) }
    })?;
    let _ = orch.services.emitter.emit("db-change", change);
    Ok(turn_id)
}

/// Flip a job's pending turn to `running` and wire it to `run_id`. Resolves the
/// run's owning database (fail-closed via `owning_db_for_run`) so a team job's
/// turn lands in its synced replica, then emits the scoped `turns` db-change.
///
/// This is the canonical turn-start the host job-start paths call. It replaced a
/// hand-rolled duplicate in the Tauri layer that wrote the `turns` UPDATE
/// unconditionally to the private DB, stranding a team job's turn (CAIRN-2206).
pub fn start_turn(orch: &Orchestrator, turn_id: &str, run_id: &str) -> Result<(), String> {
    let (db, _session_id, _job_id) = run_db({
        let dbs = orch.db.clone();
        let turn_id = turn_id.to_string();
        let run_id = run_id.to_string();
        async move {
            let db = crate::execution::routing::owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|e| e.to_string())?;
            let (session_id, job_id) = db
                .write(|conn| {
                    let turn_id = turn_id.clone();
                    let run_id = run_id.clone();
                    Box::pin(async move {
                        let mut rows = conn
                            .query(
                                "SELECT state, session_id, job_id FROM turns WHERE id = ?1",
                                (turn_id.as_str(),),
                            )
                            .await?;
                        let row = rows
                            .next()
                            .await?
                            .ok_or_else(|| db_internal("turn not found"))?;
                        let current = row.text(0)?;
                        let session_id = row.text(1)?;
                        let job_id = row.opt_text(2)?;
                        let from: TurnState = current.parse().map_err(|_| {
                            db_internal(format!("unparseable current state: {}", current))
                        })?;
                        if from != TurnState::Pending {
                            return Err(db_internal(format!(
                                "turn {} can only start from pending, got {}",
                                turn_id, from
                            )));
                        }
                        let now = chrono::Utc::now().timestamp() as i32;
                        conn.execute(
                            "UPDATE turns
                             SET state = 'running', run_id = ?1, started_at = ?2, updated_at = ?2
                             WHERE id = ?3",
                            params![run_id.as_str(), now, turn_id.as_str()],
                        )
                        .await?;
                        Ok((session_id, job_id))
                    })
                })
                .await
                .map_err(|e| e.to_string())?;
            Ok((db, session_id, job_id))
        }
    })?;
    let change = run_db({
        let db = db.clone();
        let turn_id = turn_id.to_string();
        async move { Ok(crate::notify::turn_db_change_for_id(&db, &turn_id, "update").await) }
    })?;
    let _ = orch.services.emitter.emit("db-change", change);
    Ok(())
}

fn valid_running_transition(from: &JobStatus) -> bool {
    matches!(
        from,
        JobStatus::Pending
            | JobStatus::Complete
            | JobStatus::Failed
            | JobStatus::Blocked
            | JobStatus::Idle
    )
}

pub(super) fn transition_job_to_running(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    let db = run_db({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let (execution_id, issue_id) = run_db({
        let db = db.clone();
        let job_id = job_id.to_string();
        async move {
            db.write(|conn| {
                let job_id = job_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT status, execution_id, issue_id FROM jobs WHERE id = ?1",
                            (job_id.as_str(),),
                        )
                        .await?;
                    let Some(row) = rows.next().await? else {
                        return Err(db_internal(format!("job not found: {job_id}")));
                    };
                    let current_str = row.text(0)?;
                    let execution_id = row.opt_text(1)?;
                    let issue_id = row.opt_text(2)?;
                    let from: JobStatus = current_str.parse().map_err(|_| {
                        db_internal(format!("unparseable current status: {}", current_str))
                    })?;
                    if from == JobStatus::Running {
                        return Ok((execution_id, issue_id));
                    }
                    if !valid_running_transition(&from) {
                        return Err(db_internal(format!(
                            "transition not allowed for job {job_id}: {from} -> running"
                        )));
                    }
                    let now = chrono::Utc::now().timestamp() as i32;
                    if from == JobStatus::Pending {
                        conn.execute(
                            "UPDATE jobs
                             SET status = 'running', started_at = ?1, updated_at = ?1
                             WHERE id = ?2",
                            params![now, job_id.as_str()],
                        )
                        .await?;
                    } else {
                        conn.execute(
                            "UPDATE jobs SET status = 'running', updated_at = ?1 WHERE id = ?2",
                            params![now, job_id.as_str()],
                        )
                        .await?;
                    }

                    let effective_issue_id = if let Some(exec_id) = execution_id.as_deref() {
                        conn.execute(
                            "UPDATE executions SET status = 'running' WHERE id = ?1",
                            (exec_id,),
                        )
                        .await?;
                        issue_id
                            .clone()
                            .or(load_execution_issue_id_conn(conn, exec_id).await?)
                    } else {
                        issue_id.clone()
                    };
                    if let Some(issue_id) = effective_issue_id.as_deref() {
                        crate::transitions::outcome::recompute_issue_status_conn(conn, issue_id)
                            .await?;
                    }
                    Ok((execution_id, effective_issue_id))
                })
            })
            .await
            .map_err(|e| e.to_string())
        }
    })?;
    // Reload the full job so the jobs db-change carries the complete scoping
    // set (issue/execution/parent/project ids), not just the partial query.
    let job = run_db({
        let db = db.clone();
        let job_id = job_id.to_string();
        async move {
            crate::jobs::queries::get_job(&db, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let _ = orch
        .services
        .emitter
        .emit("db-change", crate::notify::job_db_change(&job, "update"));
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({
            "table": "executions",
            "action": "update",
            "issueId": issue_id.as_deref(),
            "executionId": execution_id.as_deref(),
            "projectId": job.project_id,
        }),
    );
    if let Some(issue_id) = issue_id.as_deref() {
        let _ = orch.services.emitter.emit(
            "db-change",
            crate::notify::issue_db_change_ids("update", issue_id, Some(&job.project_id)),
        );
    }
    Ok(())
}

#[cfg(test)]
mod followup_start_reason_tests {
    use super::*;
    use crate::messages::queued::{enqueue_async, Delivery};
    use crate::storage::migrated_test_db;

    async fn insert_job(db: &LocalDb, job_id: &str, review_state: Option<&str>) {
        let job_id = job_id.to_string();
        let review_state = review_state.map(|s| s.to_string());
        db.write(|conn| {
            let job_id = job_id.clone();
            let review_state = review_state.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('p','w','P','prj','/tmp/prj',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, project_id, status, memory_review_state, created_at, updated_at)
                     VALUES (?1, 'p', 'running', ?2, 1, 1)",
                    params![job_id.as_str(), review_state.as_deref()],
                )
                .await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    async fn start_reason(db: &LocalDb, job_id: &str, user_initiated: bool) -> TurnStartReason {
        let job_id = job_id.to_string();
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move { followup_start_reason_conn(conn, &job_id, user_initiated).await })
        })
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn user_followup_while_review_sent_is_work_turn() {
        // CAIRN-2014: a user-initiated resume must be a FollowUp work turn even
        // while the memory review is `sent`. Tagging it `memory_review` would hide
        // the turn from the completion projection and strand a confirm-gated node.
        let db = migrated_test_db("turns-followup-user.db").await;
        insert_job(&db, "j-sent", Some("sent")).await;
        assert_eq!(
            start_reason(&db, "j-sent", true).await,
            TurnStartReason::FollowUp
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn system_resume_while_review_sent_is_memory_review() {
        // The genuine reflection delivery (no user content) is the MemoryReview turn.
        let db = migrated_test_db("turns-followup-system.db").await;
        insert_job(&db, "j-sent", Some("sent")).await;
        assert_eq!(
            start_reason(&db, "j-sent", false).await,
            TurnStartReason::MemoryReview
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pending_queued_message_makes_resume_a_work_turn() {
        // No explicit message, but a pending queued user follow-up (claimed by the
        // resume sweep) means the user is steering — a work turn, not a reflection.
        let db = migrated_test_db("turns-followup-queued.db").await;
        insert_job(&db, "j-sent", Some("sent")).await;
        enqueue_async(&db, "j-sent", "do more", Delivery::Queue)
            .await
            .unwrap();
        assert_eq!(
            start_reason(&db, "j-sent", false).await,
            TurnStartReason::FollowUp
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn no_review_pending_is_followup() {
        let db = migrated_test_db("turns-followup-none.db").await;
        insert_job(&db, "j-none", None).await;
        assert_eq!(
            start_reason(&db, "j-none", false).await,
            TurnStartReason::FollowUp
        );
    }
}
