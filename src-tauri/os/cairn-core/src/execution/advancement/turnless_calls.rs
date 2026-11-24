//! Recovery of packet-backed calls which have no turn-end edge to reduce them.

use crate::execution::jobs::liveness::{Liveness, WorkObserver};
use crate::models::JobStatus;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_attention::push::{push_fingerprinted_conn, Boundary, Wake};
use std::collections::BTreeMap;

/// Both restart re-derivation and the periodic sweep use this entry point.
pub(crate) async fn sweep_turnless_calls(orch: &Orchestrator) {
    let observer = match WorkObserver::read(orch).await {
        Ok(observer) => observer,
        Err(error) => {
            log::warn!("turnless call recovery could not observe work: {error}");
            return;
        }
    };
    for db in orch.db.all_dbs().await {
        match settle_calls(&db, &observer).await {
            Ok(settled) => {
                for children in settled.into_values() {
                    for (child, _) in &children {
                        let change =
                            cairn_db::changes::job_db_change_for_id(&db, child, "update").await;
                        let _ = orch.services.emitter.emit("db-change", change);
                    }
                    // The caller's parked-parent healer releases exact current
                    // dependency waits, grouped by packet origin and anchor.
                    orch.notifier.emit_change("attention_pushes");
                }
            }
            Err(error) => log::warn!("turnless call recovery: {error}"),
        }
    }
}

type Settled = BTreeMap<String, Vec<(String, JobStatus)>>;

/// Settlement and its one parent fact commit together. There is no timestamp
/// threshold: only positive evidence that the child's work is dead qualifies.
async fn settle_calls(db: &LocalDb, observer: &WorkObserver) -> DbResult<Settled> {
    let observer = observer.clone();
    db.write(move |conn| {
        let observer = observer.clone();
        Box::pin(async move {
            let mut rows = conn.query(
                "SELECT j.id, j.parent_job_id
                 FROM jobs j
                 WHERE j.recipe_node_id IS NULL AND j.parent_job_id IS NOT NULL
                   AND j.status IN ('running', 'pending', 'blocked')
                   AND NOT EXISTS (SELECT 1 FROM turns t WHERE t.job_id = j.id)
                 ORDER BY j.id",
                (),
            ).await?;
            let mut candidates = Vec::new();
            while let Some(row) = rows.next().await? {
                candidates.push((row.text(0)?, row.text(1)?));
            }
            drop(rows);
            let mut settled = Settled::new();
            for (job, parent) in candidates {
                if observer.job_conn(conn, &job).await? != Liveness::Dead {
                    continue;
                }
                let caller_terminal = {
                    let mut rows = conn.query("SELECT status FROM jobs WHERE id = ?1", (parent.as_str(),)).await?;
                    rows.next().await?
                        .map(|row| row.text(0))
                        .transpose()?
                        .is_some_and(|status| matches!(status.as_str(), "complete" | "failed" | "cancelled"))
                };
                let caller_gone = caller_terminal
                    || observer.job_conn(conn, &parent).await? == Liveness::Dead;
                if let Some(status) = cairn_workflow::advancement::recompute::
                    reduce_delegated_child_job_with_caller_gone_conn(conn, &job, caller_gone).await?
                {
                    settled.entry(parent).or_default().push((job, status));
                }
            }
            for (parent, children) in &settled {
                let fingerprint = children.iter().map(|(id, status)| format!("{id}:{status}"))
                    .collect::<Vec<_>>().join(",");
                let complete = children.iter().filter(|(_, status)| *status == JobStatus::Complete).count();
                let failed = children.len() - complete;
                let list = children.iter().map(|(id, status)| format!("- {id}: {status}"))
                    .collect::<Vec<_>>().join("\n");
                let content = format!(
                    "Recovered {} calls with no turn: {complete} complete, {failed} failed. \
                     Calls without a run failed because their caller ended before this call started.\n{list}",
                    children.len()
                );
                // The parked-parent healer owns dependency-wait wakes. This fact is
                // always a passive ride-along, never a second wake per child.
                push_fingerprinted_conn(conn, parent, &content, Wake::Passive, Boundary::Event,
                    &format!("call-recovery:{parent}"), Some(&fingerprint), true).await?;
            }
            Ok(settled)
        })
    }).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_db::turso::params;

    #[tokio::test]
    async fn recovery_settles_idle_parent_calls_once_and_preserves_owned_work() {
        let dir = tempfile::tempdir().unwrap();
        let db = std::sync::Arc::new(
            LocalDb::open_migrated_for_test(dir.path().join("calls.db"))
                .await
                .unwrap(),
        );
        let config = dir.path().join("config");
        std::fs::create_dir_all(config.join("agents")).unwrap();
        std::fs::create_dir_all(config.join("recipes")).unwrap();
        let search = std::sync::Arc::new(
            crate::storage::SearchIndex::open_or_create(dir.path().join("search")).unwrap(),
        );
        let services =
            std::sync::Arc::new(crate::services::testing::TestServicesBuilder::new().build());
        let orch = crate::orchestrator::OrchestratorBuilder::new(
            std::sync::Arc::new(crate::db::DbState::new(db.clone(), search)),
            services,
            config,
        )
        .build();
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','P','p','/tmp',1,1);
             INSERT INTO executions(id,recipe_id,project_id,status,started_at,seq,snapshot)
               VALUES('e','r','p','running',1,1,'{}');
             INSERT INTO jobs(id,project_id,execution_id,status,created_at,updated_at)
               VALUES('parent','p','e','idle',1,1),
                     ('unknown-parent','p','e','running',1,1),
                     ('busy-parent','p','e','running',1,1);
             INSERT INTO runs(id,job_id,status,created_at,updated_at)
               VALUES('parent-live','unknown-parent','live',1,1);",
        )
        .await
        .unwrap();
        for (job, parent, run, artifact, turn) in [
            ("failed", "parent", Some("crashed"), false, false),
            ("complete", "parent", Some("crashed"), true, false),
            ("unstarted-dead", "parent", None, false, false),
            ("unstarted-unknown", "unknown-parent", None, false, false),
            ("unstarted-busy", "busy-parent", None, false, false),
            ("launch-claimed", "parent", Some("crashed"), true, false),
            ("starting", "parent", Some("starting"), true, false),
            ("live", "parent", Some("live"), true, false),
            ("has-turn", "parent", Some("crashed"), true, true),
            ("process-owned", "parent", Some("crashed"), true, false),
        ] {
            let packet: crate::models::DelegatedWorkPacket =
                serde_json::from_value(serde_json::json!({
                    "id": format!("packet-{job}"), "parentJobId": parent,
                    "origin": "task_tool", "title": "Call", "agentConfigId": "Explore",
                    "ownership": {"cwd": "/tmp"}, "outputContract": {"schemaType": "return"},
                    "status": "running", "resultArtifactJobId": job, "createdAt": 1
                }))
                .unwrap();
            db.write(move |conn| {
                let packet = packet.clone();
                Box::pin(async move {
                    conn.execute("INSERT INTO jobs(id,project_id,execution_id,parent_job_id,status,created_at,updated_at)
                        VALUES(?1,'p','e',?2,'running',1,1)", (job, parent)).await?;
                    cairn_workflow::delegated_packets::insert(conn, "e", &packet).await?;
                    if let Some(status) = run {
                        conn.execute("INSERT INTO runs(id,job_id,status,created_at,updated_at)
                            VALUES(?1,?2,?3,1,1)", params![format!("run-{job}"), job, status]).await?;
                    }
                    if artifact {
                        conn.execute("INSERT INTO artifacts(id,job_id,artifact_type,output_name,data,version,confirmed,created_at,updated_at)
                            VALUES(?1,?2,'return','return','{}',1,1,1,1)", params![format!("artifact-{job}"), job]).await?;
                    }
                    if turn {
                        conn.execute("INSERT INTO turns(id,job_id,state,start_reason,sequence,created_at,updated_at,session_id)
                            VALUES('turn',?1,'interrupted','initial',1,1,1,?1)", (job,)).await?;
                    }
                    Ok(())
                })
            }).await.unwrap();
        }

        orch.process_state.processes.lock().unwrap().register(
            "run-process-owned".into(),
            crate::agent_process::process::RunHandle::test_handle(None, Some("process-owned")),
        );
        let observer = WorkObserver::launching(["busy-parent", "launch-claimed"])
            .with_processes(orch.process_state.clone());
        let settled = settle_calls(&db, &observer).await.unwrap();
        let mut outcomes = settled.get("parent").unwrap().clone();
        outcomes.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            outcomes,
            vec![
                ("complete".into(), JobStatus::Complete),
                ("failed".into(), JobStatus::Failed),
                ("unstarted-dead".into(), JobStatus::Failed),
            ]
        );
        assert_eq!(
            settled.len(),
            1,
            "only the idle parent owns recovered calls"
        );
        db.read(|conn| {
            Box::pin(async move {
                for (job, expected_job, expected_packet) in [
                    (
                        "complete",
                        "complete",
                        crate::models::DelegatedStatus::Completed,
                    ),
                    ("failed", "failed", crate::models::DelegatedStatus::Failed),
                    (
                        "unstarted-dead",
                        "failed",
                        crate::models::DelegatedStatus::Failed,
                    ),
                    (
                        "unstarted-unknown",
                        "running",
                        crate::models::DelegatedStatus::Running,
                    ),
                    (
                        "unstarted-busy",
                        "running",
                        crate::models::DelegatedStatus::Running,
                    ),
                    (
                        "launch-claimed",
                        "running",
                        crate::models::DelegatedStatus::Running,
                    ),
                    (
                        "starting",
                        "running",
                        crate::models::DelegatedStatus::Running,
                    ),
                    ("live", "running", crate::models::DelegatedStatus::Running),
                    (
                        "process-owned",
                        "running",
                        crate::models::DelegatedStatus::Running,
                    ),
                    (
                        "has-turn",
                        "running",
                        crate::models::DelegatedStatus::Running,
                    ),
                ] {
                    let mut rows = conn
                        .query("SELECT status FROM jobs WHERE id = ?1", (job,))
                        .await?;
                    assert_eq!(rows.next().await?.unwrap().text(0)?, expected_job, "{job}");
                    let packet =
                        cairn_workflow::delegated_packets::find_by_result_job(conn, "e", job)
                            .await?
                            .unwrap();
                    assert_eq!(packet.status, expected_packet, "{job}");
                }
                let mut rows = conn
                    .query("SELECT error_message FROM runs WHERE id = 'run-failed'", ())
                    .await?;
                let cause = rows.next().await?.unwrap().text(0)?;
                assert!(
                    cause.contains("crashed") && cause.contains("turn"),
                    "failure must retain its cause"
                );
                let mut rows = conn
                    .query(
                        "SELECT recipient,wake,content_ref,fingerprint FROM attention_pushes",
                        (),
                    )
                    .await?;
                let fact = rows.next().await?.unwrap();
                assert_eq!(fact.text(0)?, "parent");
                assert_eq!(fact.text(1)?, "passive");
                let content = fact.text(2)?;
                for job in ["failed", "complete", "unstarted-dead"] {
                    assert!(content.contains(job), "grouped fact identifies {job}");
                }
                assert!(!fact.text(3)?.is_empty());
                assert!(
                    rows.next().await?.is_none(),
                    "siblings share one parent fact"
                );
                Ok(())
            })
        })
        .await
        .unwrap();
        let facts_before = db
            .query_all(
                "SELECT id,content_ref,fingerprint FROM attention_pushes ORDER BY id",
                (),
                |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
            )
            .await
            .unwrap();
        assert!(settle_calls(&db, &observer).await.unwrap().is_empty());
        let facts_after = db
            .query_all(
                "SELECT id,content_ref,fingerprint FROM attention_pushes ORDER BY id",
                (),
                |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
            )
            .await
            .unwrap();
        assert_eq!(
            facts_before, facts_after,
            "replay must not rewrite parent facts"
        );
        // Once the claims disappear, their calls become newly recoverable.
        let recovered = settle_calls(
            &db,
            &WorkObserver::nothing().with_processes(orch.process_state.clone()),
        )
        .await
        .unwrap();
        assert!(recovered["parent"].contains(&("launch-claimed".into(), JobStatus::Complete)));
        assert!(recovered["busy-parent"].contains(&("unstarted-busy".into(), JobStatus::Failed)));

        let sweep_state = db.query_all(
            "SELECT j.id,j.status,p.status FROM jobs j JOIN delegated_packets p ON p.result_artifact_job_id=j.id ORDER BY j.id", (),
            |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
        ).await.unwrap();
        // Rebuild the original stranded rows, then enter through real startup
        // recovery. boot_at=0 keeps the fixture's live rows outside stale-run
        // recovery, isolating the shared turnless settlement seam.
        db.execute_script(
            "UPDATE jobs SET status='running' WHERE id IN ('failed','complete','unstarted-dead');
             UPDATE delegated_packets SET status='running'
               WHERE result_artifact_job_id IN ('failed','complete','unstarted-dead');
             DELETE FROM attention_pushes;",
        )
        .await
        .unwrap();
        crate::runs::queries::reconcile_stale_runs(&orch, orch.services.emitter.as_ref(), 0);
        let restart_state = db.query_all(
            "SELECT j.id,j.status,p.status FROM jobs j JOIN delegated_packets p ON p.result_artifact_job_id=j.id ORDER BY j.id", (),
            |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
        ).await.unwrap();
        assert_eq!(
            restart_state, sweep_state,
            "restart and sweep must share settlement"
        );
        let restart_facts = db
            .query_all("SELECT recipient,wake FROM attention_pushes", (), |row| {
                Ok((row.text(0)?, row.text(1)?))
            })
            .await
            .unwrap();
        assert_eq!(restart_facts, vec![("parent".into(), "passive".into())]);

        // A terminal caller is positive abandonment evidence even while its
        // provider run still reads live; a nonterminal Unknown caller above is not.
        let packet: crate::models::DelegatedWorkPacket =
            serde_json::from_value(serde_json::json!({
                "id": "terminal-packet", "parentJobId": "terminal-parent",
                "origin": "task_tool", "title": "Call", "agentConfigId": "Explore",
                "ownership": {"cwd": "/tmp"}, "outputContract": {"schemaType": "return"},
                "status": "running", "resultArtifactJobId": "terminal-child", "createdAt": 1
            }))
            .unwrap();
        db.write(move |conn| {
            let packet = packet.clone();
            Box::pin(async move {
                conn.execute("INSERT INTO jobs(id,project_id,execution_id,status,created_at,updated_at)
                    VALUES('terminal-parent','p','e','running',1,1)", ()).await?;
                conn.execute("INSERT INTO jobs(id,project_id,execution_id,parent_job_id,status,created_at,updated_at)
                    VALUES('terminal-child','p','e','terminal-parent','running',1,1)", ()).await?;
                conn.execute("INSERT INTO runs(id,job_id,status,created_at,updated_at)
                    VALUES('terminal-parent-run','terminal-parent','live',1,1)", ()).await?;
                cairn_workflow::delegated_packets::insert(conn, "e", &packet).await?;
                Ok(())
            })
        }).await.unwrap();
        orch.process_state.processes.lock().unwrap().register(
            "terminal-parent-run".into(),
            crate::agent_process::process::RunHandle::test_handle(None, Some("terminal-parent")),
        );
        assert!(settle_calls(&db, &observer).await.unwrap().is_empty());
        db.execute(
            "UPDATE jobs SET status='complete' WHERE id='terminal-parent'",
            (),
        )
        .await
        .unwrap();
        let terminal = settle_calls(&db, &observer).await.unwrap();
        assert_eq!(
            terminal["terminal-parent"],
            vec![("terminal-child".into(), JobStatus::Failed)]
        );
    }
}
