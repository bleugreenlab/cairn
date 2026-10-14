//! Host-boundary witnesses against the real migrated schemas.
use crate::{push, runtime::*, storage::*, wakes::*};
use async_trait::async_trait;
use cairn_db::turso::{params, Connection};
use cairn_models::{DeliveryUrgency, Message};
use conditions::{ConditionOutcome, WakeCondition};
use std::sync::{Arc, Mutex};

struct Host {
    db: Arc<LocalDb>,
    delivered: Mutex<Vec<String>>,
    nudges: Mutex<usize>,
}
#[async_trait]
impl RecipientHost for Host {
    async fn ensure_thread_session(&self, conn: &Connection, thread_id: &str) -> DbResult<String> {
        // A real write through the supplied connection witnesses that host work
        // rolls back with attention's transaction, not an independent commit.
        conn.execute(
            "INSERT INTO session_attempts(thread_id) VALUES(?1)",
            params![thread_id],
        )
        .await?;
        Ok("job".into())
    }
    async fn job_is_fenced(&self, _: &Connection, job_id: &str) -> DbResult<bool> {
        Ok(job_id == "fenced")
    }
    async fn job_issue_is_resolved(&self, _: &Connection, job_id: &str) -> DbResult<bool> {
        match job_id {
            "unreadable" => Err(cairn_db::storage::DbError::internal("issue read failed")),
            other => Ok(other == "resolved"),
        }
    }
}
#[async_trait]
impl AttentionHost for Host {
    async fn evaluate_condition(&self, _: &WakeCondition) -> ConditionOutcome {
        ConditionOutcome::Matched {
            rows: "ready".into(),
            shown: 1,
            total: Some(1),
        }
    }
    async fn database_for_issue(&self, _: &str) -> Result<Arc<LocalDb>, String> {
        Ok(self.db.clone())
    }
    async fn database_for_project(&self, _: &str) -> Arc<LocalDb> {
        self.db.clone()
    }
    async fn database_for_job(&self, _: &str) -> Result<Arc<LocalDb>, String> {
        Ok(self.db.clone())
    }
    fn nudge(&self, _: &str, _: DeliveryUrgency) -> Result<(), String> {
        *self.nudges.lock().unwrap() += 1;
        Err("executor unavailable".into())
    }
    async fn message(&self, _: &LocalDb, _: &str) -> Option<Message> {
        panic!("no message rendering expected")
    }
    async fn render_content(&self, _: ContentRequest<'_>) -> Option<String> {
        panic!("no content rendering expected")
    }
    async fn mark_thread_chapter(&self, _: &LocalDb, _: &str, _: &str, _: &str, _: &str) {
        panic!("no chapter compaction expected")
    }
    fn emit_change(&self, _: &str, _: Option<&str>) {}
    fn deliver(&self, job_id: &str, _: Option<&str>, _: &str, _: DeliveryUrgency, _: Option<&str>) {
        self.delivered.lock().unwrap().push(job_id.into());
    }
}
async fn fixture() -> (AttentionRuntime, Arc<Host>) {
    let db = Arc::new(migrated_test_db("attention-runtime.db").await);
    db.execute_script("INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
        INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','proj','/tmp',1,1);
        INSERT INTO threads(id,project_id,name,status,attention,created_at,updated_at) VALUES('thread','p','general','active','none',1,1);
        INSERT INTO jobs(id,project_id,thread_id,uri_segment,status,created_at,updated_at) VALUES('job','p','thread','thread','idle',1,1);
        INSERT INTO jobs(id,project_id,status,created_at,updated_at) VALUES('fenced','p','idle',1,1);
        INSERT INTO jobs(id,project_id,status,created_at,updated_at) VALUES('resolved','p','complete',1,1),('unreadable','p','idle',1,1),('active','p','idle',1,1);
        INSERT INTO runs(id,project_id,job_id,status,created_at,updated_at) VALUES('run','p','job','live',1,1);").await.unwrap();
    db.exclusive(|conn| {
        Box::pin(async move {
            conn.execute("CREATE TABLE session_attempts(thread_id TEXT)", ())
                .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
    let host = Arc::new(Host {
        db: db.clone(),
        delivered: Mutex::new(vec![]),
        nudges: Mutex::new(0),
    });
    (AttentionRuntime::new(db, host.clone()), host)
}
async fn count(db: &LocalDb, sql: &str) -> i64 {
    db.query_one(sql.to_owned(), (), |row| row.i64(0))
        .await
        .unwrap()
}
fn event(job_id: &str) -> WakeEvent {
    WakeEvent {
        source: WakeSource::Process {
            reference: "process-1".into(),
        },
        fact_kind: "exited".into(),
        detail_uri: None,
        urgency: DeliveryUrgency::Queue,
        muted: crate::wakes::MutedDelivery::Drop,
        delivery: WakeDelivery::Targeted {
            subscriber_job_id: job_id.into(),
            sender_name: None,
            message: "finished".into(),
        },
    }
}
#[tokio::test]
async fn closed_thread_preserves_one_shot_but_fenced_execution_consumes_it() {
    let (runtime, host) = fixture().await;
    let db = &host.db;
    for job in ["job", "fenced"] {
        subscribe_one_shot(db, job, "process", Some("process-1"), None, "agent")
            .await
            .unwrap();
    }
    db.execute("UPDATE threads SET status='closed'", ())
        .await
        .unwrap();
    assert_eq!(
        route_wake(&runtime, event("job")).await.unwrap(),
        WakeRouteAction::Dropped
    );
    assert_eq!(
        list_subscriptions_for_job(db, "job").await.unwrap().len(),
        1
    );
    assert_eq!(
        route_wake(&runtime, event("fenced")).await.unwrap(),
        WakeRouteAction::Dropped
    );
    assert!(list_subscriptions_for_job(db, "fenced")
        .await
        .unwrap()
        .is_empty());
    assert!(host.delivered.lock().unwrap().is_empty());
    db.execute("UPDATE threads SET status='active'", ())
        .await
        .unwrap();
    assert_eq!(
        route_wake(&runtime, event("job")).await.unwrap(),
        WakeRouteAction::Delivered
    );
    assert_eq!(
        route_wake(&runtime, event("job")).await.unwrap(),
        WakeRouteAction::Dropped
    );
    assert_eq!(*host.delivered.lock().unwrap(), ["job"]);
}
#[tokio::test]
async fn resolved_issue_drops_wake_and_keeps_one_shot_armed() {
    let (runtime, host) = fixture().await;
    let db = &host.db;
    for job in ["resolved", "unreadable", "active"] {
        subscribe_one_shot(db, job, "process", Some("process-1"), None, "agent")
            .await
            .unwrap();
    }
    assert_eq!(
        route_wake(&runtime, event("resolved")).await.unwrap(),
        WakeRouteAction::Dropped
    );
    assert_eq!(
        list_subscriptions_for_job(db, "resolved")
            .await
            .unwrap()
            .len(),
        1,
        "a resolved issue can reopen, so its one-shot watch must survive the drop"
    );
    assert_eq!(
        route_wake(&runtime, event("unreadable")).await.unwrap(),
        WakeRouteAction::Dropped,
        "a read error fails closed"
    );
    assert_eq!(
        route_wake(&runtime, event("active")).await.unwrap(),
        WakeRouteAction::Delivered
    );
    assert_eq!(*host.delivered.lock().unwrap(), ["active"]);
}

#[tokio::test]
async fn scheduled_occurrence_survives_failed_nudge_without_duplicate_delivery() {
    let (runtime, host) = fixture().await;
    let schedule = create_wake_schedule(&host.db, "thread", 300_000, "check later", "agent")
        .await
        .unwrap();
    let now = schedule.anchor_at;
    assert_eq!(
        fire_wake_schedule(&runtime, &schedule, now)
            .await
            .unwrap_err(),
        "executor unavailable"
    );
    assert_eq!(count(&host.db, "SELECT COUNT(*) FROM messages").await, 1);
    assert_eq!(push::list_pending(&host.db, "job").await.unwrap().len(), 1);
    assert!(
        !fire_wake_schedule(&runtime, &schedule, now).await.unwrap(),
        "stale caller snapshot must not replay committed occurrence"
    );
    assert_eq!(*host.nudges.lock().unwrap(), 1);
    assert_eq!(
        list_wake_schedules(&host.db, "thread").await.unwrap()[0].last_fired_occurrence_at,
        Some(now)
    );
}
#[tokio::test]
async fn conditional_edge_survives_failed_nudge_without_repeating_the_message() {
    let (runtime, host) = fixture().await;
    conditions::create_wake_condition(&host.db, "thread", "SELECT 1", "ready", false, "agent")
        .await
        .unwrap();
    assert_eq!(
        conditions::evaluate_thread_conditions(&runtime, "thread")
            .await
            .unwrap_err(),
        "executor unavailable"
    );
    let results = conditions::evaluate_thread_conditions(&runtime, "thread")
        .await
        .unwrap();
    assert!(
        !results[0].2,
        "the matched edge was committed before the failed nudge"
    );
    assert_eq!(count(&host.db, "SELECT COUNT(*) FROM messages").await, 1);
    assert_eq!(push::list_pending(&host.db, "job").await.unwrap().len(), 1);
    assert_eq!(*host.nudges.lock().unwrap(), 1);
}
#[tokio::test]
async fn failed_push_insert_rolls_back_host_session_work_message_and_schedule_cursor() {
    let (runtime, host) = fixture().await;
    let schedule = create_wake_schedule(&host.db, "thread", 300_000, "check later", "agent")
        .await
        .unwrap();
    // Remove the destination table to force failure after the host write and
    // message insert, without mocking the DB or relying on trigger support.
    host.db
        .exclusive(|conn| {
            Box::pin(async move {
                conn.execute("DROP TABLE attention_pushes", ()).await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    assert!(fire_wake_schedule(&runtime, &schedule, schedule.anchor_at)
        .await
        .is_err());
    assert_eq!(
        count(&host.db, "SELECT COUNT(*) FROM session_attempts").await,
        0
    );
    assert_eq!(count(&host.db, "SELECT COUNT(*) FROM messages").await, 0);
    assert_eq!(
        list_wake_schedules(&host.db, "thread").await.unwrap()[0].last_fired_occurrence_at,
        None
    );
    assert_eq!(*host.nudges.lock().unwrap(), 0);
}
#[tokio::test]
async fn delivery_stamp_and_read_cursor_roll_back_together_on_private_and_team_schemas() {
    for team in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = if team {
            let db = LocalDb::open(dir.path().join("team.db")).await.unwrap();
            MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
                .run(&db)
                .await
                .unwrap();
            db.execute_script("INSERT INTO teams(id,name,created_at,updated_at) VALUES('team','Team',1,1);
                INSERT INTO projects(id,team_id,name,key,repo_path,created_at,updated_at) VALUES('p','team','P','proj','/tmp',1,1);").await.unwrap();
            db
        } else {
            let db = migrated_test_db("attention-cursor.db").await;
            db.execute_script("INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
                INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','proj','/tmp',1,1);").await.unwrap();
            db
        };
        db.execute_script("INSERT INTO jobs(id,project_id,status,created_at,updated_at) VALUES('recipient','p','idle',1,1),('child','p','running',1,1);
            INSERT INTO runs(id,project_id,job_id,created_at,updated_at) VALUES('run','p','child',1,1);
            INSERT INTO turns(id,session_id,run_id,sequence,state,created_at,updated_at) VALUES('turn-1','session','run',1,'completed',1,1);
            INSERT INTO events(id,run_id,sequence,timestamp,event_type,data,turn_id,created_at) VALUES('turn','run',1,1,'text','{}','turn-1',1);
            INSERT INTO attention_pushes(id,recipient,content_ref,wake,boundary,key,created_at) VALUES('push','recipient','cairn://p/proj/1','wake','event','catchup:child',1);").await.unwrap();
        let result: DbResult<()> = db
            .write(|conn| {
                Box::pin(async move {
                    push::stamp_delivered_conn(conn, &["push".into()], "turn").await?;
                    push::advance_read_cursors_conn(conn, &["push".into()]).await?;
                    Err(DbError::Internal("carrying event aborted".into()))
                })
            })
            .await;
        assert!(result.is_err());
        assert_eq!(
            push::list_pending(&db, "recipient").await.unwrap().len(),
            1,
            "team={team}"
        );
        assert_eq!(
            push::read_cursor(&db, "recipient", "child").await.unwrap(),
            None
        );
        db.write(|conn| {
            Box::pin(async move {
                push::stamp_delivered_conn(conn, &["push".into()], "turn").await?;
                push::advance_read_cursors_conn(conn, &["push".into()]).await
            })
        })
        .await
        .unwrap();
        assert!(push::list_pending(&db, "recipient")
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            push::read_cursor(&db, "recipient", "child").await.unwrap(),
            Some(1)
        );
    }
}
