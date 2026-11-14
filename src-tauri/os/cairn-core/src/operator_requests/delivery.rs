//! Retry install-local intents into their owning database. Sink receipts use
//! the intent's stable ID, so commit-before-ack is safe across runner restarts.
use std::sync::Arc;

use cairn_common::requests::RequestOutbox;
use cairn_db::storage::requests;
use cairn_db::turso::params;
use cairn_uri::{parse_uri, CairnResource};
use serde_json::json;

use crate::agent_process::stream::{TranscriptEvent, TranscriptJson};
use crate::messages::queued::DeliveryUrgency;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use crate::transcripts::stream_store::{insert_event_conn, EventInsert};

/// Hosts call this once with the other process-lifetime workers. The immediate
/// first tick recovers committed intents even when no new request is filed.
pub(crate) fn spawn(orch: Orchestrator) {
    tokio::spawn(async move {
        let mut passes = 0_u32;
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            passes = (passes + 1) % 720;
            if passes == 1 {
                if let Err(error) =
                    requests::prune(&orch.db.local, chrono::Utc::now().timestamp()).await
                {
                    let error = super::scrub_text(error.to_string());
                    tracing::warn!(%error, "operator request bounded retention failed");
                }
            }
            if let Err(error) = drain_once(&orch).await {
                let error = super::scrub_text(error);
                tracing::warn!(%error, "operator request outbox sweep failed");
            }
        }
    });
}

pub(crate) async fn drain_once(orch: &Orchestrator) -> Result<usize, String> {
    let intents = requests::pending_outbox(&orch.db.local, 256)
        .await
        .map_err(|e| e.to_string())?;
    let mut delivered = 0;
    for intent in intents {
        let now = chrono::Utc::now().timestamp();
        match deliver(orch, &intent).await {
            Ok(()) => {
                requests::acknowledge_outbox(&orch.db.local, &intent.id, now)
                    .await
                    .map_err(|e| e.to_string())?;
                delivered += 1;
            }
            Err(error) => {
                let error = super::scrub_text(error);
                requests::record_outbox_failure(&orch.db.local, &intent.id, &error, now)
                    .await
                    .map_err(|e| e.to_string())?;
                tracing::warn!(intent_id = %intent.id, %error, "operator request delivery retained for retry");
            }
        }
    }
    Ok(delivered)
}

async fn deliver(orch: &Orchestrator, intent: &RequestOutbox) -> Result<(), String> {
    match intent.kind.as_str() {
        "changed" => {
            // Every transition enqueues this per member in its own transaction,
            // so it is the one post-commit signal a waiting filer needs.
            super::wait::request_changed(&intent.request_id);
            // Requests belong to the install, not to one project's replica. An
            // unscoped frame invalidates every project's private-request badge.
            orch.services.emitter.emit(
                "db-change",
                json!({
                    "table": "requests", "action": "update", "requestId": intent.request_id,
                }),
            )?;
            orch.services.emitter.emit(
                "operator-request-changed",
                json!({
                    "requestId": intent.request_id,
                }),
            )?;
            crate::execution::gates::sweep::notify_gate_evidence_changed();
            Ok(())
        }
        "wake" => deliver_wake(orch, intent).await,
        "reference" => deliver_reference(orch, intent).await,
        "dispatch" => {
            let event_id = payload_str(intent, "authorization_event_id")?;
            match requests::dispatch_disposition(&orch.db.local, event_id)
                .await
                .map_err(|e| e.to_string())?
            {
                requests::DispatchDisposition::Dispatch => {
                    super::dispatch_accepted(orch, event_id).await
                }
                requests::DispatchDisposition::Complete => Ok(()),
                requests::DispatchDisposition::Unresolved => Err(format!(
                    "request dispatch {event_id} remains claimed or unsettled; no physical retry"
                )),
            }
        }
        other => Err(format!("unknown request outbox kind '{other}'")),
    }
}

fn payload_str<'a>(intent: &'a RequestOutbox, key: &str) -> Result<&'a str, String> {
    intent
        .payload
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("request intent {} lacks {key}", intent.id))
}

async fn thread_target(
    orch: &Orchestrator,
    project: &str,
    name: &str,
) -> Result<(Arc<LocalDb>, String), String> {
    let db = orch.db.for_project(project).await;
    let (project, name) = (project.to_string(), name.to_string());
    let id = db
        .read(move |conn| {
            let (project, name) = (project.clone(), name.clone());
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT t.id FROM threads t JOIN projects p ON p.id=t.project_id
                 WHERE p.key=?1 COLLATE NOCASE AND t.name=?2 LIMIT 1",
                        params![project.as_str(), name.as_str()],
                    )
                    .await?;
                rows.next().await?.map(|row| row.text(0)).transpose()
            })
        })
        .await
        .map_err(|e| e.to_string())?
        .ok_or("request destination thread is not readable")?;
    Ok((db, id))
}

async fn thread_run(db: &LocalDb, thread_id: &str) -> Result<(String, String), String> {
    let job_id = crate::threads::ensure_thread_session(db, thread_id).await?;
    let run_id = db
        .read({
            let job_id = job_id.clone();
            move |conn| {
                let job_id = job_id.clone();
                Box::pin(async move {
                    let mut rows=conn.query(
                    "SELECT id FROM runs WHERE job_id=?1 ORDER BY created_at DESC,id DESC LIMIT 1",
                    params![job_id.as_str()],
                ).await?;
                    rows.next().await?.map(|row| row.text(0)).transpose()
                })
            }
        })
        .await
        .map_err(|e| e.to_string())?
        .ok_or("request destination thread has not run yet")?;
    Ok((job_id, run_id))
}

fn wake_content(intent: &RequestOutbox) -> Result<String, String> {
    let mut lines = std::collections::BTreeSet::new();
    let requests = intent
        .payload
        .get("requests")
        .and_then(serde_json::Value::as_array)
        .ok_or("request wake requires request snapshots")?;
    for request in requests {
        let uri = request
            .get("request_uri")
            .and_then(serde_json::Value::as_str)
            .ok_or("wake request lacks URI")?;
        let Some(resource @ CairnResource::Request { .. }) = parse_uri(uri) else {
            return Err("wake request has invalid URI".into());
        };
        if resource.to_uri() != uri {
            return Err("wake request URI is not canonical".into());
        }
        let state: cairn_common::requests::RequestState = serde_json::from_value(
            request
                .get("state")
                .cloned()
                .ok_or("wake request lacks state")?,
        )
        .map_err(|e| e.to_string())?;
        lines.insert(format!("- {uri}: {}", state.as_str()));
    }
    if lines.is_empty() {
        return Err("request wake contains no request snapshots".into());
    }
    let origins = intent
        .payload
        .get("origin_uris")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .collect::<std::collections::BTreeSet<_>>()
        })
        .unwrap_or_default();
    let mut content = format!(
        "Operator requests changed:\n{}\nRead the requests for their decisions and reports.",
        lines.into_iter().collect::<Vec<_>>().join("\n")
    );
    if !origins.is_empty() {
        content.push_str(&format!(
            "\nRequester origins: {}",
            origins.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }
    Ok(super::scrub_text(content))
}

async fn deliver_wake(orch: &Orchestrator, intent: &RequestOutbox) -> Result<(), String> {
    let destination = intent
        .destination
        .as_deref()
        .ok_or("request wake lacks destination")?;
    let content = wake_content(intent)?;
    match parse_uri(destination) {
        Some(CairnResource::Thread {
            project,
            name,
            path,
        }) if path.is_empty() => {
            let (db, thread_id) = thread_target(orch, &project, &name).await?;
            let (job_id, run_id) = thread_run(&db, &thread_id).await?;
            let key = format!("operator-request:{}", intent.id);
            let recipient_job_id = job_id.clone();
            db.write(move |conn| {
                let job_id = recipient_job_id.clone();
                let run_id = run_id.clone();
                let content = content.clone();
                let key = key.clone();
                Box::pin(async move {
                    crate::messages::delivery::insert_system_direct_push_once_conn(
                        conn,
                        &job_id,
                        Some(run_id.as_str()),
                        &content,
                        DeliveryUrgency::Steer,
                        &key,
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .map_err(|e| e.to_string())?;
            orch.services
                .emitter
                .emit("db-change", json!({"table":"messages", "action":"insert"}))?;
            // Retry the nudge even if the sink receipt already exists: the
            // previous process may have stopped between its commit and nudge.
            let orch = orch.clone();
            tokio::task::spawn_blocking(move || {
                crate::messages::delivery::nudge_job_for_urgency(
                    &orch,
                    &job_id,
                    DeliveryUrgency::Steer,
                )
                .map(|_| ())
            })
            .await
            .map_err(|e| e.to_string())??;
            Ok(())
        }
        Some(CairnResource::Issue { project, .. }) => {
            let db = orch.db.for_project(&project).await;
            let issue_id =
                deliver_issue_attention(&db, destination, &intent.id, &content, intent.created_at)
                    .await
                    .map_err(|e| e.to_string())?;
            let change = crate::notify::issue_db_change_for_id(&db, &issue_id, "update").await;
            orch.services.emitter.emit("db-change", change)?;
            orch.services
                .emitter
                .emit("db-change", json!({"table":"messages", "action":"insert"}))?;
            Ok(())
        }
        _ => Err(format!(
            "request wake destination is not an issue or thread: {destination}"
        )),
    }
}

async fn deliver_issue_attention(
    db: &LocalDb,
    uri: &str,
    intent_id: &str,
    content: &str,
    now: i64,
) -> DbResult<String> {
    let (uri, id, content) = (
        uri.to_string(),
        format!("operator-request:{intent_id}"),
        content.to_string(),
    );
    db.write(move |conn| {
        let (uri, id, content) = (uri.clone(), id.clone(), content.clone());
        Box::pin(async move {
            let issue = crate::issues::relations::resolve_issue_uri(conn, &uri).await?
                .ok_or_else(|| DbError::Row(format!("request attention issue is not readable: {uri}")))?;
            let inserted = conn.execute(
                "INSERT OR IGNORE INTO messages(id, channel_type, channel_id, sender_name, content, created_at, urgency)
                 VALUES(?1, 'issue', ?2, 'system', ?3, ?4, 'steer')",
                params![id.as_str(), issue.issue_id.as_str(), content.as_str(), now],
            ).await?;
            // Do not re-arm attention after the operator has handled an already
            // delivered notice, and never replace a stronger human decision.
            if inserted > 0 {
                conn.execute("UPDATE issues SET attention = 'needs_input', updated_at = ?2
                    WHERE id = ?1 AND attention IN ('none','idle') AND status NOT IN ('merged','closed')",
                    params![issue.issue_id.as_str(), now]).await?;
            }
            Ok(issue.issue_id)
        })
    }).await
}

async fn deliver_reference(orch: &Orchestrator, intent: &RequestOutbox) -> Result<(), String> {
    let destination = intent
        .destination
        .as_deref()
        .ok_or("request reference lacks destination")?;
    let (db, run_id) = match parse_uri(destination) {
        Some(CairnResource::Thread {
            project,
            name,
            path,
        }) if path.is_empty() => {
            let (db, thread_id) = thread_target(orch, &project, &name).await?;
            let event_id = format!("operator-request-reference:{}", intent.id);
            let existing=db.read({
                let thread_id=thread_id.clone();
                move |conn| {
                    let (thread_id,event_id)=(thread_id.clone(),event_id.clone());
                    Box::pin(async move {
                        let mut rows=conn.query(
                            "SELECT e.run_id,j.thread_id FROM events e
                             LEFT JOIN runs r ON r.id=e.run_id LEFT JOIN jobs j ON j.id=r.job_id WHERE e.id=?1",
                            params![event_id.as_str()],
                        ).await?;
                        match rows.next().await? {
                            Some(row)=>{
                                if row.opt_text(1)?.as_deref()!=Some(thread_id.as_str()) { return Err(DbError::Row("request card receipt belongs to another thread".into())); }
                                Ok(Some(row.text(0)?))
                            }
                            None=>Ok(None),
                        }
                    })
                }
            }).await.map_err(|e|e.to_string())?;
            // A replay stays on its committed run even if the destination has
            // since rotated sessions; it never appends a second live card.
            let run_id = match existing {
                Some(run) => run,
                None => thread_run(&db, &thread_id).await?.1,
            };
            (db, run_id)
        }
        Some(CairnResource::Issue { project, .. }) => {
            let issue_db = orch.db.for_project(&project).await;
            let content=format!("Operator request cairn://requests/{}. Read the request for its decision and report.",intent.request_id);
            let issue_id = deliver_issue_attention(
                &issue_db,
                destination,
                &intent.id,
                &content,
                intent.created_at,
            )
            .await
            .map_err(|e| e.to_string())?;
            orch.services.emitter.emit(
                "db-change",
                crate::notify::issue_db_change_for_id(&issue_db, &issue_id, "update").await,
            )?;
            orch.services
                .emitter
                .emit("db-change", json!({"table":"messages","action":"insert"}))?;
            let run_id = payload_str(intent, "run_id")?.to_string();
            let db = crate::execution::routing::owning_db_for_run(&orch.db, &run_id)
                .await
                .map_err(|e| e.to_string())?;
            (db, run_id)
        }
        _ => {
            return Err("request reference destination must be a canonical thread or issue".into())
        }
    };
    persist_reference(&db, reference_event(intent, &run_id))
        .await
        .map_err(|e| e.to_string())?;
    let scope = tokio::task::spawn_blocking({
        let db = db.clone();
        let run_id = run_id.clone();
        move || crate::notify::event_run_scope(db, &run_id)
    })
    .await
    .map_err(|e| e.to_string())?;
    orch.services.emitter.emit(
        "db-change",
        crate::notify::event_db_change_scoped(&run_id, None, &scope, "insert"),
    )?;
    Ok(())
}

fn reference_event(intent: &RequestOutbox, run_id: &str) -> EventInsert {
    let request_uri = format!("cairn://requests/{}", intent.request_id);
    let data = TranscriptEvent {
        event_type: "request_reference".into(),
        session_id: None,
        parent_tool_use_id: None,
        content: Some(json!({"requestUri": request_uri}).to_string()),
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id: None,
        tool_result: None,
        is_error: false,
        thinking_ms: None,
        queued_message_id: None,
        raw: Some(json!({"requestOriginUri":intent.payload.get("origin_uri"),"requestOriginRunId":intent.payload.get("run_id")})),
    }
    .observed()
    .to_event_json();
    EventInsert {
        id: format!("operator-request-reference:{}", intent.id),
        run_id: run_id.to_string(),
        session_id: None,
        timestamp: intent.created_at as i32,
        event_type: "request_reference".into(),
        data,
        parent_tool_use_id: None,
        created_at: intent.created_at as i32,
        input_tokens: None,
        cache_read_tokens: None,
        cache_create_tokens: None,
        output_tokens: None,
        thinking_tokens: None,
        turn_id: None,
        cost_usd: None,
        selected_model: None,
    }
}

async fn persist_reference(db: &Arc<LocalDb>, event: EventInsert) -> DbResult<()> {
    db.write(move |conn| {
        let event = event.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT run_id, event_type, data FROM events WHERE id = ?1",
                    params![event.id.as_str()],
                )
                .await?;
            if let Some(row) = rows.next().await? {
                if row.text(0)? != event.run_id
                    || row.text(1)? != event.event_type
                    || row.text(2)? != event.data
                {
                    return Err(DbError::Row(
                        "request reference receipt has conflicting provenance".into(),
                    ));
                }
                return Ok(());
            }
            drop(rows);
            insert_event_conn(conn, &event).await?;
            Ok(())
        })
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_session::transcripts::stream_store::testing::{count_one, insert_run, test_db};

    fn orchestrator_for_delivery(db: Arc<LocalDb>) -> Orchestrator {
        let root = tempfile::tempdir().unwrap().keep();
        let state = Arc::new(crate::db::DbState::new(
            db,
            Arc::new(crate::storage::SearchIndex::open_or_create(root.join("search.db")).unwrap()),
        ));
        Orchestrator::builder(
            state,
            Arc::new(crate::services::testing::TestServicesBuilder::new().build()),
            root,
        )
        .build()
    }

    #[tokio::test]
    async fn request_outbox_retries_unreadable_reference_and_replays_committed_sink_after_boot() {
        let (_temp, db) = test_db().await;
        insert_run(&db, "run-1").await;
        db.execute_script(
            "INSERT INTO request_cohorts(id,workspace_id,kind,state,created_at,updated_at)
             VALUES('delivery-card','default','rebuild','open',1,1);
             INSERT INTO requests(id,workspace_id,kind,target_identity,cohort_id,state,created_at,updated_at)
             VALUES('request-1','default','rebuild','install','delivery-card','open',1,1);
             INSERT INTO request_outbox(id,request_id,kind,payload_json,created_at,destination)
             VALUES('card','request-1','reference','{\"run_id\":\"run-1\"}',1,'cairn://p/proj/1'),
                   ('unreadable','request-1','reference','{\"run_id\":\"future-run\"}',1,'cairn://p/proj/1'),
                   ('change','request-1','changed','{}',1,NULL);"
        ).await.unwrap();
        let orch = orchestrator_for_delivery(db.clone());
        assert_eq!(drain_once(&orch).await.unwrap(), 2);
        assert_eq!(requests::pending_outbox(&db, 200).await.unwrap().len(), 1);
        assert_eq!(
            count_one(
                &db,
                "SELECT COUNT(*) FROM request_outbox WHERE id=?1 AND last_error IS NOT NULL",
                "unreadable"
            )
            .await,
            1
        );
        // Simulate stopping after sink commit but before acknowledgement.
        db.execute(
            "UPDATE request_outbox SET delivered_at=NULL WHERE id='card'",
            (),
        )
        .await
        .unwrap();
        insert_run(&db, "future-run").await;
        let reopened = Arc::new(LocalDb::open(db.path()).await.unwrap());
        let boot = orchestrator_for_delivery(reopened);
        assert_eq!(drain_once(&boot).await.unwrap(), 2);
        assert!(requests::pending_outbox(&db, 200).await.unwrap().is_empty());
        assert_eq!(
            count_one(&db, "SELECT COUNT(*) FROM events WHERE run_id=?1", "run-1").await,
            1
        );
        assert_eq!(
            count_one(
                &db,
                "SELECT COUNT(*) FROM events WHERE run_id=?1",
                "future-run"
            )
            .await,
            1
        );
    }

    #[test]
    fn request_wake_requires_canonical_nonempty_snapshots() {
        let mut wake = intent("wake");
        assert!(wake_content(&wake).is_err());
        wake.payload = json!({"requests":[]});
        assert!(wake_content(&wake).is_err());
        wake.payload = json!({"requests":[{"request_uri":"cairn://requests/r","state":"done"}]});
        assert!(wake_content(&wake)
            .unwrap()
            .contains("cairn://requests/r: done"));
        wake.payload =
            json!({"requests":[{"request_uri":"cairn://requests/r?outcome=done","state":"done"}]});
        assert!(wake_content(&wake).is_err());
    }

    fn intent(kind: &str) -> RequestOutbox {
        RequestOutbox {
            id: "stable-intent".into(),
            request_id: "request-1".into(),
            kind: kind.into(),
            destination: Some("cairn://p/proj/1".into()),
            payload: json!({"run_id":"run-1","state":"done"}),
            created_at: 123,
        }
    }

    #[tokio::test]
    async fn request_reference_replay_after_sink_commit_persists_one_renderable_card() {
        use crate::agent_process::stream::TranscriptEvent;
        let (_temp, db) = test_db().await;
        insert_run(&db, "run-1").await;
        let event = reference_event(&intent("reference"), "run-1");
        let parsed: TranscriptEvent = serde_json::from_str(&event.data).unwrap();
        assert_eq!(parsed.event_type, "request_reference");
        let content: serde_json::Value =
            serde_json::from_str(parsed.content.as_deref().unwrap()).unwrap();
        assert_eq!(content["requestUri"], "cairn://requests/request-1");
        persist_reference(&db, event.clone()).await.unwrap();
        // The outbox ack is deliberately absent. A new connection, as on boot,
        // retries the exact same stable sink receipt.
        let reopened = Arc::new(LocalDb::open(db.path()).await.unwrap());
        persist_reference(&reopened, event.clone()).await.unwrap();
        assert_eq!(
            count_one(&db, "SELECT COUNT(*) FROM events WHERE run_id=?1", "run-1").await,
            1
        );
        let mut conflicting = event;
        conflicting.data = conflicting.data.replace("request-1", "request-2");
        assert!(persist_reference(&reopened, conflicting).await.is_err());
    }

    #[tokio::test]
    async fn parentless_request_attention_is_durable_and_not_rearmed_by_ack_retry() {
        let (_temp, db) = test_db().await;
        insert_run(&db, "run-1").await;
        // stream-store fixture creates proj/1 as issue-1.
        let uri = db.read(|conn| Box::pin(async move {
            let mut rows = conn.query("SELECT p.key,i.number FROM issues i JOIN projects p ON p.id=i.project_id WHERE i.id='issue-1'",()).await?;
            let row = rows.next().await?.unwrap();
            Ok(cairn_uri::build_issue_uri(&row.text(0)?, row.i64(1)? as i32))
        })).await.unwrap();
        deliver_issue_attention(&db, &uri, "one", "Request completed", 1)
            .await
            .unwrap();
        assert_eq!(
            count_one(
                &db,
                "SELECT COUNT(*) FROM messages WHERE id=?1",
                "operator-request:one"
            )
            .await,
            1
        );
        db.execute("UPDATE issues SET attention='none' WHERE id='issue-1'", ())
            .await
            .unwrap();
        let reopened = LocalDb::open(db.path()).await.unwrap();
        deliver_issue_attention(&reopened, &uri, "one", "Request completed", 1)
            .await
            .unwrap();
        assert_eq!(
            count_one(
                &db,
                "SELECT COUNT(*) FROM issues WHERE id=?1 AND attention='none'",
                "issue-1"
            )
            .await,
            1
        );
        db.execute(
            "UPDATE issues SET attention='needs_approval' WHERE id='issue-1'",
            (),
        )
        .await
        .unwrap();
        deliver_issue_attention(&db, &uri, "two", "Another outcome", 2)
            .await
            .unwrap();
        assert_eq!(
            count_one(
                &db,
                "SELECT COUNT(*) FROM issues WHERE id=?1 AND attention='needs_approval'",
                "issue-1"
            )
            .await,
            1
        );
    }

    #[tokio::test]
    async fn request_reference_owner_thread_receipt_stays_on_committed_run_after_rotation() {
        let (_temp, db) = test_db().await;
        insert_run(&db, "filing-run").await;
        insert_run(&db, "owner-run").await;
        db.execute_script(
            "INSERT INTO threads(id,project_id,name,created_at,updated_at) VALUES('owner','project-1','owner',1,1);
             INSERT INTO jobs(id,project_id,thread_id,node_name,uri_segment,status,created_at,updated_at)
             VALUES('owner-job','project-1','owner','Thread','thread','running',1,1);
             UPDATE runs SET job_id='owner-job',project_id='project-1' WHERE id='owner-run';"
        ).await.unwrap();
        let mut reference = intent("reference");
        reference.destination = Some("cairn://p/proj/owner".into());
        reference.payload =
            json!({"run_id":"filing-run","origin_uri":"cairn://p/proj/1/1/builder/task/child"});
        let orch = orchestrator_for_delivery(db.clone());
        deliver_reference(&orch, &reference).await.unwrap();
        assert_eq!(
            count_one(
                &db,
                "SELECT COUNT(*) FROM events WHERE run_id=?1",
                "owner-run"
            )
            .await,
            1
        );
        assert_eq!(
            count_one(
                &db,
                "SELECT COUNT(*) FROM events WHERE run_id=?1",
                "filing-run"
            )
            .await,
            0
        );
        insert_run(&db, "owner-next").await;
        db.execute("UPDATE runs SET job_id='owner-job',project_id='project-1',created_at=2147483646 WHERE id='owner-next'",()).await.unwrap();
        let reopened = Arc::new(LocalDb::open(db.path()).await.unwrap());
        deliver_reference(&orchestrator_for_delivery(reopened), &reference)
            .await
            .unwrap();
        assert_eq!(
            count_one(
                &db,
                "SELECT COUNT(*) FROM events WHERE run_id=?1",
                "owner-run"
            )
            .await,
            1
        );
        assert_eq!(
            count_one(
                &db,
                "SELECT COUNT(*) FROM events WHERE run_id=?1",
                "owner-next"
            )
            .await,
            0
        );
    }

    #[tokio::test]
    async fn request_wake_receipt_survives_push_dismissal_and_destination_run_rotation() {
        let (_temp, db) = test_db().await;
        insert_run(&db, "run-1").await;
        db.execute_script(
            "INSERT INTO jobs(id,project_id,issue_id,node_name,status,created_at,updated_at)
             VALUES('wake-job','project-1','issue-1','builder','running',1,1);
             UPDATE runs SET job_id='wake-job' WHERE id='run-1';",
        )
        .await
        .unwrap();
        insert_run(&db, "run-2").await;
        db.execute("UPDATE runs SET job_id='wake-job' WHERE id='run-2'", ())
            .await
            .unwrap();
        let mut transition = intent("wake");
        transition.payload = json!({
            "requests":[
                {"request_uri":"cairn://requests/request-1","state":"done"},
                {"request_uri":"cairn://requests/request-2","state":"open"}
            ],
            "origin_uris":["cairn://p/proj/1/1/builder","cairn://p/proj/2/1/builder"]
        });
        let content = wake_content(&transition).unwrap();
        for expected in [
            "request-1: done",
            "request-2: open",
            "cairn://p/proj/1/1/builder",
            "cairn://p/proj/2/1/builder",
        ] {
            assert!(content.contains(expected), "{content}");
        }
        let job = "wake-job".to_string();
        for run in ["run-1", "run-2"] {
            db.write(|conn| {
                let job = job.clone();
                let content = content.clone();
                Box::pin(async move {
                    crate::messages::delivery::insert_system_direct_push_once_conn(
                        conn,
                        &job,
                        Some(run),
                        &content,
                        DeliveryUrgency::Steer,
                        "operator-request:stable",
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
            db.execute("DELETE FROM attention_pushes", ())
                .await
                .unwrap();
        }
        assert_eq!(
            count_one(
                &db,
                "SELECT COUNT(*) FROM messages WHERE id=?1 AND content LIKE '%request-2: open%' AND content LIKE '%cairn://p/proj/2/1/builder%'",
                "reconcile:operator-request:stable"
            )
            .await,
            1
        );
        assert_eq!(
            count_one(
                &db,
                "SELECT COUNT(*) FROM attention_pushes WHERE recipient=?1",
                &job
            )
            .await,
            0
        );
    }
}
