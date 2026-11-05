//! Inline detail on catch-up and review pushes, rendered against the real
//! migrated schema: what each says when its referent resolves, and that every
//! miss is today's pointer rather than an error.
use crate::delivery::{
    one_line_excerpt, prose_candidates_sql, render_push_resolved, CATCHUP_DIGEST_CHARS,
    REVIEW_ARTIFACT_SQL, WINDOW_TURNS_SQL,
};
use crate::push::{Boundary, Push, Wake, JOB_CHAT_TURNS_SQL};
use crate::runtime::*;
use crate::storage::*;
use crate::wakes::conditions::{ConditionOutcome, WakeCondition};
use async_trait::async_trait;
use cairn_db::storage::events::event_fixture::{assistant_read, assistant_text, user_text};
use cairn_db::turso::params;
use cairn_domain::{DeliveryUrgency, Message};
use std::sync::Arc;

/// A host whose every database is the one fixture database and which has no
/// messages or rendered content to offer, so what a push renders comes from
/// the attention engine alone.
struct Host {
    db: Arc<LocalDb>,
}
#[async_trait]
impl RecipientHost for Host {
    async fn ensure_thread_session(&self, _: &TrackedConnection, _: &str) -> DbResult<String> {
        panic!("rendering never opens a thread session")
    }
    async fn job_is_fenced(&self, _: &TrackedConnection, _: &str) -> DbResult<bool> {
        Ok(false)
    }
    async fn job_issue_is_resolved(&self, _: &TrackedConnection, _: &str) -> DbResult<bool> {
        Ok(false)
    }
}
#[async_trait]
impl AttentionHost for Host {
    async fn evaluate_condition(&self, _: &WakeCondition) -> ConditionOutcome {
        panic!("rendering never evaluates a condition")
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
        Ok(())
    }
    async fn message(&self, _: &LocalDb, _: &str) -> Option<Message> {
        None
    }
    async fn render_content(&self, _: ContentRequest<'_>) -> Option<String> {
        None
    }
    async fn mark_thread_chapter(&self, _: &LocalDb, _: &str, _: &str, _: &str, _: &str) {}
    fn emit_change(&self, _: &str, _: Option<&str>) {}
    fn deliver(&self, _: &str, _: Option<&str>, _: &str, _: DeliveryUrgency, _: Option<&str>) {
        panic!("rendering never delivers")
    }
}

const CHAT: &str = "cairn://p/proj/2/1/builder/chat";
const NODE: &str = "cairn://p/proj/2/1/builder";

/// Issue `proj-2`, execution 1, with the `builder` child job and its one run,
/// watched by a job on another issue.
async fn fixture(name: &str) -> (AttentionRuntime, Arc<LocalDb>) {
    let db = Arc::new(migrated_test_db(name).await);
    db.execute_script(
        "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','proj','/tmp',1,1);
         INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at) VALUES('parent','p',1,'Parent','active','active','none',1,1);
         INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at) VALUES('issue-1','p',2,'Child','active','active','none',1,1);
         INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES('exec-1','r','issue-1','p','running',1,1);
         INSERT INTO jobs(id,project_id,issue_id,uri_segment,status,created_at,updated_at) VALUES('watcher','p','parent','watcher','running',1,1);
         INSERT INTO jobs(id,project_id,issue_id,execution_id,uri_segment,status,created_at,updated_at) VALUES('child-job','p','issue-1','exec-1','builder','running',1,1);
         INSERT INTO runs(id,project_id,job_id,issue_id,created_at,updated_at) VALUES('run-1','p','child-job','issue-1',1,1);",
    )
    .await
    .unwrap();
    let host = Arc::new(Host { db: db.clone() });
    (AttentionRuntime::new(db.clone(), host), db)
}

/// One stored event: `(id, created_at, event_type, data, parent_tool_use_id)`.
type Ev<'a> = (&'a str, i64, &'a str, String, Option<&'a str>);

/// Record a turn of `child-job` started at `at`, and its events.
async fn turn(db: &LocalDb, turn_id: &str, sequence: i64, at: i64, events: &[Ev<'_>]) {
    db.execute(
        "INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,created_at,updated_at)
         VALUES(?1,'sess','run-1','child-job',?2,'completed',?3,?3)",
        params![turn_id, sequence, at],
    )
    .await
    .unwrap();
    for (index, (id, created_at, event_type, data, parent)) in events.iter().enumerate() {
        db.execute(
            "INSERT INTO events(id,run_id,sequence,timestamp,event_type,data,parent_tool_use_id,turn_id,created_at)
             VALUES(?1,'run-1',?2,?3,?4,?5,?6,?7,?3)",
            params![
                *id,
                sequence * 100 + index as i64,
                *created_at,
                *event_type,
                data.as_str(),
                *parent,
                turn_id
            ],
        )
        .await
        .unwrap();
    }
}

/// A turn holding one assistant reply.
async fn said(db: &LocalDb, turn_id: &str, sequence: i64, at: i64, text: &str) {
    let id = format!("{turn_id}-text");
    turn(
        db,
        turn_id,
        sequence,
        at,
        &[(&id, at, "assistant", assistant_text(text), None)],
    )
    .await;
}

/// An operator message typed at `child-job`, not yet read by it.
async fn queued(db: &LocalDb, content: &str, at: i64) {
    db.execute(
        "INSERT INTO queued_messages(id,job_id,content,delivery,created_at) VALUES(?1,'child-job',?2,'queue',?3)",
        params![format!("q-{at}"), content, at],
    )
    .await
    .unwrap();
}

/// Version `version` of `child-job`'s artifact `name`.
async fn artifact(db: &LocalDb, name: &str, version: i64, data: &str, at: i64) {
    db.execute(
        "INSERT INTO artifacts(id,job_id,artifact_type,data,version,output_name,created_at,updated_at)
         VALUES(?1,'child-job',?2,?3,?4,?2,?5,?5)",
        params![format!("{name}-v{version}"), name, data, version, at],
    )
    .await
    .unwrap();
}

fn push(key: &str, content_ref: &str) -> Push {
    Push {
        id: "push".into(),
        recipient: "watcher".into(),
        content_ref: content_ref.into(),
        wake: Wake::Passive,
        boundary: Boundary::Event,
        key: key.into(),
        created_at: 1,
        delivered_event_id: None,
    }
}

fn header(content_ref: &str) -> String {
    format!("Attention update (passive): {content_ref}")
}

/// Exactly what a push rendered before inline detail existed.
fn pointer(content_ref: &str, headline: &str) -> String {
    format!(
        "{}\n\n{headline}. Read {content_ref} for the current content.",
        header(content_ref)
    )
}

async fn catchup(runtime: &AttentionRuntime, offset: i64) -> (String, String) {
    let uri = format!("{CHAT}?offset={offset}");
    let rendered = render_push_resolved(runtime, &push("catchup:child-job", &uri)).await;
    (uri, rendered)
}

async fn review(runtime: &AttentionRuntime, uri: &str) -> String {
    render_push_resolved(runtime, &push(&format!("review:{uri}"), uri)).await
}

#[tokio::test]
async fn catchup_names_node_turn_count_and_newest_assistant_text() {
    let (runtime, db) = fixture("delivery-catchup-line.db").await;
    said(&db, "t1", 1, 100, "first turn, already read").await;
    said(&db, "t2", 2, 200, "second turn, inside the window").await;
    // The two replies share a second, and the older one's id sorts last: only
    // insertion order says which the chat shows as newest.
    turn(
        &db,
        "t3",
        3,
        300,
        &[
            (
                "t3-z",
                301,
                "assistant",
                assistant_text("OLDER reply"),
                None,
            ),
            (
                "t3-a",
                301,
                "assistant",
                assistant_text("NEWER reply"),
                None,
            ),
            (
                "t3-call",
                302,
                "assistant",
                assistant_read("call-1", &["file:x"]),
                None,
            ),
            (
                "t3-sub",
                303,
                "assistant",
                assistant_text("SUBAGENT reply"),
                Some("call-1"),
            ),
            (
                "t3-user",
                304,
                "user",
                user_text("LAUNCH PROMPT sentinel"),
                None,
            ),
        ],
    )
    .await;

    let (uri, rendered) = catchup(&runtime, 1).await;

    assert_eq!(
        rendered,
        format!(
            "{}\n\nbuilder · 2 new turns · \"NEWER reply\" Read {uri}",
            header(&uri)
        )
    );
    for absent in ["OLDER", "SUBAGENT", "LAUNCH PROMPT", "mcp__cairn__read"] {
        assert!(!rendered.contains(absent), "{absent}: {rendered}");
    }
}

#[tokio::test]
async fn catchup_without_assistant_text_still_counts_its_turns() {
    let (runtime, db) = fixture("delivery-catchup-count.db").await;
    said(&db, "t1", 1, 100, "already read").await;
    turn(
        &db,
        "t2",
        2,
        200,
        &[(
            "t2-call",
            201,
            "assistant",
            assistant_read("call-1", &["file:x"]),
            None,
        )],
    )
    .await;

    let (uri, rendered) = catchup(&runtime, 1).await;

    assert_eq!(
        rendered,
        format!("{}\n\nbuilder · 1 new turn. Read {uri}", header(&uri))
    );
}

/// The window walked is the window counted. A run-less session-boundary turn is
/// one of the new turns, so the walk takes it rather than reaching back past the
/// cursor, and the read turn's prose is not quoted as news.
#[tokio::test]
async fn catchup_walk_stays_inside_the_counted_window() {
    let (runtime, db) = fixture("delivery-catchup-boundary.db").await;
    said(&db, "a", 1, 100, "old").await;
    turn(
        &db,
        "b",
        2,
        200,
        &[(
            "b-call",
            201,
            "assistant",
            assistant_read("call-1", &["file:x"]),
            None,
        )],
    )
    .await;
    db.execute_script(
        "INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,created_at,updated_at)
           VALUES('c','sess-2',NULL,'child-job',1,'completed',300,300);
         INSERT INTO events(id,run_id,sequence,timestamp,event_type,data,turn_id,created_at)
           VALUES('c-boundary','run-1',300,300,'system:session_boundary','{}','c',300);",
    )
    .await
    .unwrap();

    let (uri, rendered) = catchup(&runtime, 1).await;

    assert_eq!(
        rendered,
        format!("{}\n\nbuilder · 2 new turns. Read {uri}", header(&uri))
    );
}

#[tokio::test]
async fn catchup_with_operator_messages_keeps_digest_and_adds_line() {
    let (runtime, db) = fixture("delivery-catchup-digest.db").await;
    said(&db, "t1", 1, 100, "already read").await;
    said(&db, "t2", 2, 200, "Rebased onto main.").await;
    queued(&db, "please rebase onto main", 150).await;

    let (uri, rendered) = catchup(&runtime, 1).await;

    assert!(
        rendered.starts_with(&format!("{}\n\nPassive child context", header(&uri))),
        "{rendered}"
    );
    let operator = rendered
        .find("please rebase onto main")
        .expect("the digest names the operator's message");
    let line = rendered
        .find(&format!(
            "\n\nbuilder · 1 new turn · \"Rebased onto main.\" Read {uri}"
        ))
        .expect("the chat line follows the digest");
    assert!(operator < line, "{rendered}");
}

#[tokio::test]
async fn long_assistant_text_is_cut_at_a_boundary_with_the_uri_kept() {
    let (runtime, db) = fixture("delivery-catchup-long.db").await;
    let long = "The rebase reordered the integration tests so they pass again. ".repeat(10);
    said(&db, "t1", 1, 100, &long).await;

    let (uri, rendered) = catchup(&runtime, 0).await;

    let excerpt = rendered
        .split_once("· \"")
        .and_then(|(_, rest)| rest.split_once(&format!("\" Read {uri}")))
        .map(|(excerpt, _)| excerpt)
        .unwrap_or_else(|| panic!("a quoted excerpt then the address: {rendered}"));
    assert!(
        excerpt.chars().count() <= CATCHUP_DIGEST_CHARS + 1,
        "{excerpt}"
    );
    assert!(
        excerpt.ends_with("again.…"),
        "cut after a sentence: {excerpt}"
    );
}

#[test]
fn one_line_excerpt_cuts_where_a_reader_can_stop() {
    assert_eq!(
        one_line_excerpt("short\n\n  and   spaced"),
        "short and spaced"
    );
    let exact = "y".repeat(CATCHUP_DIGEST_CHARS);
    assert_eq!(one_line_excerpt(&exact), exact, "at the bound is not cut");

    let sentence = format!("{}. {}", "a".repeat(150), "word ".repeat(40));
    assert_eq!(
        one_line_excerpt(&sentence),
        format!("{}.…", "a".repeat(150))
    );

    // No sentence end, so the last whole word inside the bound.
    let words = "word ".repeat(60);
    assert_eq!(
        one_line_excerpt(&words),
        format!("{}…", vec!["word"; 48].join(" "))
    );

    // A sentence end before half the bound is too early to stop at.
    let early = format!("Ok. {}", "word ".repeat(60));
    assert!(one_line_excerpt(&early).chars().count() > CATCHUP_DIGEST_CHARS / 2);

    let token = "x".repeat(500);
    assert_eq!(
        one_line_excerpt(&token),
        format!("{}…", "x".repeat(CATCHUP_DIGEST_CHARS))
    );

    let wide = "éééé ".repeat(80);
    let cut = one_line_excerpt(&wide);
    assert!(cut.chars().count() <= CATCHUP_DIGEST_CHARS + 1, "{cut}");
    assert!(cut.ends_with("éééé…"), "{cut}");
}

#[tokio::test]
async fn catchup_with_no_new_turns_keeps_the_pointer() {
    let (runtime, db) = fixture("delivery-catchup-empty.db").await;
    said(&db, "t1", 1, 100, "already read").await;
    said(&db, "t2", 2, 200, "also already read").await;

    let (uri, rendered) = catchup(&runtime, 2).await;

    assert_eq!(rendered, pointer(&uri, "New chat to catch up on"));
}

#[tokio::test]
async fn review_renders_artifact_title_and_summary() {
    let (runtime, db) = fixture("delivery-review.db").await;
    artifact(
        &db,
        "create-pr",
        1,
        r#"{"title":"Rebase onto main (draft)","body":"BODY sentinel"}"#,
        10,
    )
    .await;
    artifact(
        &db,
        "create-pr",
        2,
        r#"{"title":"Rebase onto main","body":"BODY sentinel"}"#,
        20,
    )
    .await;
    // Written after the PR, so "newest of any name" would wrongly pick it.
    artifact(
        &db,
        "plan",
        1,
        r#"{"title":"Plan the rebase","summary":"Reorders the tests,\nthen rebases.","content":"BODY sentinel"}"#,
        30,
    )
    .await;

    let plan = format!("{NODE}/plan");
    assert_eq!(
        review(&runtime, &plan).await,
        format!(
            "{}\n\nWork product ready for review: Plan the rebase\n\
             Reorders the tests, then rebases.\n\
             Read {plan} for the current content.",
            header(&plan)
        )
    );

    for uri in [format!("{NODE}/create-pr"), format!("{NODE}/artifact")] {
        let rendered = review(&runtime, &uri).await;
        assert_eq!(
            rendered,
            format!(
                "{}\n\nWork product ready for review: Rebase onto main\n\
                 Read {uri} for the current content.",
                header(&uri)
            )
        );
        assert!(!rendered.contains("BODY"), "{rendered}");
    }
}

#[tokio::test]
async fn failed_lookup_keeps_the_pointer() {
    let (runtime, db) = fixture("delivery-failed-lookup.db").await;
    said(&db, "t1", 1, 100, "already read").await;
    said(&db, "t2", 2, 200, "unread").await;
    artifact(&db, "broken", 1, "not json at all", 10).await;
    artifact(&db, "untitled", 1, r#"{"summary":"no title here"}"#, 10).await;
    artifact(&db, "blank", 1, r#"{"title":"   "}"#, 10).await;

    for uri in [
        format!("{NODE}/broken"),
        format!("{NODE}/untitled"),
        format!("{NODE}/blank"),
        format!("{NODE}/missing"),
        "cairn://p/proj/2/1/nobody/create-pr".to_string(),
        "cairn://p/proj/2".to_string(),
    ] {
        assert_eq!(
            review(&runtime, &uri).await,
            pointer(&uri, "Work product ready for review")
        );
    }

    let unparseable = format!("{CHAT}?offset=soon");
    assert_eq!(
        render_push_resolved(&runtime, &push("catchup:child-job", &unparseable)).await,
        pointer(&unparseable, "New chat to catch up on")
    );

    db.exclusive(|conn| {
        Box::pin(async move {
            conn.execute("ALTER TABLE events RENAME TO events_gone", ())
                .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
    let (uri, rendered) = catchup(&runtime, 1).await;
    assert_eq!(rendered, pointer(&uri, "New chat to catch up on"));
}

/// Each statement is bounded by what it shows: no scan, no sort over a job's
/// history, and each on the index that bounds it.
#[tokio::test]
async fn catchup_and_review_statements_stay_on_their_indexes() {
    let (_, db) = fixture("delivery-plans.db").await;
    let statements = [
        (
            "job chat turns",
            JOB_CHAT_TURNS_SQL.to_string(),
            "idx_turns_job_id",
        ),
        (
            "window turns",
            WINDOW_TURNS_SQL.to_string(),
            "idx_turns_job_id",
        ),
        (
            "prose candidates",
            prose_candidates_sql("idx_events_home_prose"),
            "idx_events_home_prose",
        ),
        (
            "turn fallback",
            prose_candidates_sql("idx_events_turn_id"),
            "idx_events_turn_id",
        ),
        (
            "review artifact",
            REVIEW_ARTIFACT_SQL.to_string(),
            "idx_artifacts_output_job_version",
        ),
    ];
    for (label, sql, index) in statements {
        let explain = format!("EXPLAIN QUERY PLAN {sql}");
        let plan = db
            .read(move |conn| {
                let explain = explain.clone();
                Box::pin(async move {
                    let mut rows = if explain.contains("?4") {
                        conn.query(&explain, params!["a", 1i64, "b", 4i64]).await?
                    } else if explain.contains("?2") {
                        conn.query(&explain, params!["a", "b"]).await?
                    } else {
                        conn.query(&explain, params!["a"]).await?
                    };
                    let mut steps = Vec::new();
                    while let Some(row) = rows.next().await? {
                        steps.push(row.text(3)?);
                    }
                    Ok(steps.join("\n"))
                })
            })
            .await
            .unwrap();
        println!("{label}:\n{plan}\n");
        assert!(plan.contains(index), "{label} must use {index}:\n{plan}");
        assert!(!plan.contains("SCAN"), "{label} scans:\n{plan}");
        if label != "turn fallback" {
            assert!(!plan.contains("SORTER"), "{label} sorts:\n{plan}");
        }
        if label == "job chat turns" || label == "window turns" {
            assert!(
                plan.contains("COVERING INDEX idx_events_turn_id"),
                "{label} must probe events by key alone:\n{plan}"
            );
            assert!(
                !plan.contains("idx_events_run_id"),
                "{label} must not walk a run's events:\n{plan}"
            );
        }
    }
}

fn failed_event() -> crate::events::AttentionEvent {
    crate::events::AttentionEvent {
        issue_id: "issue-1".into(),
        issue_uri: "cairn://p/proj/2".into(),
        fact: crate::events::AttentionFact::Resolved {
            final_status: cairn_domain::IssueStatus::Failed,
        },
        attention: cairn_domain::IssueAttention::None,
        status: cairn_domain::IssueStatus::Failed,
        updated_at: 10,
        route_provenance: None,
    }
}

async fn failure_fixture(name: &str) -> (AttentionRuntime, Arc<LocalDb>) {
    let (runtime, db) = fixture(name).await;
    db.execute_script(
        "UPDATE issues SET parent_issue_id='parent',parent_job_id='watcher' WHERE id='issue-1';
         UPDATE jobs SET current_session_id='watcher-session' WHERE id='watcher';
         UPDATE jobs SET status='failed' WHERE id='child-job';
         UPDATE runs SET status='crashed',error_message='Provider connection lost' WHERE id='run-1';"
    ).await.unwrap();
    (runtime, db)
}

#[tokio::test]
async fn failed_resolution_restart_recovers_once_before_and_after_delivery() {
    let (runtime, db) = failure_fixture("failed-resolution-restart").await;
    use crate::delivery::rearm_failed_resolutions;
    let rolled_back: DbResult<()> = db
        .write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "UPDATE issues SET status='failed',completed_at=10 WHERE id='issue-1'",
                    (),
                )
                .await?;
                Err(DbError::Row("simulated crash before commit".into()))
            })
        })
        .await;
    assert!(rolled_back.is_err());
    assert_eq!(
        rearm_failed_resolutions(&runtime, &db).await.unwrap(),
        0,
        "no failure notice before the settlement commits"
    );
    db.execute(
        "UPDATE issues SET status='failed',completed_at=10 WHERE id='issue-1'",
        (),
    )
    .await
    .unwrap();
    // Drop all runtime state and reopen the migrated database: the resolution
    // committed, but its normal post-commit emit never ran.
    let path = db.path().to_path_buf();
    drop(runtime);
    drop(db);
    let db = Arc::new(LocalDb::open_migrated_for_test(path).await.unwrap());
    let runtime = AttentionRuntime::new(db.clone(), Arc::new(Host { db: db.clone() }));
    assert_eq!(rearm_failed_resolutions(&runtime, &db).await.unwrap(), 1);
    let pending = crate::push::list_pending(&db, "watcher").await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].key, "resolved:cairn://p/proj/2");
    assert_eq!(rearm_failed_resolutions(&runtime, &db).await.unwrap(), 0);
    assert_eq!(
        crate::push::list_pending(&db, "watcher").await.unwrap()[0].id,
        pending[0].id
    );
    crate::push::stamp_delivered(&db, &[pending[0].id.clone()], "carrying-event")
        .await
        .unwrap();
    assert_eq!(
        rearm_failed_resolutions(&runtime, &db).await.unwrap(),
        0,
        "delivered history must suppress replay too"
    );
    assert!(crate::push::list_pending(&db, "watcher")
        .await
        .unwrap()
        .is_empty());
    db.execute(
        "UPDATE issues SET resolution_generation=resolution_generation+1 WHERE id='issue-1'",
        (),
    )
    .await
    .unwrap();
    assert_eq!(
        rearm_failed_resolutions(&runtime, &db).await.unwrap(),
        1,
        "an older generation must not silence a genuinely new resolution in the same second"
    );
}

#[tokio::test]
async fn failed_resolution_normal_emitter_races_startup_without_duplicate() {
    let (runtime, db) = failure_fixture("failed-resolution-concurrent").await;
    db.execute(
        "UPDATE issues SET status='failed',completed_at=10 WHERE id='issue-1'",
        (),
    )
    .await
    .unwrap();
    let event = failed_event();
    let (normal, boot, other_boot) = tokio::join!(
        crate::delivery::create_resolved_push_async(&runtime, &event),
        crate::delivery::rearm_failed_resolutions(&runtime, &db),
        crate::delivery::rearm_failed_resolutions(&runtime, &db),
    );
    assert_eq!(
        normal.unwrap() + boot.unwrap() + other_boot.unwrap(),
        1,
        "only one racing creator owns the durable fact and wake"
    );
    let rows = crate::push::list_pending(&db, "watcher").await.unwrap();
    assert_eq!(rows.len(), 1);
    crate::push::stamp_delivered(&db, &[rows[0].id.clone()], "carrying-event")
        .await
        .unwrap();
    let (normal, boot) = tokio::join!(
        crate::delivery::create_resolved_push_async(&runtime, &event),
        crate::delivery::rearm_failed_resolutions(&runtime, &db),
    );
    assert_eq!(normal.unwrap() + boot.unwrap(), 0);
}

#[tokio::test]
async fn failed_resolution_quiet_generation_survives_rearm_but_not_reresolution() {
    let (runtime, db) = failure_fixture("failed-resolution-quiet").await;
    db.execute(
        "UPDATE issues SET status='failed',completed_at=10,
        quiet_failure_generation=resolution_generation WHERE id='issue-1'",
        (),
    )
    .await
    .unwrap();
    for _ in 0..2 {
        assert_eq!(
            crate::delivery::rearm_failed_resolutions(&runtime, &db)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            crate::delivery::create_resolved_push_async(&runtime, &failed_event())
                .await
                .unwrap(),
            0
        );
    }
    db.execute(
        "UPDATE issues SET resolution_generation=resolution_generation+1 WHERE id='issue-1'",
        (),
    )
    .await
    .unwrap();
    assert_eq!(
        crate::delivery::rearm_failed_resolutions(&runtime, &db)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn failed_resolution_reason_names_own_failure_not_inherited_descendant() {
    let (runtime, db) = failure_fixture("failed-resolution-own-reason").await;
    db.execute_script(
        "UPDATE issues SET status='failed',completed_at=10 WHERE id='issue-1';
         INSERT INTO jobs(id,project_id,issue_id,execution_id,uri_segment,status,created_at,updated_at)
         VALUES('review','p','issue-1','exec-1','review','failed',2,20);
         INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,failure_reason,created_at,updated_at)
         VALUES('failed-work','sess','child-job',1,'failed','retry','Launch failed before run allocation',10,10);"
    ).await.unwrap();
    let rendered = render_push_resolved(
        &runtime,
        &push("resolved:cairn://p/proj/2", "cairn://p/proj/2"),
    )
    .await;
    assert!(rendered.contains(NODE), "{rendered}");
    assert!(
        rendered.contains("Launch failed before run allocation"),
        "{rendered}"
    );
    assert!(!rendered.contains("/review"), "{rendered}");
    let long = "測試 detail\n".repeat(100);
    db.execute(
        "UPDATE turns SET failure_reason=?1 WHERE id='failed-work'",
        (long,),
    )
    .await
    .unwrap();
    let reason = db
        .read(|conn| {
            Box::pin(
                async move { crate::delivery::job_failure_reason_conn(conn, "child-job").await },
            )
        })
        .await
        .unwrap()
        .unwrap();
    assert!(reason.chars().count() <= CATCHUP_DIGEST_CHARS + 1);
    assert!(!reason.contains('\n'));
    db.execute_script(
        "INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,created_at,updated_at)
         VALUES('retry-success','sess','child-job',2,'complete','retry',11,11);
         INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,created_at,updated_at)
         VALUES('failed-memory','sess','child-job',3,'failed','memory_review',12,12);",
    )
    .await
    .unwrap();
    assert!(
        db.read(|conn| Box::pin(async move {
            crate::delivery::job_failure_reason_conn(conn, "child-job").await
        }))
        .await
        .unwrap()
        .is_none(),
        "successful retry and failed memory review are not own failures"
    );
}

#[tokio::test]
async fn failed_resolution_stale_event_cannot_announce_reopened_issue() {
    let (runtime, db) = failure_fixture("failed-resolution-stale").await;
    assert_eq!(
        crate::delivery::create_resolved_push_async(&runtime, &failed_event())
            .await
            .unwrap(),
        0
    );
    assert!(crate::push::list_pending(&db, "watcher")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn failed_resolution_rearm_preserves_mute_and_self_exclusion() {
    let (runtime, db) = failure_fixture("failed-resolution-mute").await;
    crate::wakes::mute(
        &db,
        "watcher",
        "issue",
        Some("cairn://p/proj/2"),
        None,
        None,
        None,
        "agent",
    )
    .await
    .unwrap();
    crate::wakes::subscribe(
        &db,
        "child-job",
        "issue",
        Some("cairn://p/proj/2"),
        None,
        "agent",
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE issues SET status='failed',completed_at=10 WHERE id='issue-1'",
        (),
    )
    .await
    .unwrap();
    assert_eq!(
        crate::delivery::rearm_failed_resolutions(&runtime, &db)
            .await
            .unwrap(),
        1
    );
    let pending = crate::push::list_pending(&db, "watcher").await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].wake, Wake::Passive);
    assert!(crate::push::list_pending(&db, "child-job")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn failed_resolution_rearm_does_not_manufacture_stop_blocked_or_success_notices() {
    let (runtime, db) = failure_fixture("failed-resolution-control-states").await;
    for (issue, job) in [
        ("active", "cancelled"),
        ("active", "blocked"),
        ("active", "pending"),
        ("closed", "complete"),
        ("merged", "complete"),
    ] {
        db.execute("UPDATE issues SET status=?1 WHERE id='issue-1'", (issue,))
            .await
            .unwrap();
        db.execute("UPDATE jobs SET status=?1 WHERE id='child-job'", (job,))
            .await
            .unwrap();
        assert_eq!(
            crate::delivery::rearm_failed_resolutions(&runtime, &db)
                .await
                .unwrap(),
            0
        );
        assert!(crate::push::list_pending(&db, "watcher")
            .await
            .unwrap()
            .is_empty());
    }
}

#[tokio::test]
async fn failed_resolution_action_reason_and_new_job_attempt_are_authoritative() {
    let (runtime, db) = failure_fixture("failed-resolution-action").await;
    db.execute_script(
        "UPDATE issues SET status='failed',completed_at=10 WHERE id='issue-1';
         UPDATE jobs SET recipe_node_id='builder-node' WHERE id='child-job';
         INSERT INTO jobs(id,project_id,issue_id,execution_id,recipe_node_id,uri_segment,status,created_at,updated_at)
         VALUES('successful-retry','p','issue-1','exec-1','builder-node','builder-2','complete',20,20);
         INSERT INTO action_runs(id,execution_id,recipe_node_id,action_config_id,issue_id,project_id,status,
             uri_segment,error_message,created_at,completed_at)
         VALUES('pr-failure','exec-1','pr-node','builtin:pr','issue-1','p','failed',
             'create-pr','GitHub rejected publication',21,22);"
    ).await.unwrap();
    let rendered = render_push_resolved(
        &runtime,
        &push("resolved:cairn://p/proj/2", "cairn://p/proj/2"),
    )
    .await;
    assert!(
        rendered.contains("cairn://p/proj/2/1/create-pr"),
        "{rendered}"
    );
    assert!(
        rendered.contains("GitHub rejected publication"),
        "{rendered}"
    );
    assert!(!rendered.contains("Provider connection lost"), "{rendered}");
    db.execute_script(
        "INSERT INTO action_runs(id,execution_id,recipe_node_id,action_config_id,issue_id,project_id,status,
             uri_segment,created_at,completed_at)
         VALUES('pr-retry','exec-1','pr-node','builtin:pr','issue-1','p','completed','create-pr-2',23,24);"
    ).await.unwrap();
    assert!(
        db.read(|conn| Box::pin(async move {
            crate::delivery::issue_failure_conn(conn, "issue-1").await
        }))
        .await
        .unwrap()
        .is_none(),
        "an older failed action cannot replace a successful retry"
    );
}

#[tokio::test]
async fn failed_resolution_abandoned_output_reason_is_not_blocked_missing_output() {
    let (runtime, db) = failure_fixture("failed-resolution-abandonment").await;
    db.execute_script(
        "UPDATE issues SET status='failed',completed_at=10 WHERE id='issue-1';
         INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,created_at,updated_at)
         VALUES('work-complete','sess','child-job',1,'complete','initial',10,10);
         UPDATE jobs SET status='blocked',return_abandoned_at=11 WHERE id='child-job';",
    )
    .await
    .unwrap();
    assert!(
        db.read(|conn| Box::pin(async move {
            crate::delivery::job_failure_reason_conn(conn, "child-job").await
        }))
        .await
        .unwrap()
        .is_none(),
        "Blocked missing output is not failure evidence"
    );
    db.execute("UPDATE jobs SET status='failed' WHERE id='child-job'", ())
        .await
        .unwrap();
    let rendered = render_push_resolved(
        &runtime,
        &push("resolved:cairn://p/proj/2", "cairn://p/proj/2"),
    )
    .await;
    assert!(rendered.contains(NODE), "{rendered}");
    assert!(
        rendered.contains("Required output was not produced after the return was abandoned"),
        "{rendered}"
    );
    assert!(!rendered.contains("Provider connection lost"), "{rendered}");
    db.execute("UPDATE jobs SET status='complete' WHERE id='child-job'", ())
        .await
        .unwrap();
    assert!(
        db.read(|conn| Box::pin(async move {
            crate::delivery::job_failure_reason_conn(conn, "child-job").await
        }))
        .await
        .unwrap()
        .is_none(),
        "a later projection with output is no longer a failed abandonment"
    );
}

#[tokio::test]
async fn failed_resolution_inherited_branch_producer_is_not_a_delegated_task() {
    let (runtime, db) = failure_fixture("failed-resolution-inherited").await;
    db.execute_script(
        "UPDATE issues SET status='failed',completed_at=10 WHERE id='issue-1';
         INSERT INTO jobs(id,project_id,issue_id,execution_id,recipe_node_id,uri_segment,status,created_at,updated_at)
         VALUES('source','p','issue-1','exec-1','planner','planner','complete',1,1);
         UPDATE jobs SET parent_job_id='source',recipe_node_id='builder' WHERE id='child-job';
         UPDATE executions SET snapshot='{\"recipe\":{\"nodes\":[{\"id\":\"planner\"},{\"id\":\"builder\"}]}}'
         WHERE id='exec-1';
         INSERT INTO jobs(id,project_id,issue_id,execution_id,parent_job_id,uri_segment,status,created_at,updated_at)
         VALUES('delegated','p','issue-1','exec-1','child-job','task','failed',2,20);
         INSERT INTO runs(id,project_id,job_id,issue_id,status,error_message,created_at,updated_at)
         VALUES('task-run','p','delegated','issue-1','crashed','Unrelated delegated task failure',20,20);"
    ).await.unwrap();
    let rendered = render_push_resolved(
        &runtime,
        &push("resolved:cairn://p/proj/2", "cairn://p/proj/2"),
    )
    .await;
    assert!(rendered.contains(NODE), "{rendered}");
    assert!(rendered.contains("Provider connection lost"), "{rendered}");
    assert!(
        !rendered.contains("Unrelated delegated task failure"),
        "{rendered}"
    );
    db.execute_script(
        "INSERT INTO jobs(id,project_id,issue_id,execution_id,recipe_node_id,parent_job_id,uri_segment,status,created_at,updated_at)
         VALUES('inherited-retry','p','issue-1','exec-1','builder','source','builder-2','complete',30,30);"
    ).await.unwrap();
    assert!(
        db.read(|conn| Box::pin(async move {
            crate::delivery::issue_failure_conn(conn, "issue-1").await
        }))
        .await
        .unwrap()
        .is_none(),
        "the newer inherited-branch attempt supersedes the older failure"
    );
}
