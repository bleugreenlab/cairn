//! A plan revised after its builder launched reaches that builder.
//!
//! Driven through the real `write` dispatcher against the bundled planbuild
//! recipe: the planner writes and confirms its plan, the builder's launch
//! inputs are composed exactly as `prepare_job` composes them (recording the
//! version they embed), and a later patch to the plan must arrive at the
//! builder as one queued message carrying the diff. The review node, wired to
//! the plan through its `context-ref` port, launches with the plan's identity
//! rather than its text.

use crate::common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use cairn_core::internal::db::DbState;
use cairn_core::internal::execution::artifact_revisions::{
    deliver_artifact_revision, deliver_pending_revisions,
};
use cairn_core::internal::execution::jobs::compose_launch_inputs;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::TestServicesBuilder;
use cairn_core::internal::storage::{DbError, LocalDb, RowExt, SearchIndex};
use cairn_core::models::{
    ExecutionSnapshot, RecipeFile, RecipeNodeType, RecipeSnapshot, TriggerContext, TriggerType,
};
use cairn_db::turso::params;
use serde_json::json;
use tempfile::TempDir;

const PLAN_URI: &str = "cairn://p/arv/1/1/planner/plan";

/// The bundled planbuild recipe as an execution snapshot, plus the imported
/// node id of each agent keyed by its agent config (`planner`, `build`,
/// `pr-review`). Import remaps node ids, so seeded jobs must use these.
fn planbuild_snapshot() -> (String, HashMap<String, String>) {
    let file = RecipeFile::from_yaml(include_str!(
        "../../../../packs/core/recipes/planbuild.yaml"
    ))
    .expect("planbuild parses");
    let validation = file.validate();
    assert!(validation.valid, "planbuild: {:?}", validation.errors);
    let recipe = file.into_recipe(None, Some("p-1".to_string()));
    let agents = recipe
        .nodes
        .iter()
        .filter(|n| n.node_type == RecipeNodeType::Agent)
        .filter_map(|n| {
            let agent = n.agent_config.as_ref()?.agent_config_id.clone()?;
            Some((agent, n.id.clone()))
        })
        .collect::<HashMap<_, _>>();
    let snapshot = ExecutionSnapshot::new(
        RecipeSnapshot {
            id: recipe.id.clone(),
            name: recipe.name.clone(),
            description: recipe.description.clone(),
            trigger: recipe.trigger.clone(),
            nodes: recipe.nodes.clone(),
            edges: recipe.edges.clone(),
        },
        HashMap::new(),
        HashMap::new(),
        TriggerContext {
            issue_id: Some("i-1".to_string()),
            project_id: "p-1".to_string(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
    );
    (snapshot.to_json().unwrap(), agents)
}

/// Project, issue, execution, and the three agent jobs. The planner has a
/// finished turn so its status derives from the plan gate; the builder and
/// reviewer each have a session and a run to receive messages on.
async fn seed(db: &LocalDb) {
    let (snapshot, agents) = planbuild_snapshot();
    let planner = agents["planner"].clone();
    let builder = agents["build"].clone();
    let review = agents["pr-review"].clone();
    db.write(move |conn| {
        let snapshot = snapshot.clone();
        let (planner, builder, review) = (planner.clone(), builder.clone(), review.clone());
        Box::pin(async move {
            conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)", ()).await?;
            conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','arv','/tmp/p',1,1)", ()).await?;
            conn.execute("INSERT INTO issues (id, project_id, number, title, description, status, attention, created_at, updated_at) VALUES ('i-1','p-1',1,'Match crossings','Raise a crossing on permission errors','active','none',1,1)", ()).await?;
            conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('exec-1','recipe-1','i-1','p-1','running',1,1,?1)", (snapshot.as_str(),)).await?;
            for (id, node, segment, status) in [
                ("job-planner", planner.as_str(), "planner", "running"),
                ("job-builder", builder.as_str(), "builder", "running"),
                ("job-review", review.as_str(), "review", "running"),
            ] {
                conn.execute(
                    "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES (?1,'exec-1',?2,'i-1','p-1',?3,?4,?4,1,1)",
                    params![id, node, status, segment],
                )
                .await?;
            }
            conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-planner','job-planner','codex','open',1,1,1)", ()).await?;
            conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-planner','s-planner','job-planner',1,'complete','initial',1,2,2)", ()).await?;
            for job in ["builder", "review"] {
                conn.execute(
                    "INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES (?1, ?2, 'codex', 'open', 1, 1, 1)",
                    params![format!("s-{job}"), format!("job-{job}")],
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs (id, issue_id, project_id, job_id, status, session_id, created_at, updated_at, start_mode) VALUES (?1, 'i-1', 'p-1', ?2, 'live', ?3, 1, 1, 'fresh')",
                    params![format!("run-{job}"), format!("job-{job}"), format!("s-{job}")],
                )
                .await?;
            }
            Ok::<_, DbError>(())
        })
    })
    .await
    .unwrap();
}

/// A fixture orchestrator whose revision quiet period is `quiet`, so the real
/// write path delivers within the test.
fn orchestrator(temp: &TempDir, db: Arc<LocalDb>, quiet: Duration) -> Orchestrator {
    let search_index = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db, search_index));
    let services = Arc::new(TestServicesBuilder::new().build());
    Orchestrator::builder(db_state, services, temp.path().join("config"))
        .boot_at(0)
        .artifact_revision_quiet(quiet)
        .build()
}

async fn write(orch: &Orchestrator, changes: serde_json::Value) -> String {
    common::change_resource(orch, changes).await
}

fn plan(content: &str) -> serde_json::Value {
    json!({"title": "Match colon forms", "summary": "Crossing matcher", "content": content})
}

/// A plan-length body with one rule line, so a one-line revision diffs small.
fn plan_body(rule: &str) -> String {
    let mut lines: Vec<String> = (1..=30).map(|n| format!("{n}. plan step {n}")).collect();
    lines[14] = rule.to_string();
    lines.join("\n")
}

/// Write plan v1 and confirm it, as a planner and its thread do before the
/// builder launches.
async fn planner_writes_and_confirms(orch: &Orchestrator, content: &str) {
    let created = write(
        orch,
        json!([{"target": PLAN_URI, "mode": "create", "payload": plan(content)}]),
    )
    .await;
    assert!(created.contains("version 1"), "{created}");
    let confirmed = write(
        orch,
        json!([{"target": PLAN_URI, "mode": "patch", "payload": {"confirmed": true}}]),
    )
    .await;
    assert!(confirmed.contains("gate resolved"), "{confirmed}");
}

async fn launch(db: &Arc<LocalDb>, job_id: &str) -> String {
    compose_launch_inputs(db.clone(), job_id.to_string())
        .await
        .expect("launch inputs compose")
        .0
}

async fn messages_to(db: &LocalDb, run_id: &str) -> Vec<String> {
    let run_id = run_id.to_string();
    db.read(move |conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT content FROM messages WHERE recipient_run_id = ?1 ORDER BY created_at, id",
                    params![run_id.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(row.text(0)?);
            }
            Ok::<_, DbError>(out)
        })
    })
    .await
    .unwrap()
}

async fn held_version(db: &LocalDb, job_id: &str) -> Option<i64> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT version FROM job_artifact_inputs WHERE job_id = ?1 AND producer_job_id = 'job-planner' AND output_name = 'plan'",
                    params![job_id.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok::<_, DbError>(Some(row.i64(0)?)),
                None => Ok(None),
            }
        })
    })
    .await
    .unwrap()
}

/// Wait until `job_id`'s recorded plan version reaches `version`. Delivery
/// queues the message first and advances the record after, so a test that saw
/// the message must still wait for the record. Returns what it last read.
async fn await_held_version(db: &LocalDb, job_id: &str, version: i64) -> Option<i64> {
    for _ in 0..300 {
        let held = held_version(db, job_id).await;
        if held == Some(version) {
            return held;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    held_version(db, job_id).await
}

/// Wait until `run_id` has received `count` messages, or give up after a bound
/// generous enough for the quiet period plus delivery on a loaded test lane.
async fn await_messages(db: &LocalDb, run_id: &str, count: usize) -> Vec<String> {
    for _ in 0..300 {
        let messages = messages_to(db, run_id).await;
        if messages.len() >= count {
            return messages;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    messages_to(db, run_id).await
}

/// The acceptance case: a builder launched from v1, then a plan patch to v2,
/// receives exactly one message naming the plan, both versions, and who
/// patched it, with the v1-to-v2 diff.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plan_patched_after_launch_reaches_the_builder_once_with_the_diff() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db).await;
    let orch = orchestrator(&temp, db.clone(), Duration::from_millis(100));

    planner_writes_and_confirms(&orch, &plan_body("Match bare phrases, case-insensitively.")).await;
    let prompt = launch(&db, "job-builder").await;
    assert!(
        prompt.contains(&format!("version 1 of `{PLAN_URI}`")),
        "the launch prompt names the plan and the version it embeds: {prompt}"
    );
    assert!(prompt.contains("The artifact is authoritative"), "{prompt}");
    assert!(prompt.contains("Match bare phrases"), "{prompt}");
    assert_eq!(held_version(&db, "job-builder").await, Some(1));

    let patched = write(
        &orch,
        json!([{"target": PLAN_URI, "mode": "patch", "payload": {"content": plan_body("Match exact colon-prefixed Go forms.")}}]),
    )
    .await;
    assert!(patched.contains("version 2"), "{patched}");

    let messages = await_messages(&db, "run-builder", 1).await;
    assert_eq!(
        messages.len(),
        1,
        "exactly one revision message: {messages:#?}"
    );
    let message = &messages[0];
    assert!(
        message.contains(&format!("`{PLAN_URI}` is now version 2")),
        "{message}"
    );
    assert!(message.contains("working from version 1"), "{message}");
    assert!(message.contains("Revised by the operator."), "{message}");
    assert!(
        message.contains("Changes from version 1 to version 2"),
        "{message}"
    );
    assert!(
        message.contains("-Match bare phrases, case-insensitively."),
        "{message}"
    );
    assert!(
        message.contains("+Match exact colon-prefixed Go forms."),
        "{message}"
    );
    assert_eq!(await_held_version(&db, "job-builder", 2).await, Some(2));

    // Settled: nothing further arrives, and re-running delivery sends nothing.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(messages_to(&db, "run-builder").await.len(), 1);
    assert_eq!(
        deliver_artifact_revision(&orch, "job-planner", "plan", &[]).await,
        Ok(0)
    );
}

/// A burst of patches arrives as one message with the net change, and a later
/// patch that leaves the content byte-identical delivers nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_burst_coalesces_and_an_unchanged_version_delivers_nothing() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db).await;
    let orch = orchestrator(&temp, db.clone(), Duration::from_secs(3));

    planner_writes_and_confirms(&orch, &plan_body("rule A")).await;
    launch(&db, "job-builder").await;

    // 5073's shape: several versions inside one quiet period, the last of
    // which changes nothing.
    let burst = write(
        &orch,
        json!([
            {"target": PLAN_URI, "mode": "patch", "payload": {"content": plan_body("rule B")}},
            {"target": PLAN_URI, "mode": "patch", "payload": {"content": plan_body("rule C")}},
            {"target": PLAN_URI, "mode": "patch", "payload": {"content": plan_body("rule C")}},
        ]),
    )
    .await;
    assert!(burst.contains("version 4"), "{burst}");

    let messages = await_messages(&db, "run-builder", 1).await;
    tokio::time::sleep(Duration::from_millis(900)).await;
    let messages_after = messages_to(&db, "run-builder").await;
    assert_eq!(
        messages_after.len(),
        1,
        "one message for the burst: {messages_after:#?}"
    );
    let message = &messages[0];
    assert!(message.contains("is now version 4"), "{message}");
    assert!(message.contains("working from version 1"), "{message}");
    assert!(
        message.contains("-rule A") && message.contains("+rule C"),
        "net diff: {message}"
    );
    assert!(
        !message.contains("rule B"),
        "an intermediate version is not the change: {message}"
    );
    assert_eq!(await_held_version(&db, "job-builder", 4).await, Some(4));

    // v5 carries the same content as the v4 the builder now holds.
    let unchanged = write(
        &orch,
        json!([{"target": PLAN_URI, "mode": "patch", "payload": {"content": plan_body("rule C")}}]),
    )
    .await;
    assert!(unchanged.contains("version 5"), "{unchanged}");
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        messages_to(&db, "run-builder").await.len(),
        1,
        "an unchanged version delivers nothing"
    );
    assert_eq!(await_held_version(&db, "job-builder", 5).await, Some(5));
}

/// A consumer that has already finished is not told about a later revision.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_terminal_consumer_receives_nothing() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db).await;
    let orch = orchestrator(&temp, db.clone(), Duration::from_millis(100));

    planner_writes_and_confirms(&orch, &plan_body("rule A")).await;
    launch(&db, "job-builder").await;
    // A finished builder, by the facts its status derives from: its terminal
    // handoff is written and its turn has ended.
    db.execute_script(
        "INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at)
           VALUES ('a-pr','job-builder','create-pr',1,'{\"title\":\"t\",\"body\":\"b\"}',1,'create-pr',2,2);
         INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at)
           VALUES ('t-builder','s-builder','job-builder',1,'complete','initial',2,3,3);
         UPDATE jobs SET status = 'complete' WHERE id = 'job-builder';",
    )
    .await
    .unwrap();

    write(
        &orch,
        json!([{"target": PLAN_URI, "mode": "patch", "payload": {"content": plan_body("rule B")}}]),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        common::scalar_text_by_id(&db, "SELECT status FROM jobs WHERE id = ?1", "job-builder")
            .await
            .as_deref(),
        Some("complete"),
        "the builder stays finished through the plan's recompute"
    );
    assert!(messages_to(&db, "run-builder").await.is_empty());
    assert_eq!(
        deliver_artifact_revision(&orch, "job-planner", "plan", &[]).await,
        Ok(0)
    );
    assert_eq!(held_version(&db, "job-builder").await, Some(1));
}

/// The shape threads use most: patch the plan and confirm it in one write
/// batch, then patch again. The launch must embed the same-batch patch (the
/// confirm resolves the gate; inputs resolve at launch, after the batch), and
/// the later patch must arrive as exactly one revision.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_and_confirm_in_one_batch_launches_the_patched_plan() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db).await;
    let orch = orchestrator(&temp, db.clone(), Duration::from_millis(100));

    let created = write(
        &orch,
        json!([{"target": PLAN_URI, "mode": "create", "payload": plan(&plan_body("pre-patch rule"))}]),
    )
    .await;
    assert!(created.contains("awaiting user confirmation"), "{created}");
    let batch = write(
        &orch,
        json!([
            {"target": PLAN_URI, "mode": "patch", "payload": {"content": plan_body("patched rule")}},
            {"target": PLAN_URI, "mode": "patch", "payload": {"confirmed": true}},
        ]),
    )
    .await;
    assert!(
        batch.contains("version 2") && batch.contains("gate resolved"),
        "{batch}"
    );

    let prompt = launch(&db, "job-builder").await;
    assert!(prompt.contains("patched rule"), "{prompt}");
    assert!(!prompt.contains("pre-patch rule"), "{prompt}");
    assert!(
        prompt.contains(&format!("version 2 of `{PLAN_URI}`")),
        "{prompt}"
    );

    write(
        &orch,
        json!([{"target": PLAN_URI, "mode": "patch", "payload": {"content": plan_body("amended rule")}}]),
    )
    .await;
    let messages = await_messages(&db, "run-builder", 1).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        messages_to(&db, "run-builder").await.len(),
        1,
        "{messages:#?}"
    );
    assert!(
        messages[0].contains("is now version 3") && messages[0].contains("working from version 2")
    );
    assert!(messages[0].contains("-patched rule") && messages[0].contains("+amended rule"));
}

/// A restart inside the quiet period loses the in-memory timer but not the
/// record. The startup pass finds the consumer behind the latest version and
/// delivers exactly once; a second pass, like a second restart, sends nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_startup_pass_delivers_a_revision_a_restart_left_pending() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db).await;
    // A quiet period no test outlasts: any delivery here comes from the pass.
    let orch = orchestrator(&temp, db.clone(), Duration::from_secs(3600));

    planner_writes_and_confirms(&orch, &plan_body("rule A")).await;
    launch(&db, "job-builder").await;
    assert_eq!(held_version(&db, "job-builder").await, Some(1));

    // What a restart leaves behind: v2 stored, no timer to deliver it.
    let v2 = serde_json::to_string(&plan(&plan_body("rule B"))).unwrap();
    db.execute(
        "INSERT INTO artifacts (id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at)
         VALUES ('a-plan-v2', 'job-planner', 'plan', 1, ?1, 2, 'plan', 5, 5)",
        params![v2.as_str()],
    )
    .await
    .unwrap();

    assert_eq!(deliver_pending_revisions(&orch).await, 1);
    let messages = messages_to(&db, "run-builder").await;
    assert_eq!(messages.len(), 1, "{messages:#?}");
    assert!(messages[0].contains("is now version 2"), "{}", messages[0]);
    assert!(messages[0].contains("-rule A") && messages[0].contains("+rule B"));
    assert!(
        !messages[0].contains("Revised by"),
        "the writer was lost with the timer"
    );
    assert_eq!(held_version(&db, "job-builder").await, Some(2));

    assert_eq!(deliver_pending_revisions(&orch).await, 0);
    assert_eq!(messages_to(&db, "run-builder").await.len(), 1);
}

/// The review node launches with the plan's URI and current version, never its
/// text, and hears about a later revision like any other consumer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_review_node_launches_with_the_plan_by_reference() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db).await;
    let orch = orchestrator(&temp, db.clone(), Duration::from_millis(100));

    planner_writes_and_confirms(&orch, &plan_body("PLAN BODY RULE")).await;
    write(
        &orch,
        json!([{"target": PLAN_URI, "mode": "patch", "payload": {"content": plan_body("REVISED BODY RULE")}}]),
    )
    .await;

    let prompt = launch(&db, "job-review").await;
    assert!(
        prompt.contains(&format!("`{PLAN_URI}` (version 2 when you launched)")),
        "{prompt}"
    );
    assert!(prompt.contains("authoritative over any copy"), "{prompt}");
    assert!(!prompt.contains("BODY RULE"), "no plan text: {prompt}");
    assert_eq!(held_version(&db, "job-review").await, Some(2));
}
