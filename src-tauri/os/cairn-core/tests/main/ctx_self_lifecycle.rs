//! End-to-end proof of the `context-self` living-doc lifecycle, driven through
//! the real `write` verb dispatcher (`handle_write`). The recipe is loaded from
//! the authored fixture `fixtures/ctx-self-proof.yaml`, so the test proves an
//! actual recipe-file (with a `context-self` edge that round-trips through the
//! recipe format) drives the lifecycle. Its single agent owns BOTH a terminal
//! `context-out` artifact (`summary`, user-gated) AND a `context-self` living
//! doc (`scratch`, auto). The test proves:
//!
//! - a ctx-self doc can be created and patched repeatedly mid-run;
//! - each ctx-self write keeps its own per-`(job, name)` version chain and never
//!   advances the job (it stays Blocked while the terminal output is missing);
//! - a ctx-self doc is addressable as a named resource;
//! - the terminal `context-out` artifact independently gates (user-confirm) and
//!   advances the job on approve, leaving the ctx-self chain untouched.

use crate::common;

use std::collections::HashMap;
use std::sync::Arc;

use crate::common::orchestrator;
use cairn_core::internal::storage::{DbError, LocalDb, RowExt};
use cairn_core::models::{
    ExecutionSnapshot, RecipeFile, RecipeNodeType, RecipeSnapshot, TriggerContext, TriggerType,
};
use cairn_db::turso::params;
use serde_json::json;

/// Load the authored proof recipe, validate it, and build the execution snapshot
/// the run advances against. Returns the snapshot JSON plus the worker agent's
/// node id (import remaps node ids to fresh UUIDs, so the seeded job's
/// `recipe_node_id` must be the imported id, not the YAML's `worker-1`).
fn proof_snapshot() -> (String, String) {
    let file = RecipeFile::from_yaml(include_str!("../fixtures/ctx-self-proof.yaml"))
        .expect("proof recipe parses");
    let validation = file.validate();
    assert!(
        validation.valid,
        "proof recipe failed validation: {:?}",
        validation.errors
    );
    let recipe = file.into_recipe(None, Some("p-1".to_string()));
    let worker_id = recipe
        .nodes
        .iter()
        .find(|n| n.node_type == RecipeNodeType::Agent)
        .map(|n| n.id.clone())
        .expect("worker agent node");
    // The `context-self` edge must survive import to the snapshot the runtime
    // reads, or the write handler could never route a ctx-self write.
    assert!(
        recipe
            .edges
            .iter()
            .any(|e| e.source_node_id == worker_id && e.source_handle == "context-self"),
        "context-self edge must round-trip through recipe import"
    );
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
    (snapshot.to_json().unwrap(), worker_id)
}

/// Seed project/issue/execution/job + a completed worker turn so job status
/// derives off the gate (not an in-flight turn). Project id is fixed to `p-1` to
/// match the snapshot's trigger context; key `CSL` is the URI namespace. The
/// job's `recipe_node_id` is the imported worker node id; its `uri_segment`
/// stays `worker` (the URI path the agent addresses).
async fn seed(db: &LocalDb) {
    let (snapshot, worker_id) = proof_snapshot();
    db.write(move |conn| {
        let snapshot = snapshot.clone();
        let worker_id = worker_id.clone();
        Box::pin(async move {
            conn.execute("INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1)", ()).await?;
            conn.execute("INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-1','w-1','P','csl','/tmp/p',1,1)", ()).await?;
            // Thread `lead` filed the issue, so it is the gate owner that may
            // confirm the worker's terminal output.
            conn.execute("INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at) VALUES ('thread-lead','p-1','lead','active','none',1,1)", ()).await?;
            conn.execute("INSERT INTO jobs (id, project_id, thread_id, node_name, status, uri_segment, created_at, updated_at) VALUES ('job-lead','p-1','thread-lead','Thread','running','thread',1,1)", ()).await?;
            conn.execute("INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at) VALUES ('run-lead','p-1','job-lead','live',1,1)", ()).await?;
            conn.execute("INSERT INTO issues (id, project_id, number, title, status, attention, parent_thread_id, parent_job_id, created_at, updated_at) VALUES ('i-1','p-1',1,'T','active','none','thread-lead','job-lead',1,1)", ()).await?;
            conn.execute("INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot) VALUES ('exec-1','recipe-1','i-1','p-1','running',1,1,?1)", (snapshot.as_str(),)).await?;
            conn.execute("INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at) VALUES ('job-worker','exec-1',?1,'i-1','p-1','running','worker','worker',1,1)", (worker_id.as_str(),)).await?;
            conn.execute("INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at) VALUES ('s-1','job-worker','codex','open',1,1,1)", ()).await?;
            conn.execute("INSERT INTO turns (id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at) VALUES ('t-1','s-1','job-worker',1,'complete','initial',1,2,2)", ()).await?;
            Ok::<_, DbError>(())
        })
    })
    .await
    .unwrap();
}

async fn versions(db: &LocalDb, output_name: &str) -> Vec<i64> {
    let output_name = output_name.to_string();
    db.read(|conn| {
        let output_name = output_name.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT version FROM artifacts WHERE job_id='job-worker' AND output_name=?1 ORDER BY version ASC",
                    params![output_name.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(row.i64(0)?);
            }
            Ok::<_, DbError>(out)
        })
    })
    .await
    .unwrap()
}

async fn job_status(db: &LocalDb) -> String {
    common::scalar_text_by_id(db, "SELECT status FROM jobs WHERE id = ?1", "job-worker")
        .await
        .unwrap()
}

fn write_to(uri: &str, mode: &str, payload: serde_json::Value) -> serde_json::Value {
    json!([{ "target": uri, "mode": mode, "payload": payload }])
}

#[tokio::test]
async fn ctx_self_living_doc_lifecycle_alongside_terminal_gate() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db).await;
    let orch = orchestrator(&temp, db.clone());

    let scratch_uri = "cairn://p/csl/1/1/worker/scratch";
    let summary_uri = "cairn://p/csl/1/1/worker/summary";

    // --- ctx-self living doc: create then patch repeatedly -----------------
    let r1 = common::change_resource(
        &orch,
        write_to(scratch_uri, "create", json!({"note": "v1"})),
    )
    .await;
    assert!(r1.contains("version 1"), "create -> v1, got: {r1}");
    // Auto-confirm: a living doc never awaits user confirmation.
    assert!(
        !r1.contains("awaiting user confirmation"),
        "ctx-self doc auto-confirms, got: {r1}"
    );
    // A ctx-self write never advances the job: terminal output still missing.
    assert_eq!(job_status(&db).await, "blocked");

    let r2 = common::change_resource(
        &orch,
        write_to(scratch_uri, "patch", json!({"extra": "more"})),
    )
    .await;
    assert!(r2.contains("version 2"), "patch -> v2, got: {r2}");
    assert_eq!(job_status(&db).await, "blocked");

    let r3 =
        common::change_resource(&orch, write_to(scratch_uri, "patch", json!({"note": "v3"}))).await;
    assert!(r3.contains("version 3"), "patch -> v3, got: {r3}");
    assert_eq!(job_status(&db).await, "blocked");

    // Per-name version chain: scratch owns [1,2,3]; the terminal name has none.
    assert_eq!(versions(&db, "scratch").await, vec![1, 2, 3]);
    assert!(versions(&db, "summary").await.is_empty());

    // Addressable as a named resource; patch is a shallow per-key merge, so the
    // first-create `note` plus the later `extra` both survive, with `note`
    // overwritten by v3.
    let rendered = common::read_resource(&orch, scratch_uri).await;
    assert!(
        rendered.contains("v3"),
        "latest note in resource: {rendered}"
    );
    assert!(rendered.contains("more"), "merged key survives: {rendered}");

    // --- terminal output: gates on user-confirm, then advances -------------
    let rt = common::change_resource(
        &orch,
        write_to(summary_uri, "create", json!({"result": "done"})),
    )
    .await;
    assert!(rt.contains("version 1"), "terminal create -> v1, got: {rt}");
    // User policy: the terminal artifact holds for confirmation.
    assert!(
        rt.contains("awaiting user confirmation"),
        "terminal gates on user-confirm, got: {rt}"
    );
    // Terminal present but unconfirmed -> still Blocked.
    assert_eq!(job_status(&db).await, "blocked");

    // The filing thread confirms the gate (the reserved `confirmed` key on a
    // patch).
    let rc = common::change_resource_as_run(
        &orch,
        write_to(summary_uri, "patch", json!({"confirmed": true})),
        Some("run-lead"),
    )
    .await;
    assert!(
        rc.contains("gate resolved"),
        "confirm resolves gate, got: {rc}"
    );
    // The terminal output advances the job.
    assert_eq!(job_status(&db).await, "complete");

    // The ctx-self chain is untouched by the terminal lifecycle.
    assert_eq!(versions(&db, "scratch").await, vec![1, 2, 3]);
    assert_eq!(versions(&db, "summary").await, vec![1]);
}

/// An artifact read tells the node's declared output, a declared context
/// document, and a name the node does not declare apart, in both the body and
/// the affordance. The undeclared name keeps its create offer (a second name
/// beside the output is a designed capability), attributed to the schema it
/// borrows, and a write there neither gates nor hands off.
#[tokio::test]
async fn artifact_reads_tell_declared_names_from_unknown_ones() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db).await;
    let orch = orchestrator(&temp, db.clone());

    let summary = common::read_resource(&orch, "cairn://p/csl/1/1/worker/summary").await;
    let scratch = common::read_resource(&orch, "cairn://p/csl/1/1/worker/scratch").await;
    let zzz = common::read_resource(&orch, "cairn://p/csl/1/1/worker/zzz").await;

    assert_ne!(summary, scratch);
    assert_ne!(summary, zzz);
    assert_ne!(scratch, zzz);
    for read in [&summary, &scratch, &zzz] {
        assert!(!read.contains("in issue"), "{read}");
    }

    assert!(
        summary.contains(
            "`summary` has not been written yet at `cairn://p/csl/1/1/worker/summary`; it is this node's declared output artifact."
        ),
        "{summary}"
    );
    assert!(
        scratch.contains("`scratch` has not been written yet at `cairn://p/csl/1/1/worker/scratch`; it is a context document this node declares."),
        "{scratch}"
    );
    assert!(
        zzz.contains("Nothing is stored at `cairn://p/csl/1/1/worker/zzz`: `zzz` is not an artifact this node declares or has written."),
        "{zzz}"
    );
    assert!(
        zzz.contains("It declares `summary` (output) and `scratch` (context document)."),
        "{zzz}"
    );

    // The affordance: only the undeclared name's offer says it borrows a schema.
    assert!(
        zzz.contains(
            "validated against the schema of the node's declared output artifact `summary`"
        ),
        "{zzz}"
    );
    for read in [&summary, &scratch] {
        assert!(
            !read.contains("not an artifact this node declares"),
            "{read}"
        );
    }

    // The offer is attributed, not suppressed: it stays copy-ready and works.
    assert!(zzz.contains("[write artifact]"), "{zzz}");
    assert!(zzz.contains("target:\"cairn:~/zzz\""), "{zzz}");
    let written = common::change_resource(
        &orch,
        write_to(
            "cairn://p/csl/1/1/worker/zzz",
            "create",
            json!({"result": "borrowed-shape-proof"}),
        ),
    )
    .await;
    assert!(written.contains("version 1"), "{written}");
    assert!(!written.contains("awaiting user confirmation"), "{written}");
    assert_eq!(job_status(&db).await, "blocked");

    let reread = common::read_resource(&orch, "cairn://p/csl/1/1/worker/zzz").await;
    assert!(reread.contains("borrowed-shape-proof"), "{reread}");

    let other = common::read_resource(&orch, "cairn://p/csl/1/1/worker/other").await;
    assert!(other.contains("It has written `zzz`."), "{other}");
}

/// A thread keeps no artifacts and refuses every write to one, so a read of a
/// thread session's artifact address offers no write. Its sub-agent tasks keep
/// ordinary return artifacts, and an empty one names its own thread address
/// rather than a reserved issue coordinate.
#[tokio::test]
async fn thread_artifact_addresses_name_themselves_and_offer_no_refused_write() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    seed(&db).await;
    db.execute(
        "INSERT INTO jobs (id, project_id, parent_job_id, node_name, status, uri_segment, created_at, updated_at) VALUES ('job-lead-task','p-1','job-lead','explore','complete','explore',2,2)",
        (),
    )
    .await
    .unwrap();
    let orch = orchestrator(&temp, db.clone());

    for address in ["cairn://p/csl/lead/zzz", "cairn://p/csl/lead/arc"] {
        let read = common::read_resource(&orch, address).await;
        assert!(
            read.contains("A thread keeps no artifacts"),
            "{address}: {read}"
        );
        assert!(!read.contains("[write artifact]"), "{address}: {read}");
        assert!(!read.contains("mode:\"create\""), "{address}: {read}");
    }

    let task = common::read_resource(&orch, "cairn://p/csl/lead/task/explore/zzz").await;
    assert!(
        task.contains("Nothing is stored at `cairn://p/csl/lead/task/explore/zzz`: `zzz` is not an artifact this task declares"),
        "{task}"
    );
    assert!(!task.contains("/0"), "{task}");
    assert!(!task.contains("in issue"), "{task}");
    assert!(!task.contains("of issue"), "{task}");
}
