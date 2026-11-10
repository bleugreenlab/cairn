//! The owning node's repair for an `unknown` acceptance, driven through the
//! `cairn:~/rebase` resource exactly as a write sends it: a real store, real
//! journal rows, the real gate. A dismissal retires only an `unknown` row of
//! the node's own branch at its literal head, accepts nothing, and moves,
//! exports, publishes and wakes nothing.

use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::base_advance::preparation::journal_store_key;
use crate::orchestrator::base_advance::preparation::tests::{
    git, harness_or_skip, Harness, BRANCH, JOB, MAIN_LINE,
};
use crate::storage::{DbError, RowExt};

const TARGET: &str = "cairn://p/proj/1/1/builder/rebase";
const FOREIGN_RUN: &str = "run-foreign";
const REASON: &str = "the owner rewrote past this install; its drops are not needed";
/// An install's resulting head that no bookmark names.
const LOST_HEAD: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
/// A drop claim that must never reach a publication's proof.
const CLAIM: &str = "dddddddddddddddddddddddddddddddddddddddd";

/// Make the harness's job addressable as `builder`, and add a second node with
/// its own branch and live run.
async fn address(h: &Harness) {
    h.db.execute_script(format!(
        "UPDATE jobs SET uri_segment = 'builder' WHERE id = '{JOB}';
         INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, uri_segment, created_at, updated_at)
           VALUES ('job-foreign', 'exec-1', 'other', 'issue-1', 'proj-1', 'running', '{MAIN_LINE}', NULL, 'other', 2, 2);
         INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
           VALUES ('{FOREIGN_RUN}', 'issue-1', 'proj-1', 'job-foreign', 'live', 2, 2);"
    ))
    .await
    .unwrap();
}

async fn owe(h: &Harness, branch: &str, status: &str, head_before: &str) -> String {
    let id = format!("acceptance-{}", uuid::Uuid::new_v4());
    let key = journal_store_key(&h.store).unwrap();
    let claims = format!("[\"{CLAIM}\"]");
    h.db.execute(
        "INSERT INTO branch_advance_acceptances
           (id, project_id, store_path, branch, head_before, head_after, base_branch,
            base_commit, captured_operation, dropped_commits_json, inherited_drops_json,
            status, created_at)
         VALUES (?1, 'proj-1', ?2, ?3, ?4, ?5, 'main', ?4, 'op', ?6, '[]', ?7, 1)",
        cairn_db::turso::params![
            id.as_str(),
            key.as_str(),
            branch,
            head_before,
            LOST_HEAD,
            claims.as_str(),
            status
        ],
    )
    .await
    .unwrap();
    id
}

/// `(status, repair_head, repair_reason, dismissed_at is set)`.
async fn row(h: &Harness, id: &str) -> (String, Option<String>, Option<String>, bool) {
    let id = id.to_string();
    h.db.read(move |conn| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status, repair_head, repair_reason, dismissed_at IS NOT NULL
                       FROM branch_advance_acceptances WHERE id = ?1",
                    (id.as_str(),),
                )
                .await?;
            let row = rows.next().await?.expect("the row exists");
            Ok::<_, DbError>((
                row.text(0)?,
                row.opt_text(1)?,
                row.opt_text(2)?,
                row.i64(3)? != 0,
            ))
        })
    })
    .await
    .unwrap()
}

async fn status(h: &Harness, id: &str) -> String {
    row(h, id).await.0
}

fn payload(id: &str, head: &str, reason: &str) -> serde_json::Value {
    serde_json::json!({
        "action": "dismiss-acceptance",
        "acceptance_id": id,
        "expected_head": head,
        "reason": reason,
    })
}

async fn send(
    h: &Harness,
    target: &str,
    run: Option<&str>,
    payload: serde_json::Value,
    dry_run: bool,
) -> Result<String, String> {
    let item = ChangeItem {
        target: target.into(),
        mode: ChangeMode::Patch,
        payload: Some(payload),
    };
    let request = McpCallbackRequest {
        thread_id: None,
        cwd: String::new(),
        run_id: run.map(str::to_string),
        tool: "write".into(),
        payload: serde_json::Value::Null,
        tool_use_id: None,
        answer_bound_ms: None,
        origin: None,
    };
    crate::resources::mutations::dispatch_resource_change(&h.orch, &request, 0, &item, dry_run)
        .await
        .map(|applied| applied.summary)
        .map_err(|failure| failure.error)
}

/// Everything a dismissal must leave alone: both bookmarks, every Git ref in
/// the repository, the store's operation, and every message and wake.
#[derive(Debug, PartialEq)]
struct World {
    branch: String,
    main_line: String,
    git_refs: String,
    operation: String,
    messages: Vec<(String, String)>,
    wakes: Vec<String>,
}

async fn world(h: &Harness) -> World {
    World {
        branch: h.tip(BRANCH),
        main_line: h.tip(MAIN_LINE),
        git_refs: git(
            &h.repo,
            &["for-each-ref", "--format=%(refname) %(objectname)"],
        ),
        operation: crate::jj::operation_id(&h.jj, &h.store).unwrap(),
        messages: h.messages().await,
        wakes: h.wakes().await,
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_foreign_node_or_a_request_from_no_run_cannot_dismiss() {
    let h = harness_or_skip!("dismiss_foreign");
    address(&h).await;
    let base = h.tip("main");
    let tip = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let id = owe(&h, BRANCH, "unknown", &base).await;
    let before = world(&h).await;

    for run in [Some(FOREIGN_RUN), None] {
        let refusal = send(&h, TARGET, run, payload(&id, &tip, REASON), false)
            .await
            .unwrap_err();
        assert!(refusal.contains("Unauthorized"), "{run:?}: {refusal}");
        // The preview answers exactly what the apply would.
        let previewed = send(&h, TARGET, run, payload(&id, &tip, REASON), true)
            .await
            .unwrap_err();
        assert!(previewed.contains("Unauthorized"), "{run:?}: {previewed}");
    }
    assert_eq!(status(&h, &id).await, "unknown");
    assert_eq!(world(&h).await, before);
}
