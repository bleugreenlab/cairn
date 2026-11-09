//! The owning node's repair for an `unknown` acceptance, driven through the
//! `cairn:~/rebase` resource exactly as a write sends it: a real store, real
//! journal rows, the real gate. A dismissal retires only an `unknown` row of
//! the node's own branch at its literal head, accepts nothing, and moves,
//! exports, publishes and wakes nothing.

use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::base_advance::preparation::tests::{
    git, harness_or_skip, Harness, BRANCH, JOB, MAIN_LINE, RUN,
};
use crate::orchestrator::base_advance::preparation::{
    accepted_drop_claims, journal_store_key, outstanding_acceptances,
};
use crate::orchestrator::{BranchAdmissionRefusal, StoreLockClass};
use crate::storage::{DbError, RowExt};
use std::time::Duration;

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

async fn dismiss(h: &Harness, id: &str, head: &str) -> Result<String, String> {
    send(h, TARGET, Some(RUN), payload(id, head, REASON), false).await
}

/// A writer of `branch` as production admits one.
async fn admit(h: &Harness, branch: &str) -> Result<(), BranchAdmissionRefusal> {
    let reservation = h
        .orch
        .reserve_branches(&h.store, &[branch], "test writer", Duration::from_secs(5))
        .await
        .unwrap();
    let guard = h
        .orch
        .acquire_jj_store_lock(&h.store, StoreLockClass::Foreground, "test writer")
        .await;
    guard
        .branch_write_permit(
            &h.db,
            &reservation,
            "proj-1",
            branch,
            crate::orchestrator::BatchAdmission::None,
        )
        .await
        .map(|_| ())
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
async fn the_owner_dismisses_an_unknown_acceptance_and_its_next_writer_is_admitted_with_no_claims()
{
    let h = harness_or_skip!("dismiss_unknown");
    address(&h).await;
    let base = h.tip("main");
    let tip = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let id = owe(&h, BRANCH, "unknown", &base).await;
    assert!(
        matches!(
            admit(&h, BRANCH).await,
            Err(BranchAdmissionRefusal::Debt { .. })
        ),
        "the unknown row refuses the writer before the repair"
    );
    let before = world(&h).await;

    let summary = dismiss(&h, &id, &tip).await.unwrap();
    assert!(
        summary.contains("Dismissed acceptance") && summary.contains(&id),
        "{summary}"
    );
    assert!(
        summary.contains("discarded, not accepted") && summary.contains("may still refuse"),
        "the reply says what the dismissal does not prove: {summary}"
    );
    assert_eq!(
        row(&h, &id).await,
        (
            "dismissed".to_string(),
            Some(tip.clone()),
            Some(REASON.to_string()),
            true
        ),
        "the row is retired with its repair coordinates, not deleted"
    );
    assert_eq!(
        world(&h).await,
        before,
        "a dismissal moves, exports and wakes nothing"
    );
    assert!(outstanding_acceptances(&h.db, "proj-1", &h.store, BRANCH)
        .await
        .unwrap()
        .is_empty());
    assert!(
        accepted_drop_claims(&h.db, "proj-1", &h.store, BRANCH)
            .await
            .unwrap()
            .is_empty(),
        "a dismissed row's drop claims are never offered to a publication"
    );
    admit(&h, BRANCH)
        .await
        .expect("the branch's next writer is admitted once nothing is owed");

    // Repeating the exact dismissal answers without writing; a different
    // reason or head for the same row is refused.
    let again = dismiss(&h, &id, &tip).await.unwrap();
    assert!(again.contains("already dismissed"), "{again}");
    let refused = send(
        &h,
        TARGET,
        Some(RUN),
        payload(&id, &tip, "a different reason"),
        false,
    )
    .await;
    assert!(refused.is_err(), "{refused:?}");
    assert!(dismiss(&h, &id, &base).await.is_err());
    assert_eq!(
        row(&h, &id).await,
        (
            "dismissed".to_string(),
            Some(tip),
            Some(REASON.to_string()),
            true
        )
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_dismissal_naming_a_stale_or_partial_head_is_refused_and_the_row_stays_owed() {
    let h = harness_or_skip!("dismiss_stale_head");
    address(&h).await;
    let base = h.tip("main");
    let tip = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let id = owe(&h, BRANCH, "unknown", &base).await;
    let before = world(&h).await;

    // A real commit that is not the branch's head now.
    let stale = dismiss(&h, &id, &base).await.unwrap_err();
    assert!(
        stale.contains(&tip) && stale.contains(&base),
        "the refusal names the literal head and the one asked for: {stale}"
    );
    let partial = dismiss(&h, &id, &tip[..12]).await.unwrap_err();
    assert!(partial.contains("full current commit id"), "{partial}");

    assert_eq!(status(&h, &id).await, "unknown");
    assert_eq!(world(&h).await, before);
    assert!(matches!(
        admit(&h, BRANCH).await,
        Err(BranchAdmissionRefusal::Debt { .. })
    ));
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn only_an_unknown_row_of_this_nodes_own_branch_can_be_dismissed() {
    let h = harness_or_skip!("dismiss_only_unknown");
    address(&h).await;
    let base = h.tip("main");
    let tip = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let pending = owe(&h, BRANCH, "pending", &base).await;
    let accepted = owe(&h, BRANCH, "accepted", &base).await;
    let abandoned = owe(&h, BRANCH, "abandoned", &base).await;
    let other_branch = owe(&h, MAIN_LINE, "unknown", &base).await;
    let before = world(&h).await;

    for (id, kept) in [
        (pending.as_str(), "pending"),
        (accepted.as_str(), "accepted"),
        (abandoned.as_str(), "abandoned"),
        (other_branch.as_str(), "unknown"),
    ] {
        let refusal = dismiss(&h, id, &tip).await.unwrap_err();
        assert!(!refusal.is_empty(), "{id}");
        assert_eq!(status(&h, id).await, kept, "{id}: {refusal}");
    }
    // A row that does not exist.
    assert!(dismiss(&h, "acceptance-missing", &tip).await.is_err());
    assert_eq!(
        world(&h).await,
        before,
        "no refused dismissal settles, moves or wakes anything"
    );
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

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_previewed_dismissal_changes_nothing() {
    let h = harness_or_skip!("dismiss_preview");
    address(&h).await;
    let base = h.tip("main");
    let tip = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let id = owe(&h, BRANCH, "unknown", &base).await;
    let before = world(&h).await;

    let preview = send(&h, TARGET, Some(RUN), payload(&id, &tip, REASON), true)
        .await
        .unwrap();
    assert!(preview.starts_with("Would dismiss"), "{preview}");
    assert_eq!(
        row(&h, &id).await,
        ("unknown".to_string(), None, None, false)
    );
    assert_eq!(world(&h).await, before);
}
