use super::{create_review_push_for_pr_open, evaluate_review_readiness};
use crate::db::DbState;
use crate::orchestrator::attention_push::{
    latest_push_fingerprint, list_pending, stamp_delivered, Boundary, Push, Wake,
};
use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
use crate::services::testing::TestServicesBuilder;
use crate::storage::{LocalDb, SearchIndex};
use std::sync::Arc;

const ISSUE_URI: &str = "cairn://p/PRJ/7";
const REVIEW_KEY: &str = "review:cairn://p/PRJ/7";

async fn test_db() -> LocalDb {
    crate::storage::migrated_test_db("review-push.db").await
}

fn test_orchestrator(db: LocalDb) -> Orchestrator {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.keep();
    let config_dir = root.join("config");
    std::fs::create_dir_all(config_dir.join("agents")).unwrap();
    std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
    let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
    let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
    let services = Arc::new(TestServicesBuilder::new().build());
    OrchestratorBuilder::new(db_state, services, config_dir).build()
}

/// Producing builder node `j-prod` (issue `i-rev` / `cairn://p/PRJ/7`, exec
/// seq 1) whose just-ended turn carries `start_reason`, a watcher job
/// `j-watch`, and an active issue subscription for BOTH so the producing
/// node's self-exclusion is exercised.
async fn seed(db: &LocalDb, start_reason: &str) {
    db.execute_script(&format!(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p-rev','w','Project','PRJ','/tmp/repo',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('i-rev','p-rev',7,'Rev','active','active','none',1,1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES('e-rev','r','i-rev','p-rev','running',1,1);
            INSERT INTO jobs(id, execution_id, project_id, issue_id, status, uri_segment, node_name, branch, worktree_path, created_at, updated_at)
              VALUES('j-prod','e-rev','p-rev','i-rev','complete','builder','builder','b','/tmp/wt',1,1);
            INSERT INTO jobs(id, project_id, issue_id, status, node_name, created_at, updated_at)
              VALUES('j-watch','p-rev','i-rev','running','watcher',1,1);
            INSERT INTO runs(id, project_id, job_id, issue_id, created_at, updated_at)
              VALUES('r-prod','p-rev','j-prod','i-rev',1,1);
            INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
              VALUES('t-prod','s-prod','j-prod',1,'complete','{start_reason}',1,1);
            INSERT INTO wake_subscriptions(id, job_id, source_kind, source_ref, state, created_by, created_at, updated_at, one_shot)
              VALUES('sub-watch','j-watch','issue','{ISSUE_URI}','active','agent',1,1,0);
            INSERT INTO wake_subscriptions(id, job_id, source_kind, source_ref, state, created_by, created_at, updated_at, one_shot)
              VALUES('sub-prod','j-prod','issue','{ISSUE_URI}','active','agent',1,1,0);
            "
        ))
        .await
        .unwrap();
}

async fn insert_open_pr(db: &LocalDb) {
    db.execute_script(
            "INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES('mr-rev','j-prod','p-rev','i-rev','t','b','main','open',1,1);",
        )
        .await
        .unwrap();
}

async fn insert_artifact(db: &LocalDb, artifact_type: &str, confirmed: i64) {
    db.execute_script(&format!(
            "INSERT INTO artifacts
               (id, job_id, artifact_type, schema_version, data, version, output_name, confirmed, created_at, updated_at)
             VALUES('a-rev','j-prod','{artifact_type}',1,'{{}}',1,'{artifact_type}',{confirmed},1,1);"
        ))
        .await
        .unwrap();
}

async fn pending(orch: &Orchestrator, recipient: &str) -> Vec<Push> {
    list_pending(&orch.db.local, recipient).await.unwrap()
}

async fn run_review_push(orch: &Orchestrator) {
    // Both trigger edges now run through the single readiness evaluator; this
    // helper models the turn-end / checks-completion edge (fresh_red = false).
    evaluate_review_readiness(orch, "i-rev", false).await;
}

async fn run_pr_open(orch: &Orchestrator) {
    // The PR-open edge now defers to the same readiness evaluator; the source
    // branch is retained only for the caller's signature/logging.
    create_review_push_for_pr_open(orch, "i-rev", "b").await;
}

#[tokio::test]
async fn work_idle_with_unconfirmed_create_pr_artifact_pushes_review() {
    // The create-pr idle: the PR is not open yet, but the unconfirmed
    // artifact is observable -> the second predicate arm fires.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_artifact(&db, "create-pr", 0).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;

    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(watcher.len(), 1);
    assert_eq!(watcher[0].key, REVIEW_KEY);
    assert!(watcher[0].content_ref.contains("/builder/"));
    // The producing node is never a recipient of its own review.
    assert!(pending(&orch, "j-prod").await.is_empty());
}

#[tokio::test]
async fn work_idle_with_open_pr_pushes_review() {
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;

    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(watcher.len(), 1);
    assert_eq!(watcher[0].key, REVIEW_KEY);
    assert!(watcher[0]
        .content_ref
        .starts_with("cairn://p/PRJ/7/1/builder"));
    assert!(pending(&orch, "j-prod").await.is_empty());
}

#[tokio::test]
async fn settled_after_memory_review_completes_fires_review() {
    // The common build path: the builder's WORK turn produced the PR, then a
    // trailing memory-review turn ran and completed. Once that reflection turn
    // terminalizes the issue is settled, so the review fires normally — there is
    // deliberately no separate memory-review gate that would block it forever
    // (a builder's latest turn is permanently its memory-review turn; CAIRN-2483).
    let db = test_db().await;
    seed(&db, "memory_review").await; // t-prod: memory_review turn, state complete
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;

    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(watcher.len(), 1);
    assert_eq!(watcher[0].key, REVIEW_KEY);
}

#[tokio::test]
async fn work_idle_with_confirmed_create_pr_artifact_pushes_review() {
    // CAIRN-1999 shape: the create-pr artifact was already confirmed by the
    // artifact lifecycle, but the parent still needs a review push for the
    // child output even if no PR-open edge creates one.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_artifact(&db, "create-pr", 1).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;

    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(watcher.len(), 1);
    assert_eq!(watcher[0].key, REVIEW_KEY);
    assert!(watcher[0].content_ref.contains("/builder/"));
    assert!(pending(&orch, "j-prod").await.is_empty());
}

#[tokio::test]
async fn work_idle_without_reviewable_output_no_push() {
    // A work turn with neither an open PR nor a create-pr/unconfirmed-plan
    // artifact -> nothing reviewable.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_artifact(&db, "plan", 1).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;

    assert!(pending(&orch, "j-watch").await.is_empty());
}

#[tokio::test]
async fn successive_work_idles_collapse_to_one_undelivered() {
    // Two work-turn idles with the SAME reviewable state and no delivery in
    // between yield one undelivered review row: the first creates it, the
    // second is skipped by the change-trigger (CAIRN-1889) because the
    // undelivered push already carries the same fingerprint.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;
    run_review_push(&orch).await;

    assert_eq!(pending(&orch, "j-watch").await.len(), 1);
}

#[tokio::test]
async fn unchanged_fingerprint_skips_review_even_after_delivery() {
    // One review fires (fp=A); after it is delivered, a second work-turn idle
    // with the SAME reviewable state must NOT re-create a review push.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;
    let first = pending(&orch, "j-watch").await;
    assert_eq!(first.len(), 1);

    // Deliver the first push: it leaves the supersede partial index but stays
    // in the table for the fingerprint lookup.
    stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
        .await
        .unwrap();
    assert!(pending(&orch, "j-watch").await.is_empty());

    // Same diffstat -> skipped, no re-wake.
    run_review_push(&orch).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "an unchanged reviewable state must not re-create a review push"
    );
}

#[tokio::test]
async fn changed_diffstat_creates_new_review_after_delivery() {
    // New commits change the diffstat -> a fresh review push, even after the
    // first was delivered.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;
    let first = pending(&orch, "j-watch").await;
    assert_eq!(first.len(), 1);
    stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
        .await
        .unwrap();

    orch.db
        .local
        .execute_script("UPDATE merge_requests SET additions=10, deletions=2 WHERE id='mr-rev';")
        .await
        .unwrap();
    run_review_push(&orch).await;
    let second = pending(&orch, "j-watch").await;
    assert_eq!(second.len(), 1, "a changed diffstat re-creates the review");
    assert_ne!(second[0].id, first[0].id);
}

#[tokio::test]
async fn mergeability_only_change_does_not_refire_review() {
    // A mergeability settle touches non-diffstat columns only -> same
    // fingerprint -> no new review push.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_review_push(&orch).await;
    let first = pending(&orch, "j-watch").await;
    assert_eq!(first.len(), 1);
    stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
        .await
        .unwrap();

    orch.db
            .local
            .execute_script(
                "UPDATE merge_requests SET github_mergeable='MERGEABLE', updated_at=999 WHERE id='mr-rev';",
            )
            .await
            .unwrap();
    run_review_push(&orch).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "a mergeability-only settle must not re-create a review push"
    );
}

// --- CAIRN-1891: the PR-open edge of the review push ---------------------

#[tokio::test]
async fn pr_open_with_quiescent_producer_pushes_one_review() {
    // The producing builder's head turn is complete (quiescent) and the PR is
    // now open -> exactly one review to the watcher, never to the producing
    // node itself. This is the wake the create-pr idle edge cannot fire.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;

    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(watcher.len(), 1);
    assert_eq!(watcher[0].key, REVIEW_KEY);
    assert!(watcher[0]
        .content_ref
        .starts_with("cairn://p/PRJ/7/1/builder"));
    assert!(pending(&orch, "j-prod").await.is_empty());
}

#[tokio::test]
async fn pr_open_with_running_producer_does_not_push() {
    // The quiescence gate: a producing node still mid-turn (a `synchronize`
    // landing during active work) does NOT fire a review.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    db.execute_script("UPDATE turns SET state='running' WHERE id='t-prod';")
        .await
        .unwrap();
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;

    assert!(pending(&orch, "j-watch").await.is_empty());
}

#[tokio::test]
async fn pr_open_self_suspended_producer_does_not_push() {
    // A producing node self-suspended on its own work (yielded waiting on a
    // dependency/sub-agent) is not quiescent either -> no review.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    db.execute_script(
        "UPDATE turns SET state='yielded', yield_reason='dependency_wait' WHERE id='t-prod';",
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;

    assert!(pending(&orch, "j-watch").await.is_empty());
}

#[tokio::test]
async fn pr_open_resolves_builder_by_branch_not_mr_job() {
    // The live CAIRN-1891 job-identity bug: the merge_request is owned by a
    // separate pr-action node (blocked while the PR is open -> a running turn,
    // never quiescent), while the builder that did the work is a DIFFERENT job
    // on the same branch. Gating on `mr.job_id` would always bail; the gate
    // must resolve and check the builder via `source_branch`.
    let db = test_db().await;
    seed(&db, "initial").await; // builder j-prod: branch 'b', turn complete (quiescent)
                                // The pr-action node owns the merge_request and — reproducing the live
                                // shape — has NO joinable execution (execution_id NULL), so an arm-1 query
                                // that joined through mr.job_id would drop the row and read the open PR as
                                // unreviewable. The builder (j-prod) is the joinable node.
    db.execute_script(
            "INSERT INTO jobs(id, project_id, issue_id, status, uri_segment, node_name, created_at, updated_at)
               VALUES('j-prnode','p-rev','i-rev','blocked','pr','pr',1,1);
             INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
               VALUES('mr-rev','j-prnode','p-rev','i-rev','t','b','main','open',1,1);",
        )
        .await
        .unwrap();
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;

    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(
        watcher.len(),
        1,
        "must resolve the builder by source_branch and fire, not gate on the blocked pr-node"
    );
    assert_eq!(watcher[0].key, REVIEW_KEY);
}

#[tokio::test]
async fn pr_open_changed_head_sha_refires_even_with_same_diffstat() {
    // Head SHA is the precise change key: two different commits can share a
    // diffstat, so a real new commit must re-review even when +/- is unchanged.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    db.execute_script("UPDATE merge_requests SET head_sha='sha-aaa' WHERE id='mr-rev';")
        .await
        .unwrap();
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;
    let first = pending(&orch, "j-watch").await;
    assert_eq!(first.len(), 1);
    stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
        .await
        .unwrap();

    // New commit, SAME diffstat, different head SHA -> must re-fire.
    orch.db
        .local
        .execute_script("UPDATE merge_requests SET head_sha='sha-bbb' WHERE id='mr-rev';")
        .await
        .unwrap();
    run_pr_open(&orch).await;
    let second = pending(&orch, "j-watch").await;
    assert_eq!(
        second.len(),
        1,
        "a changed head SHA must re-create the review even with an unchanged diffstat"
    );
    assert_ne!(second[0].id, first[0].id);
}

#[tokio::test]
async fn running_memory_review_turn_defers_then_fires_on_settle() {
    // A running memory-review turn keeps the issue unsettled (issue_settled's
    // liveness check includes memory-review turns), so no review fires
    // mid-reflection. Once that reflection turn completes the issue settles and
    // the review fires — exactly once (CAIRN-2483).
    let db = test_db().await;
    seed(&db, "memory_review").await;
    insert_open_pr(&db).await;
    db.execute_script("UPDATE turns SET state='running' WHERE id='t-prod';")
        .await
        .unwrap();
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "a running memory-review turn must defer the review"
    );

    orch.db
        .local
        .execute_script("UPDATE turns SET state='complete' WHERE id='t-prod';")
        .await
        .unwrap();
    run_pr_open(&orch).await;
    let watcher = pending(&orch, "j-watch").await;
    assert_eq!(
        watcher.len(),
        1,
        "the review fires once the reflection turn settles"
    );
    assert_eq!(watcher[0].key, REVIEW_KEY);
}

#[tokio::test]
async fn pr_open_same_diffstat_is_deduped() {
    // A mergeability-only settle re-delivers the open PR with an unchanged
    // diffstat -> the fingerprint matches the delivered push, so no re-wake.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;
    let first = pending(&orch, "j-watch").await;
    assert_eq!(first.len(), 1);
    stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
        .await
        .unwrap();
    assert!(pending(&orch, "j-watch").await.is_empty());

    orch.db
            .local
            .execute_script(
                "UPDATE merge_requests SET github_mergeable='MERGEABLE', updated_at=999 WHERE id='mr-rev';",
            )
            .await
            .unwrap();
    run_pr_open(&orch).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "a mergeability-only settle must not re-create a review push"
    );
}

#[tokio::test]
async fn pr_open_changed_diffstat_creates_new_review() {
    // New commits change the diffstat between webhook deliveries -> a fresh
    // review push, even after the first was delivered.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;
    let first = pending(&orch, "j-watch").await;
    assert_eq!(first.len(), 1);
    stamp_delivered(&orch.db.local, &[first[0].id.clone()], "ev-1")
        .await
        .unwrap();

    orch.db
        .local
        .execute_script("UPDATE merge_requests SET additions=20, deletions=4 WHERE id='mr-rev';")
        .await
        .unwrap();
    run_pr_open(&orch).await;
    let second = pending(&orch, "j-watch").await;
    assert_eq!(second.len(), 1, "a changed diffstat re-creates the review");
    assert_ne!(second[0].id, first[0].id);
}

#[tokio::test]
async fn pr_open_and_node_idle_share_one_creator() {
    // Both edges run the same row creator: the PR-open edge creates the
    // review, and a subsequent node-idle edge against the unchanged diffstat
    // is deduped by the same fingerprint logic to the one undelivered row.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    run_pr_open(&orch).await;
    let after_pr_open = pending(&orch, "j-watch").await;
    assert_eq!(after_pr_open.len(), 1);
    let row_id = after_pr_open[0].id.clone();

    run_review_push(&orch).await;
    let after_idle = pending(&orch, "j-watch").await;
    assert_eq!(after_idle.len(), 1);
    assert_eq!(
        after_idle[0].id, row_id,
        "both edges share one push row keyed review:{{issue}}"
    );
}

#[tokio::test]
async fn pr_open_after_idle_artifact_push_supersedes_to_pr_fingerprint() {
    // The CAIRN-2410 incident, reconstructed. A builder's `create-pr` artifact
    // auto-confirms on write (CAIRN-1219); the coordinator's node-idle edge
    // pushes a review with an `artifact:` fingerprint while no merge_requests
    // row exists yet. The PR opens ~42ms later. Before the fix the PR-open edge
    // never ran on the first-class PR-node path, so this second edge never
    // re-fired and the wake was lost. Now the idle-edge artifact push, then the
    // PR opening, then the PR-open edge supersedes the SAME review:{issue} row to
    // a `pr:` fingerprint on a rousing push that wakes the idle watcher.
    let db = test_db().await;
    seed(&db, "initial").await;
    // The reviewable artifact is a CONFIRMED create-pr (the auto-confirm the
    // incident hinges on), and there is no merge_requests row yet.
    insert_artifact(&db, "create-pr", 1).await;
    let orch = test_orchestrator(db);

    // Idle edge fires first: one review push fingerprinted on the artifact.
    run_review_push(&orch).await;
    let after_idle = pending(&orch, "j-watch").await;
    assert_eq!(after_idle.len(), 1);
    let idle_row = after_idle[0].id.clone();
    let idle_fp = latest_push_fingerprint(&orch.db.local, "j-watch", REVIEW_KEY)
        .await
        .unwrap()
        .flatten()
        .unwrap();
    assert!(
        idle_fp.starts_with("artifact:"),
        "idle edge fingerprints on the artifact, got {idle_fp}"
    );

    // The PR opens: the merge_requests row lands (the 42ms-late seed).
    insert_open_pr(&orch.db.local).await;

    // The PR-open edge re-evaluates the same review key. The reviewable ref is
    // now the open PR, so the fingerprint changes to `pr:` and the row is
    // superseded in place — still exactly one undelivered review.
    run_pr_open(&orch).await;
    let after_open = pending(&orch, "j-watch").await;
    assert_eq!(
        after_open.len(),
        1,
        "supersede-by-key collapses the idle and PR-open pushes to one undelivered row"
    );
    assert_eq!(
        after_open[0].id, idle_row,
        "the PR-open push supersedes the idle push in place (same review:{{issue}} key)"
    );
    assert_eq!(
        after_open[0].wake,
        Wake::Wake,
        "the superseding review is rousing, so an idle watcher is woken"
    );
    let open_fp = latest_push_fingerprint(&orch.db.local, "j-watch", REVIEW_KEY)
        .await
        .unwrap()
        .flatten()
        .unwrap();
    assert!(
        open_fp.starts_with("pr:"),
        "the PR-open edge fingerprints on the open PR, got {open_fp}"
    );
}

// --- CAIRN-2483: the issue-quiescence and checks-settled gates -----------

#[tokio::test]
async fn fresh_red_defers_the_review() {
    // A fresh (non-cached) failing check at the completion edge defers the parent
    // review: that red is simultaneously waking the owning builder and the fix
    // loop is live. A later settle (fresh_red = false, the stale-red case) fires.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    evaluate_review_readiness(&orch, "i-rev", true).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "a fresh red must defer the parent review"
    );

    evaluate_review_readiness(&orch, "i-rev", false).await;
    assert_eq!(
        pending(&orch, "j-watch").await.len(),
        1,
        "a stale red (fresh_red=false) settles-with-warning and the review fires"
    );
}

#[tokio::test]
async fn in_flight_turn_end_checks_defer_the_review() {
    // While any job of the issue holds an in-flight turn-end run, checks are not
    // settled, so the review defers until the suite completes.
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    assert!(orch.try_begin_turn_end_checks("j-prod").is_some());
    run_review_push(&orch).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "an in-flight turn-end suite must hold the review"
    );

    orch.end_turn_end_checks("j-prod");
    run_review_push(&orch).await;
    assert_eq!(pending(&orch, "j-watch").await.len(), 1);
}

#[tokio::test]
async fn transient_action_run_defers_then_blocked_fires() {
    // The pr action opening the PR (a pending/running action_run) keeps the issue
    // unsettled until it terminalizes or blocks (the open-PR human gate).
    let db = test_db().await;
    seed(&db, "initial").await;
    insert_open_pr(&db).await;
    let orch = test_orchestrator(db);

    orch.db
        .local
        .execute_script(
            "INSERT INTO action_runs(id, execution_id, recipe_node_id, action_config_id, issue_id, project_id, status, created_at)
             VALUES('ar-pr','e-rev','pr','builtin:pr','i-rev','p-rev','running',1);",
        )
        .await
        .unwrap();
    run_review_push(&orch).await;
    assert!(
        pending(&orch, "j-watch").await.is_empty(),
        "a transient pr action_run must hold the review"
    );

    // The action blocks (PR open, human gate) -> settled -> the review fires.
    orch.db
        .local
        .execute_script("UPDATE action_runs SET status='blocked' WHERE id='ar-pr';")
        .await
        .unwrap();
    run_review_push(&orch).await;
    assert_eq!(pending(&orch, "j-watch").await.len(), 1);
}

#[tokio::test]
async fn render_push_resolved_inlines_referent_content() {
    // CAIRN-1891 Deliverable 2: a drained push renders its referent content
    // inline, not just the URI. The header carries the wake level + the
    // content_ref URI; a resolved body is appended beneath it.
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);

    let push = Push {
        id: "p-render".into(),
        recipient: "j-watch".into(),
        content_ref: ISSUE_URI.into(),
        wake: Wake::Wake,
        boundary: Boundary::Event,
        key: REVIEW_KEY.into(),
        created_at: 1,
        delivered_event_id: None,
    };
    let rendered =
        crate::orchestrator::attention_delivery::render_push_resolved(&orch, &push).await;

    let header = format!("Attention update (wake): {ISSUE_URI}");
    assert!(
        rendered.starts_with(&header),
        "header must carry the wake level + content_ref URI: {rendered}"
    );
    assert!(
        rendered.len() > header.len(),
        "expected resolved referent content inlined beneath the URI header: {rendered}"
    );
}

#[tokio::test]
async fn turn_end_cancel_resolves_immediately_when_already_cancelled() {
    use crate::orchestrator::TurnEndCancel;
    let cancel = TurnEndCancel::default();
    assert!(!cancel.is_cancelled());
    cancel.cancel();
    assert!(cancel.is_cancelled());
    tokio::time::timeout(std::time::Duration::from_secs(1), cancel.cancelled())
        .await
        .expect("an already-cancelled token resolves without blocking");
}

#[tokio::test]
async fn turn_end_cancel_wakes_a_parked_waiter() {
    use crate::orchestrator::TurnEndCancel;
    let cancel = TurnEndCancel::default();
    let waiter = {
        let cancel = cancel.clone();
        tokio::spawn(async move { cancel.cancelled().await })
    };
    // Let the waiter park on `notified()` before signalling.
    tokio::task::yield_now().await;
    cancel.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .expect("cancel wakes the parked waiter")
        .expect("the waiter task joins cleanly");
}

#[tokio::test]
async fn cancel_turn_end_checks_signals_the_in_flight_token() {
    let db = test_db().await;
    let orch = test_orchestrator(db);

    let cancel = orch
        .try_begin_turn_end_checks("j-prod")
        .expect("first claim wins the single-flight slot");
    assert!(
        orch.try_begin_turn_end_checks("j-prod").is_none(),
        "a second claim while one is in flight is refused"
    );
    assert!(!cancel.is_cancelled());

    orch.cancel_turn_end_checks("j-prod");
    assert!(
        cancel.is_cancelled(),
        "the cancel lever signals the in-flight suite's token"
    );

    orch.end_turn_end_checks("j-prod");
    // After release the slot is free again and a stale cancel is a no-op.
    orch.cancel_turn_end_checks("j-prod");
    assert!(orch.try_begin_turn_end_checks("j-prod").is_some());
}

#[tokio::test]
async fn branch_advance_cancels_the_in_flight_review_suite() {
    // A commit sealing mid-turn advances the branch; the branch-advance hook
    // cancels the job's in-flight when:review suite so its heavy compiles stop
    // starving the builder's own when:write checks. Idempotent afterward.
    let db = test_db().await;
    let orch = test_orchestrator(db);

    let cancel = orch
        .try_begin_turn_end_checks("j-prod")
        .expect("claim the job's single-flight slot");
    assert!(!cancel.is_cancelled());

    crate::execution::checks::cancel_stale_review_on_branch_advance(&orch, "j-prod");
    assert!(
        cancel.is_cancelled(),
        "a sealed commit cancels the in-flight review suite for the job"
    );

    orch.end_turn_end_checks("j-prod");
    // No suite in flight ⇒ the branch-advance cancel is a harmless no-op, and the
    // single-flight slot remains claimable.
    crate::execution::checks::cancel_stale_review_on_branch_advance(&orch, "j-prod");
    assert!(orch.try_begin_turn_end_checks("j-prod").is_some());
}

#[tokio::test]
async fn resolving_an_issue_cancels_its_jobs_turn_end_checks() {
    // The issue-scoped lever the merge/close path pulls: every job of the issue
    // with an in-flight suite is signalled to quit (CAIRN-2648).
    let db = test_db().await;
    seed(&db, "initial").await;
    let orch = test_orchestrator(db);

    let cancel = orch
        .try_begin_turn_end_checks("j-prod")
        .expect("claim the builder job's slot");
    assert!(!cancel.is_cancelled());

    crate::execution::checks_turn_end::cancel_turn_end_checks_for_issue(
        &orch,
        &orch.db.local,
        "i-rev",
    )
    .await;

    assert!(
        cancel.is_cancelled(),
        "resolving issue i-rev quits its builder job's in-flight suite"
    );
    orch.end_turn_end_checks("j-prod");
}
