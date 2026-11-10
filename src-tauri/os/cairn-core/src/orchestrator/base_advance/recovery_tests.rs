//! Durable recovery fences on the branch preparer: a budgeted transaction's
//! unverified rollback, persisted through [`persist_preparation_recoveries`],
//! refuses every demand at its locked boundary and is cleared only by the
//! targeted repair the periodic upkeep pass runs. A real store and real Git.

use super::preparation::tests::{git, harness_or_skip, Harness, BRANCH, MAIN_LINE};
use super::preparation::{persist_preparation_recoveries, PreparationPurpose, PreparationRefusal};
use crate::orchestrator::branch_recovery;

/// One periodic upkeep pass, as the orchestrator runs it.
async fn upkeep(h: &Harness) {
    super::retirement::upkeep_pass(&h.orch, &super::retirement::LegacyQueue::default()).await;
}

async fn make_due(h: &Harness) {
    h.db.execute("UPDATE jj_branch_recoveries SET next_repair_at = 0", ())
        .await
        .unwrap();
}

fn store_coordinates() -> crate::jj::RecoveryCoordinates {
    crate::jj::RecoveryCoordinates {
        scope: crate::jj::RecoveryScope::Store,
        phase: crate::jj::RollbackFailurePhase::RestoreOutcomeUnknown,
        pre_op: Some("0".repeat(40)),
        pre_view: None,
        pre_git: None,
        failed_view: None,
        bookmarks: Vec::new(),
        survivors: Vec::new(),
        evidence: vec!["test".into()],
    }
}

async fn fenced_refusal(h: &Harness, purpose: PreparationPurpose) -> String {
    match h.prepare(purpose).await.err() {
        Some(PreparationRefusal::Fenced { refusal }) => refusal,
        Some(other) => panic!("refused, but not by the fence: {other}"),
        None => panic!("prepared through a recovery fence"),
    }
}

/// A recovery the database refuses survives a restart. The fallback marker
/// refuses the preparer after this process forgets the failure, stays until
/// the upkeep pass has recorded it, and only then leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn an_unrecorded_recovery_survives_a_restart_until_upkeep_records_it() {
    let h = harness_or_skip!("recovery_marker");
    h.db.execute(
        "ALTER TABLE jj_branch_recoveries RENAME TO jj_branch_recoveries_away",
        (),
    )
    .await
    .unwrap();
    let error = branch_recovery::record_recoveries(
        &h.db,
        "proj-1",
        &h.store,
        Some(BRANCH),
        branch_recovery::BUDGET_AUTOMATIC,
        "test",
        vec![store_coordinates()],
    )
    .await
    .unwrap_err();
    assert!(error.contains("until the sweep records it"), "{error}");
    let marker = h.store.join(".jj/cairn-unrecorded-recoveries.json");
    assert!(marker.exists());

    // A restart: the process-lifetime refusal is gone, the marker is not.
    branch_recovery::forget_process_state_for_test();
    let refused = fenced_refusal(&h, PreparationPurpose::Check).await;
    assert!(
        refused.contains("could not be written to the database"),
        "{refused}"
    );

    // While the table is still unwritable, the pass keeps the marker.
    upkeep(&h).await;
    assert!(
        marker.exists(),
        "the marker is removed only after a recorded insert"
    );

    h.db.execute(
        "ALTER TABLE jj_branch_recoveries_away RENAME TO jj_branch_recoveries",
        (),
    )
    .await
    .unwrap();
    upkeep(&h).await;
    assert!(!marker.exists(), "recorded, so the marker is gone");
    let fences = branch_recovery::fences(&h.db, &h.store).await.unwrap();
    assert_eq!(fences.len(), 1, "the recovery now stands in the table");
    assert_eq!(fences[0].scope, crate::jj::RecoveryScope::Store);
    let refused = fenced_refusal(&h, PreparationPurpose::OwnerCommit).await;
    assert!(refused.contains(&fences[0].id), "{refused}");

    // An unreadable marker refuses rather than reading as absent.
    std::fs::write(&marker, "not json").unwrap();
    let refused = branch_recovery::refuse_if_fenced(&h.db, &h.store, &["agent/other"], "Probe")
        .await
        .unwrap_err();
    assert!(refused.contains("unreadable"), "{refused}");
    std::fs::remove_file(&marker).unwrap();
}

/// A recovery that cannot be persisted is reported as such, never as an
/// unchanged branch, and keeps the store refused for this process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_recovery_that_cannot_be_persisted_keeps_the_store_refused() {
    let h = harness_or_skip!("recovery_unpersisted");
    h.db.execute("DROP TABLE jj_branch_recoveries", ())
        .await
        .unwrap();
    let fenced = persist_preparation_recoveries(
        &h.db,
        "proj-1",
        &h.store,
        BRANCH,
        false,
        "test transaction",
        vec![store_coordinates()],
    )
    .await
    .expect("a recovery was taken");
    assert!(
        fenced.starts_with(crate::jj::ROLLBACK_INCOMPLETE),
        "{fenced}"
    );
    assert!(fenced.contains("not known to be unchanged"), "{fenced}");
    assert!(
        fenced.contains("recording its recovery fence in the database failed"),
        "{fenced}"
    );
    let refused = fenced_refusal(&h, PreparationPurpose::Check).await;
    assert!(
        refused.contains(&format!("Preparing `{BRANCH}` for a check refused")),
        "{refused}"
    );
    assert!(h.wakes().await.is_empty(), "a check's refusal wakes nobody");
    branch_recovery::forget_process_state_for_test();
}

const RETIRED: &str = "agent/retired";

/// `refs/heads/{bookmark}` in Git, or `None` when the ref is absent.
fn git_ref(h: &Harness, bookmark: &str) -> Option<String> {
    let out = crate::env::git()
        .args([
            "rev-parse",
            "--verify",
            "-q",
            &format!("refs/heads/{bookmark}"),
        ])
        .current_dir(&h.repo)
        .output()
        .unwrap();
    out.status
        .success()
        .then(|| String::from_utf8(out.stdout).unwrap().trim().to_string())
}

/// A two-bookmark rollback whose export failed after a squash merge: `BRANCH`
/// was carried to the squash, and the retired head `RETIRED` (same commit as
/// `BRANCH` before) is gone from Git. Its recovery is recorded as a branch
/// fence. Returns the original commit both bookmarks are to be restored to.
async fn merged_missing_recovery_fixture(h: &Harness) -> String {
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "own\n");
    let squash = h.commit(&h.main_ws, MAIN_LINE, "own.txt", "own\n");
    h.set_bookmark("main", &squash);
    h.set_bookmark(RETIRED, &own);
    crate::jj::export_git_verified(
        &h.jj,
        &h.store,
        true,
        "fixture export",
        &[(RETIRED, &own), ("main", &squash)],
    )
    .unwrap();
    // The squash has the source tree but not its commit in its ancestry.
    assert!(!h.is_ancestor(&own, &squash));
    assert_eq!(
        git(&h.repo, &["rev-parse", &format!("{own}^{{tree}}")]),
        git(&h.repo, &["rev-parse", &format!("{squash}^{{tree}}")])
    );
    h.set_bookmark(BRANCH, &squash);
    crate::jj::export_git_verified(&h.jj, &h.store, true, "failed export", &[(BRANCH, &squash)])
        .unwrap();
    git(
        &h.repo,
        &["update-ref", "-d", &format!("refs/heads/{RETIRED}")],
    );
    let coordinates = crate::jj::RecoveryCoordinates {
        scope: crate::jj::RecoveryScope::Branch,
        phase: crate::jj::RollbackFailurePhase::ExportUnverified,
        pre_op: Some(crate::jj::operation_id(&h.jj, &h.store).unwrap()),
        pre_view: None,
        pre_git: None,
        failed_view: None,
        bookmarks: vec![
            crate::jj::BookmarkRecovery {
                bookmark: BRANCH.into(),
                pre: cairn_vcs::BookmarkState::normal(&own),
                pre_git: Some(own.clone()),
                failed: cairn_vcs::BookmarkState::normal(&squash),
            },
            crate::jj::BookmarkRecovery {
                bookmark: RETIRED.into(),
                pre: cairn_vcs::BookmarkState::normal(&own),
                pre_git: Some(own.clone()),
                failed: cairn_vcs::BookmarkState::normal(&own),
            },
        ],
        survivors: vec![],
        evidence: vec!["two-bookmark rollback export failed".into()],
    };
    branch_recovery::record_recoveries(
        &h.db,
        "proj-1",
        &h.store,
        Some(BRANCH),
        branch_recovery::BUDGET_AUTOMATIC,
        "fixture",
        vec![coordinates],
    )
    .await
    .unwrap();
    own
}

/// The retired head's owner: a finished job whose pull request merged.
async fn record_retired_owner(h: &Harness, issue_status: &str) {
    h.db.execute_script(format!(
        "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
         VALUES ('retired-issue', 'proj-1', 2, 'Retired', '{issue_status}', 1, 1);
         INSERT INTO jobs (id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
         VALUES ('retired-job', 'retired-issue', 'proj-1', 'complete', '{RETIRED}', 'main', 1, 1);
         INSERT INTO merge_requests (id, job_id, issue_id, project_id, title, status, source_branch, target_branch, opened_at, updated_at)
         VALUES ('retired-pr', 'retired-job', 'retired-issue', 'proj-1', 'Retired', 'merged', '{RETIRED}', 'main', 1, 1);"
    ))
    .await
    .unwrap();
}

async fn repair_now(h: &Harness) {
    make_due(h).await;
    upkeep(h).await;
}

fn retired_in_store(h: &Harness) -> Option<String> {
    cairn_vcs::read_local_bookmark_view(&h.store)
        .unwrap()
        .bookmarks
        .get(RETIRED)
        .and_then(|state| state.commit().map(str::to_string))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn automatic_recovery_does_not_overwrite_a_foreign_git_head() {
    let h = harness_or_skip!("recovery_merged_foreign_git");
    let own = merged_missing_recovery_fixture(&h).await;
    record_retired_owner(&h, "merged").await;
    let foreign = h.tip("main");
    git(
        &h.repo,
        &["update-ref", &format!("refs/heads/{RETIRED}"), &foreign],
    );
    repair_now(&h).await;
    assert_eq!(
        branch_recovery::fences(&h.db, &h.store)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(git_ref(&h, RETIRED), Some(foreign));
    assert_eq!(retired_in_store(&h), Some(own));
}
