//! Durable recovery fences on the branch preparer: a budgeted transaction's
//! unverified rollback, persisted through [`persist_preparation_recoveries`],
//! refuses every demand at its locked boundary and is cleared only by the
//! targeted repair the periodic upkeep pass runs. A real store and real Git.

use std::path::PathBuf;
use std::time::Duration;

use super::preparation::tests::{git, harness_or_skip, Harness, BRANCH, JOB, MAIN_LINE};
use super::preparation::{persist_preparation_recoveries, PreparationPurpose, PreparationRefusal};
use crate::orchestrator::branch_recovery;

fn git_head(h: &Harness, bookmark: &str) -> String {
    git(&h.repo, &["rev-parse", &format!("refs/heads/{bookmark}")])
}

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

/// Lay a branch fence on `BRANCH` the way an unverified rollback does: a
/// budgeted forward move to `main`, then an `op restore` whose export Git
/// refuses because the ref is locked, persisted by the preparer's own path.
/// Returns the branch's own tip, the held ref lock (remove it to let the
/// repair export) and the diagnostic the preparation would report.
async fn lay_branch_fence(h: &Harness, explicit_replay: bool) -> (String, PathBuf, String) {
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "own\n");
    crate::jj::export_git_verified(&h.jj, &h.store, true, "fixture export", &[(BRANCH, &own)])
        .unwrap();
    assert_eq!(git_head(h, BRANCH), own);
    let main = h.tip("main");
    let jj = h.jj.with_operation_budget(Duration::from_secs(60));
    let op = crate::jj::operation_id(&jj, &h.store).unwrap();
    jj.run(
        &h.store,
        &[
            "bookmark",
            "set",
            BRANCH,
            "-r",
            &main,
            "--allow-backwards",
            "--ignore-working-copy",
        ],
        "forward mutation",
    )
    .unwrap();
    crate::jj::export_git_verified(&jj, &h.store, true, "forward export", &[(BRANCH, &main)])
        .unwrap();
    let lock = h
        .repo
        .join(".git/refs/heads")
        .join(format!("{BRANCH}.lock"));
    std::fs::write(&lock, "").unwrap();
    let error = crate::jj::restore_operation(&jj.cleanup_env(), &h.store, &op).unwrap_err();
    assert!(error.contains(crate::jj::ROLLBACK_INCOMPLETE), "{error}");
    let fenced = persist_preparation_recoveries(
        &h.db,
        "proj-1",
        &h.store,
        BRANCH,
        explicit_replay,
        "test transaction",
        jj.take_recoveries(),
    )
    .await
    .expect("the unverified rollback left a recovery");
    (own, lock, fenced)
}

async fn fenced_refusal(h: &Harness, purpose: PreparationPurpose) -> String {
    match h.prepare(purpose).await.err() {
        Some(PreparationRefusal::Fenced { refusal }) => refusal,
        Some(other) => panic!("refused, but not by the fence: {other}"),
        None => panic!("prepared through a recovery fence"),
    }
}

/// The c43a1f1e shape end to end on the preparer: the fence is reported as
/// fenced (never unchanged), refuses every demand of the branch with its
/// coordinates and route while leaving an unrelated branch admitted, wakes
/// nobody, and is cleared only by the targeted repair the upkeep pass runs,
/// which puts the Git head back and tells the owning issue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn an_unverified_rollback_fences_every_demand_until_upkeep_repairs_it() {
    let h = harness_or_skip!("recovery_fence_lifecycle");
    let (own, lock, fenced) = lay_branch_fence(&h, false).await;
    let fences = branch_recovery::fences(&h.db, &h.store).await.unwrap();
    assert_eq!(fences.len(), 1, "{fences:?}");
    let fence = &fences[0];
    assert_eq!(fence.scope, crate::jj::RecoveryScope::Branch);
    assert_eq!(fence.affected, vec![BRANCH.to_string()]);
    assert_eq!(fence.budget_class, branch_recovery::BUDGET_AUTOMATIC);
    assert!(
        fenced.starts_with(crate::jj::ROLLBACK_INCOMPLETE),
        "{fenced}"
    );
    assert!(fenced.contains("not known to be unchanged"), "{fenced}");
    assert!(fenced.contains(&fence.id), "{fenced}");

    // Every demand needs a replay, and every one is refused at the boundary.
    h.advance_main("incoming.txt", "theirs\n");
    for purpose in [
        PreparationPurpose::OwnerCommit,
        PreparationPurpose::OwnerMerge,
        PreparationPurpose::Check,
        PreparationPurpose::Run,
    ] {
        let refusal = fenced_refusal(&h, purpose).await;
        assert!(refusal.contains(&fence.id), "{refusal}");
        assert!(
            refusal.contains(BRANCH) && refusal.contains("automatic"),
            "{refusal}"
        );
        assert!(refusal.contains("{action:\"repair\"}"), "{refusal}");
    }
    let replay = super::replay_requests::request_branch_replay(
        &h.orch, &h.db, JOB, BRANCH, None, false, None,
    )
    .await
    .unwrap_err();
    assert!(replay.contains(&fence.id), "{replay}");
    assert!(replay.contains("Replaying"), "{replay}");
    assert!(!replay.contains("is unchanged"), "{replay}");
    assert_eq!(h.tip(BRANCH), own, "nothing moved the fenced branch");
    assert!(h.wakes().await.is_empty(), "a refusal wakes nobody");
    branch_recovery::refuse_if_fenced(&h.db, &h.store, &["agent/unrelated"], "Probe")
        .await
        .unwrap();

    // Not due yet: the pass leaves it. Once due, a still-locked ref keeps it.
    upkeep(&h).await;
    let kept = branch_recovery::fences(&h.db, &h.store).await.unwrap();
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].repair_attempts, 0, "no attempt before its backoff");
    make_due(&h).await;
    upkeep(&h).await;
    let kept = branch_recovery::fences(&h.db, &h.store).await.unwrap();
    assert_eq!(kept.len(), 1, "a refused repair keeps the fence");
    assert_eq!(kept[0].repair_attempts, 1);
    assert!(kept[0].next_repair_at >= chrono::Utc::now().timestamp() + 250);

    std::fs::remove_file(&lock).unwrap();
    make_due(&h).await;
    upkeep(&h).await;
    assert!(branch_recovery::fences(&h.db, &h.store)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        git_head(&h, BRANCH),
        own,
        "the repair put the Git head back"
    );
    assert_eq!(h.tip(BRANCH), own);
    let notice =
        h.db.query_text(
            "SELECT content FROM comments WHERE issue_id = 'issue-1' AND content LIKE ?1
             ORDER BY created_at DESC LIMIT 1",
            (format!("%{}%repaired and verified%", fence.id),),
        )
        .await
        .unwrap()
        .expect("the owning issue hears the repair outcome");
    assert!(notice.contains(BRANCH), "{notice}");

    // Repaired, the owner's next commit prepares normally.
    assert!(
        h.prepare(PreparationPurpose::OwnerCommit)
            .await
            .unwrap()
            .advanced
    );
}

/// A repair that outlives its allowance must neither hold the store lock until
/// it finishes nor be left writing unfenced. The worker is paused after it is
/// admitted to write. The attempt escalates the record to a store fence and
/// releases the lock; while the worker lives, a second attempt changes nothing
/// and an unrelated write is refused. Once it finishes, the next validates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_repair_that_outlives_its_allowance_fences_the_store_until_its_worker_ends() {
    let h = harness_or_skip!("recovery_rendezvous");
    // Left by an explicit replay: the fence names that budget class.
    let (own, lock, _) = lay_branch_fence(&h, true).await;
    std::fs::remove_file(&lock).unwrap();
    let fences = branch_recovery::fences(&h.db, &h.store).await.unwrap();
    assert_eq!(fences[0].budget_class, branch_recovery::BUDGET_EXPLICIT);
    let id = fences[0].id.clone();

    let (reached_tx, reached_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let reached_tx = std::sync::Mutex::new(reached_tx);
    let release_rx = std::sync::Mutex::new(release_rx);
    let _hook = branch_recovery::test_support::pause_repairs_after_write_admission(
        Duration::from_millis(500),
        move || {
            let _ = reached_tx.lock().unwrap().send(());
            let _ = release_rx.lock().unwrap().recv();
        },
    );
    make_due(&h).await;
    // The pass returns, bounded, with the worker still paused.
    tokio::time::timeout(Duration::from_secs(60), upkeep(&h))
        .await
        .expect("the attempt is bounded by its allowance, not by its worker");
    reached_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the worker reached write admission");
    let fence = branch_recovery::fences(&h.db, &h.store).await.unwrap();
    assert_eq!(fence.len(), 1);
    assert_eq!(fence[0].id, id);
    assert_eq!(fence[0].scope, crate::jj::RecoveryScope::Store);
    assert_eq!(fence[0].phase, "repair_interrupted");
    // The store lock is free; exclusion is held by the fence instead, and the
    // preparer refuses a branch the fence never named.
    let guard = tokio::time::timeout(
        Duration::from_secs(5),
        h.orch.acquire_jj_store_lock(
            &h.store,
            crate::orchestrator::StoreLockClass::Foreground,
            "probe while the worker lives",
        ),
    )
    .await
    .expect("the store lock was released at the allowance");
    let refused =
        branch_recovery::refuse_if_fenced(&h.db, &h.store, &["agent/unrelated"], "This write")
            .await
            .unwrap_err();
    assert!(refused.contains(&id), "{refused}");
    drop(guard);
    // A second attempt while the worker lives clears nothing.
    make_due(&h).await;
    upkeep(&h).await;
    let kept = branch_recovery::fences(&h.db, &h.store).await.unwrap();
    assert_eq!(
        kept.len(),
        1,
        "nothing clears a fence while its worker may commit"
    );
    assert!(
        kept[0]
            .last_repair_outcome
            .as_deref()
            .is_some_and(|outcome| outcome.contains("still running")),
        "{:?}",
        kept[0].last_repair_outcome
    );

    // Let the first worker finish its write, then validate.
    release_tx.send(()).unwrap();
    let finished = std::time::Instant::now();
    loop {
        make_due(&h).await;
        upkeep(&h).await;
        if branch_recovery::fences(&h.db, &h.store)
            .await
            .unwrap()
            .is_empty()
        {
            break;
        }
        assert!(
            finished.elapsed() < Duration::from_secs(60),
            "the fence never cleared: {:?}",
            branch_recovery::fences(&h.db, &h.store).await.unwrap()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(h.tip(BRANCH), own);
    assert_eq!(git_head(&h, BRANCH), own);
}

/// Managed origin imports honour fences. A store fence refuses any import; a
/// branch fence refuses one that names the fenced bookmark and otherwise
/// preserves that bookmark's import inputs. The default-bookmark reconcile is
/// refused at its own boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn origin_imports_honour_branch_and_store_fences() {
    let h = harness_or_skip!("recovery_imports");
    let (own, lock, _) = lay_branch_fence(&h, false).await;
    std::fs::remove_file(&lock).unwrap();

    let refused =
        branch_recovery::refuse_import_if_fenced(&h.db, &h.store, &[BRANCH], "Fast-forwarding")
            .await
            .unwrap_err();
    assert!(refused.contains(BRANCH), "{refused}");
    branch_recovery::refuse_import_if_fenced(&h.db, &h.store, &["main"], "Importing main")
        .await
        .expect("an import that leaves the fenced bookmark alone is admitted");

    // Git moves the fenced ref behind jj: an import would follow it.
    let main = h.tip("main");
    let before = git_head(&h, BRANCH);
    let foreign = git(
        &h.repo,
        &[
            "commit-tree",
            "-p",
            &main,
            "-m",
            "moved behind jj",
            &format!("{main}^{{tree}}"),
        ],
    );
    git(
        &h.repo,
        &["update-ref", &format!("refs/heads/{BRANCH}"), &foreign],
    );
    let preserved =
        branch_recovery::refuse_import_if_fenced(&h.db, &h.store, &["main"], "Importing main")
            .await
            .expect("unrelated imports preserve the fenced input");
    crate::jj::import_git(&h.jj.preserving_import_bookmarks(preserved), &h.store).unwrap();
    assert_eq!(git_head(&h, BRANCH), foreign);
    assert_eq!(h.tip(BRANCH), own);
    git(
        &h.repo,
        &["update-ref", &format!("refs/heads/{BRANCH}"), &before],
    );

    // A store fence refuses every import, the default-bookmark reconcile
    // included, at its own locked boundary.
    h.db.execute("UPDATE jj_branch_recoveries SET scope = 'store'", ())
        .await
        .unwrap();
    let refused = super::reconcile_store_default_bookmark(
        &h.orch,
        &h.db,
        "proj-1",
        &h.repo,
        "main",
        "test advance",
    )
    .await
    .unwrap_err();
    assert!(
        refused.contains("Reconciling `main` onto origin refused"),
        "{refused}"
    );
    assert!(refused.contains("store scope"), "{refused}");
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

async fn assert_merged_missing_fenced(h: &Harness) {
    let fences = branch_recovery::fences(&h.db, &h.store).await.unwrap();
    assert_eq!(fences.len(), 1, "{fences:?}");
    assert_eq!(git_ref(h, RETIRED), None);
}

fn assert_merged_missing_restored(h: &Harness, own: &str) {
    assert_eq!(git_head(h, BRANCH), own);
    assert_eq!(h.tip(BRANCH), own);
    assert_eq!(git_ref(h, RETIRED).as_deref(), Some(own));
    assert_eq!(retired_in_store(h).as_deref(), Some(own));
}

async fn assert_unfenced(h: &Harness) {
    assert!(branch_recovery::fences(&h.db, &h.store)
        .await
        .unwrap()
        .is_empty());
}

/// A squash-merged retired head whose Git ref the failed export deleted is
/// recreated at its recorded pre-target, and the other bookmark restored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn automatic_recovery_recreates_a_squash_merged_head_and_restores_the_other_bookmark() {
    let h = harness_or_skip!("recovery_squash_merged");
    let own = merged_missing_recovery_fixture(&h).await;
    record_retired_owner(&h, "merged").await;
    repair_now(&h).await;
    assert_unfenced(&h).await;
    assert_merged_missing_restored(&h, &own);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn automatic_recovery_restores_a_merged_head_that_is_still_present() {
    let h = harness_or_skip!("recovery_merged_present");
    let own = merged_missing_recovery_fixture(&h).await;
    record_retired_owner(&h, "merged").await;
    git(
        &h.repo,
        &["update-ref", &format!("refs/heads/{RETIRED}"), &own],
    );
    repair_now(&h).await;
    assert_unfenced(&h).await;
    assert_merged_missing_restored(&h, &own);
}

/// Neither a recorded failed target nor a later foreign target authorizes
/// recreation: the store must still hold the exact saved pre-target.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn automatic_recovery_refuses_recreation_after_the_merged_bookmark_moved() {
    let h = harness_or_skip!("recovery_merged_jj_moved");
    merged_missing_recovery_fixture(&h).await;
    record_retired_owner(&h, "merged").await;
    let squash = h.tip("main");
    let foreign = h.commit(&h.main_ws, MAIN_LINE, "foreign.txt", "new work\n");
    let mut record = branch_recovery::fences(&h.db, &h.store)
        .await
        .unwrap()
        .remove(0);
    record
        .coordinates
        .bookmarks
        .iter_mut()
        .find(|bookmark| bookmark.bookmark == RETIRED)
        .unwrap()
        .failed = cairn_vcs::BookmarkState::normal(&squash);
    h.db.execute(
        "UPDATE jj_branch_recoveries SET coordinates = ?1",
        cairn_db::turso::params![serde_json::to_string(&record.coordinates).unwrap()],
    )
    .await
    .unwrap();
    for current in [&squash, &foreign] {
        h.set_bookmark(RETIRED, current);
        repair_now(&h).await;
        assert_merged_missing_fenced(&h).await;
        assert_eq!(retired_in_store(&h).as_deref(), Some(current.as_str()));
    }
    h.jj.run(
        &h.store,
        &["bookmark", "delete", RETIRED, "--ignore-working-copy"],
        "delete merged bookmark",
    )
    .unwrap();
    repair_now(&h).await;
    assert_merged_missing_fenced(&h).await;
    assert_eq!(retired_in_store(&h), None);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn automatic_recovery_requires_a_matching_recorded_git_pre_for_recreation() {
    let h = harness_or_skip!("recovery_merged_pre_mismatch");
    merged_missing_recovery_fixture(&h).await;
    record_retired_owner(&h, "merged").await;
    let mut record = branch_recovery::fences(&h.db, &h.store)
        .await
        .unwrap()
        .remove(0);
    record
        .coordinates
        .bookmarks
        .iter_mut()
        .find(|bookmark| bookmark.bookmark == RETIRED)
        .unwrap()
        .pre_git = None;
    h.db.execute(
        "UPDATE jj_branch_recoveries SET coordinates = ?1",
        cairn_db::turso::params![serde_json::to_string(&record.coordinates).unwrap()],
    )
    .await
    .unwrap();
    repair_now(&h).await;
    assert_merged_missing_fenced(&h).await;
}

/// Recreation needs settled ownership: an unknown owner, an unmerged or
/// unresolved one, and a live run each keep the missing head fenced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn automatic_recovery_keeps_unknown_unmerged_unresolved_and_live_missing_heads_fenced() {
    let h = harness_or_skip!("recovery_merged_held");
    let own = merged_missing_recovery_fixture(&h).await;
    repair_now(&h).await;
    assert_merged_missing_fenced(&h).await;
    record_retired_owner(&h, "closed").await;
    h.db.execute(
        "UPDATE merge_requests SET status = 'closed' WHERE id = 'retired-pr'",
        (),
    )
    .await
    .unwrap();
    repair_now(&h).await;
    assert_merged_missing_fenced(&h).await;
    for (step, script) in [
        (
            "an active issue",
            "UPDATE issues SET status = 'active' WHERE id = 'retired-issue';
             UPDATE merge_requests SET status = 'merged' WHERE id = 'retired-pr';",
        ),
        (
            "a running job",
            "UPDATE issues SET status = 'merged' WHERE id = 'retired-issue';
             UPDATE jobs SET status = 'running' WHERE id = 'retired-job';",
        ),
        (
            "an open pull request",
            "UPDATE jobs SET status = 'complete' WHERE id = 'retired-job';
             UPDATE merge_requests SET status = 'open' WHERE id = 'retired-pr';",
        ),
        (
            "a live run",
            "UPDATE merge_requests SET status = 'merged' WHERE id = 'retired-pr';
             INSERT INTO runs (id, job_id, project_id, status, created_at, updated_at)
             VALUES ('retired-run', 'retired-job', 'proj-1', 'live', 1, 1);",
        ),
        (
            "a starting run",
            "UPDATE runs SET status = 'starting' WHERE id = 'retired-run';",
        ),
    ] {
        h.db.execute_script(script).await.unwrap();
        repair_now(&h).await;
        let fences = branch_recovery::fences(&h.db, &h.store).await.unwrap();
        assert_eq!(fences.len(), 1, "{step} keeps the head fenced: {fences:?}");
        assert_eq!(git_ref(&h, RETIRED), None, "{step}");
    }
    h.db.execute(
        "UPDATE runs SET status = 'exited' WHERE id = 'retired-run'",
        (),
    )
    .await
    .unwrap();
    repair_now(&h).await;
    assert_unfenced(&h).await;
    assert_merged_missing_restored(&h, &own);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn automatic_recovery_recreation_timeout_preserves_recorded_targets() {
    let h = harness_or_skip!("recovery_recreation_timeout");
    recreation_timeout_preserves_targets(&h, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn automatic_recovery_narrowed_store_recreation_timeout_preserves_recorded_targets() {
    let h = harness_or_skip!("recovery_recreation_store_timeout");
    recreation_timeout_preserves_targets(&h, true).await;
}

/// A recreation that outlives its allowance escalates to a store fence that
/// still records the retired head's pre-targets, and clears once it finishes.
async fn recreation_timeout_preserves_targets(h: &Harness, store_scope: bool) {
    let own = merged_missing_recovery_fixture(h).await;
    record_retired_owner(h, "merged").await;
    if store_scope {
        let record = branch_recovery::fences(&h.db, &h.store)
            .await
            .unwrap()
            .remove(0);
        let coordinates =
            cairn_vcs::jj::interrupted_repair_coordinates(&h.store, &record.coordinates);
        h.db.execute(
            "UPDATE jj_branch_recoveries SET scope = 'store', coordinates = ?1",
            cairn_db::turso::params![serde_json::to_string(&coordinates).unwrap()],
        )
        .await
        .unwrap();
    }
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(release_rx);
    let hook = branch_recovery::test_support::pause_repairs_after_write_admission(
        Duration::from_millis(500),
        move || {
            let _ = release_rx.lock().unwrap().recv();
        },
    );
    repair_now(h).await;
    let fences = branch_recovery::fences(&h.db, &h.store).await.unwrap();
    release_tx.send(()).unwrap();
    drop(hook);
    let escalated = &fences[0];
    assert_eq!(escalated.scope, crate::jj::RecoveryScope::Store);
    let retired = escalated
        .coordinates
        .bookmarks
        .iter()
        .find(|bookmark| bookmark.bookmark == RETIRED)
        .unwrap();
    assert_eq!(retired.pre.commit(), Some(own.as_str()), "{escalated:?}");
    assert_eq!(
        escalated.coordinates.pre_git.as_ref().unwrap().get(RETIRED),
        Some(&own)
    );
    let started = std::time::Instant::now();
    loop {
        repair_now(h).await;
        if branch_recovery::fences(&h.db, &h.store)
            .await
            .unwrap()
            .is_empty()
        {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "recreation escalation did not clear"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_merged_missing_restored(h, &own);
}
