//! Branch write admission against the acceptance journal: the permit a writer
//! that names its branch mints under the store guard, and the bracket around a
//! store-wide mutation. A real store, real journal rows, and a source contract
//! that keeps every production store-wide mutation inside one or the other.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::orchestrator::base_advance::preparation::journal_store_key;
use crate::orchestrator::base_advance::preparation::tests::fail_fence_writes;
use crate::orchestrator::base_advance::preparation::tests::{
    git, harness_or_skip, Harness, BRANCH, MAIN_LINE,
};
use crate::orchestrator::{BranchAdmissionRefusal, Orchestrator, StoreLockClass};
use crate::storage::{DbError, RowExt};

const WAIT: Duration = Duration::from_secs(5);

/// Record an install of `BRANCH` from `head_before` to `head_after` the way the
/// journal does, in `status`.
async fn owe(h: &Harness, status: &str, head_before: &str, head_after: &str) -> String {
    let id = format!("acceptance-{}", uuid::Uuid::new_v4());
    let key = journal_store_key(&h.store).unwrap();
    h.db.execute(
        "INSERT INTO branch_advance_acceptances
           (id, project_id, store_path, branch, head_before, head_after, base_branch,
            base_commit, captured_operation, dropped_commits_json, inherited_drops_json,
            status, created_at)
         VALUES (?1, 'proj-1', ?2, ?3, ?4, ?5, 'main', ?4, 'op', '[]', '[]', ?6, 1)",
        cairn_db::turso::params![
            id.as_str(),
            key.as_str(),
            BRANCH,
            head_before,
            head_after,
            status
        ],
    )
    .await
    .unwrap();
    id
}

async fn status(h: &Harness, id: &str) -> String {
    let id = id.to_string();
    h.db.read(move |conn| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status FROM branch_advance_acceptances WHERE id = ?1",
                    (id.as_str(),),
                )
                .await?;
            let row = rows.next().await?.expect("the row exists");
            row.text(0)
        })
    })
    .await
    .unwrap()
}

/// The row's status and the bracket that fences it, if any.
async fn fenced(h: &Harness, id: &str) -> (String, Option<String>) {
    let id = id.to_string();
    h.db.read(move |conn| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status, bracket_id FROM branch_advance_acceptances WHERE id = ?1",
                    (id.as_str(),),
                )
                .await?;
            let row = rows.next().await?.expect("the row exists");
            Ok::<_, DbError>((row.text(0)?, row.opt_text(1)?))
        })
    })
    .await
    .unwrap()
}

/// Fail fence writes until dropped, so a failing test cannot poison the next.
struct FailingFences;

impl FailingFences {
    fn now() -> Self {
        fail_fence_writes(true);
        Self
    }
}

impl Drop for FailingFences {
    fn drop(&mut self) {
        fail_fence_writes(false);
    }
}

/// A writer of `branch` as production admits one: reserve, take the store
/// lock, mint the permit.
async fn admit(
    orch: &Orchestrator,
    h: &Harness,
    branch: &str,
) -> Result<(), BranchAdmissionRefusal> {
    let reservation = orch
        .reserve_branches(&h.store, &[branch], "test writer", WAIT)
        .await
        .unwrap();
    let guard = orch
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

/// The production store import, bracketed exactly as the callers bracket it.
async fn bracketed_import(h: &Harness) -> Result<(), String> {
    let guard = h
        .orch
        .acquire_jj_store_lock(&h.store, StoreLockClass::Foreground, "test import")
        .await;
    let (jj, store, repo) = (h.jj.clone(), h.store.clone(), h.repo.clone());
    h.orch
        .bracket_store_mutation(guard, None, h.db.clone(), "proj-1", move || {
            crate::jj::ensure_project_store(&jj, &store, &repo)
        })
        .await
        .map(|(_guard, ())| ())
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_unknown_acceptance_refuses_the_writer_even_at_its_installed_head() {
    let h = harness_or_skip!("admit_unknown");
    let base = h.tip("main");
    let tip = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let id = owe(&h, "unknown", &base, &tip).await;

    let refusal = admit(&h.orch, &h, BRANCH).await.unwrap_err();
    assert_eq!(
        refusal,
        BranchAdmissionRefusal::Debt {
            branch: BRANCH.to_string(),
            acceptance_id: id.clone(),
            status: "unknown".to_string(),
        },
        "an unknown row is final until repaired, even when the head returns to its install"
    );
    let text = refusal.to_string();
    assert!(
        text.contains(&id) && text.contains("nothing was published") && text.contains("repair"),
        "{text}"
    );
    assert_eq!(status(&h, &id).await, "unknown");
    assert_eq!(h.tip(BRANCH), tip, "the refused writer moved nothing");

    // The gate is per branch: another branch's writer is admitted.
    admit(&h.orch, &h, MAIN_LINE)
        .await
        .expect("one branch's debt never blocks another branch");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_pending_acceptance_at_its_exact_install_settles_and_admits_in_the_same_call() {
    let h = harness_or_skip!("admit_pending_settles");
    let base = h.tip("main");
    let tip = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let id = owe(&h, "pending", &base, &tip).await;

    admit(&h.orch, &h, BRANCH)
        .await
        .expect("the exact installed head settles the row and admits the writer");
    assert_eq!(status(&h, &id).await, "accepted");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_pending_acceptance_at_neither_head_becomes_unknown_and_refuses() {
    let h = harness_or_skip!("admit_pending_neither");
    let base = h.tip("main");
    let tip = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let id = owe(&h, "pending", &base, &tip).await;
    // A commit lands on the branch outside the gate.
    h.commit(&h.branch_ws, BRANCH, "later.txt", "later\n");

    let refusal = admit(&h.orch, &h, BRANCH).await.unwrap_err();
    assert!(
        matches!(&refusal, BranchAdmissionRefusal::Debt { status, .. } if status == "unknown"),
        "{refusal}"
    );
    assert_eq!(
        status(&h, &id).await,
        "unknown",
        "never abandoned or accepted"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_restarted_orchestrator_settles_the_journal_before_its_first_writer() {
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;

    let h = harness_or_skip!("admit_restart");
    let base = h.tip("main");
    let tip = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let id = owe(&h, "pending", &base, &tip).await;

    // A fresh orchestrator over the same database: no debt is carried in
    // memory, so the first writer reads and settles the row itself.
    let home = tempfile::tempdir().unwrap();
    let restarted = Orchestrator::builder(
        Arc::new(DbState::new(
            h.db.clone(),
            Arc::new(SearchIndex::open_or_create(home.path().join("search")).unwrap()),
        )),
        Arc::new(TestServicesBuilder::new().build()),
        home.path().to_path_buf(),
    )
    .build();
    admit(&restarted, &h, BRANCH)
        .await
        .expect("the restarted writer settles the pending row before it moves anything");
    assert_eq!(status(&h, &id).await, "accepted");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_hold_restores_only_an_operation_it_captured_itself() {
    let h = harness_or_skip!("undo_own_operations");
    // The branch holds its own commit, so moving it to the base is a real move.
    let base = h.tip("main");
    let tip = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    assert_ne!(tip, base);
    // An unrelated landing reserves another branch, taken before the store
    // lock as the lock order requires.
    let _foreign = h
        .orch
        .reserve_branches(&h.store, &[MAIN_LINE], "unrelated landing", WAIT)
        .await
        .expect("the unrelated reservation is held throughout");
    let first = h
        .orch
        .acquire_jj_store_lock(&h.store, StoreLockClass::Foreground, "first hold")
        .await;
    let captured = first.capture_operation(&h.jj).unwrap();
    drop(first);

    let second = h
        .orch
        .acquire_jj_store_lock(&h.store, StoreLockClass::Foreground, "second hold")
        .await;
    let refused = second.undo_own_operations(&h.jj, &captured).unwrap_err();
    assert!(refused.contains("another store lock hold"), "{refused}");

    // Within one hold, its own work is undone even while another operation
    // reserves a branch of the store.
    let own = second.capture_operation(&h.jj).unwrap();
    h.set_bookmark(BRANCH, &base);
    assert_eq!(h.tip(BRANCH), base, "the hold moved the branch");
    second.undo_own_operations(&h.jj, &own).unwrap();
    assert_eq!(h.tip(BRANCH), tip, "the hold's own move was undone exactly");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_fence_that_cannot_be_written_refuses_the_import_before_it_runs() {
    let h = harness_or_skip!("bracket_fence_refused");
    let base = h.tip("main");
    let tip = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let id = owe(&h, "pending", &base, &tip).await;
    git(&h.repo, &["branch", "feature/arrived", &base]);

    let refused = {
        let _failing = FailingFences::now();
        bracketed_import(&h).await.unwrap_err()
    };
    assert!(refused.contains("nothing was changed"), "{refused}");
    assert!(
        crate::jj::bookmark_commit(&h.jj, &h.store, "feature/arrived").is_none(),
        "the import never ran"
    );
    assert_eq!(fenced(&h, &id).await, ("pending".to_string(), None));
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_fence_left_by_a_failed_resolution_makes_a_moved_row_unknown_at_settlement() {
    let h = harness_or_skip!("bracket_fence_moved");
    let base = h.tip("main");
    let tip = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let id = owe(&h, "pending", &base, &tip).await;
    // Outside Cairn the branch's Git ref returns to the base: the import drags
    // the bookmark back to `head_before`, which a bare settlement would read
    // as "never installed" and abandon.
    git(
        &h.repo,
        &["update-ref", &format!("refs/heads/{BRANCH}"), &base],
    );

    let guard = h
        .orch
        .acquire_jj_store_lock(&h.store, StoreLockClass::Foreground, "test import")
        .await;
    let (jj, store, repo) = (h.jj.clone(), h.store.clone(), h.repo.clone());
    let _failing = h
        .orch
        .bracket_store_mutation(guard, None, h.db.clone(), "proj-1", move || {
            let imported = crate::jj::ensure_project_store(&jj, &store, &repo);
            // The fence is written; its resolution after the import fails, as
            // a crash or a database fault would leave it.
            Ok((imported, FailingFences::now()))
        })
        .await
        .map(|(_guard, (imported, failing))| (imported.unwrap(), failing))
        .unwrap();
    let (status, bracket) = fenced(&h, &id).await;
    assert_eq!(status, "pending");
    assert!(bracket.is_some(), "the unresolved fence is durable");
    drop(_failing);

    let refusal = admit(&h.orch, &h, BRANCH).await.unwrap_err();
    assert!(
        matches!(&refusal, BranchAdmissionRefusal::Debt { status, .. } if status == "unknown"),
        "{refusal}"
    );
    assert_eq!(
        fenced(&h, &id).await,
        ("unknown".to_string(), None),
        "resolved from the stored copies, never abandoned at head_before"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_fence_left_on_an_unmoved_row_is_cleared_and_the_row_settles_normally() {
    let h = harness_or_skip!("bracket_fence_unmoved");
    let base = h.tip("main");
    let tip = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let id = owe(&h, "pending", &base, &tip).await;

    let guard = h
        .orch
        .acquire_jj_store_lock(&h.store, StoreLockClass::Foreground, "test import")
        .await;
    let (jj, store, repo) = (h.jj.clone(), h.store.clone(), h.repo.clone());
    let failing = h
        .orch
        .bracket_store_mutation(guard, None, h.db.clone(), "proj-1", move || {
            crate::jj::ensure_project_store(&jj, &store, &repo)?;
            Ok(FailingFences::now())
        })
        .await
        .map(|(_guard, failing)| failing)
        .unwrap();
    assert!(fenced(&h, &id).await.1.is_some());
    drop(failing);

    // A restarted orchestrator's first writer: the fence matches the branch's
    // copies, so it is cleared and the exact head settles the row.
    admit(&h.orch, &h, BRANCH)
        .await
        .expect("an unmoved fenced row settles at its exact install");
    assert_eq!(fenced(&h, &id).await, ("accepted".to_string(), None));
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_name_the_journal_still_owes_cannot_be_created_again() {
    let h = harness_or_skip!("admit_create");
    let base = h.tip("main");
    let id = owe(&h, "unknown", &base, &base).await;
    let guard = h
        .orch
        .acquire_jj_store_lock(&h.store, StoreLockClass::Foreground, "test mint")
        .await;
    let refusal = guard
        .branch_create_permit(&h.db, "proj-1", BRANCH)
        .await
        .map(|_| ())
        .unwrap_err();
    assert_eq!(
        refusal,
        BranchAdmissionRefusal::Debt {
            branch: BRANCH.to_string(),
            acceptance_id: id,
            status: "unknown".to_string(),
        },
        "a re-created name would inherit a row whose head_after is its mint point"
    );
    guard
        .branch_create_permit(&h.db, "proj-1", "agent/never-owed")
        .await
        .map(|_| ())
        .expect("a name the journal owes nothing is admitted");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_in_flight_run_batch_refuses_every_writer_but_its_own_exact_sealed_publication() {
    use crate::mcp::handlers::run::SealedPublication;
    use crate::orchestrator::base_advance::preparation::tests::{JOB, RUN};
    use crate::orchestrator::BatchAdmission;
    let h = harness_or_skip!("admit_in_flight_batch");
    h.db.execute_script(format!(
        "INSERT INTO sessions (id, job_id, status, backend_id, created_at, updated_at)
           VALUES ('session-batch', '{JOB}', 'open', 'backend', 1, 1);
         INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason,
                            created_at, updated_at)
           VALUES ('turn-batch', 'session-batch', '{RUN}', '{JOB}', 1, 'running', 'initial', 1, 1);
         INSERT INTO agent_waits (id, job_id, run_id, session_id, predecessor_turn_id,
                                  tool_use_id, condition_json, state, created_at)
           VALUES ('wait-batch', '{JOB}', '{RUN}', 'session-batch', 'turn-batch', 'tool-batch',
                   '{{\"kind\":\"run_batch\",\"request_id\":\"req-own\",\"commits\":true}}',
                   'pending', 1);"
    ))
    .await
    .unwrap();
    let reservation = h
        .orch
        .reserve_branches(&h.store, &[BRANCH], "test writer", WAIT)
        .await
        .unwrap();
    let guard = h
        .orch
        .acquire_jj_store_lock(&h.store, StoreLockClass::Foreground, "test writer")
        .await;
    let elsewhere = tempfile::tempdir().unwrap();
    let sealed = |job: &str, request: &str, project: &str, store: &Path, branch: &str| {
        SealedPublication::for_test(job, request, project, store, branch)
    };

    let ordinary = guard
        .branch_write_permit(&h.db, &reservation, "proj-1", BRANCH, BatchAdmission::None)
        .await
        .map(|_| ());
    assert!(
        matches!(&ordinary, Err(BranchAdmissionRefusal::InFlightBatch { branch, batch })
            if branch == BRANCH && batch.contains("req-own")),
        "an ordinary writer is held by the batch: {ordinary:?}"
    );

    // Only the exact job and request, sealed for this project, store and
    // branch, are exempt; the same store under a dotted spelling is the same.
    for (admitted, capability, why) in [
        (
            true,
            sealed(JOB, "req-own", "proj-1", &h.store, BRANCH),
            "its own",
        ),
        (
            true,
            sealed(JOB, "req-own", "proj-1", &h.store.join("."), BRANCH),
            "a dotted spelling of its store",
        ),
        (
            false,
            sealed(JOB, "req-other", "proj-1", &h.store, BRANCH),
            "another request",
        ),
        (
            false,
            sealed("job-other", "req-own", "proj-1", &h.store, BRANCH),
            "another job",
        ),
        (
            false,
            sealed(JOB, "req-own", "proj-2", &h.store, BRANCH),
            "another project",
        ),
        (
            false,
            sealed(JOB, "req-own", "proj-1", elsewhere.path(), BRANCH),
            "another store",
        ),
        (
            false,
            sealed(JOB, "req-own", "proj-1", &h.store, MAIN_LINE),
            "another branch",
        ),
    ] {
        let result = guard
            .branch_write_permit(
                &h.db,
                &reservation,
                "proj-1",
                BRANCH,
                BatchAdmission::Sealed(&capability),
            )
            .await
            .map(|_| ());
        if admitted {
            assert!(result.is_ok(), "{why} is admitted: {result:?}");
        } else {
            assert!(
                matches!(result, Err(BranchAdmissionRefusal::InFlightBatch { .. })),
                "{why} exempts nothing: {result:?}"
            );
        }
    }

    // A store deleted after minting matches nothing, even spelled as minted.
    let gone = tempfile::tempdir().unwrap();
    let gone_path = gone.path().to_path_buf();
    let stale = sealed(JOB, "req-own", "proj-1", &gone_path, BRANCH);
    assert!(stale.is_for("proj-1", &gone_path, BRANCH));
    drop(gone);
    assert!(
        !stale.is_for("proj-1", &gone_path, BRANCH),
        "an unresolvable store is never the store it was sealed for"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_writer_without_the_branch_reservation_is_refused() {
    let h = harness_or_skip!("admit_unreserved");
    let other = h
        .orch
        .reserve_branches(&h.store, &[MAIN_LINE], "test writer", WAIT)
        .await
        .unwrap();
    let guard = h
        .orch
        .acquire_jj_store_lock(&h.store, StoreLockClass::Foreground, "test writer")
        .await;
    let refusal = guard
        .branch_write_permit(
            &h.db,
            &other,
            "proj-1",
            BRANCH,
            crate::orchestrator::BatchAdmission::None,
        )
        .await
        .map(|_| ())
        .unwrap_err();
    assert_eq!(
        refusal,
        BranchAdmissionRefusal::Unreserved {
            branch: BRANCH.to_string()
        }
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_store_import_that_moves_an_owed_branch_marks_it_unknown_and_imports_the_rest() {
    let h = harness_or_skip!("bracket_moved");
    let base = h.tip("main");
    let tip = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let id = owe(&h, "pending", &base, &tip).await;
    // Outside Cairn: the branch's Git ref is reset, and an unrelated branch
    // arrives in the project's Git.
    git(
        &h.repo,
        &["update-ref", &format!("refs/heads/{BRANCH}"), &base],
    );
    git(&h.repo, &["branch", "feature/arrived", &base]);

    bracketed_import(&h)
        .await
        .expect("another branch's debt never refuses the import itself");
    assert_ne!(
        crate::jj::bookmark_commit(&h.jj, &h.store, BRANCH).as_deref(),
        Some(tip.as_str()),
        "the fixture's external move reached the owed bookmark"
    );
    assert_eq!(
        status(&h, &id).await,
        "unknown",
        "a moved owed branch is never settled against the head the import chose"
    );
    assert_eq!(
        crate::jj::bookmark_commit(&h.jj, &h.store, "feature/arrived").as_deref(),
        Some(base.as_str()),
        "other branches import normally"
    );
    let refusal = admit(&h.orch, &h, BRANCH).await.unwrap_err();
    assert!(
        matches!(&refusal, BranchAdmissionRefusal::Debt { status, .. } if status == "unknown"),
        "{refusal}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_store_import_that_leaves_an_owed_branch_alone_keeps_its_row() {
    let h = harness_or_skip!("bracket_untouched");
    let base = h.tip("main");
    let tip = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let id = owe(&h, "pending", &base, &tip).await;
    git(&h.repo, &["branch", "feature/arrived", &base]);

    bracketed_import(&h).await.unwrap();
    assert_eq!(h.tip(BRANCH), tip);
    assert_eq!(
        status(&h, &id).await,
        "pending",
        "an unmoved row is left to its settlement"
    );
    assert!(crate::jj::bookmark_commit(&h.jj, &h.store, "feature/arrived").is_some());
}

// ── Source contract ───────────────────────────────────────────────────────────────────────────

/// Store mutations that move bookmarks they may not name: every production
/// call must sit inside [`Orchestrator::bracket_store_mutation`] (or the
/// same-hold undo) within [`ADMISSION_WINDOW`] lines.
const STORE_WIDE_MUTATORS: &[&str] = &[
    "ensure_project_store(",
    "import_git(",
    "import_git_with_remote_authority(",
    "reconcile_tracked_bookmark(",
    "converge_managed_branch_after_remote_rewrite(",
    "settle_operation_heads(",
    "restore_operation(",
];

/// Writers that move (or publish) one named bookmark: every production call
/// must have its branch's permit minted within [`NAMED_WINDOW`] lines, or be
/// listed in [`ADMITTED_ELSEWHERE`] with where its admission happens.
const NAMED_WRITERS: &[&str] = &[
    "create_bookmark_at(",
    "set_bookmark_at(",
    "fast_forward_bookmark(",
    "merge_into_bookmark(",
    "fold_validated_into_bookmark(",
    "rebase_branch_onto(",
    "rebase_branch_onto_with_resolution(",
    "rebase_then_fold_into(",
    "squash_branch_onto(",
    "publish_logical_head_exported(",
    "publish_branch_to_origin(",
    "retire_bookmark_expecting(",
    "ensure_bookmark_on_origin(",
    "repair_conflicted_branch_name(",
    "repair_conflicted_bookmark_name(",
    "collapse_divergent_bookmark(",
    "install_branch_advance_local(",
    // Moves only its branch: the local bookmark, its tracked remote and git
    // ref by compare-and-swap, and exports that branch alone. It never imports.
    "fast_forward_branch_from_origin(",
];

/// What admits a store-wide mutation. Never a branch permit: a mutation that
/// can move bookmarks it does not name is safe for every other owed branch
/// only inside the bracket, or as `undo_own_operations(`, which restores an
/// operation its own guard hold captured, so nothing foreign lies between.
const STORE_WIDE_ADMISSION: &[&str] = &["bracket_store_mutation(", "undo_own_operations("];

/// What admits a named writer: its branch's permit (`admit_landing_writers(`,
/// in `pr_data::actions::default_landing`, mints `branch_write_permit` for a
/// landing's source and target together), or the bracket around it.
const NAMED_ADMISSION: &[&str] = &[
    "branch_write_permit(",
    "branch_create_permit(",
    "admit_landing_writers(",
    "bracket_store_mutation(",
];

/// How far above a store-wide mutation its admission may be.
const ADMISSION_WINDOW: usize = 12;

/// How far above a named writer its permit may be: a writer usually mints it
/// at the top of the function that then publishes.
const NAMED_WINDOW: usize = 50;

/// Named-writer calls whose admission is real but not textually near the
/// call, each with where it happens. Re-verify an entry before keeping it.
const ADMITTED_ELSEWHERE: &[(&str, &str, &str)] = &[
    (
        "mcp/handlers/run/mod.rs",
        "publish_logical_head_exported(",
        "publish_available_delta mints the branch permit on its held guard before \
         revalidating the prepared coordinate and integrating the delta; it retains that \
         guard and reservation through publication",
    ),
    (
        "mcp/handlers/write/file_mutations.rs",
        "publish_logical_head_exported(",
        "finalize_file_commit mints the prepared managed branch permit and revalidates its \
         coordinate under the write guard before the authority and recovery-fence checks; \
         the caller retains the guard through publication",
    ),
    (
        "pr_data/actions/store_merge/mod.rs",
        "squash_branch_onto(",
        "fold_onto_exact_target calls admit_landing_writers for source and target before \
         checking their exact coordinates; the same guard owns the squash and rollback",
    ),
    (
        "execution/jobs/lifecycle.rs",
        "create_bookmark_at(",
        "prepare_store_coordinate mints branch_create_permit before select_job_coordinate \
         creates the bookmark",
    ),
    (
        "execution/teardown.rs",
        "retire_bookmark_expecting(",
        "the retirement admits the branch by permit under its guard and reservation before \
         handing both to the blocking worker",
    ),
    (
        "orchestrator/base_advance/preparation.rs",
        "install_branch_advance_local(",
        "the preparer is the settler: it settles its own branch's rows under the guard first",
    ),
    (
        "orchestrator/base_advance/preparation.rs",
        "repair_conflicted_branch_name(",
        "the preparer is the settler: it checks own and base debt before the repair",
    ),
];

/// Production call sites still owed admission, one entry per call, each with
/// the owner wiring it. Shrinks to empty; never grows without an owner.
const OWED: &[(&str, &str, &str)] = &[];

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

fn is_test_cfg(line: &str) -> bool {
    line.starts_with("#[cfg(") && line.contains("test") && !line.contains("not(test")
}

/// Net `{` minus `}` in a line's code, ignoring braces inside string and
/// character literals and after a line comment, so a `format!("{{")` cannot
/// end a test module early.
fn brace_balance(line: &str) -> i64 {
    let chars: Vec<char> = line.chars().collect();
    let (mut depth, mut index, mut in_string) = (0, 0, false);
    while index < chars.len() {
        let c = chars[index];
        if in_string {
            match c {
                '\\' => index += 1,
                '"' => in_string = false,
                _ => {}
            }
        } else {
            match c {
                '/' if chars.get(index + 1) == Some(&'/') => break,
                '"' => in_string = true,
                '\'' if chars.get(index + 2) == Some(&'\'') => index += 2,
                '\'' if chars.get(index + 1) == Some(&'\\')
                    && chars.get(index + 3) == Some(&'\'') =>
                {
                    index += 3
                }
                '{' => depth += 1,
                '}' => depth -= 1,
                _ => {}
            }
        }
        index += 1;
    }
    depth
}

/// A line that can neither mutate nor admit: a comment or a string fragment.
fn is_inert(line: &str) -> bool {
    let code = line.trim_start();
    code.starts_with("//") || code.starts_with('"')
}

/// Files compiled only for tests through `#[cfg(test)] #[path = "..."]`.
fn test_only_files(files: &[PathBuf]) -> HashSet<PathBuf> {
    let mut test_only = HashSet::new();
    for file in files {
        let text = std::fs::read_to_string(file).unwrap();
        let mut in_test_attributes = false;
        for line in text.lines().map(str::trim) {
            if is_test_cfg(line) {
                in_test_attributes = true;
            } else if in_test_attributes && line.starts_with("#[path") {
                if let Some(path) = line.split('"').nth(1) {
                    test_only.insert(file.parent().unwrap().join(path));
                }
            } else if !line.starts_with("#[") {
                in_test_attributes = false;
            }
        }
    }
    test_only
}

/// The lines compiled outside tests: every item under a test-only `cfg`
/// attribute (a module, a helper, a statement) is skipped with its block.
fn production_lines(text: &str) -> Vec<&str> {
    let mut kept = Vec::new();
    let mut lines = text.lines();
    while let Some(line) = lines.next() {
        if !is_test_cfg(line.trim()) {
            kept.push(line);
            continue;
        }
        let item = lines.by_ref().find(|next| !next.trim().starts_with("#["));
        let mut depth = item.map_or(0, brace_balance);
        while depth > 0 {
            match lines.next() {
                Some(inner) => depth += brace_balance(inner),
                None => break,
            }
        }
    }
    kept
}

/// Every unadmitted mutator call in one file's production lines, as
/// `"{relative}: {mutator}"`. Each class has its own admission tokens and
/// reach; comment and string lines neither mutate nor admit.
fn unadmitted_calls(relative: &str, text: &str) -> Vec<String> {
    let lines = production_lines(text);
    let classes = [
        (STORE_WIDE_MUTATORS, STORE_WIDE_ADMISSION, ADMISSION_WINDOW),
        (NAMED_WRITERS, NAMED_ADMISSION, NAMED_WINDOW),
    ];
    let mut unadmitted = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if is_inert(line) {
            continue;
        }
        let code = line.trim_start();
        for (mutators, admission, reach) in classes {
            for mutator in mutators {
                if !code.contains(mutator) || code.contains(&format!("fn {mutator}")) {
                    continue;
                }
                let admitted = lines[index.saturating_sub(reach)..=index]
                    .iter()
                    .filter(|above| !is_inert(above))
                    .any(|above| admission.iter().any(|token| above.contains(token)));
                if !admitted {
                    unadmitted.push(format!("{relative}: {mutator}"));
                }
            }
        }
    }
    unadmitted
}

#[test]
fn a_branch_permit_never_admits_a_store_wide_mutation() {
    let permit_only = r#"
async fn take_origin_target(guard: &JjStoreGuard) -> Result<(), String> {
    guard.branch_write_permit(&db, &reservation, project, target).await?;
    crate::jj::import_git(&jj, &store)?;
    Ok(())
}
"#;
    assert_eq!(
        unadmitted_calls("sample.rs", permit_only),
        vec!["sample.rs: import_git(".to_string()],
        "an import can move other owed branches a permit does not cover"
    );

    let bracketed = r#"
async fn take_origin_target(guard: JjStoreGuard) -> Result<(), String> {
    guard.branch_write_permit(&db, &reservation, project, target).await?;
    orch.bracket_store_mutation(guard, Some(&reservation), db, project, move || {
        crate::jj::import_git(&jj, &store)
    })
    .await?;
    Ok(())
}
"#;
    assert!(unadmitted_calls("sample.rs", bracketed).is_empty());

    let one_branch_take = r#"
async fn take_origin_target(guard: JjStoreGuard) -> Result<(), String> {
    guard.branch_write_permit(&db, &reservation, project, target).await?;
    tokio::task::spawn_blocking(move || {
        crate::jj::fast_forward_branch_from_origin(&jj, &store, &target, &observed)
    });
    Ok(())
}
"#;
    assert!(
        unadmitted_calls("sample.rs", one_branch_take).is_empty(),
        "the origin take moves only its own branch, so its permit admits it"
    );

    let mentioned = r#"
fn publish() {
    // branch_write_permit( is minted by the caller
    crate::jj::publish_branch_to_origin(jj, store, branch);
}
"#;
    assert_eq!(
        unadmitted_calls("sample.rs", mentioned),
        vec!["sample.rs: publish_branch_to_origin(".to_string()],
        "a comment mentioning a permit is not one"
    );

    let permitted = r#"
fn publish() {
    let _permit = guard.branch_write_permit(&db, &reservation, project, branch).await?;
    crate::jj::publish_branch_to_origin(jj, store, branch);
}
"#;
    assert!(unadmitted_calls("sample.rs", permitted).is_empty());
}

#[test]
fn the_brace_count_ignores_literals_and_comments() {
    assert_eq!(brace_balance(r#"mod tests {"#), 1);
    assert_eq!(brace_balance(r#"let s = format!("{{"); {"#), 1);
    assert_eq!(brace_balance(r#"let c = '{'; let d = '\''; }"#), -1);
    assert_eq!(brace_balance("let x = 1; // }"), 0);
}

#[test]
fn every_production_store_wide_mutation_is_admitted_or_named_as_owed() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&root, &mut files);
    let test_only = test_only_files(&files);

    let mut unadmitted = Vec::new();
    for file in &files {
        let name = file.file_name().unwrap().to_string_lossy();
        if name == "tests.rs" || name.ends_with("_tests.rs") || test_only.contains(file) {
            continue;
        }
        let text = std::fs::read_to_string(file).unwrap();
        let relative = file
            .strip_prefix(&root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        unadmitted.extend(unadmitted_calls(&relative, &text));
    }
    unadmitted.sort();
    let mut expected: Vec<String> = OWED
        .iter()
        .chain(ADMITTED_ELSEWHERE)
        .map(|(file, mutator, _why)| format!("{file}: {mutator}"))
        .collect();
    expected.sort();
    assert_eq!(
        unadmitted, expected,
        "a production bookmark writer runs unadmitted: a store-wide mutation needs \
         `bracket_store_mutation` (or the same-hold `undo_own_operations`), never just a branch \
         permit; a named writer needs its branch's permit or the bracket. Admit it; or, only \
         while its owner is wiring it, name it in OWED with that owner; or, if a named writer's \
         admission is real but far from the call, name it in ADMITTED_ELSEWHERE with where"
    );
}
