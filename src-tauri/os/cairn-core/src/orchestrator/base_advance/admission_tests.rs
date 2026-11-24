//! Branch write admission against the acceptance journal: the permit a writer
//! that names its branch mints under the store guard, and the bracket around a
//! store-wide mutation. A real store, real journal rows, and a source contract
//! that keeps every production store-wide mutation inside one or the other.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::orchestrator::base_advance::preparation::journal_store_key;
use crate::orchestrator::base_advance::preparation::tests::{
    harness_or_skip, Harness, BRANCH, MAIN_LINE,
};
use crate::orchestrator::{BranchAdmissionRefusal, Orchestrator, StoreLockClass};
use crate::storage::RowExt;

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

    // Landing alone consumes committed store state while this issue's cells
    // run. A different issue is not an exemption, and ordinary writers remain
    // held even after a landing permit has been minted under this guard.
    for (issue, admitted) in [("issue-1", true), ("another-issue", false)] {
        let result = guard
            .branch_write_permit(
                &h.db,
                &reservation,
                "proj-1",
                BRANCH,
                BatchAdmission::LandingIssue(issue),
            )
            .await
            .map(|_| ());
        assert_eq!(result.is_ok(), admitted, "{issue}: {result:?}");
    }
    assert!(matches!(
        guard
            .branch_write_permit(&h.db, &reservation, "proj-1", BRANCH, BatchAdmission::None)
            .await,
        Err(BranchAdmissionRefusal::InFlightBatch { .. })
    ));

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
    drop(guard);
    drop(reservation);

    // The same durable wait without commit authority must admit a sibling
    // writer and must not make preparation Busy.
    h.db.execute(
        "UPDATE agent_waits SET condition_json='{\"kind\":\"run_batch\",\"request_id\":\"req-own\",\"commits\":false}' WHERE id='wait-batch'",
        (),
    ).await.unwrap();
    admit(&h.orch, &h, BRANCH)
        .await
        .expect("a commitless wait admits sibling writes");
    h.prepare(super::preparation::PreparationPurpose::OwnerCommit)
        .await
        .unwrap_or_else(|refusal| panic!("a commitless wait cannot hold preparation: {refusal}"));
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
    "retire_bookmark_expecting(",
    "repair_conflicted_branch_name(",
    "repair_conflicted_bookmark_name(",
    "collapse_divergent_bookmark(",
    "install_branch_advance_local(",
    "install_default_landing(",
    // Moves only its branch: the local bookmark, its tracked remote and git
    // ref by compare-and-swap, and exports that branch alone. It never imports.
    "fast_forward_branch_from_origin(",
];

/// What admits a store-wide mutation; each entry admits only when every one of
/// its tokens is in reach. Never a branch permit: a mutation that can move
/// bookmarks it does not name is safe for every other owed branch only inside
/// the bracket.
///
/// `bracket_admitted_store_mutation(` is the bracket without its
/// foreign-reservation refusal, so it never admits alone: the pair admits,
/// because `admit_store_import(` is what refused or returned the reserved
/// names the import then preserves.
const STORE_WIDE_ADMISSION: &[&[&str]] = &[
    &["bracket_store_mutation("],
    &["admit_store_import(", "bracket_admitted_store_mutation("],
];

/// What admits a named writer: its branch's permit (`admit_landing_writers(`,
/// in `pr_data::actions::default_landing`, mints `branch_write_permit` for a
/// landing's source and target together), or the bracket around it.
const NAMED_ADMISSION: &[&[&str]] = &[
    &["branch_write_permit("],
    &["branch_create_permit("],
    &["admit_landing_writers("],
    &["bracket_store_mutation("],
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
        "pr_data/actions/default_landing.rs",
        "install_default_landing(",
        "advance_landing_bookmarks is the shared publication installer: local ordinary and \
         approval paths admit both coordinates before journaling; remote publish_released \
         admits again after reacquiring the guard. Every caller retains that guard and \
         reservation through this native CAS and named export",
    ),
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
                let window: Vec<&str> = lines[index.saturating_sub(reach)..=index]
                    .iter()
                    .copied()
                    .filter(|above| !is_inert(above))
                    .collect();
                let admitted = admission.iter().any(|tokens| {
                    tokens
                        .iter()
                        .all(|token| window.iter().any(|above| above.contains(token)))
                });
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
fn advance() {
    // branch_write_permit( is minted by the caller
    crate::jj::set_bookmark_at(jj, store, branch, commit);
}
"#;
    assert_eq!(
        unadmitted_calls("sample.rs", mentioned),
        vec!["sample.rs: set_bookmark_at(".to_string()],
        "a comment mentioning a permit is not one"
    );

    let permitted = r#"
fn advance() {
    let _permit = guard.branch_write_permit(&db, &reservation, project, branch).await?;
    crate::jj::set_bookmark_at(jj, store, branch, commit);
}
"#;
    assert!(unadmitted_calls("sample.rs", permitted).is_empty());
}

#[test]
fn an_admitted_bracket_admits_only_beside_its_import_admission() {
    let wrapper_only = r#"
fn import(orch: &Orchestrator, guard: JjStoreGuard) -> Result<(), String> {
    run_db_borrowed(orch.bracket_admitted_store_mutation(guard, db, project, move || {
        crate::jj::ensure_project_store(&jj, &store, &repo)
    }))
}
"#;
    assert_eq!(
        unadmitted_calls("sample.rs", wrapper_only),
        vec!["sample.rs: ensure_project_store(".to_string()],
        "the admitted bracket skips the foreign-reservation refusal, so alone it admits nothing"
    );

    let admission_only = r#"
fn import(orch: &Orchestrator, guard: JjStoreGuard) -> Result<(), String> {
    let reserved = orch.admit_store_import(&guard, None)?;
    crate::jj::import_git(&jj.preserving_import_bookmarks(reserved), &store)
}
"#;
    assert_eq!(
        unadmitted_calls("sample.rs", admission_only),
        vec!["sample.rs: import_git(".to_string()],
        "an import admitted but run outside any bracket is still unbracketed"
    );

    let paired = r#"
fn import(orch: &Orchestrator, guard: JjStoreGuard) -> Result<(), String> {
    let reserved = orch.admit_store_import(&guard, None)?;
    let jj = jj.preserving_import_bookmarks(reserved);
    run_db_borrowed(orch.bracket_admitted_store_mutation(guard, db, project, move || {
        crate::jj::ensure_project_store(&jj, &store, &repo)
    }))
}
"#;
    assert!(unadmitted_calls("sample.rs", paired).is_empty());
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
         `bracket_store_mutation` (or \
         `admit_store_import` paired with `bracket_admitted_store_mutation`), never just a \
         branch permit; a named writer needs its branch's permit or the bracket. Admit it; or, only \
         while its owner is wiring it, name it in OWED with that owner; or, if a named writer's \
         admission is real but far from the call, name it in ADMITTED_ELSEWHERE with where"
    );
}

#[test]
fn run_batch_branch_fences_share_one_capability_predicate() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&root, &mut files);
    let test_only = test_only_files(&files);
    let mut definitions = Vec::new();
    for file in &files {
        let name = file.file_name().unwrap().to_string_lossy();
        if name == "tests.rs" || name.ends_with("_tests.rs") || test_only.contains(file) {
            continue;
        }
        let text = std::fs::read_to_string(file).unwrap();
        let code = production_lines(&text)
            .into_iter()
            .filter(|line| !is_inert(line))
            .collect::<Vec<_>>()
            .join("\n");
        let relative = file
            .strip_prefix(&root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        if code.contains("fn run_batch_holds_branch(") {
            definitions.push(relative.clone());
        }
        if relative != "mcp/handlers/durable_suspend.rs" {
            for independent in [
                "commits && !host",
                "commits || (!verdict_only",
                "admitted && !host",
            ] {
                assert!(
                    !code.contains(independent),
                    "{relative} redefines batch hold capability"
                );
            }
        }
    }
    assert_eq!(definitions, ["mcp/handlers/durable_suspend.rs"]);

    for (file, reader) in [
        (
            "mcp/handlers/run/mod.rs",
            "run_batch_holds_branch(self.commits, self.facts().host)",
        ),
        (
            "mcp/handlers/durable_suspend.rs",
            "run_batch_holds_branch(*commits, *host)",
        ),
        (
            "mcp/handlers/run/detached.rs",
            "durable_suspend::run_batch_holds_branch(",
        ),
        ("orchestrator/base_advance.rs", "condition.holds_branch()"),
        // Ordinary admission delegates to the same landing-aware reader.
        (
            "orchestrator/base_advance.rs",
            "inflight_run_batch_for_landing(db, project_id, branch, prefer_job, exempt, None).await",
        ),
        (
            "orchestrator/base_advance.rs",
            "durable_suspend::run_batch_holds_branch(",
        ),
        (
            "orchestrator/store_lock.rs",
            "base_advance::inflight_run_batch_for_landing(",
        ),
        (
            "orchestrator/base_advance/preparation.rs",
            "super::inflight_run_batch_for_landing(",
        ),
        (
            "orchestrator/base_advance/replay_requests.rs",
            "super::inflight_run_batch_on_branch(",
        ),
        (
            "resources/mutations/dispatch/rebase.rs",
            "inflight_run_batch_on_branch(",
        ),
        ("resources/rebase.rs", "inflight_run_batch_on_branch("),
    ] {
        let text = std::fs::read_to_string(root.join(file)).unwrap();
        let code = production_lines(&text)
            .into_iter()
            .filter(|line| !is_inert(line))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            code.contains(reader),
            "{file} must use canonical reader {reader}"
        );
    }
}
