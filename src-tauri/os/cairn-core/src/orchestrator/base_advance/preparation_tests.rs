//! Real-store preparation tests: a jj store, a migrated database and an
//! orchestrator pointed at both, with a barrier between detached computation
//! and installation so tests can move the owner, the base or the relationship.

use super::*;
use crate::storage::DbError;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use tempfile::TempDir;

type Barrier = Box<dyn FnMut(u32) + Send>;

fn barriers() -> &'static Mutex<HashMap<PathBuf, Barrier>> {
    static BARRIERS: OnceLock<Mutex<HashMap<PathBuf, Barrier>>> = OnceLock::new();
    BARRIERS.get_or_init(Default::default)
}

/// Called after detached computation, before the install lock is taken.
pub(in crate::orchestrator::base_advance) fn before_install(store: &Path, attempt: u32) {
    let mut barrier = barriers().lock().unwrap().remove(store);
    if let Some(barrier) = barrier.as_mut() {
        barrier(attempt);
    }
    if let Some(barrier) = barrier {
        barriers()
            .lock()
            .unwrap()
            .insert(store.to_path_buf(), barrier);
    }
}

fn basis_barriers() -> &'static Mutex<HashMap<PathBuf, Barrier>> {
    static BARRIERS: OnceLock<Mutex<HashMap<PathBuf, Barrier>>> = OnceLock::new();
    BARRIERS.get_or_init(Default::default)
}

/// Called after an owner conflict's basis was assessed off-lock, before the
/// lock under which it is recorded. The argument counts calls per store.
pub(super) fn after_basis_assessment(store: &Path) {
    let mut barrier = basis_barriers().lock().unwrap().remove(store);
    if let Some(barrier) = barrier.as_mut() {
        barrier(0);
    }
    if let Some(barrier) = barrier {
        basis_barriers()
            .lock()
            .unwrap()
            .insert(store.to_path_buf(), barrier);
    }
}

pub(in crate::orchestrator::base_advance) const JOB: &str = "job-prep";
pub(in crate::orchestrator::base_advance) const RUN: &str = "run-prep";
pub(in crate::orchestrator::base_advance) const BRANCH: &str = "agent/prep";
pub(in crate::orchestrator::base_advance) const MAIN_LINE: &str = "agent/main-line";

/// A real store, migrated database and orchestrator; shared with sibling
/// real-store test modules under `base_advance`.
pub(in crate::orchestrator::base_advance) struct Harness {
    _home: TempDir,
    _repo: TempDir,
    pub(in crate::orchestrator::base_advance) workspaces: TempDir,
    pub(in crate::orchestrator::base_advance) repo: PathBuf,
    pub(in crate::orchestrator::base_advance) jj: crate::jj::JjEnv,
    pub(in crate::orchestrator::base_advance) store: PathBuf,
    pub(in crate::orchestrator::base_advance) db: Arc<LocalDb>,
    pub(in crate::orchestrator::base_advance) orch: Orchestrator,
    pub(in crate::orchestrator::base_advance) main_ws: PathBuf,
    pub(in crate::orchestrator::base_advance) branch_ws: PathBuf,
}

impl Drop for Harness {
    fn drop(&mut self) {
        barriers().lock().unwrap().remove(&self.store);
    }
}

pub(in crate::orchestrator::base_advance) fn git(repo: &Path, args: &[&str]) -> String {
    let out = crate::env::git()
        .args(args)
        .current_dir(repo)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

pub(in crate::orchestrator::base_advance) async fn harness(bin: &str, name: &str) -> Harness {
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;

    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let workspaces = tempfile::tempdir().unwrap();
    std::fs::write(repo.path().join("shared.txt"), "original\n").unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    git(repo.path(), &["config", "user.email", "tests@cairn.local"]);
    git(repo.path(), &["config", "user.name", "Cairn tests"]);
    git(repo.path(), &["add", "-A"]);
    git(repo.path(), &["commit", "-q", "-m", "base"]);

    let jj = crate::jj::environment(bin, home.path());
    let store = crate::jj::project_store_dir(home.path(), repo.path());
    crate::jj::ensure_project_store(&jj, &store, repo.path()).unwrap();
    let main_ws = workspaces.path().join("main-line");
    crate::jj::add_workspace(&jj, &store, &main_ws, MAIN_LINE, "main", None).unwrap();
    let branch_ws = workspaces.path().join("branch");
    crate::jj::add_workspace(&jj, &store, &branch_ws, BRANCH, "main", None).unwrap();

    let db = Arc::new(crate::storage::migrated_test_db(name).await);
    let repo_sql = repo.path().to_string_lossy().replace('\'', "''");
    db.execute_script(format!(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
           VALUES ('proj-1', 'default', 'Project', 'proj', '{repo_sql}', 'main', 1, 1);
         INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
           VALUES ('issue-1', 'proj-1', 1, 'Issue', 'active', 1, 1);
         INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
           VALUES ('exec-1', 'recipe-default', 'issue-1', 'proj-1', 'running', 1, 1);
         INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
           VALUES ('{JOB}', 'exec-1', 'node', 'issue-1', 'proj-1', 'running', '{BRANCH}', NULL, 1, 1);
         INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
           VALUES ('{RUN}', 'issue-1', 'proj-1', '{JOB}', 'live', 1, 1);"
    ))
    .await
    .unwrap();
    let orch = Orchestrator::builder(
        Arc::new(DbState::new(
            db.clone(),
            Arc::new(SearchIndex::open_or_create(home.path().join("search")).unwrap()),
        )),
        Arc::new(TestServicesBuilder::new().build()),
        home.path().to_path_buf(),
    )
    .jj_binary_path(bin)
    .build();
    Harness {
        repo: repo.path().to_path_buf(),
        _home: home,
        _repo: repo,
        workspaces,
        jj,
        store,
        db,
        orch,
        main_ws,
        branch_ws,
    }
}

impl Harness {
    pub(in crate::orchestrator::base_advance) fn commit(
        &self,
        workspace: &Path,
        bookmark: &str,
        file: &str,
        content: &str,
    ) -> String {
        let _ = crate::jj::update_stale(&self.jj, workspace);
        std::fs::write(workspace.join(file), content).unwrap();
        let message = format!("edit {file}");
        if let Err(error) = crate::jj::seal(&self.jj, workspace, &message, None) {
            // An installed advance moved the bookmark past this workspace and
            // seal refuses from behind its tip: commit on the tip instead.
            let error = error.to_string();
            assert!(error.contains("behind its branch tip"), "seal: {error}");
            self.jj
                .run(workspace, &["new", bookmark], "start on the bookmark tip")
                .unwrap();
            std::fs::write(workspace.join(file), content).unwrap();
            crate::jj::seal(&self.jj, workspace, &message, None).unwrap();
        }
        self.tip(bookmark)
    }

    pub(in crate::orchestrator::base_advance) fn tip(&self, bookmark: &str) -> String {
        crate::jj::bookmark_commit(&self.jj, &self.store, bookmark).unwrap()
    }

    pub(in crate::orchestrator::base_advance) fn set_bookmark(
        &self,
        bookmark: &str,
        revision: &str,
    ) {
        self.jj
            .run(
                &self.store,
                &[
                    "bookmark",
                    "set",
                    bookmark,
                    "-r",
                    revision,
                    "--allow-backwards",
                    "--ignore-working-copy",
                ],
                "move bookmark",
            )
            .unwrap();
    }

    /// Land `content` in `file` on main through the main line.
    pub(in crate::orchestrator::base_advance) fn advance_main(
        &self,
        file: &str,
        content: &str,
    ) -> String {
        let tip = self.commit(&self.main_ws, MAIN_LINE, file, content);
        self.set_bookmark("main", &tip);
        tip
    }

    pub(in crate::orchestrator::base_advance) fn is_ancestor(
        &self,
        ancestor: &str,
        descendant: &str,
    ) -> bool {
        crate::env::git()
            .args(["merge-base", "--is-ancestor", ancestor, descendant])
            .current_dir(&self.repo)
            .status()
            .unwrap()
            .success()
    }

    pub(in crate::orchestrator::base_advance) fn set_barrier(
        &self,
        barrier: impl FnMut(u32) + Send + 'static,
    ) {
        barriers()
            .lock()
            .unwrap()
            .insert(self.store.clone(), Box::new(barrier));
    }

    pub(in crate::orchestrator::base_advance) async fn prepare(
        &self,
        purpose: PreparationPurpose,
    ) -> Result<PreparedBranch, PreparationRefusal> {
        prepare_branch(
            &self.orch,
            PreparationRequest::new("proj-1", BRANCH, Some(JOB.to_string()), purpose),
        )
        .await
    }

    pub(in crate::orchestrator::base_advance) async fn messages(&self) -> Vec<(String, String)> {
        self.db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT m.content, COALESCE(
                                (SELECT wake FROM attention_pushes a WHERE a.recipient = ?1
                                  ORDER BY a.rowid DESC LIMIT 1), '')
                               FROM messages m ORDER BY m.created_at",
                            (JOB,),
                        )
                        .await?;
                    let mut all = Vec::new();
                    while let Some(row) = rows.next().await? {
                        all.push((row.text(0)?, row.text(1)?));
                    }
                    Ok::<_, DbError>(all)
                })
            })
            .await
            .unwrap()
    }

    pub(in crate::orchestrator::base_advance) async fn wakes(&self) -> Vec<String> {
        self.db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query("SELECT wake FROM attention_pushes ORDER BY rowid", ())
                        .await?;
                    let mut all = Vec::new();
                    while let Some(row) = rows.next().await? {
                        all.push(row.text(0)?);
                    }
                    Ok::<_, DbError>(all)
                })
            })
            .await
            .unwrap()
    }

    pub(in crate::orchestrator::base_advance) async fn item_outcomes(
        &self,
    ) -> Vec<(String, String)> {
        self.db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT i.outcome_kind, n.status FROM jj_reconcile_items i
                               JOIN jj_reconcile_intents n ON n.id = i.intent_id
                              WHERE i.bookmark = ?1 ORDER BY i.updated_at",
                            (BRANCH,),
                        )
                        .await?;
                    let mut all = Vec::new();
                    while let Some(row) = rows.next().await? {
                        all.push((row.text(0)?, row.text(1)?));
                    }
                    Ok::<_, DbError>(all)
                })
            })
            .await
            .unwrap()
    }
}

macro_rules! harness_or_skip {
    ($name:literal) => {{
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        $crate::orchestrator::base_advance::preparation::tests::harness(&bin, $name).await
    }};
}
#[allow(unused_imports)]
pub(crate) use harness_or_skip;

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_clean_check_preparation_advances_exports_and_notes_passively_once() {
    let h = harness_or_skip!("prep_clean");
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let base = h.advance_main("incoming.txt", "theirs\n");

    let prepared = h.prepare(PreparationPurpose::Check).await.unwrap();
    assert!(prepared.advanced);
    assert_eq!(prepared.head_before, own);
    assert_eq!(prepared.base_commit, base);
    assert_eq!(prepared.relationship.base_branch, "main");
    assert_eq!(
        prepared.relationship.declared, None,
        "undeclared follows the default"
    );
    assert_eq!(prepared.export, PreparedExport::Exported);
    assert_eq!(h.tip(BRANCH), prepared.head_after);
    assert!(h.is_ancestor(&base, &prepared.head_after));
    assert_eq!(
        git(&h.repo, &["rev-parse", BRANCH]),
        prepared.head_after,
        "the Git ref is exported to the installed head"
    );
    assert_eq!(
        git(
            &h.repo,
            &["rev-parse", &format!("{}^{{tree}}", prepared.head_after)]
        ),
        prepared.tree
    );
    assert_eq!(h.tip("main"), base, "the base never moves");

    let again = h.prepare(PreparationPurpose::Check).await.unwrap();
    assert!(
        !again.advanced,
        "an up-to-date branch is captured, not replayed"
    );
    assert_eq!(again.head_before, prepared.head_after);
    assert_eq!(again.head_after, prepared.head_after);
    assert_eq!(again.tree, prepared.tree);

    let messages = h.messages().await;
    assert_eq!(
        messages.len(),
        1,
        "one passive note per base coordinate: {messages:?}"
    );
    assert!(messages[0].0.contains("not a green build"));
    assert_eq!(
        h.wakes().await,
        vec!["passive".to_string()],
        "nothing resumes the owner"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_check_conflict_records_evidence_without_a_session_or_wake_and_never_suppresses_the_owner(
) {
    let h = harness_or_skip!("prep_conflict");
    let own = h.commit(&h.branch_ws, BRANCH, "shared.txt", "mine\n");
    let base = h.advance_main("shared.txt", "theirs\n");

    for _ in 0..2 {
        let PreparationRefusal::Conflict(conflict) =
            h.prepare(PreparationPurpose::Check).await.unwrap_err()
        else {
            panic!("a conflicting check preparation is a conflict refusal");
        };
        let BaseConflict {
            head,
            base_commit,
            paths,
            record,
            ..
        } = *conflict;
        assert_eq!(
            (head.as_str(), base_commit.as_str()),
            (own.as_str(), base.as_str())
        );
        assert_eq!(paths, vec!["shared.txt".to_string()]);
        assert!(matches!(record, ConflictRecord::CheckEvidence { .. }));
    }
    assert_eq!(h.tip(BRANCH), own, "a conflict installs nothing");
    assert!(
        h.messages().await.is_empty(),
        "a check conflict sends nothing"
    );
    assert!(load_active_session(&h.db, BRANCH).await.unwrap().is_none());
    assert_eq!(
        h.item_outcomes().await,
        vec![(OUTCOME_CHECK_CONFLICT.to_string(), "completed".to_string())],
        "evidence lives on a completed, unclaimable intent row"
    );

    // The owner's own commit at the same coordinate still opens a session and
    // wakes, although a check already recorded the same fingerprint.
    let PreparationRefusal::Conflict(conflict) = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err()
    else {
        panic!("the owner's commit conflicts too");
    };
    let record = conflict.record;
    assert!(matches!(record, ConflictRecord::Session { .. }));
    let session = load_active_session(&h.db, BRANCH)
        .await
        .unwrap()
        .expect("session");
    assert_eq!(session.destination_commit, base);
    // The same head prepared again binds to that session and wakes no one.
    let bound = h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    assert!(!bound.advanced);
    assert_eq!(
        bound.session.map(|s| s.intent_id),
        Some(session.intent_id.clone())
    );
    // A steer-urgency delivery is stored with the `wake` level.
    assert_eq!(
        h.wakes().await,
        vec!["wake".to_string()],
        "one wake per fingerprint"
    );
    assert_eq!(h.tip(BRANCH), own);

    // A later check never downgrades the owner's session to check evidence.
    let _ = h.prepare(PreparationPurpose::Check).await.unwrap_err();
    assert!(load_active_session(&h.db, BRANCH).await.unwrap().is_some());
    assert_eq!(h.wakes().await.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_resolution_commit_binds_only_to_the_session_at_the_current_base() {
    let h = harness_or_skip!("prep_session_rebind");
    let own = h.commit(&h.branch_ws, BRANCH, "shared.txt", "mine\n");
    let first = h.advance_main("shared.txt", "theirs one\n");

    let PreparationRefusal::Conflict(conflict) = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err()
    else {
        panic!("the owner's commit conflicts");
    };
    let record = conflict.record;
    let ConflictRecord::Session {
        intent_id: first_session,
        ..
    } = record
    else {
        panic!("an owner conflict opens a session");
    };

    // A commit resolving the recorded conflict proceeds without catch-up.
    let bound = h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    assert!(!bound.advanced);
    assert_eq!(
        (bound.head_after.as_str(), bound.base_commit.as_str()),
        (own.as_str(), first.as_str())
    );
    assert_eq!(
        bound.session.as_ref().map(|s| s.intent_id.as_str()),
        Some(first_session.as_str())
    );

    // The base moves again. The old session binds nothing: the commit
    // recomputes, conflicts, and reopens the session at the new base.
    let second = h.advance_main("shared.txt", "theirs two\n");
    let refusal = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err();
    let PreparationRefusal::Conflict(conflict) = refusal else {
        panic!("a second base move is not bypassed: {refusal}");
    };
    let BaseConflict {
        base_commit,
        record,
        ..
    } = *conflict;
    assert_eq!(base_commit, second);
    let ConflictRecord::Session {
        intent_id: second_session,
        reopened,
    } = record
    else {
        panic!("the session reopens");
    };
    assert!(reopened);
    assert_ne!(second_session, first_session);
    assert_eq!(
        h.tip(BRANCH),
        own,
        "nothing installed across either conflict"
    );

    // And the next resolution commit binds to the new session: progress, not
    // a recursive demand for an impossible catch-up.
    let rebound = h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    assert_eq!(rebound.base_commit, second);
    assert_eq!(rebound.session.map(|s| s.intent_id), Some(second_session));
    assert_eq!(
        h.wakes().await,
        vec!["wake".to_string(), "wake".to_string()],
        "one steer per fingerprint, stored at the wake level"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_attestation_recorded_during_basis_assessment_is_not_overwritten() {
    use crate::orchestrator::conflict_session::{record_attested_basis, ResolvedSide};
    let h = harness_or_skip!("prep_basis_race");
    h.commit(&h.branch_ws, BRANCH, "shared.txt", "mine\n");
    h.advance_main("shared.txt", "theirs one\n");
    let PreparationRefusal::Conflict(conflict) = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err()
    else {
        panic!("the owner's commit conflicts");
    };
    let record = conflict.record;
    let ConflictRecord::Session {
        intent_id: first, ..
    } = record
    else {
        panic!("an owner conflict opens a session");
    };

    // The base moves, so the next owner commit reopens the session and must
    // inherit the first session's basis. While that basis is assessed, an
    // attestation is recorded on the first session at the same coordinates.
    h.advance_main("shared.txt", "theirs two\n");
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (db, seen) = (h.db.clone(), calls.clone());
    // The refusal carries no diagnostic; the recorded session is its home.
    let recorded = load_active_session(&h.db, BRANCH)
        .await
        .unwrap()
        .expect("the owner conflict is recorded");
    let (base, theirs) = (recorded.base.unwrap(), recorded.theirs.unwrap());
    basis_barriers().lock().unwrap().insert(
        h.store.clone(),
        Box::new(move |_| {
            if seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                let (db, first) = (db.clone(), first.clone());
                let side = ResolvedSide {
                    base: base.clone(),
                    theirs: theirs.clone(),
                    attested: Some("moved into shared.rs".to_string()),
                };
                std::thread::spawn(move || {
                    tokio::runtime::Runtime::new()
                        .unwrap()
                        .block_on(async move {
                            let basis = [("gone.txt".to_string(), side)].into_iter().collect();
                            record_attested_basis(&db, &first, BRANCH, &basis)
                                .await
                                .unwrap();
                        })
                })
                .join()
                .unwrap();
            }
        }),
    );

    let refusal = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err();
    basis_barriers().lock().unwrap().remove(&h.store);
    let PreparationRefusal::Conflict(conflict) = refusal else {
        panic!("the reopened commit still conflicts: {refusal}");
    };
    let record = conflict.record;
    assert!(matches!(
        record,
        ConflictRecord::Session { reopened: true, .. }
    ));
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "the stale assessment was discarded and the basis re-read from the attested row"
    );
    let session = load_active_session(&h.db, BRANCH)
        .await
        .unwrap()
        .expect("session");
    assert!(
        session.resolution_basis.contains_key("gone.txt"),
        "the attestation survives into the reopened session: {:?}",
        session.resolution_basis
    );
}

#[tokio::test]
async fn the_claims_read_seeks_newest_first_without_a_sorter() {
    let db = crate::storage::migrated_test_db("prep-claims-plan.db").await;
    let plan = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "EXPLAIN QUERY PLAN SELECT head_after FROM branch_advance_acceptances
                          WHERE project_id = ?1 AND store_path = ?2 AND branch = ?3
                            AND status = 'accepted'
                          ORDER BY created_at DESC, id DESC LIMIT ?4",
                        ("p", "/s", "b", 64_i64),
                    )
                    .await?;
                let mut plan = String::new();
                while let Some(row) = rows.next().await? {
                    plan.push_str(&row.text(3)?);
                    plan.push('\n');
                }
                Ok::<_, DbError>(plan)
            })
        })
        .await
        .unwrap();
    assert!(
        plan.contains("idx_branch_advance_acceptances_branch"),
        "the claims read seeks the branch's accepted rows:\n{plan}"
    );
}

/// Turso keeps no statistics, so the plan is the only evidence the index is
/// used; a team replica must seek it as the private database does.
#[tokio::test]
async fn the_dependent_lookup_seeks_its_index_in_both_lineages() {
    use crate::storage::{MigrationRunner, TEAM_MIGRATIONS};
    async fn plan(db: &LocalDb) -> String {
        let sql = format!("EXPLAIN QUERY PLAN {DEPENDENT_BRANCHES_SQL}");
        db.read(move |conn| {
            Box::pin(async move {
                let mut rows = conn.query(&sql, ("p", "agent/x", 513_i64)).await?;
                let mut plan = String::new();
                while let Some(row) = rows.next().await? {
                    plan.push_str(&row.text(3)?);
                    plan.push('\n');
                }
                Ok::<_, DbError>(plan)
            })
        })
        .await
        .unwrap()
    }
    let private = crate::storage::migrated_test_db("prep-dependent-plan.db").await;
    let temp = tempfile::tempdir().unwrap();
    let team = LocalDb::open(temp.path().join("team.db")).await.unwrap();
    MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
        .run(&team)
        .await
        .unwrap();
    for (lineage, db) in [("private", &private), ("team", &team)] {
        let plan = plan(db).await;
        assert!(
            plan.contains("idx_jobs_project_base_branch") && !plan.contains("SORTER"),
            "the {lineage} lineage seeks jobs(project_id, base_branch):\n{plan}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_up_to_date_branch_never_reads_its_dependents_and_a_replay_honors_the_cap() {
    let h = harness_or_skip!("prep_dependent_cap");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let rows: String = (0..=DEPENDENT_COUNT_CAP)
        .map(|n| {
            format!(
                "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
                   VALUES ('job-old-{n}', 'exec-1', 'old-{n}', 'issue-1', 'proj-1', 'complete', 'agent/old-{n}', '{BRANCH}', 9, 9);\n"
            )
        })
        .collect();
    h.db.execute_script(rows).await.unwrap();

    // Already on its base: the coordinate is captured with no traversal, so
    // more historical children than the cap do not matter.
    let prepared = h.prepare(PreparationPurpose::Check).await.unwrap();
    assert!(!prepared.advanced);

    // A replay needs complete ownership evidence and refuses past the bound.
    h.advance_main("incoming.txt", "theirs\n");
    let before = h.tip(BRANCH);
    let refusal = h.prepare(PreparationPurpose::Check).await.unwrap_err();
    assert!(
        matches!(&refusal, PreparationRefusal::Repository(why) if why.contains("not complete")),
        "{refusal}"
    );
    assert_eq!(h.tip(BRANCH), before, "nothing was installed");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_expired_budget_times_the_fast_path_out_with_a_cooldown_and_installs_nothing() {
    let h = harness_or_skip!("prep_fast_path_budget");
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    // The base moves, so the branch needs a replay: a cooldown holds only a
    // replay, never the answer for an already up-to-date branch.
    let base = h.advance_main("incoming.txt", "theirs\n");
    let mut request = PreparationRequest::new(
        "proj-1",
        BRANCH,
        Some(JOB.to_string()),
        PreparationPurpose::Check,
    );
    request.compute_budget = Duration::ZERO;
    let refusal = prepare_branch(&h.orch, request).await.unwrap_err();
    assert!(
        matches!(&refusal, PreparationRefusal::TimedOut { head, base_commit, .. }
            if *head == own && *base_commit == base),
        "a timed-out probe is a typed timeout, not an answer: {refusal}"
    );
    assert_eq!(h.tip(BRANCH), own, "nothing was installed");
    let cooldown =
        h.db.query_text(
            "SELECT fingerprint FROM jj_reconcile_quarantines WHERE bookmark = ?1",
            (BRANCH,),
        )
        .await
        .unwrap();
    assert_eq!(
        cooldown,
        Some(format!("{own}:{base}")),
        "the probe's coordinates cool down"
    );
    // The unchanged head is then held by its cooldown under the normal budget.
    assert!(matches!(
        h.prepare(PreparationPurpose::Check).await.unwrap_err(),
        PreparationRefusal::Cooldown { .. }
    ));
}

static FENCE_WRITES_FAIL: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Fault injection for the store-wide bracket's fence writes (tests are
/// serialized on the jj lock, so one switch serves them all).
pub(in crate::orchestrator::base_advance) fn fail_fence_writes(fail: bool) {
    FENCE_WRITES_FAIL.store(fail, std::sync::atomic::Ordering::SeqCst);
}

pub(super) fn fence_writes_fail() -> bool {
    FENCE_WRITES_FAIL.load(std::sync::atomic::Ordering::SeqCst)
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_left_fence_resolves_from_its_copies_and_a_dismissal_is_exact() {
    let h = harness_or_skip!("prep_fence_dismiss");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    h.advance_main("incoming.txt", "theirs\n");
    failing_acceptances()
        .lock()
        .unwrap()
        .insert(h.store.clone());
    let prepared = h.prepare(PreparationPurpose::Check).await.unwrap();
    failing_acceptances().lock().unwrap().remove(&h.store);
    let owed = outstanding_acceptances(&h.db, "proj-1", &h.store, BRANCH)
        .await
        .unwrap();
    let row = owed[0].id.clone();

    // A bracket fences the row with what it read, then "crashes" before
    // finishing. The branch did not move, so settlement clears the fence and
    // settles the row normally at its exact head.
    let state = crate::jj::read_local_branch_state(&h.store, BRANCH).unwrap();
    fence_acceptances(
        &h.db,
        &[AcceptanceFence {
            id: row.clone(),
            bracket_id: "bracket-1".into(),
            bookmark: state.bookmark.clone(),
            git_ref: state.git_ref.clone(),
        }],
    )
    .await
    .unwrap();
    h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    assert_eq!(acceptance_rows(&h).await[0].0, "accepted");

    // A fence whose copies no longer match makes the row unknown, never
    // accepted, even when the literal head is the candidate head.
    h.advance_main("incoming.txt", "theirs again\n");
    failing_acceptances()
        .lock()
        .unwrap()
        .insert(h.store.clone());
    let second = h.prepare(PreparationPurpose::Check).await.unwrap();
    failing_acceptances().lock().unwrap().remove(&h.store);
    let pending = outstanding_acceptances(&h.db, "proj-1", &h.store, BRANCH)
        .await
        .unwrap()[0]
        .id
        .clone();
    fence_acceptances(
        &h.db,
        &[AcceptanceFence {
            id: pending.clone(),
            bracket_id: "bracket-2".into(),
            bookmark: Some(prepared.head_after.clone()),
            git_ref: None,
        }],
    )
    .await
    .unwrap();
    let refusal = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err();
    assert!(
        matches!(&refusal, PreparationRefusal::Busy(BusyReason::AcceptanceDebt { status, .. }) if status == "unknown"),
        "{refusal}"
    );

    // Dismissal: exact head, reserved branch and a reason; anything else refuses.
    let reservation = h
        .orch
        .reserve_branches(
            &h.store,
            &[BRANCH],
            "test dismissal",
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let guard = h
        .orch
        .acquire_jj_store_lock(&h.store, StoreLockClass::Foreground, "test dismissal")
        .await;
    let dismiss = |head: String, reason: &'static str| {
        let (db, store) = (h.db.clone(), h.store.clone());
        let (guard, reservation, pending) = (&guard, &reservation, pending.clone());
        async move {
            dismiss_unknown_acceptance(
                &db,
                guard,
                reservation,
                "proj-1",
                &store,
                BRANCH,
                &pending,
                &head,
                reason,
            )
            .await
        }
    };
    assert!(
        dismiss(prepared.head_after.clone(), "stale").await.is_err(),
        "wrong head"
    );
    assert!(
        dismiss(second.head_after.clone(), "").await.is_err(),
        "no reason"
    );
    assert_eq!(
        dismiss(second.head_after.clone(), "operator checked the branch")
            .await
            .unwrap(),
        AcceptanceDismissal::Dismissed
    );
    assert_eq!(
        dismiss(second.head_after.clone(), "operator checked the branch")
            .await
            .unwrap(),
        AcceptanceDismissal::AlreadyDismissed
    );
    assert!(
        dismiss(second.head_after.clone(), "another reason")
            .await
            .is_err(),
        "only the same dismissal repeats"
    );
    drop(guard);
    let rows = acceptance_rows(&h).await;
    assert_eq!(rows.last().unwrap().0, "dismissed");
    assert!(outstanding_acceptances(&h.db, "proj-1", &h.store, BRANCH)
        .await
        .unwrap()
        .is_empty());
    let claims = accepted_drop_claims(&h.db, "proj-1", &h.store, BRANCH)
        .await
        .unwrap();
    assert!(
        claims.iter().all(|(head, _)| *head != second.head_after),
        "a dismissed row never authorizes a claim"
    );
}

async fn export_state(h: &Harness) -> Vec<String> {
    h.db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT export_state FROM branch_advance_acceptances WHERE branch = ?1
                      ORDER BY created_at, id",
                    (BRANCH,),
                )
                .await?;
            let mut all = Vec::new();
            while let Some(row) = rows.next().await? {
                all.push(row.text(0)?);
            }
            Ok::<_, DbError>(all)
        })
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_export_lost_after_acceptance_is_recovered_at_the_exact_head_or_superseded() {
    let h = harness_or_skip!("prep_export_recovery");
    let before = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    h.advance_main("incoming.txt", "theirs\n");
    // The export fails after acceptance, its retry too: jj holds the install,
    // the row still owes the export and the Git ref is at the starting head.
    failing_exports().lock().unwrap().insert(h.store.clone());
    let prepared = h.prepare(PreparationPurpose::Check).await.unwrap();
    failing_exports().lock().unwrap().remove(&h.store);
    assert!(
        matches!(
            prepared.export,
            PreparedExport::Failed {
                recorded: Some(_),
                ..
            }
        ),
        "{:?}",
        prepared.export
    );
    assert!(prepared.advanced);
    assert_eq!(export_state(&h).await, vec!["pending".to_string()]);
    assert_eq!(git(&h.repo, &["rev-parse", BRANCH]), before);
    // A check does not hold the reservation: the owed export is reported.
    let checked = h.prepare(PreparationPurpose::Check).await.unwrap();
    assert!(
        matches!(
            checked.export,
            PreparedExport::Failed {
                recorded: Some(_),
                ..
            }
        ),
        "never a false NotNeeded: {:?}",
        checked.export
    );
    let recovered = h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    assert!(!recovered.advanced);
    assert_eq!(
        recovered.export,
        PreparedExport::Exported,
        "the owner performs it"
    );
    assert_eq!(git(&h.repo, &["rev-parse", BRANCH]), prepared.head_after);
    assert_eq!(export_state(&h).await, vec!["exported".to_string()]);

    // Owed again, but the branch moved on: the later writer owns its export.
    h.db.execute(
        "UPDATE branch_advance_acceptances SET export_state = 'pending', exported_at = NULL",
        (),
    )
    .await
    .unwrap();
    h.commit(&h.branch_ws, BRANCH, "later.txt", "later\n");
    h.prepare(PreparationPurpose::Check).await.unwrap();
    assert_eq!(export_state(&h).await, vec!["superseded".to_string()]);
}

fn failing_acceptances() -> &'static Mutex<std::collections::HashSet<PathBuf>> {
    static FAILING: OnceLock<Mutex<std::collections::HashSet<PathBuf>>> = OnceLock::new();
    FAILING.get_or_init(Default::default)
}

/// Fault injection: every acceptance journal transition of `store` fails.
pub(super) fn acceptance_write_fails(store: &Path) -> bool {
    failing_acceptances().lock().unwrap().contains(store)
}

fn failing_exports() -> &'static Mutex<std::collections::HashSet<PathBuf>> {
    static FAILING: OnceLock<Mutex<std::collections::HashSet<PathBuf>>> = OnceLock::new();
    FAILING.get_or_init(Default::default)
}

/// Fault injection: every Git export a preparation of `store` attempts fails
/// without touching Git.
pub(super) fn export_fails(store: &Path) -> bool {
    failing_exports().lock().unwrap().contains(store)
}

async fn acceptance_rows(h: &Harness) -> Vec<(String, String, Vec<String>, Vec<String>)> {
    h.db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status, head_after, dropped_commits_json, inherited_drops_json
                       FROM branch_advance_acceptances WHERE branch = ?1
                      ORDER BY created_at, id",
                    (BRANCH,),
                )
                .await?;
            let mut all = Vec::new();
            while let Some(row) = rows.next().await? {
                all.push((
                    row.text(0)?,
                    row.text(1)?,
                    serde_json::from_str(&row.text(2)?).unwrap(),
                    serde_json::from_str(&row.text(3)?).unwrap(),
                ));
            }
            Ok::<_, DbError>(all)
        })
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_failed_acceptance_write_blocks_publication_until_the_exact_head_settles_it() {
    let h = harness_or_skip!("prep_acceptance_fault");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    h.advance_main("incoming.txt", "theirs\n");
    failing_acceptances()
        .lock()
        .unwrap()
        .insert(h.store.clone());

    // The install is accepted locally; its journal row cannot be marked.
    let prepared = h.prepare(PreparationPurpose::Check).await.unwrap();
    assert!(
        prepared.advanced,
        "an accepted install is never reported as nothing installed"
    );
    assert!(
        prepared
            .warnings
            .iter()
            .any(|warning| warning.contains("still pending")),
        "the pending journal row is reported: {:?}",
        prepared.warnings
    );
    let blocked = outstanding_acceptances(&h.db, "proj-1", &h.store, BRANCH)
        .await
        .unwrap()[0]
        .id
        .clone();
    assert_eq!(h.tip(BRANCH), prepared.head_after);
    let rows = acceptance_rows(&h).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "pending");

    // Any spelling of the store finds the same debt: a `..` path and a
    // symbolic link resolve to the journal's canonical identity.
    let dotted = h.store.join("..").join(h.store.file_name().unwrap());
    let alias = h.workspaces.path().join("store-alias");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&h.store, &alias).unwrap();
    #[cfg(windows)]
    std::os::windows::fs::symlink_dir(&h.store, &alias).unwrap();
    for spelling in [&dotted, &alias] {
        let owed = outstanding_acceptances(&h.db, "proj-1", spelling, BRANCH)
            .await
            .unwrap();
        assert_eq!(owed.len(), 1, "{} must find the debt", spelling.display());
    }
    assert!(
        outstanding_acceptances(&h.db, "proj-1", &h.workspaces.path().join("absent"), BRANCH)
            .await
            .is_err(),
        "an unresolvable store fails closed, never reads as no debt"
    );

    // While owed, an owner's commit is refused and a check is flagged.
    let refusal = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err();
    assert!(
        matches!(&refusal, PreparationRefusal::Busy(BusyReason::AcceptanceDebt { acceptance_id, .. })
            if *acceptance_id == blocked),
        "{refusal}"
    );
    let flagged = h.prepare(PreparationPurpose::Check).await.unwrap();
    assert!(
        flagged
            .warnings
            .iter()
            .any(|warning| warning.contains(&blocked)),
        "a check judging the coordinate names the owed row: {:?}",
        flagged.warnings
    );

    // Once the journal writes again, the exact head settles the row.
    failing_acceptances().lock().unwrap().remove(&h.store);
    let settled = h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    assert!(settled.warnings.is_empty(), "{:?}", settled.warnings);
    assert_eq!(acceptance_rows(&h).await[0].0, "accepted");
    assert!(outstanding_acceptances(&h.db, "proj-1", &h.store, BRANCH)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_unprovable_head_keeps_the_debt_and_its_claims() {
    let h = harness_or_skip!("prep_acceptance_unknown");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    h.advance_main("incoming.txt", "theirs\n");
    failing_acceptances()
        .lock()
        .unwrap()
        .insert(h.store.clone());
    let prepared = h.prepare(PreparationPurpose::Check).await.unwrap();
    failing_acceptances().lock().unwrap().remove(&h.store);
    // A commit lands on the branch outside the gate: neither head the row knows.
    h.commit(&h.branch_ws, BRANCH, "later.txt", "later\n");
    let refusal = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err();
    assert!(
        matches!(&refusal, PreparationRefusal::Busy(BusyReason::AcceptanceDebt { status, .. }) if status == "unknown"),
        "{refusal}"
    );
    let rows = acceptance_rows(&h).await;
    assert_eq!(rows[0].0, "unknown", "never abandoned without proof");
    assert_eq!(rows[0].1, prepared.head_after);
    // An unproven row keeps its claims but they are no evidence: neither a
    // publication nor a later advance's inheritance may use them.
    assert!(accepted_drop_claims(&h.db, "proj-1", &h.store, BRANCH)
        .await
        .unwrap()
        .is_empty());

    // The shared settle-first entry for writers outside preparation agrees.
    let guard = h
        .orch
        .acquire_jj_store_lock(&h.store, StoreLockClass::Foreground, "test writer")
        .await;
    let owed = settle_branch_acceptances(&h.db, &guard, "proj-1", &h.store, BRANCH)
        .await
        .unwrap()
        .expect("still owed");
    assert_eq!(owed.status, "unknown");
    drop(guard);

    // A conflicted name on an owed branch is never repaired: the repair could
    // choose the row's candidate head and launder an unproven acceptance.
    // Both targets differ from the current tip; a side that sets the tip it
    // already has changes nothing and jj merges the operations trivially.
    let op = crate::jj::operation_id(&h.jj, &h.store).unwrap();
    let tip = h.tip(BRANCH);
    for target in [prepared.head_before.as_str(), prepared.head_after.as_str()] {
        h.jj.run(
            &h.store,
            &[
                "--at-op",
                &op,
                "bookmark",
                "set",
                BRANCH,
                "-r",
                target,
                "--allow-backwards",
                "--ignore-working-copy",
            ],
            "divergent bookmark",
        )
        .unwrap();
    }
    h.jj.run(
        &h.store,
        &["log", "-r", "root()", "--ignore-working-copy", "--no-graph"],
        "merge the concurrent operations",
    )
    .unwrap();
    let refusal = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err();
    assert!(
        matches!(
            &refusal,
            PreparationRefusal::Busy(BusyReason::AcceptanceDebt { .. })
        ),
        "{refusal}"
    );
    assert!(
        matches!(
            cairn_vcs::resolve_coordinate(&h.store, BRANCH).await,
            Err(cairn_vcs::CoordinateResolutionError::Conflicted { .. })
        ),
        "the owed branch's name is left as it was"
    );
    h.set_bookmark(BRANCH, &tip);

    // Even back at the candidate head, an unknown row is no evidence.
    h.set_bookmark(BRANCH, &prepared.head_after);
    let refusal = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err();
    assert!(
        matches!(&refusal, PreparationRefusal::Busy(BusyReason::AcceptanceDebt { status, .. }) if status == "unknown"),
        "{refusal}"
    );
    assert_eq!(acceptance_rows(&h).await[0].0, "unknown");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn dropped_commit_claims_carry_across_local_advances_until_published() {
    let h = harness_or_skip!("prep_acceptance_chain");
    // The branch's commit becomes empty once the base makes the same change.
    let own = h.commit(&h.branch_ws, BRANCH, "shared.txt", "same\n");
    h.advance_main("shared.txt", "same\n");
    let first = h.prepare(PreparationPurpose::Check).await.unwrap();
    assert!(first.advanced);
    let rows = acceptance_rows(&h).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "accepted");
    assert!(
        rows[0].2.contains(&own),
        "the dropped commit is claimed: {rows:?}"
    );

    // A second local advance before any push carries the claim.
    h.advance_main("incoming.txt", "later\n");
    let second = h.prepare(PreparationPurpose::Check).await.unwrap();
    assert!(second.advanced);
    let rows = acceptance_rows(&h).await;
    assert_eq!(rows.len(), 2);
    assert!(
        rows[1].3.contains(&own),
        "the second row inherits the first's drop: {rows:?}"
    );
    assert_eq!(
        accepted_drop_claims(&h.db, "proj-1", &h.store, BRANCH)
            .await
            .unwrap()
            .len(),
        1,
        "only the newest accepted row's claims are read; it carries the chain"
    );
    let claims = accepted_drop_claims(&h.db, "proj-1", &h.store, BRANCH)
        .await
        .unwrap();
    assert!(claims
        .iter()
        .any(|(head, drops)| *head == second.head_after && drops.contains(&own)));
    let dotted = h.store.join("..").join(h.store.file_name().unwrap());
    assert_eq!(
        accepted_drop_claims(&h.db, "proj-1", &dotted, BRANCH)
            .await
            .unwrap(),
        claims,
        "claims are read under the same canonical identity"
    );

    // A confirmed publication at the current head resets the claims: the next
    // install descends from it and inherits nothing, though its own drops
    // are still recorded.
    let reservation = h
        .orch
        .reserve_branches(
            &h.store,
            &[BRANCH, "main", "agent/unowned"],
            "test publication",
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let guard = h
        .orch
        .acquire_jj_store_lock(&h.store, StoreLockClass::Foreground, "test publication")
        .await;
    let main_head = h.tip("main");
    assert!(
        record_publication_checkpoint(
            &h.db,
            &guard,
            &reservation,
            "proj-1",
            &h.store,
            "agent/unowned",
            &main_head
        )
        .await
        .is_err(),
        "a branch no live job owns records no checkpoint"
    );
    assert!(
        record_publication_checkpoint(
            &h.db,
            &guard,
            &reservation,
            "proj-1",
            &h.store,
            "main",
            &main_head
        )
        .await
        .is_err(),
        "the project default never records a checkpoint"
    );
    assert!(
        record_publication_checkpoint(
            &h.db,
            &guard,
            &reservation,
            "proj-1",
            &h.store,
            BRANCH,
            &own
        )
        .await
        .is_err(),
        "a checkpoint names the exact literal head"
    );
    for _ in 0..2 {
        record_publication_checkpoint(
            &h.db,
            &guard,
            &reservation,
            "proj-1",
            &h.store,
            BRANCH,
            &second.head_after,
        )
        .await
        .unwrap();
    }
    drop(guard);
    drop(reservation);
    // A new commit of the branch's own after the checkpoint, absorbed by the
    // base: the install records that drop, and inherits none of the claims
    // the checkpoint published.
    let later_own = h.commit(&h.branch_ws, BRANCH, "shared2.txt", "same2\n");
    h.advance_main("shared2.txt", "same2\n");
    let third = h.prepare(PreparationPurpose::Check).await.unwrap();
    assert!(third.advanced);
    let rows = acceptance_rows(&h).await;
    assert_eq!(rows.len(), 3);
    assert!(
        rows[2].2.contains(&later_own),
        "the post-checkpoint drop is claimed: {rows:?}"
    );
    assert!(
        rows[2].3.is_empty(),
        "the reset row inherits nothing: {rows:?}"
    );

    // Rewound behind the checkpoint: the head no longer descends from what
    // was published, so the reset does not apply and the earlier claim is
    // inherited again rather than lost.
    h.set_bookmark(BRANCH, &first.head_after);
    h.advance_main("incoming.txt", "after the rewind\n");
    let rewound = h.prepare(PreparationPurpose::Check).await.unwrap();
    assert!(rewound.advanced);
    let rows = acceptance_rows(&h).await;
    assert_eq!(rows.len(), 4);
    assert!(
        rows[3].3.contains(&own),
        "a head behind the checkpoint keeps its unpublished claims: {rows:?}"
    );

    // The journal is the only evidence: removing it removes the claims.
    h.db.execute("DELETE FROM branch_advance_acceptances", ())
        .await
        .unwrap();
    assert!(accepted_drop_claims(&h.db, "proj-1", &h.store, BRANCH)
        .await
        .unwrap()
        .is_empty());
}

/// Append `count` empty commits to `from` with `git fast-import`, as
/// `refs/heads/main-long`; returns the new tip.
fn append_empty_commits(repo: &Path, from: &str, count: usize) -> String {
    use std::io::Write;
    let mut stream = String::new();
    for number in 0..count {
        let parent = if number == 0 {
            from.to_string()
        } else {
            format!(":{number}")
        };
        let (mark, time) = (number + 1, 1_700_000_000 + number);
        stream.push_str(&format!(
            "commit refs/heads/main-long\nmark :{mark}\ncommitter Cairn tests <tests@cairn.local> {time} +0000\ndata 0\nfrom {parent}\n\n"
        ));
    }
    let mut child = crate::env::git()
        .args(["fast-import", "--quiet"])
        .current_dir(repo)
        .stdin(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stream.as_bytes())
        .unwrap();
    assert!(child.wait().unwrap().success(), "git fast-import");
    git(repo, &["rev-parse", "refs/heads/main-long"])
}

/// A parent lands as a native squash; its child carries the parent's whole
/// prefix, a foreign commit merged and then reverted included. Preparing the
/// child records that whole prefix as removed from it by the install, so the
/// child's old origin copy is proven by the journal claim even when main has
/// grown past the squash beyond the receipt walk's reach, and an outside
/// amendment the squash never recorded is still refused.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_child_of_a_squash_landed_parent_claims_its_whole_represented_prefix() {
    squash_landed_child("prep_squash_prefix", false).await;
}

/// The same, with the child's own tip an unmapped merge of its own work and
/// the receipted foreign commit, carrying an authored change of its own: the
/// merge is replayed with that change, the parent's revert still holds (the
/// merge brings no foreign effect back), and the foreign commit is claimed
/// with the rest of the represented prefix.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_child_merging_a_receipted_foreign_commit_keeps_its_own_change_and_claims_it() {
    squash_landed_child("prep_squash_prefix_merge", true).await;
}

async fn squash_landed_child(name: &str, own_merges_foreign: bool) {
    const PARENT: &str = "agent/parent";
    const FOREIGN: &str = "agent/foreign";
    let Some(bin) = crate::jj::tests::jj_bin() else {
        return;
    };
    let h = harness(&bin, name).await;
    let main_before = h.tip("main");

    // A foreign commit no live job owns, merged into the parent and then
    // reverted there, after the parent's own commit.
    let foreign_ws = h.workspaces.path().join("foreign");
    crate::jj::add_workspace(&h.jj, &h.store, &foreign_ws, FOREIGN, "main", None).unwrap();
    let foreign = h.commit(&foreign_ws, FOREIGN, "foreign.txt", "foreign effect\n");
    let parent_ws = h.workspaces.path().join("parent");
    crate::jj::add_workspace(&h.jj, &h.store, &parent_ws, PARENT, "main", None).unwrap();
    let own_parent = h.commit(&parent_ws, PARENT, "parent.txt", "parent work\n");
    h.jj.run(
        &h.store,
        &[
            "new",
            &own_parent,
            &foreign,
            "--no-edit",
            "-m",
            "merge foreign",
            "--ignore-working-copy",
        ],
        "merge the foreign commit",
    )
    .unwrap();
    let merge =
        h.jj.run(
            &h.store,
            &[
                "log",
                "-r",
                &format!("children({own_parent}) & children({foreign})"),
                "-T",
                "commit_id",
                "--no-graph",
                "--ignore-working-copy",
            ],
            "find the merge",
        )
        .unwrap()
        .trim()
        .to_string();
    h.set_bookmark(PARENT, &merge);
    let _ = crate::jj::update_stale(&h.jj, &parent_ws);
    h.jj.run(&parent_ws, &["new", PARENT], "start on the merge")
        .unwrap();
    std::fs::remove_file(parent_ws.join("foreign.txt")).unwrap();
    crate::jj::seal(&h.jj, &parent_ws, "revert foreign effect", None).unwrap();
    let revert = h.tip(PARENT);

    // The child: cut from the parent's tip, plus its own commit; its origin
    // copy is that head.
    h.set_bookmark(BRANCH, &revert);
    let mut own = h.commit(&h.branch_ws, BRANCH, "child.txt", "child work\n");
    if own_merges_foreign {
        // The child's own merge of the foreign commit, with an authored
        // change: F is already in its history, so the merge's tree is the
        // child's plus that change, and the revert still holds.
        let _ = crate::jj::update_stale(&h.jj, &h.branch_ws);
        h.jj.run(
            &h.branch_ws,
            &["new", &own, &foreign],
            "start the own merge",
        )
        .unwrap();
        std::fs::write(h.branch_ws.join("merge.txt"), "merge work\n").unwrap();
        crate::jj::seal(&h.jj, &h.branch_ws, "own merge of foreign", None).unwrap();
        own = h.tip(BRANCH);
        assert_eq!(
            git(&h.repo, &["rev-list", "--parents", "-n", "1", &own])
                .split_whitespace()
                .count(),
            3,
            "the child's tip is a two-parent merge"
        );
    }
    h.db.execute_script(format!(
        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
           VALUES ('job-parent', 'exec-1', 'parent', 'issue-1', 'proj-1', 'running', '{PARENT}', 'main', 2, 2);
         UPDATE jobs SET base_branch = '{PARENT}' WHERE id = '{JOB}';"
    ))
    .await
    .unwrap();
    let origin = tempfile::tempdir().unwrap();
    git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
    git(
        &h.repo,
        &["remote", "add", "origin", &origin.path().to_string_lossy()],
    );
    git(
        &h.repo,
        &[
            "push",
            "-q",
            "origin",
            &format!("{own}:refs/heads/{BRANCH}"),
        ],
    );
    h.jj.run(
        &h.store,
        &[
            "git",
            "fetch",
            "--remote",
            "origin",
            "--ignore-working-copy",
        ],
        "record origin's copy",
    )
    .unwrap();
    crate::jj::track_bookmark(&h.jj, &h.store, BRANCH).unwrap();
    let prefix: std::collections::BTreeSet<String> =
        git(&h.repo, &["rev-list", &revert, &format!("^{main_before}")])
            .lines()
            .map(str::to_string)
            .collect();
    assert_eq!(
        prefix,
        [
            own_parent.clone(),
            foreign.clone(),
            merge.clone(),
            revert.clone()
        ]
        .into_iter()
        .collect(),
        "the child's inherited prefix"
    );

    // The parent lands as a native squash; nothing of the child moves.
    let landed = crate::jj::squash_branch_onto(
        &h.jj,
        &h.store,
        PARENT,
        &revert,
        &main_before,
        "land the parent",
    )
    .unwrap();
    h.set_bookmark("main", &landed.squashed_commit);
    assert_eq!(
        h.tip(BRANCH),
        own,
        "the child is untouched before it is prepared"
    );
    assert_eq!(h.tip(FOREIGN), foreign);

    // Main grows past the squash further than the receipt walk inspects.
    let long = append_empty_commits(&h.repo, &landed.squashed_commit, 4100);
    crate::jj::import_git(&h.jj, &h.store).unwrap();
    h.set_bookmark("main", &long);
    h.db.execute("UPDATE jobs SET base_branch = 'main' WHERE id = ?1", (JOB,))
        .await
        .unwrap();

    // Name every commit, so a refusal's commit id can be read against the
    // graph it was computed on.
    eprintln!(
        "squash-prefix fixture: own_parent={own_parent} foreign={foreign} merge={merge} \
         revert={revert} own={own} main_before={main_before} landed={} long={long}\n{}\nlanded \
         evolution:\n{}",
        landed.squashed_commit,
        git(
            &h.repo,
            &[
                "log",
                "--format=%H parents=%P %s",
                &own,
                &format!("^{main_before}")
            ]
        ),
        h.jj.run(
            &h.store,
            &[
                "evolog",
                "-r",
                &landed.squashed_commit,
                "--no-graph",
                "-T",
                "commit_id ++ \"\\n\"",
                "--ignore-working-copy",
            ],
            "show the squash's predecessors",
        )
        .unwrap_or_else(|error| format!("(evolog failed: {error})")),
    );
    let prepared = match h.prepare(PreparationPurpose::OwnerCommit).await {
        Ok(prepared) => prepared,
        Err(refusal) => panic!("the child prepares onto the long base: {refusal}"),
    };
    assert!(prepared.advanced);
    assert_eq!(prepared.base_commit, long);
    let head = prepared.head_after.clone();
    assert_eq!(
        git(&h.repo, &["show", &format!("{head}:child.txt")]),
        "child work"
    );
    assert_eq!(
        git(&h.repo, &["show", &format!("{head}:parent.txt")]),
        "parent work"
    );
    if own_merges_foreign {
        assert_eq!(
            git(&h.repo, &["show", &format!("{head}:merge.txt")]),
            "merge work",
            "the own merge's authored change is replayed"
        );
    }
    assert!(
        crate::env::git()
            .args(["cat-file", "-e", &format!("{head}:foreign.txt")])
            .current_dir(&h.repo)
            .status()
            .unwrap()
            .code()
            != Some(0),
        "the reverted foreign effect is not resurrected"
    );
    assert_eq!(h.tip(FOREIGN), foreign, "no other branch moves");
    assert_eq!(h.tip(PARENT), landed.squashed_commit);
    let rows = acceptance_rows(&h).await;
    let row = rows.last().unwrap();
    assert_eq!(row.0, "accepted");
    assert_eq!(
        row.2
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        prefix,
        "the whole represented prefix is removed from the child by its install: {rows:?}"
    );
    assert!(row.3.is_empty(), "nothing inherited: {rows:?}");

    // The old origin copy is proven by the claim alone: the receipt walk
    // cannot reach the squash past main's growth.
    let claims: Vec<crate::jj::AcceptedAdvanceDrops> =
        accepted_drop_claims(&h.db, "proj-1", &h.store, BRANCH)
            .await
            .unwrap()
            .into_iter()
            .map(
                |(head_after, dropped_commit_ids)| crate::jj::AcceptedAdvanceDrops {
                    head_after,
                    dropped_commit_ids,
                },
            )
            .collect();
    let prove = |remote: String, claims: Vec<crate::jj::AcceptedAdvanceDrops>| {
        crate::jj::origin_copy_is_superseded_with_accepted(
            &h.jj,
            &h.store,
            BRANCH,
            &head,
            &remote,
            &claims,
            Duration::from_secs(120),
        )
    };
    assert!(
        !matches!(prove(own.clone(), Vec::new()), Ok(true)),
        "without the claim the walk does not reach the squash"
    );
    assert!(
        matches!(prove(own.clone(), claims.clone()), Ok(true)),
        "the claim proves the old origin copy"
    );

    // An outside amendment of the foreign commit, never in the receipt, is
    // unaccounted for and refused by name.
    let amended = git(
        &h.repo,
        &[
            "commit-tree",
            &format!("{foreign}^{{tree}}"),
            "-p",
            &format!("{foreign}^"),
            "-m",
            "amended foreign outside Cairn",
        ],
    );
    git(
        &h.repo,
        &[
            "push",
            "-q",
            "-f",
            "origin",
            &format!("{amended}:refs/heads/{BRANCH}"),
        ],
    );
    h.jj.run(
        &h.store,
        &[
            "git",
            "fetch",
            "--remote",
            "origin",
            "--ignore-working-copy",
        ],
        "record origin's outside copy",
    )
    .unwrap();
    // Fetching merged origin's move into the local name; put the local
    // branch back where the install left it.
    h.set_bookmark(BRANCH, &head);
    match prove(amended.clone(), claims) {
        Err(crate::jj::AdvanceComputeError::UnaccountedRemoteCommits { commits }) => {
            assert!(commits.contains(&amended), "{commits:?}")
        }
        other => panic!("the outside amendment must be refused by name: {other:?}"),
    }
}

/// A preparation pinned to a head the branch has moved past refuses under the
/// snapshot guard before anything is settled, repaired, recorded or sent.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_stale_expected_head_refuses_before_anything_moves() {
    let h = harness_or_skip!("prep_expected_head");
    let first = h.commit(&h.branch_ws, BRANCH, "own.txt", "one\n");
    let now = h.commit(&h.branch_ws, BRANCH, "own.txt", "two\n");
    h.advance_main("incoming.txt", "theirs\n");
    let operation = crate::jj::operation_id(&h.jj, &h.store).unwrap();
    let mut request = PreparationRequest::new(
        "proj-1",
        BRANCH,
        Some(JOB.to_string()),
        PreparationPurpose::OwnerCommit,
    );
    request.expected_head = Some(first.clone());
    let refusal = prepare_branch(&h.orch, request).await.unwrap_err();
    assert!(
        matches!(&refusal, PreparationRefusal::Moved { what } if what.contains(&now) && what.contains(&first)),
        "{refusal}"
    );
    assert_eq!(h.tip(BRANCH), now);
    assert_eq!(
        crate::jj::operation_id(&h.jj, &h.store).unwrap(),
        operation,
        "the store did not change"
    );
    assert!(
        acceptance_rows(&h).await.is_empty(),
        "nothing was journaled"
    );
    assert!(h.item_outcomes().await.is_empty());
    assert!(h.wakes().await.is_empty());
    assert!(h.messages().await.is_empty());

    // Pinned to the head it is actually at, the same request prepares.
    let mut request = PreparationRequest::new(
        "proj-1",
        BRANCH,
        Some(JOB.to_string()),
        PreparationPurpose::OwnerCommit,
    );
    request.expected_head = Some(now.clone());
    let prepared = prepare_branch(&h.orch, request).await.unwrap();
    assert_eq!(prepared.head_before, now);
}

/// A replay whose own removals would push the row past the claim cap, though
/// its inheritance alone fits, is refused before the install: no row, no move.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_replay_past_the_claim_cap_is_refused_before_it_installs() {
    let h = harness_or_skip!("prep_claim_cap");
    let anchor = h.tip("main");
    // An accepted install at the branch's fork point that already carries the
    // whole cap of claims; the next install inherits all of them.
    let claims: Vec<String> = (0..INHERITED_DROP_CAP)
        .map(|number| format!("{number:040x}"))
        .collect();
    h.db.execute(
        "INSERT INTO branch_advance_acceptances
           (id, project_id, store_path, branch, head_before, head_after, base_branch,
            base_commit, captured_operation, accepted_operation, dropped_commits_json,
            inherited_drops_json, status, created_at, export_state)
         VALUES ('acceptance-full', 'proj-1', ?1, ?2, ?3, ?3, 'main', ?3, 'op', 'op', ?4,
                 '[]', 'accepted', 1, 'superseded')",
        cairn_db::turso::params![
            journal_store_key(&h.store).unwrap(),
            BRANCH,
            anchor.as_str(),
            serde_json::to_string(&claims).unwrap()
        ],
    )
    .await
    .unwrap();
    // The branch's own commit becomes empty on the base: one more removal.
    let own = h.commit(&h.branch_ws, BRANCH, "shared.txt", "same\n");
    h.advance_main("shared.txt", "same\n");
    let operation = crate::jj::operation_id(&h.jj, &h.store).unwrap();

    let refusal = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err();
    assert_eq!(
        crate::jj::operation_id(&h.jj, &h.store).unwrap(),
        operation,
        "the store did not change"
    );
    assert!(
        matches!(&refusal, PreparationRefusal::Repository(why)
            if why.contains("removed-from-branch claims")
                && why.contains(&(INHERITED_DROP_CAP + 1).to_string())),
        "{refusal}"
    );
    assert_eq!(h.tip(BRANCH), own, "nothing was installed");
    assert_eq!(git(&h.repo, &["rev-parse", BRANCH]), own);
    let rows = acceptance_rows(&h).await;
    assert_eq!(rows.len(), 1, "no row was journaled: {rows:?}");
}

fn soon() -> std::time::Instant {
    std::time::Instant::now() + Duration::from_secs(10)
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn nested_declared_dependents_are_evidence_only_and_never_move() {
    let h = harness_or_skip!("prep_dependents");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    // A child built on the branch, and a grandchild built on the child.
    let child_ws = h.workspaces.path().join("child");
    crate::jj::add_workspace(&h.jj, &h.store, &child_ws, "agent/child", BRANCH, None).unwrap();
    let child = h.commit(&child_ws, "agent/child", "child.txt", "child\n");
    let grand_ws = h.workspaces.path().join("grand");
    crate::jj::add_workspace(
        &h.jj,
        &h.store,
        &grand_ws,
        "agent/grand",
        "agent/child",
        None,
    )
    .unwrap();
    let grand = h.commit(&grand_ws, "agent/grand", "grand.txt", "grand\n");
    h.db.execute_script(format!(
        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
           VALUES ('job-child', 'exec-1', 'child', 'issue-1', 'proj-1', 'running', 'agent/child', '{BRANCH}', 5, 5);
         INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
           VALUES ('job-grand', 'exec-1', 'grand', 'issue-1', 'proj-1', 'running', 'agent/grand', 'agent/child', 6, 6);
         INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
           VALUES ('job-done', 'exec-1', 'done', 'issue-1', 'proj-1', 'complete', 'agent/done', '{BRANCH}', 7, 7);"
    ))
    .await
    .unwrap();
    assert_eq!(
        dependent_branches(&h.db, "proj-1", BRANCH, soon())
            .await
            .unwrap(),
        vec![
            "agent/child".to_string(),
            "agent/done".to_string(),
            "agent/grand".to_string()
        ],
        "transitive, live only, never the branch itself"
    );
    assert!(
        dependent_branches(&h.db, "proj-1", BRANCH, std::time::Instant::now())
            .await
            .is_err(),
        "an exhausted lookup budget fails closed"
    );
    assert!(dependent_branches(&h.db, "proj-1", "agent/grand", soon())
        .await
        .unwrap()
        .is_empty());

    h.advance_main("incoming.txt", "theirs\n");
    let prepared = h.prepare(PreparationPurpose::Check).await.unwrap();
    assert!(prepared.advanced);
    assert_eq!(h.tip("agent/child"), child, "a dependent is never replayed");
    assert_eq!(h.tip("agent/grand"), grand);
    let notes = h.messages().await;
    assert_eq!(
        notes.len(),
        1,
        "only the prepared branch's owner is noted: {notes:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_branchless_job_gets_its_exact_target_and_revalidates_its_own_advance() {
    let h = harness_or_skip!("prep_direct");
    h.db.execute_script(
        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
           VALUES ('job-direct', 'exec-1', 'direct', 'issue-1', 'proj-1', 'running', NULL, NULL, 3, 3);
         INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
           VALUES ('job-blank', 'exec-1', 'blank', 'issue-1', 'proj-1', 'running', '   ', NULL, 4, 4);",
    )
    .await
    .unwrap();
    let head = h.tip("main");
    let op = crate::jj::operation_id(&h.jj, &h.store).unwrap();
    let prepared = prepare_branch(
        &h.orch,
        PreparationRequest::new(
            "proj-1",
            "main",
            Some("job-direct".into()),
            PreparationPurpose::OwnerCommit,
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        (prepared.head_after.as_str(), prepared.base_commit.as_str()),
        (head.as_str(), head.as_str())
    );
    assert!(!prepared.advanced);
    assert_eq!(
        crate::jj::operation_id(&h.jj, &h.store).unwrap(),
        op,
        "no replay onto itself"
    );

    // A whitespace-only branch is branchless too, not unmanaged.
    let blank = prepare_branch(
        &h.orch,
        PreparationRequest::new(
            "proj-1",
            "main",
            Some("job-blank".into()),
            PreparationPurpose::OwnerCommit,
        ),
    )
    .await
    .unwrap();
    assert_eq!(blank.head_after, head);
    assert!(blank.relationship.is_direct_target("main"));

    // The writer's own publication advances the target, which is its base.
    let next = h.advance_main("own-direct.txt", "direct\n");
    let guard = h
        .orch
        .acquire_jj_store_lock(&h.store, StoreLockClass::Foreground, "test publication")
        .await;
    prepared
        .revalidate_for_publication(&h.db, &guard, &next)
        .await
        .unwrap();
    assert!(
        prepared.revalidate(&h.db, &guard).await.is_err(),
        "the captured head is stale"
    );
    drop(guard);

    let refusal = prepare_branch(
        &h.orch,
        PreparationRequest::new(
            "proj-1",
            BRANCH,
            Some("job-direct".into()),
            PreparationPurpose::OwnerCommit,
        ),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(refusal, PreparationRefusal::Unmanaged { .. }),
        "only its own target: {refusal}"
    );
}

fn write_probe(
    h: &Harness,
    tool_use_id: &str,
) -> crate::mcp::handlers::write::replay::WriteReplayProbe {
    crate::mcp::handlers::write::replay::WriteReplayProbe::new(
        h.db.clone(),
        crate::mcp::handlers::write::replay::WriteCall::for_test(RUN, tool_use_id),
    )
}

async fn publish_write(db: &LocalDb, tool_use_id: &str) {
    db.execute(
        "INSERT INTO write_replay_ledger(run_id, tool_use_id, report, created_at)
         VALUES (?1, ?2, '{\"applied\":[]}', 1)",
        (RUN, tool_use_id.to_string()),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_already_published_write_is_refused_under_the_snapshot_guard_before_any_mutation() {
    let h = harness_or_skip!("prep_probe_snapshot");
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    h.advance_main("incoming.txt", "theirs\n");
    publish_write(&h.db, "toolu-published").await;
    let op = crate::jj::operation_id(&h.jj, &h.store).unwrap();

    let mut request = PreparationRequest::new(
        "proj-1",
        BRANCH,
        Some(JOB.to_string()),
        PreparationPurpose::OwnerCommit,
    );
    request.write_replay = Some(write_probe(&h, "toolu-published"));
    let refusal = prepare_branch(&h.orch, request).await.unwrap_err();
    assert!(
        matches!(refusal, PreparationRefusal::AlreadyPublished),
        "{refusal}"
    );
    assert_eq!(h.tip(BRANCH), own);
    assert_eq!(
        crate::jj::operation_id(&h.jj, &h.store).unwrap(),
        op,
        "no repair, export or install"
    );
    assert!(h.messages().await.is_empty());
    assert!(h.item_outcomes().await.is_empty());

    // An unpublished write prepares normally.
    let mut request = PreparationRequest::new(
        "proj-1",
        BRANCH,
        Some(JOB.to_string()),
        PreparationPurpose::OwnerCommit,
    );
    request.write_replay = Some(write_probe(&h, "toolu-fresh"));
    assert!(prepare_branch(&h.orch, request).await.unwrap().advanced);
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_write_published_during_computation_is_refused_under_the_install_guard() {
    let h = harness_or_skip!("prep_probe_install");
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    h.advance_main("incoming.txt", "theirs\n");
    let db = h.db.clone();
    h.set_barrier(move |_| {
        let db = db.clone();
        std::thread::spawn(move || {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(publish_write(&db, "toolu-racing"))
        })
        .join()
        .unwrap();
    });

    let mut request = PreparationRequest::new(
        "proj-1",
        BRANCH,
        Some(JOB.to_string()),
        PreparationPurpose::OwnerCommit,
    );
    request.write_replay = Some(write_probe(&h, "toolu-racing"));
    let refusal = prepare_branch(&h.orch, request).await.unwrap_err();
    assert!(
        matches!(refusal, PreparationRefusal::AlreadyPublished),
        "{refusal}"
    );
    assert_eq!(
        h.tip(BRANCH),
        own,
        "the computed candidate was never installed"
    );
    assert!(h.messages().await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_check_never_waits_for_a_reserved_branch_and_an_owner_commit_uses_an_inherited_one() {
    let h = harness_or_skip!("prep_reservation");
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    h.advance_main("incoming.txt", "theirs\n");
    let held = Arc::new(
        h.orch
            .reserve_branches(
                &h.store,
                &[BRANCH],
                "test publication",
                Duration::from_secs(5),
            )
            .await
            .unwrap(),
    );

    let started = std::time::Instant::now();
    let refusal = h.prepare(PreparationPurpose::Check).await.unwrap_err();
    assert!(
        matches!(&refusal, PreparationRefusal::Busy(BusyReason::Reservation(holder)) if holder.contains("test publication")),
        "{refusal}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "a check tries once"
    );
    assert_eq!(h.tip(BRANCH), own);
    assert!(h.messages().await.is_empty(), "busy wakes nobody");

    // The holder passes its reservation down; preparation never re-acquires.
    let mut request = PreparationRequest::new(
        "proj-1",
        BRANCH,
        Some(JOB.to_string()),
        PreparationPurpose::OwnerCommit,
    );
    request.reservation = Some(held.clone());
    let prepared = prepare_branch(&h.orch, request).await.unwrap();
    assert!(prepared.advanced);

    // A reservation of another branch does not cover this one.
    let other = Arc::new(
        h.orch
            .reserve_branches(&h.store, &[MAIN_LINE], "other", Duration::from_secs(5))
            .await
            .unwrap(),
    );
    let mut request = PreparationRequest::new(
        "proj-1",
        BRANCH,
        Some(JOB.to_string()),
        PreparationPurpose::OwnerCommit,
    );
    request.reservation = Some(other);
    assert!(matches!(
        prepare_branch(&h.orch, request).await.unwrap_err(),
        PreparationRefusal::Repository(_)
    ));
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_owner_commit_during_computation_wins_and_is_carried_on_retry() {
    let h = harness_or_skip!("prep_owner_race");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "one\n");
    let base = h.advance_main("incoming.txt", "theirs\n");
    let (jj, ws, store) = (h.jj.clone(), h.branch_ws.clone(), h.store.clone());
    let raced = Arc::new(Mutex::new(None));
    let record = raced.clone();
    h.set_barrier(move |attempt| {
        if attempt == 1 {
            let _ = crate::jj::update_stale(&jj, &ws);
            std::fs::write(ws.join("own.txt"), "two\n").unwrap();
            crate::jj::seal(&jj, &ws, "concurrent owner commit", None).unwrap();
            *record.lock().unwrap() = crate::jj::bookmark_commit(&jj, &store, BRANCH);
        }
    });

    let prepared = h.prepare(PreparationPurpose::Check).await.unwrap();
    let owner_commit = raced.lock().unwrap().clone().unwrap();
    assert_eq!(prepared.attempts, 2, "the stale candidate was discarded");
    assert_eq!(prepared.head_before, owner_commit);
    assert!(h.is_ancestor(&base, &prepared.head_after));
    assert_eq!(
        git(
            &h.repo,
            &["show", &format!("{}:own.txt", prepared.head_after)]
        ),
        "two",
        "the intervening commit is never overwritten"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_base_move_during_computation_retries_onto_the_new_base() {
    let h = harness_or_skip!("prep_base_race");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    h.advance_main("incoming.txt", "first\n");
    let (jj, ws, store) = (h.jj.clone(), h.main_ws.clone(), h.store.clone());
    let moved = Arc::new(Mutex::new(None));
    let record = moved.clone();
    h.set_barrier(move |attempt| {
        if attempt == 1 {
            let _ = crate::jj::update_stale(&jj, &ws);
            std::fs::write(ws.join("incoming.txt"), "second\n").unwrap();
            crate::jj::seal(&jj, &ws, "second base", None).unwrap();
            let tip = crate::jj::bookmark_commit(&jj, &store, MAIN_LINE).unwrap();
            jj.run(
                &store,
                &[
                    "bookmark",
                    "set",
                    "main",
                    "-r",
                    &tip,
                    "--ignore-working-copy",
                ],
                "race main",
            )
            .unwrap();
            *record.lock().unwrap() = Some(tip);
        }
    });

    let prepared = h.prepare(PreparationPurpose::Check).await.unwrap();
    let second = moved.lock().unwrap().clone().unwrap();
    assert_eq!(prepared.attempts, 2);
    assert_eq!(prepared.base_commit, second, "never a mixed coordinate");
    assert!(h.is_ancestor(&second, &prepared.head_after));
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_relationship_move_during_computation_retries_against_the_new_declaration() {
    let h = harness_or_skip!("prep_relationship_race");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    h.advance_main("incoming.txt", "main side\n");
    // An integration branch cut from the original base with its own change.
    let integration_ws = h.workspaces.path().join("integration");
    crate::jj::add_workspace(
        &h.jj,
        &h.store,
        &integration_ws,
        "agent/parent",
        "main",
        None,
    )
    .unwrap();
    let parent_tip = h.commit(&integration_ws, "agent/parent", "parent.txt", "parent\n");
    let db = h.db.clone();
    h.set_barrier(move |attempt| {
        if attempt == 1 {
            let db = db.clone();
            std::thread::spawn(move || {
                tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(async move {
                        db.execute(
                            "UPDATE jobs SET base_branch = 'agent/parent' WHERE id = ?1",
                            (JOB,),
                        )
                        .await
                        .unwrap();
                    })
            })
            .join()
            .unwrap();
        }
    });

    let prepared = h.prepare(PreparationPurpose::Check).await.unwrap();
    assert_eq!(prepared.attempts, 2);
    assert_eq!(
        prepared.relationship.declared.as_deref(),
        Some("agent/parent")
    );
    assert_eq!(prepared.relationship.base_branch, "agent/parent");
    assert_eq!(prepared.base_commit, parent_tip);
    assert!(h.is_ancestor(&parent_tip, &prepared.head_after));
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_owner_that_keeps_moving_exhausts_bounded_retries_without_losing_a_commit() {
    let h = harness_or_skip!("prep_exhausted");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "0\n");
    h.advance_main("incoming.txt", "theirs\n");
    let (jj, ws) = (h.jj.clone(), h.branch_ws.clone());
    h.set_barrier(move |attempt| {
        let _ = crate::jj::update_stale(&jj, &ws);
        std::fs::write(ws.join("own.txt"), format!("{attempt}\n")).unwrap();
        crate::jj::seal(&jj, &ws, "owner keeps committing", None).unwrap();
    });

    let refusal = h.prepare(PreparationPurpose::Check).await.unwrap_err();
    assert!(
        matches!(refusal, PreparationRefusal::RetriesExhausted { attempts, .. } if attempts == PREPARATION_ATTEMPTS),
        "{refusal}"
    );
    let tip = h.tip(BRANCH);
    assert_eq!(
        git(&h.repo, &["show", &format!("{tip}:own.txt")]),
        PREPARATION_ATTEMPTS.to_string(),
        "the owner's last commit is the branch"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_legacy_pending_intent_at_the_coordinate_refuses_rather_than_being_reused() {
    let h = harness_or_skip!("prep_legacy");
    h.commit(&h.branch_ws, BRANCH, "shared.txt", "mine\n");
    let base = h.advance_main("shared.txt", "theirs\n");
    h.db.execute(
        "INSERT INTO jj_reconcile_intents
           (id, project_id, store_path, target_branch, destination_commit, status, created_at, updated_at)
         VALUES ('legacy-1', 'proj-1', ?1, 'main', ?2, 'pending', 1, 1)",
        (h.store.to_string_lossy().into_owned(), base.clone()),
    )
    .await
    .unwrap();

    let refusal = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err();
    assert!(
        matches!(&refusal, PreparationRefusal::Busy(BusyReason::LegacyIntent { intent_id, status })
            if intent_id == "legacy-1" && status == "pending"),
        "{refusal}"
    );
    let status =
        h.db.query_text(
            "SELECT status FROM jj_reconcile_intents WHERE id = 'legacy-1'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        status.as_deref(),
        Some("pending"),
        "accepted legacy work is untouched"
    );
    assert!(h.item_outcomes().await.is_empty());
    assert!(h.messages().await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_conflicted_own_branch_name_is_repaired_by_an_owner_but_not_by_a_check_or_read() {
    let h = harness_or_skip!("prep_conflicted_name");
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    h.advance_main("incoming.txt", "theirs\n");
    // Both divergent targets must differ from the current head, or the side
    // that keeps it changes nothing and jj merges the operations trivially.
    // Repair keeps the target containing the other: `own`.
    h.commit(&h.branch_ws, BRANCH, "extra.txt", "extra\n");
    // Two operations from the same parent set the bookmark differently; the
    // next ordinary load merges them into a conflicted name.
    let op = crate::jj::operation_id(&h.jj, &h.store).unwrap();
    let parent = git(&h.repo, &["rev-parse", &format!("{own}^")]);
    for target in [own.as_str(), parent.as_str()] {
        h.jj.run(
            &h.store,
            &[
                "--at-op",
                &op,
                "bookmark",
                "set",
                BRANCH,
                "-r",
                target,
                "--allow-backwards",
                "--ignore-working-copy",
            ],
            "divergent bookmark",
        )
        .unwrap();
    }
    h.jj.run(
        &h.store,
        &["log", "-r", "root()", "--ignore-working-copy", "--no-graph"],
        "merge the concurrent operations",
    )
    .unwrap();
    let read = cairn_vcs::resolve_coordinate(&h.store, BRANCH).await;
    assert!(
        matches!(
            read,
            Err(cairn_vcs::CoordinateResolutionError::Conflicted { .. })
        ),
        "fixture must hold a conflicted name: {read:?}"
    );
    let again = cairn_vcs::resolve_coordinate(&h.store, BRANCH).await;
    assert!(
        matches!(
            again,
            Err(cairn_vcs::CoordinateResolutionError::Conflicted { .. })
        ),
        "a pure read never repairs"
    );

    let refusal = h.prepare(PreparationPurpose::Check).await.unwrap_err();
    assert!(
        matches!(refusal, PreparationRefusal::Unresolved { .. }),
        "{refusal}"
    );
    assert!(
        matches!(
            cairn_vcs::resolve_coordinate(&h.store, BRANCH).await,
            Err(cairn_vcs::CoordinateResolutionError::Conflicted { .. })
        ),
        "a check never chooses the branch's target"
    );
    assert!(h.messages().await.is_empty());

    let prepared = h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    assert_eq!(h.tip(BRANCH), prepared.head_after);
    assert!(h.is_ancestor(&prepared.base_commit, &prepared.head_after));
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_conflicting_check_never_chooses_a_settled_conflicted_name() {
    let h = harness_or_skip!("prep_split_conflict");
    let own = h.commit(&h.branch_ws, BRANCH, "shared.txt", "mine\n");
    let base = h.advance_main("shared.txt", "theirs\n");
    // Both targets must differ from the current head (see the name-repair
    // fixture above); repair then keeps `own`, which still conflicts.
    h.commit(&h.branch_ws, BRANCH, "extra.txt", "extra\n");
    // Two concurrent operations set the branch differently and are left
    // unsettled: two operation heads, and a conflicted name once merged.
    let op = crate::jj::operation_id(&h.jj, &h.store).unwrap();
    let parent = git(&h.repo, &["rev-parse", &format!("{own}^")]);
    for target in [own.as_str(), parent.as_str()] {
        h.jj.run(
            &h.store,
            &[
                "--at-op",
                &op,
                "bookmark",
                "set",
                BRANCH,
                "-r",
                target,
                "--allow-backwards",
                "--ignore-working-copy",
            ],
            "divergent bookmark",
        )
        .unwrap();
    }
    let mut sides = vec![own.clone(), parent.clone()];
    sides.sort();
    for purpose in [PreparationPurpose::Check, PreparationPurpose::Run] {
        let refusal = h.prepare(purpose).await.unwrap_err();
        assert!(
            matches!(refusal, PreparationRefusal::Unresolved { .. }),
            "{refusal}"
        );
        // Settlement may have happened; choosing the name's target may not.
        match cairn_vcs::resolve_coordinate(&h.store, BRANCH).await {
            Err(cairn_vcs::CoordinateResolutionError::Conflicted { mut targets, .. }) => {
                targets.sort();
                assert_eq!(targets, sides, "a {purpose:?} keeps both competing targets");
            }
            other => panic!("a {purpose:?} must leave the name conflicted: {other:?}"),
        }
        assert_eq!(
            cairn_vcs::resolve_coordinate(&h.store, "main")
                .await
                .unwrap(),
            base,
            "the base is unchanged"
        );
    }
    assert!(load_active_session(&h.db, BRANCH).await.unwrap().is_none());
    assert!(h.messages().await.is_empty(), "nothing woke");
    assert!(
        h.item_outcomes().await.is_empty(),
        "no conflict was reached or recorded"
    );

    // The owner's commit settles, repairs, and then meets the real conflict:
    // a session, and the branch still at its own content.
    let refusal = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err();
    let PreparationRefusal::Conflict(conflict) = refusal else {
        panic!("the owner reaches the content conflict: {refusal}");
    };
    let BaseConflict {
        base_commit,
        record,
        ..
    } = *conflict;
    assert_eq!(base_commit, base);
    assert!(matches!(record, ConflictRecord::Session { .. }));
    let tip = h.tip(BRANCH);
    assert_eq!(
        git(&h.repo, &["show", &format!("{tip}:shared.txt")]),
        "mine"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_unrelated_run_batch_holds_an_owner_commit_but_not_a_check() {
    let h = harness_or_skip!("prep_held");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    h.advance_main("incoming.txt", "theirs\n");
    // A wait hangs off a real session and turn of the owner's run.
    h.db.execute_script(format!(
        "INSERT INTO sessions (id, job_id, status, backend_id, created_at, updated_at)
           VALUES ('session-held', '{JOB}', 'open', 'backend', 1, 1);
         INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason,
                            created_at, updated_at)
           VALUES ('turn-held', 'session-held', '{RUN}', '{JOB}', 1, 'running', 'initial', 1, 1);"
    ))
    .await
    .unwrap();
    h.db.execute(
        "INSERT INTO agent_waits (id, job_id, run_id, session_id, predecessor_turn_id,
                                  tool_use_id, condition_json, state, created_at)
         VALUES ('wait-1', ?1, ?2, 'session-held', 'turn-held', 'tool-held', ?3, 'pending', 1)",
        (
            JOB,
            RUN,
            serde_json::json!({
                "kind": "run_batch",
                "request_id": "req-other",
                "commits": true,
                "label": "unrelated batch",
            })
            .to_string(),
        ),
    )
    .await
    .unwrap();
    let held = crate::orchestrator::base_advance::inflight_run_batch_except(
        &h.db,
        "proj-1",
        BRANCH,
        Some(JOB),
        None,
    )
    .await
    .unwrap();
    assert!(
        held.is_some(),
        "the fixture's wait must be one the canonical reader treats as a run batch"
    );
    let before = h.tip(BRANCH);
    let refusal = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err();
    assert!(
        matches!(refusal, PreparationRefusal::Busy(BusyReason::RunBatch(_))),
        "{refusal}"
    );
    assert_eq!(h.tip(BRANCH), before);

    let check = h.prepare(PreparationPurpose::Check).await.unwrap();
    assert!(
        check.advanced,
        "a check is never held by in-flight run work"
    );

    // The publication of that very batch is exempt from its own hold.
    let base = h.advance_main("incoming.txt", "theirs again\n");
    let mut exempt = PreparationRequest::new(
        "proj-1",
        BRANCH,
        Some(JOB.to_string()),
        PreparationPurpose::OwnerCommit,
    );
    exempt.sealed = Some(crate::mcp::handlers::run::SealedPublication::for_test(
        JOB,
        "req-other",
        "proj-1",
        &h.store,
        BRANCH,
    ));
    let published = prepare_branch(&h.orch, exempt).await.unwrap();
    assert!(published.advanced);
    assert_eq!(published.base_commit, base);
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_unmanaged_or_ambiguous_branch_is_refused_before_touching_the_store() {
    let h = harness_or_skip!("prep_unmanaged");
    let op = crate::jj::operation_id(&h.jj, &h.store).unwrap();
    let refusal = prepare_branch(
        &h.orch,
        PreparationRequest::for_check("proj-1", "agent/nobody", None),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(refusal, PreparationRefusal::Unmanaged { .. }),
        "{refusal}"
    );
    assert_eq!(
        live_branch_owner(&h.db, "proj-1", BRANCH)
            .await
            .unwrap()
            .as_deref(),
        Some(JOB)
    );

    h.db.execute_script(format!(
        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
           VALUES ('job-other', 'exec-1', 'node2', 'issue-1', 'proj-1', 'running', '{BRANCH}', 'agent/elsewhere', 2, 2);"
    ))
    .await
    .unwrap();
    let refusal = prepare_branch(
        &h.orch,
        PreparationRequest::for_check("proj-1", BRANCH, None),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(refusal, PreparationRefusal::RelationshipAmbiguous { .. }),
        "{refusal}"
    );
    assert_eq!(crate::jj::operation_id(&h.jj, &h.store).unwrap(), op);
}

/// cairn/6114 item 6, by shape: no resolution-basis read runs inside an
/// `under_guard` closure, and an owner's conflict assesses its inherited basis
/// before it takes the store lock, through the bounded offload.
#[test]
fn the_preparer_reads_resolution_bases_only_through_the_bounded_offload() {
    const SOURCE: &str = include_str!("preparation.rs");
    let production = SOURCE.split("#[cfg(test)]\n#[path").next().unwrap();
    for closure in production.split("under_guard(").skip(1) {
        let closure = closure.split(".await").next().unwrap();
        for read in [
            "resolution_basis_at(",
            "inherited_basis(",
            "replay_baseline_at(",
            "plan_resolution_carry(",
        ] {
            assert!(
                !closure.contains(read),
                "{read} runs under the store guard: {closure}"
            );
        }
    }
    let conflicted = production
        .split("async fn conflicted(")
        .nth(1)
        .expect("conflicted");
    let assessed = conflicted
        .find("bounded_inherited_basis(")
        .expect("the inherited basis is bounded");
    let locked = conflicted
        .find("lock(\"record conflict\")")
        .expect("the record-conflict lock");
    assert!(assessed < locked, "the basis is assessed before the lock");
    for (wrapper, read) in [
        ("async fn bounded_inherited_basis(", "resolution_basis_at("),
        ("async fn declared_plan(", "replay_baseline_at("),
    ] {
        let body = production
            .split(wrapper)
            .nth(1)
            .unwrap_or_else(|| panic!("{wrapper}"))
            .split("\n    }\n")
            .next()
            .unwrap();
        let offload = body
            .find("bounded_store_assessment(")
            .unwrap_or_else(|| panic!("{wrapper} offloads"));
        let at = body
            .find(read)
            .unwrap_or_else(|| panic!("{wrapper} reads {read}"));
        assert!(
            offload < at,
            "{wrapper} reads {read} only inside the offload"
        );
    }
}

/// A jj wrapper whose content reads at one armed commit sleep past any
/// budget: the specimen's per-call store cost (cairn/6752 measured about 3 s
/// per `jj file show`, so an assessment over 137 paths needs about 20 min).
/// Returns the wrapper, the arming file and the file each slow read's pid is
/// appended to.
#[cfg(unix)]
fn slow_reads_wrapper(tools: &Path, real: &str) -> (PathBuf, PathBuf, PathBuf) {
    use std::os::unix::fs::PermissionsExt;

    let wrapper = tools.join("jj-slow-basis");
    let armed = tools.join("armed");
    let reads = tools.join("reads");
    let quote = |s: &str| format!("'{}'", s.replace('\'', "'\\''"));
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nif test -s {armed}; then\n  slow=$(cat {armed})\n  case \" $* \" in\n    *' file show '*\" -r $slow \"*)\n      echo $$ >> {reads}\n      exec sleep 30\n      ;;\n  esac\nfi\nexec {real} \"$@\"\n",
            armed = quote(&armed.to_string_lossy()),
            reads = quote(&reads.to_string_lossy()),
            real = quote(real),
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    (wrapper, armed, reads)
}

/// cairn/6114 item 6 and cairn/6752 part 2 on the canonical preparer. A
/// previous session's basis whose read outlasts the budget never ends the
/// branch's preparation in a cooldown: the conflict at the new base is
/// recorded as usual and the reopened session keeps the earlier basis.
/// Throughout the slow read the store lock is free, and another branch
/// prepares to completion on the same single runtime thread, so neither a
/// guard nor a runtime worker is held. The slow read is reaped.
///
/// On an owner's conflict the first reader of that basis is the declared-merge
/// probe (`replay_baseline_at` at the head, which is the new `ours`). Its
/// timeout is only a missing optimisation, so preparation degrades to the
/// ordinary conflict, and the session-basis assessment does not repeat the read.
#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
#[serial_test::serial(jj)]
async fn a_timed_out_declared_plan_probe_records_the_conflict_without_a_cooldown() {
    use std::time::{Duration, Instant};

    let Some(real) = crate::jj::tests::jj_bin() else {
        return;
    };
    let tools = tempfile::tempdir().unwrap();
    let (wrapper, armed, reads) = slow_reads_wrapper(tools.path(), &real);
    let h = harness(wrapper.to_str().unwrap(), "prep_slow_basis").await;

    let own = h.commit(&h.branch_ws, BRANCH, "shared.txt", "mine\n");
    let first = h.advance_main("shared.txt", "theirs one\n");
    let PreparationRefusal::Conflict(conflict) = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err()
    else {
        panic!("the owner's commit conflicts and opens a session");
    };
    let ConflictRecord::Session {
        intent_id: first_session,
        ..
    } = conflict.record
    else {
        panic!("the owner's commit conflicts and opens a session");
    };

    const OTHER: &str = "agent/prep-other";
    let other_ws = h.workspaces.path().join("other");
    crate::jj::add_workspace(&h.jj, &h.store, &other_ws, OTHER, "main", None).unwrap();
    h.commit(&other_ws, OTHER, "other.txt", "other work\n");
    h.db.execute_script(format!(
        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
           VALUES ('job-other', 'exec-1', 'node-other', 'issue-1', 'proj-1', 'running', '{OTHER}', NULL, 1, 1);"
    ))
    .await
    .unwrap();

    // The base moves again: the owner's next commit conflicts afresh, and its
    // new session must inherit from the first, whose sides it re-reads.
    let second = h.advance_main("shared.txt", "theirs two\n");
    let earlier_basis = load_active_session(&h.db, BRANCH)
        .await
        .unwrap()
        .expect("the first session")
        .resolution_basis;
    std::fs::write(&armed, &first).unwrap();
    let read_pids = || {
        std::fs::read_to_string(&reads)
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect::<Vec<_>>()
    };

    let slow_log = crate::orchestrator::conflict_session::SLOW_ASSESSMENT_LOG_TARGET;
    crate::test_support::capture_log(slow_log);
    let slow_done = std::cell::Cell::new(false);
    let started = Instant::now();
    let slow = async {
        let refusal = h.prepare(PreparationPurpose::OwnerCommit).await;
        slow_done.set(true);
        (refusal, started.elapsed())
    };
    let meanwhile = async {
        tokio::time::timeout(Duration::from_secs(20), async {
            while read_pids().is_empty() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the slow basis read starts");
        let guard = tokio::time::timeout(
            Duration::from_secs(2),
            h.orch.acquire_jj_store_lock(
                &h.store,
                crate::orchestrator::StoreLockClass::Foreground,
                "probe during the slow basis read",
            ),
        )
        .await
        .expect("no store guard is held across the basis read");
        drop(guard);
        let prepared = prepare_branch(
            &h.orch,
            PreparationRequest::new(
                "proj-1",
                OTHER,
                Some("job-other".to_string()),
                PreparationPurpose::Check,
            ),
        )
        .await
        .expect("the other branch prepares");
        assert!(
            !slow_done.get(),
            "the other branch finished while the slow read was still running"
        );
        prepared
    };
    let ((refusal, elapsed), other) = tokio::join!(slow, meanwhile);
    eprintln!("preparation with a slow inherited basis: {elapsed:?}");

    let refusal = refusal.unwrap_err();
    let PreparationRefusal::Conflict(conflict) = &refusal else {
        panic!("a slow declared-plan probe degrades to the ordinary conflict: {refusal}");
    };
    assert_eq!(
        (conflict.head.as_str(), conflict.base_commit.as_str()),
        (own.as_str(), second.as_str())
    );
    let ConflictRecord::Session {
        intent_id: reopened,
        reopened: true,
    } = &conflict.record
    else {
        panic!(
            "the conflict reopens the owner's session: {:?}",
            conflict.record
        );
    };
    assert_ne!(reopened, &first_session);
    assert!(
        elapsed < DEFAULT_COMPUTE_BUDGET + Duration::from_secs(10),
        "bounded by the budget, not the 30 s read: {elapsed:?}"
    );
    for pid in read_pids() {
        let alive = std::process::Command::new("kill")
            .args(["-0", &pid])
            .output()
            .unwrap()
            .status
            .success();
        assert!(!alive, "the timed-out read {pid} was reaped");
    }
    assert!(other.advanced);
    assert!(h.is_ancestor(&second, &other.head_after));
    assert_eq!(h.tip(BRANCH), own, "a conflict installs nothing");

    let session = load_active_session(&h.db, BRANCH)
        .await
        .unwrap()
        .expect("the session is reopened at the new base");
    assert_eq!(&session.intent_id, reopened);
    assert_eq!(session.destination_commit, second);
    assert_eq!(
        session.resolution_basis, earlier_basis,
        "the reopened session keeps the earlier basis"
    );
    let quarantine = crate::orchestrator::base_advance::load_reconcile_quarantine(
        &h.db, "proj-1", &h.store, BRANCH,
    )
    .await
    .unwrap();
    let quarantined = quarantine.map(|row| row.failure_kind);
    assert_ne!(
        quarantined.as_deref(),
        Some(crate::jj::RECONCILE_TIMEOUT),
        "no timeout cooldown is recorded"
    );
    // The owner's next commit binds the reopened session and publishes.
    let bound = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .expect("the next commit binds the reopened session");
    assert_eq!(
        bound.session.map(|bound| bound.intent_id),
        Some(reopened.clone())
    );
    assert_eq!(
        read_pids().len(),
        1,
        "one slow read: the session basis did not re-read what the probe timed out on"
    );
    // The slow assessment names its scope, the branch and how many paths it
    // read: what the production specimen could not.
    let named: Vec<String> = crate::test_support::captured_log(slow_log)
        .into_iter()
        .filter(|line| line.contains(&format!("branch={BRANCH} ")))
        .collect();
    assert!(
        named.iter().any(|line| {
            line.starts_with("slow declared merge plan:")
                && line.contains("candidate_paths=1 ")
                && line.ends_with("outcome=timed out")
        }),
        "the slow basis is logged with its scope, branch and path count: {named:?}"
    );
    assert!(
        !named
            .iter()
            .any(|line| line.starts_with("slow conflict session basis:")),
        "the session basis never re-ran the slow read: {named:?}"
    );
}

/// cairn/6752 part 1, the cairn/4113 specimen in small. A merge-mode branch
/// with an open catch-up session; the base moves; the owner commits. The
/// catch-up merge itself is cheap, but the reopened session's inherited basis
/// reads the earlier session's sides path by path, and on the specimen's store
/// that read needs minutes against a 10 s budget. It used to time the
/// preparation out, strand the commit in a hold and cool the coordinate down,
/// so nothing published while the base kept moving. Now the slow read keeps
/// the earlier basis, the conflict at the new base is recorded, and the
/// owner's commit publishes on its own head with the merge pending.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_merge_mode_owner_commit_publishes_when_the_base_moves_and_its_basis_is_slow() {
    use std::time::{Duration, Instant};

    let Some(real) = crate::jj::tests::jj_bin() else {
        return;
    };
    let tools = tempfile::tempdir().unwrap();
    let (wrapper, armed, reads) = slow_reads_wrapper(tools.path(), &real);
    let h = harness(wrapper.to_str().unwrap(), "prep_merge_slow_basis").await;
    let budget = Duration::from_secs(2);
    let owner_commit = || {
        let mut request = PreparationRequest::new(
            "proj-1",
            BRANCH,
            Some(JOB.to_string()),
            PreparationPurpose::OwnerCommit,
        );
        request.compute_budget = budget;
        prepare_branch(&h.orch, request)
    };

    let own = h.commit(&h.branch_ws, BRANCH, "shared.txt", "mine\n");
    h.enable_merge_mode().await;
    let first = h.advance_main("shared.txt", "theirs one\n");
    let opened = owner_commit()
        .await
        .expect("the first conflict publishes under the session it opens");
    let first_session = opened.session.expect("bound to the opened session");
    assert!(!opened.advanced);

    // Main moves on, as it did for 4113 (fa13df6d -> c1d09b85).
    let second = h.advance_main("shared.txt", "theirs two\n");
    std::fs::write(&armed, &first).unwrap();
    let started = Instant::now();
    let pending = owner_commit()
        .await
        .expect("the owner's commit is not stranded by a slow basis");
    let elapsed = started.elapsed();
    assert!(
        !std::fs::read_to_string(&reads)
            .unwrap_or_default()
            .is_empty(),
        "the basis read really outlasted the budget"
    );
    assert!(
        elapsed < budget * 3 + Duration::from_secs(5),
        "bounded by the budget, not the 30 s read: {elapsed:?}"
    );
    assert!(!pending.advanced, "the catch-up merge stays pending");
    assert_eq!(pending.head_after, own);
    assert_eq!(pending.base_commit, second);
    assert_eq!(h.tip(BRANCH), own);
    let session = load_active_session(&h.db, BRANCH)
        .await
        .unwrap()
        .expect("the conflict is recorded at the new base");
    assert_eq!(session.destination_commit, second);
    assert_ne!(session.intent_id, first_session.intent_id);
    assert_eq!(
        pending.session.map(|bound| bound.intent_id),
        Some(session.intent_id.clone())
    );
    let quarantined = crate::orchestrator::base_advance::load_reconcile_quarantine(
        &h.db, "proj-1", &h.store, BRANCH,
    )
    .await
    .unwrap()
    .map(|row| row.failure_kind);
    assert_ne!(
        quarantined.as_deref(),
        Some(crate::jj::RECONCILE_TIMEOUT),
        "no timeout cooldown holds the branch"
    );

    // Every later commit keeps publishing under that session.
    let next = h.commit(&h.branch_ws, BRANCH, "own.txt", "more work\n");
    let later = owner_commit().await.expect("the next commit publishes");
    assert_eq!(later.head_after, next);
    assert_eq!(
        later.session.map(|bound| bound.intent_id),
        Some(session.intent_id)
    );
}

#[tokio::test]
#[serial_test::serial(jj)]
async fn an_unreadable_patch_comparison_in_after_install_cancels_the_original_wave() {
    assert_after_install_wave_disposition(false, false).await;
}

#[tokio::test]
#[serial_test::serial(jj)]
async fn merge_mode_unresolved_after_install_keeps_the_wave_without_a_readable_patch_comparison() {
    assert_after_install_wave_disposition(true, false).await;
}

#[tokio::test]
#[serial_test::serial(jj)]
async fn merge_mode_resolution_after_install_cancels_the_wave_when_the_patch_changes() {
    assert_after_install_wave_disposition(true, true).await;
}

async fn assert_after_install_wave_disposition(merge_mode: bool, resolved: bool) {
    let Some(bin) = crate::jj::tests::jj_bin() else {
        eprintln!("skipping an_unreadable_patch_comparison_in_after_install_cancels_the_original_wave: jj not resolvable");
        return;
    };
    for unreadable_before in [true, false] {
        let h = harness(&bin, "unknown-review-patch").await;
        let head = h.tip(BRANCH);
        let base = h.tip("main");
        let tree = crate::jj::logical_tree_hash(&h.jj, &h.store, &head).unwrap();
        let cairn_vcs::SettledOperation::Single(operation) =
            cairn_vcs::settled_operation(&h.store).unwrap()
        else {
            panic!("fixture is settled");
        };
        let unknown = "f".repeat(40);
        assert!(crate::jj::own_patch_against(&h.jj, &h.store, &unknown, &base).is_err());
        if merge_mode {
            h.enable_merge_mode().await;
        }
        let changed =
            resolved.then(|| h.commit(&h.branch_ws, BRANCH, "shared.txt", "resolved patch\n"));
        let mut request = PreparationRequest::new(
            "proj-1",
            BRANCH,
            Some(JOB.to_string()),
            PreparationPurpose::Check,
        );
        if resolved {
            request.carry = Some(Arc::new(crate::jj::ResolutionCarry::from_tip(
                &head,
                &["shared.txt".to_string()],
            )));
        }
        let context = Context {
            orch: &h.orch,
            db: &h.db,
            shared_db: h.db.clone(),
            jj: h.jj.clone(),
            store: h.store.clone(),
            repo_path: h.repo.to_string_lossy().into_owned(),
            request: &request,
            owned: Mutex::new(None),
            recorded: Default::default(),
            fenced_here: Mutex::new(None),
        };
        let relationship = BaseRelationship {
            owner_job_id: JOB.into(),
            declared: None,
            project_default: "main".into(),
            target_override: None,
            base_branch: "main".into(),
        };
        let mut installed = crate::jj::InstalledBranchAdvance {
            coordinate: crate::jj::AdvanceCoordinate {
                branch: BRANCH.into(),
                head: head.clone(),
                base_branch: "main".into(),
                base: base.clone(),
                operation,
            },
            head: head.clone(),
            tree: tree.clone(),
            export: Ok(()),
            healing: crate::jj::AncestryHeal {
                tip: head.clone(),
                healed: Vec::new(),
                dropped: Vec::new(),
            },
            dropped_commit_ids: Vec::new(),
            accepted_operation: None,
            recoveries: Vec::new(),
        };
        let wave = h
            .orch
            .try_begin_turn_end_checks(JOB, "unknown-patch-wave")
            .unwrap();
        let plan = Arc::new(crate::execution::checks_turn_end::WavePlan {
            head: head.clone(),
            base: base.clone(),
            tree_hash: tree.clone(),
            checks: HashMap::new(),
            applicable_names: Default::default(),
            input_hashes: HashMap::new(),
            expected_toolchain: String::new(),
            unsupported: Default::default(),
            capture: None,
        });
        wave.cancel()
            .set_runtime_status("unknown-patch-wave", plan.clone());
        context
            .after_install(&[], &relationship, &installed, &mut Vec::new())
            .await;
        assert!(
            !wave.is_cancelled(),
            "a readable identical patch keeps the wave"
        );
        let comparison_side = changed.clone().unwrap_or(unknown);
        if unreadable_before {
            installed.coordinate.head = comparison_side;
        } else {
            installed.head = comparison_side;
        }
        context
            .after_install(&[], &relationship, &installed, &mut Vec::new())
            .await;
        assert_eq!(
            wave.is_cancelled(),
            !merge_mode || resolved,
            "unresolved merge preserves the original wave without comparison; replay and changed resolution withdraw it"
        );
        assert_eq!(plan.head, head);
        assert_eq!(plan.base, base);
        assert_eq!(plan.tree_hash, tree);
        assert_eq!(
            h.tip(BRANCH),
            changed.unwrap_or(head),
            "after_install itself changes no bookmark"
        );
    }
}

impl Harness {
    pub(in crate::orchestrator::base_advance) async fn enable_merge_mode(&self) {
        use crate::orchestrator::catch_up::{self, SetCatchUpMode};

        catch_up::set(
            &self.db,
            SetCatchUpMode {
                project_id: "proj-1",
                store: &self.store,
                bookmark: BRANCH,
                job_id: JOB,
                actor: "operator",
                review_paths: &[],
                reviewed_through: &self.tip("main"),
            },
        )
        .await
        .unwrap();
        assert!(
            catch_up::mode_for(&self.db, "proj-1", &self.store, BRANCH)
                .await
                .unwrap()
                .is_some(),
            "the fixture's live job and active issue must not expire merge mode"
        );
    }

    pub(in crate::orchestrator::base_advance) async fn assert_merge_advance(
        &self,
        source: &str,
        destination: &str,
        installed: &str,
        prior_commits: &[String],
    ) {
        assert_eq!(
            git(&self.repo, &["show", "-s", "--format=%P", installed]),
            format!("{source} {destination}"),
            "one merge with the untouched source first and exact new base second"
        );
        assert_eq!(
            git(
                &self.repo,
                &[
                    "rev-list",
                    "--count",
                    installed,
                    &format!("^{source}"),
                    &format!("^{destination}")
                ],
            ),
            "1",
            "no rewritten source commits or structural replay repair"
        );
        for prior in prior_commits {
            assert!(
                self.is_ancestor(prior, installed),
                "source commit {prior} was rewritten"
            );
        }
        assert_eq!(git(&self.repo, &["rev-parse", BRANCH]), installed);
        let recorded = self
            .db
            .query_text(
                "SELECT base_commit FROM branch_advance_acceptances
              WHERE branch = ?1 AND head_before = ?2 AND head_after = ?3 AND status = 'accepted'",
                (BRANCH, source.to_string(), installed.to_string()),
            )
            .await
            .unwrap();
        assert_eq!(
            recorded.as_deref(),
            Some(destination),
            "the accepted install durably records its exact base"
        );
        let job_base = self
            .db
            .query_text("SELECT base_commit FROM jobs WHERE id = ?1", (JOB,))
            .await
            .unwrap();
        assert_eq!(
            job_base.as_deref(),
            Some(destination),
            "the live job's durable base advances to the installed destination"
        );
    }
}

async fn merge_mode_prepares_again_after_main_moves(purpose: PreparationPurpose, name: &str) {
    let Some(bin) = crate::jj::tests::jj_bin() else {
        return;
    };
    let h = harness(&bin, name).await;
    let first = h.commit(&h.branch_ws, BRANCH, "own.txt", "first\n");
    let second = h.commit(&h.branch_ws, BRANCH, "own.txt", "second\n");
    h.enable_merge_mode().await;
    let d1 = h.advance_main("incoming.txt", "first base\n");
    let prepared = h.prepare(purpose).await.unwrap();
    assert!(prepared.advanced);
    h.assert_merge_advance(
        &second,
        &d1,
        &prepared.head_after,
        &[first.clone(), second.clone()],
    )
    .await;

    let d2 = h.advance_main("incoming.txt", "second base\n");
    let again = h.prepare(purpose).await.unwrap();
    assert!(again.advanced);
    assert_eq!(again.head_before, prepared.head_after);
    assert_eq!(again.base_commit, d2);
    h.assert_merge_advance(
        &prepared.head_after,
        &d2,
        &again.head_after,
        &[first, second, prepared.head_after.clone()],
    )
    .await;
    let unchanged = h.prepare(purpose).await.unwrap();
    assert!(
        !unchanged.advanced,
        "the same coordinate must not add another merge"
    );
    assert_eq!(unchanged.head_after, again.head_after);
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn merge_mode_owner_commit_preparation_preserves_source_ids_when_main_moves_again() {
    merge_mode_prepares_again_after_main_moves(
        PreparationPurpose::OwnerCommit,
        "prep_merge_commit",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn merge_mode_check_preparation_preserves_source_ids_when_main_moves_again() {
    merge_mode_prepares_again_after_main_moves(PreparationPurpose::Check, "prep_merge_check").await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn merge_mode_run_preparation_preserves_source_ids_when_main_moves_again() {
    merge_mode_prepares_again_after_main_moves(PreparationPurpose::Run, "prep_merge_run").await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn merge_mode_owner_merge_preparation_preserves_source_ids_when_main_moves_again() {
    merge_mode_prepares_again_after_main_moves(
        PreparationPurpose::OwnerMerge,
        "prep_merge_owner_merge",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn merge_mode_conflict_retries_a_changed_destination_without_replaying_source_history() {
    let h = harness_or_skip!("prep_merge_conflict_retry");
    let first = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let own = h.commit(&h.branch_ws, BRANCH, "shared.txt", "mine\n");
    h.enable_merge_mode().await;
    let d1 = h.advance_main("shared.txt", "theirs\n");
    // The owner's own commit does not wait on its catch-up: the conflict is
    // recorded, and the commit publishes under that session, unadvanced.
    let pending = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .expect("a merge-mode owner commit publishes with its catch-up pending");
    assert!(!pending.advanced);
    assert_eq!(pending.head_after, own);
    assert_eq!(pending.base_commit, d1);
    let session = load_active_session(&h.db, BRANCH).await.unwrap().unwrap();
    assert_eq!(session.theirs.as_deref(), Some(d1.as_str()));
    assert_eq!(
        pending
            .session
            .as_ref()
            .map(|bound| bound.intent_id.as_str()),
        Some(session.intent_id.as_str())
    );
    assert!(
        pending
            .warnings
            .iter()
            .any(|warning| warning.contains("still pending")),
        "the owner is told the merge is pending: {:?}",
        pending.warnings
    );
    assert_eq!(
        h.prepare(PreparationPurpose::OwnerMerge)
            .await
            .err()
            .map(|refusal| refusal.kind()),
        Some("conflicts with base"),
        "landing still needs the catch-up"
    );
    assert_eq!(
        h.tip(BRANCH),
        own,
        "a failed merge installs no structural repair"
    );
    assert!(acceptance_rows(&h).await.is_empty());

    // A destination that removes its conflicting edit can now merge. A prior
    // conflict, or its bound-session shortcut, must not hold this new pair.
    let d2 = h.advance_main("shared.txt", "original\n");
    let prepared = h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    assert!(prepared.advanced);
    h.assert_merge_advance(&own, &d2, &prepared.head_after, &[first, own.clone()])
        .await;
    assert_eq!(
        git(
            &h.repo,
            &["show", &format!("{}:shared.txt", prepared.head_after)]
        ),
        "mine"
    );
    assert!(load_active_session(&h.db, BRANCH).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn merge_mode_cached_check_conflict_opens_owner_session_once_and_retries_new_destination() {
    let h = harness_or_skip!("prep_merge_cached_conflict");
    let own = h.commit(&h.branch_ws, BRANCH, "shared.txt", "mine\n");
    h.enable_merge_mode().await;
    let d1 = h.advance_main("shared.txt", "theirs\n");
    let PreparationRefusal::Conflict(first) =
        h.prepare(PreparationPurpose::Check).await.unwrap_err()
    else {
        panic!("the first check must record a merge conflict");
    };
    assert!(matches!(first.record, ConflictRecord::CheckEvidence { .. }));
    let objects = git(&h.repo, &["count-objects", "-v"]);
    let PreparationRefusal::Conflict(again) =
        h.prepare(PreparationPurpose::Check).await.unwrap_err()
    else {
        panic!("the repeated check must reuse its merge conflict");
    };
    assert_eq!(
        first, again,
        "the same complete conflict evidence is reused"
    );
    assert_eq!(
        git(&h.repo, &["count-objects", "-v"]),
        objects,
        "a cached retry must not write another hidden merge object"
    );
    assert!(load_active_session(&h.db, BRANCH).await.unwrap().is_none());
    assert!(h.wakes().await.is_empty());

    let PreparationRefusal::Conflict(owner) =
        h.prepare(PreparationPurpose::OwnerMerge).await.unwrap_err()
    else {
        panic!("the owner's merge promotes the cached diagnostic to a session");
    };
    assert_eq!(owner.paths, first.paths);
    assert!(matches!(owner.record, ConflictRecord::Session { .. }));
    let session = load_active_session(&h.db, BRANCH).await.unwrap().unwrap();
    assert_eq!(session.theirs.as_deref(), Some(d1.as_str()));
    assert_eq!(session.ours.as_deref(), Some(own.as_str()));
    let PreparationRefusal::Conflict(repeated_owner) =
        h.prepare(PreparationPurpose::OwnerMerge).await.unwrap_err()
    else {
        panic!("an unresolved owner merge must still refuse");
    };
    assert_eq!(
        repeated_owner.record,
        ConflictRecord::Session {
            intent_id: session.intent_id.clone(),
            reopened: false,
        }
    );
    let bound = h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    assert!(!bound.advanced);
    assert_eq!(bound.head_before, own);
    assert_eq!(bound.head_after, own);
    assert_eq!(bound.session.unwrap().intent_id, session.intent_id);
    assert_eq!(h.wakes().await, vec!["wake".to_string()]);
    assert_eq!(h.tip(BRANCH), own);
    assert!(acceptance_rows(&h).await.is_empty());

    let d2 = h.advance_main("shared.txt", "original\n");
    let prepared = h.prepare(PreparationPurpose::OwnerMerge).await.unwrap();
    assert!(prepared.advanced);
    h.assert_merge_advance(&own, &d2, &prepared.head_after, std::slice::from_ref(&own))
        .await;
    assert!(load_active_session(&h.db, BRANCH).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn merge_mode_review_path_notes_reach_each_live_job_once_per_base_without_waking() {
    use crate::orchestrator::catch_up::{self, SetCatchUpMode};

    let h = harness_or_skip!("prep_merge_multi_job_notes");
    h.db.execute_script(format!(
        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, created_at, updated_at)
           VALUES ('job-prep-peer', 'exec-1', 'peer', 'issue-1', 'proj-1', 'running', '{BRANCH}', 2, 2);
         INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
           VALUES ('run-prep-peer', 'issue-1', 'proj-1', 'job-prep-peer', 'live', 2, 2);"
    )).await.unwrap();
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    catch_up::set(
        &h.db,
        SetCatchUpMode {
            project_id: "proj-1",
            store: &h.store,
            bookmark: BRANCH,
            job_id: JOB,
            actor: "operator",
            review_paths: &["incoming.txt".to_string()],
            reviewed_through: &h.tip("main"),
        },
    )
    .await
    .unwrap();

    for (index, content) in ["first base\n", "second base\n"].into_iter().enumerate() {
        let base = h.advance_main("incoming.txt", content);
        let before = h.tip(BRANCH);
        let prepared = h.prepare(PreparationPurpose::Check).await.unwrap();
        h.assert_merge_advance(
            &before,
            &base,
            &prepared.head_after,
            std::slice::from_ref(&own),
        )
        .await;
        let repeated = h.prepare(PreparationPurpose::Check).await.unwrap();
        assert!(!repeated.advanced);
        let notes = h.messages().await;
        assert_eq!(
            notes.len(),
            (index + 1) * 2,
            "exactly one note per job and base, not a generic duplicate"
        );
        for (note, _) in &notes {
            assert!(note.contains("by merge"), "{note}");
            assert!(
                note.contains("Review-required paths") && note.contains("incoming.txt"),
                "{note}"
            );
        }
        let deliveries = h.db.read(|conn| Box::pin(async move {
            let mut rows = conn.query(
                "SELECT recipient, wake, CAST(COUNT(*) AS TEXT) FROM attention_pushes GROUP BY recipient, wake ORDER BY recipient",
                (),
            ).await?;
            let mut all = Vec::new();
            while let Some(row) = rows.next().await? {
                all.push((row.text(0)?, row.text(1)?, row.text(2)?));
            }
            Ok::<_, DbError>(all)
        })).await.unwrap();
        let count = (index + 1).to_string();
        assert_eq!(
            deliveries,
            vec![
                (JOB.to_string(), "passive".to_string(), count.clone()),
                ("job-prep-peer".to_string(), "passive".to_string(), count),
            ],
            "review-path hits gate landing but never resume either owner"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn every_live_job_on_a_branch_hears_each_clean_note_and_conflict_steer_once() {
    let h = harness_or_skip!("prep_two_job_delivery");
    h.db.execute_script(format!(
        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, created_at, updated_at)
           VALUES ('job-prep-peer', 'exec-1', 'peer', 'issue-1', 'proj-1', 'running', '{BRANCH}', 2, 2);
         INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
           VALUES ('run-prep-peer', 'issue-1', 'proj-1', 'job-prep-peer', 'live', 2, 2);"
    ))
    .await
    .unwrap();
    let deliveries = || async {
        h.db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT recipient, wake, CAST(COUNT(*) AS TEXT) FROM attention_pushes
                          GROUP BY recipient, wake ORDER BY recipient, wake",
                        (),
                    )
                    .await?;
                let mut all = Vec::new();
                while let Some(row) = rows.next().await? {
                    all.push((row.text(0)?, row.text(1)?, row.text(2)?));
                }
                Ok::<_, DbError>(all)
            })
        })
        .await
        .unwrap()
    };
    let row = |job: &str, wake: &str| (job.to_string(), wake.to_string(), "1".to_string());

    h.commit(&h.branch_ws, BRANCH, "shared.txt", "mine\n");
    h.advance_main("incoming.txt", "theirs\n");
    assert!(h.prepare(PreparationPurpose::Check).await.unwrap().advanced);
    let again = h.prepare(PreparationPurpose::Check).await.unwrap();
    assert!(!again.advanced, "an unchanged base notes nobody again");
    assert_eq!(
        deliveries().await,
        vec![row(JOB, "passive"), row("job-prep-peer", "passive")],
        "each live job hears the clean advance once"
    );

    h.advance_main("shared.txt", "theirs\n");
    for _ in 0..2 {
        assert!(matches!(
            h.prepare(PreparationPurpose::OwnerMerge).await.unwrap_err(),
            PreparationRefusal::Conflict(_)
        ));
    }
    assert_eq!(
        deliveries().await,
        vec![
            row(JOB, "passive"),
            row(JOB, "wake"),
            row("job-prep-peer", "passive"),
            row("job-prep-peer", "wake"),
        ],
        "each live job hears the conflict steer once per fingerprint"
    );
}

/// A child whose immediate parent has an open conflict session with its own
/// base is held by every purpose that would move it: a typed refusal naming
/// the parent and its session, the child unmoved, nothing recorded, nobody
/// woken. The hold is read from the parent's session, so closing it releases
/// the child and the next demand prepares it onto the parent.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_child_of_a_parent_mid_conflict_is_held_until_the_parents_session_closes() {
    const PARENT: &str = "agent/held-parent";
    let h = harness_or_skip!("prep_held_by_parent");
    let parent_ws = h.workspaces.path().join("held-parent");
    crate::jj::add_workspace(&h.jj, &h.store, &parent_ws, PARENT, "main", None).unwrap();
    let parent_tip = h.commit(&parent_ws, PARENT, "parent.txt", "parent work\n");
    let child_tip = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let main = h.tip("main");
    let store = h.store.to_string_lossy().into_owned();
    h.db.execute_script(format!(
        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
           VALUES ('job-parent', 'exec-1', 'parent', 'issue-1', 'proj-1', 'running', '{PARENT}', 'main', 2, 2);
         UPDATE jobs SET base_branch = '{PARENT}' WHERE id = '{JOB}';
         INSERT INTO jj_reconcile_intents
           (id, project_id, store_path, target_branch, destination_commit, trigger_sources_json, status, created_at, updated_at)
         VALUES ('parent-session', 'proj-1', '{store}', 'main', '{main}', '[\"demand:commit\"]', 'completed', 2, 2);
         INSERT INTO jj_reconcile_items
           (intent_id, bookmark, observed_tip, status, outcome_kind, diagnostic_version, resolution_state, updated_at)
         VALUES ('parent-session', '{PARENT}', '{parent_tip}', 'completed', 'conflicted', 1, 'open', 2);"
    ))
    .await
    .unwrap();
    let (messages, wakes) = (h.messages().await, h.wakes().await);
    let expected = ParentConflictHold {
        branch: BRANCH.to_string(),
        parent: PARENT.to_string(),
        session: "parent-session".to_string(),
        parent_base: "main".to_string(),
        parent_destination: main.clone(),
    };

    for purpose in [
        PreparationPurpose::OwnerCommit,
        PreparationPurpose::Run,
        PreparationPurpose::Check,
    ] {
        let refusal = match h.prepare(purpose).await {
            Ok(prepared) => panic!(
                "{purpose:?} prepared a held child at {}",
                prepared.head_after
            ),
            Err(refusal) => refusal,
        };
        let PreparationRefusal::HeldByParentConflict(hold) = &refusal else {
            panic!("{purpose:?} must be held by the parent's session: {refusal:?}");
        };
        assert_eq!(**hold, expected, "{purpose:?}");
        assert_eq!(refusal.kind(), "held by parent conflict");
        let text = refusal.to_string();
        assert!(
            text.contains(PARENT) && text.contains("parent-session"),
            "{text}"
        );
        assert_eq!(h.tip(BRANCH), child_tip, "{purpose:?} moved the held child");
    }
    assert_eq!(h.messages().await, messages, "a hold sends nothing");
    assert_eq!(h.wakes().await, wakes, "a hold wakes nobody");
    assert!(
        h.item_outcomes().await.is_empty(),
        "a hold records no evidence or session on the child"
    );
    assert!(load_active_session(&h.db, BRANCH).await.unwrap().is_none());

    close_open_sessions_for_branch(&h.db, PARENT).await.unwrap();
    let prepared = h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    assert!(prepared.advanced, "the released child is prepared");
    assert_eq!(prepared.base_commit, parent_tip);
    assert!(h.is_ancestor(&parent_tip, &prepared.head_after));
    assert_eq!(h.tip(BRANCH), prepared.head_after);
}
