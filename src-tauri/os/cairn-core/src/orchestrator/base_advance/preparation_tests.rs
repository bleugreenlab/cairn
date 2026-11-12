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

static FENCE_WRITES_FAIL: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub(super) fn fence_writes_fail() -> bool {
    FENCE_WRITES_FAIL.load(std::sync::atomic::Ordering::SeqCst)
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

/// Two suites of one `cairn check run` prepare the same branch at once. The
/// one that reaches the install while its sibling's preparation still holds
/// the branch's reservation waits for it and then installs, rather than
/// refusing as busy.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_check_waits_for_a_sibling_preparation_holding_the_branch() {
    let h = harness_or_skip!("prep_sibling_check");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "own\n");
    let base = h.advance_main("base.txt", "base\n");
    let sibling = h
        .orch
        .reserve_branches(
            &h.store,
            &[BRANCH],
            &format!("branch preparation of `{BRANCH}` for check"),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let orch = h.orch.clone();
    let mut waiting = tokio::spawn(async move {
        prepare_branch(
            &orch,
            PreparationRequest::for_check("proj-1", BRANCH, Some(JOB.to_string())),
        )
        .await
    });
    // A preparation that does not wait refuses well inside this window.
    if let Ok(early) = tokio::time::timeout(Duration::from_secs(3), &mut waiting).await {
        panic!(
            "the check must wait on its sibling's reservation, not end: {:?}",
            early
                .unwrap()
                .map(|prepared| prepared.head_after)
                .map_err(|refusal| refusal.to_string())
        );
    }
    drop(sibling);
    let prepared = tokio::time::timeout(Duration::from_secs(60), waiting)
        .await
        .expect("the check proceeds once its sibling releases the branch")
        .unwrap()
        .expect("the check prepares the branch after waiting");
    assert!(prepared.advanced, "the waiting check installed the advance");
    assert!(h.is_ancestor(&base, &prepared.head_after));
    assert_eq!(h.tip(BRANCH), prepared.head_after);
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
