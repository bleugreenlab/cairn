use super::*;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::db::DbState;
use crate::services::testing::{MockGitClient, TestServicesBuilder};
use crate::services::GitClient;
use crate::storage::{LocalDb, SearchIndex};
use cairn_db::turso::params;

const OLD: &str = "086a7e5f0000000000000000000000000000000a";
const NEW: &str = "9e77a2750000000000000000000000000000000b";

fn at(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(seconds, 0).unwrap()
}

fn test_orchestrator(db: LocalDb, git: Arc<dyn GitClient>) -> Orchestrator {
    let config_dir = tempfile::tempdir().unwrap().keep();
    let db_state = Arc::new(DbState::new(
        Arc::new(db),
        Arc::new(SearchIndex::open_or_create(config_dir.join("search-index.db")).unwrap()),
    ));
    let mut services = TestServicesBuilder::new().build();
    services.git = git;
    Orchestrator::builder(db_state, Arc::new(services), config_dir).build()
}

async fn seed_project(db: &LocalDb, id: &str, repo_path: &str) {
    let (id, repo_path) = (id.to_string(), repo_path.to_string());
    db.write(|conn| {
        let (id, repo_path) = (id.clone(), repo_path.clone());
        Box::pin(async move {
            conn.execute(
                "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                 VALUES (?1, 'default', 'Project', ?1, ?2, 'main', 1, 1)",
                params![id.as_str(), repo_path.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

// ---------------------------------------------------------------------------
// Label shapes.
// ---------------------------------------------------------------------------

#[test]
fn a_non_default_branch_names_its_commit_and_age() {
    let now = at(10_000);
    assert_eq!(
        format_revision_label("agent/foo", "1a2b3c4d5e6f", Some(10_000 - 720), now, None),
        "agent/foo@1a2b3c4d committed 12m ago"
    );
}

#[test]
fn a_current_default_branch_says_when_origin_was_checked() {
    let now = at(10_000);
    let freshness = Freshness {
        state: Some(BranchSync {
            checked_at: Some(at(10_000 - 31)),
            remote_tip: Some(NEW.into()),
            ..Default::default()
        }),
        served_contains_tip: Some(true),
    };
    assert_eq!(
        format_revision_label("main", NEW, Some(10_000 - 120), now, Some(&freshness)),
        "main@9e77a275 committed 2m ago, origin checked 31s ago"
    );
}

#[test]
fn a_default_branch_behind_origin_is_marked_stale_with_the_reason() {
    let now = at(10_000);
    let mut state = BranchSync {
        checked_at: Some(at(9_990)),
        remote_tip: Some(NEW.into()),
        sync_started_at: Some(at(10_000 - 100)),
        ..Default::default()
    };
    let label = |state: &BranchSync| {
        format_revision_label(
            "main",
            OLD,
            Some(9_000),
            now,
            Some(&Freshness {
                state: Some(state.clone()),
                served_contains_tip: Some(false),
            }),
        )
    };
    assert_eq!(
        label(&state),
        "main@086a7e5f STALE: origin main is 9e77a275 (sync in progress 1m40s)"
    );

    state.sync_started_at = None;
    state.last_error = Some(SyncError {
        at: at(10_000 - 180),
        phase: Phase::Sync,
        message: "jj git import failed\nsecond line".into(),
    });
    assert_eq!(
        label(&state),
        "main@086a7e5f STALE: origin main is 9e77a275 (sync failed 3m ago: jj git import failed)"
    );

    state.last_error = None;
    assert_eq!(
        label(&state),
        "main@086a7e5f STALE: origin main is 9e77a275 (sync pending)"
    );
}

/// The read resolved `main` to A; a sync then imported B and checked origin a
/// moment ago. The tree served is still A, so the label must say STALE even
/// though everything the store knows now is current.
#[test]
fn a_served_commit_older_than_origin_is_stale_even_after_a_fresh_sync() {
    let now = at(10_000);
    let freshness = Freshness {
        state: Some(BranchSync {
            checked_at: Some(now),
            remote_tip: Some(NEW.into()),
            ..Default::default()
        }),
        served_contains_tip: Some(false),
    };
    assert_eq!(
        format_revision_label("main", OLD, Some(9_000), now, Some(&freshness)),
        "main@086a7e5f STALE: origin main is 9e77a275 (sync pending)"
    );
}

#[test]
fn a_served_commit_ahead_of_origin_is_not_stale() {
    let now = at(10_000);
    let freshness = Freshness {
        state: Some(BranchSync {
            checked_at: Some(at(9_990)),
            remote_tip: Some(OLD.into()),
            ..Default::default()
        }),
        served_contains_tip: Some(true),
    };
    assert_eq!(
        format_revision_label("main", NEW, Some(9_900), now, Some(&freshness)),
        "main@9e77a275 committed 1m ago, origin checked 10s ago"
    );
}

#[test]
fn a_default_branch_never_checked_says_so() {
    let now = at(10_000);
    assert_eq!(
        format_revision_label(
            "main",
            OLD,
            Some(10_000 - 7_200),
            now,
            Some(&Freshness::default())
        ),
        "main@086a7e5f committed 2h ago, origin not checked since launch"
    );
}

#[test]
fn a_failing_check_is_reported_even_when_nothing_looks_stale() {
    let now = at(10_000);
    let freshness = Freshness {
        state: Some(BranchSync {
            checked_at: Some(at(10_000 - 600)),
            remote_tip: Some(OLD.into()),
            last_error: Some(SyncError {
                at: at(10_000 - 20),
                phase: Phase::Check,
                message: "git ls-remote origin refs/heads/main failed: Could not resolve host"
                    .into(),
            }),
            ..Default::default()
        }),
        served_contains_tip: Some(true),
    };
    assert_eq!(
        format_revision_label("main", OLD, None, now, Some(&freshness)),
        "main@086a7e5f, origin checked 10m ago (last check failed 20s ago: git ls-remote origin refs/heads/main failed: Could not resolve host)"
    );
}

#[test]
fn a_project_without_origin_says_there_is_nothing_to_be_behind() {
    let freshness = Freshness {
        state: Some(BranchSync {
            no_origin: true,
            ..Default::default()
        }),
        served_contains_tip: None,
    };
    assert_eq!(
        format_revision_label("main", OLD, Some(9_000), at(9_000), Some(&freshness)),
        "main@086a7e5f committed 0s ago, no origin remote"
    );
}

#[test]
fn durations_are_compact_and_never_negative() {
    assert_eq!(age(-5), "0s");
    assert_eq!(age(42), "42s");
    assert_eq!(age(12 * 60 + 59), "12m");
    assert_eq!(age(3 * 3_600 + 1), "3h");
    assert_eq!(age(2 * 86_400), "2d");
    assert_eq!(elapsed(100), "1m40s");
    assert_eq!(elapsed(120), "2m");
    assert_eq!(elapsed(40), "40s");
}

// ---------------------------------------------------------------------------
// Fetch ref-lock races.
// ---------------------------------------------------------------------------

#[test]
fn ref_lock_races_are_parsed_from_git_stderr() {
    let stderr = "error: cannot lock ref 'refs/remotes/origin/main': is at 9e77a275 but expected 086a7e5f\n \
                  ! 086a7e5f..9e77a275  main -> origin/main  (unable to update local ref)\n\
                  error: some local refs could not be updated";
    assert_eq!(
        parse_ref_lock_races(stderr),
        vec![(
            "refs/remotes/origin/main".to_string(),
            "9e77a275".to_string()
        )]
    );
    assert!(parse_ref_lock_races("fatal: unable to access origin").is_empty());
}

#[test]
fn a_lost_race_counts_as_success_only_when_the_ref_holds_the_winner() {
    let repo = tempfile::tempdir().unwrap();
    crate::jj::tests::init_project(repo.path());
    let head = crate::jj::tests::git_stdout(repo.path(), &["rev-parse", "HEAD"]);
    crate::jj::tests::git(
        repo.path(),
        &["update-ref", "refs/remotes/origin/main", &head],
    );
    let race = |at: &str| {
        format!("error: cannot lock ref 'refs/remotes/origin/main': is at {at} but expected {OLD}")
    };
    assert!(lost_ref_race_is_settled(repo.path(), &race(&head)));
    assert!(!lost_ref_race_is_settled(repo.path(), &race(NEW)));
    assert!(!lost_ref_race_is_settled(
        repo.path(),
        "fatal: network down"
    ));
}

// ---------------------------------------------------------------------------
// The single-flight driver.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_observations_run_exactly_one_pass() {
    let registry = BaseBranchSyncRegistry::default();
    let passes = AtomicUsize::new(0);
    let pass = || async {
        passes.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    };
    let caught_up = || async { false };
    tokio::join!(
        drive(&registry, "p", "main", pass, caught_up),
        drive(&registry, "p", "main", pass, caught_up),
        drive(&registry, "p", "main", pass, caught_up),
    );
    assert_eq!(passes.load(Ordering::SeqCst), 1);
    assert!(!registry.in_flight("p", "main"));
    assert_eq!(
        registry.snapshot("p", "main").unwrap().sync_started_at,
        None
    );
}

#[tokio::test]
async fn a_tip_observed_mid_sync_earns_one_follow_up_pass() {
    let registry = BaseBranchSyncRegistry::default();
    let passes = AtomicUsize::new(0);
    let moved_again = AtomicBool::new(true);
    drive(
        &registry,
        "p",
        "main",
        || async {
            passes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
        || async { moved_again.swap(false, Ordering::SeqCst) },
    )
    .await;
    assert_eq!(passes.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_failed_pass_records_the_error_and_a_later_success_clears_it() {
    let registry = BaseBranchSyncRegistry::default();
    let passes = AtomicUsize::new(0);
    drive(
        &registry,
        "p",
        "main",
        || async {
            passes.fetch_add(1, Ordering::SeqCst);
            Err("git fetch origin failed: offline".to_string())
        },
        || async { true },
    )
    .await;
    assert_eq!(
        passes.load(Ordering::SeqCst),
        1,
        "a failure is not retried hot"
    );
    let error = registry.snapshot("p", "main").unwrap().last_error.unwrap();
    assert_eq!(error.phase, Phase::Sync);
    assert_eq!(error.message, "git fetch origin failed: offline");

    drive(
        &registry,
        "p",
        "main",
        || async { Ok(()) },
        || async { false },
    )
    .await;
    assert_eq!(registry.snapshot("p", "main").unwrap().last_error, None);
}

#[tokio::test]
async fn a_panicking_pass_does_not_wedge_the_project() {
    let registry = Arc::new(BaseBranchSyncRegistry::default());
    let panicking = registry.clone();
    let joined = tokio::spawn(async move {
        drive(
            &panicking,
            "p",
            "main",
            || async { panic!("pass blew up") },
            || async { false },
        )
        .await;
    })
    .await;
    assert!(joined.is_err());
    assert!(!registry.in_flight("p", "main"));
    let ran = AtomicBool::new(false);
    drive(
        &registry,
        "p",
        "main",
        || async {
            ran.store(true, Ordering::SeqCst);
            Ok(())
        },
        || async { false },
    )
    .await;
    assert!(ran.load(Ordering::SeqCst));
}

// ---------------------------------------------------------------------------
// Check recording.
// ---------------------------------------------------------------------------

#[test]
fn a_successful_check_clears_a_check_error_but_not_a_sync_error() {
    let registry = BaseBranchSyncRegistry::default();
    registry.record_error("p", "main", Phase::Check, "offline", at(1));
    registry.record_check("p", "main", Some(NEW.into()), Source::Poll, at(2));
    let state = registry.snapshot("p", "main").unwrap();
    assert_eq!(state.last_error, None);
    assert_eq!(state.remote_tip.as_deref(), Some(NEW));
    assert_eq!(state.remote_tip_source, Some(Source::Poll));

    registry.record_error("p", "main", Phase::Sync, "import failed", at(3));
    registry.record_check("p", "main", None, Source::Fetch, at(4));
    let state = registry.snapshot("p", "main").unwrap();
    assert_eq!(state.last_error.unwrap().phase, Phase::Sync);
    assert_eq!(
        state.remote_tip.as_deref(),
        Some(NEW),
        "a check that cannot name the tip keeps the last known one"
    );
    assert_eq!(state.checked_at, Some(at(4)));
}

#[tokio::test]
async fn a_failed_poll_keeps_the_last_successful_check_time() {
    let db = crate::storage::migrated_test_db("default-branch-poll-failure.db").await;
    seed_project(&db, "proj", "/repo").await;
    let mut git = MockGitClient::new();
    git.expect_remote_get_url()
        .returning(|_| Ok("git@github.com:acme/repo.git".into()));
    git.expect_ls_remote_branches()
        .returning(|_, _, _| Err("git ls-remote origin refs/heads/main failed: offline".into()));
    let orch = test_orchestrator(db, Arc::new(git));
    orch.base_branch_sync
        .record_check("proj", "main", Some(OLD.into()), Source::Poll, at(100));

    poll_project(&orch, "proj", Path::new("/repo"), &["main".to_string()]).await;

    let state = orch.base_branch_sync.snapshot("proj", "main").unwrap();
    assert_eq!(state.checked_at, Some(at(100)));
    assert_eq!(state.remote_tip.as_deref(), Some(OLD));
    let error = state.last_error.unwrap();
    assert_eq!(error.phase, Phase::Check);
    assert!(error.message.contains("offline"));
}

/// One unreachable origin must not delay any other project's check.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_origin_does_not_delay_other_projects() {
    let db = crate::storage::migrated_test_db("default-branch-poll-isolation.db").await;
    seed_project(&db, "hung", "/repos/hung").await;
    seed_project(&db, "healthy", "/repos/healthy").await;
    let mut git = MockGitClient::new();
    git.expect_remote_get_url()
        .returning(|_| Ok("git@github.com:acme/repo.git".into()));
    // One expectation per repository: mockall holds an expectation's lock
    // while its closure runs, so a shared one would serialize the two calls
    // and the test would measure the mock instead of the poll.
    git.expect_ls_remote_branches()
        .withf(|repo, _, _| repo == Path::new("/repos/hung"))
        .returning(|_, _, _| {
            std::thread::sleep(Duration::from_secs(3));
            Ok(HashMap::new())
        });
    git.expect_ls_remote_branches()
        .withf(|repo, _, _| repo == Path::new("/repos/healthy"))
        .returning(|_, _, _| Ok(HashMap::new()));
    let orch = test_orchestrator(db, Arc::new(git));

    let started = std::time::Instant::now();
    poll_once(&orch).await;
    loop {
        if orch
            .base_branch_sync
            .snapshot("healthy", "main")
            .is_some_and(|state| state.checked_at.is_some())
        {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the healthy project waited behind the hung origin"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !orch.base_branch_sync.begin_poll("hung"),
        "the hung project's check is still in flight, so the next tick skips it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_check_that_outlives_its_budget_is_recorded_as_failed() {
    let db = crate::storage::migrated_test_db("default-branch-poll-timeout.db").await;
    let mut git = MockGitClient::new();
    git.expect_remote_get_url()
        .returning(|_| Ok("git@github.com:acme/repo.git".into()));
    git.expect_ls_remote_branches().returning(|_, _, _| {
        std::thread::sleep(Duration::from_millis(500));
        Ok(HashMap::new())
    });
    let orch = test_orchestrator(db, Arc::new(git));

    poll_project_within(
        &orch,
        "proj",
        Path::new("/repo"),
        &["main".to_string()],
        Duration::from_millis(50),
    )
    .await;

    let state = orch.base_branch_sync.snapshot("proj", "main").unwrap();
    assert_eq!(state.checked_at, None);
    let error = state.last_error.unwrap();
    assert_eq!(error.phase, Phase::Check);
    assert!(
        error.message.contains("did not answer"),
        "{}",
        error.message
    );
}

#[tokio::test]
async fn a_project_without_origin_is_recorded_as_such() {
    let db = crate::storage::migrated_test_db("default-branch-poll-no-origin.db").await;
    let mut git = MockGitClient::new();
    git.expect_remote_get_url()
        .returning(|_| Err("No such remote 'origin'".into()));
    git.expect_ls_remote_branches().never();
    let orch = test_orchestrator(db, Arc::new(git));

    poll_project(&orch, "proj", Path::new("/repo"), &["main".to_string()]).await;

    assert!(
        orch.base_branch_sync
            .snapshot("proj", "main")
            .unwrap()
            .no_origin
    );
}

// ---------------------------------------------------------------------------
// Fetch serialization.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_fetches_of_one_repository_never_overlap() {
    let db = crate::storage::migrated_test_db("default-branch-fetch-serial.db").await;
    let inside = Arc::new(AtomicBool::new(false));
    let fetches = Arc::new(AtomicUsize::new(0));
    let mut git = MockGitClient::new();
    {
        let inside = inside.clone();
        let fetches = fetches.clone();
        git.expect_fetch_origin_branch().returning(move |_, _| {
            assert!(
                !inside.swap(true, Ordering::SeqCst),
                "two fetches of one repository overlapped"
            );
            std::thread::sleep(Duration::from_millis(40));
            fetches.fetch_add(1, Ordering::SeqCst);
            inside.store(false, Ordering::SeqCst);
            Ok(())
        });
    }
    let orch = test_orchestrator(db, Arc::new(git));
    let repo = Path::new("/repo");
    let (a, b, c) = tokio::join!(
        fetch_origin_serialized(&orch, repo, "main", None),
        fetch_origin_serialized(&orch, repo, "main", None),
        fetch_origin_serialized(&orch, repo, "main", None),
    );
    a.unwrap();
    b.unwrap();
    c.unwrap();
    assert_eq!(fetches.load(Ordering::SeqCst), 3);
}

/// The work handed to `with_fetch_lock` must not start until the lock is held.
/// A `spawn_blocking` handle passed in directly would already be running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fetch_work_starts_only_once_the_lock_is_held() {
    let db = crate::storage::migrated_test_db("default-branch-fetch-lazy.db").await;
    let orch = test_orchestrator(db, Arc::new(MockGitClient::new()));
    let store = PathBuf::from("/store");
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let holder = {
        let orch = orch.clone();
        let store = store.clone();
        tokio::spawn(async move {
            with_fetch_lock(&orch, &store, || async move {
                held_tx.send(()).unwrap();
                release_rx.await.unwrap();
            })
            .await
        })
    };
    held_rx.await.unwrap();

    let started = Arc::new(AtomicBool::new(false));
    let waiter = {
        let orch = orch.clone();
        let store = store.clone();
        let started = started.clone();
        tokio::spawn(async move {
            with_fetch_lock(&orch, &store, || {
                tokio::task::spawn_blocking(move || started.store(true, Ordering::SeqCst))
            })
            .await
            .unwrap()
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !started.load(Ordering::SeqCst),
        "fetch work ran while another fetch held the lock"
    );
    release_tx.send(()).unwrap();
    holder.await.unwrap();
    waiter.await.unwrap();
    assert!(started.load(Ordering::SeqCst));
}

/// A fetch on the default branch's behalf touches the default branch's tracking
/// ref and nothing else. A whole-remote fetch that received a lagging
/// advertisement once rewrote two agent branches' tracking refs backwards.
#[tokio::test]
async fn a_default_branch_fetch_leaves_agent_branch_tracking_refs_alone() {
    use crate::jj::tests::{git, git_stdout, init_project};

    let origin = tempfile::tempdir().unwrap();
    let proj = tempfile::tempdir().unwrap();
    git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
    init_project(proj.path());
    let origin_url = origin.path().to_string_lossy().into_owned();
    git(proj.path(), &["remote", "add", "origin", &origin_url]);
    git(proj.path(), &["push", "-q", "origin", "main"]);
    let base = git_stdout(proj.path(), &["rev-parse", "HEAD"]);

    // Origin holds an agent branch one commit ahead of base, and main moves on.
    std::fs::write(proj.path().join("agent.rs"), "agent\n").unwrap();
    git(proj.path(), &["add", "-A"]);
    git(proj.path(), &["commit", "-q", "-m", "agent work"]);
    git(
        proj.path(),
        &["push", "-q", "origin", "HEAD:refs/heads/agent/x"],
    );
    git(proj.path(), &["checkout", "-q", "--detach", &base]);
    std::fs::write(proj.path().join("main.rs"), "main\n").unwrap();
    git(proj.path(), &["add", "-A"]);
    git(proj.path(), &["commit", "-q", "-m", "main advance"]);
    let advanced = git_stdout(proj.path(), &["rev-parse", "HEAD"]);
    git(proj.path(), &["push", "-q", "origin", "HEAD:main"]);

    // The local tracking refs disagree with origin on both branches.
    git(
        proj.path(),
        &["update-ref", "refs/remotes/origin/main", &base],
    );
    git(
        proj.path(),
        &["update-ref", "refs/remotes/origin/agent/x", &base],
    );

    let db = crate::storage::migrated_test_db("default-branch-fetch-scope.db").await;
    let orch = test_orchestrator(db, Arc::new(crate::services::RealGitClient));
    fetch_origin_serialized(&orch, proj.path(), "main", None)
        .await
        .unwrap();

    assert_eq!(
        git_stdout(proj.path(), &["rev-parse", "refs/remotes/origin/main"]),
        advanced,
        "the default branch is fetched"
    );
    assert_eq!(
        git_stdout(proj.path(), &["rev-parse", "refs/remotes/origin/agent/x"]),
        base,
        "an agent branch's tracking ref is not the default-branch fetch's to write"
    );
}

#[tokio::test]
async fn a_recorded_fetch_failure_is_a_check_error() {
    let db = crate::storage::migrated_test_db("default-branch-fetch-error.db").await;
    let mut git = MockGitClient::new();
    git.expect_fetch_origin_branch()
        .returning(|_, _| Err("git fetch origin failed: offline".into()));
    let orch = test_orchestrator(db, Arc::new(git));
    let error = fetch_origin_serialized(
        &orch,
        Path::new("/repo"),
        "main",
        Some(FetchRecord {
            project_id: "proj",
            source: Source::Fetch,
        }),
    )
    .await
    .unwrap_err();
    assert!(error.contains("offline"));
    let state = orch.base_branch_sync.snapshot("proj", "main").unwrap();
    assert_eq!(state.checked_at, None);
    assert_eq!(state.last_error.unwrap().phase, Phase::Check);
}

// ---------------------------------------------------------------------------
// End to end: an advance made outside Cairn reaches the store on one poll.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn one_poll_pass_brings_an_outside_push_into_the_store() {
    if crate::jj::tests::jj_bin().is_none() {
        eprintln!(
            "skipping one_poll_pass_brings_an_outside_push_into_the_store: jj not resolvable"
        );
        return;
    }
    use crate::jj::tests::{git, git_stdout, init_project};

    let origin = tempfile::tempdir().unwrap();
    let proj = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
    init_project(proj.path());
    let origin_url = origin.path().to_string_lossy().into_owned();
    git(proj.path(), &["remote", "add", "origin", &origin_url]);
    git(proj.path(), &["push", "-q", "origin", "main"]);
    let repo_path = proj.path().to_string_lossy().into_owned();

    let db = crate::storage::migrated_test_db("default-branch-e2e.db").await;
    seed_project(&db, "proj-e2e", &repo_path).await;
    let orch = test_orchestrator(db, Arc::new(crate::services::RealGitClient));
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
    crate::jj::ensure_project_store(&jj, &store, Path::new(&repo_path)).unwrap();
    let before = crate::jj::bookmark_commit(&jj, &store, "main").unwrap();

    // Someone merges on GitHub: origin moves, and Cairn is told nothing.
    git(
        outside.path(),
        &[
            "clone",
            "-q",
            &origin_url,
            &outside.path().join("c").to_string_lossy(),
        ],
    );
    let clone = outside.path().join("c");
    git(&clone, &["config", "user.email", "o@e.com"]);
    git(&clone, &["config", "user.name", "o"]);
    std::fs::write(clone.join("landed.rs"), "landed\n").unwrap();
    git(&clone, &["add", "-A"]);
    git(&clone, &["commit", "-q", "-m", "merged outside Cairn (#2)"]);
    git(&clone, &["push", "-q", "origin", "main"]);
    let landed = git_stdout(&clone, &["rev-parse", "HEAD"]);

    // Before the poll the label already cannot claim more than it knows.
    let label = revision_label(&orch, "proj-e2e", "main", "main", &store, &before).await;
    assert!(label.contains("origin not checked since launch"), "{label}");

    poll_project(
        &orch,
        "proj-e2e",
        Path::new(&repo_path),
        &["main".to_string()],
    )
    .await;
    // The poll started the sync in the background; this waits for it, or runs
    // it if it has not begun.
    sync(&orch, "proj-e2e", "main").await;

    assert_eq!(
        crate::jj::bookmark_commit(&jj, &store, "main").as_deref(),
        Some(landed.as_str()),
        "one poll pass must bring an outside push into the store's main"
    );
    let label = revision_label(&orch, "proj-e2e", "main", "main", &store, &landed).await;
    assert!(
        label.starts_with(&format!("main@{} committed ", &landed[..8])),
        "{label}"
    );
    assert!(label.contains("origin checked "), "{label}");
    assert!(!label.contains("STALE"), "{label}");

    // A read that resolved `main` before the sync served the old tree. The
    // store is current now, but that read's label must still say STALE.
    let label = revision_label(&orch, "proj-e2e", "main", "main", &store, &before).await;
    assert!(
        label.starts_with(&format!(
            "main@{} STALE: origin main is {}",
            &before[..8],
            &landed[..8]
        )),
        "{label}"
    );

    // And a read still pinned to the old commit while origin is ahead says so.
    orch.base_branch_sync
        .record_tip("proj-e2e", "main", NEW, Source::Push);
    let label = revision_label(&orch, "proj-e2e", "main", "main", &store, &landed).await;
    assert!(
        label.starts_with(&format!(
            "main@{} STALE: origin main is 9e77a275",
            &landed[..8]
        )),
        "{label}"
    );
}

// ---------------------------------------------------------------------------
// End to end: a parent's integration branch advanced outside Cairn.
// ---------------------------------------------------------------------------

const PARENT: &str = "agent/proj-1-builder-0";
const CHILD: &str = "agent/proj-2-builder-0";

async fn seed_job(db: &LocalDb, id: &str, branch: &str, base_branch: &str) {
    db.execute(
        "INSERT INTO jobs (id, project_id, status, branch, base_branch, created_at, updated_at)
         VALUES (?1, 'proj-parent', 'running', ?2, ?3, 1, 1)",
        params![id, branch, base_branch],
    )
    .await
    .unwrap();
}

/// The specimen of cairn/5038: a child's pull request into its parent's
/// integration branch is merged in GitHub's UI, so origin's copy of the parent
/// moves and Cairn is told nothing. One poll pass must bring it into the store,
/// a child cut afterwards must start from the new tip, and the child already
/// cut from the old tip must be replayed onto it.
#[tokio::test(flavor = "multi_thread")]
async fn one_poll_pass_takes_an_outside_merge_into_a_parent_branch_and_replays_its_child() {
    if crate::jj::tests::jj_bin().is_none() {
        eprintln!("skipping: jj not resolvable");
        return;
    }
    use crate::jj::tests::{git, git_stdout, init_project};

    let origin = tempfile::tempdir().unwrap();
    let proj = tempfile::tempdir().unwrap();
    let workspaces = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
    init_project(proj.path());
    let origin_url = origin.path().to_string_lossy().into_owned();
    git(proj.path(), &["remote", "add", "origin", &origin_url]);
    git(proj.path(), &["push", "-q", "origin", "main"]);
    let repo_path = proj.path().to_string_lossy().into_owned();

    let db = crate::storage::migrated_test_db("base-branch-parent-e2e.db").await;
    seed_project(&db, "proj-parent", &repo_path).await;
    let orch = test_orchestrator(db, Arc::new(crate::services::RealGitClient));
    let db = orch.db.local.clone();
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
    crate::jj::ensure_project_store(&jj, &store, Path::new(&repo_path)).unwrap();

    // The parent's integration branch, published, and a child cut from it.
    let seal = |workspace: &Path, branch: &str, file: &str| {
        std::fs::write(workspace.join(file), format!("{file}\n")).unwrap();
        crate::jj::seal(&jj, workspace, &format!("edit {file}"), None).unwrap();
        let tip = crate::jj::bookmark_commit(&jj, &store, branch).unwrap();
        crate::jj::export_git_verified(&jj, &store, true, "fixture", &[(branch, tip.as_str())])
            .unwrap();
        tip
    };
    let parent_ws = workspaces.path().join("parent");
    crate::jj::add_workspace(&jj, &store, &parent_ws, PARENT, "main", None).unwrap();
    let parent_tip = seal(&parent_ws, PARENT, "parent.rs");
    crate::jj::push_store_bookmark(&jj, &store, PARENT).unwrap();
    let child_ws = workspaces.path().join("child");
    crate::jj::add_workspace(&jj, &store, &child_ws, CHILD, PARENT, None).unwrap();
    let child_tip = seal(&child_ws, CHILD, "child.rs");
    seed_job(&db, "parent", PARENT, "main").await;
    seed_job(&db, "child", CHILD, PARENT).await;

    assert_eq!(
        crate::orchestrator::base_advance::load_tracked_base_branches(&db, "proj-parent", "main")
            .await
            .unwrap(),
        vec![PARENT.to_string()],
        "a branch live work is cut from is tracked"
    );

    // A sibling's pull request into the parent is merged in GitHub's UI.
    let clone = outside.path().join("c");
    git(
        outside.path(),
        &[
            "clone",
            "-q",
            "--branch",
            PARENT,
            &origin_url,
            &clone.to_string_lossy(),
        ],
    );
    git(&clone, &["config", "user.email", "o@e.com"]);
    git(&clone, &["config", "user.name", "o"]);
    std::fs::write(clone.join("step3.rs"), "step 3\n").unwrap();
    git(&clone, &["add", "-A"]);
    git(
        &clone,
        &["commit", "-q", "-m", "Merge pull request #26 (step 3)"],
    );
    git(
        &clone,
        &["push", "-q", "origin", &format!("HEAD:refs/heads/{PARENT}")],
    );
    let landed = git_stdout(&clone, &["rev-parse", "HEAD"]);

    // While origin is known to be ahead, a read of the parent says STALE.
    orch.base_branch_sync
        .record_tip("proj-parent", PARENT, &landed, Source::Push);
    let label = revision_label(&orch, "proj-parent", PARENT, "main", &store, &parent_tip).await;
    assert!(
        label.starts_with(&format!(
            "{PARENT}@{} STALE: origin {PARENT} is {}",
            &parent_tip[..8],
            &landed[..8]
        )),
        "{label}"
    );
    let drift = known_drift(&orch, "proj-parent", &store, PARENT)
        .await
        .expect("the store is known to be behind");
    assert_eq!(drift.remote, landed);
    assert_eq!(drift.local.as_deref(), Some(parent_tip.as_str()));

    poll_project(
        &orch,
        "proj-parent",
        Path::new(&repo_path),
        &["main".to_string(), PARENT.to_string()],
    )
    .await;
    sync(&orch, "proj-parent", PARENT).await;

    assert_eq!(
        crate::jj::bookmark_commit(&jj, &store, PARENT).as_deref(),
        Some(landed.as_str()),
        "one poll pass must bring the outside merge into the store's parent branch"
    );
    assert_eq!(
        known_drift(&orch, "proj-parent", &store, PARENT).await,
        None
    );
    let label = revision_label(&orch, "proj-parent", PARENT, "main", &store, &landed).await;
    assert!(label.contains("origin checked "), "{label}");
    assert!(!label.contains("STALE"), "{label}");

    // A child cut now starts from the new tip.
    let git_rev_parse = |revision: &str| {
        crate::services::GitClient::rev_parse(
            &crate::services::RealGitClient,
            Path::new(&repo_path),
            vec![revision.to_string()],
        )
        .ok()
        .filter(|sha| !sha.is_empty())
    };
    assert_eq!(
        crate::jj::resolve_base_rev(&jj, &store, PARENT, git_rev_parse),
        landed,
        "a child created after the sync is cut from the new tip"
    );

    // And the child cut from the old tip is replayed onto it.
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let tip = crate::jj::bookmark_commit(&jj, &store, CHILD).unwrap();
        if tip != child_tip && crate::jj::revset_descends_from(&jj, &store, &tip, &landed) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the existing child was never replayed onto the advanced parent"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ---------------------------------------------------------------------------
// Publication against origin's copy of a store-owned branch.
// ---------------------------------------------------------------------------

/// A published parent integration branch with a child cut from it, so the
/// parent is a tracked base branch.
struct PublishedParent {
    _dirs: Vec<tempfile::TempDir>,
    orch: Orchestrator,
    jj: crate::jj::JjEnv,
    store: PathBuf,
    repo_path: String,
    origin: PathBuf,
    parent_ws: PathBuf,
    published: String,
}

impl PublishedParent {
    async fn new(db_name: &str) -> Option<Self> {
        crate::jj::tests::jj_bin()?;
        use crate::jj::tests::{git, init_project};
        let origin = tempfile::tempdir().unwrap();
        let proj = tempfile::tempdir().unwrap();
        let workspaces = tempfile::tempdir().unwrap();
        git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
        init_project(proj.path());
        let origin_url = origin.path().to_string_lossy().into_owned();
        git(proj.path(), &["remote", "add", "origin", &origin_url]);
        git(proj.path(), &["push", "-q", "origin", "main"]);
        let repo_path = proj.path().to_string_lossy().into_owned();

        let db = crate::storage::migrated_test_db(db_name).await;
        seed_project(&db, "proj-parent", &repo_path).await;
        let orch = test_orchestrator(db, Arc::new(crate::services::RealGitClient));
        let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
        let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
        crate::jj::ensure_project_store(&jj, &store, Path::new(&repo_path)).unwrap();

        let parent_ws = workspaces.path().join("parent");
        crate::jj::add_workspace(&jj, &store, &parent_ws, PARENT, "main", None).unwrap();
        let fixture = Self {
            orch,
            jj,
            store,
            repo_path,
            origin: origin.path().to_path_buf(),
            parent_ws,
            published: String::new(),
            _dirs: vec![origin, proj, workspaces],
        };
        let published = fixture.seal("parent.rs");
        crate::jj::push_store_bookmark(&fixture.jj, &fixture.store, PARENT).unwrap();
        let child_ws = fixture._dirs[2].path().join("child");
        crate::jj::add_workspace(&fixture.jj, &fixture.store, &child_ws, CHILD, PARENT, None)
            .unwrap();
        let db = fixture.orch.db.local.clone();
        seed_job(&db, "parent", PARENT, "main").await;
        seed_job(&db, "child", CHILD, PARENT).await;
        Some(Self {
            published,
            ..fixture
        })
    }

    /// Seal a new file onto the parent's branch in the store, exported.
    fn seal(&self, file: &str) -> String {
        std::fs::write(self.parent_ws.join(file), format!("{file}\n")).unwrap();
        crate::jj::seal(&self.jj, &self.parent_ws, &format!("edit {file}"), None).unwrap();
        let tip = crate::jj::bookmark_commit(&self.jj, &self.store, PARENT).unwrap();
        crate::jj::export_git_verified(&self.jj, &self.store, true, "fixture", &[(PARENT, &tip)])
            .unwrap();
        tip
    }

    /// Push a commit onto origin's `branch` from a clone Cairn knows nothing of.
    fn push_outside(&self, branch: &str, file: &str) -> String {
        use crate::jj::tests::{git, git_stdout};
        let outside = tempfile::tempdir().unwrap();
        let clone = outside.path().join("c");
        git(
            outside.path(),
            &[
                "clone",
                "-q",
                "--branch",
                branch,
                &self.origin.to_string_lossy(),
                &clone.to_string_lossy(),
            ],
        );
        git(&clone, &["config", "user.email", "o@e.com"]);
        git(&clone, &["config", "user.name", "o"]);
        std::fs::write(clone.join(file), format!("{file}\n")).unwrap();
        git(&clone, &["add", "-A"]);
        git(&clone, &["commit", "-q", "-m", "pushed outside Cairn"]);
        git(
            &clone,
            &["push", "-q", "origin", &format!("HEAD:refs/heads/{branch}")],
        );
        git_stdout(&clone, &["rev-parse", "HEAD"])
    }

    fn origin_tip(&self, branch: &str) -> String {
        crate::jj::tests::git_stdout(
            &self.origin,
            &["rev-parse", &format!("refs/heads/{branch}")],
        )
    }

    fn store_tip(&self) -> String {
        crate::jj::bookmark_commit(&self.jj, &self.store, PARENT).unwrap()
    }

    async fn publish(&self) -> Result<(), String> {
        crate::orchestrator::base_advance::publish_managed_branch(&self.orch, &self.store, PARENT)
            .await
    }
}

/// The specimen of sink report #398: `main` advances, base advance replays the
/// already-published parent onto it (every commit gets a new id), and origin
/// still holds the pre-rewrite tip, which the poll has just confirmed. That tip
/// is the store's own replaced publication, not drift: the replay's publication
/// goes ahead, as a sideways move leased on exactly that tip.
#[tokio::test(flavor = "multi_thread")]
async fn a_base_advance_replay_of_a_published_parent_is_published_over_its_old_tip() {
    let Some(f) = PublishedParent::new("base-advance-republish.db").await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    poll_project(
        &f.orch,
        "proj-parent",
        Path::new(&f.repo_path),
        &["main".to_string(), PARENT.to_string()],
    )
    .await;
    assert_eq!(
        f.orch
            .base_branch_sync
            .snapshot("proj-parent", PARENT)
            .and_then(|state| state.remote_tip),
        Some(f.published.clone()),
        "the poll knows origin's parent is at the published tip"
    );

    let main = f.push_outside("main", "main-advance.rs");
    sync(&f.orch, "proj-parent", "main").await;

    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    while f.origin_tip(PARENT) == f.published {
        if std::time::Instant::now() >= deadline {
            panic!(
                "the replayed parent never reached origin (store at {}); publishing now says: {:?}",
                f.store_tip(),
                f.publish().await
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let tip = f.store_tip();
    assert_ne!(tip, f.published, "the parent was replayed");
    assert!(crate::jj::revset_descends_from(
        &f.jj, &f.store, &tip, &main
    ));
    assert_eq!(f.origin_tip(PARENT), tip, "origin took the replay sideways");
    assert_eq!(
        known_drift(&f.orch, "proj-parent", &f.store, PARENT).await,
        None
    );
    assert_eq!(
        f.orch
            .base_branch_sync
            .snapshot("proj-parent", PARENT)
            .and_then(|state| state.last_error),
        None,
        "no sync recorded the replaced publication as a divergence"
    );
}

/// cairn/5038's guarantee: origin holding a commit the store never had (a
/// push or a UI merge into the branch) while the store holds its own unpublished
/// work is refused, naming both commits, and origin keeps the outside commit.
#[tokio::test(flavor = "multi_thread")]
async fn publishing_over_a_commit_the_store_never_had_is_refused() {
    let Some(f) = PublishedParent::new("publish-refuses-foreign.db").await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    let local = f.seal("unpublished.rs");
    let outside = f.push_outside(PARENT, "outside.rs");
    f.orch
        .base_branch_sync
        .record_tip("proj-parent", PARENT, &outside, Source::Push);

    let error = f.publish().await.unwrap_err();
    assert!(
        error.starts_with(&format!("Refusing to publish `{PARENT}`")),
        "{error}"
    );
    assert!(
        error.contains(&local[..8]) && error.contains(&outside[..8]),
        "{error}"
    );
    assert_eq!(
        f.origin_tip(PARENT),
        outside,
        "origin keeps the outside commit"
    );
    assert_eq!(f.store_tip(), local, "the store's work is untouched");
}

/// And when origin's extra commit is a pure fast-forward, the publication's
/// re-check takes it and goes ahead.
#[tokio::test(flavor = "multi_thread")]
async fn publishing_takes_an_outside_fast_forward_first() {
    let Some(f) = PublishedParent::new("publish-takes-fast-forward.db").await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    let outside = f.push_outside(PARENT, "outside.rs");
    f.orch
        .base_branch_sync
        .record_tip("proj-parent", PARENT, &outside, Source::Push);

    f.publish().await.unwrap();
    assert_eq!(f.store_tip(), outside);
    assert_eq!(f.origin_tip(PARENT), outside);
}
