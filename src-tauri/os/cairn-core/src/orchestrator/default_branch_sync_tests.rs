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
        state: Some(DefaultBranchSync {
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
    let mut state = DefaultBranchSync {
        checked_at: Some(at(9_990)),
        remote_tip: Some(NEW.into()),
        sync_started_at: Some(at(10_000 - 100)),
        ..Default::default()
    };
    let label = |state: &DefaultBranchSync| {
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
        state: Some(DefaultBranchSync {
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
        state: Some(DefaultBranchSync {
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
        state: Some(DefaultBranchSync {
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
        state: Some(DefaultBranchSync {
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
    let registry = DefaultBranchSyncRegistry::default();
    let passes = AtomicUsize::new(0);
    let pass = || async {
        passes.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    };
    let caught_up = || async { false };
    tokio::join!(
        drive(&registry, "p", pass, caught_up),
        drive(&registry, "p", pass, caught_up),
        drive(&registry, "p", pass, caught_up),
    );
    assert_eq!(passes.load(Ordering::SeqCst), 1);
    assert!(!registry.in_flight("p"));
    assert_eq!(registry.snapshot("p").unwrap().sync_started_at, None);
}

#[tokio::test]
async fn a_tip_observed_mid_sync_earns_one_follow_up_pass() {
    let registry = DefaultBranchSyncRegistry::default();
    let passes = AtomicUsize::new(0);
    let moved_again = AtomicBool::new(true);
    drive(
        &registry,
        "p",
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
    let registry = DefaultBranchSyncRegistry::default();
    let passes = AtomicUsize::new(0);
    drive(
        &registry,
        "p",
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
    let error = registry.snapshot("p").unwrap().last_error.unwrap();
    assert_eq!(error.phase, Phase::Sync);
    assert_eq!(error.message, "git fetch origin failed: offline");

    drive(&registry, "p", || async { Ok(()) }, || async { false }).await;
    assert_eq!(registry.snapshot("p").unwrap().last_error, None);
}

#[tokio::test]
async fn a_panicking_pass_does_not_wedge_the_project() {
    let registry = Arc::new(DefaultBranchSyncRegistry::default());
    let panicking = registry.clone();
    let joined = tokio::spawn(async move {
        drive(
            &panicking,
            "p",
            || async { panic!("pass blew up") },
            || async { false },
        )
        .await;
    })
    .await;
    assert!(joined.is_err());
    assert!(!registry.in_flight("p"));
    let ran = AtomicBool::new(false);
    drive(
        &registry,
        "p",
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
    let registry = DefaultBranchSyncRegistry::default();
    registry.record_error("p", Phase::Check, "offline", at(1));
    registry.record_check("p", Some(NEW.into()), Source::Poll, at(2));
    let state = registry.snapshot("p").unwrap();
    assert_eq!(state.last_error, None);
    assert_eq!(state.remote_tip.as_deref(), Some(NEW));
    assert_eq!(state.remote_tip_source, Some(Source::Poll));

    registry.record_error("p", Phase::Sync, "import failed", at(3));
    registry.record_check("p", None, Source::Fetch, at(4));
    let state = registry.snapshot("p").unwrap();
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
    git.expect_ls_remote_branch()
        .returning(|_, _, _| Err("git ls-remote origin refs/heads/main failed: offline".into()));
    let orch = test_orchestrator(db, Arc::new(git));
    orch.default_branch_sync
        .record_check("proj", Some(OLD.into()), Source::Poll, at(100));

    poll_project(&orch, "proj", Path::new("/repo"), "main").await;

    let state = orch.default_branch_sync.snapshot("proj").unwrap();
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
    git.expect_ls_remote_branch()
        .withf(|repo, _, _| repo == Path::new("/repos/hung"))
        .returning(|_, _, _| {
            std::thread::sleep(Duration::from_secs(3));
            Ok(None)
        });
    git.expect_ls_remote_branch()
        .withf(|repo, _, _| repo == Path::new("/repos/healthy"))
        .returning(|_, _, _| Ok(None));
    let orch = test_orchestrator(db, Arc::new(git));

    let started = std::time::Instant::now();
    poll_once(&orch).await;
    loop {
        if orch
            .default_branch_sync
            .snapshot("healthy")
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
        !orch.default_branch_sync.begin_poll("hung"),
        "the hung project's check is still in flight, so the next tick skips it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_check_that_outlives_its_budget_is_recorded_as_failed() {
    let db = crate::storage::migrated_test_db("default-branch-poll-timeout.db").await;
    let mut git = MockGitClient::new();
    git.expect_remote_get_url()
        .returning(|_| Ok("git@github.com:acme/repo.git".into()));
    git.expect_ls_remote_branch().returning(|_, _, _| {
        std::thread::sleep(Duration::from_millis(500));
        Ok(None)
    });
    let orch = test_orchestrator(db, Arc::new(git));

    poll_project_within(
        &orch,
        "proj",
        Path::new("/repo"),
        "main",
        Duration::from_millis(50),
    )
    .await;

    let state = orch.default_branch_sync.snapshot("proj").unwrap();
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
    git.expect_ls_remote_branch().never();
    let orch = test_orchestrator(db, Arc::new(git));

    poll_project(&orch, "proj", Path::new("/repo"), "main").await;

    assert!(orch.default_branch_sync.snapshot("proj").unwrap().no_origin);
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
        git.expect_fetch_origin().returning(move |_| {
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
        fetch_origin_serialized(&orch, repo, None),
        fetch_origin_serialized(&orch, repo, None),
        fetch_origin_serialized(&orch, repo, None),
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

#[tokio::test]
async fn a_recorded_fetch_failure_is_a_check_error() {
    let db = crate::storage::migrated_test_db("default-branch-fetch-error.db").await;
    let mut git = MockGitClient::new();
    git.expect_fetch_origin()
        .returning(|_| Err("git fetch origin failed: offline".into()));
    let orch = test_orchestrator(db, Arc::new(git));
    let error = fetch_origin_serialized(
        &orch,
        Path::new("/repo"),
        Some(FetchRecord {
            project_id: "proj",
            default_branch: "main",
            source: Source::Fetch,
        }),
    )
    .await
    .unwrap_err();
    assert!(error.contains("offline"));
    let state = orch.default_branch_sync.snapshot("proj").unwrap();
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

    poll_project(&orch, "proj-e2e", Path::new(&repo_path), "main").await;
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
    orch.default_branch_sync
        .record_tip("proj-e2e", NEW, Source::Push);
    let label = revision_label(&orch, "proj-e2e", "main", "main", &store, &landed).await;
    assert!(
        label.starts_with(&format!(
            "main@{} STALE: origin main is 9e77a275",
            &landed[..8]
        )),
        "{label}"
    );
}
