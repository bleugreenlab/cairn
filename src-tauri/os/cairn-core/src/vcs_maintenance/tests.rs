use super::*;
use crate::jj::NamespaceRefs;

fn census(keep: u64, packed_bytes: u64) -> RefStoreCensus {
    let mut census = RefStoreCensus {
        packed_bytes,
        ..Default::default()
    };
    census.namespaces.insert(
        "jj/keep".into(),
        NamespaceRefs {
            packed: keep,
            loose: 0,
        },
    );
    census
}

#[test]
fn a_prune_is_due_daily_or_when_a_threshold_is_crossed() {
    let now = 10 * DAY as i64;
    let small = census(100, 1024);
    assert!(
        prune_is_due(&small, None, now),
        "a store never pruned is due"
    );
    assert!(!prune_is_due(&small, Some(now - 3600), now));
    assert!(prune_is_due(&small, Some(now - DAY as i64), now));
    assert!(
        prune_is_due(&census(KEEP_REF_THRESHOLD + 1, 1024), Some(now - 60), now),
        "too many keep refs makes a prune due at once"
    );
    assert!(prune_is_due(
        &census(100, PACKED_BYTES_THRESHOLD + 1),
        Some(now - 60),
        now
    ));
}

#[test]
fn pruning_is_the_default_and_only_a_large_first_prune_waits() {
    let small = census(100, 1024);
    let large = census(KEEP_REF_THRESHOLD + 1, 1024);
    assert_eq!(
        prune_gate(PrunePolicy::Default, false, &small),
        PruneGate::Open
    );
    assert_eq!(
        prune_gate(PrunePolicy::Default, false, &large),
        PruneGate::AwaitingConfirmation
    );
    assert_eq!(
        prune_gate(PrunePolicy::Default, true, &large),
        PruneGate::Open,
        "a store pruned once needs nothing for later growth"
    );
    assert_eq!(
        prune_gate(PrunePolicy::Confirmed, false, &large),
        PruneGate::Open
    );
    assert_eq!(
        prune_gate(PrunePolicy::OptedOut, true, &small),
        PruneGate::OptedOut
    );
}

fn objects(packs: u64, kept_packs: u64, loose_objects: u64) -> ObjectStoreCensus {
    ObjectStoreCensus {
        packs,
        kept_packs,
        loose_objects,
        ..Default::default()
    }
}

#[test]
fn compaction_is_due_past_gits_own_auto_gc_thresholds() {
    assert!(compaction_due(&objects(PACK_LIMIT + 1, 0, 0)));
    assert!(!compaction_due(&objects(PACK_LIMIT, 0, 0)));
    assert!(
        !compaction_due(&objects(50, 10, 0)),
        "kept packs do not count against the limit"
    );
    assert!(compaction_due(&objects(61, 10, 0)));
    assert!(compaction_due(&objects(1, 0, 6_701)));
    assert!(!compaction_due(&objects(1, 0, LOOSE_OBJECT_LIMIT)));
}

#[test]
fn compaction_runs_on_every_store_except_an_opted_out_one() {
    let large = census(KEEP_REF_THRESHOLD + 1, 1024);
    let gate = |policy| prune_gate(policy, false, &large);
    assert_eq!(gate(PrunePolicy::Default), PruneGate::AwaitingConfirmation);
    assert!(
        compaction_allowed(gate(PrunePolicy::Default)),
        "compaction removes nothing, so it does not wait for the first-prune confirmation"
    );
    assert!(compaction_allowed(gate(PrunePolicy::Confirmed)));
    assert!(!compaction_allowed(gate(PrunePolicy::OptedOut)));
}

#[test]
fn a_reclaim_is_due_once_per_retention_window() {
    let now = 100 * DAY as i64;
    assert!(reclaim_is_due(None, now), "a store never reclaimed is due");
    assert!(!reclaim_is_due(Some(now - 29 * DAY as i64), now));
    assert!(reclaim_is_due(Some(now - 30 * DAY as i64), now));
}

#[test]
fn the_operators_git_busies_the_object_store_but_a_leftover_packed_refs_lock_does_not() {
    let temp = tempfile::TempDir::new().unwrap();
    let git_dir = temp.path();
    assert_eq!(object_store_busy(git_dir, SystemTime::now()), None);

    std::fs::write(git_dir.join("packed-refs.lock"), "").unwrap();
    // Read the clock after the write, so the lock's age is at least the offset.
    let now = SystemTime::now();
    let fresh = object_store_busy(git_dir, now + Duration::from_secs(60)).unwrap();
    assert_eq!(fresh, "packed-refs.lock (1m old)");
    assert_eq!(
        object_store_busy(git_dir, now + Duration::from_secs(11 * 60)),
        None,
        "a packed-refs.lock older than ten minutes is a leftover, not a writer"
    );

    std::fs::write(git_dir.join("gc.pid"), "123 host").unwrap();
    let now = SystemTime::now();
    let gc = object_store_busy(git_dir, now + Duration::from_secs(3 * 3600)).unwrap();
    assert_eq!(
        gc, "gc.pid (3h old)",
        "a gc that runs for hours is still never raced, and the skip names its age"
    );
    assert_eq!(
        object_store_busy(git_dir, now + Duration::from_secs(13 * 3600)),
        None,
        "git's own rule: a gc.pid older than twelve hours is a gc that died"
    );
    std::fs::remove_file(git_dir.join("gc.pid")).unwrap();
    std::fs::write(git_dir.join("index.lock"), "").unwrap();
    let now = SystemTime::now();
    assert!(object_store_busy(git_dir, now)
        .unwrap()
        .starts_with("index.lock"));
    assert_eq!(
        object_store_busy(git_dir, now + Duration::from_secs(11 * 60)),
        None,
        "a leftover index.lock does not block object maintenance for good"
    );
}

mod rows {
    use super::super::*;
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
    use tempfile::TempDir;

    async fn insert_prune(
        db: &LocalDb,
        id: &str,
        started_at: i64,
        outcome: &str,
        reclaimed: Option<i64>,
    ) {
        db.execute(
            "INSERT INTO ref_store_samples
               (id, project_id, store_path, kind, started_at, finished_at, outcome, duration_ms,
                reclaimed_bytes)
             VALUES (?1, 'p', '/stores/p', 'prune', ?2, ?2, ?3, 1, ?4)",
            params![id, started_at, outcome, reclaimed],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn only_a_prune_that_reclaimed_and_did_not_fail_counts_as_a_reclaim() {
        let temp = TempDir::new().unwrap();
        let db = LocalDb::open(temp.path().join("reclaim.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        let store = Path::new("/stores/p");
        assert_eq!(last_successful_reclaim(&db, store).await, None);

        insert_prune(&db, "reclaimed", 100, "ok", Some(5_000)).await;
        insert_prune(&db, "no-reclaim", 200, "ok", None).await;
        insert_prune(
            &db,
            "failed",
            300,
            "failed:git repack --cruft: no space",
            Some(0),
        )
        .await;
        assert_eq!(last_successful_reclaim(&db, store).await, Some(100));

        insert_prune(&db, "partial", 400, "partial:refused", Some(0)).await;
        assert_eq!(
            last_successful_reclaim(&db, store).await,
            Some(400),
            "a partial prune still reclaimed"
        );
        insert_prune(
            &db,
            "reclaim-failed",
            500,
            "partial:reclaim-failed:git repack --cruft: no space",
            None,
        )
        .await;
        assert_eq!(
            last_successful_reclaim(&db, store).await,
            Some(400),
            "a failed reclaim is retried at the next prune"
        );
        assert_eq!(
            last_successful_prune(&db, store, false).await,
            Some(500),
            "but the prune's sweep counts, so the prune is not repeated hourly"
        );
    }

    /// A prune that truncated and then failed only its reclaim did truncate, so
    /// the weekly truncation is not repeated at the next daily prune. A partial
    /// prune whose truncation was refused records no `ops_reparented` and never
    /// counts.
    #[tokio::test]
    async fn a_truncation_counts_when_only_the_reclaim_failed() {
        let temp = TempDir::new().unwrap();
        let db = LocalDb::open(temp.path().join("truncation.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        let store = Path::new("/stores/p");
        let insert = |id: &'static str, at: i64, outcome: &'static str, reparented: Option<i64>| {
            let db = &db;
            async move {
                db.execute(
                    "INSERT INTO ref_store_samples
                       (id, project_id, store_path, kind, started_at, finished_at, outcome,
                        duration_ms, ops_reparented)
                     VALUES (?1, 'p', '/stores/p', 'prune', ?2, ?2, ?3, 1, ?4)",
                    params![id, at, outcome, reparented],
                )
                .await
                .unwrap();
            }
        };
        insert("refused", 100, "partial:truncation refused", None).await;
        insert("failed", 200, "failed:git update-ref: boom", Some(5)).await;
        assert_eq!(last_successful_prune(&db, store, true).await, None);
        insert(
            "reclaim-failed",
            300,
            "partial:reclaim-failed:git repack --cruft: no space",
            Some(5),
        )
        .await;
        assert_eq!(last_successful_prune(&db, store, true).await, Some(300));
    }

    /// The rebuild that widens `kind` keeps every row a running installation
    /// already recorded, and then accepts a `compact` row.
    #[tokio::test]
    async fn the_object_store_migration_keeps_existing_rows_and_accepts_compactions() {
        let temp = TempDir::new().unwrap();
        let db = LocalDb::open(temp.path().join("migration.db"))
            .await
            .unwrap();
        // Select by name, since a sibling migration can renumber this one, and
        // filter by version rather than slicing: composition order is not
        // file-number order.
        let version = TURSO_MIGRATIONS
            .iter()
            .find(|migration| migration.name() == "object_store_maintenance")
            .expect("the object-store maintenance migration is registered")
            .version();
        let head: Vec<_> = TURSO_MIGRATIONS
            .iter()
            .filter(|migration| migration.version() < version)
            .cloned()
            .collect();
        MigrationRunner::new(head).run(&db).await.unwrap();
        db.execute_batch(
            "INSERT INTO ref_store_samples
               (id, project_id, store_path, kind, started_at, finished_at, outcome,
                packed_bytes, packed_entries, loose_refs, ref_locks, duration_ms)
             VALUES ('m', 'p', '/stores/p', 'measure', 1, 1, 'ok', 10, 2, 3, 0, 5);
             INSERT INTO ref_store_samples
               (id, project_id, store_path, kind, started_at, finished_at, outcome,
                ops_reparented, detail_json, duration_ms)
             VALUES ('p', 'p', '/stores/p', 'prune', 2, 2, 'ok', 7, '{}', 9);",
        )
        .await
        .unwrap();

        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();

        let kept = db
            .query_all(
                "SELECT id, kind, outcome, packed_entries, ops_reparented, packs
                 FROM ref_store_samples ORDER BY id",
                (),
                |row| {
                    Ok((
                        row.text(0)?,
                        row.text(1)?,
                        row.text(2)?,
                        row.opt_i64(3)?,
                        row.opt_i64(4)?,
                        row.opt_i64(5)?,
                    ))
                },
            )
            .await
            .unwrap();
        assert_eq!(
            kept,
            vec![
                (
                    "m".into(),
                    "measure".into(),
                    "ok".into(),
                    Some(2),
                    None,
                    None
                ),
                ("p".into(), "prune".into(), "ok".into(), None, Some(7), None),
            ]
        );
        db.execute(
            "INSERT INTO ref_store_samples
               (id, project_id, store_path, kind, started_at, finished_at, outcome, duration_ms,
                packs, kept_packs, pack_bytes, loose_objects, multi_pack_index)
             VALUES ('c', 'p', '/stores/p', 'compact', 3, 3, 'ok', 1, 2, 1, 100, 0, 1)",
            (),
        )
        .await
        .expect("a compact row is accepted");
        assert!(db
            .execute(
                "INSERT INTO ref_store_samples
                   (id, project_id, store_path, kind, started_at, finished_at, outcome,
                    duration_ms)
                 VALUES ('x', 'p', '/stores/p', 'gc', 3, 3, 'ok', 1)",
                (),
            )
            .await
            .is_err());
    }
}

#[test]
fn stranded_refs_name_their_job_only_in_the_two_segment_form() {
    assert_eq!(
        stranded_ref_job("refs/cairn/stranded/job-1/attempt-2"),
        Some("job-1")
    );
    assert_eq!(stranded_ref_job("refs/cairn/stranded/attempt-2"), None);
    assert_eq!(stranded_ref_job("refs/cairn/checks/job-1/attempt-2"), None);
}

#[test]
fn rescued_refs_carry_their_capture_time() {
    assert_eq!(
        rescued_ref_millis("refs/cairn/rescued/attempt-rescue/1786899898763"),
        Some(1_786_899_898_763)
    );
    assert_eq!(
        rescued_ref_millis("refs/cairn/rescued/label/not-a-time"),
        None
    );
}

/// The record shapes the live database carries: a pull request whose two
/// status sources disagree is terminal if either says so, a draft never
/// published to GitHub follows its issue, and anything not done holds.
#[test]
fn a_branch_is_finished_only_by_records_with_nothing_still_holding_it() {
    let merged = Some(("merged", 100));
    let finish = |build: &dyn Fn(&mut BranchLifecycle)| {
        let mut lifecycle = BranchLifecycle::default();
        build(&mut lifecycle);
        lifecycle.finished_by(1_000)
    };
    assert!(finish(&|b| b.job("complete", merged)));
    assert!(finish(&|b| b.job("cancelled", Some(("closed", 100)))));
    assert!(!finish(&|b| b.job("complete", Some(("active", 100)))));
    assert!(
        !finish(&|b| b.job("idle", merged)),
        "a node not done holds it"
    );
    assert!(
        !finish(&|b| b.job("complete", Some(("merged", 2_000)))),
        "inside the grace"
    );
    assert!(!finish(&|_| {}), "no record, no finish");

    assert!(finish(&|b| b.pull_request(
        "open",
        Some("MERGED"),
        true,
        100,
        merged
    )));
    assert!(finish(&|b| b.pull_request(
        "merged",
        Some("OPEN"),
        true,
        100,
        merged
    )));
    assert!(finish(&|b| b.pull_request(
        "open",
        None,
        false,
        100,
        Some(("closed", 100))
    )));
    assert!(finish(&|b| b.pull_request("closed", None, true, 100, None)));
    assert!(
        !finish(&|b| b.pull_request("open", Some("OPEN"), true, 100, merged)),
        "a published pull request still open holds it"
    );
    assert!(!finish(&|b| {
        b.job("complete", merged);
        b.pull_request("open", None, true, 100, merged);
    }));
}

mod pass {
    use super::super::*;
    use crate::db::DbState;
    use crate::jj::tests::{git, git_stdout, init_project, jj_bin};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct Fixture {
        _temp: TempDir,
        orch: Orchestrator,
        repo: PathBuf,
        project: ProjectStore,
        commit: String,
    }

    async fn fixture() -> Option<Fixture> {
        let bin = jj_bin()?;
        let temp = TempDir::new().unwrap();
        let config_dir = temp.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let local = LocalDb::open_migrated_for_test(config_dir.join("maintenance.db"))
            .await
            .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config_dir.clone(),
        )
        .jj_binary_path(bin.clone())
        .build();

        let repo = temp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_project(&repo);
        let store = crate::jj::project_store_dir(&config_dir, &repo);
        let jj = crate::jj::JjEnv::resolve(&bin, &config_dir);
        crate::jj::ensure_project_store(&jj, &store, &repo).unwrap();
        orch.db
            .local
            .execute(
                "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('p-refs', 'default', 'Refs', 'REFS', ?1, 1, 1)",
                params![repo.to_string_lossy().into_owned()],
            )
            .await
            .unwrap();
        let commit = git_stdout(&repo, &["rev-parse", "HEAD"]);
        Some(Fixture {
            _temp: temp,
            orch,
            repo,
            project: ProjectStore {
                project_id: "p-refs".into(),
                store,
                base_branches: vec!["main".into()],
            },
            commit,
        })
    }

    impl Fixture {
        fn create_ref(&self, name: &str) {
            self.create_ref_at(name, &self.commit);
        }

        fn create_ref_at(&self, name: &str, commit: &str) {
            git(&self.repo, &["update-ref", name, commit]);
        }

        /// An issue resolved as `status` at `resolved`, with one node on
        /// `branch` in `job_status`.
        async fn branch_record(
            &self,
            number: i64,
            branch: &str,
            status: &str,
            resolved: i64,
            job_status: &str,
        ) {
            let issue = format!("issue-{number}");
            let (merged_at, closed_at) = match status {
                "merged" => (Some(resolved), None),
                _ => (None, Some(resolved)),
            };
            self.orch
                .db
                .local
                .execute(
                    "INSERT INTO issues (id, project_id, number, title, status, progress, attention,
                                         created_at, updated_at, merged_at, closed_at)
                     VALUES (?1, 'p-refs', ?2, ?1, ?3, 'backlog', 'none', 1, ?4, ?5, ?6)",
                    params![issue.as_str(), number, status, resolved, merged_at, closed_at],
                )
                .await
                .unwrap();
            self.orch
                .db
                .local
                .execute(
                    "INSERT INTO jobs (id, project_id, issue_id, branch, status, created_at,
                                       updated_at)
                     VALUES (?1, 'p-refs', ?2, ?3, ?4, 1, 1)",
                    params![format!("job-{number}"), issue.as_str(), branch, job_status],
                )
                .await
                .unwrap();
        }

        fn has_ref(&self, name: &str) -> bool {
            cairn_platform::env::git()
                .args(["rev-parse", "--verify", "--quiet", name])
                .current_dir(&self.repo)
                .stdout(std::process::Stdio::null())
                .status()
                .unwrap()
                .success()
        }

        async fn issue_with_job(
            &self,
            number: i64,
            job: &str,
            status: &str,
            resolved: Option<i64>,
        ) {
            let issue = format!("issue-{number}");
            let issue = issue.as_str();
            self.orch
                .db
                .local
                .execute(
                    "INSERT INTO issues (id, project_id, number, title, status, progress, attention,
                                         created_at, updated_at, merged_at)
                     VALUES (?1, 'p-refs', ?2, ?1, ?3, 'backlog', 'none', 1, 1, ?4)",
                    params![issue, number, status, resolved],
                )
                .await
                .unwrap();
            self.orch
                .db
                .local
                .execute(
                    "INSERT INTO jobs (id, project_id, issue_id, created_at, updated_at)
                     VALUES (?1, 'p-refs', ?2, 1, 1)",
                    params![job, issue],
                )
                .await
                .unwrap();
        }

        async fn last_prune(&self) -> (String, Option<i64>) {
            self.orch
                .db
                .local
                .query_one(
                    "SELECT outcome, removed_cairn_refs FROM ref_store_samples
                     WHERE kind = 'prune' ORDER BY started_at DESC, rowid DESC LIMIT 1",
                    (),
                    |row| Ok((row.text(0)?, row.opt_i64(1)?)),
                )
                .await
                .unwrap()
        }
    }

    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn the_sweep_retires_finished_cairn_refs_and_keeps_live_ones() {
        let Some(fx) = fixture().await else {
            eprintln!("skipping ref-store sweep test: jj not resolvable");
            return;
        };
        let now_ms = unix_now() * 1000;
        let old_ms = now_ms - 40 * DAY as i64 * 1000;
        let resolved_long_ago = unix_now() - 40 * DAY as i64;

        // Packed refs carry no age of their own, so these are old.
        fx.create_ref("refs/cairn/checks/finished");
        fx.create_ref("refs/cairn/checks/held-by-a-batch");
        git(&fx.repo, &["pack-refs", "--all"]);
        crate::execution::checks::hold_check_ref("refs/cairn/checks/held-by-a-batch");
        // A loose ref written moments ago may belong to another process's batch.
        fx.create_ref("refs/cairn/checks/just-written");

        fx.issue_with_job(1, "job-open", "backlog", None).await;
        fx.issue_with_job(2, "job-done", "merged", Some(resolved_long_ago))
            .await;
        fx.create_ref("refs/cairn/stranded/job-open/attempt-1");
        fx.create_ref("refs/cairn/stranded/job-done/attempt-2");
        fx.create_ref("refs/cairn/stranded/legacy-attempt");

        fx.create_ref(&format!("refs/cairn/rescued/old/{old_ms}"));
        fx.create_ref(&format!("refs/cairn/rescued/new/{now_ms}"));

        prune(&fx.orch, &fx.project).await;

        let (outcome, removed) = fx.last_prune().await;
        assert_eq!(outcome, "ok");
        assert_eq!(removed, Some(3), "one finished ref from each namespace");
        assert!(!fx.has_ref("refs/cairn/checks/finished"));
        assert!(fx.has_ref("refs/cairn/checks/held-by-a-batch"));
        assert!(fx.has_ref("refs/cairn/checks/just-written"));
        assert!(fx.has_ref("refs/cairn/stranded/job-open/attempt-1"));
        assert!(!fx.has_ref("refs/cairn/stranded/job-done/attempt-2"));
        assert!(
            fx.has_ref("refs/cairn/stranded/legacy-attempt"),
            "a stranded ref nothing attributes is kept"
        );
        assert!(!fx.has_ref(&format!("refs/cairn/rescued/old/{old_ms}")));
        assert!(fx.has_ref(&format!("refs/cairn/rescued/new/{now_ms}")));
        assert!(fx.has_ref("refs/heads/main"));
    }

    /// The prune hides a commit nothing references once it is older than the
    /// grace window, and leaves the base and the operator's branches alone.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn the_prune_hides_an_old_commit_nothing_references() {
        let Some(fx) = fixture().await else {
            eprintln!("skipping the view-sweep prune test: jj not resolvable");
            return;
        };
        let jj = crate::jj::environment(&fx.orch.jj_binary_path, &fx.orch.config_dir);
        let jj_at = |args: &[&str], timestamp: Option<&str>| {
            let mut command = cairn_platform::env::command(jj.binary_path());
            command.current_dir(&fx.project.store).args(args);
            for (key, value) in jj.shell_env() {
                command.env(key, value);
            }
            if let Some(timestamp) = timestamp {
                command.env("JJ_TIMESTAMP", timestamp);
            }
            let output = command.output().unwrap();
            assert!(output.status.success(), "jj {args:?}: {output:?}");
            String::from_utf8_lossy(&output.stdout).trim().to_string()
        };
        jj_at(
            &[
                "new",
                "--no-edit",
                "-r",
                "main",
                "-m",
                "superseded long ago",
                "--ignore-working-copy",
            ],
            Some("2020-01-01T00:00:00+00:00"),
        );
        let stray_revset = "children(main) & description(substring:\"superseded long ago\")";
        let visible = |revset: &str| {
            jj_at(
                &[
                    "log",
                    "-r",
                    &format!("({revset}) & ::visible_heads()"),
                    "--no-graph",
                    "-T",
                    "commit_id",
                    "--ignore-working-copy",
                ],
                None,
            )
        };
        let stray = visible(stray_revset);
        assert!(
            !stray.is_empty(),
            "precondition: the stray commit is visible"
        );

        prune(&fx.orch, &fx.project).await;

        assert_eq!(fx.last_prune().await.0, "ok");
        assert_eq!(visible(&stray), "", "the unreferenced commit is hidden");
        assert_eq!(visible("main"), fx.commit, "the base is untouched");
        assert!(fx.has_ref("refs/heads/main"));
    }

    /// The prune retires the origin tracking ref of a branch Cairn's records
    /// finish, and the history only that ref held is hidden. A branch whose
    /// published pull request is still open, one a running node is on, and one
    /// no record names all keep their refs, though origin advertises none of
    /// them: the sweep never asks the remote.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn the_prune_retires_tracking_refs_only_of_branches_the_records_finish() {
        let Some(fx) = fixture().await else {
            eprintln!("skipping the tracking-ref retirement test: jj not resolvable");
            return;
        };
        let origin = fx._temp.path().join("origin.git");
        git(
            fx._temp.path(),
            &["init", "-q", "--bare", &origin.to_string_lossy()],
        );
        git(
            &fx.repo,
            &["remote", "add", "origin", &origin.to_string_lossy()],
        );

        let finished = "agent/refs-1-builder-0";
        let open_pr = "agent/refs-2-builder-0";
        let live = "agent/refs-3-builder-0";
        let unnamed = "agent/refs-4-builder-0";
        let tree = git_stdout(&fx.repo, &["rev-parse", "main^{tree}"]);
        let mut tips = std::collections::HashMap::new();
        for branch in [finished, open_pr, live, unnamed] {
            let output = cairn_platform::env::git()
                .args(["commit-tree", &tree, "-p", &fx.commit, "-m", branch])
                .env("GIT_AUTHOR_DATE", "2020-01-01T00:00:00+00:00")
                .env("GIT_COMMITTER_DATE", "2020-01-01T00:00:00+00:00")
                .current_dir(&fx.repo)
                .output()
                .unwrap();
            assert!(output.status.success(), "{output:?}");
            let tip = String::from_utf8_lossy(&output.stdout).trim().to_string();
            fx.create_ref_at(&format!("refs/remotes/origin/{branch}"), &tip);
            tips.insert(branch, tip);
        }
        let jj = crate::jj::environment(&fx.orch.jj_binary_path, &fx.orch.config_dir);
        crate::jj::import_git(&jj, &fx.project.store).unwrap();
        jj.run(
            &fx.project.store,
            &[
                "bookmark",
                "track",
                "glob:agent/*",
                "--remote=origin",
                "--ignore-working-copy",
            ],
            "track the pushed branches",
        )
        .unwrap();
        // Cairn deletes a branch's local bookmark when its work lands, so each
        // branch is held only by its origin tracking ref, as on the live store.
        jj.run(
            &fx.project.store,
            &[
                "bookmark",
                "delete",
                "glob:agent/*",
                "--ignore-working-copy",
            ],
            "delete the local bookmarks",
        )
        .unwrap();
        jj.run(
            &fx.project.store,
            &["git", "export", "--ignore-working-copy"],
            "export the deletions",
        )
        .unwrap();
        let visible = |commit: &str| {
            !jj.run(
                &fx.project.store,
                &[
                    "log",
                    "-r",
                    &format!("{commit} & ::visible_heads()"),
                    "--no-graph",
                    "-T",
                    "commit_id",
                    "--ignore-working-copy",
                ],
                "visible",
            )
            .unwrap()
            .trim()
            .is_empty()
        };
        assert!(visible(&tips[finished]), "precondition: origin anchors it");

        let long_ago = unix_now() - 40 * DAY as i64;
        fx.branch_record(1, finished, "merged", long_ago, "complete")
            .await;
        fx.branch_record(2, open_pr, "closed", long_ago, "complete")
            .await;
        fx.orch
            .db
            .local
            .execute(
                "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title,
                                             source_branch, target_branch, status, opened_at,
                                             updated_at, github_pr_number, github_state)
                 VALUES ('mr-2', 'job-2', 'p-refs', 'issue-2', 'open', ?1, 'main', 'open', 1, 1,
                         42, 'OPEN')",
                params![open_pr],
            )
            .await
            .unwrap();
        fx.branch_record(3, live, "merged", long_ago, "running")
            .await;

        prune(&fx.orch, &fx.project).await;

        assert_eq!(fx.last_prune().await.0, "ok");
        assert!(!fx.has_ref(&format!("refs/remotes/origin/{finished}")));
        assert!(
            !visible(&tips[finished]),
            "the history only it held is hidden"
        );
        for kept in [open_pr, live, unnamed] {
            assert!(fx.has_ref(&format!("refs/remotes/origin/{kept}")), "{kept}");
            assert!(visible(&tips[kept]), "{kept}");
        }
        let detail: serde_json::Value = serde_json::from_str(
            &fx.orch
                .db
                .local
                .query_one(
                    "SELECT detail_json FROM ref_store_samples WHERE kind = 'prune'
                     ORDER BY started_at DESC, rowid DESC LIMIT 1",
                    (),
                    |row| row.text(0),
                )
                .await
                .unwrap(),
        )
        .unwrap();
        let retired = &detail["origin_tracking_refs"];
        assert_eq!(retired["retired"], 1, "{detail}");
        assert_eq!(
            retired["refs"],
            json!([{
                "ref": format!("refs/remotes/origin/{finished}"),
                "oid": tips[finished],
            }]),
            "the record carries what re-creating the ref takes"
        );
        assert!(!retired["operation_before"].as_str().unwrap().is_empty());
        assert!(detail["view_sweep"]["commits_hidden"].as_u64().is_some());
    }

    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_pending_reconcile_intent_skips_the_prune_and_says_so() {
        let Some(fx) = fixture().await else {
            eprintln!("skipping ref-store reconcile skip test: jj not resolvable");
            return;
        };
        fx.create_ref("refs/cairn/checks/finished");
        git(&fx.repo, &["pack-refs", "--all"]);
        fx.orch
            .db
            .local
            .execute(
                "INSERT INTO jj_reconcile_intents
                   (id, project_id, store_path, target_branch, destination_commit, status,
                    created_at, updated_at)
                 VALUES ('intent', 'p-refs', ?1, 'agent/x', ?2, 'pending', 1, 1)",
                params![store_key(&fx.project.store), fx.commit.as_str()],
            )
            .await
            .unwrap();

        prune(&fx.orch, &fx.project).await;

        assert_eq!(
            fx.last_prune().await.0,
            "skipped:reconcile-in-flight",
            "the skip is recorded with its reason"
        );
        assert!(
            fx.has_ref("refs/cairn/checks/finished"),
            "a skipped prune changes nothing"
        );
    }

    /// A live foreign workspace refuses the truncation; the sweep still runs,
    /// and the prune is recorded as partial with the refusal, not as ok.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_refused_truncation_is_recorded_as_partial() {
        let Some(fx) = fixture().await else {
            eprintln!("skipping ref-store partial-prune test: jj not resolvable");
            return;
        };
        let jj = crate::jj::JjEnv::resolve(&jj_bin().unwrap(), fx._temp.path());
        let workspace = fx._temp.path().join("live-workspace");
        crate::jj::add_workspace(
            &jj,
            &fx.project.store,
            &workspace,
            "agent/live",
            "main",
            None,
        )
        .unwrap();
        fx.create_ref("refs/cairn/checks/finished");
        git(&fx.repo, &["pack-refs", "--all"]);

        prune(&fx.orch, &fx.project).await;

        let (outcome, removed) = fx.last_prune().await;
        assert!(outcome.starts_with("partial:"), "{outcome}");
        assert!(
            outcome.contains("live-workspace"),
            "names the workspace: {outcome}"
        );
        assert_eq!(removed, Some(1), "the sweep still ran");
        assert!(!fx.has_ref("refs/cairn/checks/finished"));
        let overview = ref_store_overview(&fx.orch, "p-refs", &fx.repo).await;
        assert!(
            overview.last_ok_prune_at.is_some(),
            "the daily sweep cadence holds"
        );
        assert_eq!(
            overview.last_truncation_at, None,
            "but nothing was truncated"
        );
    }

    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn the_operators_own_git_lock_skips_the_prune() {
        let Some(fx) = fixture().await else {
            eprintln!("skipping ref-store operator-busy test: jj not resolvable");
            return;
        };
        std::fs::write(fx.repo.join(".git/index.lock"), "").unwrap();
        prune(&fx.orch, &fx.project).await;
        assert_eq!(fx.last_prune().await.0, "skipped:operator-git-busy");
    }

    /// Grow the fixture repository by `count` packs, one commit each. An
    /// incremental `git repack` packs only objects a ref reaches, so each
    /// round commits what it packs.
    fn grow_packs(fx: &Fixture, count: usize) {
        // Past 50 packs every `git commit` would start a detached
        // `git gc --auto` that repacks everything behind the test's back:
        // the very repack the pack limit exists to pre-empt.
        git(&fx.repo, &["config", "gc.auto", "0"]);
        git(&fx.repo, &["config", "maintenance.auto", "false"]);
        for index in 0..count {
            std::fs::write(fx.repo.join("grow.txt"), format!("round {index}\n")).unwrap();
            git(&fx.repo, &["add", "-A"]);
            git(&fx.repo, &["commit", "-q", "-m", &format!("round {index}")]);
            git(&fx.repo, &["repack", "-q", "-d"]);
        }
    }

    fn census_now(fx: &Fixture) -> ObjectStoreCensus {
        crate::jj::census_object_store(&fx.repo.join(".git")).unwrap()
    }

    /// A store past git's pack limit is compacted by the hourly pass while its
    /// first prune still waits for confirmation, and the compaction is recorded
    /// with the census before and after it. An opted-out store is not touched.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn the_hourly_pass_compacts_a_store_over_the_pack_limit_unless_opted_out() {
        let Some(fx) = fixture().await else {
            eprintln!("skipping object-store compaction pass test: jj not resolvable");
            return;
        };
        grow_packs(&fx, PACK_LIMIT as usize + 2);
        assert!(compaction_due(&census_now(&fx)));

        set_prune_confirmed(&fx.orch.db.local, "p-refs", &fx.project.store, false)
            .await
            .unwrap();
        run_pass(&fx.orch).await;
        assert_eq!(sample_count(&fx, "compact").await, 0, "opted out");
        assert!(compaction_due(&census_now(&fx)));

        fx.orch
            .db
            .local
            .execute("DELETE FROM ref_store_maintenance", ())
            .await
            .unwrap();
        // A prune (and its reclaim, which repacks everything) ran an hour ago,
        // so this pass compacts and nothing else.
        fx.orch
            .db
            .local
            .execute(
                "INSERT INTO ref_store_samples
                   (id, project_id, store_path, kind, started_at, finished_at, outcome,
                    duration_ms, reclaimed_bytes)
                 VALUES ('earlier', 'p-refs', ?1, 'prune', ?2, ?2, 'ok', 1, 0)",
                params![store_key(&fx.project.store), unix_now() - 3600],
            )
            .await
            .unwrap();
        run_pass(&fx.orch).await;
        assert_eq!(sample_count(&fx, "prune").await, 1, "no prune was due");
        assert_eq!(sample_count(&fx, "compact").await, 1);
        let after = census_now(&fx);
        assert!(!compaction_due(&after), "{after:?}");
        assert!(after.multi_pack_index);

        let overview = ref_store_overview(&fx.orch, "p-refs", &fx.repo).await;
        let compaction = overview
            .last_compaction
            .expect("the compaction is recorded");
        assert_eq!(compaction.outcome, "ok");
        assert!(compaction.before.unwrap().packs > PACK_LIMIT);
        assert_eq!(compaction.after.unwrap().packs, after.packs);
        assert_eq!(
            overview.objects.unwrap().packs,
            after.packs,
            "the page reads the census the compaction left"
        );

        run_pass(&fx.orch).await;
        assert_eq!(
            sample_count(&fx, "compact").await,
            1,
            "a compacted store is not compacted again"
        );
    }

    /// The operator's own gc is never raced; the skip is recorded with the
    /// lock and its age.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_compaction_waits_out_the_operators_own_gc() {
        let Some(fx) = fixture().await else {
            eprintln!("skipping object-store busy test: jj not resolvable");
            return;
        };
        grow_packs(&fx, PACK_LIMIT as usize + 2);
        std::fs::write(fx.repo.join(".git/gc.pid"), "1 host").unwrap();
        let git_dir = fx.repo.join(".git");
        compact(&fx.orch.db.local, &fx.project, &git_dir, census_now(&fx)).await;

        let overview = ref_store_overview(&fx.orch, "p-refs", &fx.repo).await;
        let compaction = overview.last_compaction.unwrap();
        assert!(
            compaction
                .outcome
                .starts_with("skipped:operator-git-busy:gc.pid ("),
            "{}",
            compaction.outcome
        );
        assert!(compaction_due(&census_now(&fx)), "nothing was repacked");
    }

    /// A rescued ref past the retention window: retired by a prune and by
    /// nothing else.
    fn expired_rescued_ref() -> String {
        let old_ms = (unix_now() - 40 * DAY as i64) * 1000;
        format!("refs/cairn/rescued/old/{old_ms}")
    }

    async fn sample_count(fx: &Fixture, kind: &str) -> i64 {
        fx.orch
            .db
            .local
            .query_one(
                "SELECT COUNT(*) FROM ref_store_samples WHERE kind = ?1",
                params![kind.to_string()],
                |row| row.i64(0),
            )
            .await
            .unwrap()
    }

    /// A runner that leaked check refs before it stopped needs no operator:
    /// the first pass of the next one retires every check ref no batch holds
    /// once it is past the cross-process grace. That is the check runner's own
    /// deletion completed, not a prune, so a store opted out of pruning gets
    /// it too.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn the_first_pass_retires_check_refs_an_earlier_runner_left_behind() {
        let Some(fx) = fixture().await else {
            eprintln!("skipping check-ref recovery test: jj not resolvable");
            return;
        };
        // Packed refs carry no age of their own, so these are old.
        fx.create_ref("refs/cairn/checks/left-behind");
        fx.create_ref("refs/cairn/checks/recovery-held-by-a-batch");
        git(&fx.repo, &["pack-refs", "--all"]);
        crate::execution::checks::hold_check_ref("refs/cairn/checks/recovery-held-by-a-batch");
        // A loose ref written moments ago may belong to another process's batch.
        fx.create_ref("refs/cairn/checks/recovery-just-written");
        set_prune_confirmed(&fx.orch.db.local, "p-refs", &fx.project.store, false)
            .await
            .unwrap();

        run_pass(&fx.orch).await;

        assert!(!fx.has_ref("refs/cairn/checks/left-behind"));
        assert!(fx.has_ref("refs/cairn/checks/recovery-held-by-a-batch"));
        assert!(fx.has_ref("refs/cairn/checks/recovery-just-written"));
        assert_eq!(
            sample_count(&fx, "prune").await,
            0,
            "the store is opted out, so nothing else was pruned"
        );
    }

    /// Nobody has to remember a switch: a small store is pruned by default,
    /// and an explicit opt-out keeps it measured and untouched.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn the_hourly_pass_prunes_by_default_and_honours_an_opt_out() {
        let Some(fx) = fixture().await else {
            eprintln!("skipping ref-store pass test: jj not resolvable");
            return;
        };
        // Only a prune retires this; check refs are retired on every pass.
        let expired = expired_rescued_ref();
        fx.create_ref(&expired);

        set_prune_confirmed(&fx.orch.db.local, "p-refs", &fx.project.store, false)
            .await
            .unwrap();
        run_pass(&fx.orch).await;
        assert_eq!(sample_count(&fx, "measure").await, 1);
        assert_eq!(sample_count(&fx, "prune").await, 0, "opted out");
        assert!(fx.has_ref(&expired));

        fx.orch
            .db
            .local
            .execute("DELETE FROM ref_store_maintenance", ())
            .await
            .unwrap();
        run_pass(&fx.orch).await;
        assert_eq!(sample_count(&fx, "prune").await, 1, "pruned by default");
        assert_eq!(fx.last_prune().await.0, "ok");
        assert!(!fx.has_ref(&expired));
        let overview = ref_store_overview(&fx.orch, "p-refs", &fx.repo).await;
        let reclaim = overview
            .last_reclaim
            .expect("a store never reclaimed is reclaimed by its first prune");
        assert!(reclaim.after.unwrap().multi_pack_index);
        assert_eq!(
            last_successful_reclaim(&fx.orch.db.local, &fx.project.store).await,
            Some(reclaim.started_at)
        );

        run_pass(&fx.orch).await;
        assert_eq!(
            sample_count(&fx, "prune").await,
            1,
            "a store pruned within the day is not pruned again"
        );
    }

    /// The one gate: a store already above a threshold, never pruned, waits
    /// for confirmation, says so once a day rather than every hour, and keeps
    /// measuring; confirmed, it is pruned.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_large_store_waits_for_its_first_prune_confirmation() {
        let Some(fx) = fixture().await else {
            eprintln!("skipping ref-store first-prune gate test: jj not resolvable");
            return;
        };
        // Only a prune retires this; check refs are retired on every pass.
        let expired = expired_rescued_ref();
        fx.create_ref(&expired);
        git(&fx.repo, &["pack-refs", "--all"]);
        // Grow `packed-refs` past the 4 MB threshold with stale keep refs.
        let packed = fx.repo.join(".git/packed-refs");
        let mut entries: Vec<String> = std::fs::read_to_string(&packed)
            .unwrap()
            .lines()
            .filter(|line| !line.starts_with('#') && !line.starts_with('^'))
            .map(str::to_string)
            .collect();
        for index in 0..60_000u64 {
            entries.push(format!("{} refs/jj/keep/{index:040x}", fx.commit));
        }
        entries.sort_by(|a, b| {
            a.split_once(' ')
                .unwrap()
                .1
                .cmp(b.split_once(' ').unwrap().1)
        });
        std::fs::write(
            &packed,
            format!(
                "# pack-refs with: peeled fully-peeled sorted \n{}\n",
                entries.join("\n")
            ),
        )
        .unwrap();
        assert!(std::fs::metadata(&packed).unwrap().len() > PACKED_BYTES_THRESHOLD);

        run_pass(&fx.orch).await;
        run_pass(&fx.orch).await;
        assert_eq!(sample_count(&fx, "measure").await, 2, "it keeps measuring");
        assert_eq!(
            sample_count(&fx, "prune").await,
            1,
            "the wait is recorded once a day, not every hour"
        );
        assert_eq!(fx.last_prune().await.0, AWAITING_CONFIRMATION);
        assert!(fx.has_ref(&expired), "nothing was pruned");

        set_prune_confirmed(&fx.orch.db.local, "p-refs", &fx.project.store, true)
            .await
            .unwrap();
        run_pass(&fx.orch).await;
        assert_eq!(fx.last_prune().await.0, "ok");
        assert!(!fx.has_ref(&expired));
        assert!(
            std::fs::metadata(&packed).map_or(0, |meta| meta.len()) < PACKED_BYTES_THRESHOLD,
            "the confirmed prune shrank packed-refs"
        );
    }
}
