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
}
