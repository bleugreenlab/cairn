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

mod pass {
    use super::super::*;
    use crate::db::DbState;
    use crate::jj::tests::{git, git_stdout, init_project, jj_bin};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
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
        let local = LocalDb::open(config_dir.join("maintenance.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config_dir.clone(),
        )
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
            },
            commit,
        })
    }

    impl Fixture {
        fn create_ref(&self, name: &str) {
            git(&self.repo, &["update-ref", name, &self.commit]);
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

    /// Nobody has to remember a switch: a small store is pruned by default,
    /// and an explicit opt-out keeps it measured and untouched.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn the_hourly_pass_prunes_by_default_and_honours_an_opt_out() {
        let Some(fx) = fixture().await else {
            eprintln!("skipping ref-store pass test: jj not resolvable");
            return;
        };
        fx.create_ref("refs/cairn/checks/finished");
        git(&fx.repo, &["pack-refs", "--all"]);

        set_prune_confirmed(&fx.orch.db.local, "p-refs", &fx.project.store, false)
            .await
            .unwrap();
        run_pass(&fx.orch).await;
        assert_eq!(sample_count(&fx, "measure").await, 1);
        assert_eq!(sample_count(&fx, "prune").await, 0, "opted out");
        assert!(fx.has_ref("refs/cairn/checks/finished"));

        fx.orch
            .db
            .local
            .execute("DELETE FROM ref_store_maintenance", ())
            .await
            .unwrap();
        run_pass(&fx.orch).await;
        assert_eq!(sample_count(&fx, "prune").await, 1, "pruned by default");
        assert_eq!(fx.last_prune().await.0, "ok");
        assert!(!fx.has_ref("refs/cairn/checks/finished"));

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
        fx.create_ref("refs/cairn/checks/finished");
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
        assert!(
            fx.has_ref("refs/cairn/checks/finished"),
            "nothing was pruned"
        );

        set_prune_confirmed(&fx.orch.db.local, "p-refs", &fx.project.store, true)
            .await
            .unwrap();
        run_pass(&fx.orch).await;
        assert_eq!(fx.last_prune().await.0, "ok");
        assert!(!fx.has_ref("refs/cairn/checks/finished"));
        assert!(
            std::fs::metadata(&packed).map_or(0, |meta| meta.len()) < PACKED_BYTES_THRESHOLD,
            "the confirmed prune shrank packed-refs"
        );
    }
}
