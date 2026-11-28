//! An operator request's commit, end to end on a store backed by a remote:
//! sealed on `request/<attempt>` at the default branch's tip, landed through
//! `exact_landing_source` with no job or merge request, discarded and sealed
//! again when the tip moved, retired once landed, and recovered from the
//! journal by its subject alone.

use super::*;
use crate::pr_data::actions::default_landing::{
    discard_request_branch, land_request_commit, retire_request_branch, RequestLandingError,
};
use crate::pr_data::actions::landing_record::{
    begin_landing_publication, journal_publication_outcome, Publication, PublicationOutcome,
};

const PROJECT: &str = "proj-1";

/// The fixture with a real git client: a request landing reads and repairs
/// the project checkout, and releases its pin, through it.
async fn real_git_fixture(bin: &str) -> MergeFixture {
    setup_clean_merge_with_git(bin, crate::services::RealGitClient).await
}

/// The command's result as an executor cell hands it back: a commit on `base`
/// in the project repository changing `files`, and the routed request it
/// answers. Built through a side index, never the working tree.
fn request_delta(
    fx: &MergeFixture,
    request_id: &str,
    base: &str,
    files: &[(&str, &str)],
) -> (
    cairn_common::executor_protocol::CellRequest,
    crate::fleet::MutationDelta,
) {
    let repo = fx._proj.path();
    let index = tempfile::tempdir().unwrap();
    let index = index.path().join("index");
    let plumb = |args: &[&str], input: Option<&str>| -> String {
        use std::io::Write as _;
        let mut child = crate::env::git()
            .args(args)
            .env("GIT_INDEX_FILE", &index)
            .current_dir(repo)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.unwrap_or_default().as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success(), "git {args:?} failed");
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    };
    plumb(&["read-tree", base], None);
    for (name, content) in files {
        let blob = plumb(&["hash-object", "-w", "--stdin"], Some(content));
        plumb(
            &[
                "update-index",
                "--add",
                "--cacheinfo",
                &format!("100644,{blob},{name}"),
            ],
            None,
        );
    }
    let tree = plumb(&["write-tree"], None);
    let commit = plumb(
        &["commit-tree", &tree, "-p", base, "-m", "command output"],
        None,
    );
    let request = cairn_common::executor_protocol::CellRequest {
        request_id: request_id.into(),
        attempt_id: format!("{request_id}-cell"),
        project_id: PROJECT.into(),
        repository: cairn_common::executor_protocol::RepositoryLocator::ColocatedPath {
            project_id: PROJECT.into(),
            repository_id: "repository".into(),
            absolute_path: repo.to_string_lossy().into_owned(),
        },
        base_commit: base.to_string(),
        command: "bun run bumpver --files-only 1.2.3".into(),
        command_class: cairn_common::executor_protocol::CellCommandClass::Other,
        placement_work_class: cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
        owner: None,
        cwd: String::new(),
        env: Vec::new(),
        priority: cairn_common::wire_vocabulary::CellPriority::ReviewCheck,
        wait_horizon_unix_ms: u64::MAX,
        waiting_since_unix_ms: 0,
        timeout_ms: 1_000,
        mutation_policy: cairn_common::executor_protocol::MutationPolicy::AllowDelta,
        // An operator act: no job parked it.
        requesting_job_id: None,
        affinity_key: None,
        executor: None,
        pinned_executor_id: None,
        placement_mobility: Default::default(),
        verdict_platforms: Vec::new(),
        avoided_executor_ids: Vec::new(),
        project_requirements: Default::default(),
        command_resource_identity: None,
        resource_reservation: Default::default(),
        learned_estimate: None,
        scratch_sync: None,
        home_affinity: None,
        publication: None,
    };
    let delta = crate::fleet::MutationDelta {
        base_commit: base.to_string(),
        delta_commit: commit,
        upload_receipt: None,
    };
    (request, delta)
}

fn sealed_message(attempt: &str) -> String {
    format!(
        "Bump version to 1.2.3\n\nCairn-Request-Act: act-digest\n{}",
        crate::jj::request_attempt_trailer(attempt)
    )
}

fn admits_version(path: &str) -> bool {
    path == "VERSION"
}

async fn seal(
    fx: &MergeFixture,
    request_id: &str,
    branch: &str,
    message: &str,
    files: &[(&str, &str)],
) -> Result<crate::mcp::handlers::run::SealedRequestCommit, String> {
    let tip = crate::jj::bookmark_commit(&fx.jj, &fx.store, "main").unwrap();
    let (request, delta) = request_delta(fx, request_id, &tip, files);
    crate::mcp::handlers::run::seal_request_delta(
        &fx.orch,
        &fx.store,
        &request,
        &delta,
        branch,
        message,
        &admits_version,
    )
    .await
    .map_err(|refusal| refusal.to_string())
}

async fn count(fx: &MergeFixture, sql: &str, value: &str) -> i64 {
    fx.orch
        .db
        .local
        .query_opt_i64(sql, params![value])
        .await
        .unwrap()
        .unwrap_or_default()
}

/// The whole path: seal at the tip, refuse to land once the tip moved, discard
/// and seal again, land exactly the sealed commit, run the default branch's
/// follow-ups, and retire the branch. Nothing writes a job or merge request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_request_commit_seals_at_the_tip_lands_exactly_and_reseals_when_the_tip_moves() {
    let Some(bin) = jj_bin() else { return };
    let fx = real_git_fixture(&bin).await;
    let repo_path = fx._proj.path().to_string_lossy().into_owned();
    let attempt = "att-spike-c-land";
    let branch = crate::jj::request_branch(attempt);
    let bookmark = |name: &str| crate::jj::bookmark_commit(&fx.jj, &fx.store, name);
    let rows_before = (
        count(&fx, "SELECT COUNT(*) FROM merge_requests WHERE ?1 = ?1", "").await,
        count(&fx, "SELECT COUNT(*) FROM jobs WHERE ?1 = ?1", "").await,
    );

    // An empty delta, and one outside `admits`, seal nothing.
    let empty = seal(
        &fx,
        "req-empty",
        &branch,
        &sealed_message(attempt),
        &[("seed.rs", "base\n")],
    )
    .await
    .unwrap_err();
    assert!(empty.contains("changed nothing"), "{empty}");
    let outside = seal(
        &fx,
        "req-outside",
        &branch,
        &sealed_message(attempt),
        &[("VERSION", "1.2.3\n"), ("other.rs", "x\n")],
    )
    .await
    .unwrap_err();
    assert!(outside.contains("other.rs"), "{outside}");
    assert_eq!(bookmark(&branch), None, "a refused seal keeps no branch");

    // Sealed at the tip: one managed commit whose only parent is the tip.
    let tip = bookmark("main").unwrap();
    let first = seal(
        &fx,
        "req-1",
        &branch,
        &sealed_message(attempt),
        &[("VERSION", "1.2.3\n")],
    )
    .await
    .expect("a request delta seals on a fresh request branch");
    assert_eq!(first.base, tip);
    assert_eq!(parents_of(&fx, &first.commit), vec![tip.clone()]);
    assert_eq!(bookmark(&branch).as_deref(), Some(first.commit.as_str()));
    let message = git_stdout(
        fx._proj.path(),
        &["log", "-1", "--format=%B", &first.commit],
    );
    assert!(
        message.contains(&crate::jj::request_attempt_trailer(attempt)),
        "{message}"
    );
    assert_eq!(
        git_stdout(
            fx._proj.path(),
            &["log", "-1", "--format=%an", &first.commit]
        ),
        cairn_vcs::MANAGED_IDENTITY_NAME
    );
    // Preparation never advances it.
    let refused = crate::orchestrator::base_advance::preparation::prepare_branch(
        &fx.orch,
        crate::orchestrator::base_advance::preparation::PreparationRequest::new(
            PROJECT,
            branch.as_str(),
            None,
            crate::orchestrator::base_advance::preparation::PreparationPurpose::OwnerMerge,
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(refused.kind(), "request branch", "{refused}");

    // Origin's main moves before the landing: nothing lands, and the refusal
    // is BaseMoved, not a retried landing.
    let moved = push_main_outside_cairn(&fx, "outside.rs");
    let base_moved = land_request_commit(&fx.orch, PROJECT, attempt, &branch, "Bump version")
        .await
        .unwrap_err();
    assert!(
        matches!(base_moved, RequestLandingError::BaseMoved(_)),
        "{base_moved:?}"
    );
    assert_eq!(git_stdout(&fx.origin_path, &["rev-parse", "main"]), moved);
    assert_eq!(bookmark("main").as_deref(), Some(moved.as_str()));

    // Discarded only as this attempt's own sealed commit.
    let foreign = crate::jj::discard_unlanded_bookmark_expecting(
        &fx.store,
        &fx.ctx.source_branch,
        &bookmark(&fx.ctx.source_branch).unwrap(),
    )
    .unwrap_err();
    assert!(foreign.contains("only a `request/` branch"), "{foreign}");
    discard_request_branch(&fx.orch, PROJECT, &branch, &first.commit)
        .await
        .expect("the unlanded request branch is discarded");
    assert_eq!(bookmark(&branch), None);
    assert_eq!(
        crate::jj::read_local_branch_state(&fx.store, &branch)
            .unwrap()
            .git_ref,
        None
    );

    // Sealed again on the new tip, it lands as exactly that commit.
    let second = seal(
        &fx,
        "req-2",
        &branch,
        &sealed_message(attempt),
        &[("VERSION", "1.2.3\n")],
    )
    .await
    .expect("a request seals again on the moved tip");
    assert_eq!(parents_of(&fx, &second.commit), vec![moved.clone()]);
    let landed = land_request_commit(&fx.orch, PROJECT, attempt, &branch, "Bump version")
        .await
        .expect("the request commit lands on the tip it was sealed on");
    assert_eq!(
        landed.landed_commit, second.commit,
        "nothing was carried or squashed"
    );
    assert_eq!(
        git_stdout(&fx.origin_path, &["rev-parse", "main"]),
        second.commit
    );
    assert_eq!(bookmark("main").as_deref(), Some(second.commit.as_str()));
    assert!(
        crate::orchestrator::base_advance::follow_probe::ran(&repo_path, "main"),
        "the default branch's follow-ups ran for the request landing"
    );

    // Every row the attempt journaled names no merge request and carries its
    // subject; its publication intent still begins with `publication`.
    let rows = count(
        &fx,
        "SELECT COUNT(*) FROM merge_attempt_events WHERE job_id = ?1",
        attempt,
    )
    .await;
    assert!(
        rows >= 5,
        "two attempts journaled their gates, outcomes and publication"
    );
    let unmarked = count(
        &fx,
        &format!(
            "SELECT COUNT(*) FROM merge_attempt_events WHERE job_id = ?1 AND \
             (merge_request_id IS NOT NULL OR detail IS NULL OR detail NOT LIKE \
             '%\"subject\":{{\"operatorRequest\":{{\"attemptId\":\"{attempt}\"%')"
        ),
        attempt,
    )
    .await;
    assert_eq!(
        unmarked, 0,
        "every request row carries subject.operatorRequest"
    );
    let intents = count(
        &fx,
        "SELECT COUNT(*) FROM merge_attempt_events WHERE job_id = ?1 AND state = 'executing' \
         AND detail LIKE '{\"publication\":%'",
        attempt,
    )
    .await;
    assert_eq!(intents, 1);
    assert_eq!(
        (
            count(&fx, "SELECT COUNT(*) FROM merge_requests WHERE ?1 = ?1", "").await,
            count(&fx, "SELECT COUNT(*) FROM jobs WHERE ?1 = ?1", "").await,
        ),
        rows_before,
        "a request landing writes no merge request and no job"
    );
    assert!(!crate::pr_data::actions::context::resolution_probe::asked_about(attempt));

    // Retired once landed.
    retire_request_branch(&fx.orch, PROJECT, &branch, "main", &landed.landed_commit)
        .await
        .expect("the landed request branch retires");
    assert_eq!(bookmark(&branch), None);
}

/// A request publication whose outcome was never journaled is found by
/// landing recovery with no merge request on its row, and settles from the
/// publication evidence alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn an_unknown_request_publication_settles_through_the_request_arm() {
    let Some(bin) = jj_bin() else { return };
    let fx = real_git_fixture(&bin).await;
    let attempt = "att-spike-c-recover";
    let branch = crate::jj::request_branch(attempt);
    let tip = crate::jj::bookmark_commit(&fx.jj, &fx.store, "main").unwrap();
    let sealed = seal(
        &fx,
        "req-recover",
        &branch,
        &sealed_message(attempt),
        &[("VERSION", "9\n")],
    )
    .await
    .unwrap();
    // The landing installed itself in the store and then the process ended
    // before it journaled the outcome.
    fx.jj
        .run(
            &fx.store,
            &[
                "bookmark",
                "set",
                "main",
                "-r",
                &sealed.commit,
                "--ignore-working-copy",
            ],
            "install the landing",
        )
        .unwrap();
    let tree = crate::jj::sealed_tree_hash_via_git(&fx.jj, &fx.store, &sealed.commit).unwrap();
    let publication_id = format!("{attempt}-gate-publication-1");
    let journal = MergeAttemptJournal {
        attempt_id: &publication_id,
        project_id: PROJECT,
        merge_request_id: None,
        job_id: attempt,
        target_ref: "main",
        source_ref: &branch,
        policy_json: "{}",
    };
    let publication = Publication {
        target_branch: "main".into(),
        expected_old: tip.clone(),
        target_before: tip.clone(),
        landing_commit: sealed.commit.clone(),
        landing_tree: tree,
        source_branch: branch.clone(),
        source_before: sealed.commit.clone(),
        source_after: sealed.commit.clone(),
        shape: crate::pr_data::actions::landing_record::PublicationShape::FastForward,
    };
    let db = fx.orch.db.local.clone();
    drop(
        begin_landing_publication(&db, &journal, publication.clone(), true)
            .await
            .unwrap(),
    );
    journal_publication_outcome(
        &db,
        &journal,
        &publication,
        &PublicationOutcome::Unknown("the process ended".into()),
    )
    .await
    .unwrap();

    let recovered =
        crate::pr_data::actions::landing_record::reconcile_pending_landings(&fx.orch).await;
    assert!(
        recovered.contains(
            &crate::pr_data::actions::landing_record::LandingRecovery::Recorded(
                sealed.commit.clone()
            )
        ),
        "{recovered:?}"
    );
    let settled = db
        .query_opt_text(
            "SELECT detail FROM merge_attempt_events WHERE attempt_id = ?1 AND sequence = 3 \
             AND state = 'succeeded' AND merge_request_id IS NULL",
            params![publication_id.as_str()],
        )
        .await
        .unwrap()
        .expect("recovery journaled the landing");
    assert!(
        settled.contains(&sealed.commit) && settled.contains("operatorRequest"),
        "{settled}"
    );
    assert!(!crate::pr_data::actions::context::resolution_probe::asked_about(attempt));
}

/// A request row whose `job_id` matches no job is excluded by subject from the
/// approval reconciliation, never resolved as a job's merge request by either
/// recovery pass, and kept by retention, which deletes nothing it owns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn an_ownerless_request_row_never_reaches_a_job_lookup_and_is_kept() {
    let Some(bin) = jj_bin() else { return };
    let fx = real_git_fixture(&bin).await;
    let attempt = "att-spike-c-orphan";
    let db = fx.orch.db.local.clone();
    let journal = MergeAttemptJournal {
        attempt_id: "att-spike-c-orphan-gate",
        project_id: PROJECT,
        merge_request_id: None,
        job_id: attempt,
        target_ref: "main",
        source_ref: "request/att-spike-c-orphan",
        policy_json: "{}",
    };
    // The shape that once reached the approval reconcile: latest event
    // `executing`, no publication intent.
    crate::pr_data::actions::merge::journal_merge_attempt_event(
        &db,
        &journal,
        0,
        "executing",
        &crate::pr_data::actions::merge::JournalEntry::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        count(&fx, "SELECT COUNT(*) FROM jobs WHERE id = ?1", attempt).await,
        0
    );

    let unsettled = crate::pr_data::actions::human_approval::unsettled_attempts(&db)
        .await
        .unwrap();
    assert!(
        unsettled.iter().all(|row| row.job_id != attempt),
        "excluded by its subject"
    );
    crate::pr_data::actions::default_landing::reconcile_approved_merges(&fx.orch).await;
    let recovered =
        crate::pr_data::actions::landing_record::reconcile_pending_landings(&fx.orch).await;
    assert!(recovered.is_empty(), "{recovered:?}");
    assert!(
        !crate::pr_data::actions::context::resolution_probe::asked_about(attempt),
        "no recovery pass resolved the attempt id as a job's merge request"
    );

    // Retention: the table's rows are owned, not evicted, and an evictor pass
    // leaves the row with no merge request in place.
    let rule = cairn_db::storage::retention::retention_for("merge_attempt_events").unwrap();
    assert_eq!(rule.rule.kind(), "owned");
    let mut state = cairn_db::storage::retention::EvictorState::default();
    cairn_db::storage::retention::run_pass(
        &db,
        chrono::Utc::now().timestamp() + 10 * 365 * 86_400,
        cairn_db::storage::retention::EvictorBudget::PRODUCTION,
        &mut state,
    )
    .await;
    assert_eq!(
        count(
            &fx,
            "SELECT COUNT(*) FROM merge_attempt_events WHERE job_id = ?1",
            attempt
        )
        .await,
        1,
        "the row outlives every eviction rule"
    );
}

// ── A declared `lands` act, through its consumer ──────────────────────────────────────────────────────

mod declared_lands {
    use super::*;
    use crate::operator_requests::fenced_command::consumer::{
        perform_lands, CommandRun, LandsEnd, LandsRunner, Observed,
    };
    use crate::operator_requests::fenced_command::declared;
    use cairn_common::requests::FencedCommandAct;
    use std::sync::atomic::{AtomicU32, Ordering};

    const CONFIG: &str = r#"approvedCommands:
  bump:
    label: Bump the version
    consequence: Sets {version} in package.json and lands that one commit.
    params:
      version:
        type: releaseVersion
        current: {path: package.json, jsonKey: version}
    command: ./bump {version}
    lands:
      paths: [package.json]
      message: "Bump version to {version}"
"#;

    fn package(version: &str) -> String {
        format!("{{\"version\":\"{version}\"}}\n")
    }

    async fn fixture(bin: &str) -> MergeFixture {
        setup_clean_merge_seeded(
            bin,
            crate::services::RealGitClient,
            &[
                (".cairn/config.yaml", CONFIG),
                ("package.json", &package("1.2.3")),
            ],
        )
        .await
    }

    /// The act a filing of `bump <asked>` freezes at the store's tip now.
    fn frozen(fx: &MergeFixture, asked: &str) -> FencedCommandAct {
        let tip = crate::jj::bookmark_commit(&fx.jj, &fx.store, "main").unwrap();
        let (settings, _) =
            crate::config::project_settings::parse_project_settings(CONFIG).unwrap();
        let spec = settings.approved_commands.unwrap().remove("bump").unwrap();
        let resolved = declared::resolve(
            "bump",
            &spec,
            &[("version".to_string(), asked.to_string())].into(),
            &tip,
            &|path, key| {
                let text = crate::jj::text_at_revision(&fx.jj, &fx.store, &tip, path)?.unwrap();
                let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                Ok(value[key].as_str().unwrap().to_string())
            },
        )
        .unwrap();
        FencedCommandAct {
            executor_id: "exec-1".into(),
            device_id: "device-1".into(),
            project_id: PROJECT.into(),
            branch: "main".into(),
            commit: tip,
            cwd: String::new(),
            command: resolved.command,
            timeout_ms: 1_000,
            node: "cairn://p/proj/1/1/builder".into(),
            declaration: Some(resolved.declaration),
        }
    }

    #[derive(Clone, Copy, PartialEq)]
    enum Command {
        /// Writes the resolved version, and main moves while the first run
        /// is still running.
        RacesMain,
        /// Writes the resolved version.
        Clean,
        /// Writes the resolved version and exits 1.
        ExitsOne,
        /// Exits 0 having written a version the card never showed.
        WritesAnother,
    }

    /// The accepted command as the cell would run it: its tracked writes come
    /// back as a delta on the base it ran at.
    struct FakeRun<'a> {
        fx: &'a MergeFixture,
        command: Command,
        writes: &'static str,
        runs: AtomicU32,
    }

    impl<'a> FakeRun<'a> {
        fn new(fx: &'a MergeFixture, command: Command, writes: &'static str) -> Self {
            Self {
                fx,
                command,
                writes,
                runs: AtomicU32::new(0),
            }
        }

        fn runs(&self) -> u32 {
            self.runs.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl LandsRunner for FakeRun<'_> {
        async fn run(&self, _: &Orchestrator, base: &str) -> CommandRun {
            let run = self.runs.fetch_add(1, Ordering::SeqCst) + 1;
            let written = if self.command == Command::WritesAnother {
                "9.9.9"
            } else {
                self.writes
            };
            let (request, delta) = request_delta(
                self.fx,
                &format!("req-declared-{run}"),
                base,
                &[("package.json", &package(written))],
            );
            if self.command == Command::RacesMain && run == 1 {
                push_main_outside_cairn(self.fx, "outside.rs");
            }
            CommandRun {
                observed: Observed::Ran {
                    exit_code: Some(if self.command == Command::ExitsOne {
                        1
                    } else {
                        0
                    }),
                    timed_out: false,
                    output: String::new(),
                    duration_ms: None,
                    device_id: String::new(),
                    connection_generation: 4,
                },
                request,
                delta: Some(delta),
            }
        }

        async fn recheck(&self, _: &Orchestrator) -> Result<(), String> {
            Ok(())
        }
    }

    fn set_main(jj: &crate::jj::JjEnv, store: &Path, commit: &str) {
        jj.run(
            store,
            &[
                "bookmark",
                "set",
                "main",
                "-r",
                commit,
                "--allow-backwards",
                "--ignore-working-copy",
            ],
            "move main",
        )
        .unwrap();
    }

    /// Main moving after the seal captured its exact coordinate, and before
    /// the publication revalidated it, is the same bounded rerun as a move
    /// the landing sees: the command runs again on the new tip and lands.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn main_moving_inside_the_seal_reruns_the_command_on_the_new_tip() {
        let Some(bin) = jj_bin() else { return };
        let fx = fixture(&bin).await;
        let bookmark = |name: &str| crate::jj::bookmark_commit(&fx.jj, &fx.store, name);
        let sm = frozen(&fx, "sm");
        let before = bookmark("main").unwrap();
        // Main really moves on origin and is fetched; the store's `main` is
        // held back at `before` until the seal has captured it.
        let moved = push_main_outside_cairn(&fx, "moved-in-seal.rs");
        fx.jj
            .run(
                &fx.store,
                &["git", "fetch", "--ignore-working-copy"],
                "fetch main",
            )
            .unwrap();
        set_main(&fx.jj, &fx.store, &before);
        let (jj, store, target) = (fx.jj.clone(), fx.store.clone(), moved.clone());
        *crate::mcp::handlers::run::AFTER_EXACT_CAPTURE
            .lock()
            .unwrap() = Some(Box::new(move || set_main(&jj, &store, &target)));

        let run = FakeRun::new(&fx, Command::Clean, "1.2.4");
        let landed = perform_lands(&fx.orch, "att-bump-seal", &sm, &run).await;
        let LandsEnd::Landed { commit, base, .. } = &landed.end else {
            panic!("{landed:?}");
        };
        assert_eq!(run.runs(), 2, "the seal saw main move, so it ran again");
        assert_eq!(base.as_deref(), Some(moved.as_str()));
        assert_eq!(parents_of(&fx, commit), vec![moved.clone()]);
        assert_eq!(bookmark(&crate::jj::request_branch("att-bump-seal")), None);
    }

    /// The whole consumer path on a remote-backed store: main moves under the
    /// first run, so its sealed commit is discarded and the command runs once
    /// more on the new tip; exactly that second sealed commit lands, with the
    /// act's trailer, and the request branch is retired. A later attempt of
    /// the same act adopts it without running; an act resolved from the old
    /// version is refused as stale before running; a failed exit and a sealed
    /// version the card never showed land nothing and leave no branch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn a_declared_bump_lands_once_reruns_when_main_moves_and_is_adopted_after() {
        let Some(bin) = jj_bin() else { return };
        let fx = fixture(&bin).await;
        let bookmark = |name: &str| crate::jj::bookmark_commit(&fx.jj, &fx.store, name);
        let rows_before = (
            count(&fx, "SELECT COUNT(*) FROM merge_requests WHERE ?1 = ?1", "").await,
            count(&fx, "SELECT COUNT(*) FROM jobs WHERE ?1 = ?1", "").await,
        );
        let sm = frozen(&fx, "sm");
        let md = frozen(&fx, "md");

        let racing = FakeRun::new(&fx, Command::RacesMain, "1.2.4");
        let landed = perform_lands(&fx.orch, "att-bump-1", &sm, &racing).await;
        let LandsEnd::Landed {
            commit,
            sealed,
            base,
        } = &landed.end
        else {
            panic!("{landed:?}");
        };
        assert_eq!(
            racing.runs(),
            2,
            "main moved once, so the command ran twice"
        );
        assert_eq!(landed.runs, 2);
        assert_eq!(sealed.as_deref(), Some(commit.as_str()));
        assert_eq!(parents_of(&fx, commit), vec![base.clone().unwrap()]);
        assert_eq!(&git_stdout(&fx.origin_path, &["rev-parse", "main"]), commit);
        assert_eq!(
            git_stdout(&fx.origin_path, &["show", "main:package.json"]),
            package("1.2.4").trim()
        );
        let message = git_stdout(&fx.origin_path, &["log", "-1", "--format=%B", "main"]);
        assert!(message.starts_with("Bump version to 1.2.4"), "{message}");
        assert!(
            message.contains(&format!(
                "Cairn-Request-Act: {}",
                declared::act_sha(&sm).unwrap()
            )) && message.contains(&crate::jj::request_attempt_trailer("att-bump-1")),
            "{message}"
        );
        assert_eq!(bookmark(&crate::jj::request_branch("att-bump-1")), None);
        declared::verify_landed(&fx.orch, &sm, commit)
            .await
            .unwrap();
        assert!(
            declared::verify_landed(&fx.orch, &sm, base.as_ref().unwrap())
                .await
                .is_err()
        );

        // A later attempt of the same act adopts the landed commit and runs
        // nothing.
        let again = FakeRun::new(&fx, Command::Clean, "1.2.4");
        let adopted = perform_lands(&fx.orch, "att-bump-2", &sm, &again).await;
        assert_eq!(
            adopted.end,
            LandsEnd::Landed {
                commit: commit.clone(),
                sealed: None,
                base: None
            }
        );
        assert_eq!(again.runs(), 0);

        // An act resolved from 1.2.3 no longer starts from main's version.
        let stale_run = FakeRun::new(&fx, Command::Clean, "1.3.0");
        let stale = perform_lands(&fx.orch, "att-bump-3", &md, &stale_run).await;
        assert!(
            matches!(&stale.end, LandsEnd::Failed(why) if why.contains("stale against main's 1.2.4")),
            "{stale:?}"
        );
        assert_eq!(stale_run.runs(), 0);

        // A failed exit, and a sealed version the card never showed, land
        // nothing and keep no branch.
        let next = frozen(&fx, "sm");
        for (attempt, command, refusal) in [
            ("att-bump-4", Command::ExitsOne, "exited 1"),
            (
                "att-bump-5",
                Command::WritesAnother,
                "not the 1.2.5 the card shows",
            ),
        ] {
            let run = FakeRun::new(&fx, command, "1.2.5");
            let refused = perform_lands(&fx.orch, attempt, &next, &run).await;
            assert!(
                matches!(&refused.end, LandsEnd::Failed(why) if why.contains(refusal)),
                "{refused:?}"
            );
            assert_eq!(run.runs(), 1);
            assert_eq!(bookmark(&crate::jj::request_branch(attempt)), None);
            assert_eq!(&git_stdout(&fx.origin_path, &["rev-parse", "main"]), commit);
        }

        assert_eq!(
            (
                count(&fx, "SELECT COUNT(*) FROM merge_requests WHERE ?1 = ?1", "").await,
                count(&fx, "SELECT COUNT(*) FROM jobs WHERE ?1 = ?1", "").await,
            ),
            rows_before,
            "a declared landing writes no merge request and no job"
        );
    }
}
