//! A write wave driven end to end against a colocated fake executor:
//! [`await_write_check_wave`] over [`run_write_checks_after_seal_inner`] on a
//! real jj-coordinated project whose sealed write selects two declared fixers
//! and three verdicts, with no cached verdicts.
//!
//! What it pins is the request shape of the write cadence: each fixer is its
//! own change-returning request, chained on the previous fixer's fold; each
//! verdict is its own pure-verdict request on the commit the chain ended at,
//! all in flight at once, each recorded as its own request settles; and a
//! verdict that rewrites the tree without declaring `fixes` is reported, never
//! folded.

use super::*;
use crate::db::DbState;
use crate::jj::tests::{git, git_stdout, init_project, jj_bin};
use crate::mcp::handlers::branch::BranchResolution;
use crate::services::testing::TestServicesBuilder;
use crate::storage::SearchIndex;
use cairn_common::executor_protocol::{
    CellExecutionMeta, ExecutorMessage, ProcessBatchItemOutcome, TrackedModificationEvidence,
};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

const JOB: &str = "job";
const BRANCH: &str = "agent/feature";
const VERDICTS: [&str; 3] = ["alpha", "beta", "gamma"];
/// The verdict whose result the machine holds until the test releases it.
const HELD: &str = "beta";
/// The verdict that writes to the tree without declaring `fixes`.
const UNDECLARED: &str = "gamma";
/// A deadlock guard, never an oracle: every wait below is for an event the
/// wave produces in well under a second.
const DEADLINE: Duration = Duration::from_secs(60);

fn commit(repo: &Path, files: &[(&str, &str)], message: &str) -> String {
    for (path, content) in files {
        let path = repo.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }
    git(repo, &["add", "-A"]);
    git(repo, &["commit", "-q", "-m", message]);
    git_stdout(repo, &["rev-parse", "HEAD"])
}

/// Each fixer owns one file, so neither's fold moves the other's key; the
/// verdicts read everything.
fn contract() -> String {
    let mut config = String::from(
        "checks:\n  \
         fmt-a:\n    command: fix a.txt\n    when: write\n    fixes: true\n    verdictPlatforms: []\n    impact:\n      - a.txt\n  \
         fmt-b:\n    command: fix b.txt\n    when: write\n    fixes: true\n    impact:\n      - b.txt\n",
    );
    for verdict in VERDICTS {
        config.push_str(&format!(
            "  {verdict}:\n    command: echo {verdict}\n    when: write\n    impact:\n      - \"**\"\n"
        ));
    }
    config
}

/// What a fixer leaves its file as.
fn fixed(path: &str) -> String {
    format!("formatted {path}\n")
}

/// The delta a fixer of `path` returns on `base`: a commit off `base` with
/// the file in its fixed form, or `None` when `base` already has it, as a
/// real formatter returns no change on a formatted tree.
fn rewrite(repo: &Path, base: &str, path: &str) -> Option<String> {
    let content = fixed(path);
    let current = std::process::Command::new("git")
        .args(["show", &format!("{base}:{path}")])
        .current_dir(repo)
        .output()
        .unwrap();
    if current.status.success() && current.stdout == content.as_bytes() {
        return None;
    }
    let scratch = tempfile::tempdir().unwrap();
    let index = scratch.path().join("index");
    let run = |args: &[&str], stdin: Option<&str>| -> String {
        use std::io::Write;
        let mut child = std::process::Command::new("git")
            .args(args)
            .current_dir(repo)
            .env("GIT_INDEX_FILE", &index)
            .env("GIT_AUTHOR_NAME", "fixer")
            .env("GIT_AUTHOR_EMAIL", "fixer@example.com")
            .env("GIT_COMMITTER_NAME", "fixer")
            .env("GIT_COMMITTER_EMAIL", "fixer@example.com")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        if let Some(input) = stdin {
            child
                .stdin
                .take()
                .unwrap()
                .write_all(input.as_bytes())
                .unwrap();
        }
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success(), "git {args:?}");
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    };
    run(&["read-tree", base], None);
    let blob = run(&["hash-object", "-w", "--stdin"], Some(&content));
    run(
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("100644,{blob},{path}"),
        ],
        None,
    );
    let tree = run(&["write-tree"], None);
    Some(run(
        &["commit-tree", &tree, "-p", base, "-m", "fixer output"],
        None,
    ))
}

/// The colocated machine's answer to a one-item batch that exited zero.
fn answered(
    request: &CellRequest,
    header: &str,
    generation: u64,
    tracked_modifications: Option<TrackedModificationEvidence>,
    mutation_delta: Option<crate::fleet::MutationDelta>,
) -> CellOutcome {
    let item = ProcessBatchItemOutcome {
        header: header.into(),
        body: String::new(),
        succeeded: true,
        suspended: false,
        images: Vec::new(),
        exit_code: Some(0),
        timed_out: false,
        started_at_unix_ms: 1,
        finished_at_unix_ms: 2,
        duration_ms: 1,
        peak_rss_bytes: None,
        peak_process: None,
        cpu: None,
        granted_units: None,
        disk_delta_bytes: None,
        sandbox_denials: Vec::new(),
        tracked_modifications,
        environment_fingerprint: String::new(),
        verdict_platform: None,
        verdict_arch: None,
        toolchain_fingerprint: None,
        verdict_environment_hash: None,
        stop: None,
        build_jobs: None,
        unrecorded_cache_checkpoint: None,
        build_job_processes: None,
    };
    CellOutcome::Completed {
        request_id: request.request_id.clone(),
        attempt_id: request.attempt_id.clone(),
        exit_code: Some(0),
        output: serde_json::to_string(&[item]).unwrap(),
        timed_out: false,
        metadata: CellExecutionMeta {
            warmth: None,
            load_context: None,
            executor_id: crate::fleet::COLOCATED_EXECUTOR_ID.into(),
            executor_device_id: "local-device".into(),
            executor_connection_generation: generation,
            cell_id: "cell".into(),
            cell_epoch: 1,
            started_at_unix_ms: 1,
            finished_at_unix_ms: 2,
            duration_ms: Some(1),
            commands_exited_at_unix_ms: None,
            peak_rss_bytes: None,
            peak_physical_footprint_bytes: None,
            peak_process: None,
            cpu: None,
            allocation: None,
            disk_delta_bytes: None,
            measurement_quality: None,
            environment_fingerprint: String::new(),
            verdict_platform: None,
            verdict_arch: None,
            verdict_environment_hash: None,
            sandbox: None,
            toolchain_fingerprint: None,
            containment: None,
            stop: None,
            cell_origin: None,
            test_phase: None,
            build_job_processes: None,
        },
        mutation_delta: mutation_delta.map(Box::new),
        sandbox_denials: Vec::new(),
        tracked_modifications: None,
        node_scratch: None,
    }
}

/// What the machine saw of one granted request.
#[derive(Debug)]
struct Grant {
    suite: String,
    policy: MutationPolicy,
    base_commit: String,
    request_id: String,
    items: usize,
}

/// Observation rows: check name to the commits it was recorded at, with the
/// output tail of the last.
async fn observations(db: &LocalDb) -> BTreeMap<String, (Vec<String>, String)> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT check_name, commit_sha, output_tail FROM check_result_observations
                     ORDER BY ran_at",
                    (),
                )
                .await?;
            let mut found: BTreeMap<String, (Vec<String>, String)> = BTreeMap::new();
            while let Some(row) = rows.next().await? {
                let entry = found.entry(row.text(0)?).or_default();
                entry.0.push(row.text(1)?);
                entry.1 = row.text(2)?;
            }
            Ok::<_, crate::storage::DbError>(found)
        })
    })
    .await
    .unwrap()
}

/// Two fixers and three verdicts on one write. The fixers run as two chained
/// change-returning requests, the second on the first's fold, and their
/// folds land as two commits. The verdicts run as three single-suite
/// pure-verdict requests on the second fold, all in flight at once; alpha's
/// and gamma's verdicts are recorded while beta's is still out. Gamma
/// rewrote a file without declaring `fixes`: its rewrite is reported with its
/// verdict and never folded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn fixers_chain_as_requests_and_each_verdict_is_its_own_request_recorded_as_it_settles() {
    let Some(bin) = jj_bin() else {
        return;
    };
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let repo = std::fs::canonicalize(project.path()).unwrap();
    init_project(&repo);
    let base = commit(
        &repo,
        &[
            (".cairn/config.yaml", contract().as_str()),
            ("a.txt", "a\n"),
            ("b.txt", "b\n"),
        ],
        "declare the write checks",
    );
    git(&repo, &["checkout", "-q", "-b", BRANCH]);
    let sealed = commit(
        &repo,
        &[("a.txt", "unformatted a\n"), ("b.txt", "unformatted b\n")],
        "the write",
    );
    git(&repo, &["checkout", "-q", "main"]);

    let config_dir = home.path().to_path_buf();
    let coordinates = {
        let jj = crate::jj::environment(&bin, &config_dir);
        let (config_dir, repo) = (config_dir.clone(), repo.clone());
        tokio::task::spawn_blocking(move || {
            crate::jj::coordinate_repository(&jj, &config_dir, &repo)
        })
        .await
        .unwrap()
        .unwrap()
    };
    let local = LocalDb::open_migrated_for_test(config_dir.join("checks.db"))
        .await
        .unwrap();
    let repo_path = repo.to_string_lossy().into_owned();
    local
        .execute(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1)",
            (),
        )
        .await
        .unwrap();
    local
        .execute(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                 VALUES ('proj','ws','P','p',?1,'main',1,1)",
            (repo_path.as_str(),),
        )
        .await
        .unwrap();
    local
        .execute_script(
            "INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
               VALUES('issue','proj',7,'I','active','active','none',1,1);
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
               VALUES('exec','r','issue','proj','running',1,1);
             INSERT INTO jobs(id,project_id,issue_id,execution_id,recipe_node_id,status,node_name,uri_segment,branch,created_at,updated_at)
               VALUES('job','proj','issue','exec','node','running','builder','builder','agent/feature',1,1);
             INSERT INTO runs(id, issue_id, project_id, job_id, status, created_at, updated_at)
               VALUES('run', 'issue', 'proj', 'job', 'live', 1, 1);",
        )
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

    // The colocated machine claims every offer and reports each grant. A
    // fixer is answered at once with its rewrite. The verdicts are held until
    // all three are granted; then every one but beta is answered, and beta
    // waits for the test.
    let (sender, mut executor) = mpsc::unbounded_channel();
    let generation = orch.fleet.attach_announced_executor(sender);
    let fleet = orch.fleet.clone();
    let machine_repo = repo.clone();
    let (grants_tx, mut grants) = mpsc::unbounded_channel::<Grant>();
    let (release_held, held_released) = oneshot::channel::<()>();
    let machine = cairn_common::task::spawn(async move {
        let answer = |request: &CellRequest, outcome: CellOutcome| {
            fleet.handle_executor_message(
                crate::fleet::COLOCATED_EXECUTOR_ID,
                generation,
                ExecutorMessage::Result {
                    request_id: request.request_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    outcome,
                },
            );
        };
        let mut verdicts = Vec::new();
        while verdicts.len() < VERDICTS.len() {
            match executor.recv().await {
                Some(ExecutorMessage::Offer { entry_id, .. }) => {
                    fleet.handle_executor_message(
                        crate::fleet::COLOCATED_EXECUTOR_ID,
                        generation,
                        ExecutorMessage::Claim { entry_id },
                    );
                }
                Some(ExecutorMessage::Granted {
                    request,
                    batch: Some(batch),
                    ..
                }) => {
                    let suite = request
                        .affinity_key
                        .as_deref()
                        .and_then(|key| key.strip_prefix(&format!("{JOB}:")))
                        .expect("every write check carries a job:suite key")
                        .to_string();
                    grants_tx
                        .send(Grant {
                            suite: suite.clone(),
                            policy: request.mutation_policy.clone(),
                            base_commit: request.base_commit.clone(),
                            request_id: request.request_id.clone(),
                            items: batch.items.len(),
                        })
                        .unwrap();
                    let header = batch.items[0].header.clone();
                    if request.mutation_policy == MutationPolicy::AllowDelta {
                        let path = if suite == "fmt-a" { "a.txt" } else { "b.txt" };
                        let delta = rewrite(&machine_repo, &request.base_commit, path).map(
                            |delta_commit| crate::fleet::MutationDelta {
                                base_commit: request.base_commit.clone(),
                                delta_commit,
                                upload_receipt: None,
                            },
                        );
                        answer(
                            &request,
                            answered(&request, &header, generation, None, delta),
                        );
                    } else {
                        // A pure-verdict cell discards what its command
                        // wrote and reports it, as the executor does.
                        let tracked = (suite == UNDECLARED).then(|| TrackedModificationEvidence {
                            paths: vec!["a.txt".into()],
                            files_changed: 1,
                            lines_added: 1,
                            lines_deleted: 1,
                            index_restore_failure: None,
                        });
                        verdicts.push((
                            suite,
                            request.clone(),
                            answered(&request, &header, generation, tracked, None),
                        ));
                    }
                }
                Some(_) => continue,
                None => panic!("the wave's checks are offered to the colocated executor"),
            }
        }
        let mut held = None;
        for (suite, request, outcome) in verdicts {
            if suite == HELD {
                held = Some((request, outcome));
            } else {
                answer(&request, outcome);
            }
        }
        held_released.await.unwrap();
        let (request, outcome) = held.expect("beta was granted");
        answer(&request, outcome);
        // Kept open so the executor stays attached while the wave finishes.
        executor
    });

    let run_context = RunContext {
        run_id: "run".into(),
        job_id: JOB.into(),
        exec_seq: Some(1),
        issue_id: Some("issue".into()),
        issue_number: Some(7),
        thread_id: None,
        project_id: "proj".into(),
        project_key: "p".into(),
        job_name: Some("builder".into()),
        agent_config_id: None,
    };
    let sealed_resolution = BranchResolution {
        project_id: "proj".into(),
        repository_path: coordinates,
        object_repository_path: repo.clone(),
        rev: BRANCH.into(),
        commit_id: sealed.clone(),
        default_commit_id: base,
        default_branch: "main".into(),
    };
    let (progress, progress_seen) = tokio::sync::watch::channel(WaveProgress::default());
    let wave_orch = orch.clone();
    let claims = orch.lane_claims.open_wave(JOB);
    let ticket = orch.late_write_checks.open(JOB, "tool-use");
    let ceiling = Duration::from_secs(600);
    let guard = WriteChecksInFlightGuard::new(&orch, JOB);
    let wave = cairn_common::task::spawn(async move {
        await_write_check_wave(
            guard,
            InlineWaitBounds {
                start: ceiling,
                total: ceiling,
            },
            progress_seen,
            async move {
                run_write_checks_after_seal_inner(
                    &wave_orch,
                    Some(&run_context),
                    &repo_path,
                    Some(&sealed_resolution),
                    progress,
                    claims,
                    &ticket,
                )
                .await
            },
            |_late: String| std::future::ready(()),
        )
        .await
    });

    // 1. Two fixers, two chained change-returning requests: fmt-a on the
    // sealed commit, fmt-b on the commit fmt-a's fold landed.
    let mut seen = Vec::new();
    for _ in 0..2 + VERDICTS.len() {
        let grant = tokio::time::timeout(DEADLINE, grants.recv())
            .await
            .unwrap_or_else(|_| panic!("every request is granted while beta is held; saw {seen:?}"))
            .expect("the machine reports its grants");
        seen.push(grant);
    }
    for grant in &seen {
        assert_eq!(grant.items, 1, "one suite, one batch item: {grant:?}");
    }
    let [fmt_a, fmt_b, verdict_grants @ ..] = seen.as_slice() else {
        unreachable!()
    };
    assert_eq!(
        (
            fmt_a.suite.as_str(),
            &fmt_a.policy,
            fmt_a.base_commit.as_str()
        ),
        ("fmt-a", &MutationPolicy::AllowDelta, sealed.as_str()),
        "the first fixer runs on the sealed commit"
    );
    assert_eq!(
        (fmt_b.suite.as_str(), &fmt_b.policy),
        ("fmt-b", &MutationPolicy::AllowDelta)
    );
    let fold_a = fmt_b.base_commit.clone();
    assert_ne!(fold_a, sealed, "the second fixer runs on the first's fold");
    assert_eq!(
        git_stdout(&repo, &["show", &format!("{fold_a}:a.txt")]),
        fixed("a.txt").trim(),
        "and sees its rewrite"
    );
    assert_eq!(
        git_stdout(&repo, &["rev-parse", &format!("{fold_a}^")]),
        sealed,
        "fmt-a's fold is a commit of its own on the sealed commit"
    );

    // 2. Three verdicts, three pure-verdict requests, all on fmt-b's fold and
    // all granted before any of them is answered.
    let fold_b = verdict_grants[0].base_commit.clone();
    assert_eq!(
        verdict_grants
            .iter()
            .map(|grant| grant.suite.as_str())
            .collect::<BTreeSet<_>>(),
        VERDICTS.into_iter().collect::<BTreeSet<_>>()
    );
    for grant in verdict_grants {
        assert_eq!(grant.policy, MutationPolicy::PureVerdict, "{grant:?}");
        assert_eq!(
            grant.base_commit, fold_b,
            "every verdict is about the tree that lands"
        );
    }
    assert_eq!(
        git_stdout(&repo, &["rev-parse", &format!("{fold_b}^")]),
        fold_a,
        "fmt-b's fold is a commit of its own on fmt-a's"
    );
    assert_eq!(
        seen.iter()
            .map(|grant| grant.request_id.as_str())
            .collect::<BTreeSet<_>>()
            .len(),
        seen.len(),
        "distinct requests: {seen:?}"
    );

    // 3. Alpha's and gamma's verdicts are recorded while beta's request is
    // still out, and the wave waits for beta.
    let db = orch.db.local.clone();
    let answered = tokio::time::timeout(DEADLINE, async {
        loop {
            let found = observations(&db).await;
            if found.contains_key("alpha") && found.contains_key(UNDECLARED) {
                break found;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("each answered verdict is recorded as its own request settles");
    assert!(
        !answered.contains_key(HELD),
        "beta has no verdict while its result is held: {answered:?}"
    );
    assert!(!wave.is_finished(), "the wave waits for beta");

    release_held.send(()).unwrap();
    let summary = tokio::time::timeout(DEADLINE, wave)
        .await
        .expect("the wave finishes once beta settles")
        .unwrap()
        .expect("the wave answers with a summary");
    let _executor = machine.await.unwrap();
    assert!(
        grants.try_recv().is_err(),
        "no check ran twice: neither fold invalidated the other fixer"
    );

    // 4. Every lane is recorded once, at the commit the chain ended at, and
    // the branch carries both fixers' rewrites and nothing of gamma's.
    let recorded = observations(&db).await;
    for name in ["fmt-a", "fmt-b", "alpha", "beta", "gamma"] {
        let (commits, _) = recorded
            .get(name)
            .unwrap_or_else(|| panic!("{name} is recorded: {recorded:?}"));
        assert_eq!(commits, &vec![fold_b.clone()], "{name}");
    }
    assert_eq!(
        git_stdout(&repo, &["rev-parse", &format!("refs/heads/{BRANCH}")]),
        fold_b,
        "two folds, and no third for gamma's undeclared rewrite"
    );
    for path in ["a.txt", "b.txt"] {
        assert_eq!(
            git_stdout(&repo, &["show", &format!("{fold_b}:{path}")]),
            fixed(path).trim()
        );
    }
    assert!(
        summary.contains(&format!(
            "fixed, {}, {}; 2 files",
            &fold_a[..12],
            &fold_b[..12]
        )),
        "{summary}"
    );
    for verdict in VERDICTS {
        assert!(
            summary.contains(&format!("\u{2713} {verdict}")),
            "{summary}"
        );
    }

    // 5. Gamma's rewrite is reported with its verdict, through the executor's
    // tracked-modification evidence, and was discarded.
    let (_, gamma_tail) = &recorded[UNDECLARED];
    assert!(
        gamma_tail.contains("note: check modified tracked paths: a.txt")
            && gamma_tail.contains("changes were discarded"),
        "{gamma_tail}"
    );
}
