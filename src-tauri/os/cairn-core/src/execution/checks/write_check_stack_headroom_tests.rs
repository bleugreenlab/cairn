use super::*;
use crate::db::DbState;
use crate::jj::tests::{git, git_stdout, init_project, jj_bin};
use crate::mcp::handlers::branch::BranchResolution;
use crate::services::testing::TestServicesBuilder;
use crate::stack_headroom::within_budget;
use crate::storage::SearchIndex;
use cairn_common::executor_protocol::{
    CellExecutionMeta, ExecutorMessage, ProcessBatchItemOutcome,
};
use tokio::sync::mpsc;

pub(super) fn commit(repo: &Path, files: &[(&str, &str)], message: &str) -> String {
    for (path, content) in files {
        let path = repo.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }
    git(repo, &["add", "-A"]);
    git(repo, &["commit", "-q", "-m", message]);
    git_stdout(repo, &["rev-parse", "HEAD"])
}

/// The colocated machine's answer to the one-item batch it was granted:
/// the item ran and exited zero.
pub(super) fn passed_batch(request: &CellRequest, header: &str, generation: u64) -> CellOutcome {
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
        tracked_modifications: None,
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
        mutation_delta: None,
        sandbox_denials: Vec::new(),
        tracked_modifications: None,
        node_scratch: None,
    }
}

/// One `when: write` gate check on a sealed write, placed on an attached
/// colocated executor that claims it and answers it passed. The wave is
/// the product's own: [`await_write_check_wave`] spawning
/// [`run_write_checks_after_seal_inner`], with ceilings far above anything
/// the wave needs, so the call returns when the wave settles and never at
/// a bound. The verdict is asserted by name, so a fixture that planned or
/// placed nothing fails rather than passing for want of a deep path.
#[test]
#[serial_test::serial(jj)]
fn a_write_check_submission_fits_in_half_a_default_stack() {
    let Some(bin) = jj_bin() else {
        return;
    };
    let home = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let repo = project.path().to_path_buf();
    init_project(&repo);
    let base = commit(
        &repo,
        &[(
            ".cairn/config.yaml",
            "checks:\n  smoke:\n    command: test -f README.md\n    when: write\n    policy: gate\n    impact:\n      - \"**\"\n",
        )],
        "declare smoke",
    );
    git(&repo, &["checkout", "-q", "-b", "agent/feature"]);
    let sealed = commit(&repo, &[("README.md", "present\n")], "the write");

    within_budget("write-check submission", move || async move {
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
        for statement in [
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1)",
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                 VALUES ('proj','ws','P','p',?1,'main',1,1)",
            "INSERT INTO jobs(id, project_id, branch, status, created_at, updated_at)
                 VALUES ('job','proj','agent/feature','running',1,1)",
            "INSERT INTO runs(id, project_id, job_id, created_at, updated_at)
                 VALUES ('run','proj','job',1,1)",
        ] {
            if statement.contains("?1") {
                local.execute(statement, (repo_path.as_str(),)).await.unwrap();
            } else {
                local.execute(statement, ()).await.unwrap();
            }
        }
        let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config_dir.clone(),
        )
        .jj_binary_path(bin.clone())
        .build();

        let (sender, mut executor) = mpsc::unbounded_channel();
        let generation = orch.fleet.attach_executor(sender);
        let fleet = orch.fleet.clone();
        let machine = tokio::spawn(async move {
            let entry_id = loop {
                match executor.recv().await {
                    Some(ExecutorMessage::Offer { entry_id, .. }) => break entry_id,
                    Some(_) => continue,
                    None => panic!("the write cell is offered to the colocated executor"),
                }
            };
            fleet.handle_executor_message(
                crate::fleet::COLOCATED_EXECUTOR_ID,
                generation,
                ExecutorMessage::Claim { entry_id },
            );
            let (request, batch) = loop {
                match executor.recv().await {
                    Some(ExecutorMessage::Granted {
                        request,
                        batch: Some(batch),
                        ..
                    }) => break (request, batch),
                    Some(_) => continue,
                    None => panic!("the claimed write cell is granted with its batch"),
                }
            };
            assert_eq!(batch.items.len(), 1, "one check, one batch item");
            let outcome = passed_batch(&request, &batch.items[0].header, generation);
            fleet.handle_executor_message(
                crate::fleet::COLOCATED_EXECUTOR_ID,
                generation,
                ExecutorMessage::Result {
                    request_id: request.request_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    outcome,
                },
            );
        });

        let run_context = RunContext {
            run_id: "run".into(),
            job_id: "job".into(),
            exec_seq: Some(1),
            issue_id: None,
            issue_number: None,
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
            rev: "agent/feature".into(),
            commit_id: sealed,
            default_commit_id: base,
            default_branch: "main".into(),
        };
        let (progress, progress_seen) = tokio::sync::watch::channel(WaveProgress::default());
        let wave_orch = orch.clone();
        let claims = orch.lane_claims.open_wave("job");
        let ceiling = std::time::Duration::from_secs(600);
        let summary = await_write_check_wave(
            WriteChecksInFlightGuard::new(&orch, "job"),
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
                    "tool-use",
                    Some(&sealed_resolution),
                    progress,
                    claims,
                )
                .await
            },
            |_late: String| std::future::ready(()),
        )
        .await
        .expect("the wave answers with a summary");
        assert!(
            summary.contains("\u{2713} smoke"),
            "the write check was placed, run, and answered passed: {summary}"
        );
        machine.await.unwrap();
    });
}
