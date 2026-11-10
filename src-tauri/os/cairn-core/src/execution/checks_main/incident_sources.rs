//! Replay prospective filing sources on the main cadence's existing repair edges.
use super::*;
use crate::execution::main_incidents_store::{self as store, IncidentSource, SourceDraft};
use crate::storage::DbResult;

pub(super) async fn capture(
    db: &LocalDb,
    project: &str,
    lane: &CadenceLane,
    commit: Option<&str>,
    verdict: &CadenceVerdict,
    attempted: &Attempted,
    at: i64,
) -> DbResult<Option<String>> {
    if matches!(verdict, CadenceVerdict::Cancelled { .. }) {
        return Ok(None);
    }
    let event = verdict
        .row()
        .map(|r| format!("observation:{}", r.id))
        .unwrap_or_else(|| format!("attempt:{}", attempted.event_id));
    let (class, cause) = match verdict {
        CadenceVerdict::Death { cause, .. } => (Some("death".into()), Some(cause.clone())),
        CadenceVerdict::NoVerdict { cause, .. } => (Some("no-verdict".into()), Some(cause.clone())),
        _ => (None, None),
    };
    store::enqueue_source(
        db,
        project,
        &SourceDraft {
            event_key: event.clone(),
            observation_id: verdict.row().map(|r| r.id.clone()),
            check_name: lane.check.clone(),
            platform: lane.platform.clone(),
            commit_sha: commit.map(str::to_string),
            notify: Some(lane.notify.clone()),
            lane_kind: Some(
                match lane.verdict {
                    LaneVerdict::PerTest => "per-test",
                    LaneVerdict::Exit => "exit",
                }
                .into(),
            ),
            cause_class: class,
            cause,
        },
        at / 1000,
    )
    .await?;
    Ok(Some(event))
}

/// Prepare every source that is due: never prepared, failed before, or held
/// for corroboration past the end of its wait.
pub(super) async fn retry(orch: &Orchestrator, db: &LocalDb) {
    let sources = match store::pending_sources(db, 20, now_ms() / 1000).await {
        Ok(sources) => sources,
        Err(error) => {
            log::error!("main incident source scan failed: {error}");
            return;
        }
    };
    for source in sources {
        match prepare(orch, db, &source).await {
            Ok((note, _)) => {
                if filing_notice_due(orch, &source.project_id, &note, now_ms()) {
                    let notify = failure_notify(db, &source).await;
                    deliver(orch, db, &source.project_id, &notify, &note).await;
                }
            }
            Err(error) => {
                let message = preparation_failed(db, &source, &error).await;
                let notify = failure_notify(db, &source).await;
                if filing_notice_due(orch, &source.project_id, &message, now_ms()) {
                    deliver(orch, db, &source.project_id, &notify, &message).await;
                }
            }
        }
    }
}

/// Re-decide the reds a lane's earlier rows hold for corroboration, now that
/// a new row (`current`, which is not revisited) has landed on it, and say what
/// that settled for the new row's notice. A source that stays held says
/// nothing unless it filed an incident; its row's own notice listed it.
pub(super) async fn revisit_held(
    orch: &Orchestrator,
    db: &LocalDb,
    project: &str,
    lane: &CadenceLane,
    current: Option<&str>,
) -> String {
    let sources = match store::held_sources(db, project, &lane.check, &lane.platform).await {
        Ok(sources) => sources,
        Err(error) => {
            log::error!("main incident held-source scan failed: {error}");
            return format!("\nReds held for corroboration on this lane could not be re-read: {error}. They stay held, and the heartbeat decides them when their wait ends.\n");
        }
    };
    let mut out = String::new();
    for source in sources
        .iter()
        .filter(|source| Some(source.draft.event_key.as_str()) != current)
    {
        match prepare(orch, db, source).await {
            Ok((note, still_held)) => {
                if !still_held || note.contains("Durable main incidents") {
                    out.push_str(&note);
                }
            }
            Err(error) => {
                out.push('\n');
                out.push_str(&preparation_failed(db, source, &error).await);
                out.push('\n');
            }
        }
    }
    out
}

/// Record a source's preparation failure for retry, and the notice naming it.
async fn preparation_failed(db: &LocalDb, source: &IncidentSource, error: &str) -> String {
    let mut message = format!(
        "FAIL-TO-FILE: main incident source {} could not be prepared: {error}; retained for retry. Held observation: {}.",
        source.draft.event_key,
        source.draft.observation_id.as_deref().unwrap_or("none (row-less source)")
    );
    if let Err(error) = store::finish_source(
        db,
        &source.project_id,
        &source.draft.event_key,
        Some(error.to_string()),
        now_ms() / 1000,
    )
    .await
    {
        log::error!("could not record main incident source retry error: {error}");
    }
    if let Ok(health) = store::source_health(db, Some(&source.project_id), now_ms() / 1000).await {
        message.push_str(&format!("\n{health}"));
    }
    log::error!("{message}");
    message
}

// Routing a preparation error is not classifying the observation. In particular,
// a current checkout may supply a recipient, never a replacement lane verdict.
async fn failure_notify(db: &LocalDb, source: &IncidentSource) -> String {
    if let Some(notify) = &source.draft.notify {
        return notify.clone();
    }
    if let Ok(Some(repo_path)) = db
        .query_opt_text(
            "SELECT repo_path FROM projects WHERE id=?1",
            (source.project_id.clone(),),
        )
        .await
    {
        let repo = PathBuf::from(repo_path);
        if let Some(commit) = &source.draft.commit_sha {
            if let Ok(loaded) = load_checks_contract_at_commit_detailed(&repo, commit).await {
                if let Some(cadence) = loaded
                    .contract
                    .checks
                    .get(&source.draft.check_name)
                    .and_then(|check| check.main_cadence.as_ref())
                {
                    return cadence.notify.clone();
                }
            }
        }
        let current = tokio::task::spawn_blocking(move || {
            crate::config::project_settings::load_checks_contract(&repo)
        })
        .await;
        if let Ok(Some(contract)) = current {
            if let Some(cadence) = contract
                .checks
                .get(&source.draft.check_name)
                .and_then(|check| check.main_cadence.as_ref())
            {
                return cadence.notify.clone();
            }
        }
    }
    // An unclassifiable source is a testing-side preparation failure, not
    // evidence of an execution-side death or a new immutable code verdict.
    "testing".into()
}

/// Rebuild a source's report from its immutable observation and file it:
/// filing's note, and whether the observation still holds reds for
/// corroboration.
async fn prepare(
    orch: &Orchestrator,
    db: &LocalDb,
    source: &IncidentSource,
) -> Result<(String, bool), String> {
    let draft = &source.draft;
    let (key, repo_path) = db
        .query_one(
            "SELECT key,repo_path FROM projects WHERE id=?1",
            (source.project_id.clone(),),
            |r| Ok((r.text(0)?, r.text(1)?)),
        )
        .await
        .map_err(|e| e.to_string())?;
    let (kind, notify) = match (draft.lane_kind.as_deref(), draft.notify.as_deref()) {
        (Some(kind), Some(notify)) => (
            match kind {
                "per-test" => LaneVerdict::PerTest,
                "exit" => LaneVerdict::Exit,
                other => return Err(format!("unknown recorded lane kind {other}")),
            },
            notify.to_string(),
        ),
        _ => {
            let loaded = load_checks_contract_at_commit_detailed(
                Path::new(&repo_path),
                draft
                    .commit_sha
                    .as_deref()
                    .ok_or_else(|| "source commit unavailable".to_string())?,
            )
            .await
            .map_err(|e| format!("cannot read source commit's check contract: {e:?}"))?;
            let check = loaded
                .contract
                .checks
                .get(&draft.check_name)
                .ok_or_else(|| "source check absent from its immutable contract".to_string())?;
            let cadence = check.main_cadence.as_ref().ok_or_else(|| {
                "source check has no main cadence in its immutable contract".to_string()
            })?;
            (LaneVerdict::of(check), cadence.notify.clone())
        }
    };
    let lane = CadenceLane {
        check: draft.check_name.clone(),
        platform: draft.platform.clone(),
        notify: notify.clone(),
        verdict: kind,
    };
    let names = |executor: &str| orch.fleet.executor_name_for_text(executor);
    let verdict = if let Some(id) = &draft.observation_id {
        kind.classify(load_row(db, &key, id, &names).await?)
    } else {
        match draft.cause_class.as_deref() {
            Some("death") => CadenceVerdict::Death {
                row: None,
                cause: draft
                    .cause
                    .clone()
                    .unwrap_or_else(|| "diagnostic unavailable".into()),
            },
            Some("no-verdict") => CadenceVerdict::NoVerdict {
                row: None,
                cause: draft
                    .cause
                    .clone()
                    .unwrap_or_else(|| "diagnostic unavailable".into()),
            },
            _ => return Err("row-less source has no recorded outcome classification".into()),
        }
    };
    let repo = {
        let jj_bin = orch.jj_binary_path.clone();
        let config = orch.config_dir.clone();
        let root = PathBuf::from(repo_path);
        tokio::task::spawn_blocking(move || {
            let jj = crate::jj::environment(&jj_bin, &config);
            crate::jj::coordinate_repository(&jj, &config, &root)
        })
        .await
        .map_err(|e| e.to_string())?
    };
    let chain = repo
        .as_ref()
        .ok()
        .zip(draft.commit_sha.as_deref())
        .and_then(|(repo, commit)| main_chain(orch, repo, commit).ok());
    let comparison = comparison_for(
        db,
        &source.project_id,
        &key,
        &lane,
        verdict.row(),
        chain.as_ref(),
        &names,
    )
    .await;
    let bracket = match (comparison.previous(), &repo) {
        (Some(previous), Ok(repo)) => {
            merge_bracket(
                orch,
                db,
                &source.project_id,
                &key,
                repo,
                &previous.commit,
                draft.commit_sha.as_deref().unwrap_or("unresolved"),
            )
            .await
        }
        (_, Err(error)) => Bracket {
            unavailable: Some(error.clone()),
            ..Default::default()
        },
        _ => Bracket::default(),
    };
    let report = CadenceReport {
        check: &draft.check_name,
        platform: &draft.platform,
        kind,
        commit: draft.commit_sha.as_deref(),
        verdict: &verdict,
        comparison: &comparison,
        bracket: &bracket,
        now_ms: now_ms(),
        rearmed: None,
    };
    Ok(incident_note(
        orch,
        db,
        &source.project_id,
        &key,
        &notify,
        &report,
        Some((&draft.event_key, source.created_at * 1000)),
    )
    .await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use cairn_db::turso::params;

    fn git(repo: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(repo)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    async fn fixture_with_notices(
        suppress_notices: bool,
    ) -> (tempfile::TempDir, Arc<LocalDb>, Orchestrator, PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(repo.join(".cairn")).unwrap();
        git(&repo, &["init", "-b", "main"]);
        std::fs::write(repo.join(".cairn/config.yaml"),
            "checks:\n  rust-tests:\n    command: cargo nextest run\n    fullCommand: cargo nextest run\n    verdictPlatforms: [linux]\n    mainCadence:\n      platforms: [linux]\n      notify: testing\n").unwrap();
        git(&repo, &["add", ".cairn/config.yaml"]);
        git(
            &repo,
            &[
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "-m",
                "initial",
            ],
        );
        let db = Arc::new(
            LocalDb::open_migrated_for_test(temp.path().join("source.db"))
                .await
                .unwrap(),
        );
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
               VALUES('p','w','Project','proj','',1,1);
             INSERT INTO threads(id,project_id,name,created_at,updated_at)
               VALUES('testing','p','testing',1,1),('execution','p','execution',1,1);",
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE projects SET repo_path=?1 WHERE id='p'",
            params![repo.to_string_lossy().as_ref()],
        )
        .await
        .unwrap();
        let state = Arc::new(DbState::new(
            db.clone(),
            Arc::new(SearchIndex::open_or_create(temp.path().join("search.db")).unwrap()),
        ));
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = Orchestrator::builder(state, services, temp.path().join("config")).build();
        if suppress_notices {
            // Filing-only fixtures need not wake provider-backed thread sessions.
            orch.main_cadence
                .lock()
                .unwrap()
                .filing_notices
                .insert("p".into(), now_ms());
        }
        (temp, db, orch, repo)
    }

    async fn fixture() -> (tempfile::TempDir, Arc<LocalDb>, Orchestrator, PathBuf) {
        fixture_with_notices(true).await
    }

    async fn observed(db: &LocalDb, id: &str, commit: &str, tree: &str, failed: bool, at: i64) {
        let excerpt = failed.then_some("assertion failed: old red");
        observed_on(db, id, commit, tree, "host", excerpt, at).await;
    }

    /// A row of `binary$test` on `host`: red with `excerpt`, or green.
    async fn observed_on(
        db: &LocalDb,
        id: &str,
        commit: &str,
        tree: &str,
        host: &str,
        excerpt: Option<&str>,
        at: i64,
    ) {
        let failed = excerpt.is_some();
        db.execute(
            "INSERT INTO check_result_observations
             (id,project_id,commit_sha,tree_hash,check_name,input_hash,environment_fingerprint,
              exit_code,verdict,complete,reusable,parser_version,result_schema_version,ran_at,duration_ms,
              cadence,output_tail,executor_id,verdict_platform,selection)
             VALUES(?1,'p',?2,?3,'rust-tests',?1,'env',?4,?5,1,0,1,1,?6,1,
                    'main','retained log',?7,'linux','full')",
            params![id,commit,tree,if failed {100} else {0},if failed {"failed"} else {"passed"},at,host],
        ).await.unwrap();
        db.execute(
            "INSERT INTO check_test_results(observation_id,test_id,status,failure_excerpt)
             VALUES(?1,'binary$test',?2,?3)",
            params![
                id,
                if failed { "failed" } else { "passed" },
                excerpt.unwrap_or("")
            ],
        )
        .await
        .unwrap();
    }

    async fn count(db: &LocalDb, table: &str) -> i64 {
        db.query_one(format!("SELECT COUNT(*) FROM {table}"), (), |r| r.i64(0))
            .await
            .unwrap()
    }

    async fn unavailable_contract_notice(current_notify: Option<&str>, expected_thread: &str) {
        let (_temp, db, orch, repo) = fixture_with_notices(false).await;
        db.execute("UPDATE projects SET default_branch='main' WHERE id='p'", ())
            .await
            .unwrap();
        db.execute_script(
            "INSERT INTO threads(id,project_id,name,created_at,updated_at)
             VALUES('custom','p','custom',1,1);",
        )
        .await
        .unwrap();
        if let Some(notify) = current_notify {
            let config = repo.join(".cairn/config.yaml");
            let text = std::fs::read_to_string(&config).unwrap();
            std::fs::write(
                config,
                text.replace("notify: testing", &format!("notify: {notify}")),
            )
            .unwrap();
        } else {
            std::fs::write(repo.join(".cairn/config.yaml"), "checks: {}\n").unwrap();
        }
        git(&repo, &["add", ".cairn/config.yaml"]);
        git(
            &repo,
            &[
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "--allow-empty",
                "-m",
                "current routing",
            ],
        );
        let tree = git(&repo, &["rev-parse", "HEAD^{tree}"]);
        observed(&db, "unreadable", &"f".repeat(40), &tree, true, now_ms()).await;
        let pending = store::pending_sources(&db, 20, now_ms() / 1000)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert!(pending[0].draft.notify.is_none() && pending[0].draft.lane_kind.is_none());
        retry(&orch, &db).await;
        let notice = db
            .query_opt_text(
                "SELECT content FROM messages WHERE channel_type='thread' AND channel_id=?1",
                (expected_thread.to_string(),),
            )
            .await
            .unwrap()
            .expect("unpreparable source must notify its owner, not only log");
        for expected in [
            "FAIL-TO-FILE",
            "observation:unreadable",
            "Held observation: unreadable",
            "cannot read source commit's check contract",
            "retained for retry",
        ] {
            assert!(notice.contains(expected), "missing {expected}: {notice}");
        }
        retry(&orch, &db).await;
        retry(&orch, &db).await;
        let health = store::source_health(&db, Some("p"), now_ms() / 1000)
            .await
            .unwrap();
        assert_eq!(
            (health.pending, health.held_observations, health.stuck),
            (1, 1, 1)
        );
        assert!(health.to_string().contains("FAIL-TO-FILE"));
        let pending = store::pending_sources(&db, 20, now_ms() / 1000)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert!(pending[0].draft.notify.is_none() && pending[0].draft.lane_kind.is_none());
        assert_eq!(count(&db, "issues").await, 0);
        assert_eq!(count(&db, "main_incident_evidence").await, 0);
        db.execute(
            "DELETE FROM check_result_observations WHERE id='unreadable'",
            (),
        )
        .await
        .unwrap();
        assert_eq!(count(&db, "check_result_observations").await, 1);
        retry(&orch, &db).await;
        assert_eq!(
            count(&db, "messages").await,
            1,
            "notice remains silence-bound"
        );
    }

    #[tokio::test]
    async fn unavailable_source_contract_notifies_current_cadence_without_inventing_verdict() {
        unavailable_contract_notice(Some("custom"), "custom").await;
    }

    #[tokio::test]
    async fn unavailable_source_contract_falls_back_to_testing_without_inventing_verdict() {
        unavailable_contract_notice(None, "testing").await;
    }

    #[tokio::test]
    async fn ambiguous_labelled_source_holds_observation_notifies_and_replays_after_repair() {
        let (_temp, db, orch, repo) = fixture_with_notices(false).await;
        db.execute_script(
            "INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at)
             VALUES('owner-a','p',1,'Owner A','active',1,1),
                   ('owner-b','p',2,'Owner B','failed',1,1);
             UPDATE projects SET next_issue_number=3 WHERE id='p';
             INSERT INTO labels(id,workspace_id,name,color,created_at,updated_at)
             VALUES('owner-label','w','incident:test-failure:binary$test','#000',1,1);
             INSERT INTO issue_labels(issue_id,label_id,created_at)
             VALUES('owner-a','owner-label',1),('owner-b','owner-label',1);",
        )
        .await
        .unwrap();
        let commit = git(&repo, &["rev-parse", "HEAD"]);
        let tree = git(&repo, &["rev-parse", "HEAD^{tree}"]);
        observed(&db, "ambiguous", &commit, &tree, true, now_ms()).await;
        retry(&orch, &db).await;
        retry(&orch, &db).await;
        let health = store::source_health(&db, Some("p"), now_ms() / 1000)
            .await
            .unwrap();
        assert_eq!((health.pending, health.held_observations), (1, 1));
        let pending = store::pending_sources(&db, 20, now_ms() / 1000)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        let notice = db
            .query_opt_text(
                "SELECT content FROM messages WHERE channel_type='thread' AND channel_id='testing'",
                (),
            )
            .await
            .unwrap()
            .expect("ambiguous source must notify its owners");
        for expected in [
            "Incident filing failed",
            "ambiguous",
            "incident:test-failure:binary$test",
            "cairn://p/proj/1",
            "cairn://p/proj/2",
        ] {
            assert!(notice.contains(expected), "missing {expected}: {notice}");
        }
        assert_eq!(
            count(&db, "messages").await,
            1,
            "notice remains silence-bound"
        );
        assert_eq!(count(&db, "issues").await, 2);
        assert_eq!(count(&db, "comments").await, 0);
        assert_eq!(count(&db, "main_incident_evidence").await, 0);
        db.execute(
            "DELETE FROM check_result_observations WHERE id='ambiguous'",
            (),
        )
        .await
        .unwrap();
        assert_eq!(count(&db, "check_result_observations").await, 1);

        db.execute("DELETE FROM issue_labels WHERE issue_id='owner-b'", ())
            .await
            .unwrap();
        retry(&orch, &db).await;
        assert!(store::pending_sources(&db, 20, now_ms() / 1000)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(count(&db, "issues").await, 2);
        assert_eq!(count(&db, "comments").await, 1);
        assert_eq!(count(&db, "main_incident_evidence").await, 1);
        assert_eq!(
            db.query_one("SELECT issue_uri FROM main_incident_evidence", (), |r| r
                .text(0),)
                .await
                .unwrap(),
            "cairn://p/proj/1"
        );
        retry(&orch, &db).await;
        assert_eq!(count(&db, "comments").await, 1);
        db.execute(
            "DELETE FROM check_result_observations WHERE id='ambiguous'",
            (),
        )
        .await
        .unwrap();
        assert_eq!(count(&db, "check_result_observations").await, 0);
    }

    #[tokio::test]
    async fn repeated_filing_failure_is_named_and_pins_raw_observation_until_recovery() {
        let (_temp, db, orch, repo) = fixture().await;
        let commit = git(&repo, &["rev-parse", "HEAD"]);
        let tree = git(&repo, &["rev-parse", "HEAD^{tree}"]);
        observed(&db, "held", &commit, &tree, true, now_ms()).await;
        db.execute_script(
            "CREATE TRIGGER reject_incident_comment BEFORE INSERT ON comments
            BEGIN SELECT RAISE(ABORT,'repeated filing fault'); END;",
        )
        .await
        .unwrap();
        for _ in 0..3 {
            retry(&orch, &db).await;
        }
        assert_eq!(count(&db, "issues").await, 0);
        let health = store::source_health(&db, Some("p"), now_ms() / 1000)
            .await
            .unwrap();
        assert_eq!(
            (health.pending, health.held_observations, health.stuck),
            (1, 1, 1)
        );
        let report = health.to_string();
        assert!(
            report.contains("FAIL-TO-FILE")
                && report.contains("observation:held")
                && report.contains("repeated filing fault")
        );
        db.execute("DELETE FROM check_result_observations WHERE id='held'", ())
            .await
            .unwrap();
        assert_eq!(count(&db, "check_result_observations").await, 1);
        db.execute_script("DROP TRIGGER reject_incident_comment;")
            .await
            .unwrap();
        retry(&orch, &db).await;
        assert!(store::pending_sources(&db, 20, now_ms() / 1000)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(count(&db, "issues").await, 1);
        db.execute("DELETE FROM check_result_observations WHERE id='held'", ())
            .await
            .unwrap();
        assert_eq!(count(&db, "check_result_observations").await, 0);
        assert_eq!(count(&db, "main_incident_evidence").await, 1);
    }

    #[tokio::test]
    async fn replay_files_committed_red_after_advanced_tip_and_newer_green_before_any_intent() {
        let (_temp, db, orch, repo) = fixture().await;
        let initial = git(&repo, &["rev-parse", "HEAD"]);
        let initial_tree = git(&repo, &["rev-parse", "HEAD^{tree}"]);
        observed(&db, "red", &initial, &initial_tree, true, 1000).await;
        assert_eq!(count(&db, "main_incident_evidence").await, 0);
        assert_eq!(
            store::pending_sources(&db, 20, now_ms() / 1000)
                .await
                .unwrap()
                .len(),
            1
        );
        // A crash has lost all in-memory reporting state; main advances before replay.
        std::fs::write(repo.join("advance"), "new tip").unwrap();
        git(&repo, &["add", "advance"]);
        git(
            &repo,
            &[
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "-m",
                "advanced",
            ],
        );
        let tip = git(&repo, &["rev-parse", "HEAD"]);
        let tip_tree = git(&repo, &["rev-parse", "HEAD^{tree}"]);
        observed(&db, "green", &tip, &tip_tree, false, 2000).await;
        retry(&orch, &db).await;
        let pending = store::pending_sources(&db, 20, now_ms() / 1000)
            .await
            .unwrap();
        assert!(
            pending.is_empty(),
            "pending source replay failed: {pending:?}"
        );
        assert_eq!(count(&db, "issues").await, 1);
        assert_eq!(count(&db, "main_incident_evidence").await, 1);
        let (body, observation) = db
            .query_one(
                "SELECT body,observation_id FROM main_incident_evidence",
                (),
                |r| Ok((r.text(0)?, r.text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(observation, "red");
        assert!(body.contains(&initial) && body.contains(&initial_tree));
        assert!(body.contains("assertion failed: old red"));
        assert_eq!(count(&db, "executions").await, 0);
        retry(&orch, &db).await;
        assert_eq!(count(&db, "comments").await, 1);
    }

    /// A held timing red is not a failure to file: its source stays
    /// unacknowledged (so retention keeps the observation) without reading as
    /// pending or stuck, and the next row on the lane decides it.
    #[tokio::test]
    async fn a_held_timing_red_keeps_its_observation_until_a_second_host_corroborates_it() {
        const TIMEOUT: &str = "TIMEOUT: nextest terminated this test at its slow-timeout bound (src-tauri/.config/nextest.toml)\nran 663s before termination";
        let (_temp, db, orch, repo) = fixture().await;
        let commit = git(&repo, &["rev-parse", "HEAD"]);
        let tree = git(&repo, &["rev-parse", "HEAD^{tree}"]);
        observed_on(
            &db,
            "fedora-row",
            &commit,
            &tree,
            "fedora",
            Some(TIMEOUT),
            now_ms(),
        )
        .await;
        retry(&orch, &db).await;
        assert_eq!(count(&db, "issues").await, 0);
        assert_eq!(count(&db, "main_incident_evidence").await, 0);
        let now = now_ms() / 1000;
        assert!(store::pending_sources(&db, 20, now)
            .await
            .unwrap()
            .is_empty());
        let health = store::source_health(&db, Some("p"), now).await.unwrap();
        assert_eq!(
            (
                health.pending,
                health.held_for_corroboration,
                health.held_observations,
                health.stuck
            ),
            (0, 1, 1, 0)
        );
        assert!(
            health.to_string().contains("1 held for corroboration"),
            "{health}"
        );
        db.execute(
            "DELETE FROM check_result_observations WHERE id='fedora-row'",
            (),
        )
        .await
        .unwrap();
        assert_eq!(
            count(&db, "check_result_observations").await,
            1,
            "the held observation is the evidence, so retention keeps it"
        );
        // The heartbeat takes it back once the wait ends.
        assert_eq!(
            store::pending_sources(&db, 20, now + 7 * 3600)
                .await
                .unwrap()
                .len(),
            1
        );

        observed_on(
            &db,
            "sp2-row",
            &commit,
            &tree,
            "sp2",
            Some(TIMEOUT),
            now_ms(),
        )
        .await;
        let lane = CadenceLane {
            check: "rust-tests".into(),
            platform: "linux".into(),
            notify: "testing".into(),
            verdict: LaneVerdict::PerTest,
        };
        let note = revisit_held(&orch, &db, "p", &lane, Some("observation:sp2-row")).await;
        assert!(note.contains("filed, corroborated"), "{note}");
        assert_eq!(count(&db, "issues").await, 1);
        assert!(store::held_sources(&db, "p", "rust-tests", "linux")
            .await
            .unwrap()
            .is_empty());
        // The second host's own source files onto the same incident.
        retry(&orch, &db).await;
        assert!(store::pending_sources(&db, 20, now_ms() / 1000)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(count(&db, "issues").await, 1);
        assert_eq!(count(&db, "main_incident_evidence").await, 2);
        db.execute(
            "DELETE FROM check_result_observations WHERE id='fedora-row'",
            (),
        )
        .await
        .unwrap();
        assert_eq!(
            count(&db, "check_result_observations").await,
            1,
            "acknowledgement released the held observation"
        );
    }

    #[tokio::test]
    async fn rowless_attempt_redelivery_is_idempotent_but_a_new_attempt_can_recur_without_commit_change(
    ) {
        let (_temp, db, orch, _repo) = fixture().await;
        let lane = CadenceLane {
            check: "rust-tests".into(),
            platform: "linux".into(),
            notify: "testing".into(),
            verdict: LaneVerdict::PerTest,
        };
        let verdict = CadenceVerdict::Death {
            row: None,
            cause: "executor unavailable".into(),
        };
        let first = Attempted::default();
        capture(&db, "p", &lane, None, &verdict, &first, 1000)
            .await
            .unwrap();
        capture(&db, "p", &lane, None, &verdict, &first, 1000)
            .await
            .unwrap();
        assert_eq!(
            store::pending_sources(&db, 20, now_ms() / 1000)
                .await
                .unwrap()
                .len(),
            1
        );
        retry(&orch, &db).await;
        assert!(store::pending_sources(&db, 20, now_ms() / 1000)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(count(&db, "issues").await, 1);
        db.execute("UPDATE issues SET status='closed',closed_at=2", ())
            .await
            .unwrap();
        capture(&db, "p", &lane, None, &verdict, &first, 1000)
            .await
            .unwrap();
        retry(&orch, &db).await;
        assert_eq!(count(&db, "issues").await, 1);
        let second = Attempted::default();
        assert_ne!(first.event_id, second.event_id);
        capture(&db, "p", &lane, None, &verdict, &second, 3000)
            .await
            .unwrap();
        retry(&orch, &db).await;
        assert_eq!(count(&db, "issues").await, 2);
        assert_eq!(count(&db, "comments").await, 2);
        assert_eq!(count(&db, "main_incidents").await, 2);
        let body = db
            .query_one("SELECT body FROM main_incident_evidence LIMIT 1", (), |r| {
                r.text(0)
            })
            .await
            .unwrap();
        assert!(body.contains("Actual tree/platform/runtime/executor: unavailable"));
    }
}
