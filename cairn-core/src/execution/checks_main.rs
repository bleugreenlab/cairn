//! Convergence loop for the invariant that the current default-branch tree has a
//! verdict for every applicable review check.

use std::path::{Path, PathBuf};

use crate::execution::checks::{verify_review_tree, ReviewTreeCheckScope, ReviewTreeGateResult};
use crate::fleet::CellPriority;
use crate::jj::{logical_tree_hash, tree_entries, GraphFileChange, JjEnv};
use crate::orchestrator::{attention_push, Orchestrator};
use crate::storage::RowExt;

/// Re-evaluate default-branch attestation after its canonical bookmark has been
/// reconciled. Calling this on every observed advance is intentional: immutable
/// input-key reuse turns an already-attested head into cache hits without running
/// commands, while execution claims coalesce overlapping observations of one gap.
pub(crate) fn spawn(
    orch: &Orchestrator,
    project_id: String,
    repo_path: String,
    default_branch: String,
    carried_by_job: Option<String>,
) {
    let orch = orch.clone();
    tokio::spawn(async move {
        for attempt in 1..=3 {
            match run(
                &orch,
                &project_id,
                &repo_path,
                &default_branch,
                carried_by_job.as_deref(),
            )
            .await
            {
                Ok(()) => return,
                Err(error) if attempt < 3 => {
                    log::warn!("main-verdict convergence for project {project_id} attempt {attempt}: {error}; retrying");
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                }
                Err(error) => {
                    log::error!("main-verdict convergence for project {project_id} exhausted retries: {error}");
                }
            }
        }
    });
}

/// Startup repair edge. A process may exit after observing an advance but before
/// its detached wave records every verdict, so every launch rechecks every local
/// project's current canonical default head. Cache reuse makes covered projects
/// execution-free.
pub(crate) async fn spawn_all_current(orch: &Orchestrator) {
    for db in orch.db.all_dbs().await {
        let projects = db
            .query_all(
                "SELECT id, repo_path, default_branch FROM projects WHERE repo_path != '' AND default_branch IS NOT NULL",
                (),
                |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
            )
            .await;
        match projects {
            Ok(projects) => {
                for (project_id, repo_path, default_branch) in projects {
                    spawn(orch, project_id, repo_path, default_branch, None);
                }
            }
            Err(error) => log::warn!("main-verdict startup scan failed: {error}"),
        }
    }
}

async fn run(
    orch: &Orchestrator,
    project_id: &str,
    repo_path: &str,
    default_branch: &str,
    carried_by_job: Option<&str>,
) -> Result<(), String> {
    let result_db = crate::execution::routing::owning_db_for_project(&orch.db, project_id)
        .await
        .map_err(|error| error.to_string())?;
    let repo_root = PathBuf::from(repo_path);
    let store = crate::jj::project_store_dir(&orch.config_dir, &repo_root);
    // Every retry refreshes the remote and reconciles the canonical bookmark
    // itself. The webhook's sibling-reconcile path is best-effort and may fail
    // before reaching this cadence; main attestation must not inherit that gap or
    // successfully attest the stale local bookmark.
    if orch.services.git.remote_get_url(&repo_root).is_ok() {
        let git = orch.services.git.clone();
        let fetch_root = repo_root.clone();
        tokio::task::spawn_blocking(move || git.fetch_origin(&fetch_root))
            .await
            .map_err(|error| format!("main-attestation fetch task failed: {error}"))??;
        let jj = JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        let guard = orch
            .acquire_jj_store_lock(&store, "main-attestation canonical refresh")
            .await;
        let _phase = guard.phase("main-attestation canonical refresh");
        crate::jj::ensure_project_store(&jj, &store, &repo_root)
            .map_err(|error| format!("main-attestation store import failed: {error}"))?;
        crate::jj::reconcile_tracked_bookmark(&jj, &store, default_branch)
            .map_err(|error| format!("main-attestation bookmark reconcile failed: {error}"))?;
    }
    let repository = if crate::jj::is_jj_dir(&store) {
        store
    } else {
        repo_root.clone()
    };
    let commit = cairn_vcs::resolve_coordinate(&repository, default_branch)
        .await
        .map_err(|error| format!("default head '{default_branch}' is unresolvable: {error}"))?;
    let jj = JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    // Fail closed: logical_tree_hash must resolve the real Git tree. CAIRN-3553
    // removes the historical commit-id fallback at this shared seam.
    let tree_hash = logical_tree_hash(&jj, &repository, &commit).map_err(|e| e.to_string())?;
    let entries = tree_entries(&jj, &repository, &commit).map_err(|e| e.to_string())?;
    let whole_tree = whole_tree_applicability(&entries);
    let owner = carried_by_job
        .map(str::to_string)
        .unwrap_or_else(|| format!("main:{project_id}"));

    let result = verify_review_tree(
        orch,
        result_db,
        project_id,
        repo_path,
        Path::new(repo_path),
        &commit,
        &tree_hash,
        &entries,
        &whole_tree,
        &owner,
        CellPriority::ReviewCheck,
        ReviewTreeCheckScope::All,
    )
    .await;
    deliver(orch, project_id, carried_by_job, &commit, result).await
}

fn whole_tree_applicability(entries: &[(String, String)]) -> Vec<GraphFileChange> {
    entries
        .iter()
        .map(|(path, _)| GraphFileChange {
            path: path.clone(),
            previous_path: None,
            status: "modified".to_string(),
            additions: 0,
            deletions: 0,
        })
        .collect()
}

async fn deliver(
    orch: &Orchestrator,
    project_id: &str,
    carried_by_job: Option<&str>,
    commit: &str,
    result: ReviewTreeGateResult,
) -> Result<(), String> {
    let carrier = carried_by_job.unwrap_or("an external default-branch advance");
    let (report, fingerprint_part) = main_health_report(carrier, commit, &result);
    if matches!(result, ReviewTreeGateResult::Green) {
        log::info!("{report}");
        return Ok(());
    }
    log::error!("{report}");
    let db = crate::execution::routing::owning_db_for_project(&orch.db, project_id)
        .await
        .map_err(|error| error.to_string())?;
    let carrier_issue = match carried_by_job {
        Some(job_id) => carrier_issue(&db, job_id).await?,
        None => None,
    };
    let (issue_id, issue_uri) = match carrier_issue {
        Some(issue) => issue,
        None => main_health_issue(orch, &db, project_id).await?,
    };
    let key = format!("main-health:{project_id}");
    // Commit identity belongs in the report, not the state fingerprint. A new
    // main commit with the same underlying failure is one stable red.
    let fingerprint = main_health_fingerprint(&fingerprint_part);
    let already_recorded = db
        .query_opt_text(
            "SELECT id FROM comments WHERE issue_id = ?1 AND content LIKE ?2 LIMIT 1",
            (issue_id.clone(), format!("%main-health:{fingerprint}%")),
        )
        .await
        .map_err(|error| error.to_string())?
        .is_some();
    if !already_recorded {
        let content = format!("<!-- main-health:{fingerprint} -->\n{report}");
        crate::issues::comments::create(
            &db,
            &*orch.services.clock,
            crate::models::CreateComment {
                issue_id,
                content,
                source: crate::models::CommentSource::Agent,
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    }
    let watchers = crate::orchestrator::wakes::watcher_jobs_for_issue(&db, &issue_uri).await?;
    for recipient in watchers {
        if attention_push::latest_push_fingerprint(&db, &recipient, &key)
            .await
            .map_err(|error| error.to_string())?
            .flatten()
            .as_deref()
            == Some(fingerprint.as_str())
        {
            continue;
        }
        attention_push::push_with_fingerprint(
            &db,
            &recipient,
            &issue_uri,
            attention_push::Wake::Wake,
            attention_push::Boundary::Event,
            &key,
            Some(&fingerprint),
        )
        .await
        .map_err(|error| format!("failed to queue main-health attention: {error}"))?;
        let _ = crate::messages::delivery::nudge_job_for_urgency(
            orch,
            &recipient,
            crate::messages::queued::DeliveryUrgency::Steer,
        );
    }
    Ok(())
}

async fn main_health_issue(
    orch: &Orchestrator,
    db: &crate::storage::LocalDb,
    project_id: &str,
) -> Result<(String, String), String> {
    const TITLE: &str = "Main branch health requires attention";
    if let Some(existing) = db
        .query_opt(
            "SELECT i.id, p.key, i.number FROM issues i JOIN projects p ON p.id = i.project_id WHERE i.project_id = ?1 AND i.title = ?2 AND i.status NOT IN ('merged', 'closed') ORDER BY i.created_at DESC LIMIT 1",
            (project_id.to_string(), TITLE.to_string()),
            |row| Ok((row.text(0)?, format!("cairn://p/{}/{}", row.text(1)?, row.i64(2)?))),
        )
        .await
        .map_err(|error| error.to_string())?
    {
        return Ok(existing);
    }
    let issue = crate::issues::crud::create(
        db,
        &*orch.services.clock,
        crate::models::CreateIssue {
            project_id: project_id.to_string(),
            title: TITLE.to_string(),
            description: Some("Cairn detected a failing check on the current default-branch tree. Repair this with a surgical child change and no ride-alongs.".to_string()),
            backend_override: None,
            label_ids: None,
            kind: crate::models::IssueKind::Issue,
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    db.execute(
        "UPDATE issues SET status = 'active', progress = 'active', attention = 'needed' WHERE id = ?1",
        (issue.id.clone(),),
    )
    .await
    .map_err(|error| error.to_string())?;
    let key = db
        .query_opt_text(
            "SELECT key FROM projects WHERE id = ?1",
            (project_id.to_string(),),
        )
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "main-health project disappeared".to_string())?;
    Ok((issue.id, format!("cairn://p/{key}/{}", issue.number)))
}

async fn carrier_issue(
    db: &crate::storage::LocalDb,
    job_id: &str,
) -> Result<Option<(String, String)>, String> {
    let job_id = job_id.to_string();
    db.query_opt(
        "SELECT i.id, p.key, i.number FROM jobs j JOIN issues i ON i.id = j.issue_id JOIN projects p ON p.id = i.project_id WHERE j.id = ?1 LIMIT 1",
        (job_id,),
        |row| Ok((row.text(0)?, format!("cairn://p/{}/{}", row.text(1)?, row.i64(2)?))),
    )
    .await
    .map_err(|error| error.to_string())
}

fn main_health_fingerprint(failure_identity: &str) -> String {
    format!("main-health-v2:{failure_identity}")
}

fn main_health_report(
    carrier: &str,
    commit: &str,
    result: &ReviewTreeGateResult,
) -> (String, String) {
    let prefix = format!("Main-health attestation at {commit}, carried by {carrier}");
    match result {
        ReviewTreeGateResult::Green => (format!("{prefix}: green"), "green".into()),
        ReviewTreeGateResult::CheckFailed { name, detail } => (
            format!("{prefix}: check '{name}' failed: {detail}. Suggested repair: open a surgical child issue for this failure; do not add ride-along changes."),
            format!("check:{name}:{detail}"),
        ),
        ReviewTreeGateResult::InfrastructureFailure(detail) => (
            format!("{prefix}: check infrastructure failed: {detail}"),
            format!("infrastructure:{detail}"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whole_tree_drives_applicability_without_a_merge_diff() {
        let changed = whole_tree_applicability(&[("src/lib.rs".into(), "blob".into())]);
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].path, "src/lib.rs");
    }

    #[test]
    fn stable_red_fingerprint_is_independent_of_the_advancing_commit() {
        let first = main_health_fingerprint("check:rust-tests:E0599");
        let second = main_health_fingerprint("check:rust-tests:E0599");
        assert_eq!(first, second);
        assert!(!first.contains("commit-a"));
        assert!(!second.contains("commit-b"));
    }

    #[test]
    fn compile_failure_names_the_carrier_and_surgical_repair_shape() {
        let (report, _) = main_health_report(
            "CAIRN-3549",
            "main-head",
            &ReviewTreeGateResult::CheckFailed {
                name: "rust-tests".into(),
                detail: "error[E0599]: test target cannot compile".into(),
            },
        );
        assert!(report.contains("CAIRN-3549"));
        assert!(report.contains("E0599"));
        assert!(report.contains("surgical child issue"));
        assert!(report.contains("do not add ride-along changes"));
    }
}
