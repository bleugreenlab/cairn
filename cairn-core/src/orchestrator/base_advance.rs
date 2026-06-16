//! Proactive base-branch advance notifications for downstream jobs.
//!
//! When a PR merges into a base branch, in-flight jobs that were branched from
//! that base and touch overlapping files receive a queued direct message. The
//! message is non-waking: it is delivered at the recipient's next prompt/tool
//! boundary instead of resuming the job mechanically.

use std::collections::HashSet;
use std::path::Path;

use crate::messages::delivery::{latest_run_for_job, queue_system_direct};
use crate::models::ExecutionSnapshot;
use crate::orchestrator::Orchestrator;
use crate::services::GitClient;
use crate::storage::{DbError, DbResult, RowExt};
use turso::params;

#[derive(Debug)]
struct MergedJob {
    id: String,
    project_id: String,
    issue_id: Option<String>,
    base_branch: Option<String>,
    worktree_path: Option<String>,
}

#[derive(Debug)]
struct SiblingJob {
    id: String,
    worktree_path: String,
}

#[derive(Debug)]
struct MergeRequestInfo {
    pr_number: Option<i64>,
}

#[derive(Debug)]
struct IssueInfo {
    project_key: String,
    number: i64,
}

/// Queue non-waking notifications for in-flight siblings whose changes overlap
/// a merged job that advanced their shared base branch.
pub async fn notify_downstream_of_base_advance(
    orch: &Orchestrator,
    merged_job_id: &str,
) -> Result<(), String> {
    let Some(merged_job) = load_merged_job_for_owner(orch, merged_job_id).await? else {
        log::debug!(
            "Skipping base advance notify: no implementation job found for owner {}",
            merged_job_id
        );
        return Ok(());
    };
    let Some(base_branch) = merged_job.base_branch.as_deref() else {
        log::debug!(
            "Skipping base advance notify for job {}: no base_branch",
            merged_job.id
        );
        return Ok(());
    };
    let Some(merged_worktree) = merged_job.worktree_path.as_deref() else {
        log::debug!(
            "Skipping base advance notify for job {}: no worktree_path",
            merged_job.id
        );
        return Ok(());
    };

    let merged_files = changed_files(&*orch.services.git, Path::new(merged_worktree), base_branch)
        .map_err(|error| {
            log::warn!(
                "Failed to compute changed files for merged job {}: {}",
                merged_job.id,
                error
            );
            error
        })
        .ok();
    let mr_info = load_merge_request_info(orch, merged_job_id, &merged_job.id).await?;
    let issue_info = match merged_job.issue_id.as_deref() {
        Some(issue_id) => load_issue_info(orch, issue_id).await?,
        None => None,
    };
    let siblings =
        load_sibling_jobs(orch, &merged_job.project_id, base_branch, &merged_job.id).await?;

    for sibling in siblings {
        let sibling_files = changed_files(
            &*orch.services.git,
            Path::new(&sibling.worktree_path),
            base_branch,
        )
        .map_err(|error| {
            log::warn!(
                "Failed to compute changed files for sibling job {}: {}",
                sibling.id,
                error
            );
            error
        })
        .ok();

        let overlap = match (&merged_files, &sibling_files) {
            (Some(merged), Some(sibling)) => overlap_files(merged, sibling),
            _ => Vec::new(),
        };
        if merged_files.is_some() && sibling_files.is_some() && overlap.is_empty() {
            continue;
        }

        let Some(run_id) = latest_run_for_job(&orch.db.local, &sibling.id) else {
            log::debug!(
                "Skipping base advance notify for sibling job {}: no run",
                sibling.id
            );
            continue;
        };
        let note = build_note(
            base_branch,
            mr_info.as_ref().and_then(|info| info.pr_number),
            issue_info.as_ref(),
            merged_files.as_deref(),
            sibling_files.as_deref(),
            &overlap,
        );
        queue_system_direct(orch, &run_id, &note)?;
        log::info!(
            "Queued base branch advance notification for sibling job {} after merged job {}",
            sibling.id,
            merged_job.id
        );
    }

    Ok(())
}

fn changed_files(
    git: &dyn GitClient,
    worktree_path: &Path,
    base_branch: &str,
) -> Result<Vec<String>, String> {
    let output = git.run(
        worktree_path,
        vec![
            "diff".to_string(),
            "--name-only".to_string(),
            format!("{}...HEAD", base_branch),
        ],
    )?;
    if !output.success {
        return Err(format!("git diff failed: {}", output.stderr));
    }
    Ok(output
        .stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

fn overlap_files(merged_files: &[String], sibling_files: &[String]) -> Vec<String> {
    let sibling_set: HashSet<&str> = sibling_files.iter().map(String::as_str).collect();
    merged_files
        .iter()
        .filter(|file| sibling_set.contains(file.as_str()))
        .cloned()
        .collect()
}

fn build_note(
    base_branch: &str,
    pr_number: Option<i64>,
    issue_info: Option<&IssueInfo>,
    merged_files: Option<&[String]>,
    sibling_files: Option<&[String]>,
    overlap: &[String],
) -> String {
    let pr_fragment = pr_number
        .map(|number| format!("PR #{} merged", number))
        .unwrap_or_else(|| "A PR merged".to_string());
    let issue_fragment = issue_info
        .map(|issue| format!(" (cairn://p/{}/{})", issue.project_key, issue.number))
        .unwrap_or_default();
    let file_section = if merged_files.is_none() || sibling_files.is_none() {
        "Files it changed could not be determined; rebase to be safe.".to_string()
    } else {
        format!(
            "Files it changed that overlap yours: {}.",
            format_file_list(overlap)
        )
    };

    format!(
        "[Base branch update] Your base branch `{}` advanced — {}{}.\n{}\nRebase before opening/refreshing your PR:\n  git fetch origin {}\n  git rebase origin/{}\n  # resolve conflicts, then verify build + tests\nIf your work does not touch these, you can ignore this.",
        base_branch, pr_fragment, issue_fragment, file_section, base_branch, base_branch
    )
}

fn format_file_list(files: &[String]) -> String {
    if files.is_empty() {
        "none".to_string()
    } else {
        files
            .iter()
            .map(|file| format!("`{}`", file))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

async fn load_merged_job_for_owner(
    orch: &Orchestrator,
    owner_id: &str,
) -> Result<Option<MergedJob>, String> {
    if let Some(job) = load_job_by_id(orch, owner_id).await? {
        return Ok(Some(job));
    }

    let Some(action_run) = load_action_run_pr_owner(orch, owner_id).await? else {
        return Ok(None);
    };

    if let Some(parent_job_id) = action_run.parent_job_id.as_deref() {
        if let Some(job) = load_job_by_id(orch, parent_job_id).await? {
            if job.worktree_path.is_some() && job.base_branch.is_some() {
                return Ok(Some(job));
            }
        }
    }

    if let Some(job) =
        find_context_source_job(orch, &action_run.execution_id, &action_run.recipe_node_id).await?
    {
        return Ok(Some(job));
    }

    latest_complete_implementation_job(orch, &action_run.execution_id).await
}

#[derive(Debug)]
struct ActionRunOwner {
    execution_id: String,
    recipe_node_id: String,
    parent_job_id: Option<String>,
}

async fn load_job_by_id(orch: &Orchestrator, job_id: &str) -> Result<Option<MergedJob>, String> {
    let job_id = job_id.to_string();
    orch.db
        .local
        .read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move { load_job_by_id_conn(conn, &job_id).await })
        })
        .await
        .map_err(|error| error.to_string())
}

async fn load_job_by_id_conn(
    conn: &turso::Connection,
    job_id: &str,
) -> DbResult<Option<MergedJob>> {
    let mut rows = conn
        .query(
            "SELECT id, project_id, issue_id, base_branch, worktree_path
             FROM jobs
             WHERE id = ?1",
            params![job_id],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| {
            Ok(MergedJob {
                id: row.text(0)?,
                project_id: row.text(1)?,
                issue_id: row.opt_text(2)?,
                base_branch: row.opt_text(3)?,
                worktree_path: row.opt_text(4)?,
            })
        })
        .transpose()
}

async fn load_action_run_pr_owner(
    orch: &Orchestrator,
    owner_id: &str,
) -> Result<Option<ActionRunOwner>, String> {
    let owner_id = owner_id.to_string();
    orch.db
        .local
        .read(|conn| {
            let owner_id = owner_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT execution_id, recipe_node_id, parent_job_id
                         FROM action_runs
                         WHERE id = ?1",
                        params![owner_id.as_str()],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| {
                        Ok(ActionRunOwner {
                            execution_id: row.text(0)?,
                            recipe_node_id: row.text(1)?,
                            parent_job_id: row.opt_text(2)?,
                        })
                    })
                    .transpose()
            })
        })
        .await
        .map_err(|error| error.to_string())
}

async fn find_context_source_job(
    orch: &Orchestrator,
    execution_id: &str,
    pr_node_id: &str,
) -> Result<Option<MergedJob>, String> {
    let execution_id = execution_id.to_string();
    let pr_node_id = pr_node_id.to_string();
    orch.db
        .local
        .read(|conn| {
            let execution_id = execution_id.clone();
            let pr_node_id = pr_node_id.clone();
            Box::pin(async move {
                let snapshot = load_execution_snapshot_conn(conn, &execution_id).await?;
                for edge in snapshot.recipe.edges.iter().filter(|edge| {
                    edge.edge_type.to_string() == "context" && edge.target_node_id == pr_node_id
                }) {
                    let mut rows = conn
                        .query(
                            "SELECT id, project_id, issue_id, base_branch, worktree_path
                             FROM jobs
                             WHERE execution_id = ?1
                               AND recipe_node_id = ?2
                               AND worktree_path IS NOT NULL
                               AND branch IS NOT NULL
                               AND status <> 'cancelled'
                             ORDER BY created_at DESC
                             LIMIT 1",
                            params![execution_id.as_str(), edge.source_node_id.as_str()],
                        )
                        .await?;
                    if let Some(row) = rows.next().await? {
                        return Ok(Some(MergedJob {
                            id: row.text(0)?,
                            project_id: row.text(1)?,
                            issue_id: row.opt_text(2)?,
                            base_branch: row.opt_text(3)?,
                            worktree_path: row.opt_text(4)?,
                        }));
                    }
                }
                Ok(None)
            })
        })
        .await
        .map_err(|error| error.to_string())
}

async fn latest_complete_implementation_job(
    orch: &Orchestrator,
    execution_id: &str,
) -> Result<Option<MergedJob>, String> {
    let execution_id = execution_id.to_string();
    orch.db
        .local
        .read(|conn| {
            let execution_id = execution_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, project_id, issue_id, base_branch, worktree_path
                         FROM jobs
                         WHERE execution_id = ?1
                           AND worktree_path IS NOT NULL
                           AND branch IS NOT NULL
                           AND status = 'complete'
                         ORDER BY completed_at DESC, updated_at DESC
                         LIMIT 1",
                        params![execution_id.as_str()],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| {
                        Ok(MergedJob {
                            id: row.text(0)?,
                            project_id: row.text(1)?,
                            issue_id: row.opt_text(2)?,
                            base_branch: row.opt_text(3)?,
                            worktree_path: row.opt_text(4)?,
                        })
                    })
                    .transpose()
            })
        })
        .await
        .map_err(|error| error.to_string())
}

async fn load_execution_snapshot_conn(
    conn: &turso::Connection,
    execution_id: &str,
) -> DbResult<ExecutionSnapshot> {
    let mut rows = conn
        .query(
            "SELECT snapshot FROM executions WHERE id = ?1",
            params![execution_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Err(DbError::Row("execution not found".to_string()));
    };
    let Some(snapshot_json) = row.opt_text(0)? else {
        return Err(DbError::Row("execution has no snapshot".to_string()));
    };
    ExecutionSnapshot::from_json(&snapshot_json).map_err(|error| DbError::Row(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::services::testing::{MockGitClient, TestServicesBuilder};
    use crate::services::GitOutput;
    use crate::storage::{LocalDb, SearchIndex};
    use std::path::Path;
    use std::sync::Arc;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("base-advance-test.db").await
    }

    fn test_orchestrator(db: LocalDb, git: MockGitClient) -> Orchestrator {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.keep();
        let index_path = config_dir.join("search-index.db");
        let db_state = Arc::new(DbState::new(
            Arc::new(db),
            Arc::new(SearchIndex::open_or_create(index_path).unwrap()),
        ));
        let services = Arc::new(TestServicesBuilder::new().with_git(git).build());
        Orchestrator::builder(db_state, services, config_dir).build()
    }

    async fn seed_base_advance_fixture(db: &LocalDb) {
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj-1', 'default', 'Project', 'PROJ', '/repo', 'main', 1, 1)",
                    (),
                )
                .await?;
                for (id, number) in [
                    ("issue-1", 1_i64),
                    ("issue-2", 2_i64),
                    ("issue-3", 3_i64),
                    ("issue-4", 4_i64),
                ] {
                    conn.execute(
                        "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                         VALUES (?1, 'proj-1', ?2, 'Issue', 'active', 1, 1)",
                        params![id, number],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                         VALUES (?1, 'recipe-default', ?2, 'proj-1', 'running', 1, 1)",
                        params![format!("exec-{}", number).as_str(), id],
                    )
                    .await?;
                }
                for (job, exec, issue, status, worktree) in [
                    ("job-merged", "exec-1", "issue-1", "complete", "/wt/merged"),
                    ("job-overlap", "exec-2", "issue-2", "running", "/wt/overlap"),
                    ("job-clean", "exec-3", "issue-3", "running", "/wt/clean"),
                    ("job-complete", "exec-4", "issue-4", "complete", "/wt/complete"),
                ] {
                    conn.execute(
                        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, worktree_path, base_branch, created_at, updated_at)
                         VALUES (?1, ?2, 'node', ?3, 'proj-1', ?4, ?5, 'integration', 1, 1)",
                        params![job, exec, issue, status, worktree],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
                         VALUES (?1, ?2, 'proj-1', ?3, 'live', 1, 1)",
                        params![format!("run-{}", job).as_str(), issue, job],
                    )
                    .await?;
                }
                conn.execute(
                    "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at, github_pr_number)
                     VALUES ('mr-1', 'job-merged', 'proj-1', 'issue-1', 'PR', 'feature', 'integration', 'merged', 1, 1, 42)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn notifies_only_in_flight_siblings_with_overlapping_files() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;

        let mut git = MockGitClient::new();
        git.expect_run()
            .returning(|repo: &Path, _args: Vec<String>| {
                let stdout = match repo.to_string_lossy().as_ref() {
                    "/wt/merged" => "src/shared.rs\nsrc/merged_only.rs",
                    "/wt/overlap" => "src/shared.rs\nsrc/overlap_only.rs",
                    "/wt/clean" => "src/clean.rs",
                    other => panic!("unexpected git diff repo {other}"),
                };
                Ok(GitOutput {
                    success: true,
                    stdout: stdout.to_string(),
                    stderr: String::new(),
                })
            });
        let orch = test_orchestrator(db, git);

        notify_downstream_of_base_advance(&orch, "job-merged")
            .await
            .unwrap();

        let messages: Vec<(String, String, Option<i64>)> = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT recipient_run_id, content, delivered_at FROM messages ORDER BY created_at",
                            (),
                        )
                        .await?;
                    let mut messages = Vec::new();
                    while let Some(row) = rows.next().await? {
                        messages.push((row.text(0)?, row.text(1)?, row.opt_i64(2)?));
                    }
                    Ok::<_, DbError>(messages)
                })
            })
            .await
            .unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].0, "run-job-overlap");
        assert!(messages[0].1.contains("[Base branch update]"));
        assert!(messages[0].1.contains("PR #42 merged"));
        assert!(messages[0].1.contains("cairn://p/PROJ/1"));
        assert!(messages[0].1.contains("`src/shared.rs`"));
        assert!(messages[0].2.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolves_action_run_owner_to_implementation_job() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        db.execute_script(
            "
            INSERT INTO action_runs (id, execution_id, recipe_node_id, action_config_id, issue_id, project_id, status, parent_job_id, created_at)
            VALUES ('action-pr', 'exec-1', 'pr-node', 'builtin:create_pr', 'issue-1', 'proj-1', 'blocked', 'job-merged', 1);
            UPDATE merge_requests SET job_id = 'action-pr' WHERE id = 'mr-1';
            ",
        )
        .await
        .unwrap();

        let mut git = MockGitClient::new();
        git.expect_run()
            .returning(|repo: &Path, _args: Vec<String>| {
                let stdout = match repo.to_string_lossy().as_ref() {
                    "/wt/merged" => "src/shared.rs",
                    "/wt/overlap" => "src/shared.rs",
                    "/wt/clean" => "src/clean.rs",
                    other => panic!("unexpected git diff repo {other}"),
                };
                Ok(GitOutput {
                    success: true,
                    stdout: stdout.to_string(),
                    stderr: String::new(),
                })
            });
        let orch = test_orchestrator(db, git);

        notify_downstream_of_base_advance(&orch, "action-pr")
            .await
            .unwrap();

        let content: String = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn.query("SELECT content FROM messages", ()).await?;
                    let row = rows.next().await?.unwrap();
                    row.text(0)
                })
            })
            .await
            .unwrap();
        assert!(content.contains("PR #42 merged"));
        assert!(content.contains("`src/shared.rs`"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sends_when_a_diff_cannot_be_computed() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;

        let mut git = MockGitClient::new();
        git.expect_run()
            .returning(
                |repo: &Path, _args: Vec<String>| match repo.to_string_lossy().as_ref() {
                    "/wt/merged" => Ok(GitOutput {
                        success: true,
                        stdout: "src/shared.rs".to_string(),
                        stderr: String::new(),
                    }),
                    "/wt/overlap" | "/wt/clean" => Err("missing worktree".to_string()),
                    other => panic!("unexpected git diff repo {other}"),
                },
            );
        let orch = test_orchestrator(db, git);

        notify_downstream_of_base_advance(&orch, "job-merged")
            .await
            .unwrap();

        let count: i64 = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn.query("SELECT COUNT(*) FROM messages", ()).await?;
                    let row = rows.next().await?.unwrap();
                    row.i64(0)
                })
            })
            .await
            .unwrap();
        assert_eq!(count, 2);
    }
}

async fn load_sibling_jobs(
    orch: &Orchestrator,
    project_id: &str,
    base_branch: &str,
    merged_job_id: &str,
) -> Result<Vec<SiblingJob>, String> {
    let project_id = project_id.to_string();
    let base_branch = base_branch.to_string();
    let merged_job_id = merged_job_id.to_string();
    orch.db
        .local
        .read(|conn| {
            let project_id = project_id.clone();
            let base_branch = base_branch.clone();
            let merged_job_id = merged_job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, worktree_path
                         FROM jobs
                         WHERE project_id = ?1
                           AND base_branch = ?2
                           AND id != ?3
                           AND status NOT IN ('complete', 'failed')
                           AND worktree_path IS NOT NULL",
                        params![
                            project_id.as_str(),
                            base_branch.as_str(),
                            merged_job_id.as_str()
                        ],
                    )
                    .await?;
                let mut siblings = Vec::new();
                while let Some(row) = rows.next().await? {
                    siblings.push(SiblingJob {
                        id: row.text(0)?,
                        worktree_path: row.text(1)?,
                    });
                }
                Ok(siblings)
            })
        })
        .await
        .map_err(|error| error.to_string())
}

async fn load_merge_request_info(
    orch: &Orchestrator,
    owner_id: &str,
    implementation_job_id: &str,
) -> Result<Option<MergeRequestInfo>, String> {
    let owner_id = owner_id.to_string();
    let implementation_job_id = implementation_job_id.to_string();
    orch.db
        .local
        .read(|conn| {
            let owner_id = owner_id.clone();
            let implementation_job_id = implementation_job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT github_pr_number
                         FROM merge_requests
                         WHERE job_id = ?1 OR job_id = ?2
                         ORDER BY CASE WHEN job_id = ?1 THEN 0 ELSE 1 END
                         LIMIT 1",
                        params![owner_id.as_str(), implementation_job_id.as_str()],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| {
                        Ok(MergeRequestInfo {
                            pr_number: row.opt_i64(0)?,
                        })
                    })
                    .transpose()
            })
        })
        .await
        .map_err(|error| error.to_string())
}

async fn load_issue_info(orch: &Orchestrator, issue_id: &str) -> Result<Option<IssueInfo>, String> {
    let issue_id = issue_id.to_string();
    orch.db
        .local
        .read(|conn| {
            let issue_id = issue_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT p.key, i.number
                         FROM issues i
                         JOIN projects p ON p.id = i.project_id
                         WHERE i.id = ?1",
                        params![issue_id.as_str()],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| {
                        Ok(IssueInfo {
                            project_key: row.text(0)?,
                            number: row.i64(1)?,
                        })
                    })
                    .transpose()
            })
        })
        .await
        .map_err(|error| error.to_string())
}
