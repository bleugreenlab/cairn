//! Sync an edited `create-pr` artifact's prose into the PR it already produced.

use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use cairn_db::turso::params;

use super::context::{db_error, query_mr_context_for_create_pr_artifact_job};

/// Update the local merge-request prose cache after a create-pr artifact edit
/// has become the PR's source of truth.
async fn update_merge_request_title_body(
    db: &LocalDb,
    mr_id: &str,
    title: &str,
    body: Option<&str>,
    now: i64,
) -> Result<(), String> {
    let mr_id = mr_id.to_string();
    let title = title.to_string();
    let body = body.map(ToOwned::to_owned);
    db.write(|conn| {
        let mr_id = mr_id.clone();
        let title = title.clone();
        let body = body.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE merge_requests
                 SET title = ?1, body = ?2, updated_at = ?3
                 WHERE id = ?4",
                params![title.as_str(), body.as_deref(), now, mr_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to update merge request title/body", e))
}

fn gh_pr_edit_args<'a>(pr_number: &'a str, title: &'a str, body: &'a str) -> [&'a str; 7] {
    ["pr", "edit", pr_number, "--title", title, "--body", body]
}

async fn edit_github_pr_title_body(
    repo_path: &str,
    pr_number: i32,
    title: &str,
    body: Option<&str>,
) -> Result<(), String> {
    let pr_number = pr_number.to_string();
    let body = body.unwrap_or("");
    let output = crate::env::gh()
        .args(gh_pr_edit_args(&pr_number, title, body))
        .current_dir(repo_path)
        .output()
        .map_err(|e| format!("Failed to run gh pr edit: {e}"))?;
    if output.status.success() {
        return Ok(());
    }
    Err(format!(
        "Failed to update GitHub PR #{pr_number}: {}",
        String::from_utf8_lossy(&output.stderr)
    ))
}

/// The branch a merge-request row publishes from.
async fn source_branch_for_merge_request(db: &LocalDb, mr_id: &str) -> Result<String, String> {
    db.query_text(
        "SELECT source_branch FROM merge_requests WHERE id = ?1",
        (mr_id.to_string(),),
    )
    .await
    .map_err(|error| format!("Failed to resolve merge request source branch: {error}"))?
    .filter(|branch| !branch.trim().is_empty())
    .ok_or_else(|| format!("merge request {mr_id} has no source branch to publish"))
}

/// Publish the branch behind an already-open PR before its prose is re-synced.
///
/// Re-writing a `create-pr` artifact is the sanctioned trigger for publishing a
/// builder's branch, and the affordance docs tell agents so. Without this the
/// artifact edit moved only the PR's title and body: a builder that pushed
/// review fixes and re-saved its artifact left the PR presenting the pre-fix
/// tree as its reviewable, mergeable version, and a coordinator merging on first
/// sight of the artifact ships code the review already invalidated.
///
/// Fails closed. This is external state other people and agents act on, so
/// "could not publish" must not be reported as a completed sync.
async fn publish_branch_for_artifact_sync(
    orch: &Orchestrator,
    repo_path: &str,
    mr_id: &str,
) -> Result<(), String> {
    let branch = source_branch_for_merge_request(&orch.db.local, mr_id).await?;
    let store = crate::jj::project_store_dir(&orch.config_dir, std::path::Path::new(repo_path));
    crate::orchestrator::base_advance::publish_managed_branch(orch, &store, &branch)
        .await
        .map_err(|error| {
            format!(
            "Failed to publish `{branch}` before syncing the create-pr artifact: {error}. The pull \
             request still presents its previous head, so the artifact was not synced."
        )
        })
}

/// Sync an edited `create-pr` artifact into the already-open PR it produced:
/// publish the branch, then update the prose.
///
/// A builder can rewrite its `create-pr` artifact after a PR node/action has
/// opened the live PR. In that case the artifact is the source of truth the
/// reviewer should see — both its *tree* and its prose — so the branch is
/// published to origin first and only then are GitHub and the local
/// `merge_requests` cache updated.
pub(crate) async fn sync_create_pr_artifact_for_job(
    orch: &Orchestrator,
    job_id: &str,
    title: &str,
    body: Option<&str>,
) -> Result<bool, String> {
    let job_id = job_id.to_string();
    let Some(mr_context) = orch
        .db
        .local
        .read(|conn| {
            let job_id = job_id.clone();
            Box::pin(
                async move { query_mr_context_for_create_pr_artifact_job(conn, &job_id).await },
            )
        })
        .await
        .map_err(|e| db_error("Failed to resolve merge request", e))?
    else {
        return Ok(false);
    };

    // Only a bound remote PR has a head to advance; a local-only change has no
    // origin to publish to and nothing external reading it.
    if let Some(pr_number) = mr_context.github_pr_number {
        publish_branch_for_artifact_sync(orch, &mr_context.repo_path, &mr_context.mr_id).await?;
        edit_github_pr_title_body(&mr_context.repo_path, pr_number, title, body).await?;
    }

    let now = chrono::Utc::now().timestamp();
    update_merge_request_title_body(&orch.db.local, &mr_context.mr_id, title, body, now).await?;
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "merge_requests", "action": "update"}),
    );
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pr_data::actions::test_support::{
        migrated_db, seed_local_open_merge_request, seed_pr_node_merge_request_for_artifact_job,
        seed_remote_open_merge_request, test_orchestrator,
    };
    use crate::services::testing::MockGitClient;
    use crate::storage::RowExt;

    #[test]
    fn gh_pr_edit_args_include_title_and_body() {
        assert_eq!(
            gh_pr_edit_args("1538", "New title", "New body"),
            [
                "pr",
                "edit",
                "1538",
                "--title",
                "New title",
                "--body",
                "New body"
            ]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_create_pr_artifact_updates_local_merge_request_cache() {
        let db = migrated_db().await;
        seed_local_open_merge_request(&db, "owner-job").await;
        let orch = test_orchestrator(db, MockGitClient::new());

        let synced = sync_create_pr_artifact_for_job(
            &orch,
            "owner-job",
            "New archive semantics",
            Some("Updated PR body"),
        )
        .await
        .unwrap();

        assert!(synced);
        let row: (String, String) = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT title, body FROM merge_requests WHERE id = 'mr-local'",
                            (),
                        )
                        .await?;
                    let row = rows.next().await?.unwrap();
                    Ok((row.text(0)?, row.text(1)?))
                })
            })
            .await
            .unwrap();
        assert_eq!(row.0, "New archive semantics");
        assert_eq!(row.1, "Updated PR body");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_create_pr_artifact_follows_pr_node_owner() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());

        let synced = sync_create_pr_artifact_for_job(
            &orch,
            "builder-job",
            "New PR-node title",
            Some("New PR-node body"),
        )
        .await
        .unwrap();

        assert!(synced);
        let row: (String, String) = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT title, body FROM merge_requests WHERE id = 'mr-pr-node'",
                            (),
                        )
                        .await?;
                    let row = rows.next().await?.unwrap();
                    Ok((row.text(0)?, row.text(1)?))
                })
            })
            .await
            .unwrap();
        assert_eq!(row.0, "New PR-node title");
        assert_eq!(row.1, "New PR-node body");
    }

    /// Re-saving the artifact is the sanctioned trigger for publishing a
    /// builder's branch, so a sync that cannot publish must NOT report success.
    /// Reporting `Ok` here is how two pull requests came to present their
    /// pre-review trees as the reviewable version while their branches carried
    /// the fixes.
    #[tokio::test(flavor = "current_thread")]
    async fn sync_create_pr_artifact_fails_closed_when_the_branch_cannot_be_published() {
        let db = migrated_db().await;
        let repo = tempfile::tempdir().unwrap();
        seed_remote_open_merge_request(&db, "owner-job", &repo.path().to_string_lossy()).await;
        let orch = test_orchestrator(db, MockGitClient::new());

        let error = sync_create_pr_artifact_for_job(&orch, "owner-job", "Title", Some("Body"))
            .await
            .expect_err("an unpublishable branch must not report a completed sync");

        assert!(
            error.contains("Failed to publish") && error.contains("agent/PROJ-4-builder-0"),
            "the failure must name the branch it could not publish: {error}"
        );
        assert!(
            error.contains("was not synced"),
            "the failure must say the artifact did not land: {error}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_create_pr_artifact_noops_without_merge_request() {
        let db = migrated_db().await;
        let orch = test_orchestrator(db, MockGitClient::new());

        let synced = sync_create_pr_artifact_for_job(&orch, "missing-job", "Title", Some("Body"))
            .await
            .unwrap();

        assert!(!synced);
    }
}
