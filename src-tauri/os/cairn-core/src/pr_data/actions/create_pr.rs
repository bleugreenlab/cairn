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

/// Resolve the branch a merge request publishes from and push it through the
/// recovering wrapper every managed push uses. Returns the branch published;
/// the error names the branch.
async fn publish_merge_request_branch(
    orch: &Orchestrator,
    repo_path: &str,
    mr_id: &str,
) -> Result<String, String> {
    let branch = source_branch_for_merge_request(&orch.db.local, mr_id).await?;
    let store = crate::jj::project_store_dir(&orch.config_dir, std::path::Path::new(repo_path));
    crate::orchestrator::base_advance::publish_managed_branch(orch, &store, &branch)
        .await
        .map_err(|error| format!("could not publish `{branch}`: {error}"))?;
    Ok(branch)
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
    publish_merge_request_branch(orch, repo_path, mr_id)
        .await
        .map(|_| ())
        .map_err(|error| {
            format!(
                "Failed to publish before syncing the create-pr artifact: {error}. The pull \
                 request still presents its previous head, so the artifact was not synced."
            )
        })
}

/// What a failed `action:"publish"` leaves to do. A convergence refusal means
/// origin holds history this branch does not share, which Cairn will not
/// overwrite on its own; anything else is a cause the reply has just named.
fn publish_failure_remedy(error: &str) -> &'static str {
    if error.contains("stale publication recovery refused") {
        "GitHub holds a copy of this branch whose history this branch does not share, so Cairn \
         will not overwrite it. A person needs to decide which history the pull request should \
         carry."
    } else {
        "The pull request still shows its previous head. Patch this artifact with \
         action:\"publish\" again once the cause above is cleared."
    }
}

/// The merge request's recorded lifecycle, read before anything is pushed.
async fn merge_request_is_recorded_open(db: &LocalDb, mr_id: &str) -> Result<bool, String> {
    let mr_id = mr_id.to_string();
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT 1 FROM merge_requests
                      WHERE id = ?1
                        AND status NOT IN ('merged', 'closed')
                        AND (github_state IS NULL
                             OR lower(github_state) NOT IN ('merged', 'closed'))",
                    params![mr_id.as_str()],
                )
                .await?;
            Ok(rows.next().await?.is_some())
        })
    })
    .await
    .map_err(|error| db_error("Failed to read the merge request's state", error))
}

/// What a live re-read after the push establishes about the pull request.
struct RefreshedAfterPublish<'a> {
    summary: String,
    /// The refresh's own comparison of the PR head with the branch head. It is
    /// `Unknown` for a pull request no longer open, and whenever the comparison
    /// could not run.
    heads: &'a crate::pr_data::publication::HeadComparison,
}

/// The reply to a successful push. It claims the pull request holds the
/// branch only when the refresh's comparison found the heads `Equal`; otherwise
/// it says what the push did and keeps the divergence, or the uncertainty, the
/// refresh found. The branch can move again between the push and the re-read
/// (a seal, a base advance), so the pushed commit is named from origin, not
/// from the bookmark.
fn publish_outcome_message(
    branch: &str,
    origin_tip: Option<&str>,
    refreshed: Result<RefreshedAfterPublish<'_>, String>,
) -> String {
    use crate::pr_data::publication::HeadComparison;

    let short = |commit: &str| commit.chars().take(12).collect::<String>();
    let pushed = match origin_tip {
        Some(tip) => format!("Pushed `{branch}`; origin holds {}", short(tip)),
        None => format!("Pushed `{branch}`; origin's tip could not be read back"),
    };
    match refreshed {
        Err(error) => format!(
            "{pushed}, but re-reading the pull request failed: {error}. Whether it shows the \
             pushed head is unknown; patch with action:\"refresh\" to re-read it."
        ),
        Ok(RefreshedAfterPublish {
            heads: HeadComparison::Divergent(divergence),
            ..
        }) => format!(
            "{pushed}. {} The branch may have moved again during the push, or GitHub has not \
             caught up yet; read the artifact again before acting on the pull request's signals.",
            divergence.note(branch)
        ),
        Ok(RefreshedAfterPublish {
            heads: HeadComparison::Unknown,
            summary,
        }) => format!(
            "{pushed}. The re-read of {summary} could not compare its head with the branch, so \
             whether it shows the pushed head is unknown."
        ),
        Ok(RefreshedAfterPublish {
            heads: HeadComparison::Equal(head),
            summary,
        }) => format!(
            "{pushed}, and {summary} holds {}, the branch's current head.",
            short(head)
        ),
    }
}

/// `action:"publish"` on a PR artifact: publish the branch behind the job's
/// OPEN pull request at its current head, then re-read the pull request and
/// report what it verifiably holds.
///
/// This is the lever an agent reaches for when the artifact warns that the pull
/// request holds an older head than the branch, instead of committing again to
/// force a push. The publish self-heals a stale or untracked origin copy of the
/// branch that shares its history, and refuses one that does not.
///
/// Refused without pushing unless the pull request is bound and open, both as
/// recorded and as GitHub reports it live: publishing the branch of a merged or
/// closed pull request could recreate a remote branch removed on purpose.
///
/// A commit barrier first: commits earlier batches sealed on the branch but
/// could not publish are republished, so the push carries them.
pub(crate) async fn publish_pr_branch_for_job(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<String, String> {
    let barrier = crate::mcp::handlers::run::held::job_commit_barrier(orch, job_id).await;
    publish_pr_branch_past_barrier(orch, job_id)
        .await
        .map(|reply| barrier.prefix(reply))
        .map_err(|error| barrier.prefix(error))
}

async fn publish_pr_branch_past_barrier(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<String, String> {
    use crate::models::PrState;

    let mr = crate::pr_data::actions::try_resolve_mr_context_for_job(&orch.db.local, job_id)
        .await?
        .ok_or_else(|| {
            format!("job {job_id} has no pull request whose branch could be published")
        })?;
    if mr.is_local {
        return Err(
            "this change was opened without a GitHub remote, so there is nothing to publish; it \
             merges through Cairn's store"
                .to_string(),
        );
    }
    if mr.github_pr_number.is_none() {
        return Err(
            "no pull request is bound to this change yet, so there is no pull request head to \
             advance; patch with action:\"refresh\" to bind one that exists"
                .to_string(),
        );
    }
    if !merge_request_is_recorded_open(&orch.db.local, &mr.mr_id).await? {
        return Err(
            "this pull request is merged or closed, so its branch was not published".to_string(),
        );
    }
    let live = crate::pr_data::actions::refresh::pr_snapshot_for_job(orch, &mr.job_id, true)
        .await
        .map_err(|error| {
            format!(
                "could not confirm the pull request is still open, so its branch was not \
                 published: {error}"
            )
        })?;
    if live.unbound.is_some() || !matches!(live.cache.state, PrState::Open) {
        return Err(format!(
            "GitHub reports this pull request as {}, so its branch was not published",
            live.cache.state
        ));
    }

    let branch = publish_merge_request_branch(orch, &mr.repo_path, &mr.mr_id)
        .await
        .map_err(|error| format!("{error}. {}", publish_failure_remedy(&error)))?;

    let origin_tip = crate::pr_data::publication::origin_branch_tip(
        &*orch.services.git,
        std::path::Path::new(&mr.repo_path),
        &branch,
    )
    .ok()
    .flatten();
    orch.invalidate_pr_refresh(&mr.job_id, "branch-published");
    let refreshed =
        crate::pr_data::actions::refresh::pr_snapshot_for_job(orch, &mr.job_id, true).await;
    Ok(publish_outcome_message(
        &branch,
        origin_tip.as_deref(),
        refreshed
            .as_deref()
            .map(|snapshot| RefreshedAfterPublish {
                summary: crate::pr_data::publication::refreshed_summary(&snapshot.cache),
                heads: &snapshot.heads,
            })
            .map_err(|error| error.to_string()),
    ))
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
    orch.invalidate_pr_refresh(&mr_context.job_id, "pull-request-title-or-body");
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
            error.contains("Failed to publish") && error.contains("agent/proj-4-builder-0"),
            "the failure must name the branch it could not publish: {error}"
        );
        assert!(
            error.contains("was not synced"),
            "the failure must say the artifact did not land: {error}"
        );
    }

    /// A merged or closed pull request's branch is never published: the refusal
    /// comes from the recorded state, before any push is attempted (reaching
    /// the push here would fail with "could not publish", since the fixture has
    /// no store).
    #[tokio::test(flavor = "current_thread")]
    async fn publish_refuses_a_merged_or_closed_pull_request_without_pushing() {
        for (column, value) in [
            ("status", "closed"),
            ("status", "merged"),
            ("github_state", "MERGED"),
            ("github_state", "CLOSED"),
        ] {
            let db = migrated_db().await;
            let repo = tempfile::tempdir().unwrap();
            seed_remote_open_merge_request(&db, "owner-job", &repo.path().to_string_lossy()).await;
            db.execute(
                &format!("UPDATE merge_requests SET {column} = ?1 WHERE id = 'mr-remote'"),
                params![value],
            )
            .await
            .unwrap();
            let orch = test_orchestrator(db, MockGitClient::new());

            let error = publish_pr_branch_for_job(&orch, "owner-job")
                .await
                .expect_err("a merged or closed pull request must not be published");
            assert!(
                error.contains("merged or closed") && error.contains("not published"),
                "{column}={value}: {error}"
            );
            assert!(
                !error.contains("could not publish"),
                "{column}={value}: the refusal must precede any push: {error}"
            );
        }
    }

    /// Without a bound pull request there is no head to advance.
    #[tokio::test(flavor = "current_thread")]
    async fn publish_refuses_an_unbound_change_without_pushing() {
        let db = migrated_db().await;
        let repo = tempfile::tempdir().unwrap();
        seed_remote_open_merge_request(&db, "owner-job", &repo.path().to_string_lossy()).await;
        db.execute(
            "UPDATE merge_requests SET github_pr_number = NULL WHERE id = 'mr-remote'",
            (),
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db, MockGitClient::new());

        let error = publish_pr_branch_for_job(&orch, "owner-job")
            .await
            .expect_err("an unbound change has no pull request head to advance");
        assert!(error.contains("no pull request is bound"), "{error}");
    }

    fn reply_for(origin_tip: &str, heads: &crate::pr_data::publication::HeadComparison) -> String {
        publish_outcome_message(
            "agent/proj-1-builder-0",
            Some(origin_tip),
            Ok(RefreshedAfterPublish {
                summary: "PR #7 (state OPEN, +1 -0)".to_string(),
                heads,
            }),
        )
    }

    /// The branch advanced from A to B after A was pushed, and the refreshed
    /// snapshot still reports the pull request at A. The reply names what origin
    /// holds, keeps the divergence warning, and does not claim the pull request
    /// holds the branch.
    #[test]
    fn a_branch_that_moved_after_the_push_is_not_reported_as_published() {
        let a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let heads = crate::pr_data::publication::classify_heads(a, Some(b.to_string()), |_| true);
        let message = reply_for(a, &heads);
        assert!(message.contains("origin holds aaaaaaaaaaaa"), "{message}");
        assert!(message.contains("bbbbbbbbbbbb"), "{message}");
        assert!(
            message.contains("describes the version the pull request holds"),
            "{message}"
        );
        assert!(!message.contains("the branch's current head"), "{message}");
    }

    /// The refresh's comparison could not run: branch metadata exists, but
    /// GitHub reported an empty head or the store could not read the bookmark.
    /// Built through the production classifier, the reply says unknown and
    /// never claims convergence.
    #[test]
    fn an_uncomparable_refresh_reports_unknown_not_convergence() {
        let tip = "cccccccccccccccccccccccccccccccccccccccc";
        for heads in [
            crate::pr_data::publication::classify_heads("", Some(tip.to_string()), |_| true),
            crate::pr_data::publication::classify_heads(tip, None, |_| true),
        ] {
            let message = reply_for(tip, &heads);
            assert!(message.contains("unknown"), "{heads:?}: {message}");
            assert!(
                !message.contains("the branch's current head"),
                "{heads:?}: {message}"
            );
        }
    }

    /// Convergence is claimed only from an `Equal` comparison, and a failed
    /// re-read claims nothing about the PR head.
    #[test]
    fn publish_claims_convergence_only_from_an_equal_comparison() {
        let tip = "cccccccccccccccccccccccccccccccccccccccc";
        let equal = crate::pr_data::publication::classify_heads(
            tip,
            Some(tip.to_string()),
            |_| unreachable!(),
        );
        let agreed = reply_for(tip, &equal);
        assert!(
            agreed.contains("holds cccccccccccc, the branch's current head"),
            "{agreed}"
        );

        let failed = publish_outcome_message("b", None, Err("offline".to_string()));
        assert!(failed.contains("could not be read back"), "{failed}");
        assert!(
            failed.contains("offline") && failed.contains("unknown"),
            "{failed}"
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
