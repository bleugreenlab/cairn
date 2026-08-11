use std::sync::Arc;

use cairn_common::protocol::CallbackRequest;

use super::RunContext;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

fn required_run_id(request: &CallbackRequest) -> Result<&str, String> {
    request
        .run_id
        .as_deref()
        .filter(|run_id| !run_id.is_empty())
        .ok_or_else(|| "Authenticated agent request is missing its run ID".to_string())
}

pub(crate) async fn lookup_run(
    db: &LocalDb,
    request: &CallbackRequest,
) -> Result<RunContext, String> {
    lookup_run_by_id(db, required_run_id(request)?).await
}

pub(crate) async fn lookup_home_uri(
    db: &LocalDb,
    request: &CallbackRequest,
) -> Result<String, String> {
    lookup_home_uri_by_run_id(db, required_run_id(request)?).await
}

/// Resolve a run's context and the database that owns it (CAIRN-2132).
///
/// A run id is a globally-unique UUID and a project lives wholly in one
/// database, so a run appears in at most one open database. This probes the
/// private database first and short-circuits on a hit — the overwhelming
/// majority of installs have no team databases and most runs are local — and
/// only fans out across open team replicas on a private-DB miss. With no team
/// DBs open it degenerates to a single private-DB lookup: a strict no-op for
/// local-only installs. Returns the owning `Arc<LocalDb>` so every downstream
/// write for the request targets the right replica instead of defaulting to the
/// private database (which would silently misroute a shared-project run).
pub(crate) async fn lookup_run_routed(
    dbs: &crate::db::DbState,
    request: &CallbackRequest,
) -> Result<(RunContext, Arc<LocalDb>), String> {
    let run_id = required_run_id(request)?;
    let db = crate::execution::routing::routing_db_for_id(dbs, run_id)
        .await
        .map_err(|e| e.to_string())?;
    let ctx = lookup_run_by_id(&db, run_id).await?;
    Ok((ctx, db))
}

/// The home base URI for the request's run, searching every open database the
/// same way [`lookup_run_routed`] does (CAIRN-2132). Resolves `cairn:~/...`
/// targets for a run whose rows live in a team replica.
pub(crate) async fn lookup_home_uri_routed(
    dbs: &crate::db::DbState,
    request: &CallbackRequest,
) -> Result<String, String> {
    let run_id = required_run_id(request)?;
    let db = crate::execution::routing::routing_db_for_id(dbs, run_id)
        .await
        .map_err(|e| e.to_string())?;
    lookup_home_uri_by_run_id(&db, run_id).await
}

async fn lookup_home_uri_by_run_id(db: &LocalDb, run_id: &str) -> Result<String, String> {
    let run_id = run_id.to_string();
    db.read(|conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT job_id FROM runs WHERE id = ?1 LIMIT 1",
                    (run_id.as_str(),),
                )
                .await?;

            let Some(row) = rows.next().await? else {
                return Err(DbError::Row(format!("No run found with id '{}'", run_id)));
            };
            let job_id = row.text(0)?;
            crate::jobs::queries::home_uri_for_job_conn(conn, &job_id)
                .await?
                .ok_or_else(|| DbError::Row(format!("Cannot build home URI for run {run_id}")))
        })
    })
    .await
    .map_err(|e| e.to_string())
}

pub(crate) async fn lookup_run_by_id(db: &LocalDb, run_id: &str) -> Result<RunContext, String> {
    let run_id = run_id.to_string();
    db.read(|conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "
                    SELECT r.id, r.job_id, j.execution_id, j.recipe_node_id,
                           r.issue_id, i.number, j.project_id, p.key, j.node_name,
                           e.seq, j.agent_config_id
                    FROM runs r
                    JOIN jobs j ON r.job_id = j.id
                    LEFT JOIN issues i ON r.issue_id = i.id
                    JOIN projects p ON j.project_id = p.id
                    LEFT JOIN executions e ON j.execution_id = e.id
                    WHERE r.id = ?1
                    LIMIT 1
                    ",
                    (run_id.as_str(),),
                )
                .await?;

            rows.next()
                .await?
                .map(|row| run_context_from_row(&row))
                .transpose()?
                .ok_or_else(|| DbError::Row(format!("No run found with id '{}'", run_id)))
        })
    })
    .await
    .map_err(|e| e.to_string())
}

fn run_context_from_row(row: &cairn_db::turso::Row) -> DbResult<RunContext> {
    let issue_number = row.opt_i64(5)?.map(|value| value as i32);
    let exec_seq = row.opt_i64(9)?.map(|value| value as i32);
    let job_name = row.opt_text(8)?;

    Ok(RunContext {
        run_id: row.text(0)?,
        job_id: row.opt_text(1)?.unwrap_or_default(),
        exec_seq,
        issue_id: row.opt_text(4)?,
        issue_number,
        project_id: row.text(6)?,
        project_key: row.text(7)?,
        job_name,
        agent_config_id: row.opt_text(10)?,
    })
}

/// Why an authenticated batch whose run cannot be resolved may not commit.
///
/// Phrased once, beside the gate that returns it, so both commit verbs answer a
/// caller in the same words.
fn unresolvable_identity_refusal(error: &str) -> String {
    format!(
        "Refusing to commit: this batch carries a run identity Cairn cannot resolve, so the \
         posture that decides whether a commit may be taken — whether the job owns a branch of \
         its own or is thread-owned and running directly on the project's base branch — is \
         unknown. A commit is not taken on an unknown posture. Re-send the batch without \
         commit_msg; reading, running commands, and scratch files are unaffected. ({error})"
    )
}

/// The commit-posture gate both commit verbs take before they act on a
/// `commit_msg` (CAIRN-3874).
///
/// One boundary asked by `write` and by `run`, because it protects one thing: a
/// thread owns no branch, so a batch of its that carries a `commit_msg` seals
/// onto the project's DEFAULT branch, with no pull request and no review
/// surface. `jobs.thread_id` is the authoritative owner signal and
/// [`crate::threads::commit_refusal_for_job`] is the one predicate that reads
/// it; nothing here adds a second notion of what a thread is.
///
/// Fail-closed on IDENTITY, fail-open on ownership. A request carrying no run id
/// is a user's own — the desktop app and an operator's `cairn write` carry no
/// `CAIRN_RUN_ID` — so there is no agent posture to enforce and it passes
/// untouched. A request that does claim a run identity has one that must
/// resolve: while an unresolvable run was treated as ordinary, a job whose run
/// row had gone missing was indistinguishable from an issue-owned one, and the
/// refusal simply did not fire. A posture that cannot be established is not an
/// allowance. Ownership itself keeps the safe direction it has always had: a
/// resolvable job that is not thread-owned commits.
pub(crate) async fn commit_posture_refusal(
    dbs: &crate::db::DbState,
    request: &CallbackRequest,
) -> Option<String> {
    if required_run_id(request).is_err() {
        return None;
    }
    match lookup_run_routed(dbs, request).await {
        Ok((context, db)) => crate::threads::commit_refusal_for_job(&db, &context.job_id).await,
        Err(error) => Some(unresolvable_identity_refusal(&error)),
    }
}

/// Resolve a project's DB id by key (uppercased).
pub(crate) async fn project_id_by_key(db: &LocalDb, key: &str) -> Result<String, String> {
    let key = cairn_common::uri::canonical_project(key);
    db.query_text(
        "SELECT id FROM projects WHERE key = ?1 LIMIT 1",
        (key.clone(),),
    )
    .await
    .map_err(|e| format!("Failed to load project: {e}"))?
    .ok_or_else(|| format!("No project found with key '{key}'"))
}

pub(crate) async fn project_path(db: &LocalDb, project_id: &str) -> Result<Option<String>, String> {
    let project_id = project_id.to_string();
    db.query_text(
        "SELECT repo_path FROM projects WHERE id = ?1",
        (project_id,),
    )
    .await
    .map_err(|e| format!("Failed to load project path: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::storage::{MigrationRunner, SearchIndex, TURSO_MIGRATIONS};

    async fn local_dbs() -> std::sync::Arc<DbState> {
        let dir = tempfile::tempdir().unwrap().keep();
        let db = LocalDb::open(dir.join("p.turso.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        let search = SearchIndex::open_or_create(dir.join("p.search")).unwrap();
        std::sync::Arc::new(DbState::new(
            std::sync::Arc::new(db),
            std::sync::Arc::new(search),
        ))
    }

    #[tokio::test]
    async fn thread_run_gets_the_canonical_thread_home() {
        let dbs = local_dbs().await;
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','PRJ','/tmp/p',1,1)",
            "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at) VALUES ('t','p','design','active','none',1,1)",
            "INSERT INTO jobs (id, thread_id, project_id, node_name, agent_config_id, status, created_at, updated_at, uri_segment) VALUES ('j','t','p','thread','thread','running',1,1,'thread')",
            "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at) VALUES ('r-thread','p','j','live',1,1)",
        ] {
            dbs.local.execute(sql, ()).await.unwrap();
        }

        assert_eq!(
            lookup_home_uri_by_run_id(&dbs.local, "r-thread")
                .await
                .unwrap(),
            "cairn://p/PRJ/design"
        );

        dbs.local
            .execute("UPDATE threads SET name='channels' WHERE id='t'", ())
            .await
            .unwrap();
        let request = CallbackRequest {
            cwd: String::new(),
            run_id: Some("r-thread".to_string()),
            tool: "write".to_string(),
            payload: serde_json::json!({}),
            tool_use_id: None,
            thread_id: None,
        };
        let target =
            crate::resources::resolve_home_relative_resource_uri(&dbs, &request, "cairn:~/tasks")
                .await
                .unwrap();
        assert_eq!(target, "cairn://p/PRJ/channels/tasks");
        assert!(
            crate::resources::mutations::blocking_append_kind(&crate::mcp::types::ChangeItem {
                target,
                mode: crate::mcp::types::ChangeMode::Append,
                payload: Some(serde_json::json!({})),
            })
            .is_some(),
            "the live-resolved tasks URI must enter blocking task routing"
        );
    }

    async fn seed_run(db: &LocalDb) {
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','PRJ','/tmp/p',1,1)",
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i','p',7,'T','active',1,1)",
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e','recipe','i','p','running',1,1)",
            "INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, branch) VALUES ('j','e','i','p','Builder','running',1,1,'builder','agent/test')",
            "INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at) VALUES ('r','i','p','j','live',1,1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
    }

    fn request_for_run(run_id: &str) -> CallbackRequest {
        CallbackRequest {
            thread_id: None,
            run_id: Some(run_id.to_string()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn lookup_run_routed_resolves_a_private_run_to_the_private_db() {
        let dbs = local_dbs().await;
        seed_run(&dbs.local).await;
        let (ctx, db) = lookup_run_routed(&dbs, &request_for_run("r"))
            .await
            .expect("a seeded private run resolves");
        assert_eq!(ctx.project_key, "PRJ");
        assert_eq!(ctx.run_id, "r");
        assert!(
            Arc::ptr_eq(&db, &dbs.local),
            "a private run resolves to the private database (strict no-op for local installs)"
        );
    }

    #[tokio::test]
    async fn workflow_child_run_gets_a_node_shaped_home() {
        let dbs = local_dbs().await;
        // A caller node `builder` and a workflow that is a CHILD of it but is
        // addressable as a node. Its home must be node-shaped so the harness's
        // `cairn:~/calls` resolves as a NodeCalls collection.
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p','w','P','PRJ','/tmp/p',1,1)",
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at) VALUES ('i','p',9,'T','active',1,1)",
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('e','recipe','i','p','running',1,1)",
            "INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment) VALUES ('j-b','e','i','p','Builder','running',1,1,'builder')",
            "INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, node_name, agent_config_id, status, created_at, updated_at, uri_segment) VALUES ('j-wf','e','j-b','i','p','WF','workflow','running',1,1,'wf')",
            "INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at) VALUES ('r-wf','i','p','j-wf','live',1,1)",
        ] {
            dbs.local.execute(sql, ()).await.unwrap();
        }
        let home = lookup_home_uri_by_run_id(&dbs.local, "r-wf").await.unwrap();
        assert!(
            !home.contains("/task/") && home.ends_with("/wf"),
            "workflow home must be node-shaped, got: {home}"
        );
    }

    #[tokio::test]
    async fn lookup_run_routed_errors_clearly_when_run_is_absent_everywhere() {
        let dbs = local_dbs().await;
        let err = match lookup_run_routed(&dbs, &request_for_run("ghost")).await {
            Ok(_) => panic!("an unknown run must error, never silently misroute"),
            Err(err) => err,
        };
        assert!(
            err.contains("ghost"),
            "the error should name the missing run id: {err}"
        );
    }

    #[tokio::test]
    async fn lookup_run_refuses_missing_or_empty_run_identity() {
        let dbs = local_dbs().await;
        seed_run(&dbs.local).await;

        for run_id in [None, Some(String::new())] {
            let request = CallbackRequest {
                cwd: "/tmp/wt".to_string(),
                run_id,
                ..Default::default()
            };
            let err = lookup_run_routed(&dbs, &request)
                .await
                .err()
                .expect("missing identity must be refused");
            assert!(err.contains("missing its run ID"), "{err}");
        }
    }

    #[tokio::test]
    async fn lookup_run_identity_is_independent_of_cwd() {
        let dbs = local_dbs().await;
        seed_run(&dbs.local).await;
        let request = CallbackRequest {
            cwd: "/forged/other/project".to_string(),
            ..request_for_run("r")
        };

        let (context, _) = lookup_run_routed(&dbs, &request)
            .await
            .expect("the authenticated run resolves regardless of cwd");
        assert_eq!(context.run_id, "r");
        assert_eq!(context.project_key, "PRJ");
    }

    /// The commit fence, at the predicate both verbs share.
    ///
    /// Ownership decides: the thread-owned run is refused with the posture and
    /// with where the work belongs, and the ordinary issue run beside it in the
    /// same database still commits. Asserting both directions is what keeps this
    /// a fence rather than a blanket.
    #[tokio::test]
    async fn a_thread_owned_run_is_refused_a_commit_and_an_issue_run_is_not() {
        let dbs = local_dbs().await;
        seed_run(&dbs.local).await;
        for sql in [
            "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at) VALUES ('t','p','design','active','none',1,1)",
            "INSERT INTO jobs (id, thread_id, project_id, node_name, agent_config_id, status, created_at, updated_at, uri_segment) VALUES ('j-t','t','p','thread','thread','running',1,1,'thread')",
            "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at) VALUES ('r-thread','p','j-t','live',1,1)",
        ] {
            dbs.local.execute(sql, ()).await.unwrap();
        }

        let refusal = commit_posture_refusal(&dbs, &request_for_run("r-thread"))
            .await
            .expect("a thread-owned job may not commit");
        assert!(
            refusal.contains("thread-owned job") && refusal.contains("child issue"),
            "the refusal must carry the posture and where the work belongs: {refusal}"
        );

        assert!(
            commit_posture_refusal(&dbs, &request_for_run("r"))
                .await
                .is_none(),
            "an ordinary issue job still commits"
        );
    }

    /// A request with no run identity is a user's own and is not gated: the
    /// desktop app and an operator shell carry no `CAIRN_RUN_ID`, and there is no
    /// agent posture to enforce on them.
    #[tokio::test]
    async fn a_request_with_no_run_identity_is_not_gated() {
        let dbs = local_dbs().await;
        seed_run(&dbs.local).await;

        for run_id in [None, Some(String::new())] {
            let request = CallbackRequest {
                cwd: "/tmp/wt".to_string(),
                run_id,
                ..Default::default()
            };
            assert!(
                commit_posture_refusal(&dbs, &request).await.is_none(),
                "an unauthenticated write is the user's own and commits as it always has"
            );
        }
    }

    /// An identity that does not resolve is an unknown posture, and an unknown
    /// posture may not commit.
    ///
    /// This is the direction that matters: while it returned "allow", a job whose
    /// run row had gone missing was indistinguishable from an issue-owned one, so
    /// the fence simply did not fire (CAIRN-3874).
    #[tokio::test]
    async fn an_authenticated_request_whose_run_cannot_be_resolved_is_refused() {
        let dbs = local_dbs().await;
        seed_run(&dbs.local).await;

        let refusal = commit_posture_refusal(&dbs, &request_for_run("ghost"))
            .await
            .expect("an unresolvable identity may not commit");
        assert!(
            refusal.contains("cannot resolve") && refusal.contains("ghost"),
            "the refusal must say the posture is unknown and name the run: {refusal}"
        );
    }

    #[tokio::test]
    async fn simultaneous_runs_with_the_same_residence_do_not_cross_resolve() {
        let dbs = local_dbs().await;
        for sql in [
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w','W',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p1','w','One','ONE','/repos/one',1,1)",
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p2','w','Two','TWO','/repos/two',1,1)",
            "INSERT INTO jobs (id, project_id, node_name, status, created_at, updated_at, uri_segment) VALUES ('j1','p1','Builder','running',1,1,'builder-one')",
            "INSERT INTO jobs (id, project_id, node_name, status, created_at, updated_at, uri_segment) VALUES ('j2','p2','Builder','running',1,1,'builder-two')",
            "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at) VALUES ('r1','p1','j1','live',1,1)",
            "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at) VALUES ('r2','p2','j2','live',1,1)",
        ] {
            dbs.local.execute(sql, ()).await.unwrap();
        }

        let shared_residence = "/home/tester/.cairn/scratch/PRJ.1.1.parent";
        let first = CallbackRequest {
            cwd: shared_residence.to_string(),
            ..request_for_run("r1")
        };
        let second = CallbackRequest {
            cwd: shared_residence.to_string(),
            ..request_for_run("r2")
        };

        let (first, _) = lookup_run_routed(&dbs, &first).await.unwrap();
        let (second, _) = lookup_run_routed(&dbs, &second).await.unwrap();
        assert_eq!(
            (first.run_id.as_str(), first.project_key.as_str()),
            ("r1", "ONE")
        );
        assert_eq!(
            (second.run_id.as_str(), second.project_key.as_str()),
            ("r2", "TWO")
        );
    }
}
