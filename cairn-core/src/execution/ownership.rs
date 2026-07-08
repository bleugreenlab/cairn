//! Team-execution device ownership (CAIRN-2629).
//!
//! Every execution carries a synced `runner_device_id` naming the machine that
//! OWNS it: only that machine claims and runs its jobs. This module is the single
//! read side of that column, shared by every start/claim entry point
//! (`start_job_background`, `continue_job_impl`) so the ownership guard has one
//! implementation.
//!
//! IMPORTANT — this is NOT a distributed lock. Ownership rides async row sync, so
//! a just-reassigned execution has a stale-sync window (up to one pull cycle) in
//! which a returning old owner and the new owner can both believe they own it.
//! v1 accepts this: reassignment is an explicit manual action, typically aimed at
//! an offline owner. Cross-machine single-claim comes from ownership (only the
//! owner ever proceeds), NOT from a conditional UPDATE — an embedded-replica CAS
//! is local-first and would let two machines both "win" locally.

use crate::storage::{LocalDb, RowExt};

/// Read `executions.runner_device_id` for one execution. `None` when the
/// execution is missing OR the column is NULL — a legacy pre-CAIRN-2629 row,
/// which the ownership guard treats as "anyone may run" so nothing regresses.
pub async fn execution_runner_device_id(db: &LocalDb, execution_id: &str) -> Option<String> {
    let execution_id = execution_id.to_string();
    db.read(|conn| {
        let execution_id = execution_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT runner_device_id FROM executions WHERE id = ?1",
                    (execution_id.as_str(),),
                )
                .await?;
            match rows.next().await? {
                Some(row) => row.opt_text(0),
                None => Ok(None),
            }
        })
    })
    .await
    .ok()
    .flatten()
}

/// If `execution_id` is owned by a device OTHER than `this_device_id`, return
/// that owner's id — the caller must DEFER (not claim, not run; leave the job
/// pending for the owner's sweep to pick up).
///
/// Returns `None` ("this machine may proceed") when this machine owns the
/// execution, when the owner is unset (legacy NULL, or a private execution
/// stamped with this machine's id), or when the execution can't be read — the
/// read-error case fails OPEN so a transient replica hiccup never strands an
/// otherwise-startable job.
pub async fn deferred_owner(
    db: &LocalDb,
    execution_id: &str,
    this_device_id: &str,
) -> Option<String> {
    match execution_runner_device_id(db, execution_id).await {
        Some(owner) if owner != this_device_id => Some(owner),
        _ => None,
    }
}

/// The normalized key of a project id, or `None` when unknown.
pub async fn project_key(db: &LocalDb, project_id: &str) -> Option<String> {
    let project_id = project_id.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT \"key\" FROM projects WHERE id = ?1",
                    (project_id.as_str(),),
                )
                .await?;
            match rows.next().await? {
                Some(row) => row.opt_text(0),
                None => Ok(None),
            }
        })
    })
    .await
    .ok()
    .flatten()
}

/// The key of the project an execution belongs to — via its own `project_id`, or
/// (for an issue-scoped execution, whose `project_id` is NULL) via its issue's
/// project. `None` when it can't be resolved.
pub async fn project_key_for_execution(db: &LocalDb, execution_id: &str) -> Option<String> {
    let execution_id = execution_id.to_string();
    db.read(|conn| {
        let execution_id = execution_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT p.\"key\" FROM executions e \
                     JOIN projects p ON p.id = COALESCE( \
                        e.project_id, \
                        (SELECT i.project_id FROM issues i WHERE i.id = e.issue_id)) \
                     WHERE e.id = ?1",
                    (execution_id.as_str(),),
                )
                .await?;
            match rows.next().await? {
                Some(row) => row.opt_text(0),
                None => Ok(None),
            }
        })
    })
    .await
    .ok()
    .flatten()
}

/// Whether `device_id` advertises a LOCAL clone of `project_key` in the synced
/// `device_presence` table. Fail-CLOSED: a missing table, missing row, or
/// unparseable `project_keys` all yield `false`, so ownership can never be handed
/// to a device that has not proven it can run the project.
pub async fn device_has_clone(db: &LocalDb, device_id: &str, project_key: &str) -> bool {
    let device_id = device_id.to_string();
    let keys_json: Option<String> = db
        .read(|conn| {
            let device_id = device_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT project_keys FROM device_presence WHERE device_id = ?1",
                        (device_id.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => row.opt_text(0),
                    None => Ok(None),
                }
            })
        })
        .await
        .ok()
        .flatten();
    match keys_json {
        Some(json) => serde_json::from_str::<Vec<String>>(&json)
            .map(|keys| keys.iter().any(|k| k == project_key))
            .unwrap_or(false),
        None => false,
    }
}

/// Whether THIS machine has a local clone of `project_key`, read authoritatively
/// from the PRIVATE routing overlay (`project_routes.local_repo_path`) — the same
/// source `prepare_job` resolves against, and more current than `device_presence`
/// (which is derived from it and can lag). Used to validate a reassignment TO this
/// machine, which — unlike execution creation — the launch flow does not gate.
pub async fn local_has_clone(private_db: &LocalDb, project_key: &str) -> bool {
    let project_key = project_key.to_string();
    private_db
        .query_text(
            "SELECT local_repo_path FROM project_routes \
             WHERE project_key = ?1 AND local_repo_path IS NOT NULL",
            (project_key,),
        )
        .await
        .ok()
        .flatten()
        .is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{MigrationRunner, TEAM_MIGRATIONS, TURSO_MIGRATIONS};

    async fn db() -> LocalDb {
        let dir = tempfile::tempdir().unwrap().keep();
        let db = LocalDb::open(dir.join("t.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    async fn team_db() -> LocalDb {
        let dir = tempfile::tempdir().unwrap().keep();
        let db = LocalDb::open(dir.join("team.db")).await.unwrap();
        MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    async fn seed(db: &LocalDb, id: &str, owner: Option<&str>) {
        db.execute(
            "INSERT INTO executions (id, recipe_id, status, started_at, runner_device_id) \
             VALUES (?1, 'r', 'running', 1, ?2)",
            (id.to_string(), owner.map(|s| s.to_string())),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn owner_match_may_proceed() {
        let db = db().await;
        seed(&db, "e1", Some("devA")).await;
        assert_eq!(deferred_owner(&db, "e1", "devA").await, None);
    }

    #[tokio::test]
    async fn other_owner_defers_and_names_the_owner() {
        let db = db().await;
        seed(&db, "e1", Some("devA")).await;
        assert_eq!(
            deferred_owner(&db, "e1", "devB").await,
            Some("devA".to_string())
        );
    }

    #[tokio::test]
    async fn null_owner_is_anyone_may_run() {
        let db = db().await;
        seed(&db, "e1", None).await;
        assert_eq!(deferred_owner(&db, "e1", "devB").await, None);
    }

    #[tokio::test]
    async fn missing_execution_fails_open() {
        let db = db().await;
        assert_eq!(deferred_owner(&db, "nope", "devB").await, None);
    }

    #[tokio::test]
    async fn device_has_clone_reads_presence_and_fails_closed() {
        let db = team_db().await;
        db.execute(
            "INSERT INTO device_presence \
             (device_id, device_name, last_seen, project_keys, updated_at) \
             VALUES ('devA', 'A', 1, '[\"PROJ\"]', 1)",
            (),
        )
        .await
        .unwrap();
        assert!(device_has_clone(&db, "devA", "PROJ").await);
        assert!(!device_has_clone(&db, "devA", "OTHER").await);
        assert!(
            !device_has_clone(&db, "devB", "PROJ").await,
            "no presence row"
        );
    }

    #[tokio::test]
    async fn device_has_clone_false_without_table() {
        // A private DB has no device_presence table; must fail closed, not panic.
        let db = db().await;
        assert!(!device_has_clone(&db, "devA", "PROJ").await);
    }

    #[tokio::test]
    async fn local_has_clone_reads_private_route() {
        let db = db().await;
        // team_id is a nullable FK; leave it NULL to avoid seeding the registry.
        db.execute(
            "INSERT INTO project_routes (project_key, local_repo_path, created_at) \
             VALUES ('CLONED', '/tmp/x', 0)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO project_routes (project_key, local_repo_path, created_at) \
             VALUES ('UNCLONED', NULL, 0)",
            (),
        )
        .await
        .unwrap();
        assert!(local_has_clone(&db, "CLONED").await);
        assert!(
            !local_has_clone(&db, "UNCLONED").await,
            "route without a clone"
        );
        assert!(!local_has_clone(&db, "MISSING").await, "no route at all");
    }

    #[tokio::test]
    async fn project_key_resolves_project_and_issue_scoped() {
        let db = team_db().await;
        db.execute_script(
            "INSERT INTO teams (id,name,created_at,updated_at) VALUES ('t','T',1,1);\
             INSERT INTO projects (id,team_id,name,\"key\",repo_path,created_at,updated_at) \
               VALUES ('p','t','P','PKEY','/tmp',1,1);\
             INSERT INTO issues (id,project_id,number,title,created_at,updated_at) \
               VALUES ('i','p',1,'T',1,1);\
             INSERT INTO executions (id,recipe_id,project_id,status,started_at) \
               VALUES ('e-proj','r','p','running',1);\
             INSERT INTO executions (id,recipe_id,issue_id,status,started_at) \
               VALUES ('e-issue','r','i','running',1);",
        )
        .await
        .unwrap();
        assert_eq!(
            project_key_for_execution(&db, "e-proj").await.as_deref(),
            Some("PKEY")
        );
        assert_eq!(
            project_key_for_execution(&db, "e-issue").await.as_deref(),
            Some("PKEY"),
            "issue-scoped execution resolves its project via the issue"
        );
    }
}
