pub use cairn_db::relations::*;

#[cfg(test)]
mod parent_tests {
    use cairn_db::turso::params;

    use super::*;
    use crate::issues::crud;
    use crate::storage::LocalDb;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("parent-issue-relations.db").await
    }

    async fn seed_parent_child(db: &LocalDb) {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
            VALUES('p', 'w', 'Project', 'proj', '/tmp/repo', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES('parent', 'p', 1, 'Parent', 'backlog', 'backlog', 'none', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at, parent_issue_id)
            VALUES('child', 'p', 2, 'Child', 'backlog', 'backlog', 'none', 2, 2, 'parent');
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES('orphan', 'p', 3, 'Orphan', 'backlog', 'backlog', 'none', 3, 3);
            ",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn resolve_parent_branch_uses_live_parent_job_branch() {
        let db = migrated_db().await;
        seed_parent_child(&db).await;

        assert!(db
            .read(|conn| Box::pin(async move { resolve_parent_branch(conn, "child").await }))
            .await
            .unwrap()
            .is_none());
        assert!(db
            .read(|conn| Box::pin(async move { resolve_parent_branch(conn, "orphan").await }))
            .await
            .unwrap()
            .is_none());

        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO jobs(id, project_id, issue_id, status, branch, created_at, updated_at)
                     VALUES(?1, 'p', 'parent', 'blocked', 'agent/parent', 10, 10)",
                    params!["parent-job"],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let branch = db
            .read(|conn| Box::pin(async move { resolve_parent_branch(conn, "child").await }))
            .await
            .unwrap();
        assert_eq!(branch.as_deref(), Some("agent/parent"));
    }

    #[tokio::test]
    async fn resolve_parent_branch_ignores_terminal_parent_jobs() {
        let db = migrated_db().await;
        seed_parent_child(&db).await;

        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO jobs(id, project_id, issue_id, status, branch, created_at, updated_at)
                     VALUES(?1, 'p', 'parent', 'complete', 'agent/stale', 10, 10)",
                    params!["terminal-parent-job"],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let branch = db
            .read(|conn| Box::pin(async move { resolve_parent_branch(conn, "child").await }))
            .await
            .unwrap();
        assert!(branch.is_none());
    }

    #[tokio::test]
    async fn resolve_parent_branch_uses_parent_coordinate_without_checkout() {
        let db = migrated_db().await;
        seed_parent_child(&db).await;

        // Filesystem residence is irrelevant: a branch-bearing parent job is a
        // complete inheritance coordinate even when it has no checkout.
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO jobs(id, project_id, issue_id, status, branch, created_at, updated_at)
                     VALUES(?1, 'p', 'parent', 'blocked', 'agent/parent', 10, 10)",
                    params!["ambient-parent-job"],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let branch = db
            .read(|conn| Box::pin(async move { resolve_parent_branch(conn, "child").await }))
            .await
            .unwrap();
        assert_eq!(branch.as_deref(), Some("agent/parent"));
    }

    #[tokio::test]
    async fn resolve_parent_branch_ignores_parent_job_on_a_different_issue() {
        // `issues.parent_job_id` primarily records the CALLER's root job for wake
        // routing, which is not necessarily a job on the declared parent issue: a
        // run on issue A can reparent a child under issue B, recording A's job.
        // The exact-job fast path must NOT hand the caller's (issue A's) branch to
        // a child declared under issue B — it is gated on the job's `issue_id`
        // matching `parent_issue_id`. Here parent-b has no branch-bearing job, so
        // the child correctly resolves to no integration branch.
        let db = migrated_db().await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
            VALUES('p', 'w', 'Project', 'proj', '/tmp/repo', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES('parent-a', 'p', 1, 'Parent A', 'backlog', 'backlog', 'none', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES('parent-b', 'p', 2, 'Parent B', 'backlog', 'backlog', 'none', 2, 2);
            INSERT INTO jobs(id, project_id, issue_id, status, branch, created_at, updated_at)
            VALUES('job-a', 'p', 'parent-a', 'blocked', 'agent/parent-a', 10, 10);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at, parent_issue_id, parent_job_id)
            VALUES('child', 'p', 3, 'Child', 'backlog', 'backlog', 'none', 3, 3, 'parent-b', 'job-a');
            ",
        )
        .await
        .unwrap();

        let branch = db
            .read(|conn| Box::pin(async move { resolve_parent_branch(conn, "child").await }))
            .await
            .unwrap();
        assert!(
            branch.is_none(),
            "a child under parent-b must not inherit the caller job's branch on parent-a: {branch:?}"
        );
    }

    #[tokio::test]
    async fn resolve_parent_branch_uses_matching_parent_job_even_when_terminal() {
        // The coordinator-on-a-new-branch case: the spawner job is on the parent
        // issue itself. The child inherits its durable integration branch through
        // `parent_job_id` even after that coordinator job goes terminal,
        // which the non-terminal parent-issue fallback would miss. Under the base
        // branch target the coordinator job has no branch at all, so this lookup
        // finds none and the child falls through to the project default — the
        // whole child-branching difference between the two targets, with no
        // branch-target-specific code anywhere in this resolver.
        let db = migrated_db().await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
            VALUES('p', 'w', 'Project', 'proj', '/tmp/repo', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES('parent', 'p', 1, 'Parent', 'backlog', 'backlog', 'none', 1, 1);
            INSERT INTO jobs(id, project_id, issue_id, status, branch, created_at, updated_at)
            VALUES('coord-job', 'p', 'parent', 'complete', 'agent/coord', 10, 10);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at, parent_issue_id, parent_job_id)
            VALUES('child', 'p', 2, 'Child', 'backlog', 'backlog', 'none', 2, 2, 'parent', 'coord-job');
            ",
        )
        .await
        .unwrap();

        let branch = db
            .read(|conn| Box::pin(async move { resolve_parent_branch(conn, "child").await }))
            .await
            .unwrap();
        assert_eq!(branch.as_deref(), Some("agent/coord"));
    }

    #[tokio::test]
    async fn resolve_parent_branch_returns_none_for_parent_thread_id() {
        let db = migrated_db().await;
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','proj','/tmp/p',1,1);
             INSERT INTO threads(id,project_id,name,status,attention,created_at,updated_at) VALUES('thread','p','topic','active','none',1,1);
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,parent_thread_id,created_at,updated_at) VALUES('child','p',1,'Child','backlog','backlog','none','thread',1,1);"
        ).await.unwrap();
        let branch = db
            .read(|conn| Box::pin(async move { resolve_parent_branch(conn, "child").await }))
            .await
            .unwrap();
        assert!(branch.is_none());
    }

    #[tokio::test]
    async fn list_children_returns_children_for_parent() {
        let db = migrated_db().await;
        seed_parent_child(&db).await;

        let children = crud::list_children(&db, "parent").await.unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].id, "child");
        assert_eq!(children[0].parent_issue_id.as_deref(), Some("parent"));
    }
}
