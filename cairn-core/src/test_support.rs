use crate::storage::{LocalDb, RowExt};

pub(crate) struct ThreadFixture {
    pub project_id: &'static str,
    pub thread_id: &'static str,
    pub session_job_id: &'static str,
    pub host_execution_id: &'static str,
    pub task_job_id: &'static str,
    pub child_issue_id: &'static str,
    pub child_issue_job_id: &'static str,
}

/// The production shape of a thread that has actually run: session job with a
/// synthetic host execution (seq NULL, issue_id NULL), children by
/// `issues.parent_thread_id`, spawned tasks carrying the inherited thread_id.
/// Mirrors thread session creation and host snapshot insertion; do not invent
/// ownership columns in individual tests.
pub(crate) async fn seed_thread_session(db: &LocalDb) -> ThreadFixture {
    let fixture = ThreadFixture {
        project_id: "thread-project",
        thread_id: "thread-id",
        session_job_id: "thread-session-job",
        host_execution_id: "thread-host-execution",
        task_job_id: "thread-task-job",
        child_issue_id: "thread-child-issue",
        child_issue_job_id: "thread-child-issue-job",
    };
    for sql in [
        "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('thread-workspace','Thread Workspace',1,1)",
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('thread-project','thread-workspace','Thread Project','thread-project','/tmp/thread-project',1,1)",
        "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at) VALUES ('thread-id','thread-project','design','active','none',1,1)",
        "INSERT INTO jobs (id, thread_id, project_id, node_name, agent_config_id, status, created_at, updated_at, uri_segment) VALUES ('thread-session-job','thread-id','thread-project','Thread','thread','running',1,1,'thread')",
        "INSERT INTO executions (id, recipe_id, project_id, status, started_at) VALUES ('thread-host-execution','host-thread-session-job','thread-project','running',1)",
        "UPDATE jobs SET execution_id='thread-host-execution' WHERE id='thread-session-job'",
        "INSERT INTO jobs (id, parent_job_id, thread_id, project_id, node_name, agent_config_id, status, created_at, updated_at, uri_segment) VALUES ('thread-task-job','thread-session-job','thread-id','thread-project','Task','build','running',1,1,'task')",
        "INSERT INTO issues (id, project_id, number, title, status, parent_thread_id, created_at, updated_at) VALUES ('thread-child-issue','thread-project',7,'Child issue','active','thread-id',1,1)",
        "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('thread-child-execution','recipe','thread-child-issue','thread-project','running',1,1)",
        "INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, agent_config_id, status, created_at, updated_at, uri_segment) VALUES ('thread-child-issue-job','thread-child-execution','thread-child-issue','thread-project','Builder','build','running',1,1,'builder')",
    ] {
        db.execute(sql, ()).await.unwrap();
    }
    fixture
}

#[tokio::test]
async fn thread_fixture_matches_the_post_snapshot_production_shape() {
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};

    let dir = tempfile::tempdir().unwrap();
    let db = LocalDb::open(dir.path().join("fixture.db")).await.unwrap();
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    let fixture = seed_thread_session(&db).await;

    let session_shape = db
        .query_one(
            "SELECT issue_id IS NULL, thread_id IS NOT NULL, execution_id IS NOT NULL FROM jobs WHERE id=?1",
            (fixture.session_job_id,),
            |row| Ok((row.i64(0)?, row.i64(1)?, row.i64(2)?)),
        )
        .await
        .unwrap();
    assert_eq!(session_shape, (1, 1, 1));
    let host_shape = db
        .query_one(
            "SELECT issue_id IS NULL, seq IS NULL, recipe_id FROM executions WHERE id=?1",
            (fixture.host_execution_id,),
            |row| Ok((row.i64(0)?, row.i64(1)?, row.text(2)?)),
        )
        .await
        .unwrap();
    assert_eq!(host_shape, (1, 1, "host-thread-session-job".to_string()));
    let child_shape = db
        .query_one(
            "SELECT parent_thread_id IS NOT NULL, parent_issue_id IS NULL FROM issues WHERE id=?1",
            (fixture.child_issue_id,),
            |row| Ok((row.i64(0)?, row.i64(1)?)),
        )
        .await
        .unwrap();
    assert_eq!(child_shape, (1, 1));
    let child_job_shape = db
        .query_one(
            "SELECT issue_id IS NOT NULL, thread_id IS NULL FROM jobs WHERE id=?1",
            (fixture.child_issue_job_id,),
            |row| Ok((row.i64(0)?, row.i64(1)?)),
        )
        .await
        .unwrap();
    assert_eq!(child_job_shape, (1, 1));
}
