use std::collections::HashSet;

use cairn_db::turso::params;

use crate::config::slugify_resource_segment;
use crate::storage::{DbResult, RowExt};

fn slug_or_trimmed(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let slug = slugify_resource_segment(trimmed);
    (!slug.is_empty()).then_some(slug)
}

pub(crate) fn next_available_segment(base: &str, reserved: &HashSet<String>) -> String {
    let base = slug_or_trimmed(base).unwrap_or_else(|| "resource".to_string());
    if !reserved.contains(&base) {
        return base;
    }

    let mut suffix = 2;
    loop {
        let candidate = format!("{base}-{suffix}");
        if !reserved.contains(&candidate) {
            return candidate;
        }
        suffix += 1;
    }
}

pub(crate) fn node_segment_base(
    node_name: Option<&str>,
    snapshot_node_name: Option<&str>,
    recipe_node_id: Option<&str>,
) -> String {
    node_name
        .and_then(slug_or_trimmed)
        .or_else(|| snapshot_node_name.and_then(slug_or_trimmed))
        .or_else(|| recipe_node_id.and_then(slug_or_trimmed))
        .unwrap_or_else(|| "node".to_string())
}

pub(crate) fn task_segment_base(
    explicit_task_name: Option<&str>,
    task_description: Option<&str>,
    agent_config_id: Option<&str>,
) -> String {
    explicit_task_name
        .and_then(slug_or_trimmed)
        .or_else(|| task_description.and_then(slug_or_trimmed))
        .or_else(|| agent_config_id.and_then(slug_or_trimmed))
        .unwrap_or_else(|| "task".to_string())
}

async fn existing_segments(
    conn: &cairn_db::turso::Connection,
    sql: &str,
    bind: &str,
) -> DbResult<HashSet<String>> {
    let mut rows = conn.query(sql, params![bind]).await?;
    let mut reserved = HashSet::new();
    while let Some(row) = rows.next().await? {
        if let Some(segment) = row.opt_text(0)? {
            reserved.insert(segment);
        }
    }
    Ok(reserved)
}

/// Allocate a top-level node segment for an execution, deduped across **both**
/// `jobs.uri_segment` and `action_runs.uri_segment`. Agent nodes (jobs) and
/// action nodes (action_runs, e.g. a `pr` node) share one URI namespace under
/// `cairn://p/PROJ/N/EXEC/<segment>`, so a same-named agent and action can't
/// both claim `pr` — the second gets `pr-2`. Used by both job creation and
/// action_run creation so the namespace stays single-owner.
pub async fn allocate_top_level_segment(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
    execution_id: &str,
    base_segment: &str,
) -> DbResult<String> {
    let mut reserved = HashSet::new();

    let mut job_rows = conn
        .query(
            "SELECT uri_segment
             FROM jobs
             WHERE issue_id = ?1
               AND execution_id = ?2
               AND parent_job_id IS NULL
               AND uri_segment IS NOT NULL",
            params![issue_id, execution_id],
        )
        .await?;
    while let Some(row) = job_rows.next().await? {
        if let Some(segment) = row.opt_text(0)? {
            reserved.insert(segment);
        }
    }
    drop(job_rows);

    let mut action_rows = conn
        .query(
            "SELECT uri_segment
             FROM action_runs
             WHERE execution_id = ?1
               AND uri_segment IS NOT NULL",
            params![execution_id],
        )
        .await?;
    while let Some(row) = action_rows.next().await? {
        if let Some(segment) = row.opt_text(0)? {
            reserved.insert(segment);
        }
    }
    drop(action_rows);

    Ok(next_available_segment(base_segment, &reserved))
}

/// Allocate a sub-agent task's segment, deduped across everything its ADDRESS
/// could collide with.
///
/// For a task under an issue node that is its parent job: the address is
/// `.../{exec}/{node}/task/{segment}`, and the node is in it, so siblings under
/// one parent are the whole namespace.
///
/// For a task under a THREAD the address is `.../{thread}/task/{segment}` — it
/// names the thread, never the session job that spawned the task. A thread mints
/// a fresh session whenever its previous one goes terminal
/// (`ensure_thread_session_conn`), so deduping per parent would let two sessions
/// of one thread each hand out the same segment from a description they both
/// slugify the same way, and two distinct jobs would answer to one URI. Whichever
/// resolved would take the other's artifact writes, todos, and messages.
/// Uniqueness therefore has to be scoped the way the address is: across every
/// task of every session of that thread.
pub async fn allocate_child_task_segment(
    conn: &cairn_db::turso::Connection,
    parent_job_id: &str,
    base_segment: &str,
) -> DbResult<String> {
    // The thread arm contributes nothing when the parent is an ordinary node:
    // its `thread_id` is NULL, and `session.thread_id = NULL` is never true.
    let reserved = existing_segments(
        conn,
        "SELECT task.uri_segment
         FROM jobs task
         LEFT JOIN jobs session ON session.id = task.parent_job_id
         WHERE task.uri_segment IS NOT NULL
           AND (
             task.parent_job_id = ?1
             OR (
               session.parent_job_id IS NULL
               AND session.uri_segment = 'thread'
               AND session.thread_id = (SELECT thread_id FROM jobs WHERE id = ?1)
             )
           )",
        parent_job_id,
    )
    .await?;
    Ok(next_available_segment(base_segment, &reserved))
}

pub async fn allocate_project_job_segment(
    conn: &cairn_db::turso::Connection,
    project_id: &str,
    base_segment: &str,
) -> DbResult<String> {
    let reserved = existing_segments(
        conn,
        "SELECT uri_segment
         FROM jobs
         WHERE project_id = ?1
           AND issue_id IS NULL
           AND execution_id IS NULL
           AND parent_job_id IS NULL
           AND uri_segment IS NOT NULL",
        project_id,
    )
    .await?;
    Ok(next_available_segment(base_segment, &reserved))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn node_segment_base_prefers_stored_name_then_snapshot_then_recipe_id() {
        assert_eq!(
            node_segment_base(Some("Builder One"), Some("Snapshot"), Some("recipe-id")),
            "builder-one"
        );
        assert_eq!(
            node_segment_base(None, Some("Snapshot Node"), Some("recipe-id")),
            "snapshot-node"
        );
        assert_eq!(
            node_segment_base(None, None, Some("recipe-id")),
            "recipe-id"
        );
        assert_eq!(node_segment_base(None, None, None), "node");
    }

    #[test]
    fn task_segment_base_prefers_explicit_metadata_then_agent() {
        assert_eq!(
            task_segment_base(Some("Explore"), Some("Fallback"), Some("worker")),
            "explore"
        );
        assert_eq!(
            task_segment_base(None, Some("Read the docs"), Some("worker")),
            "read-the-docs"
        );
        assert_eq!(
            task_segment_base(None, None, Some("Build Agent")),
            "build-agent"
        );
        assert_eq!(task_segment_base(None, None, None), "task");
    }

    #[test]
    fn empty_resource_slug_falls_back_to_typed_base() {
        assert_eq!(node_segment_base(Some("🧪"), None, None), "node");
        assert_eq!(task_segment_base(Some("🧪"), None, None), "task");
    }

    #[test]
    fn next_available_segment_adds_deterministic_suffixes() {
        let reserved = HashSet::from(["explore".to_string(), "explore-2".to_string()]);
        assert_eq!(next_available_segment("Explore", &reserved), "explore-3");
    }

    /// A top-level job and a top-level action node sharing a base name must get
    /// distinct segments: the allocator reserves across both `jobs.uri_segment`
    /// and `action_runs.uri_segment` for the execution (CAIRN-1222).
    #[tokio::test]
    async fn top_level_segment_dedupes_across_jobs_and_action_runs() {
        use crate::storage::{DbError, LocalDb, MigrationRunner, TURSO_MIGRATIONS};

        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("seg.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();

        let (seg, seg2) = db
            .write(|conn| {
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w', 'W', 1, 1)",
                        (),
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                         VALUES ('p', 'w', 'N', 'K', '/tmp', 1, 1)",
                        (),
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO issues (id, project_id, number, title, description, status, progress, attention, priority, created_at, updated_at)
                         VALUES ('i', 'p', 1, 't', '', 'active', 'active', 'none', 0, 1, 1)",
                        (),
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                         VALUES ('e', 'default', 'i', 'p', 'running', 1, 1)",
                        (),
                    )
                    .await?;

                    // A top-level job already claims "pr".
                    conn.execute(
                        "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
                         VALUES ('j', 'e', 'i', 'p', 'pending', 'pr', 'pr', 1, 1)",
                        (),
                    )
                    .await?;

                    // An action node with the same base must not collide.
                    let seg = allocate_top_level_segment(conn, "i", "e", "pr").await?;

                    // Persist it, then a further allocation must avoid both tables.
                    conn.execute(
                        "INSERT INTO action_runs (id, execution_id, recipe_node_id, action_config_id, project_id, status, created_at, uri_segment)
                         VALUES ('ar', 'e', 'pr-1', 'builtin:pr', 'p', 'pending', 1, ?1)",
                        params![seg.as_str()],
                    )
                    .await?;
                    let seg2 = allocate_top_level_segment(conn, "i", "e", "pr").await?;

                    Ok::<_, DbError>((seg, seg2))
                })
            })
            .await
            .unwrap();

        assert_eq!(seg, "pr-2");
        assert_eq!(seg2, "pr-3");
    }

    /// Two sessions of one thread cannot hand out the same task segment.
    ///
    /// A thread task is addressed `.../{thread}/task/{segment}` — the session it
    /// hangs off is not in the URI. A thread mints a fresh session whenever its
    /// previous one goes terminal, so deduping per parent job would let the
    /// second session reissue a segment the first already used (recurring thread
    /// work reuses descriptions, and segments are description slugs). Two jobs
    /// would then answer to one address, and whichever resolved would take the
    /// other's artifact writes, todos, and messages.
    ///
    /// An issue node's tasks are unaffected: the node IS in their address, so
    /// their namespace stays the parent job.
    #[tokio::test]
    async fn a_thread_task_segment_is_unique_across_every_session_of_the_thread() {
        use crate::storage::{DbError, LocalDb, MigrationRunner, TURSO_MIGRATIONS};

        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("thread-seg.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();

        let (across_sessions, under_a_node) = db
            .write(|conn| {
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w', 'W', 1, 1)",
                        (),
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                         VALUES ('p', 'w', 'N', 'K', '/tmp', 1, 1)",
                        (),
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
                         VALUES ('th', 'p', 'general', 'active', 'none', 1, 1)",
                        (),
                    )
                    .await?;
                    // The thread's first session went terminal, so a second was
                    // minted alongside it — the shape `general` is in today.
                    conn.execute(
                        "INSERT INTO jobs (id, thread_id, project_id, status, uri_segment, node_name, created_at, updated_at)
                         VALUES ('s1', 'th', 'p', 'complete', 'thread', 'Thread', 1, 1)",
                        (),
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO jobs (id, thread_id, project_id, status, uri_segment, node_name, created_at, updated_at)
                         VALUES ('s2', 'th', 'p', 'idle', 'thread', 'Thread', 2, 2)",
                        (),
                    )
                    .await?;
                    // The first session already spawned `survey`, which now owns
                    // cairn://p/K/general/task/survey.
                    conn.execute(
                        "INSERT INTO jobs (id, parent_job_id, project_id, status, uri_segment, node_name, created_at, updated_at)
                         VALUES ('t1', 's1', 'p', 'complete', 'survey', 'Explore', 3, 3)",
                        (),
                    )
                    .await?;

                    // The SECOND session spawns a task from the same description.
                    let across_sessions = allocate_child_task_segment(conn, "s2", "survey").await?;

                    // An ordinary issue node with a `survey` task of its own must
                    // not be pulled into the thread's namespace.
                    conn.execute(
                        "INSERT INTO jobs (id, project_id, status, uri_segment, node_name, created_at, updated_at)
                         VALUES ('node', 'p', 'running', 'builder', 'Builder', 4, 4)",
                        (),
                    )
                    .await?;
                    let under_a_node = allocate_child_task_segment(conn, "node", "survey").await?;

                    Ok::<_, DbError>((across_sessions, under_a_node))
                })
            })
            .await
            .unwrap();

        assert_eq!(
            across_sessions, "survey-2",
            "a second session must not reissue a segment the thread's address already resolves"
        );
        assert_eq!(
            under_a_node, "survey",
            "an issue node's task namespace is its own parent job, and stays that way"
        );
    }
}
