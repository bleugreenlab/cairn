//! Jobs blocked on a missing required output: the one definition of "a job's
//! turn ended without the artifact it owes, and nothing is waiting on anyone
//! but the job".
//!
//! The answer is derived at read time from the sweep's own rule
//! (`cairn_workflow::transitions::projection::blocked_on_missing_output`),
//! through the same facts the sweep derives the job's status from
//! (`cairn_workflow::advancement::recompute::missing_output_conn`). Nothing is
//! stored, so the mark can never drift from the status, and a job that was
//! blocked before this read existed is marked the first time it is read.
//!
//! It is a mark and never a send. The census, the notification center and the
//! node summary read it; no message, push, wake or re-prompt follows from it.
//!
//! ## Exclusive with approvals
//!
//! A job whose gated artifact waits on its approver has an artifact row, so it
//! is never blocked on a missing one: this list and
//! `execution::approvals` never name the same job. They are separate modules
//! because approvals is driven from `artifacts` and cannot see a job with none.
//!
//! ## Top-level jobs only
//!
//! A delegated task owes its parent a `return`, and the parent hears of a
//! missing one through the task's own follow-up path, not through this mark.
//!
//! ## Cost
//!
//! The candidate statement seeks blocked top-level jobs by
//! `idx_jobs_status_execution` and never reads an execution snapshot. Only a
//! candidate's snapshot is loaded, to derive its facts, and blocked top-level
//! jobs are few. The tests pin the candidate plan.

use cairn_db::turso::Value as SqlValue;

use crate::execution::approvals::ApprovalScope;
use crate::storage::{DbResult, LocalDb, RowExt};

/// One top-level job held Blocked because its turn ended without its output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockedOnOutput {
    pub job_id: String,
    pub issue_id: String,
    pub issue_number: i64,
    /// The node's display name (`node_name`, else its URI segment).
    pub node_name: String,
    pub exec_seq: Option<i64>,
    /// The job's own address, where a reader goes to look at or resume it.
    pub node_uri: String,
    /// The artifact it owes, when its contract names one.
    pub output_name: Option<String>,
    /// When the work turn that owed the output ended (the job's last update
    /// when no turn records an end).
    pub since: i64,
}

impl BlockedOnOutput {
    /// What is missing, by name: `plan`, `create-pr`, or `output` for an
    /// unnamed contract.
    pub fn what(&self) -> &str {
        self.output_name.as_deref().unwrap_or("output")
    }
}

/// Blocked top-level jobs on unresolved issues, scope clause appended. The
/// `CROSS JOIN`s keep `jobs` the driving table, so the status seek drives the
/// statement rather than a walk of `issues`. Exposed to the tests that pin its
/// plan.
const CANDIDATES_SQL: &str =
    "SELECT j.id, j.issue_id, COALESCE(j.node_name, j.uri_segment), j.uri_segment,
        e.seq, i.number, p.key, j.execution_id, j.recipe_node_id, j.updated_at
   FROM jobs j
   CROSS JOIN executions e INDEXED BY idx_executions_hold_facts ON e.id = j.execution_id
   CROSS JOIN issues i ON i.id = j.issue_id
   CROSS JOIN projects p ON p.id = i.project_id
  WHERE j.status = 'blocked'
    AND j.parent_job_id IS NULL
    AND j.execution_id IS NOT NULL
    AND j.recipe_node_id IS NOT NULL
    AND i.status NOT IN ('merged', 'closed')";

fn scoped_statements(scope: &ApprovalScope) -> Vec<(String, Vec<SqlValue>)> {
    let listed = |column: &str, ids: &[String]| {
        ids.chunks(cairn_db::sql::IN_LIST_CHUNK)
            .map(|chunk| {
                (
                    format!(
                        "{CANDIDATES_SQL} AND {column} IN ({})",
                        cairn_db::sql::placeholders(chunk.len())
                    ),
                    chunk.iter().cloned().map(SqlValue::Text).collect(),
                )
            })
            .collect()
    };
    match scope {
        ApprovalScope::Issues(ids) => listed("j.issue_id", ids),
        ApprovalScope::Jobs(ids) => listed("j.id", ids),
        ApprovalScope::Project(project_id) => vec![(
            format!("{CANDIDATES_SQL} AND j.project_id = ?"),
            vec![SqlValue::Text(project_id.clone())],
        )],
    }
}

struct Candidate {
    row: BlockedOnOutput,
    execution_id: String,
    node_id: String,
    updated_at: i64,
}

/// Every top-level job in `scope` that is blocked on a missing output, oldest
/// first. A candidate whose facts cannot be derived (a malformed snapshot) is
/// logged and left off; one bad job never fails the reader.
pub async fn blocked_on_output_conn(
    conn: &crate::storage::TrackedConnection,
    scope: &ApprovalScope,
) -> DbResult<Vec<BlockedOnOutput>> {
    let mut candidates = Vec::new();
    for (sql, params) in scoped_statements(scope) {
        let mut rows = conn.query(&sql, params).await?;
        while let Some(row) = rows.next().await? {
            let segment = row.opt_text(3)?;
            let exec_seq = row.opt_i64(4)?;
            let number = row.i64(5)?;
            let project_key = row.text(6)?;
            let node_uri = match (exec_seq, segment.as_deref()) {
                (Some(seq), Some(segment)) => cairn_uri::build_job_base_uri(
                    &project_key,
                    number as i32,
                    seq as i32,
                    segment,
                    None,
                ),
                _ => cairn_uri::build_issue_uri(&project_key, number as i32),
            };
            candidates.push(Candidate {
                row: BlockedOnOutput {
                    job_id: row.text(0)?,
                    issue_id: row.text(1)?,
                    issue_number: number,
                    node_name: row.opt_text(2)?.unwrap_or_else(|| "node".to_string()),
                    exec_seq,
                    node_uri,
                    output_name: None,
                    since: 0,
                },
                execution_id: row.text(7)?,
                node_id: row.text(8)?,
                updated_at: row.i64(9)?,
            });
        }
    }

    let mut blocked = Vec::new();
    for candidate in candidates {
        let Candidate {
            mut row,
            execution_id,
            node_id,
            updated_at,
        } = candidate;
        match cairn_workflow::advancement::recompute::missing_output_conn(
            conn,
            &row.job_id,
            &execution_id,
            &node_id,
        )
        .await
        {
            Ok(Some(missing)) => {
                row.output_name = missing.output_name;
                row.since = missing.since.unwrap_or(updated_at);
                blocked.push(row);
            }
            Ok(None) => {}
            Err(error) => log::warn!(
                "missing-output mark skipped for job {}: {error}",
                row.job_id
            ),
        }
    }
    blocked.sort_by(|a, b| (a.since, &a.job_id).cmp(&(b.since, &b.job_id)));
    Ok(blocked)
}

/// [`blocked_on_output_conn`] on its own read.
pub async fn blocked_on_output(
    db: &LocalDb,
    scope: ApprovalScope,
) -> DbResult<Vec<BlockedOnOutput>> {
    db.read(move |conn| {
        let scope = scope.clone();
        Box::pin(async move { blocked_on_output_conn(conn, &scope).await })
    })
    .await
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::execution::approvals::tests::{query_plan, steps_reaching};

    /// A planner node whose `context-out` feeds a `plan` artifact under a
    /// `user` confirm policy: the shape of every Plan recipe.
    pub(crate) fn planner_snapshot_json() -> String {
        serde_json::json!({
            "recipe": {
                "id": "r", "name": "R", "description": null, "trigger": "manual",
                "nodes": [
                    {"id": "planner", "nodeType": "agent", "name": "Planner", "position": {"x": 0.0, "y": 0.0}},
                    {"id": "plan", "nodeType": "artifact", "name": "Plan", "position": {"x": 1.0, "y": 0.0},
                     "artifactConfig": {"name": "plan", "schema": {"type": "object"}, "confirmPolicy": "user"}}
                ],
                "edges": [
                    {"id": "ctx", "edgeType": "context", "sourceNodeId": "planner", "sourceHandle": "context-out",
                     "targetNodeId": "plan", "targetHandle": "in"}
                ]
            },
            "agents": {}, "skills": {},
            "triggerContext": {"projectId": "p", "triggerType": "manual"},
            "delegatedPackets": [], "createdAt": 1
        })
        .to_string()
    }

    /// Issue `number` (id `issue_id`) in project `p` (key `prj`), its execution
    /// `e-{issue_id}` holding the planner job `job_id`, stored `blocked`, whose
    /// one work turn completed at `ended_at` without writing its plan. The
    /// project row is created when it does not exist.
    pub(crate) async fn seed_blocked_planner(
        db: &LocalDb,
        issue_id: &str,
        number: i64,
        job_id: &str,
        ended_at: i64,
    ) {
        let snapshot = planner_snapshot_json();
        let (issue_id, job_id) = (issue_id.to_string(), job_id.to_string());
        db.write(move |conn| {
            let (snapshot, issue_id, job_id) = (snapshot.clone(), issue_id.clone(), job_id.clone());
            Box::pin(async move {
                use cairn_db::turso::params;
                conn.execute(
                    "INSERT OR IGNORE INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('p','default','P','prj','/tmp/p',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT OR IGNORE INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
                     VALUES (?1,'p',?2,'Plan the backoff','active','none',1,1)",
                    params![issue_id.as_str(), number],
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
                     VALUES ('e-' || ?1,'r',?1,'p','running',1,1,?2)",
                    params![issue_id.as_str(), snapshot.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
                     VALUES (?1,'e-' || ?2,'planner',?2,'p','blocked','planner','Planner',1,?3)",
                    params![job_id.as_str(), issue_id.as_str(), ended_at + 7],
                )
                .await?;
                conn.execute(
                    "INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
                     VALUES ('s-' || ?1, ?1, 'claude', 'open', 1, 1, 1)",
                    params![job_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at)
                     VALUES ('t-' || ?1, 's-' || ?1, ?1, 1, 'complete', 'initial', 1, ?2, ?2)",
                    params![job_id.as_str(), ended_at],
                )
                .await?;
                Ok::<_, crate::storage::DbError>(())
            })
        })
        .await
        .unwrap();
    }

    async fn fixture(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        seed_blocked_planner(&db, "i4", 4, "j-plan", 500).await;
        db
    }

    async fn listed(db: &LocalDb) -> Vec<BlockedOnOutput> {
        blocked_on_output(db, ApprovalScope::Issues(vec!["i4".into()]))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_planner_whose_turn_ended_without_its_plan_is_listed() {
        let db = fixture("missing-output-listed.db").await;
        let blocked = listed(&db).await;
        assert_eq!(
            blocked,
            vec![BlockedOnOutput {
                job_id: "j-plan".into(),
                issue_id: "i4".into(),
                issue_number: 4,
                node_name: "Planner".into(),
                exec_seq: Some(1),
                node_uri: "cairn://p/prj/4/1/planner".into(),
                output_name: Some("plan".into()),
                since: 500,
            }]
        );
        assert_eq!(blocked[0].what(), "plan");
        for scope in [
            ApprovalScope::Jobs(vec!["j-plan".into()]),
            ApprovalScope::Project("p".into()),
        ] {
            assert_eq!(blocked_on_output(&db, scope).await.unwrap(), blocked);
        }
    }

    /// A truncated turn owes its output as a completed one does.
    #[tokio::test]
    async fn a_truncated_turn_without_its_plan_is_listed() {
        let db = fixture("missing-output-truncated.db").await;
        db.execute(
            "UPDATE turns SET state = 'interrupted', end_reason = 'watchdog_recovery'",
            (),
        )
        .await
        .unwrap();
        let blocked = listed(&db).await;
        assert_eq!(blocked.len(), 1, "{blocked:#?}");
    }

    #[tokio::test]
    async fn nothing_is_listed_once_the_job_is_not_blocked_on_its_output() {
        let cases: [(&str, &str); 6] = [
            (
                "an unconfirmed plan is an approval",
                "INSERT INTO artifacts(id, job_id, artifact_type, data, version, output_name, confirmed, created_at, updated_at)
                 VALUES ('a1', 'j-plan', 'plan', '{}', 1, 'plan', 0, 600, 600)",
            ),
            ("a task job", "UPDATE jobs SET parent_job_id = 'j-plan'"),
            ("the issue merged", "UPDATE issues SET status = 'merged'"),
            ("the issue closed", "UPDATE issues SET status = 'closed'"),
            (
                "a later turn is live",
                "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, updated_at)
                 VALUES ('t-live', 's-j-plan', 'j-plan', 2, 'running', 'resume', 600, 600)",
            ),
            (
                "the job was given up on for it",
                "UPDATE jobs SET return_abandoned_at = 600",
            ),
        ];
        for (index, (case, sql)) in cases.into_iter().enumerate() {
            let db = fixture(&format!("missing-output-not-listed-{index}.db")).await;
            db.execute(sql, ()).await.unwrap();
            assert!(listed(&db).await.is_empty(), "{case}");
        }
    }

    #[tokio::test]
    async fn a_later_memory_review_turn_does_not_move_since() {
        let db = fixture("missing-output-review.db").await;
        db.execute(
            "INSERT INTO turns(id, session_id, job_id, sequence, state, start_reason, created_at, ended_at, updated_at)
             VALUES ('t-review', 's-j-plan', 'j-plan', 2, 'complete', 'memory_review', 600, 900, 900)",
            (),
        )
        .await
        .unwrap();
        let blocked = listed(&db).await;
        assert_eq!(blocked.len(), 1, "{blocked:#?}");
        assert_eq!(blocked[0].since, 500);
    }

    #[tokio::test]
    async fn a_malformed_snapshot_skips_only_its_own_job() {
        let db = fixture("missing-output-malformed.db").await;
        seed_blocked_planner(&db, "i5", 5, "j-broken", 400).await;
        db.execute(
            "UPDATE executions SET snapshot = 'not json' WHERE id = 'e-i5'",
            (),
        )
        .await
        .unwrap();
        let blocked = blocked_on_output(&db, ApprovalScope::Project("p".into()))
            .await
            .unwrap();
        assert_eq!(
            blocked
                .iter()
                .map(|b| b.job_id.as_str())
                .collect::<Vec<_>>(),
            vec!["j-plan"]
        );
    }

    /// The candidate read runs on every census and notification rebuild, so it
    /// seeks blocked jobs and never walks `jobs`, and reads executions only
    /// through the covering hold-facts index, never a snapshot-carrying row.
    #[tokio::test]
    async fn the_candidate_read_seeks_blocked_jobs_and_never_reads_a_snapshot() {
        let db = fixture("missing-output-plan.db").await;
        for scope in [
            ApprovalScope::Issues(vec!["i4".into(), "i5".into()]),
            ApprovalScope::Jobs(vec!["j-plan".into()]),
            ApprovalScope::Project("p".into()),
        ] {
            for (sql, params) in scoped_statements(&scope) {
                let plan = query_plan(&db, sql, params).await;
                assert!(!plan.contains("SCAN"), "{scope:?}:\n{plan}");
                assert!(!plan.contains("AUTOMATIC"), "{scope:?}:\n{plan}");
                for step in steps_reaching(&plan, "e") {
                    assert!(step.contains("COVERING INDEX"), "{scope:?}:\n{plan}");
                }
            }
        }
    }
}
