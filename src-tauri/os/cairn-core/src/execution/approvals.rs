//! Pending approvals: the one definition of "a gated artifact is waiting on its
//! approver".
//!
//! A job's approval is pending when the job is top-level and has not ended, it
//! owns no pull request (a PR's lifecycle is its own approval), its issue is
//! unresolved, and the newest version of one of its artifact chains is
//! unconfirmed. The census, the notification center, the gated-artifact
//! affordance and the ask-condition sweep all read it here.
//!
//! ## Not keyed on `blocked`
//!
//! A planner whose plan waits can still be stored `running` (a warm run, or the
//! post-completion memory review), so a check on `jobs.status = 'blocked'`
//! misses it. Confirmability is already decoupled from `blocked`
//! (`cairn_workflow::checkpoints::validate_confirmable`, CAIRN-1576); this is
//! the same rule.
//!
//! ## Which chain is the gate, without the snapshot
//!
//! Confirmation targets the node's terminal artifact, whose name comes from the
//! execution snapshot. Loading a snapshot here would read hundreds of kilobytes
//! per job on every census. It is not needed: the write path gives a `user`
//! confirm policy only to the terminal name (or the generic alias), so only
//! the terminal chain is ever stored unconfirmed (`addressed_confirm_policy`
//! in `mcp::handlers::comments_artifacts`), and once a chain is confirmed every
//! later write keeps it confirmed (`resolve_confirmed`). So the chain whose
//! newest version is unconfirmed is the terminal artifact awaiting a human,
//! read from the confirmation fact itself.
//!
//! A legacy chain may have a NULL `output_name` (written before names were
//! stored); its versions are compared among themselves by a separate probe,
//! because `NULL = NULL` is never true.
//!
//! ## Index-only on `artifacts`
//!
//! Every artifact column these statements take is in
//! `idx_artifacts_awaiting_confirmation` (partial on `confirmed = 0`), and the
//! newer-version probe is covered by `idx_artifacts_output_job_version`, so no
//! artifact row is ever read. The tests pin both plans.

use std::collections::HashMap;

use cairn_db::turso::Value as SqlValue;

use crate::execution::gates::authoring::gate_vocabulary;
use crate::routes::TriggerClause;
use crate::storage::{DbResult, LocalDb, RowExt};

/// How long an undeferred approval may wait before it reads `stale`. A mark,
/// never a wake: nothing is sent when an approval crosses it (cairn/5708).
pub const STALE_AFTER_SECS: i64 = 7 * 86_400;

/// The `ask_conditions.ask_kind` of a pending gated artifact.
pub const ASK_KIND_ARTIFACT: &str = "artifact";

/// A deferral that has not fired: the approver asked to be woken when `until`
/// comes true.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenDeferral {
    pub condition_id: String,
    pub until: Vec<TriggerClause>,
    /// The clauses as the held-execution census renders them, joined by `and`.
    pub until_rendered: String,
    pub created_at: i64,
}

/// One gated artifact waiting on its approver.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingApproval {
    pub job_id: String,
    pub issue_id: String,
    pub issue_number: i64,
    /// The node's display name (`node_name`, else its URI segment).
    pub node_name: String,
    pub exec_seq: Option<i64>,
    /// The exact pending version.
    pub artifact_id: String,
    pub output_name: Option<String>,
    /// The artifact's canonical named address (`/plan`, never the `/artifact`
    /// alias, CAIRN-1219).
    pub artifact_uri: String,
    /// The producing node's own address (its `/messages` is where feedback
    /// goes).
    pub node_uri: String,
    /// When this version was written: the age of the wait.
    pub pending_since: i64,
    pub deferral: Option<OpenDeferral>,
    /// The newest deferral on this version that fired because its cause came
    /// true or could no longer come true. Its id makes the operator's
    /// re-surfaced notification a new occurrence.
    pub resurfaced_by: Option<String>,
}

impl PendingApproval {
    /// What is waiting, by name: `plan`, `create-pr`, or `output` for an
    /// unnamed checkpoint artifact.
    pub fn what(&self) -> &str {
        self.output_name.as_deref().unwrap_or("output")
    }

    pub fn age(&self, now: i64) -> i64 {
        now.saturating_sub(self.pending_since).max(0)
    }

    /// Stale means waiting longer than [`STALE_AFTER_SECS`] with no deferral.
    /// A deferral is its own explanation, so a deferred approval never is.
    pub fn is_stale(&self, now: i64) -> bool {
        self.deferral.is_none() && self.age(now) > STALE_AFTER_SECS
    }

    /// The first instant [`Self::is_stale`] holds, for an undeferred approval
    /// that is not stale yet, so a cached projection knows when to rebuild.
    pub fn stale_from(&self) -> Option<i64> {
        self.deferral
            .is_none()
            .then_some(self.pending_since + STALE_AFTER_SECS + 1)
    }
}

/// Which jobs to look at.
#[derive(Debug, Clone)]
pub enum ApprovalScope {
    Issues(Vec<String>),
    Jobs(Vec<String>),
    Project(String),
}

/// The pending-approval statement, scope clause appended. Exposed to the tests
/// that pin its plan.
const PENDING_SQL: &str =
    "SELECT j.id, j.issue_id, COALESCE(j.node_name, j.uri_segment), j.uri_segment,
        e.seq, i.number, p.key, a.id, a.output_name, a.version, a.created_at
   FROM jobs j
   JOIN artifacts a ON a.job_id = j.id
   JOIN issues i ON i.id = j.issue_id
   JOIN projects p ON p.id = i.project_id
   LEFT JOIN executions e ON e.id = j.execution_id
  WHERE a.confirmed = 0
    AND j.parent_job_id IS NULL
    AND j.status NOT IN ('complete', 'failed', 'cancelled')
    AND i.status NOT IN ('merged', 'closed')
    AND NOT EXISTS (SELECT 1 FROM merge_requests mr WHERE mr.job_id = j.id)
    AND NOT EXISTS (SELECT 1 FROM artifacts n
                     WHERE n.output_name = a.output_name AND n.job_id = a.job_id
                       AND n.version > a.version)
    AND NOT EXISTS (SELECT 1 FROM artifacts u
                     WHERE a.output_name IS NULL AND u.output_name IS NULL
                       AND u.job_id = a.job_id AND u.version > a.version)";

const CONDITIONS_SQL: &str = "SELECT id, ask_id, until_json, created_at, fired_at, fired_kind
   FROM ask_conditions
  WHERE ask_kind = 'artifact' AND ask_id IN";

fn scoped_statements(scope: &ApprovalScope) -> Vec<(String, Vec<SqlValue>)> {
    let listed = |column: &str, ids: &[String]| {
        ids.chunks(cairn_db::sql::IN_LIST_CHUNK)
            .map(|chunk| {
                (
                    format!(
                        "{PENDING_SQL} AND {column} IN ({})",
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
            format!("{PENDING_SQL} AND j.project_id = ?"),
            vec![SqlValue::Text(project_id.clone())],
        )],
    }
}

/// Every pending approval in `scope`, at most one per job (its newest pending
/// chain), each with its open deferral and its last re-surfacing cause.
pub async fn pending_approvals_conn(
    conn: &crate::storage::TrackedConnection,
    scope: &ApprovalScope,
) -> DbResult<Vec<PendingApproval>> {
    let mut newest: HashMap<String, (i64, i64, PendingApproval)> = HashMap::new();
    for (sql, params) in scoped_statements(scope) {
        let mut rows = conn.query(&sql, params).await?;
        while let Some(row) = rows.next().await? {
            let job_id = row.text(0)?;
            let segment = row.opt_text(3)?;
            let exec_seq = row.opt_i64(4)?;
            let number = row.i64(5)?;
            let project_key = row.text(6)?;
            let output_name = row.opt_text(8)?;
            let version = row.i64(9)?;
            let created_at = row.i64(10)?;
            let (artifact_uri, node_uri) = match (exec_seq, segment.as_deref()) {
                (Some(seq), Some(segment)) => (
                    cairn_uri::build_node_artifact_uri_named(
                        &project_key,
                        number as i32,
                        seq as i32,
                        segment,
                        output_name.as_deref(),
                    ),
                    cairn_uri::build_job_base_uri(
                        &project_key,
                        number as i32,
                        seq as i32,
                        segment,
                        None,
                    ),
                ),
                _ => {
                    let issue = cairn_uri::build_issue_uri(&project_key, number as i32);
                    (issue.clone(), issue)
                }
            };
            let approval = PendingApproval {
                job_id: job_id.clone(),
                issue_id: row.text(1)?,
                issue_number: number,
                node_name: row.opt_text(2)?.unwrap_or_else(|| "node".to_string()),
                exec_seq,
                artifact_id: row.text(7)?,
                output_name,
                artifact_uri,
                node_uri,
                pending_since: created_at,
                deferral: None,
                resurfaced_by: None,
            };
            let replaces = newest
                .get(&job_id)
                .is_none_or(|(at, v, _)| (created_at, version) > (*at, *v));
            if replaces {
                newest.insert(job_id, (created_at, version, approval));
            }
        }
    }
    let mut approvals: Vec<PendingApproval> = newest
        .into_values()
        .map(|(_, _, approval)| approval)
        .collect();
    approvals.sort_by(|a, b| (a.pending_since, &a.job_id).cmp(&(b.pending_since, &b.job_id)));
    attach_conditions(conn, &mut approvals).await?;
    Ok(approvals)
}

/// [`pending_approvals_conn`] on its own read.
pub async fn pending_approvals(
    db: &LocalDb,
    scope: ApprovalScope,
) -> DbResult<Vec<PendingApproval>> {
    db.read(move |conn| {
        let scope = scope.clone();
        Box::pin(async move { pending_approvals_conn(conn, &scope).await })
    })
    .await
}

/// The pending approval on one job, if there is one.
pub async fn pending_approval_for_job_conn(
    conn: &crate::storage::TrackedConnection,
    job_id: &str,
) -> DbResult<Option<PendingApproval>> {
    Ok(
        pending_approvals_conn(conn, &ApprovalScope::Jobs(vec![job_id.to_string()]))
            .await?
            .into_iter()
            .next(),
    )
}

async fn attach_conditions(
    conn: &crate::storage::TrackedConnection,
    approvals: &mut [PendingApproval],
) -> DbResult<()> {
    if approvals.is_empty() {
        return Ok(());
    }
    let vocabulary = gate_vocabulary();
    let index: HashMap<String, usize> = approvals
        .iter()
        .enumerate()
        .map(|(at, approval)| (approval.artifact_id.clone(), at))
        .collect();
    let ids: Vec<String> = index.keys().cloned().collect();
    for chunk in ids.chunks(cairn_db::sql::IN_LIST_CHUNK) {
        let sql = format!(
            "{CONDITIONS_SQL} ({}) ORDER BY created_at ASC, id ASC",
            cairn_db::sql::placeholders(chunk.len())
        );
        let params: Vec<SqlValue> = chunk.iter().cloned().map(SqlValue::Text).collect();
        let mut rows = conn.query(&sql, params).await?;
        while let Some(row) = rows.next().await? {
            let condition_id = row.text(0)?;
            let Some(&at) = index.get(&row.text(1)?) else {
                continue;
            };
            let approval = &mut approvals[at];
            let fired_at = row.opt_i64(4)?;
            match (fired_at, row.opt_text(5)?.as_deref()) {
                (None, _) => {
                    let until = parse_until(&row.text(2)?);
                    approval.deferral = Some(OpenDeferral {
                        condition_id,
                        until_rendered: render_until(&vocabulary, &until),
                        until,
                        created_at: row.i64(3)?,
                    });
                }
                (Some(_), Some("satisfied" | "foreclosed")) => {
                    approval.resurfaced_by = Some(condition_id);
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// A stored `until_json`, as clauses. A row this code wrote is always an array
/// of objects; anything else reads as no clauses rather than failing a census.
pub fn parse_until(until_json: &str) -> Vec<TriggerClause> {
    serde_json::from_str(until_json).unwrap_or_default()
}

/// Clauses as prose, the way the held-execution census renders `after:` gates.
pub fn render_until(
    vocabulary: &crate::execution::gates::vocabulary::GateVocabulary,
    until: &[TriggerClause],
) -> String {
    until
        .iter()
        .map(|clause| vocabulary.render(clause))
        .collect::<Vec<_>>()
        .join(" and ")
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Project `prj`, issue 4 on execution 1 with a planner job (stored
    /// `running`, as 4527's was while its plan waited).
    pub(crate) async fn approvals_fixture(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p','default','P','prj','/tmp/p',1,1);
             INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
               VALUES ('i4','p',4,'Four','active','none',1,1);
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('e4','recipe','i4','p','running',1,1);
             INSERT INTO jobs(id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
               VALUES ('j-plan','e4','i4','p','running','planner','Planner',1,1);",
        )
        .await
        .unwrap();
        db
    }

    pub(crate) async fn write_plan(db: &LocalDb, id: &str, version: i64, confirmed: i64, at: i64) {
        db.execute(
            "INSERT INTO artifacts(id, job_id, artifact_type, data, version, output_name, confirmed, created_at, updated_at)
             VALUES (?1, 'j-plan', 'plan', '{}', ?2, 'plan', ?3, ?4, ?4)",
            cairn_db::turso::params![id, version, confirmed, at],
        )
        .await
        .unwrap();
    }

    async fn pending(db: &LocalDb) -> Vec<PendingApproval> {
        pending_approvals(db, ApprovalScope::Issues(vec!["i4".into()]))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_running_planner_with_an_unconfirmed_plan_is_pending() {
        let db = approvals_fixture("approvals-running.db").await;
        write_plan(&db, "a1", 1, 0, 500).await;
        let listed = pending(&db).await;
        assert_eq!(listed.len(), 1, "{listed:#?}");
        let approval = &listed[0];
        assert_eq!(approval.artifact_id, "a1");
        assert_eq!(approval.node_name, "Planner");
        assert_eq!(approval.pending_since, 500);
        assert_eq!(approval.artifact_uri, "cairn://p/prj/4/1/planner/plan");
        assert_eq!(approval.what(), "plan");
        for scope in [
            ApprovalScope::Jobs(vec!["j-plan".into()]),
            ApprovalScope::Project("p".into()),
        ] {
            assert_eq!(pending_approvals(&db, scope).await.unwrap(), listed);
        }
    }

    #[tokio::test]
    async fn the_newest_unconfirmed_version_is_the_one_pending() {
        let db = approvals_fixture("approvals-revised.db").await;
        write_plan(&db, "a1", 1, 0, 500).await;
        write_plan(&db, "a2", 2, 0, 700).await;
        let listed = pending(&db).await;
        assert_eq!(
            listed
                .iter()
                .map(|a| a.artifact_id.as_str())
                .collect::<Vec<_>>(),
            vec!["a2"]
        );
        assert_eq!(listed[0].pending_since, 700, "a revision restarts the wait");
    }

    #[tokio::test]
    async fn nothing_is_pending_once_it_cannot_be_approved() {
        let cases: [(&str, &str); 6] = [
            ("confirmed", "UPDATE artifacts SET confirmed = 1"),
            (
                "a confirmed later version",
                "INSERT INTO artifacts(id, job_id, artifact_type, data, version, output_name, confirmed, created_at, updated_at)
                 VALUES ('a9', 'j-plan', 'plan', '{}', 2, 'plan', 1, 900, 900)",
            ),
            ("the job ended", "UPDATE jobs SET status = 'complete'"),
            ("the issue merged", "UPDATE issues SET status = 'merged'"),
            (
                "the job owns a PR",
                "INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
                 VALUES ('mr', 'j-plan', 'p', 'i4', 'T', 'b', 'main', 'open', 1, 1)",
            ),
            ("a task job", "UPDATE jobs SET parent_job_id = 'j-plan'"),
        ];
        for (index, (case, sql)) in cases.into_iter().enumerate() {
            let db = approvals_fixture(&format!("approvals-not-pending-{index}.db")).await;
            write_plan(&db, "a1", 1, 0, 500).await;
            db.execute(sql, ()).await.unwrap();
            assert!(pending(&db).await.is_empty(), "{case}");
        }
    }

    /// A legacy chain with no `output_name` (openpnp-mcp/5's plan): only its
    /// newest version is pending, and confirming that one settles it.
    #[tokio::test]
    async fn an_unnamed_legacy_chain_is_pending_only_at_its_newest_version() {
        let db = approvals_fixture("approvals-unnamed.db").await;
        for (id, version, at) in [("u1", 1, 500), ("u2", 2, 600)] {
            db.execute(
                "INSERT INTO artifacts(id, job_id, artifact_type, data, version, confirmed, created_at, updated_at)
                 VALUES (?1, 'j-plan', 'plan', '{}', ?2, 0, ?3, ?3)",
                cairn_db::turso::params![id, version, at],
            )
            .await
            .unwrap();
        }
        let listed = pending(&db).await;
        assert_eq!(
            listed
                .iter()
                .map(|a| a.artifact_id.as_str())
                .collect::<Vec<_>>(),
            vec!["u2"]
        );
        assert_eq!(listed[0].what(), "output");
        db.execute("UPDATE artifacts SET confirmed = 1 WHERE id = 'u2'", ())
            .await
            .unwrap();
        assert!(
            pending(&db).await.is_empty(),
            "the older version is not pending"
        );
    }

    #[tokio::test]
    async fn stale_is_a_week_undeferred() {
        let db = approvals_fixture("approvals-stale.db").await;
        write_plan(&db, "a1", 1, 0, 0).await;
        let approval = pending(&db).await.remove(0);
        assert!(!approval.is_stale(STALE_AFTER_SECS));
        assert!(approval.is_stale(STALE_AFTER_SECS + 1));
        assert_eq!(approval.stale_from(), Some(STALE_AFTER_SECS + 1));
        let deferred = PendingApproval {
            deferral: Some(OpenDeferral {
                condition_id: "c".into(),
                until: Vec::new(),
                until_rendered: String::new(),
                created_at: 0,
            }),
            ..approval
        };
        assert!(!deferred.is_stale(10 * STALE_AFTER_SECS));
        assert_eq!(deferred.stale_from(), None);
    }

    pub(crate) async fn query_plan(db: &LocalDb, sql: String, params: Vec<SqlValue>) -> String {
        db.read(move |conn| {
            let sql = format!("EXPLAIN QUERY PLAN {sql}");
            let params = params.clone();
            Box::pin(async move {
                let mut rows = conn.query(&sql, params).await?;
                let mut plan = Vec::new();
                while let Some(row) = rows.next().await? {
                    plan.push(row.text(3)?);
                }
                Ok(plan.join("\n"))
            })
        })
        .await
        .unwrap()
    }

    /// The plan steps that read `alias`, whether Turso names it bare
    /// (`SCAN a …`) or with its table (`SCAN executions AS ex …`).
    pub(crate) fn steps_reaching<'a>(plan: &'a str, alias: &str) -> Vec<&'a str> {
        plan.lines()
            .filter(|line| {
                let words: Vec<&str> = line.split_whitespace().collect();
                words.get(1) == Some(&alias)
                    || (words.get(2) == Some(&"AS") && words.get(3) == Some(&alias))
            })
            .collect()
    }

    /// Every plan step that reaches `alias` reads only an index.
    pub(crate) fn assert_index_only(plan: &str, aliases: &[&str]) {
        for alias in aliases {
            let steps = steps_reaching(plan, alias);
            assert!(!steps.is_empty(), "no step reaches {alias}:\n{plan}");
            for step in steps {
                assert!(
                    step.contains("COVERING INDEX"),
                    "{alias} is read from its table, which assembles the whole wide row:\n{plan}"
                );
            }
        }
        assert!(!plan.contains("AUTOMATIC"), "{plan}");
    }

    /// The loader runs on every census, notification rebuild and affordance
    /// render, so it must never read an artifact row (cairn/5894: a row read
    /// assembles the row's `data`). Every scope's plan is index-only on both
    /// artifact aliases.
    #[tokio::test]
    async fn the_pending_approval_reads_never_touch_an_artifact_row() {
        let db = approvals_fixture("approvals-plan.db").await;
        for scope in [
            ApprovalScope::Issues(vec!["i4".into(), "i5".into()]),
            ApprovalScope::Jobs(vec!["j-plan".into()]),
            ApprovalScope::Project("p".into()),
        ] {
            for (sql, params) in scoped_statements(&scope) {
                let plan = query_plan(&db, sql, params).await;
                assert_index_only(&plan, &["a", "n", "u"]);
                assert!(
                    plan.contains("idx_artifacts_awaiting_confirmation"),
                    "{scope:?}:\n{plan}"
                );
                assert!(
                    !plan.contains("SCAN j") && !plan.contains("SCAN a"),
                    "{scope:?}:\n{plan}"
                );
            }
        }
        let plan = query_plan(
            &db,
            format!("{CONDITIONS_SQL} (?, ?) ORDER BY created_at ASC, id ASC"),
            vec![SqlValue::Text("a".into()), SqlValue::Text("b".into())],
        )
        .await;
        assert!(plan.contains("idx_ask_conditions_ask"), "{plan}");
        assert!(!plan.contains("SCAN ask_conditions"), "{plan}");
    }

    /// The executions step is one primary-key seek per pending approval. It is
    /// index-only once cairn/5894's `idx_executions_hold_facts` (keyed by id,
    /// carrying `seq`) exists, applied here with that migration's exact DDL.
    #[tokio::test]
    async fn the_execution_seq_is_index_only_under_the_hold_facts_index() {
        let db = approvals_fixture("approvals-plan-executions.db").await;
        let pinned = query_plan(
            &db,
            scoped_statements(&ApprovalScope::Jobs(vec!["j-plan".into()]))
                .remove(0)
                .0,
            vec![SqlValue::Text("j-plan".into())],
        )
        .await;
        assert!(!pinned.contains("SCAN e"), "{pinned}");
        db.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_executions_hold_facts
                ON executions(id, issue_id, seq, lifecycle, released_at, runner_device_id,
                              release_override_json)",
        )
        .await
        .unwrap();
        for scope in [
            ApprovalScope::Issues(vec!["i4".into()]),
            ApprovalScope::Jobs(vec!["j-plan".into()]),
            ApprovalScope::Project("p".into()),
        ] {
            for (sql, params) in scoped_statements(&scope) {
                let plan = query_plan(&db, sql, params).await;
                assert_index_only(&plan, &["a", "n", "u", "e"]);
            }
        }
    }
}
