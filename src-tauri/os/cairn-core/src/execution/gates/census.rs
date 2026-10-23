//! The gate census: the ONE place an `after:` gate becomes words.
//!
//! Release is asymmetric — a gate that comes true starts its execution, but a
//! gate that never comes true produces no attention at all. Some predicates are
//! unsatisfiable in practice rather than in principle (a check lane that has no
//! executor, a lane that rarely goes reusably green), so nothing forecloses them
//! and nothing wakes anyone. The rendering is the whole defense: every surface
//! that lists executions or summarizes an issue shows a held execution as held,
//! names each unmet predicate, and says what the evidence for it is.
//!
//! Everything that displays a gate goes through [`census`]. Rust resources call
//! it directly; the frontend receives its output on the issue and execution DTOs
//! (`Issue::held_executions`, `Execution::gate_census`) and never renders a
//! predicate itself. The predicate prose comes from the fact source's own
//! [`GateFact::render`](super::vocabulary::GateFact::render) and the evidence
//! phrases from [`GateOutcome::evidence_phrase`], so a held execution reads the
//! same in the sidebar, the issue list, the project overview, and a thread seed.

use std::collections::HashMap;
use std::sync::LazyLock;

use cairn_common::identity::PrincipalRef;
use cairn_db::sql::{placeholders, IN_LIST_CHUNK};
use cairn_db::turso::{Connection, Value as SqlValue};
use serde_json::Value;

use super::vocabulary::{GateOutcome, GateVocabulary};
use crate::models::{
    ExecutionGate, ExecutionGateCensus, GateCensusEntry, GateEvidenceState, HeldExecution,
    ReleaseOverride,
};
use crate::routes::{FactRegistry, TriggerClause};
use crate::storage::{DbResult, RowExt};
use cairn_workflow::gates::store::list_gates_for_executions_conn;

/// The shared fact registry, whose gate half is the vocabulary every census
/// renders through. Built once: rendering is on every issue-list read.
static REGISTRY: LazyLock<FactRegistry> = LazyLock::new(FactRegistry::default);

fn vocabulary() -> &'static GateVocabulary {
    REGISTRY.gates()
}

/// Prefix for a foreclosed gate's evidence phrase. The reason after it is the
/// fact source's own words for what closed the door.
const FORECLOSED_PREFIX: &str = "can never be satisfied";

/// Render one gate: its predicate in prose and where its evidence stands.
///
/// Precedence is satisfied, then foreclosed, then the newest resolution. A gate
/// no sweep has resolved yet reads as `no verdict yet`, which is exactly true.
pub fn render_gate(vocabulary: &GateVocabulary, gate: &ExecutionGate) -> GateCensusEntry {
    let predicate = match serde_json::from_str::<TriggerClause>(&gate.predicate_json) {
        Ok(clause) => vocabulary.render(&clause),
        // A stored predicate that no longer parses is still something an
        // execution is held behind, so it renders as itself rather than
        // vanishing from the census.
        Err(_) => gate.predicate_json.clone(),
    };
    let (state, evidence) = if gate.is_satisfied() {
        (
            GateEvidenceState::Satisfied,
            GateOutcome::Satisfied.evidence_phrase().to_string(),
        )
    } else if gate.is_foreclosed() {
        (
            GateEvidenceState::Foreclosed,
            match gate.foreclosed_reason.as_deref().map(str::trim) {
                Some(reason) if !reason.is_empty() => format!("{FORECLOSED_PREFIX}: {reason}"),
                _ => FORECLOSED_PREFIX.to_string(),
            },
        )
    } else if gate.last_outcome.as_deref() == Some(GateOutcome::Refuted.as_str()) {
        (
            GateEvidenceState::Refuted,
            GateOutcome::Refuted.evidence_phrase().to_string(),
        )
    } else {
        (
            GateEvidenceState::NoEvidence,
            GateOutcome::NoEvidence.evidence_phrase().to_string(),
        )
    };
    GateCensusEntry {
        gate_id: gate.id.clone(),
        predicate,
        state,
        evidence,
    }
}

/// Prefix for the summary of a gated, unreleased execution that can never
/// start. The reason after it names what ended it.
const WONT_START_PREFIX: &str = "won't start";

/// The census for one execution, or `None` when it has nothing to say: an
/// execution filed without gates and never released by hand.
///
/// `gates` must be this execution's gates in authored order. `wont_start` is
/// why a gated, unreleased execution fails [`cairn_db::awaiting_release_sql!`]
/// — the predicate the sweep releases by — and so can never start (see
/// [`wont_start_reason`]). It is ignored for an execution that is not gated and
/// unreleased, which has already started or never waited.
pub fn census(
    gates: &[ExecutionGate],
    released_at: Option<i64>,
    release_override_json: Option<&str>,
    wont_start: Option<&str>,
) -> Option<ExecutionGateCensus> {
    census_with(
        vocabulary(),
        gates,
        released_at,
        release_override_json,
        wont_start,
    )
}

/// [`census`] over an explicit vocabulary, so a registered extension fact
/// renders through the same path a built-in one does.
pub fn census_with(
    vocabulary: &GateVocabulary,
    gates: &[ExecutionGate],
    released_at: Option<i64>,
    release_override_json: Option<&str>,
    wont_start: Option<&str>,
) -> Option<ExecutionGateCensus> {
    let release_override = release_override_json.map(parse_release_override);
    if gates.is_empty() && release_override.is_none() {
        return None;
    }
    let entries: Vec<GateCensusEntry> = gates
        .iter()
        .map(|gate| render_gate(vocabulary, gate))
        .collect();
    let gated_unreleased = !gates.is_empty() && released_at.is_none();
    let wont_start = wont_start.filter(|_| gated_unreleased).map(str::to_string);
    let held = gated_unreleased && wont_start.is_none();
    let summary = match &wont_start {
        // The gates are not what stops it, so the summary does not list them
        // as though they were; `gates` still carries every one.
        Some(reason) => format!("{WONT_START_PREFIX}: {reason}"),
        None => summarize(held, &entries, release_override.as_ref()),
    };
    Some(ExecutionGateCensus {
        held,
        wont_start,
        summary,
        gates: entries,
        release_override,
    })
}

/// Why a gated, unreleased execution that is no longer live can never start.
///
/// The issue's resolution outranks supersession, matching how a gate patch on
/// such an execution is refused: the issue being done is the larger fact.
/// `issue` is `(project key, number, merged)` and present only when the issue
/// is resolved; `successor_seq` is the seq of the execution whose launch
/// fenced this one, when the supersession audit row names it.
pub fn wont_start_reason(
    superseded: bool,
    successor_seq: Option<i64>,
    issue: Option<(&str, i64, bool)>,
) -> String {
    if let Some((key, number, merged)) = issue {
        let resolution = if merged { "merged" } else { "closed" };
        return format!(
            "{}/{number} {resolution}",
            cairn_common::uri::canonical_project(key)
        );
    }
    match (superseded, successor_seq) {
        (true, Some(seq)) => format!("superseded by exec {seq}"),
        _ => "superseded".to_string(),
    }
}

/// One line naming every gate that is not yet met, each with its evidence.
fn summarize(
    held: bool,
    entries: &[GateCensusEntry],
    release_override: Option<&ReleaseOverride>,
) -> String {
    let waiting: Vec<String> = entries
        .iter()
        .filter(|entry| entry.state != GateEvidenceState::Satisfied)
        .map(|entry| format!("{} ({})", entry.predicate, entry.evidence))
        .collect();
    if held {
        return match waiting.is_empty() {
            // Every gate latched but the sweep has not released it yet: the
            // window is one tick, and saying so beats calling it waiting.
            true => "held, every gate met — releases on the next sweep".to_string(),
            false => format!("held, waiting on {}", waiting.join("; ")),
        };
    }
    match release_override {
        Some(release) if waiting.is_empty() => {
            format!("released early by {} (override)", release.principal)
        }
        Some(release) => format!(
            "released early by {} (override), not waiting on {}",
            release.principal,
            waiting.join("; ")
        ),
        None => "released: every gate met".to_string(),
    }
}

/// `exec N — <summary>`: the line a text census prints per execution.
pub fn census_line(seq: Option<i64>, census: &ExecutionGateCensus) -> String {
    match seq {
        Some(seq) => format!("exec {seq} — {}", census.summary),
        None => census.summary.clone(),
    }
}

/// Decode `release_override_json` (`{principal, at}`).
///
/// The override's PRESENCE is the fact that matters — someone decided not to
/// wait — so an unreadable record still renders as an override, naming what
/// it can.
fn parse_release_override(json: &str) -> ReleaseOverride {
    let value: Value = serde_json::from_str(json).unwrap_or(Value::Null);
    let principal = value
        .get("principal")
        .map(principal_label)
        .unwrap_or_else(|| "an unrecorded principal".to_string());
    ReleaseOverride {
        principal,
        at: value.get("at").and_then(Value::as_i64),
    }
}

fn principal_label(value: &Value) -> String {
    if let Some(text) = value.as_str() {
        return text.to_string();
    }
    match serde_json::from_value::<PrincipalRef>(value.clone()) {
        Ok(PrincipalRef::Human { subject, .. }) => subject,
        Ok(PrincipalRef::Agent { node, .. }) => node,
        Ok(PrincipalRef::Machine { device_id }) => format!("machine {device_id}"),
        Ok(PrincipalRef::External {
            provider,
            namespace,
            id,
        }) => format!("{provider}:{namespace}/{id}"),
        Err(_) => value.to_string(),
    }
}

fn bound_ids(ids: &[String]) -> Vec<SqlValue> {
    ids.iter().map(|id| SqlValue::Text(id.clone())).collect()
}

/// Every fact a census reads about one execution, beyond its gates. A filter
/// over `e` (and optionally `i`/`p`) is appended after `WHERE`.
///
/// Column 5 is the shared predicate's complement — gated and unreleased, yet
/// NOT [`cairn_db::awaiting_release_sql!`] — so the census calls an execution
/// held exactly when the sweep would release it, never by a second definition
/// that could drift from the sweep's.
const CENSUS_ROW_SQL: &str = concat!(
    "SELECT e.id, e.issue_id, e.seq, e.released_at, e.release_override_json,
            CASE WHEN ",
    cairn_db::gated_unreleased_sql!(),
    " AND NOT (",
    cairn_db::awaiting_release_sql!(),
    ") THEN 1 ELSE 0 END,
            e.lifecycle = 'superseded',
            (SELECT MAX(successor.seq)
               FROM execution_supersessions supersession
               JOIN executions successor ON successor.id = supersession.successor_execution_id
              WHERE supersession.superseded_execution_id = e.id),
            p.key, i.number, i.merged_at, i.closed_at
       FROM executions e
       LEFT JOIN issues i ON i.id = e.issue_id
       LEFT JOIN projects p ON p.id = i.project_id
      WHERE "
);

/// One execution's census-relevant facts, as [`CENSUS_ROW_SQL`] reads them.
struct CensusRow {
    id: String,
    issue_id: Option<String>,
    seq: Option<i64>,
    released_at: Option<i64>,
    release_override_json: Option<String>,
    wont_start: Option<String>,
}

/// [`CENSUS_ROW_SQL`] over `filter`, a predicate on `e` (and optionally
/// `i`/`p`), in issue then execution order.
fn census_rows_sql(filter: &str) -> String {
    format!("{CENSUS_ROW_SQL}{filter} ORDER BY e.issue_id, e.seq, e.id")
}

async fn census_rows(
    conn: &Connection,
    sql: &str,
    bound: Vec<SqlValue>,
) -> DbResult<Vec<CensusRow>> {
    let mut rows = conn.query(sql, bound).await?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await? {
        let wont_start = if row.i64(5)? != 0 {
            let key = row.opt_text(8)?;
            let number = row.opt_i64(9)?;
            let merged = row.opt_i64(10)?.is_some();
            let resolved = merged || row.opt_i64(11)?.is_some();
            let issue = match (resolved, key.as_deref(), number) {
                (true, Some(key), Some(number)) => Some((key, number, merged)),
                _ => None,
            };
            Some(wont_start_reason(row.i64(6)? != 0, row.opt_i64(7)?, issue))
        } else {
            None
        };
        out.push(CensusRow {
            id: row.text(0)?,
            issue_id: row.opt_text(1)?,
            seq: row.opt_i64(2)?,
            released_at: row.opt_i64(3)?,
            release_override_json: row.opt_text(4)?,
            wont_start,
        });
    }
    Ok(out)
}

/// Render `rows` against their gates, keeping only those with a census.
async fn render_rows(
    conn: &Connection,
    rows: Vec<CensusRow>,
) -> DbResult<Vec<(CensusRow, ExecutionGateCensus)>> {
    let ids: Vec<String> = rows.iter().map(|row| row.id.clone()).collect();
    let mut gates = list_gates_for_executions_conn(conn, &ids).await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let execution_gates = gates.remove(&row.id).unwrap_or_default();
            census(
                &execution_gates,
                row.released_at,
                row.release_override_json.as_deref(),
                row.wont_start.as_deref(),
            )
            .map(|census| (row, census))
        })
        .collect())
}

/// The census rows of `n` listed executions that can have a census: gated, or
/// released by hand.
fn executions_census_sql(n: usize) -> String {
    census_rows_sql(&format!(
        "e.id IN ({})
           AND (e.release_override_json IS NOT NULL
                OR EXISTS (SELECT 1 FROM execution_gates g WHERE g.execution_id = e.id))",
        placeholders(n)
    ))
}

/// The census of each execution in `execution_ids` that has one. Two queries
/// per [`IN_LIST_CHUNK`] executions, however many a list shows.
pub async fn census_for_executions_conn(
    conn: &Connection,
    execution_ids: &[String],
) -> DbResult<HashMap<String, ExecutionGateCensus>> {
    let mut out = HashMap::new();
    for chunk in execution_ids.chunks(IN_LIST_CHUNK) {
        let rows = census_rows(conn, &executions_census_sql(chunk.len()), bound_ids(chunk)).await?;
        out.extend(
            render_rows(conn, rows)
                .await?
                .into_iter()
                .map(|(row, census)| (row.id, census)),
        );
    }
    Ok(out)
}

/// The census statement for gated, unreleased executions selected by `filter`,
/// a SQL predicate over `e`.
fn gated_unreleased_rows_sql(filter: &str) -> String {
    census_rows_sql(&format!(
        "{filter} AND e.issue_id IS NOT NULL AND {}",
        cairn_db::gated_unreleased_sql!()
    ))
}

/// The gated, unreleased executions on `n` issues.
fn issues_gated_sql(n: usize) -> String {
    gated_unreleased_rows_sql(&format!("e.issue_id IN ({})", placeholders(n)))
}

/// Gated, unreleased executions selected by `sql` (built by
/// [`gated_unreleased_rows_sql`]) over `bound`, in issue then execution order,
/// paired with their issue id. Both kinds come back — those waiting to start
/// (`census.held`) and those that never will (`census.wont_start`) — for the
/// caller to list apart.
///
/// This reads facts rather than `status = 'held'`: the status projection
/// derives `held` for both kinds (see `cairn_workflow::transitions::outcome`),
/// and only the shared predicate tells them apart.
async fn gated_unreleased_rows(
    conn: &Connection,
    sql: &str,
    bound: Vec<SqlValue>,
) -> DbResult<Vec<(String, HeldExecution)>> {
    let rows = census_rows(conn, sql, bound).await?;
    Ok(render_rows(conn, rows)
        .await?
        .into_iter()
        .filter_map(|(row, census)| {
            let issue_id = row.issue_id?;
            Some((
                issue_id,
                HeldExecution {
                    execution_id: row.id,
                    seq: row.seq,
                    line: census_line(row.seq, &census),
                    census,
                },
            ))
        })
        .collect())
}

/// One issue's gated, unreleased executions, split by whether they can still
/// start. Every surface lists the two apart: only `held` is waiting.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IssueGatedExecutions {
    /// Waiting to start: the sweep releases these when their gates are met.
    pub held: Vec<HeldExecution>,
    /// Gated and unreleased, but superseded or on a resolved issue: they will
    /// never start, whatever their gates do.
    pub wont_start: Vec<HeldExecution>,
}

impl IssueGatedExecutions {
    fn push(&mut self, execution: HeldExecution) {
        if execution.census.held {
            self.held.push(execution);
        } else {
            self.wont_start.push(execution);
        }
    }
}

/// Every gated, unreleased execution on each of `issue_ids`, grouped by issue
/// and split into waiting and won't-start.
pub async fn gated_executions_for_issues_conn(
    conn: &Connection,
    issue_ids: &[String],
) -> DbResult<HashMap<String, IssueGatedExecutions>> {
    let mut grouped = HashMap::<String, IssueGatedExecutions>::new();
    // Each issue lands in exactly one chunk, so its executions keep their seq
    // order across the split.
    for chunk in issue_ids.chunks(IN_LIST_CHUNK) {
        let gated =
            gated_unreleased_rows(conn, &issues_gated_sql(chunk.len()), bound_ids(chunk)).await?;
        for (issue_id, execution) in gated {
            grouped.entry(issue_id).or_default().push(execution);
        }
    }
    Ok(grouped)
}

/// A gated, unreleased execution on an open issue in one project, for a
/// project census. `execution.census.held` says whether it is waiting.
#[derive(Debug, Clone)]
pub struct ProjectHeldExecution {
    pub issue_number: i64,
    pub issue_title: String,
    pub execution: HeldExecution,
}

/// Every gated, unreleased execution on an unresolved issue in `project_id`,
/// waiting and won't-start alike, ordered by issue number then execution seq —
/// the order a release burst would start the waiting ones in. A merged or
/// closed issue's are left out: the issue's own resolution already ended that
/// work, and the issue's own surfaces say so.
pub async fn held_executions_for_project_conn(
    conn: &Connection,
    project_id: &str,
) -> DbResult<Vec<ProjectHeldExecution>> {
    let held = gated_unreleased_rows(
        conn,
        &gated_unreleased_rows_sql(
            "e.issue_id IN (SELECT id FROM issues
                             WHERE project_id = ?1 AND status NOT IN ('merged', 'closed'))",
        ),
        vec![SqlValue::Text(project_id.to_string())],
    )
    .await?;
    if held.is_empty() {
        return Ok(Vec::new());
    }
    let mut issue_ids: Vec<String> = held.iter().map(|(issue_id, _)| issue_id.clone()).collect();
    issue_ids.sort();
    issue_ids.dedup();
    let mut issues = HashMap::<String, (i64, String)>::new();
    for chunk in issue_ids.chunks(IN_LIST_CHUNK) {
        let sql = format!(
            "SELECT id, number, title FROM issues WHERE id IN ({})",
            placeholders(chunk.len())
        );
        let mut rows = conn.query(&sql, bound_ids(chunk)).await?;
        while let Some(row) = rows.next().await? {
            issues.insert(row.text(0)?, (row.i64(1)?, row.text(2)?));
        }
    }
    let mut out: Vec<ProjectHeldExecution> = held
        .into_iter()
        .filter_map(|(issue_id, execution)| {
            let (issue_number, issue_title) = issues.get(&issue_id)?.clone();
            Some(ProjectHeldExecution {
                issue_number,
                issue_title,
                execution,
            })
        })
        .collect();
    out.sort_by_key(|held| (held.issue_number, held.execution.seq));
    Ok(out)
}

/// Seed gate rows directly, through the same store helpers the sweep uses, so
/// census tests exercise exactly the rows a real sweep would leave behind.
#[cfg(test)]
pub(crate) mod test_support {
    use cairn_workflow::gates::store::{insert_gates_conn, record_outcome_conn};

    use crate::storage::LocalDb;

    /// Renders as `check rust-tests passed on cairn://p/cairn/4724`.
    pub const CHECK_GATE: &str = r#"{"fact":"check_verdict","check":"rust-tests","issue":"cairn://p/cairn/4724","verdict":"passed"}"#;
    /// Renders as `check lint passed on cairn://p/cairn/4724`.
    pub const LINT_GATE: &str = r#"{"fact":"check_verdict","check":"lint","issue":"cairn://p/cairn/4724","verdict":"passed"}"#;

    /// The census both test gates produce: one quiet, one answered.
    pub const HELD_SUMMARY: &str = "held, waiting on check rust-tests passed on cairn://p/cairn/4724 (no verdict yet); check lint passed on cairn://p/cairn/4724 (evidence says no)";

    /// Gate an existing execution with `gates` — `(predicate, last_outcome)`,
    /// where `None` leaves the gate as no sweep has touched it.
    pub async fn gate_execution(db: &LocalDb, execution_id: &str, gates: &[(&str, Option<&str>)]) {
        let execution_id = execution_id.to_string();
        let gates: Vec<(String, Option<String>)> = gates
            .iter()
            .map(|(predicate, outcome)| (predicate.to_string(), outcome.map(str::to_string)))
            .collect();
        db.write(move |conn| {
            let execution_id = execution_id.clone();
            let gates = gates.clone();
            Box::pin(async move {
                let predicates: Vec<String> = gates.iter().map(|(p, _)| p.clone()).collect();
                let ids = insert_gates_conn(conn, &execution_id, &predicates, 1).await?;
                for (id, (_, outcome)) in ids.iter().zip(&gates) {
                    if let Some(outcome) = outcome {
                        record_outcome_conn(conn, id, outcome, None).await?;
                    }
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    /// The pair of gates that renders [`HELD_SUMMARY`].
    pub async fn hold_execution(db: &LocalDb, execution_id: &str) {
        gate_execution(
            db,
            execution_id,
            &[
                (CHECK_GATE, Some("no_evidence")),
                (LINT_GATE, Some("refuted")),
            ],
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{hold_execution, HELD_SUMMARY};
    use super::*;
    use crate::storage::LocalDb;

    async fn issue_with_two_executions() -> LocalDb {
        let db = crate::storage::migrated_test_db("gate-census.db").await;
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p', 'default', 'P', 'cairn', '/tmp/p', 1, 1);
             INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
               VALUES('i', 'p', 7, 'Gated', 'complete', 'complete', 'none', 1, 1);
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, lifecycle)
               VALUES('e-done', 'r', 'i', 'p', 'complete', 1, 1, 'superseded');
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES('e-held', 'r', 'i', 'p', 'held', 2, 2);",
        )
        .await
        .unwrap();
        hold_execution(&db, "e-held").await;
        db
    }

    /// A held execution beside a completed one on the same issue: only the
    /// held one is listed as held, and only it carries a census.
    #[tokio::test]
    async fn a_held_execution_beside_a_completed_one_is_the_only_one_held() {
        let db = issue_with_two_executions().await;
        let (held, census) = db
            .read(|conn| {
                Box::pin(async move {
                    let held = gated_executions_for_issues_conn(conn, &["i".to_string()]).await?;
                    let census = census_for_executions_conn(
                        conn,
                        &["e-done".to_string(), "e-held".to_string()],
                    )
                    .await?;
                    Ok((held, census))
                })
            })
            .await
            .unwrap();

        let held = &held["i"].held;
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].execution_id, "e-held");
        assert_eq!(held[0].seq, Some(2));
        assert_eq!(held[0].census.summary, HELD_SUMMARY);
        assert!(
            !census.contains_key("e-done"),
            "an ungated execution has no census"
        );
        assert_eq!(census["e-held"].summary, HELD_SUMMARY);
    }

    /// The plan of `sql` with `n` bound ids, one node per line.
    async fn query_plan(db: &LocalDb, sql: String, n: usize) -> Vec<String> {
        db.read(move |conn| {
            let sql = format!("EXPLAIN QUERY PLAN {sql}");
            Box::pin(async move {
                let bound = vec![SqlValue::Text("x".into()); n];
                let mut rows = conn.query(&sql, bound).await?;
                let mut plan = Vec::new();
                while let Some(row) = rows.next().await? {
                    plan.push(row.text(3)?);
                }
                Ok(plan)
            })
        })
        .await
        .unwrap()
    }

    /// The issue route hydrates the issue's gated executions and every listed
    /// execution's census through these two statements. With the ids bound as
    /// one `json_each` array the planner could not seek and scanned the whole
    /// executions table: 5.4 s and 2.7 s on a production database of 5.5k
    /// executions (cairn/5206). Every table they reach, the gate EXISTS
    /// probes and the supersession subquery included, must be a seek.
    #[tokio::test]
    async fn census_statements_never_scan() {
        let db = crate::storage::migrated_test_db("gate-census-plan.db").await;
        for (label, sql, driving_index) in [
            ("issues", issues_gated_sql(3), "idx_executions_issue_seq"),
            ("executions", executions_census_sql(3), "(id=?)"),
        ] {
            let plan = query_plan(&db, sql, 3).await;
            assert!(
                plan.iter()
                    .any(|line| line.starts_with("SEARCH e ") && line.contains(driving_index)),
                "{label}: executions are reached by {driving_index}: {plan:#?}"
            );
            assert!(
                plan.iter()
                    .any(|line| line.contains("idx_execution_gates_execution")),
                "{label}: the gate probes seek by execution: {plan:#?}"
            );
            assert!(
                !plan.iter().any(|line| line.starts_with("SCAN")),
                "{label}: no table in the census is scanned: {plan:#?}"
            );
        }
    }

    /// More issues than one bound list carries: the held execution in the
    /// second chunk is still found and grouped under its issue.
    #[tokio::test]
    async fn gated_executions_span_chunks() {
        let db = issue_with_two_executions().await;
        let mut ids: Vec<String> = (0..IN_LIST_CHUNK).map(|n| format!("absent-{n}")).collect();
        ids.push("i".to_string());
        let gated = db
            .read(move |conn| {
                let ids = ids.clone();
                Box::pin(async move { gated_executions_for_issues_conn(conn, &ids).await })
            })
            .await
            .unwrap();
        assert_eq!(gated.len(), 1);
        assert_eq!(gated["i"].held[0].execution_id, "e-held");
    }

    /// Every surface reads its gated executions through these two: the issue
    /// listings and the per-execution census.
    async fn listings(
        db: &LocalDb,
    ) -> (IssueGatedExecutions, HashMap<String, ExecutionGateCensus>) {
        db.read(|conn| {
            Box::pin(async move {
                let mut gated = gated_executions_for_issues_conn(conn, &["i".to_string()]).await?;
                let census = census_for_executions_conn(conn, &["e-held".to_string()]).await?;
                Ok((gated.remove("i").unwrap_or_default(), census))
            })
        })
        .await
        .unwrap()
    }

    /// The defect: a gated row a later launch fenced is still gated and
    /// unreleased, but the sweep will never release it. It must read as won't
    /// start — naming its successor — and leave the held listing.
    #[tokio::test]
    async fn a_superseded_gated_execution_wont_start_and_is_not_held() {
        let db = issue_with_two_executions().await;
        db.execute_script(
            "UPDATE executions SET lifecycle = 'superseded' WHERE id = 'e-held';
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES('e-next', 'r', 'i', 'p', 'running', 3, 3);
             INSERT INTO execution_supersessions(id, issue_id, superseded_execution_id,
                 successor_execution_id, reason, actor_kind, created_at)
               VALUES('s', 'i', 'e-held', 'e-next', 'successor_created', 'system', 3);",
        )
        .await
        .unwrap();

        let (gated, census) = listings(&db).await;
        assert!(gated.held.is_empty(), "a superseded row is not waiting");
        assert_eq!(gated.wont_start.len(), 1);
        assert_eq!(
            gated.wont_start[0].line,
            "exec 2 — won't start: superseded by exec 3"
        );
        let census = &census["e-held"];
        assert!(!census.held);
        assert_eq!(census.wont_start.as_deref(), Some("superseded by exec 3"));
        assert_eq!(census.summary, "won't start: superseded by exec 3");
        assert_eq!(census.gates.len(), 2, "its gates are still listed");
    }

    /// The other half: a gated row on an issue that has since merged or closed.
    #[tokio::test]
    async fn a_gated_execution_on_a_resolved_issue_wont_start() {
        for (column, resolution) in [("merged_at", "merged"), ("closed_at", "closed")] {
            let db = issue_with_two_executions().await;
            db.execute(
                &format!("UPDATE issues SET status = '{resolution}', {column} = 5 WHERE id = 'i'"),
                (),
            )
            .await
            .unwrap();

            let (gated, census) = listings(&db).await;
            let expected = format!("won't start: cairn/7 {resolution}");
            assert!(gated.held.is_empty(), "{resolution}: not waiting");
            assert_eq!(
                gated.wont_start[0].line,
                format!("exec 2 — {expected}"),
                "{resolution}"
            );
            assert!(!census["e-held"].held);
            assert_eq!(census["e-held"].summary, expected);
        }
    }

    /// A held next attempt is filed `pending`, not superseded: it is exactly
    /// what the sweep will release, so it stays waiting.
    #[tokio::test]
    async fn a_pending_gated_execution_is_still_waiting() {
        let db = issue_with_two_executions().await;
        db.execute(
            "UPDATE executions SET lifecycle = 'pending' WHERE id = 'e-held'",
            (),
        )
        .await
        .unwrap();

        let (gated, census) = listings(&db).await;
        assert!(gated.wont_start.is_empty());
        assert_eq!(gated.held.len(), 1);
        assert_eq!(gated.held[0].census.summary, HELD_SUMMARY);
        assert!(census["e-held"].held);
        assert_eq!(census["e-held"].wont_start, None);
    }

    /// The census and the sweep share one definition: every execution the
    /// census calls held is one the sweep would try to release, and nothing it
    /// calls won't-start is.
    #[tokio::test]
    async fn the_census_and_the_sweep_agree_on_what_is_waiting() {
        let db = issue_with_two_executions().await;
        let releasable = || async {
            db.read(|conn| {
                Box::pin(async move {
                    cairn_workflow::gates::store::list_held_executions_conn(conn).await
                })
            })
            .await
            .unwrap()
            .into_iter()
            .map(|held| held.execution_id)
            .collect::<Vec<_>>()
        };
        assert_eq!(releasable().await, vec!["e-held".to_string()]);
        assert_eq!(listings(&db).await.0.held.len(), 1);

        db.execute(
            "UPDATE executions SET lifecycle = 'superseded' WHERE id = 'e-held'",
            (),
        )
        .await
        .unwrap();
        assert!(releasable().await.is_empty());
        let (gated, _) = listings(&db).await;
        assert!(gated.held.is_empty());
        assert_eq!(
            gated.wont_start[0].census.summary, "won't start: superseded",
            "no audit row names a successor, so none is claimed"
        );
    }

    /// Releasing the execution takes it out of every held listing, and its
    /// census then says how it started.
    #[tokio::test]
    async fn a_released_execution_is_no_longer_held() {
        let db = issue_with_two_executions().await;
        db.execute(
            "UPDATE executions SET released_at = 9, release_override_json = '{\"principal\":\"operator\",\"at\":9}' WHERE id = 'e-held'",
            (),
        )
        .await
        .unwrap();
        let (held, census) = db
            .read(|conn| {
                Box::pin(async move {
                    Ok((
                        gated_executions_for_issues_conn(conn, &["i".to_string()]).await?,
                        census_for_executions_conn(conn, &["e-held".to_string()]).await?,
                    ))
                })
            })
            .await
            .unwrap();
        assert!(held.is_empty());
        assert!(!census["e-held"].held);
        assert!(census["e-held"]
            .summary
            .starts_with("released early by operator (override)"));
    }

    fn gate(predicate: &str) -> ExecutionGate {
        ExecutionGate {
            id: "g".into(),
            execution_id: "e".into(),
            seq: 0,
            predicate_json: predicate.into(),
            satisfied_at: None,
            last_outcome: None,
            foreclosed_at: None,
            foreclosed_reason: None,
            created_at: 1,
        }
    }

    const CHECK: &str = r#"{"fact":"check_verdict","check":"rust-tests","issue":"cairn://p/cairn/4724","verdict":"passed"}"#;
    const MERGED: &str =
        r#"{"fact":"issue_resolution","issue":"cairn://p/cairn/4724","resolution":"merged"}"#;

    #[test]
    fn an_unresolved_gate_reads_no_verdict_yet() {
        let census =
            census(&[gate(CHECK)], None, None, None).expect("a gated execution has a census");
        assert!(census.held);
        assert_eq!(
            census.summary,
            "held, waiting on check rust-tests passed on cairn://p/cairn/4724 (no verdict yet)"
        );
        assert_eq!(
            census_line(Some(1), &census),
            "exec 1 — held, waiting on check rust-tests passed on cairn://p/cairn/4724 (no verdict yet)"
        );
    }

    #[test]
    fn each_evidence_state_has_its_own_phrase() {
        let no_evidence = ExecutionGate {
            last_outcome: Some("no_evidence".into()),
            ..gate(CHECK)
        };
        let refuted = ExecutionGate {
            last_outcome: Some("refuted".into()),
            ..gate(CHECK)
        };
        let foreclosed = ExecutionGate {
            last_outcome: Some("refuted".into()),
            foreclosed_at: Some(5),
            foreclosed_reason: Some("cairn/4724 closed without merging".into()),
            ..gate(MERGED)
        };
        let satisfied = ExecutionGate {
            satisfied_at: Some(5),
            last_outcome: Some("refuted".into()),
            ..gate(MERGED)
        };
        let rendered: Vec<(GateEvidenceState, String)> =
            [&no_evidence, &refuted, &foreclosed, &satisfied]
                .into_iter()
                .map(|gate| {
                    let entry = render_gate(vocabulary(), gate);
                    (entry.state, entry.evidence)
                })
                .collect();
        assert_eq!(
            rendered,
            vec![
                (GateEvidenceState::NoEvidence, "no verdict yet".to_string()),
                (GateEvidenceState::Refuted, "evidence says no".to_string()),
                (
                    GateEvidenceState::Foreclosed,
                    "can never be satisfied: cairn/4724 closed without merging".to_string()
                ),
                (GateEvidenceState::Satisfied, "satisfied".to_string()),
            ],
            "foreclosure outranks the last resolution; a latched gate stays met"
        );
    }

    #[test]
    fn met_gates_drop_out_of_the_waiting_list() {
        let met = ExecutionGate {
            id: "g0".into(),
            satisfied_at: Some(3),
            ..gate(MERGED)
        };
        let open = ExecutionGate {
            id: "g1".into(),
            seq: 1,
            last_outcome: Some("refuted".into()),
            ..gate(CHECK)
        };
        let census = census(&[met, open], None, None, None).unwrap();
        assert_eq!(census.gates.len(), 2, "the census still lists every gate");
        assert_eq!(
            census.summary,
            "held, waiting on check rust-tests passed on cairn://p/cairn/4724 (evidence says no)"
        );
    }

    #[test]
    fn a_manual_release_renders_as_an_override_with_its_principal() {
        let census = census(
            &[gate(CHECK)],
            Some(10),
            Some(r#"{"principal":{"kind":"agent","node":"cairn://p/cairn/4735/1/coordinator"},"at":10}"#),
            // A released execution started; no reason can unstart it.
            Some("superseded"),
        )
        .unwrap();
        assert_eq!(census.wont_start, None);
        assert!(!census.held);
        assert_eq!(
            census.release_override,
            Some(ReleaseOverride {
                principal: "cairn://p/cairn/4735/1/coordinator".into(),
                at: Some(10)
            })
        );
        assert_eq!(
            census.summary,
            "released early by cairn://p/cairn/4735/1/coordinator (override), not waiting on check rust-tests passed on cairn://p/cairn/4724 (no verdict yet)"
        );
    }

    #[test]
    fn an_ungated_execution_has_no_census() {
        assert_eq!(census(&[], None, None, None), None);
        assert_eq!(census(&[], Some(1), None, None), None);
    }

    #[test]
    fn an_unparseable_predicate_still_renders() {
        let entry = render_gate(vocabulary(), &gate("not json"));
        assert_eq!(entry.predicate, "not json");
        assert_eq!(entry.evidence, "no verdict yet");
    }
}
