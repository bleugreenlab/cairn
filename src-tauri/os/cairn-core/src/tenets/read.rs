//! The read projection: a stored tenet plus what it means.
//!
//! Derived fields are computed here rather than stored: the successor from the
//! other rows' `supersedes`, the gated executions from `execution_gates`, and
//! the labels a surface prints beside a verdict.

use std::collections::HashMap;

use cairn_common::identity::display::PrincipalAliases;
use cairn_common::identity::PrincipalRef;
use serde::Serialize;

use super::policy::{release_label, TenetRelease};
use super::{
    tenet_uri, AnswerSource, TenetOrigin, TenetRow, TenetStatus, Verdict, TABLE, TENET_GATE_FACT,
};
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

const COLUMNS: &str = "t.id, t.project_id, t.number, t.proposing_thread_id, t.slug, t.statement,
    t.status, t.verdict, t.comment, t.answered_by_json, t.answer_source, t.answered_at,
    t.supersedes, t.origin, t.legacy_json, t.contradiction_alerted_at,
    t.proposer_principal_json, t.created_at, t.updated_at";

/// A `SELECT` of every stored column from `tenets t` under `filter`.
pub(super) fn select_sql(filter: &str) -> String {
    format!("SELECT {COLUMNS} FROM {TABLE} t WHERE {filter}")
}

fn json_column<T: serde::de::DeserializeOwned>(
    value: Option<String>,
    column: &str,
) -> DbResult<Option<T>> {
    value
        .map(|text| {
            serde_json::from_str(&text)
                .map_err(|error| DbError::Row(format!("tenets.{column}: {error}")))
        })
        .transpose()
}

/// Decode one row selected with [`select_sql`].
pub(super) fn decode(row: &cairn_db::turso::Row) -> DbResult<TenetRow> {
    let status = row.text(6)?;
    let origin = row.text(13)?;
    let answer_source = row.opt_text(10)?;
    Ok(TenetRow {
        id: row.text(0)?,
        project_id: row.text(1)?,
        number: row.i64(2)?,
        proposing_thread_id: row.text(3)?,
        slug: row.opt_text(4)?,
        statement: row.text(5)?,
        status: TenetStatus::parse(&status)
            .ok_or_else(|| DbError::Row(format!("tenets.status: unknown {status}")))?,
        verdict: match row.opt_text(7)? {
            Some(verdict) => Some(
                Verdict::parse(&verdict)
                    .ok_or_else(|| DbError::Row(format!("tenets.verdict: unknown {verdict}")))?,
            ),
            None => None,
        },
        comment: row.opt_text(8)?,
        answered_by: json_column(row.opt_text(9)?, "answered_by_json")?,
        answer_source: match answer_source {
            Some(source) => {
                Some(AnswerSource::parse(&source).ok_or_else(|| {
                    DbError::Row(format!("tenets.answer_source: unknown {source}"))
                })?)
            }
            None => None,
        },
        answered_at: row.opt_i64(11)?,
        supersedes: row.opt_i64(12)?,
        origin: TenetOrigin::parse(&origin)
            .ok_or_else(|| DbError::Row(format!("tenets.origin: unknown {origin}")))?,
        legacy: json_column(row.opt_text(14)?, "legacy_json")?,
        contradiction_alerted_at: row.opt_i64(15)?,
        proposer: json_column(Some(row.text(16)?), "proposer_principal_json")?
            .ok_or_else(|| DbError::Row("tenets.proposer_principal_json: missing".into()))?,
        created_at: row.i64(17)?,
        updated_at: row.i64(18)?,
    })
}

/// The row for tenet `number` in `project_id`, read on the caller's connection.
pub(crate) async fn row_conn(
    conn: &cairn_db::turso::Connection,
    project_id: &str,
    number: i64,
) -> DbResult<Option<TenetRow>> {
    let mut rows = conn
        .query(
            &select_sql("t.project_id = ?1 AND t.number = ?2"),
            cairn_db::turso::params![project_id, number],
        )
        .await?;
    rows.next().await?.map(|row| decode(&row)).transpose()
}

/// What a tenet address finds in one database.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TenetLookup {
    /// The tenet's PROJECT has no row here: this host cannot see it, which is
    /// silence, never evidence that the tenet is gone.
    Unreachable,
    /// The project is here and the tenet is not: it never existed, or it was
    /// an unanswered proposal deleted with its thread.
    Missing,
    Found(Box<TenetRow>),
}

/// Look up tenet `number` of the project keyed `project_key`.
pub(crate) async fn lookup_conn(
    conn: &cairn_db::turso::Connection,
    project_key: &str,
    number: i64,
) -> DbResult<TenetLookup> {
    let mut rows = conn
        .query(
            "SELECT id FROM projects WHERE key = ?1 COLLATE NOCASE LIMIT 1",
            cairn_db::turso::params![cairn_common::uri::canonical_project(project_key)],
        )
        .await?;
    let Some(project_id) = rows.next().await?.map(|row| row.text(0)).transpose()? else {
        return Ok(TenetLookup::Unreachable);
    };
    drop(rows);
    Ok(match row_conn(conn, &project_id, number).await? {
        Some(row) => TenetLookup::Found(Box::new(row)),
        None => TenetLookup::Missing,
    })
}

/// An execution held (or once held) on a tenet's verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TenetGate {
    pub execution_id: String,
    /// The gated issue's canonical URI, when the execution belongs to one.
    pub issue: Option<String>,
    pub exec_seq: Option<i64>,
    /// This tenet's gate is met. The execution may still wait on its other
    /// gates.
    pub satisfied: bool,
    /// The execution has started: every gate it waited on is met, or it was
    /// released by hand.
    pub released: bool,
    /// Still waiting on THIS tenet: its gate is unmet and not foreclosed, and
    /// the execution has not released.
    pub held: bool,
    pub foreclosed: bool,
}

/// One automatic evidence judgement about a tenet. Agents never write these.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TenetEvidence {
    pub source_uri: String,
    /// `supports` or `contradicts`.
    pub sign: String,
    /// The probability of the chosen sign. Measured when `measured_by` is set;
    /// otherwise the judge reported it about itself.
    pub confidence: f64,
    /// `native` or `logprobs`, as the models facet's `decide` measured it.
    /// `None` for a row written before evidence was judged through `decide`.
    pub measured_by: Option<String>,
    pub evaluated_at: i64,
}

impl TenetEvidence {
    /// The confidence with how it was measured, as an expanded evidence line
    /// prints it: `0.82 logprobs`, or `0.82 self-reported` for a row that
    /// predates measurement.
    pub fn measurement(&self) -> String {
        format!(
            "{:.2} {}",
            self.confidence,
            self.measured_by.as_deref().unwrap_or("self-reported")
        )
    }
}

/// A tenet as every surface reads it.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TenetView {
    #[serde(flatten)]
    pub row: TenetRow,
    pub uri: String,
    pub project_key: String,
    /// `project/name` of the proposing thread, or its id when the thread row
    /// is gone.
    pub proposing_thread: String,
    /// The bare name of the proposing thread, when its row resolves.
    pub proposing_thread_name: Option<String>,
    /// The approved tenet that replaced this one.
    pub superseded_by: Option<i64>,
    /// Who answered, as it is always printed beside the verdict.
    pub answerer_label: Option<String>,
    /// What this project's tenet gates release on.
    pub policy_label: String,
    pub gates: Vec<TenetGate>,
    /// Automatic evidence judgements, newest first.
    pub evidence: Vec<TenetEvidence>,
    /// How many of the proposing thread's tenets the operator answered
    /// `your_call`, so a reader can calibrate how often the thread is simply
    /// trusted.
    pub thread_your_calls: i64,
}

/// How the answerer reads beside a verdict.
pub fn answerer_label(
    source: Option<AnswerSource>,
    principal: Option<&PrincipalRef>,
) -> Option<String> {
    let label = || PrincipalAliases::default().display(principal, None).label;
    match source? {
        AnswerSource::Principal => Some(format!("signed in as {}", label())),
        AnswerSource::Device => Some(format!("anonymous device {}", label())),
        AnswerSource::Migrated => Some("migrated ruling".to_string()),
    }
}

/// The statuses a surface's `status` name selects, and whether they read
/// oldest first. `pending` is the inbox, worked oldest first; `log` is
/// everything the operator has decided. Every surface spells the filter this
/// way, the resource's `?status=` and the Tenets tab alike.
pub fn status_filter(value: Option<&str>) -> Result<(Vec<TenetStatus>, bool), String> {
    match value {
        None => Ok((Vec::new(), false)),
        Some("pending") => Ok((vec![TenetStatus::Proposed], true)),
        Some("log") => Ok((
            vec![
                TenetStatus::Approved,
                TenetStatus::Declined,
                TenetStatus::Superseded,
            ],
            false,
        )),
        Some(other) => TenetStatus::parse(other)
            .map(|status| (vec![status], status == TenetStatus::Proposed))
            .ok_or_else(|| {
                format!(
                    "unknown tenet status `{other}`: expected proposed, approved, declined, withdrawn, superseded, pending, or log"
                )
            }),
    }
}

/// The id of the thread a surface's `thread` name selects in this project, or
/// the refusal for a name no thread of the project carries.
pub async fn thread_named(db: &LocalDb, project_id: &str, name: &str) -> Result<String, String> {
    let project = project_id.to_string();
    let wanted = name.to_string();
    db.read(|conn| {
        Box::pin(
            async move { crate::threads::thread_id_by_name_conn(conn, &project, &wanted).await },
        )
    })
    .await
    .map_err(|error| format!("Failed to resolve thread {name}: {error}"))?
    .ok_or_else(|| format!("no thread named `{name}` in this project"))
}

/// A narrowing of a project's tenets.
#[derive(Debug, Clone, Default)]
pub struct TenetFilter {
    /// Keep only these statuses. Empty keeps every status.
    pub statuses: Vec<TenetStatus>,
    pub thread_id: Option<String>,
    pub slug: Option<String>,
    /// Case-insensitive substring of the statement.
    pub search: Option<String>,
    /// Oldest first, the order an inbox is worked in. Newest first otherwise.
    pub oldest_first: bool,
    pub limit: Option<usize>,
}

struct ProjectFacts {
    key: String,
    release: TenetRelease,
}

async fn project_facts(db: &LocalDb, project_id: &str) -> Result<ProjectFacts, String> {
    db.query_opt(
        "SELECT key, tenet_release FROM projects WHERE id = ?1",
        (project_id.to_string(),),
        |row| Ok((row.text(0)?, row.text(1)?)),
    )
    .await
    .map_err(|error| format!("Failed to load project: {error}"))?
    .map(|(key, release)| ProjectFacts {
        key,
        release: TenetRelease::parse(&release).unwrap_or(TenetRelease::Answered),
    })
    .ok_or_else(|| format!("project not found: {project_id}"))
}

async fn load_rows(db: &LocalDb, filter: &str, key: String) -> Result<Vec<TenetRow>, String> {
    db.query_all(select_sql(filter), (key,), decode)
        .await
        .map_err(|error| format!("Failed to read tenets: {error}"))
}

/// Project the rows of ONE project into views.
async fn project_views(
    db: &LocalDb,
    project_id: &str,
    rows: Vec<TenetRow>,
) -> Result<Vec<TenetView>, String> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let facts = project_facts(db, project_id).await?;
    let pid = project_id.to_string();
    let threads: HashMap<String, String> = db
        .query_all(
            "SELECT id, name FROM threads WHERE project_id = ?1",
            (pid.clone(),),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .map_err(|error| format!("Failed to read threads: {error}"))?
        .into_iter()
        .collect();
    // Who replaced whom, and how often each thread was trusted, read across
    // the whole project rather than only the rows being projected.
    struct Link {
        number: i64,
        supersedes: Option<i64>,
        status: String,
        verdict: Option<String>,
        thread: String,
    }
    let links = db
        .query_all(
            "SELECT number, supersedes, status, verdict, proposing_thread_id
             FROM tenets WHERE project_id = ?1",
            (pid.clone(),),
            |row| {
                Ok(Link {
                    number: row.i64(0)?,
                    supersedes: row.opt_i64(1)?,
                    status: row.text(2)?,
                    verdict: row.opt_text(3)?,
                    thread: row.text(4)?,
                })
            },
        )
        .await
        .map_err(|error| format!("Failed to read tenets: {error}"))?;
    let mut superseded_by: HashMap<i64, i64> = HashMap::new();
    let mut your_calls: HashMap<String, i64> = HashMap::new();
    for link in &links {
        if let Some(target) = link.supersedes {
            if matches!(link.status.as_str(), "approved" | "superseded") {
                superseded_by.entry(target).or_insert(link.number);
            }
        }
        if link.verdict.as_deref() == Some("your_call") {
            *your_calls.entry(link.thread.clone()).or_default() += 1;
        }
    }
    let gates = gates_by_tenet(db, project_id).await?;
    let mut evidence = evidence_by_tenet(db, project_id).await?;
    let policy_label = release_label(facts.release);

    Ok(rows
        .into_iter()
        .map(|row| {
            let uri = tenet_uri(&facts.key, row.number);
            let name = threads.get(&row.proposing_thread_id).cloned();
            TenetView {
                uri: uri.clone(),
                project_key: facts.key.clone(),
                proposing_thread: match &name {
                    Some(name) => format!("{}/{name}", facts.key),
                    None => row.proposing_thread_id.clone(),
                },
                proposing_thread_name: name,
                superseded_by: superseded_by.get(&row.number).copied(),
                answerer_label: answerer_label(row.answer_source, row.answered_by.as_ref()),
                policy_label: policy_label.clone(),
                gates: gates.get(&uri).cloned().unwrap_or_default(),
                evidence: evidence.remove(&row.id).unwrap_or_default(),
                thread_your_calls: your_calls
                    .get(&row.proposing_thread_id)
                    .copied()
                    .unwrap_or(0),
                row,
            }
        })
        .collect())
}

/// Every evidence row in the project, keyed by tenet id, newest first.
async fn evidence_by_tenet(
    db: &LocalDb,
    project_id: &str,
) -> Result<HashMap<String, Vec<TenetEvidence>>, String> {
    let rows = db
        .query_all(
            "SELECT tenet_id, source_uri, sign, confidence, measured_by, evaluated_at
             FROM tenet_evidence WHERE project_id = ?1
             ORDER BY evaluated_at DESC, id",
            (project_id.to_string(),),
            |row| {
                Ok((
                    row.text(0)?,
                    TenetEvidence {
                        source_uri: row.text(1)?,
                        sign: row.text(2)?,
                        confidence: row.f64(3)?,
                        measured_by: row.opt_text(4)?,
                        evaluated_at: row.i64(5)?,
                    },
                ))
            },
        )
        .await
        .map_err(|error| format!("Failed to read tenet evidence: {error}"))?;
    let mut out: HashMap<String, Vec<TenetEvidence>> = HashMap::new();
    for (tenet, item) in rows {
        out.entry(tenet).or_default().push(item);
    }
    Ok(out)
}

/// Every execution gate in the project that names a tenet, keyed by the
/// tenet's canonical URI.
async fn gates_by_tenet(
    db: &LocalDb,
    project_id: &str,
) -> Result<HashMap<String, Vec<TenetGate>>, String> {
    let rows: Vec<(String, TenetGate)> = db
        .query_all(
            "SELECT json_extract(g.predicate_json, '$.tenet'), e.id, e.seq, e.released_at,
                    g.foreclosed_at, p.key, i.number, g.satisfied_at
             FROM execution_gates g
             JOIN executions e ON e.id = g.execution_id
             LEFT JOIN issues i ON i.id = e.issue_id
             LEFT JOIN projects p ON p.id = i.project_id
             WHERE (e.project_id = ?1 OR i.project_id = ?1)
               AND json_extract(g.predicate_json, '$.fact') = ?2
             ORDER BY e.started_at, g.seq",
            (project_id.to_string(), TENET_GATE_FACT.to_string()),
            |row| {
                let released = row.opt_i64(3)?.is_some();
                let foreclosed = row.opt_i64(4)?.is_some();
                let satisfied = row.opt_i64(7)?.is_some();
                let issue = row
                    .opt_text(5)?
                    .zip(row.opt_i64(6)?)
                    .map(|(key, number)| cairn_common::uri::build_issue_uri(&key, number as i32));
                Ok((
                    row.text(0)?,
                    TenetGate {
                        execution_id: row.text(1)?,
                        issue,
                        exec_seq: row.opt_i64(2)?,
                        satisfied,
                        released,
                        held: !satisfied && !foreclosed && !released,
                        foreclosed,
                    },
                ))
            },
        )
        .await
        .map_err(|error| format!("Failed to read tenet gates: {error}"))?;
    let mut out: HashMap<String, Vec<TenetGate>> = HashMap::new();
    for (tenet, gate) in rows {
        let tenet = match cairn_common::uri::parse_uri(&tenet) {
            Some(resource) => resource.to_uri(),
            None => tenet,
        };
        out.entry(tenet).or_default().push(gate);
    }
    Ok(out)
}

/// Resolve an address key, a number or a migrated ruling's slug, to the
/// tenet's canonical number.
pub async fn resolve_number(
    db: &LocalDb,
    project_id: &str,
    key: &str,
) -> Result<Option<i64>, String> {
    if let Ok(number) = key.parse::<i64>() {
        return Ok((number > 0).then_some(number));
    }
    db.query_opt(
        "SELECT number FROM tenets WHERE project_id = ?1 AND slug = ?2",
        (project_id.to_string(), key.to_string()),
        |row| row.i64(0),
    )
    .await
    .map_err(|error| format!("Failed to resolve tenet {key}: {error}"))
}

/// One tenet, by number.
pub async fn get(db: &LocalDb, project_id: &str, number: i64) -> Result<Option<TenetView>, String> {
    let rows = db
        .query_all(
            select_sql("t.project_id = ?1 AND t.number = ?2"),
            (project_id.to_string(), number),
            decode,
        )
        .await
        .map_err(|error| format!("Failed to read tenet: {error}"))?;
    Ok(project_views(db, project_id, rows)
        .await?
        .into_iter()
        .next())
}

/// A project's tenets under `filter`.
pub async fn list(
    db: &LocalDb,
    project_id: &str,
    filter: &TenetFilter,
) -> Result<Vec<TenetView>, String> {
    let mut rows = load_rows(db, "t.project_id = ?1", project_id.to_string()).await?;
    let search = filter.search.as_deref().map(str::to_lowercase);
    rows.retain(|row| {
        (filter.statuses.is_empty() || filter.statuses.contains(&row.status))
            && filter
                .thread_id
                .as_deref()
                .is_none_or(|thread| row.proposing_thread_id == thread)
            && filter
                .slug
                .as_deref()
                .is_none_or(|slug| row.slug.as_deref() == Some(slug))
            && search
                .as_deref()
                .is_none_or(|needle| row.statement.to_lowercase().contains(needle))
    });
    if filter.oldest_first {
        rows.sort_by_key(|row| row.number);
    } else {
        rows.sort_by_key(|row| std::cmp::Reverse(row.number));
    }
    if let Some(limit) = filter.limit {
        rows.truncate(limit);
    }
    project_views(db, project_id, rows).await
}

/// Every tenet a thread proposed, oldest first.
pub async fn for_thread(db: &LocalDb, thread_id: &str) -> Result<Vec<TenetView>, String> {
    let mut rows = load_rows(db, "t.proposing_thread_id = ?1", thread_id.to_string()).await?;
    rows.sort_by_key(|row| row.number);
    let Some(project_id) = rows.first().map(|row| row.project_id.clone()) else {
        return Ok(Vec::new());
    };
    project_views(db, &project_id, rows).await
}

/// How many of a project's proposals await the operator's verdict.
pub async fn pending_count(db: &LocalDb, project_id: &str) -> Result<i64, String> {
    db.query_one(
        "SELECT COUNT(*) FROM tenets WHERE project_id = ?1 AND status = 'proposed'",
        (project_id.to_string(),),
        |row| row.i64(0),
    )
    .await
    .map_err(|error| format!("Failed to count tenets: {error}"))
}
