//! Labels for resolved issues that were filed before the forecaster existed,
//! so a refit has a population to fit on.
//!
//! Each pass labels at most [`Budget::calls`] issues, oldest first, under a
//! daily spend cap read back from the labels themselves, so a restart cannot
//! reset it. An issue is labelled from what was known at filing:
//!
//! - **Filing maker**: the maker selected for the agent node the issue's own
//!   trigger starts in its first execution's recipe graph (ported from
//!   `extract.py::filing_job`), else the earliest launched job's.
//! - **Filing text**: the creation call's own payload, read through the event
//!   history seam so archived runs answer too, and tied to the issue by the
//!   number its tool result returned or, failing that, by an exact title in
//!   the call's window (ported from `extract.py::creation_texts`). Without a
//!   tie, the issue's current text, recorded as such.
//!
//! An issue not filed with an Anthropic model, or whose text would not scrub,
//! is recorded as skipped and never reconsidered for the same versions.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use cairn_db::turso::params;
use regex::Regex;
use serde_json::Value;

use super::applicability::{maker, ANTHROPIC};
use super::fit::FLOOR;
use super::question::{self, sha256_hex};
use super::store::{self, LabelRecord, LabelSource, LabelStatus, TextSource};
use super::{AnswerError, AnswerInput, ScoreAnswer, ScoreAnswerer};
use crate::storage::events::history::{EventFilter, HistoryReader};
use crate::storage::{DbResult, LocalDb, RowExt};

/// How much one pass may do.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Budget {
    /// Model calls.
    pub calls: usize,
    /// Unlabelled issues looked at (each costs a few reads).
    pub examined: usize,
    /// Dollars the backfill may spend in any 24 hours.
    pub daily_cap_usd: f64,
    /// What each call is charged until its reply reports a cost, and for
    /// good when none does.
    pub reserve_usd: f64,
    pub call_timeout: Duration,
}

impl Default for Budget {
    /// About 7k issues at cairn/6271's $0.000064 a call is under $0.50, so the
    /// cap only matters if the price moves. The reservation is about 30 times
    /// the measured price, so an uncosted call is never under-counted.
    fn default() -> Self {
        Self {
            calls: 50,
            examined: 500,
            daily_cap_usd: 1.0,
            reserve_usd: 0.002,
            call_timeout: Duration::from_secs(60),
        }
    }
}

/// What one pass did.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BackfillReport {
    pub examined: usize,
    pub labelled: usize,
    pub skipped: usize,
    pub refused: usize,
    pub failed: usize,
    /// Why the pass stopped before its budget, when it did.
    pub stopped: Option<String>,
}

/// A creation call may run this long before the issue row it creates.
const CREATE_LOOKBACK_SECS: i64 = 600;
/// How many events after a creation call its tool result is looked for in.
const RESULT_SCAN: i64 = 400;

static CREATE_TARGET: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)^cairn:(?://p/([^/]+)|@)/issues/?$").expect("valid regex"));
static CREATED_SUMMARY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"Created issue ([A-Za-z0-9_.-]+)/(\d+)").expect("valid regex"));
static ISSUE_URI: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^cairn://p/([^/]+)/(\d+)$").expect("valid regex"));

/// An issue the backfill may label.
#[derive(Debug, Clone, PartialEq)]
pub struct Candidate {
    pub issue_id: String,
    pub project_key: Option<String>,
    pub number: i64,
    pub title: String,
    pub description: String,
    pub created_at: i64,
}

/// Resolved issues created on or after `floor` with an execution, oldest
/// first.
pub async fn resolved_issue_ids(db: &LocalDb, floor: i64) -> DbResult<Vec<String>> {
    db.query_all(
        "SELECT i.id FROM issues i
         WHERE i.created_at >= ?1 AND i.status IN ('merged', 'closed')
           AND EXISTS (SELECT 1 FROM executions e WHERE e.issue_id = i.id)
         ORDER BY i.created_at, i.id",
        params![floor],
        |row| row.text(0),
    )
    .await
}

pub async fn candidate(db: &LocalDb, issue_id: &str) -> DbResult<Option<Candidate>> {
    db.query_opt(
        "SELECT i.id, p.key, i.number, i.title, i.description, i.created_at
         FROM issues i LEFT JOIN projects p ON p.id = i.project_id
         WHERE i.id = ?1",
        params![issue_id],
        |row| {
            Ok(Candidate {
                issue_id: row.text(0)?,
                project_key: row.opt_text(1)?,
                number: row.i64(2)?,
                title: row.text(3)?,
                description: row.opt_text(4)?.unwrap_or_default(),
                created_at: row.i64(5)?,
            })
        },
    )
    .await
}

/// A top-level agent job of an issue's first execution, with what its launch
/// selected.
#[derive(Debug, Clone, PartialEq)]
pub struct FirstJob {
    pub id: String,
    pub recipe_node_id: Option<String>,
    pub created_at: i64,
    pub started_at: Option<i64>,
    pub model: Option<String>,
    pub selection_maker: Option<String>,
    pub selection_model: Option<String>,
}

/// The job of the agent node the issue's trigger starts, and how it was
/// found.
///
/// Roots are the agent nodes reached from the recipe's trigger along control
/// edges without passing another agent (delegated-task triggers, added to
/// the snapshot later, are not the issue's trigger). Several roots run in
/// parallel; the earliest launched is taken. Without a graph root, the
/// earliest launched job stands in. Jobs of one execution are created in the
/// same second with random ids, so neither says which ran first.
pub fn filing_job<'a>(
    nodes: Option<&Value>,
    edges: Option<&Value>,
    jobs: &'a [FirstJob],
) -> Option<(&'a FirstJob, &'static str)> {
    let as_array =
        |value: Option<&Value>| value.and_then(Value::as_array).cloned().unwrap_or_default();
    let kinds: HashMap<String, String> = as_array(nodes)
        .iter()
        .filter_map(|node| {
            Some((
                node.get("id")?.as_str()?.to_string(),
                node.get("nodeType")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            ))
        })
        .collect();
    let mut control: HashMap<String, Vec<String>> = HashMap::new();
    for edge in as_array(edges) {
        if edge.get("edgeType").and_then(Value::as_str) != Some("control") {
            continue;
        }
        let (Some(source), Some(target)) = (
            edge.get("sourceNodeId").and_then(Value::as_str),
            edge.get("targetNodeId").and_then(Value::as_str),
        ) else {
            continue;
        };
        control
            .entry(source.to_string())
            .or_default()
            .push(target.to_string());
    }
    let mut frontier: Vec<String> = kinds
        .iter()
        .filter(|(id, kind)| kind.as_str() == "trigger" && !id.starts_with("delegated-"))
        .map(|(id, _)| id.clone())
        .collect();
    let mut seen: std::collections::HashSet<String> = frontier.iter().cloned().collect();
    let mut roots = std::collections::HashSet::new();
    while let Some(node) = frontier.pop() {
        for target in control.get(&node).into_iter().flatten() {
            if !seen.insert(target.clone()) {
                continue;
            }
            if kinds.get(target).map(String::as_str) == Some("agent") {
                roots.insert(target.clone());
            } else {
                frontier.push(target.clone());
            }
        }
    }
    let launch = |job: &&FirstJob| {
        (
            job.started_at.unwrap_or(i64::MAX),
            job.created_at,
            job.id.clone(),
        )
    };
    let rooted = jobs
        .iter()
        .filter(|job| {
            job.recipe_node_id
                .as_ref()
                .is_some_and(|id| roots.contains(id))
        })
        .min_by_key(launch);
    match rooted {
        Some(job) => Some((job, "graph")),
        None => jobs
            .iter()
            .min_by_key(launch)
            .map(|job| (job, "launch_order")),
    }
}

/// The maker selected for an issue's filing-time agent, and how the agent
/// was found; `None` when its first execution ran no job.
pub async fn filing_selection(
    db: &LocalDb,
    issue_id: &str,
) -> DbResult<Option<(String, &'static str)>> {
    let executions = db
        .query_all(
            "SELECT id, started_at, coalesce(seq, 0) FROM executions WHERE issue_id = ?1",
            params![issue_id],
            |row| Ok((row.opt_i64(1)?, row.i64(2)?, row.text(0)?)),
        )
        .await?;
    // The first execution by start (unstarted first, as SQL orders NULL),
    // then sequence, then id.
    let Some((_, _, execution_id)) = executions.into_iter().min() else {
        return Ok(None);
    };
    let graph = db
        .query_opt(
            "SELECT CASE WHEN json_valid(snapshot) THEN json_extract(snapshot, '$.recipe.nodes') END,
                    CASE WHEN json_valid(snapshot) THEN json_extract(snapshot, '$.recipe.edges') END
             FROM executions WHERE id = ?1",
            params![execution_id.as_str()],
            |row| Ok((row.opt_text(0)?, row.opt_text(1)?)),
        )
        .await?
        .unwrap_or_default();
    let parse =
        |text: Option<String>| text.and_then(|text| serde_json::from_str::<Value>(&text).ok());
    let (nodes, edges) = (parse(graph.0), parse(graph.1));
    let jobs = db
        .query_all(
            "SELECT id, recipe_node_id, created_at, started_at, model,
                    CASE WHEN json_valid(selection) THEN json_extract(selection, '$.maker') END,
                    CASE WHEN json_valid(selection) THEN json_extract(selection, '$.chosen.model') END
             FROM jobs
             WHERE execution_id = ?1 AND parent_job_id IS NULL AND parent_tool_use_id IS NULL",
            params![execution_id.as_str()],
            |row| {
                Ok(FirstJob {
                    id: row.text(0)?,
                    recipe_node_id: row.opt_text(1)?,
                    created_at: row.i64(2)?,
                    started_at: row.opt_i64(3)?,
                    model: row.opt_text(4)?,
                    selection_maker: row.opt_text(5)?,
                    selection_model: row.opt_text(6)?,
                })
            },
        )
        .await?;
    Ok(
        filing_job(nodes.as_ref(), edges.as_ref(), &jobs).map(|(job, method)| {
            let maker = maker(job.selection_maker.as_deref())
                .or_else(|| maker(job.selection_model.as_deref()))
                .or_else(|| maker(job.model.as_deref()))
                .unwrap_or_else(|| "unknown".to_string());
            (maker, method)
        }),
    )
}

/// An issue's filing text and where it came from.
#[derive(Debug, Clone, PartialEq)]
pub struct FilingText {
    pub title: String,
    pub description: String,
    pub source: TextSource,
    /// How an event text was tied to the issue.
    pub method: Option<&'static str>,
}

/// The write call's input: `toolInput`, or the matching entry of `toolUses`.
pub fn call_input<'a>(event: &'a Value, tool_use_id: Option<&str>) -> Option<&'a Value> {
    let uses: Vec<&Value> = event
        .get("toolUses")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|use_| use_.get("input").is_some_and(Value::is_object))
        .collect();
    if let Some(found) = uses
        .iter()
        .find(|use_| tool_use_id.is_some() && use_.get("id").and_then(Value::as_str) == tool_use_id)
    {
        return found.get("input");
    }
    if let Some(input) = event.get("toolInput").filter(|input| input.is_object()) {
        return Some(input);
    }
    match uses.as_slice() {
        [only] => only.get("input"),
        _ => None,
    }
}

/// Change index -> (title, description) of every issue creation in a write
/// call.
fn creations(call: &Value) -> Vec<(usize, String, String)> {
    let Some(changes) = call.get("changes").and_then(Value::as_array) else {
        return Vec::new();
    };
    changes
        .iter()
        .enumerate()
        .filter_map(|(index, change)| {
            let mode = change.get("mode").and_then(Value::as_str)?;
            if mode != "append" && mode != "create" {
                return None;
            }
            let target = change.get("target").and_then(Value::as_str)?;
            if !CREATE_TARGET.is_match(target) {
                return None;
            }
            let payload = change.get("payload")?.as_object()?;
            let title = match payload.get("title")? {
                Value::String(title) if !title.is_empty() => title.trim().to_string(),
                _ => return None,
            };
            let description = match payload.get("description") {
                Some(Value::String(text)) => text.clone(),
                _ => String::new(),
            };
            Some((index, title, description))
        })
        .collect()
}

/// Change index -> (project key, issue number) from a write reply.
pub fn created_by_result(result_text: &str) -> HashMap<usize, (String, i64)> {
    let mut out = HashMap::new();
    let reply = result_text.find('{').and_then(|start| {
        serde_json::Deserializer::from_str(&result_text[start..])
            .into_iter::<Value>()
            .next()
            .and_then(Result::ok)
    });
    let applied = reply
        .as_ref()
        .and_then(|reply| reply.get("applied"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for entry in applied {
        let Some(index) = entry.get("index").and_then(Value::as_u64) else {
            continue;
        };
        let uri = entry
            .get("data")
            .and_then(|data| data.get("uri"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let summary = entry.get("summary").and_then(Value::as_str).unwrap_or("");
        let found = ISSUE_URI
            .captures(uri)
            .or_else(|| CREATED_SUMMARY.captures(summary));
        if let Some(caps) = found {
            if let Ok(number) = caps[2].parse::<i64>() {
                out.insert(index as usize, (caps[1].to_lowercase(), number));
            }
        }
    }
    out
}

struct WriteCall {
    ts: i64,
    result_ts: Option<i64>,
    run_id: String,
    tool_use_id: Option<String>,
    event_id: String,
}

/// The creation call's text for `candidate`, when one can be decoded and
/// tied to it.
async fn creation_text(db: &LocalDb, candidate: &Candidate) -> Result<Option<FilingText>, String> {
    let calls = db
        .query_all(
            "SELECT t.ts, t.result_ts, t.run_id, t.tool_use_id, t.event_id
             FROM tool_invocations t
             WHERE t.ts >= ?1 AND t.ts <= ?2
               AND (t.verb = 'write' OR t.tool_name IN ('change', 'edit'))
             ORDER BY t.ts, t.id",
            params![
                candidate.created_at - CREATE_LOOKBACK_SECS,
                candidate.created_at + 5
            ],
            |row| {
                Ok(WriteCall {
                    ts: row.i64(0)?,
                    result_ts: row.opt_i64(1)?,
                    run_id: row.text(2)?,
                    tool_use_id: row.opt_text(3)?,
                    event_id: row.text(4)?,
                })
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    let key = candidate.project_key.as_deref().map(str::to_lowercase);
    let mut reader = HistoryReader::new(db);
    for call in calls {
        let rows = reader
            .run_events_by_id(&call.run_id, std::slice::from_ref(&call.event_id))
            .await
            .map_err(|error| error.to_string())?;
        let Some(row) = rows.into_iter().next() else {
            continue;
        };
        let Ok(event) = serde_json::from_str::<Value>(&row.event.data) else {
            continue;
        };
        let Some(input) = call_input(&event, call.tool_use_id.as_deref()) else {
            continue;
        };
        let creates = creations(input);
        if creates.is_empty() {
            continue;
        }
        let sequence = i64::from(row.event.sequence);
        let mut tied = HashMap::new();
        if let Some(tool_use_id) = call.tool_use_id.as_deref() {
            let page = reader
                .run_events_window(
                    &call.run_id,
                    sequence + 1 + RESULT_SCAN,
                    sequence + 1,
                    RESULT_SCAN as usize,
                    EventFilter::types(&["tool_result"]),
                )
                .await
                .map_err(|error| error.to_string())?;
            for result in page.rows {
                let Ok(data) = serde_json::from_str::<Value>(&result.event.data) else {
                    continue;
                };
                if data.get("toolUseId").and_then(Value::as_str) != Some(tool_use_id) {
                    continue;
                }
                let text = match data.get("toolResult") {
                    Some(Value::String(text)) => text.clone(),
                    Some(other) => other.to_string(),
                    None => String::new(),
                };
                tied = created_by_result(&text);
                break;
            }
        }
        let end = call.ts.max(call.result_ts.unwrap_or(call.ts));
        for (index, title, description) in creates {
            if let Some((project, number)) = tied.get(&index) {
                if key.as_deref() == Some(project.as_str()) && *number == candidate.number {
                    return Ok(Some(FilingText {
                        title,
                        description,
                        source: TextSource::Event,
                        method: Some("result_uri"),
                    }));
                }
                continue;
            }
            let (from, to) = (call.ts - 5, end + 120);
            if title != candidate.title.trim()
                || candidate.created_at < from
                || candidate.created_at > to
            {
                continue;
            }
            let near = db
                .query_one(
                    "SELECT count(*) FROM issues
                     WHERE trim(title) = ?1 AND created_at >= ?2 AND created_at <= ?3",
                    params![title.as_str(), from, to],
                    |row| row.i64(0),
                )
                .await
                .map_err(|error| error.to_string())?;
            if near == 1 {
                return Ok(Some(FilingText {
                    title,
                    description,
                    source: TextSource::Event,
                    method: Some("title_window"),
                }));
            }
        }
    }
    Ok(None)
}

/// The issue's filing text: the creation call's payload when it can be tied,
/// else its current text.
pub async fn filing_text(db: &LocalDb, candidate: &Candidate) -> FilingText {
    match creation_text(db, candidate).await {
        Ok(Some(text)) => text,
        Ok(None) => current_text(candidate),
        Err(error) => {
            log::debug!(
                "token forecast backfill: the creation call of {} could not be read: {error}",
                candidate.issue_id
            );
            current_text(candidate)
        }
    }
}

fn current_text(candidate: &Candidate) -> FilingText {
    FilingText {
        title: candidate.title.clone(),
        description: candidate.description.clone(),
        source: TextSource::Current,
        method: None,
    }
}

/// One backfill pass over every database, recording labels in `labels_db`.
pub async fn pass(
    dbs: &[Arc<LocalDb>],
    labels_db: &LocalDb,
    answerer: &dyn ScoreAnswerer,
    now: i64,
    budget: Budget,
) -> BackfillReport {
    let mut report = BackfillReport::default();
    let answerer_version = answerer.version().to_string();
    let question_version = question::question_version();
    let done = match store::labelled_issue_ids(labels_db, &answerer_version, question_version).await
    {
        Ok(done) => done,
        Err(error) => {
            report.stopped = Some(format!("could not read the labels: {error}"));
            return report;
        }
    };
    let record = |issue_id: &str,
                  status: LabelStatus,
                  skip_reason: Option<String>,
                  answer: Option<ScoreAnswer>,
                  text: Option<(&str, TextSource)>,
                  selection: Option<(&str, &str)>| LabelRecord {
        issue_id: issue_id.to_string(),
        answerer_version: answerer_version.clone(),
        question_version: question_version.to_string(),
        status,
        skip_reason,
        answer,
        text_hash: text.map(|(text, _)| sha256_hex(text)),
        text_source: text.map(|(_, source)| source),
        filing_maker: selection.map(|(maker, _)| maker.to_string()),
        filing_maker_method: selection.map(|(_, method)| method.to_string()),
        source: LabelSource::Backfill,
    };
    let mut calls = 0;
    for db in dbs {
        let ids = match resolved_issue_ids(db, FLOOR).await {
            Ok(ids) => ids,
            Err(error) => {
                log::warn!("token forecast backfill could not list resolved issues: {error}");
                continue;
            }
        };
        for issue_id in ids.iter().filter(|id| !done.contains(*id)) {
            if report.examined >= budget.examined || calls >= budget.calls {
                return report;
            }
            report.examined += 1;
            let candidate = match candidate(db, issue_id).await {
                Ok(Some(candidate)) => candidate,
                Ok(None) => continue,
                Err(error) => {
                    log::warn!("token forecast backfill could not read {issue_id}: {error}");
                    report.failed += 1;
                    continue;
                }
            };
            let selection = match filing_selection(db, issue_id).await {
                Ok(selection) => selection,
                Err(error) => {
                    log::warn!(
                        "token forecast backfill could not read {issue_id}'s filing selection: {error}"
                    );
                    report.failed += 1;
                    continue;
                }
            };
            let (maker, method) = selection.unwrap_or_else(|| ("unknown".to_string(), "none"));
            let write = |label: LabelRecord| async move {
                if let Err(error) = store::upsert_label(labels_db, &label, now).await {
                    log::warn!(
                        "token forecast backfill could not record {}: {error}",
                        label.issue_id
                    );
                }
            };
            if maker != ANTHROPIC {
                let reason = if maker == "unknown" {
                    "maker_unknown"
                } else {
                    "maker_not_anthropic"
                };
                write(record(
                    issue_id,
                    LabelStatus::Skipped,
                    Some(reason.to_string()),
                    None,
                    None,
                    Some((maker.as_str(), method)),
                ))
                .await;
                report.skipped += 1;
                continue;
            }
            let text = filing_text(db, &candidate).await;
            let scrubbed = match question::scrubbed_filing_text(&text.title, &text.description) {
                Ok(scrubbed) => scrubbed,
                Err(refused) => {
                    write(record(
                        issue_id,
                        LabelStatus::Skipped,
                        Some(format!("scrub_refused: {refused}")),
                        None,
                        None,
                        Some((maker.as_str(), method)),
                    ))
                    .await;
                    report.skipped += 1;
                    continue;
                }
            };
            match store::backfill_spend_since(labels_db, now - 86_400).await {
                Ok(spent) if spent + budget.reserve_usd > budget.daily_cap_usd => {
                    report.stopped = Some(format!(
                        "the daily cap of ${:.2} is reached (${spent:.4} in the last 24 hours)",
                        budget.daily_cap_usd
                    ));
                    return report;
                }
                Ok(_) => {}
                Err(error) => {
                    report.stopped = Some(format!("could not read the backfill's spend: {error}"));
                    return report;
                }
            }
            // Reserved before the call and never released, so a refusal, a
            // failure, a timeout or a reply without a cost still counts.
            let spend_id = format!("{issue_id}:{now}:{calls}");
            if let Err(error) =
                store::reserve_spend(labels_db, &spend_id, issue_id, budget.reserve_usd, now).await
            {
                report.stopped = Some(format!("could not reserve the call's spend: {error}"));
                return report;
            }
            calls += 1;
            let settle = |cost: Option<f64>| {
                let spend_id = spend_id.clone();
                async move {
                    let Some(cost) = cost else { return };
                    if let Err(error) = store::settle_spend(labels_db, &spend_id, cost, now).await {
                        log::warn!("token forecast backfill could not settle {spend_id}: {error}");
                    }
                }
            };
            let input = AnswerInput::new(scrubbed);
            let deadline = Instant::now() + budget.call_timeout;
            let text_ref = Some((input.text.as_str(), text.source));
            match tokio::time::timeout(budget.call_timeout, answerer.answer(&input, deadline)).await
            {
                Ok(Ok(answer)) => {
                    settle(answer.provenance.cost_usd).await;
                    write(record(
                        issue_id,
                        LabelStatus::Labelled,
                        None,
                        Some(answer),
                        text_ref,
                        Some((maker.as_str(), method)),
                    ))
                    .await;
                    report.labelled += 1;
                }
                Ok(Err(AnswerError::Refused { reason, cost_usd })) => {
                    settle(cost_usd).await;
                    write(record(
                        issue_id,
                        LabelStatus::Refused,
                        Some(reason),
                        None,
                        text_ref,
                        Some((maker.as_str(), method)),
                    ))
                    .await;
                    report.refused += 1;
                }
                Ok(Err(AnswerError::Unanswerable(reason))) => {
                    // Every call would be refused the same way until the
                    // configuration changes.
                    report.stopped = Some(reason);
                    return report;
                }
                Ok(Err(AnswerError::Failed(error))) => {
                    log::warn!("token forecast backfill: {issue_id} was not answered: {error}");
                    report.failed += 1;
                }
                Err(_) => {
                    log::warn!("token forecast backfill: {issue_id} timed out");
                    report.failed += 1;
                }
            }
        }
    }
    report
}
