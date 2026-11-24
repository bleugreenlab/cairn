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
use cairn_db::turso::Value as SqlValue;
use serde_json::Value;

use super::vocabulary::{GateOutcome, GateVocabulary};
use crate::models::{
    CapacityHold, ExecutionGate, ExecutionGateCensus, GateCensusEntry, GateEvidenceState,
    HeldExecution, ReleaseOverride,
};
use crate::routes::{FactRegistry, TriggerClause};
use crate::storage::{DbResult, LocalDb, RowExt};
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
    if let Some(fact) = super::capacity::capacity_fact(&gate.predicate_json) {
        return render_capacity_gate(fact, gate);
    }
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
        hold: None,
        state,
        evidence,
    }
}

/// A capacity hold renders from the facts the queue stored on its row. It is
/// never latched before release, so one the last decision found clear
/// (`last_outcome = satisfied`) reads satisfied and drops out of the waiting
/// list, while one that blocks reads as the cap it is. A row whose facts are
/// unknown (cleared by a migration, or not yet written) reads as its cap's
/// name until the next sweep rewrites it.
fn render_capacity_gate(fact: &str, gate: &ExecutionGate) -> GateCensusEntry {
    let cap = cap_name(fact);
    let cleared = gate.is_satisfied()
        || gate.last_outcome.as_deref() == Some(GateOutcome::Satisfied.as_str());
    let hold = gate
        .detail
        .as_deref()
        .and_then(|detail| super::capacity::decode_hold(fact, detail));
    let predicate = hold
        .as_ref()
        .map(segment)
        .unwrap_or_else(|| cap.to_string());
    let (state, evidence) = if cleared {
        (
            GateEvidenceState::Satisfied,
            GateOutcome::Satisfied.evidence_phrase().to_string(),
        )
    } else {
        (GateEvidenceState::NoEvidence, cap.to_string())
    };
    GateCensusEntry {
        gate_id: gate.id.clone(),
        predicate,
        hold,
        state,
        evidence,
    }
}

fn cap_name(fact: &str) -> &'static str {
    match fact {
        super::capacity::FLEET_SLOT_FACT => "fleet cap",
        _ => "thread cap",
    }
}

/// `"1st"`, `"2nd"`, ...
pub(crate) fn ordinal(n: usize) -> String {
    let suffix = match (n % 10, n % 100) {
        (_, 11..=13) => "th",
        (1, _) => "st",
        (2, _) => "nd",
        (3, _) => "rd",
        _ => "th",
    };
    format!("{n}{suffix}")
}

/// A hold's segment of the queued line: `39/30 running`, `harness 5/5 running`.
pub fn segment(hold: &CapacityHold) -> String {
    match hold {
        CapacityHold::Fleet { running, limit, .. } => format!("{running}/{limit} running"),
        CapacityHold::Thread {
            thread,
            open,
            limit,
            reentries,
            ..
        } => {
            let mut line = format!("{thread} {}/{limit} running", open.len());
            if *reentries > 0 {
                line.push_str(&format!(" · {reentries} re-entry ahead"));
            }
            line
        }
    }
}

/// A hold's line of hover detail: `Fleet · 3rd · 1 unplaced`, `harness · 1st
/// · 5783, 5790`. The children and the unplaced count live here, never on the
/// line itself.
fn hold_detail(hold: &CapacityHold) -> String {
    let mut parts = Vec::new();
    match hold {
        CapacityHold::Fleet {
            unplaced, place, ..
        } => {
            parts.push("Fleet".to_string());
            parts.extend(place.map(ordinal));
            if *unplaced > 0 {
                parts.push(format!("{unplaced} unplaced"));
            }
        }
        CapacityHold::Thread {
            thread,
            open,
            place,
            ..
        } => {
            parts.push(thread.clone());
            parts.extend(place.map(ordinal));
            if !open.is_empty() {
                let numbers: Vec<String> = open.iter().map(i64::to_string).collect();
                parts.push(numbers.join(", "));
            }
        }
    }
    parts.join(" · ")
}

/// `Queued · 3rd · 39/30 running · harness 5/5 running`. One place shows: the
/// first hold's, which is the fleet's when the fleet cap holds (it sorts
/// first), and none at all when that hold has not been numbered yet.
fn queued_line(place: Option<usize>, segments: impl IntoIterator<Item = String>) -> String {
    let mut parts = vec!["Queued".to_string()];
    parts.extend(place.map(ordinal));
    parts.extend(segments);
    parts.join(" · ")
}

/// The queued line for `holds` as decided, before the queue has numbered
/// them. A surface that can read the census reads its summary instead, which
/// carries the place.
pub fn queued_summary(holds: &[CapacityHold]) -> String {
    queued_line(
        holds.first().and_then(CapacityHold::place),
        holds.iter().map(segment),
    )
}

/// The queued line of one cap-held execution as its census reads now, place
/// included; rendered from `holds` when the census cannot be read or no longer
/// calls it queued.
pub async fn queued_summary_for(
    db: &LocalDb,
    execution_id: &str,
    holds: &[CapacityHold],
) -> String {
    let id = execution_id.to_string();
    let census = db
        .read(move |conn| {
            let id = id.clone();
            Box::pin(
                async move { census_for_executions_conn(conn, std::slice::from_ref(&id)).await },
            )
        })
        .await
        .ok()
        .and_then(|mut censuses| censuses.remove(execution_id));
    match census {
        Some(census) if census.queued => census.summary,
        _ => queued_summary(holds),
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
/// [`wont_start_reason`]). A withdrawn execution may have no gates. The reason
/// is ignored for a released execution, which has already started.
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
    if gates.is_empty() && release_override.is_none() && wont_start.is_none() {
        return None;
    }
    let entries: Vec<GateCensusEntry> = gates
        .iter()
        .map(|gate| render_gate(vocabulary, gate))
        .collect();
    // Whether each entry is a capacity hold, known facts or not: an entry's
    // `hold` is absent for a capacity row whose facts are unknown.
    let capacity: Vec<bool> = gates
        .iter()
        .map(|gate| super::capacity::capacity_fact(&gate.predicate_json).is_some())
        .collect();
    let gated_unreleased = !gates.is_empty() && released_at.is_none();
    let wont_start = wont_start
        .filter(|_| released_at.is_none())
        .map(str::to_string);
    let held = gated_unreleased && wont_start.is_none();
    let unmet_caps: Vec<&GateCensusEntry> = entries
        .iter()
        .zip(&capacity)
        .filter(|(entry, is_cap)| **is_cap && entry.state != GateEvidenceState::Satisfied)
        .map(|(entry, _)| entry)
        .collect();
    let unmet_authored = entries
        .iter()
        .zip(&capacity)
        .any(|(entry, is_cap)| !*is_cap && entry.state != GateEvidenceState::Satisfied);
    let queued = held && !unmet_caps.is_empty() && !unmet_authored;
    let (summary, detail) = match &wont_start {
        // The gates are not what stops it, so the summary does not list them
        // as though they were; `gates` still carries every one.
        Some(reason) => (format!("{WONT_START_PREFIX}: {reason}"), None),
        None if queued => {
            let place = unmet_caps
                .first()
                .and_then(|entry| entry.hold.as_ref())
                .and_then(CapacityHold::place);
            let line = queued_line(
                place,
                unmet_caps.iter().map(|entry| entry.predicate.clone()),
            );
            let mut detail: Vec<String> = unmet_caps
                .iter()
                .map(|entry| match &entry.hold {
                    Some(hold) => hold_detail(hold),
                    None => entry.predicate.clone(),
                })
                .collect();
            // A thread's own reorder moved it ahead (cairn/5848): said once,
            // on hover, never on the terse line.
            // `placed_by` names a deliberate placement only: a row a fleet move
            // froze (`Pin{kind: Frozen}`, cairn/5943) keeps whatever placer it
            // already had, and the move records itself on the moved row alone.
            if let Some(by) = unmet_caps
                .iter()
                .find_map(|entry| entry.hold.as_ref().and_then(CapacityHold::placed_by))
            {
                detail.push(format!("placed ahead by {by}"));
            }
            (line, Some(detail.join("\n")))
        }
        None => (
            summarize(held, &entries, &capacity, release_override.as_ref()),
            None,
        ),
    };
    Some(ExecutionGateCensus {
        held,
        wont_start,
        queued,
        summary,
        detail,
        gates: entries,
        release_override,
        evaluated_at: None,
        job_held: None,
    })
}

/// The census of a started execution whose job its thread's bound holds:
/// `census` when it has one, otherwise a bare one, carrying the hold's words
/// as its summary so the job-held line never reads as running.
fn with_job_held(census: Option<ExecutionGateCensus>, reason: String) -> ExecutionGateCensus {
    let summary = format!("started, but a job is held: {reason}");
    match census {
        Some(census) => ExecutionGateCensus {
            summary,
            job_held: Some(reason),
            ..census
        },
        None => ExecutionGateCensus {
            held: false,
            wont_start: None,
            queued: false,
            summary,
            detail: None,
            gates: Vec::new(),
            release_override: None,
            evaluated_at: None,
            job_held: Some(reason),
        },
    }
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
            cairn_uri::canonical_project(key)
        );
    }
    match (superseded, successor_seq) {
        (true, Some(seq)) => format!("superseded by exec {seq}"),
        _ => "superseded".to_string(),
    }
}

/// One line naming every gate that is not yet met, each with its evidence, for
/// an execution that is not queued. `capacity` says which entries are
/// capacity holds.
fn summarize(
    held: bool,
    entries: &[GateCensusEntry],
    capacity: &[bool],
    release_override: Option<&ReleaseOverride>,
) -> String {
    let unmet = || {
        entries
            .iter()
            .zip(capacity)
            .filter(|(entry, _)| entry.state != GateEvidenceState::Satisfied)
    };
    let waiting: Vec<String> = unmet()
        .filter(|(_, is_cap)| !**is_cap)
        .map(|(entry, _)| format!("{} ({})", entry.predicate, entry.evidence))
        .collect();
    let capped: Vec<&str> = unmet()
        .filter(|(_, is_cap)| **is_cap)
        .map(|(entry, _)| entry.predicate.as_str())
        .collect();
    if held {
        let mut parts = Vec::new();
        if !waiting.is_empty() {
            parts.push(format!("waiting on {}", waiting.join("; ")));
        }
        parts.extend(capped.iter().map(|reason| reason.to_string()));
        return match parts.is_empty() {
            // Every gate latched but the sweep has not released it yet: the
            // window is one tick, and saying so beats calling it waiting.
            true => "held, every gate met — releases on the next sweep".to_string(),
            false => format!("held, {}", parts.join("; ")),
        };
    }
    match release_override {
        Some(release) if release.granted.is_some() => {
            let thread = release
                .granted
                .as_ref()
                .map(|granted| granted.thread.as_str())
                .unwrap_or_default();
            format!(
                "started now by {thread} under the operator's grant ({}){}",
                release.principal,
                skipped_phrase(&release.skipped)
            )
        }
        Some(release) if release.operator => {
            format!(
                "started now by the operator ({}){}",
                release.principal,
                skipped_phrase(&release.skipped)
            )
        }
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

/// `, skipping the fleet and thread caps`: what a Start now skipped.
pub(crate) fn skipped_phrase(skipped: &[String]) -> String {
    // `open_prs` is the open-PR bound; `thread_focus` and `thread_open_prs`
    // are what earlier starts of a held job recorded.
    let words: Vec<&str> = skipped
        .iter()
        .map(|label| match label.as_str() {
            "open_prs" | "thread_open_prs" => "open-PR",
            "thread_focus" => "thread",
            other => other,
        })
        .collect();
    match words.as_slice() {
        [] => String::new(),
        [one] => format!(", skipping the {one} cap"),
        many => format!(", skipping the {} caps", many.join(" and ")),
    }
}

/// The line a text census prints per execution: `exec N — <summary>`, or the
/// summary alone for a queued execution, whose `Queued · …` line names no
/// execution because only one of an issue's executions can be queued.
pub fn census_line(seq: Option<i64>, census: &ExecutionGateCensus) -> String {
    match seq {
        Some(seq) if !census.queued => format!("exec {seq} — {}", census.summary),
        _ => census.summary.clone(),
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
        operator: value
            .get("operator")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        skipped: value
            .get("skipped")
            .and_then(Value::as_array)
            .map(|skipped| {
                skipped
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        granted: value
            .get("granted")
            .and_then(|granted| serde_json::from_value(granted.clone()).ok()),
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
///
/// Every column it takes from `e` and from `successor` is in
/// `idx_executions_hold_facts`, so neither is read from its row (and its
/// snapshot). `e` names no `INDEXED BY`: each filter below seeks it through an
/// `IN` list, which a forced index would turn into a scan of the whole index.
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
               JOIN executions successor INDEXED BY idx_executions_hold_facts
                 ON successor.id = supersession.successor_execution_id
              WHERE supersession.superseded_execution_id = e.id),
            p.key, i.number, i.merged_at, i.closed_at,
            CASE WHEN e.lifecycle = 'withdrawn' THEN
              COALESCE((SELECT json_object('kind', actor_kind, 'ref', actor_ref, 'at', created_at)
                FROM execution_supersessions
                WHERE issue_id = e.issue_id AND superseded_execution_id = e.id AND reason = 'withdrawn'
                ORDER BY created_at DESC, id DESC LIMIT 1), '{}')
            END
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
pub(super) fn census_rows_sql(filter: &str) -> String {
    format!("{CENSUS_ROW_SQL}{filter} ORDER BY e.issue_id, e.seq, e.id")
}

#[track_caller]
fn census_rows<'a, C: cairn_db::storage::QueryConn + ?Sized>(
    conn: &'a C,
    sql: &'a str,
    bound: Vec<SqlValue>,
) -> impl std::future::Future<Output = DbResult<Vec<CensusRow>>> + Send + 'a {
    let rows = conn.query_rows(sql, bound);
    async move {
        let mut rows = rows.await?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            let wont_start = if let Some(withdrawal) = row.opt_text(12)? {
                Some(withdrawal_reason(&withdrawal))
            } else if row.i64(5)? != 0 {
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
}

/// Render `rows` against their gates, keeping only those with a census.
async fn render_rows(
    conn: &crate::storage::TrackedConnection,
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
pub(super) fn executions_census_sql(n: usize) -> String {
    census_rows_sql(&format!(
        "e.id IN ({})
           AND (e.lifecycle = 'withdrawn' OR e.release_override_json IS NOT NULL
                OR EXISTS (SELECT 1 FROM execution_gates g WHERE g.execution_id = e.id))",
        placeholders(n)
    ))
}

/// The census of each execution in `execution_ids` that has one, including
/// started executions with job holds. Three queries per [`IN_LIST_CHUNK`]
/// executions, plus a batched launch-hold projection.
pub async fn census_for_executions_conn(
    conn: &crate::storage::TrackedConnection,
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
        for (execution, reason) in super::open_prs::thread_held_executions_conn(conn, chunk).await?
        {
            let census = with_job_held(out.remove(&execution), reason);
            out.insert(execution, census);
        }
    }
    let mut launch_censuses = HashMap::<String, ExecutionGateCensus>::new();
    // Release is one-way: a launch-side cap can still own a pending builder
    // after the execution's authored gates have released.
    for chunk in execution_ids.chunks(IN_LIST_CHUNK) {
        let sql = format!(
            concat!(
                "SELECT e.id, COALESCE(j.node_name, j.recipe_node_id, j.id), supply_hold.boundary_source, supply_hold.observed_text,
                        supply_hold.created_at, supply_hold.resume_gate,
                        supply_hold.launch_attempts, supply_hold.updated_at
                 FROM executions e JOIN issues i ON i.id = e.issue_id
                 JOIN jobs j ON j.execution_id = e.id
                 JOIN provider_limit_holds supply_hold ON supply_hold.job_id = j.id
                 WHERE e.id IN ({}) AND i.merged_at IS NULL AND i.closed_at IS NULL AND ",
                cairn_db::live_execution_sql!(),
                " AND ", cairn_db::supply_hold_owns_job_sql!(),
                " AND ", cairn_db::launch_held_execution_sql!(),
                " ORDER BY supply_hold.created_at, supply_hold.id"
            ), placeholders(chunk.len())
        );
        let mut rows = conn.query(&sql, bound_ids(chunk)).await?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        while let Some(row) = rows.next().await? {
            let source = row.text(2)?;
            let standing = standing_for_hold(
                &source,
                row.opt_text(3)?.unwrap_or_else(|| source.clone()),
                row.i64(4)?,
                row.opt_text(5)?,
                row.i64(6)?,
                row.i64(7)?,
                None,
            );
            let summary = format!("{} · {}", row.text(1)?, standing.line(now));
            let queued = matches!(standing, FocusStanding::Queued { .. });
            launch_censuses
                .entry(row.text(0)?)
                .and_modify(|census| {
                    census.summary.push_str("; ");
                    census.summary.push_str(&summary);
                    census.queued &= queued;
                })
                .or_insert(ExecutionGateCensus {
                    held: true,
                    wont_start: None,
                    queued,
                    summary,
                    detail: None,
                    gates: Vec::new(),
                    release_override: None,
                    evaluated_at: None,
                    job_held: None,
                });
        }
    }
    for (execution, mut census) in launch_censuses {
        // Launch-side presentation must retain the thread hold's Start now marker.
        census.job_held = out.remove(&execution).and_then(|prior| prior.job_held);
        out.insert(execution, census);
    }
    Ok(out)
}

/// The census statement for gated, unreleased executions selected by `filter`,
/// a SQL predicate over `e`. The gate list is its row source
/// ([`cairn_db::gated_execution_source_sql!`]), so `filter` must not be one
/// the planner would rather drive from.
fn gated_unreleased_rows_sql(filter: &str) -> String {
    census_rows_sql(&format!(
        "{} AND {filter} AND e.issue_id IS NOT NULL AND {}",
        cairn_db::gated_execution_source_sql!(),
        cairn_db::gated_unreleased_sql!()
    ))
}

/// The gated, unreleased executions on `n` issues. The unary `+` keeps
/// `idx_executions_issue_seq` from serving the issue list: through it, every
/// execution on those issues is read from its row, snapshot and all, before
/// the gate test drops the ungated ones.
pub(super) fn issues_gated_sql(n: usize) -> String {
    gated_unreleased_rows_sql(&format!("+e.issue_id IN ({})", placeholders(n)))
}

/// The gated, unreleased executions on the unresolved issues of project `?1`.
/// The project's issues are a per-row probe, not a list of every open issue
/// whose executions would each be read.
pub(super) fn project_gated_sql() -> String {
    gated_unreleased_rows_sql(
        "EXISTS (SELECT 1 FROM issues project_issue
                  WHERE project_issue.id = e.issue_id AND project_issue.project_id = ?1
                    AND project_issue.status NOT IN ('merged', 'closed'))",
    )
}

/// Gated, unreleased census rows paired with their issue id.
/// Both kinds come back — those waiting to start
/// (`census.held`) and those that never will (`census.wont_start`) — for the
/// caller to list apart.
///
/// This reads facts rather than `status = 'held'`: the status projection
/// derives `held` for both kinds (see `cairn_workflow::transitions::outcome`),
/// and only the shared predicate tells them apart.
async fn gated_unreleased_rows(
    conn: &crate::storage::TrackedConnection,
    rows: Vec<CensusRow>,
) -> DbResult<Vec<(String, HeldExecution)>> {
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
    conn: &crate::storage::TrackedConnection,
    issue_ids: &[String],
) -> DbResult<HashMap<String, IssueGatedExecutions>> {
    let mut grouped = HashMap::<String, IssueGatedExecutions>::new();
    // Each issue lands in exactly one chunk, so its executions keep their seq
    // order across the split.
    for chunk in issue_ids.chunks(IN_LIST_CHUNK) {
        let rows = census_rows(conn, &issues_gated_sql(chunk.len()), bound_ids(chunk)).await?;
        let gated = gated_unreleased_rows(conn, rows).await?;
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
    conn: &crate::storage::TrackedConnection,
    project_id: &str,
) -> DbResult<Vec<ProjectHeldExecution>> {
    let rows = census_rows(
        conn,
        &project_gated_sql(),
        vec![SqlValue::Text(project_id.to_string())],
    )
    .await?;
    let held = gated_unreleased_rows(conn, rows).await?;
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

    #[tokio::test]
    async fn census_rows_preserves_caller_statement_identity() {
        use std::time::Duration;

        let db = crate::storage::migrated_test_db("census-tracking.db").await;
        let conn = db.connect().await.unwrap();
        let sql = "SELECT ?1, 'i', 2, NULL, NULL, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL";
        let line = line!() + 1;
        let rows = census_rows(&conn, sql, vec![SqlValue::Text("e".into())])
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "e");
        assert_eq!(rows[0].issue_id.as_deref(), Some("i"));
        assert_eq!(rows[0].seq, Some(2));
        assert!(rows[0].released_at.is_none());
        assert!(rows[0].wont_start.is_none());
        let holders = db.overdue_holders(Duration::ZERO, Duration::ZERO);
        assert_eq!(holders.len(), 1);
        assert_eq!(&*holders[0].statement, format!("{}:{line} {sql}", file!()));

        let raw_rows = census_rows(&*conn, sql, vec![SqlValue::Text("raw".into())])
            .await
            .unwrap();
        assert_eq!(raw_rows[0].id, "raw");
        assert!(census_rows(&conn, "SELECT missing_column", vec![])
            .await
            .is_err());
    }

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
    /// probes and the supersession subquery included, must be a seek, and
    /// every execution, the successor too, is read from
    /// `idx_executions_hold_facts` rather than its row, whose snapshot a row
    /// read pays for (cairn/5894). The one scan allowed is the gate list the
    /// issue and project statements are driven from, over a covering index.
    #[tokio::test]
    async fn census_statements_never_scan() {
        let db = crate::storage::migrated_test_db("gate-census-plan.db").await;
        for (label, sql, bound) in [
            ("issues", issues_gated_sql(3), 3),
            ("executions", executions_census_sql(3), 3),
            ("project", project_gated_sql(), 1),
        ] {
            let plan = query_plan(&db, sql, bound).await;
            for alias in ["e", "successor"] {
                assert!(
                    plan.iter().any(|line| line
                        == &format!(
                            "SEARCH {alias} USING COVERING INDEX idx_executions_hold_facts (id=?)"
                        )),
                    "{label}: {alias} is sought in the hold-facts index: {plan:#?}"
                );
            }
            assert!(
                plan.iter()
                    .any(|line| line.contains("idx_execution_gates_execution")),
                "{label}: the gate probes seek by execution: {plan:#?}"
            );
            assert!(
                !plan.iter().any(|line| line.starts_with("SCAN")
                    && !line
                        .starts_with("SCAN execution_gates AS gated_source USING COVERING INDEX")),
                "{label}: no table in the census is scanned: {plan:#?}"
            );
            assert!(
                !plan
                    .iter()
                    .any(|line| line.contains("sqlite_autoindex_executions_1")),
                "{label}: no execution is read from its row: {plan:#?}"
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
                format!("UPDATE issues SET status = '{resolution}', {column} = 5 WHERE id = 'i'"),
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
            detail: None,
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
                at: Some(10),
                operator: false,
                skipped: Vec::new(),
                granted: None,
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

    fn capacity_gate(id: &str, fact: &str, seq: i64, hold: Option<&CapacityHold>) -> ExecutionGate {
        use crate::execution::gates::capacity::{capacity_predicate, encode_hold};
        ExecutionGate {
            id: id.into(),
            seq,
            last_outcome: Some("no_evidence".into()),
            detail: hold.map(encode_hold),
            ..gate(&capacity_predicate(fact))
        }
    }

    /// Every way a queued execution reads, as the operator sees it: one terse
    /// line with one place, the children and the unplaced count on hover.
    #[test]
    fn a_queued_execution_reads_one_terse_line_with_its_detail_on_hover() {
        use crate::execution::gates::capacity::{FLEET_SLOT_FACT, THREAD_FOCUS_FACT};
        let fleet = CapacityHold::Fleet {
            running: 39,
            limit: 30,
            unplaced: 1,
            place: Some(3),
            placed_by: None,
            pin: None,
        };
        let thread = CapacityHold::Thread {
            thread: "harness".into(),
            open: vec![5783, 5790, 5791, 5792, 5793],
            reentries: 0,
            limit: 5,
            place: Some(1),
            placed_by: None,
            pin: None,
        };
        let fleet_gate = capacity_gate("gf", FLEET_SLOT_FACT, 1000, Some(&fleet));
        let thread_gate = capacity_gate("gt", THREAD_FOCUS_FACT, 1001, Some(&thread));
        let read = |gates: &[ExecutionGate]| census(gates, None, None, None).unwrap();
        let mut rendered = Vec::new();

        let fleet_only = read(std::slice::from_ref(&fleet_gate));
        assert!(fleet_only.held && fleet_only.queued);
        assert_eq!(fleet_only.summary, "Queued · 3rd · 39/30 running");
        assert_eq!(
            fleet_only.detail.as_deref(),
            Some("Fleet · 3rd · 1 unplaced")
        );
        assert_eq!(
            census_line(Some(2), &fleet_only),
            "Queued · 3rd · 39/30 running",
            "a queued line names no execution"
        );
        assert_eq!(fleet_only.gates[0].evidence, "fleet cap");
        assert_eq!(fleet_only.gates[0].hold.as_ref(), Some(&fleet));
        rendered.push(fleet_only);

        let thread_only = read(std::slice::from_ref(&thread_gate));
        assert_eq!(thread_only.summary, "Queued · 1st · harness 5/5 running");
        assert_eq!(
            thread_only.detail.as_deref(),
            Some("harness · 1st · 5783, 5790, 5791, 5792, 5793")
        );
        rendered.push(thread_only);

        let both = read(&[fleet_gate.clone(), thread_gate.clone()]);
        assert_eq!(
            both.summary, "Queued · 3rd · 39/30 running · harness 5/5 running",
            "one place, the fleet's"
        );
        assert_eq!(
            both.detail.as_deref(),
            Some("Fleet · 3rd · 1 unplaced\nharness · 1st · 5783, 5790, 5791, 5792, 5793")
        );
        rendered.push(both);

        // A thread's reorder: one hover line naming it, and the terse line
        // unchanged.
        let mut moved_fleet = fleet.clone();
        moved_fleet.set_placed_by(Some("harness".into()));
        let mut moved_thread = thread.clone();
        moved_thread.set_placed_by(Some("harness".into()));
        let moved = read(&[
            capacity_gate("gf", FLEET_SLOT_FACT, 1000, Some(&moved_fleet)),
            capacity_gate("gt", THREAD_FOCUS_FACT, 1001, Some(&moved_thread)),
        ]);
        assert_eq!(
            moved.summary,
            "Queued · 3rd · 39/30 running · harness 5/5 running"
        );
        assert_eq!(
            moved.detail.as_deref(),
            Some(
                "Fleet · 3rd · 1 unplaced\nharness · 1st · 5783, 5790, 5791, 5792, 5793\n\
                 placed ahead by harness"
            )
        );
        rendered.push(moved);

        let unnumbered = CapacityHold::Fleet {
            running: 6,
            limit: 6,
            unplaced: 0,
            place: None,
            placed_by: None,
            pin: None,
        };
        let fresh = read(&[capacity_gate(
            "gn",
            FLEET_SLOT_FACT,
            1000,
            Some(&unnumbered),
        )]);
        assert_eq!(fresh.summary, "Queued · 6/6 running");
        assert_eq!(fresh.detail.as_deref(), Some("Fleet"));
        assert_eq!(queued_summary(&[unnumbered]), "Queued · 6/6 running");
        rendered.push(fresh);

        let unknown = read(&[
            capacity_gate("gu", FLEET_SLOT_FACT, 1000, None),
            capacity_gate("gv", THREAD_FOCUS_FACT, 1001, None),
        ]);
        assert!(
            unknown.queued,
            "a capacity row with no facts is still a cap"
        );
        assert_eq!(unknown.summary, "Queued · fleet cap · thread cap");
        assert_eq!(unknown.detail.as_deref(), Some("fleet cap\nthread cap"));
        assert_eq!(unknown.gates[0].hold, None);
        rendered.push(unknown);

        // An authored gate still waiting: not queued, and it keeps its label.
        let mixed = read(&[gate(CHECK), fleet_gate]);
        assert!(mixed.held && !mixed.queued);
        assert_eq!(mixed.detail, None);
        assert!(
            census_line(Some(1), &mixed).starts_with("exec 1 — held, waiting on check"),
            "{}",
            mixed.summary
        );
        rendered.push(mixed);

        let text = format!("{rendered:?}");
        assert!(!text.contains("in progress"), "{text}");
        assert!(!text.contains("waiting for a machine"), "{text}");
    }
}

/// Derived display state, never an execution or job status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FocusStanding {
    Running,
    Queued {
        reentry: bool,
        place: Option<usize>,
        reason: String,
        since: i64,
    },
    HeldForPrs {
        reason: String,
    },
    Stopped,
    /// Its turn reads running, but this host holds no process for it: only a
    /// run row still reads live, last updated at `since`. Neither running nor
    /// known to be dead, so it holds no slot and nothing resumes it.
    Unknown {
        since: i64,
    },
    /// Held by a gated hold that only the operator's Continue or Start now
    /// releases. It is out of the band, so it has no place in line.
    NeedsOperator {
        reason: String,
        since: i64,
    },
    Parked {
        cause: FocusParkCause,
    },
}

impl FocusStanding {
    pub(crate) fn view(&self, observed_at: i64) -> crate::models::FocusStandingView {
        let kind = match self {
            Self::Running => crate::models::FocusStandingKind::Running,
            Self::Queued { .. } => crate::models::FocusStandingKind::Queued,
            Self::HeldForPrs { .. } => crate::models::FocusStandingKind::HeldForPrs,
            Self::Stopped | Self::Unknown { .. } => crate::models::FocusStandingKind::Stopped,
            Self::NeedsOperator { .. } => crate::models::FocusStandingKind::NeedsOperator,
            Self::Parked { .. } => crate::models::FocusStandingKind::Parked,
        };
        let (reentry, place) = match self {
            Self::Queued { reentry, place, .. } => (*reentry, *place),
            _ => (false, None),
        };
        let label = match self {
            Self::Parked { cause } => format!("Parked · {}", cause.render_line(None)),
            Self::HeldForPrs { reason } => format!(
                "Held · thread {}",
                reason.split(':').next().unwrap_or(reason)
            ),
            _ => self.render_line(None),
        };
        crate::models::FocusStandingView {
            observed_at: Some(observed_at),
            label: Some(label),
            kind,
            line: self.render_line(None),
            reentry,
            place,
        }
    }

    pub(crate) fn line(&self, now: i64) -> String {
        self.render_line(Some(now))
    }

    fn render_line(&self, now: Option<i64>) -> String {
        match self {
            Self::Running => "Running".into(),
            Self::Stopped => "Stopped".into(),
            Self::Unknown { since } => format!(
                "Unknown · reads running with no process on this host · its run last updated {}",
                standing_age(now, *since)
            ),
            Self::Queued {
                reentry,
                place,
                reason,
                since,
            } => {
                let band = if *reentry { "re-entry" } else { "fresh" };
                let place = place
                    .map(|n| format!(" · {} in line", ordinal(n)))
                    .unwrap_or_default();
                format!(
                    "Queued · {band}{place} · {reason} · held {}",
                    standing_age(now, *since)
                )
            }
            Self::HeldForPrs { reason } => format!("Held by open-PR bound · thread {reason}"),
            Self::NeedsOperator { reason, since } => format!(
                "Stopped · needs the operator · {reason} · {}",
                standing_age(now, *since)
            ),
            Self::Parked { cause } => format!("Parked · {}", cause.render_line(now)),
        }
    }

    pub(crate) fn tier(&self) -> usize {
        match self {
            Self::Running => 0,
            Self::Queued { reentry: true, .. } => 1,
            Self::Queued { .. } => 2,
            Self::HeldForPrs { .. } => 3,
            Self::Parked { .. } => 4,
            Self::Stopped | Self::Unknown { .. } | Self::NeedsOperator { .. } => 5,
        }
    }
}

/// What a hold's `resume_gate` asks of the operator.
fn gate_phrase(gate: &str, attempts: i64) -> String {
    use crate::orchestrator::wakes::{RESUME_GATE_LAUNCH_FAILED, RESUME_GATE_TOOL_IN_FLIGHT};
    match gate {
        RESUME_GATE_LAUNCH_FAILED => {
            format!("launch failed {attempts} times; Continue or Start now retries it")
        }
        RESUME_GATE_TOOL_IN_FLIGHT => {
            "a tool call has no recorded result; Continue resumes it".into()
        }
        other => format!("held ({other}); Continue resumes it"),
    }
}

fn standing_for_hold(
    source: &str,
    reason: String,
    since: i64,
    gate: Option<String>,
    attempts: i64,
    updated: i64,
    place: Option<usize>,
) -> FocusStanding {
    match (gate, source) {
        (Some(gate), _) => FocusStanding::NeedsOperator {
            reason: gate_phrase(&gate, attempts),
            since: updated,
        },
        (None, "thread_focus") => FocusStanding::Queued {
            reentry: true,
            place,
            reason,
            since,
        },
        (None, "thread_open_prs") => FocusStanding::HeldForPrs { reason },
        (None, _) => FocusStanding::Parked {
            cause: FocusParkCause::Other(format!("held ({source}) · {reason}")),
        },
    }
}

/// One batched projection for queue, overview and seed. Listing membership still
/// comes from the open-child predicate; activity is read through the caps' sole
/// classifier, against `observer`, so a child reads running exactly when the
/// focus cap would count it. A running task outranks every wait on its parent.
pub(crate) async fn focus_standings_conn(
    conn: &crate::storage::TrackedConnection,
    thread_id: &str,
    observer: &crate::execution::jobs::liveness::WorkObserver,
) -> DbResult<HashMap<String, FocusStanding>> {
    use super::capacity::{activity_job_rows_conn, Activity, ActivityScope, ParkCause};
    use cairn_db::turso::params;
    let mut standings = HashMap::new();
    let mut children = conn
        .query(
            concat!(
                "SELECT i.id FROM issues i INDEXED BY idx_issues_parent_thread_id WHERE i.parent_thread_id = ?1 AND ",
                cairn_db::open_thread_child_sql!()
            ),
            params![thread_id],
        )
        .await?;
    let mut ids = Vec::new();
    while let Some(row) = children.next().await? {
        let id = row.text(0)?;
        standings.insert(
            id.clone(),
            FocusStanding::Parked {
                cause: FocusParkCause::Inactive,
            },
        );
        ids.push(id);
    }
    drop(children);

    // Fresh held starts are not necessarily open under the listing predicate.
    for (index, queued) in super::reorder::thread_queue_conn(conn, thread_id)
        .await?
        .iter()
        .enumerate()
    {
        let mut rows = conn
            .query(
                "SELECT issue_id FROM executions WHERE id = ?1",
                params![queued.execution_id.as_str()],
            )
            .await?;
        if let Some(row) = rows.next().await? {
            let id = row.text(0)?;
            standings.insert(
                id,
                FocusStanding::Queued {
                    reentry: false,
                    place: Some(index + 1),
                    reason: super::reorder::places_phrase(queued, " · "),
                    since: queued.held_since,
                },
            );
        }
    }

    let mut holds = conn
        .query(
            concat!(
            "SELECT i.id, supply_hold.boundary_source, supply_hold.observed_text, supply_hold.created_at,
                    supply_hold.resume_gate, supply_hold.launch_attempts, supply_hold.updated_at
             FROM issues i INDEXED BY idx_issues_parent_thread_id
             JOIN executions e ON e.issue_id = i.id JOIN jobs j ON j.execution_id = e.id
             JOIN provider_limit_holds supply_hold ON supply_hold.job_id = j.id
             WHERE i.parent_thread_id = ?1 AND i.merged_at IS NULL AND i.closed_at IS NULL AND ",
             cairn_db::live_execution_sql!(),
             " AND (", cairn_db::supply_hold_owns_job_sql!(),
             " OR (supply_hold.state = 'active' AND supply_hold.kind = 'yield' AND j.current_turn_id = supply_hold.turn_id))
             ORDER BY COALESCE(supply_hold.queue_key, supply_hold.created_at * 1000000), supply_hold.id"
            ),
            params![thread_id],
        )
        .await?;
    let mut place = 0;
    while let Some(row) = holds.next().await? {
        let id = row.text(0)?;
        let source = row.text(1)?;
        let reason = row.opt_text(2)?.unwrap_or_else(|| source.clone());
        // A gated hold is out of the band (`cairn_db::reentry_band_hold_sql!`):
        // nothing but the operator releases it, so it takes no place in line.
        if source == "thread_focus" && row.opt_text(4)?.is_none() {
            place += 1;
        }
        let standing = standing_for_hold(
            &source,
            reason,
            row.i64(3)?,
            row.opt_text(4)?,
            row.i64(5)?,
            row.i64(6)?,
            Some(place),
        );
        standings
            .entry(id)
            .and_modify(|old| {
                if standing.tier() < old.tier()
                    || matches!(
                        old,
                        FocusStanding::Parked {
                            cause: FocusParkCause::Inactive
                        }
                    )
                {
                    *old = standing.clone();
                }
            })
            .or_insert(standing);
    }
    drop(holds);

    let scope = crate::execution::approvals::ApprovalScope::Issues(ids);
    for approval in crate::execution::approvals::pending_approvals_conn(conn, &scope).await? {
        let mut parent = conn.query(
            "SELECT p.key, pi.number FROM issues i LEFT JOIN issues pi ON pi.id = i.parent_issue_id
             LEFT JOIN projects p ON p.id = pi.project_id WHERE i.id = ?1",
            params![approval.issue_id.as_str()]).await?;
        let who = match parent.next().await? {
            Some(row) => match (row.opt_text(0)?, row.opt_i64(1)?) {
                (Some(key), Some(number)) => format!("{key}/{number}"),
                _ => "you".into(),
            },
            None => "approver unknown".into(),
        };
        let cause = FocusParkCause::PlanAwaiting {
            what: approval.what().into(),
            who,
            since: approval.pending_since,
            deferred_until: approval.deferral.map(|d| d.until_rendered),
        };
        if !matches!(
            standings.get(&approval.issue_id),
            Some(
                FocusStanding::Queued { .. }
                    | FocusStanding::HeldForPrs { .. }
                    | FocusStanding::NeedsOperator { .. }
            )
        ) {
            standings.insert(approval.issue_id, FocusStanding::Parked { cause });
        }
    }

    let mut prs = conn.query(
        "SELECT i.id, m.github_pr_number, m.checks_status, m.checks_json FROM issues i INDEXED BY idx_issues_parent_thread_id JOIN merge_requests m ON m.issue_id = i.id
         WHERE i.parent_thread_id = ?1 AND i.merged_at IS NULL AND i.closed_at IS NULL
         AND m.status = 'open' ORDER BY m.opened_at", params![thread_id]).await?;
    while let Some(row) = prs.next().await? {
        let id = row.text(0)?;
        let number = row.opt_i64(1)?;
        let status = row.opt_text(2)?;
        let checks: Option<Vec<crate::models::Check>> = row
            .opt_text(3)?
            .and_then(|json| serde_json::from_str(&json).ok());
        let lanes = match status.as_deref() {
            Some("SUCCESS") => "merge".into(),
            Some("FAILURE" | "ERROR") => {
                let names = checks
                    .as_ref()
                    .map(|checks| {
                        checks
                            .iter()
                            .filter(|c| c.state == crate::models::CheckState::Failure)
                            .map(|c| c.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_default();
                if names.is_empty() {
                    "red lanes (names unavailable)".into()
                } else {
                    format!("red lanes ({names})")
                }
            }
            Some("PENDING") => {
                let names = checks
                    .as_ref()
                    .map(|checks| {
                        checks
                            .iter()
                            .filter(|c| c.state == crate::models::CheckState::Pending)
                            .map(|c| c.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_default();
                if names.is_empty() {
                    "lanes (names unavailable)".into()
                } else {
                    format!("lanes ({names})")
                }
            }
            _ => "lanes (no current PR verdict available)".into(),
        };
        if matches!(
            standings.get(&id),
            Some(FocusStanding::Parked {
                cause: FocusParkCause::Inactive
            })
        ) {
            standings.insert(
                id,
                FocusStanding::Parked {
                    cause: FocusParkCause::PrAwaiting { number, lanes },
                },
            );
        }
    }
    drop(prs);

    // Operator waits are excluded from the working-row SQL, so read their head
    // separately instead of guessing from the denormalised job status.
    let mut operators = conn
        .query(
            concat!("SELECT i.id, t.yield_reason, COALESCE(t.ended_at, t.updated_at) FROM issues i INDEXED BY idx_issues_parent_thread_id
         JOIN executions e ON e.issue_id = i.id JOIN jobs j ON j.execution_id = e.id
         JOIN turns t ON t.id = j.current_turn_id
         WHERE i.parent_thread_id = ?1 AND i.merged_at IS NULL AND i.closed_at IS NULL
         AND j.status = 'running' AND t.state = 'yielded'
         AND t.yield_reason IN ('user_input', 'permission', 'safety_pause') AND ", cairn_db::live_execution_sql!(), " AND NOT (", cairn_db::gated_unreleased_sql!(), ")"),
            params![thread_id],
        )
        .await?;
    while let Some(row) = operators.next().await? {
        let id = row.text(0)?;
        if matches!(
            standings.get(&id),
            Some(FocusStanding::Parked {
                cause: FocusParkCause::Inactive
            })
        ) || matches!(
            standings.get(&id),
            Some(FocusStanding::Parked {
                cause: FocusParkCause::PrAwaiting { .. }
            })
        ) {
            let kind = if row.text(1)? == "user_input" {
                "question"
            } else {
                "permission"
            };
            standings.insert(
                id,
                FocusStanding::Parked {
                    cause: FocusParkCause::OperatorAwaiting {
                        kind: kind.into(),
                        who: "operator".into(),
                        since: row.i64(2)?,
                    },
                },
            );
        }
    }
    drop(operators);

    for row in activity_job_rows_conn(conn, ActivityScope::Thread(thread_id), observer).await? {
        let Some(id) = row.issue_id else { continue };
        if row.activity == Activity::InFlight {
            standings.insert(id, FocusStanding::Running);
        } else if !matches!(
            standings.get(&id),
            Some(
                FocusStanding::Running
                    | FocusStanding::Queued { .. }
                    | FocusStanding::HeldForPrs { .. }
                    | FocusStanding::NeedsOperator { .. }
            )
        ) {
            let Activity::Parked(cause) = row.activity else {
                continue;
            };
            let standing = match cause {
                ParkCause::Stopped => {
                    // A complete head on a job still running or pending is the
                    // gap between a turn ending and its flush or follow-up, or
                    // a published result awaiting merge: not a stop. It keeps
                    // its parked standing; failed, interrupted and cancelled
                    // heads read as stopped. Neither counts as in flight.
                    let mut head = conn.query("SELECT t.state FROM jobs j JOIN turns t ON t.id = j.current_turn_id WHERE j.id = ?1", params![row.job_id.as_str()]).await?;
                    let complete = match head.next().await? {
                        Some(head) => head.text(0)? == "complete",
                        None => false,
                    };
                    drop(head);
                    if complete {
                        standings.entry(id).or_insert(FocusStanding::Parked {
                            cause: FocusParkCause::Inactive,
                        });
                        continue;
                    }
                    FocusStanding::Stopped
                }
                ParkCause::LivenessUnknown(since) => FocusStanding::Unknown { since },
                // Its predecessors' own rows say where the child stands.
                ParkCause::Operator | ParkCause::AwaitingPredecessors => continue,
                ParkCause::Supply => FocusStanding::Parked {
                    cause: FocusParkCause::Other("held (supply)".into()),
                },
                ParkCause::Dependencies => FocusStanding::Parked {
                    cause: FocusParkCause::Other("awaiting task work".into()),
                },
                ParkCause::LanesAwaited => FocusStanding::Parked {
                    cause: FocusParkCause::LanesAwaited,
                },
                ParkCause::SleepingUntil(deadline) => FocusStanding::Parked {
                    cause: FocusParkCause::SleepingUntil(deadline),
                },
                ParkCause::Unclassified(reason) => FocusStanding::Parked {
                    cause: FocusParkCause::Other(reason),
                },
            };
            if matches!(
                standings.get(&id),
                None | Some(FocusStanding::Parked {
                    cause: FocusParkCause::Inactive | FocusParkCause::PrAwaiting { .. }
                })
            ) {
                standings.insert(id, standing);
            }
        }
    }
    Ok(standings)
}

fn standing_stamp(at: i64) -> String {
    crate::clock::stamp(at).unwrap_or_else(|| format!("{at} (timestamp unavailable)"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FocusParkCause {
    Inactive,
    PlanAwaiting {
        what: String,
        who: String,
        since: i64,
        deferred_until: Option<String>,
    },
    PrAwaiting {
        number: Option<i64>,
        lanes: String,
    },
    OperatorAwaiting {
        kind: String,
        who: String,
        since: i64,
    },
    LanesAwaited,
    SleepingUntil(Option<i64>),
    Other(String),
}

fn standing_age(now: Option<i64>, since: i64) -> String {
    match now {
        Some(now) => crate::clock::format_elapsed(now.saturating_sub(since).max(0)),
        None => format!("since {}", standing_stamp(since)),
    }
}

impl FocusParkCause {
    fn render_line(&self, now: Option<i64>) -> String {
        match self {
            Self::Inactive => "no agent work in flight".into(),
            Self::PlanAwaiting {
                what,
                who,
                since,
                deferred_until,
            } => {
                let waited = now.map(|now| now.saturating_sub(*since).max(0));
                let age = standing_age(now, *since);
                let head = format!("{what} awaiting {who}");
                match deferred_until {
                    Some(until) => format!("{head} · deferred until {until} · {age}"),
                    None if waited.is_some_and(|waited| {
                        waited > crate::execution::approvals::STALE_AFTER_SECS
                    }) =>
                    {
                        format!("{head} · {age} · stale")
                    }
                    None => format!("{head} · {age}"),
                }
            }
            Self::PrAwaiting { number, lanes } => format!(
                "PR {} awaiting {lanes}",
                number
                    .map(|n| format!("#{n}"))
                    .unwrap_or_else(|| "(number unavailable)".into())
            ),
            Self::OperatorAwaiting { kind, who, since } => {
                format!("{kind} awaiting {who} · {}", standing_age(now, *since))
            }
            Self::LanesAwaited => "awaiting lanes".into(),
            Self::SleepingUntil(deadline) => deadline
                .map(|ms| format!("waiting until {}", standing_stamp(ms / 1000)))
                .unwrap_or_else(|| "sleeping (deadline unavailable)".into()),
            Self::Other(reason) => reason.clone(),
        }
    }
}

#[cfg(test)]
mod focus_snapshot_tests {
    use super::*;

    #[test]
    fn source_timestamps_do_not_tick_cached_display_text() {
        let standings = [
            FocusStanding::Queued {
                reentry: true,
                place: None,
                reason: "focus".into(),
                since: 1,
            },
            FocusStanding::NeedsOperator {
                reason: "resume".into(),
                since: 1,
            },
            FocusStanding::Parked {
                cause: FocusParkCause::PlanAwaiting {
                    what: "plan".into(),
                    who: "you".into(),
                    since: 1,
                    deferred_until: None,
                },
            },
            FocusStanding::Parked {
                cause: FocusParkCause::OperatorAwaiting {
                    kind: "question".into(),
                    who: "operator".into(),
                    since: 1,
                },
            },
        ];
        for standing in standings {
            let earlier = standing.view(100);
            let later = standing.view(100_000);
            assert_eq!(earlier.line, later.line);
            assert_eq!(earlier.label, later.label);
            assert_eq!(earlier.observed_at, Some(100));
            assert_eq!(later.observed_at, Some(100_000));
        }
        let unranked =
            standing_for_hold("thread_focus", "focus".into(), 1, None, 0, 1, None).view(100);
        assert_eq!(unranked.place, None);
    }
}

fn withdrawal_reason(record: &str) -> String {
    let value: serde_json::Value = serde_json::from_str(record).unwrap_or_default();
    let kind = value["kind"].as_str().unwrap_or("unknown actor");
    let actor = match value["ref"].as_str() {
        Some(reference) => format!("{kind} {reference}"),
        None => kind.to_string(),
    };
    let at = value["at"]
        .as_i64()
        .map(|at| crate::clock::stamp(at).unwrap_or_else(|| at.to_string()))
        .unwrap_or_else(|| "unknown time".into());
    format!("withdrawn by {actor} at {at}; never started")
}
